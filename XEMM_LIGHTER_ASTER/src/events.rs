//! Unified, venue-agnostic event model and the JSONL run log. Events are written
//! in capture order (already `(local_recv_ts, seq)`-sorted) with a `RunHeader`
//! as the first line, so a streaming read is replay-ready. Decimals serialize as
//! strings (rust_decimal `serde-str`).

use std::fs::File;
use std::io::{self, BufRead, BufReader, BufWriter, Read, Seek, SeekFrom, Write};
use std::path::Path;

use anyhow::{anyhow, Context, Result};
use chrono::{DateTime, Utc};
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};

use crate::config::Config;
use crate::markets::MarketSpec;
use crate::types::{MarketId, Side};

pub type PriceLevel = (Decimal, Decimal);

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Event {
    pub seq: u64,
    pub local_recv_ts: DateTime<Utc>,
    pub market: MarketId,
    pub kind: EventKind,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum EventKind {
    /// Aster top-20 partial-depth snapshot (whole-book replace).
    AsterDepth {
        bids: Vec<PriceLevel>,
        asks: Vec<PriceLevel>,
        exch_ts: DateTime<Utc>,
    },
    /// Aster aggregated trade print.
    AsterAggTrade {
        price: Decimal,
        qty: Decimal,
        buyer_is_maker: bool,
        exch_ts: DateTime<Utc>,
    },
    /// Hyperliquid 20-level l2Book snapshot (whole-book replace).
    HlL2Book {
        bids: Vec<PriceLevel>,
        asks: Vec<PriceLevel>,
        exch_ts: DateTime<Utc>,
    },
    /// Hyperliquid trade (diagnostics only; the hedge uses the HL book).
    HlTrade {
        side: Side, // aggressor
        price: Decimal,
        qty: Decimal,
        exch_ts: DateTime<Utc>,
    },
}

/// First line of a run log: enough to replay without touching the network.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RunHeader {
    pub run_id: String,
    pub started_at: DateTime<Utc>,
    pub mode: String,
    pub code_version: String,
    pub config: Config,
    pub market_specs: Vec<MarketSpec>,
}

pub fn write_header<W: Write>(w: &mut W, header: &RunHeader) -> Result<()> {
    serde_json::to_writer(&mut *w, header)?;
    w.write_all(b"\n")?;
    Ok(())
}

pub fn write_event<W: Write>(w: &mut W, ev: &Event) -> Result<()> {
    serde_json::to_writer(&mut *w, ev)?;
    w.write_all(b"\n")?;
    Ok(())
}

/// zstd compression level used when the run-log path ends in `.zst`. Level 3 is zstd's
/// default — a strong size/speed tradeoff; the recorder runs at a few dozen events/sec so
/// it is never CPU-bound here. Decompressed bytes are identical regardless of level.
const ZSTD_LEVEL: i32 = 3;

/// zstd frame magic number (the four leading bytes of every zstd stream). Used to
/// auto-detect a compressed log on read, by content rather than by file extension.
const ZSTD_MAGIC: [u8; 4] = [0x28, 0xB5, 0x2F, 0xFD];

/// Run-log sink: either a plain buffered file or a zstd-compressed stream. The
/// `write_header`/`write_event` helpers work through it unchanged (it implements `Write`),
/// and the *decompressed* byte stream is identical to the plain JSONL — so a recorded log
/// replays deterministically regardless of which variant produced it.
pub enum LogWriter {
    Plain(BufWriter<File>),
    Zstd(zstd::stream::write::Encoder<'static, BufWriter<File>>),
}

impl LogWriter {
    /// Finish the stream cleanly. For zstd this writes the frame epilogue (required for a
    /// fully-readable file) and flushes the inner file; for plain it just flushes. Must be
    /// called once on the clean-shutdown path (consumes `self`).
    pub fn finish(self) -> Result<()> {
        match self {
            LogWriter::Plain(mut w) => w.flush()?,
            LogWriter::Zstd(enc) => enc.finish()?.flush()?,
        }
        Ok(())
    }
}

impl Write for LogWriter {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        match self {
            LogWriter::Plain(w) => w.write(buf),
            LogWriter::Zstd(w) => w.write(buf),
        }
    }
    fn flush(&mut self) -> io::Result<()> {
        match self {
            LogWriter::Plain(w) => w.flush(),
            LogWriter::Zstd(w) => w.flush(),
        }
    }
}

/// Create a run-log sink, choosing zstd compression iff `path` ends in `.zst`. Creates
/// parent dirs is the caller's job (unchanged from before).
pub fn open_log_writer(path: impl AsRef<Path>) -> Result<LogWriter> {
    let path = path.as_ref();
    let file = File::create(path).with_context(|| format!("creating {}", path.display()))?;
    let buf = BufWriter::new(file);
    if path.extension().and_then(|e| e.to_str()) == Some("zst") {
        let enc = zstd::stream::write::Encoder::new(buf, ZSTD_LEVEL)
            .with_context(|| format!("starting zstd encoder for {}", path.display()))?;
        Ok(LogWriter::Zstd(enc))
    } else {
        Ok(LogWriter::Plain(buf))
    }
}

/// Open a run log for reading, transparently decompressing a zstd stream. Detection is by
/// the leading magic bytes (not the extension), so a correctly-built log always reads even
/// if it was named without `.zst`.
fn open_log_reader(path: &Path) -> Result<Box<dyn BufRead>> {
    let mut file = File::open(path).with_context(|| format!("opening {}", path.display()))?;
    let mut magic = [0u8; 4];
    let n = read_up_to(&mut file, &mut magic)?;
    file.seek(SeekFrom::Start(0))
        .with_context(|| format!("rewinding {}", path.display()))?;
    if n == 4 && magic == ZSTD_MAGIC {
        let dec = zstd::stream::read::Decoder::new(file)
            .with_context(|| format!("starting zstd decoder for {}", path.display()))?;
        Ok(Box::new(BufReader::new(dec)))
    } else {
        Ok(Box::new(BufReader::new(file)))
    }
}

/// Read up to `buf.len()` bytes, returning how many were read (a short file yields < len
/// rather than erroring), so magic-byte sniffing is safe on tiny/empty logs.
fn read_up_to<R: Read>(r: &mut R, buf: &mut [u8]) -> io::Result<usize> {
    let mut filled = 0;
    while filled < buf.len() {
        match r.read(&mut buf[filled..]) {
            Ok(0) => break,
            Ok(n) => filled += n,
            Err(ref e) if e.kind() == io::ErrorKind::Interrupted => continue,
            Err(e) => return Err(e),
        }
    }
    Ok(filled)
}

/// Streaming reader: yields the header up front, then events line by line. Transparently
/// decompresses a zstd-compressed log (see [`open_log_reader`]).
pub struct EventLogReader {
    lines: io::Lines<Box<dyn BufRead>>,
}

impl EventLogReader {
    pub fn open(path: impl AsRef<Path>) -> Result<(RunHeader, Self)> {
        let path = path.as_ref();
        let mut lines = open_log_reader(path)?.lines();
        let first = lines
            .next()
            .ok_or_else(|| anyhow!("empty event log {}", path.display()))??;
        let header: RunHeader =
            serde_json::from_str(&first).context("parsing run header (first line)")?;
        Ok((header, EventLogReader { lines }))
    }
}

impl Iterator for EventLogReader {
    type Item = Result<Event>;

    fn next(&mut self) -> Option<Self::Item> {
        loop {
            match self.lines.next()? {
                Ok(line) if line.trim().is_empty() => continue,
                Ok(line) => {
                    return Some(serde_json::from_str(&line).context("parsing event line"));
                }
                Err(e) => return Some(Err(e.into())),
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use rust_decimal_macros::dec;

    fn ts() -> DateTime<Utc> {
        DateTime::from_timestamp(1_700_000_000, 0).unwrap()
    }

    #[test]
    fn event_json_roundtrip() {
        let ev = Event {
            seq: 7,
            local_recv_ts: ts(),
            market: "BTC".into(),
            kind: EventKind::AsterAggTrade {
                price: dec!(12345.6),
                qty: dec!(0.01),
                buyer_is_maker: true,
                exch_ts: ts(),
            },
        };
        let s = serde_json::to_string(&ev).unwrap();
        // Decimal must serialize as a string, not a float.
        assert!(s.contains("\"12345.6\""));
        let back: Event = serde_json::from_str(&s).unwrap();
        assert_eq!(back.seq, 7);
        match back.kind {
            EventKind::AsterAggTrade { price, buyer_is_maker, .. } => {
                assert_eq!(price, dec!(12345.6));
                assert!(buyer_is_maker);
            }
            _ => panic!("wrong kind"),
        }
    }

    #[test]
    fn depth_roundtrip() {
        let ev = Event {
            seq: 1,
            local_recv_ts: ts(),
            market: "ETH".into(),
            kind: EventKind::HlL2Book {
                bids: vec![(dec!(100.0), dec!(2)), (dec!(99.9), dec!(5))],
                asks: vec![(dec!(100.1), dec!(3))],
                exch_ts: ts(),
            },
        };
        let s = serde_json::to_string(&ev).unwrap();
        let back: Event = serde_json::from_str(&s).unwrap();
        match back.kind {
            EventKind::HlL2Book { bids, asks, .. } => {
                assert_eq!(bids.len(), 2);
                assert_eq!(asks[0], (dec!(100.1), dec!(3)));
            }
            _ => panic!("wrong kind"),
        }
    }

    fn tmp_path(ext: &str) -> std::path::PathBuf {
        std::env::temp_dir().join(format!("xemm_log_{}.{ext}", uuid::Uuid::new_v4()))
    }

    /// Write raw lines through a `LogWriter`, then read them back through the
    /// auto-detecting reader; returns the lines the reader produced.
    fn write_then_read(path: &Path, lines: &[&str]) -> Vec<String> {
        {
            let mut w = open_log_writer(path).unwrap();
            for l in lines {
                w.write_all(l.as_bytes()).unwrap();
                w.write_all(b"\n").unwrap();
            }
            w.finish().unwrap();
        }
        open_log_reader(path).unwrap().lines().map(|l| l.unwrap()).collect()
    }

    #[test]
    fn zstd_log_roundtrips_and_is_smaller() {
        let path = tmp_path("zst");
        let pad = "x".repeat(2000); // repetitive payload so compression clearly shrinks it
        let lines: Vec<String> =
            (0..50).map(|i| format!("{{\"n\":{i},\"pad\":\"{pad}\"}}")).collect();
        let refs: Vec<&str> = lines.iter().map(|s| s.as_str()).collect();
        let got = write_then_read(&path, &refs);
        assert_eq!(got, lines, "decompressed lines must match what was written");
        let on_disk = std::fs::metadata(&path).unwrap().len();
        let raw: u64 = lines.iter().map(|l| l.len() as u64 + 1).sum();
        assert!(on_disk < raw / 2, "zstd file {on_disk}B not < half of raw {raw}B");
        std::fs::remove_file(&path).ok();
    }

    #[test]
    fn plain_log_roundtrips_and_is_uncompressed() {
        let path = tmp_path("jsonl");
        let lines = ["{\"a\":1}", "{\"b\":2}", "{\"c\":3}"];
        let got = write_then_read(&path, &lines);
        assert_eq!(got, lines);
        // A `.jsonl` path must stay plain text (not start with the zstd magic).
        let bytes = std::fs::read(&path).unwrap();
        assert_ne!(&bytes[..4], &ZSTD_MAGIC, "plain log must not be zstd");
        std::fs::remove_file(&path).ok();
    }

    #[test]
    fn detects_zstd_by_content_not_extension() {
        // Build a real zstd file, copy it under a non-`.zst` name, and confirm the reader
        // still decompresses it — detection is by magic bytes, not the extension.
        let zst = tmp_path("zst");
        write_then_read(&zst, &["{\"hdr\":true}"]);
        let mislabeled = tmp_path("jsonl");
        std::fs::copy(&zst, &mislabeled).unwrap();
        let got: Vec<String> =
            open_log_reader(&mislabeled).unwrap().lines().map(|l| l.unwrap()).collect();
        assert_eq!(got, vec!["{\"hdr\":true}".to_string()]);
        std::fs::remove_file(&zst).ok();
        std::fs::remove_file(&mislabeled).ok();
    }
}

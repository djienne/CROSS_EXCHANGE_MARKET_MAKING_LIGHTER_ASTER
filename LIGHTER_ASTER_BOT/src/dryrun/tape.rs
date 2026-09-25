//! The market-data tape: one market's raw public Aster and Lighter feeds, the dry run's input,
//! recorded by `record` for backtests. One file per UTC day, `<dir>/<MARKET>/<YYYY-MM-DD>.tape.zst`,
//! of tab-separated lines `<arrival µs since the epoch>\t<kind>\t<payload>`:
//!
//! | kind | payload |
//! |---|---|
//! | `A` | an Aster combined-stream frame, raw: `depth20@100ms` (20 levels), `bookTicker`, `aggTrade` |
//! | `A-` | the Aster connection ended, or a reconnect failed (empty); later frames follow a reconnect |
//! | `L` | a Lighter frame, raw: `order_book` (snapshot, then deltas: the whole book), `trade`, `market_stats` (funding) |
//! | `L-` | the Lighter connection ended, or a reconnect failed (empty) |
//! | `F` | an Aster `premiumIndex` response (funding), every minute |
//! | `X`, `O` | the Aster `exchangeInfo` and Lighter `orderBooks` responses (filters), hourly |
//!
//! A file is concatenated zstd frames, one per flush (every 10 s), so a kill loses at most the
//! last 10 s and `zstd -dc` reads any file. Recording never waits: the network tasks hand each
//! line to a writer thread, and drop (and count) it if the writer is a whole queue behind.

use std::fs::{File, OpenOptions};
use std::io::{BufRead, BufReader, Write};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::mpsc::{sync_channel, Receiver, RecvTimeoutError, SyncSender, TrySendError};
use std::sync::Arc;
use std::time::{Duration, Instant};

use anyhow::{anyhow, Context, Result};
use tokio_util::sync::CancellationToken;

use super::clock::wall_us;
use super::feed::{self, Wire};
use crate::lighter::messages::OrderBooksResponse;
use crate::lighter::ws::{stream_url, subscribe_loop, SubscribeOptions};

/// Lines the writer may fall behind by: ~6 minutes of HYPE at ~60 lines/s.
const QUEUE: usize = 20_000;
const FLUSH: Duration = Duration::from_secs(10);
/// A flush also happens once this much is buffered.
const MAX_BUFFER: usize = 8 << 20;
const LEVEL: i32 = 9;
const SPECS_EVERY: Duration = Duration::from_secs(3_600);

enum Msg {
    Line(i64, &'static str, String),
    Stop,
}

/// A handle on the writer; clones share it.
#[derive(Clone)]
pub struct Tape {
    tx: SyncSender<Msg>,
    dropped: Arc<AtomicU64>,
}

impl Tape {
    /// Starts the writer thread for `<dir>/<market>/`, appending to the day's file.
    pub fn open(dir: &Path, market: &str) -> Result<(Tape, std::thread::JoinHandle<Result<()>>)> {
        let dir = dir.join(market);
        std::fs::create_dir_all(&dir).with_context(|| format!("creating {}", dir.display()))?;
        let (tx, rx) = sync_channel(QUEUE);
        let writer = std::thread::Builder::new().name("tape".into()).spawn(move || write(&dir, rx))?;
        Ok((Tape { tx, dropped: Arc::default() }, writer))
    }

    /// Records `payload` as arriving now.
    pub fn record(&self, kind: &'static str, payload: &str) {
        self.record_at(wall_us(), kind, payload);
    }

    fn record_at(&self, arrival_us: i64, kind: &'static str, payload: &str) {
        if let Err(TrySendError::Full(_)) = self.tx.try_send(Msg::Line(arrival_us, kind, payload.to_string())) {
            let dropped = self.dropped.fetch_add(1, Ordering::Relaxed) + 1;
            if dropped.is_power_of_two() {
                tracing::warn!("tape: the writer is behind; {dropped} lines dropped so far");
            }
        }
    }

    /// Writes what is queued, then stops the writer.
    pub fn stop(&self) {
        let _ = self.tx.send(Msg::Stop);
    }
}

fn utc_day(arrival_us: i64) -> String {
    chrono::DateTime::from_timestamp_micros(arrival_us).map_or_else(|| "invalid-time".into(), |t| t.format("%Y-%m-%d").to_string())
}

fn write(dir: &Path, rx: Receiver<Msg>) -> Result<()> {
    let mut buffer = Vec::new();
    let mut day: Option<String> = None;
    let mut due = Instant::now() + FLUSH;
    loop {
        match rx.recv_timeout(due.saturating_duration_since(Instant::now())) {
            Ok(Msg::Line(arrival_us, kind, payload)) => {
                let line_day = utc_day(arrival_us);
                if day.as_deref() != Some(line_day.as_str()) {
                    flush(dir, day.as_deref(), &mut buffer)?;
                    day = Some(line_day);
                }
                write!(buffer, "{arrival_us}\t{kind}\t")?;
                // The venues send single-line JSON; a stray newline would split the record.
                buffer.extend(payload.bytes().map(|b| if b == b'\n' || b == b'\r' { b' ' } else { b }));
                buffer.push(b'\n');
                if buffer.len() >= MAX_BUFFER {
                    flush(dir, day.as_deref(), &mut buffer)?;
                }
            }
            Ok(Msg::Stop) | Err(RecvTimeoutError::Disconnected) => return flush(dir, day.as_deref(), &mut buffer),
            Err(RecvTimeoutError::Timeout) => {}
        }
        if Instant::now() >= due {
            flush(dir, day.as_deref(), &mut buffer)?;
            due = Instant::now() + FLUSH;
        }
    }
}

/// Appends `buffer` to `day`'s file as one zstd frame.
fn flush(dir: &Path, day: Option<&str>, buffer: &mut Vec<u8>) -> Result<()> {
    let Some(day) = day.filter(|_| !buffer.is_empty()) else { return Ok(()) };
    let path = dir.join(format!("{day}.tape.zst"));
    let frame = zstd::bulk::compress(buffer, LEVEL)?;
    let mut file = OpenOptions::new().create(true).append(true).open(&path).with_context(|| format!("opening {}", path.display()))?;
    file.write_all(&frame)?;
    file.sync_data()?;
    buffer.clear();
    Ok(())
}

/// A tape file's lines as (arrival µs, kind, payload). A frame cut short by a kill ends it.
pub fn read(path: &Path) -> Result<Vec<(i64, String, String)>> {
    let mut lines = Vec::new();
    for line in BufReader::new(zstd::stream::read::Decoder::new(File::open(path)?)?).lines() {
        let Ok(line) = line else { break };
        let mut parts = line.splitn(3, '\t');
        let (Some(at), Some(kind), Some(payload)) = (parts.next(), parts.next(), parts.next()) else {
            return Err(anyhow!("{}: malformed line {}", path.display(), lines.len() + 1));
        };
        lines.push((at.parse()?, kind.to_string(), payload.to_string()));
    }
    Ok(lines)
}

/// `record`: tapes `market`'s public feeds from the venues in `config` into `dir` until a stop
/// signal. It needs no credentials and sends no orders.
pub async fn record(config: &Path, market: &str, dir: PathBuf, stop: CancellationToken) -> Result<()> {
    let cfg = crate::controller::BotConfig::load(config)?;
    let (taker_markets, _) = cfg.select(&market.to_ascii_uppercase())?;
    let market = &taker_markets[0];
    let aster_base = cfg.maker.live.aster.base_url.trim_end_matches('/').to_string();
    let lighter_base = cfg.maker.live.hyperliquid.base_url.trim_end_matches('/').to_string();
    let symbol = market.aster_symbol.to_ascii_uppercase();
    let http = reqwest::Client::builder().timeout(Duration::from_secs(20)).build()?;
    let get = move |url: String| {
        let request = http.get(url);
        async move { anyhow::Ok(request.send().await?.error_for_status()?.text().await?) }
    };
    let order_books = get(format!("{lighter_base}/api/v1/orderBooks")).await.context("fetching Lighter orderBooks")?;
    let index = serde_json::from_str::<OrderBooksResponse>(&order_books)?
        .order_books
        .into_iter()
        .find(|b| b.symbol.eq_ignore_ascii_case(&market.lighter_symbol))
        .with_context(|| format!("Lighter orderBooks has no {}", market.lighter_symbol))?
        .market_id;

    let (tape, writer) = Tape::open(&dir, &market.id().0)?;
    let specs = [("X", format!("{aster_base}/fapi/v3/exchangeInfo")), ("O", format!("{lighter_base}/api/v1/orderBooks"))];
    tokio::spawn({
        let tape = tape.clone();
        async move {
            let mut tick = tokio::time::interval(SPECS_EVERY);
            loop {
                tick.tick().await;
                for &(kind, ref url) in &specs {
                    match get(url.clone()).await {
                        Ok(body) => tape.record(kind, &body),
                        Err(e) => tracing::warn!("recorder: {url}: {e:#}"),
                    }
                }
            }
        }
    });
    let aster_url = feed::aster_url(&crate::connectors::aster::ws_root(&aster_base), &[symbol.clone()]);
    tokio::spawn({
        let tape = tape.clone();
        async move {
            feed::aster_stream(aster_url, "recorder Aster", |wire| match wire {
                Wire::Text(text) => tape.record("A", text),
                Wire::Closed => tape.record("A-", ""),
            })
            .await
        }
    });
    let channels = ["order_book", "trade", "market_stats"].map(|c| format!("{c}/{index}")).to_vec();
    let mut opts = SubscribeOptions::new(&stream_url(&lighter_base), "recorder Lighter", channels);
    opts.reconnect_base = 0.5;
    opts.reconnect_max = feed::UPSTREAM_BACKOFF_MAX.as_secs_f64();
    let (on_frame, on_close) = (tape.clone(), tape.clone());
    tokio::spawn(subscribe_loop(opts, None, move |frame| on_frame.record("L", frame.raw), move || on_close.record("L-", "")));
    let on_body = tape.clone();
    tokio::spawn(async move { feed::aster_premium_poll(aster_base, vec![symbol], "recorder premiumIndex", |body| on_body.record("F", body)).await });
    tracing::info!("recording {} (Aster {}, Lighter market {index}) into {}", market.id().0, market.aster_symbol, dir.display());

    // A failed writer (a full disk) stops the recorder too, so the restart policy shows it.
    let watch = {
        let stop = stop.clone();
        tokio::task::spawn_blocking(move || {
            let result = writer.join();
            stop.cancel();
            result
        })
    };
    stop.cancelled().await;
    tape.stop();
    watch.await?.map_err(|_| anyhow!("the tape writer panicked"))?
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn lines_land_in_their_utc_day_and_a_restart_appends() {
        let dir = crate::dryrun::tests::temp_dir("tape");
        let day = 1_790_294_400_000_000; // 2026-09-25T00:00:00Z
        let (tape, writer) = Tape::open(&dir, "HYPE").unwrap();
        tape.record_at(day - 1, "A", r#"{"stream":"x"}"#);
        tape.record_at(day, "L", "{\"a\":\n1}");
        tape.stop();
        writer.join().unwrap().unwrap();
        // A restart appends to the day's file as a further zstd frame.
        let (tape, writer) = Tape::open(&dir, "HYPE").unwrap();
        tape.record_at(day + 1, "L-", "");
        tape.stop();
        writer.join().unwrap().unwrap();

        let files = dir.join("HYPE");
        assert_eq!(read(&files.join("2026-09-24.tape.zst")).unwrap(), vec![(day - 1, "A".into(), r#"{"stream":"x"}"#.into())]);
        assert_eq!(
            read(&files.join("2026-09-25.tape.zst")).unwrap(),
            vec![(day, "L".into(), "{\"a\": 1}".into()), (day + 1, "L-".into(), String::new())]
        );
    }
}

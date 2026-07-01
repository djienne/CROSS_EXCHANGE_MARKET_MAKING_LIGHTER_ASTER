//! Append-only telemetry journal (plan §1.D / §14.9). The hot path must never block on
//! disk, so the strategy/execution threads only ever *enqueue* a record into a BOUNDED
//! channel (a non-blocking `try_send`); a dedicated writer task drains it to JSONL. If the
//! process dies we recover from exchange state, not from this log — it is an audit trail,
//! not the source of truth. The bound (vs an unbounded channel) is the safety property: a
//! stalled writer (disk full / NFS hang) drops the NEWEST record at enqueue time (`try_send`
//! fails; the already-queued backlog is preserved) instead of growing without limit and
//! OOM-killing the process — which would skip the orderly shutdown that cancels resting
//! orders on both venues. Drops are counted and warned so an incident-time gap in the
//! audit trail is visible rather than silent.

use std::io::Write;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{SystemTime, UNIX_EPOCH};

use serde::Serialize;
use tokio::sync::mpsc::{channel, Receiver, Sender};
use tracing::warn;

/// Bounded journal capacity (~records). Deep enough to absorb a long quoting burst, capped so a
/// wedged writer can never exhaust memory. At ~100 B/record this is a few MB worst case.
const JOURNAL_CAP: usize = 65_536;

/// One journal record: a monotonic stamp, a wall-clock stamp, a kind tag, and a free-form
/// JSON detail. Kept schema-light so any plane can log without a central enum churn.
///
/// `ts_ms` (epoch milliseconds, stamped at enqueue) is what downstream PnL/history tooling
/// (`combined_pnl.py`, `trade_history.py`) uses to window trades — `mono_ns` alone is
/// process-relative and useless across restarts.
#[derive(Debug, Clone, Serialize)]
pub struct JournalRecord {
    pub mono_ns: i64,
    pub ts_ms: i64,
    pub kind: &'static str,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub market: Option<String>,
    pub detail: serde_json::Value,
}

/// Lifetime count of records dropped at enqueue (full/closed channel). Static because the
/// journal handle is cloned everywhere and drops must aggregate across all planes.
static DROPPED_RECORDS: AtomicU64 = AtomicU64::new(0);

/// A cloneable journal handle. Cloning shares the same channel, so every plane logs into one
/// stream. A `null()` handle silently drops records (paper/tests).
#[derive(Clone)]
pub struct Journal {
    tx: Option<Sender<JournalRecord>>,
}

impl Journal {
    /// A no-op journal that drops every record (default for tests / when journaling is off).
    pub fn null() -> Self {
        Journal { tx: None }
    }

    /// Create a journal plus its receiver. The caller spawns [`run_journal_writer`] with the
    /// receiver on a cold task.
    pub fn channel() -> (Self, Receiver<JournalRecord>) {
        let (tx, rx) = channel(JOURNAL_CAP);
        (Journal { tx: Some(tx) }, rx)
    }

    /// Enqueue a record. NON-BLOCKING and infallible from the caller's view: `try_send` never
    /// awaits, and a full (writer wedged) or closed (writer gone) channel drops this record
    /// rather than ever stalling the hot path or growing unbounded. The journal is an audit
    /// trail, so a dropped record under extreme backpressure is acceptable — an OOM is not —
    /// but drops are counted and warned (rate-limited) so the gap is visible.
    #[inline]
    pub fn record(&self, mono_ns: i64, kind: &'static str, market: Option<String>, detail: serde_json::Value) {
        if let Some(tx) = &self.tx {
            let ts_ms = SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .map(|d| d.as_millis() as i64)
                .unwrap_or(0);
            if tx.try_send(JournalRecord { mono_ns, ts_ms, kind, market, detail }).is_err() {
                let dropped = DROPPED_RECORDS.fetch_add(1, Ordering::Relaxed) + 1;
                // Warn on the first drop and then once per 1024 so a wedged writer during an
                // incident is loud without the warn itself becoming a flood.
                if dropped == 1 || dropped % 1024 == 0 {
                    warn!("journal backpressure: {dropped} record(s) dropped at enqueue (writer wedged or stopped)");
                }
            }
        }
    }
}

/// Records the operator needs to see the instant they happen — the money path (`fill`,
/// `net`) and the exceptional self-heal / position-touching events (`freeze`, `unfreeze`,
/// `flatten_pending`, `recover_hedge`, `recover_flatten`) — are flushed to disk immediately
/// so a live `tail -f` of the journal surfaces them in real time. The high-frequency quoting
/// churn (`place`/`replace`/`cancel`) stays batched (up to 64) so a fast requote loop never
/// fsync-thrashes the writer. Cold path only; the hot path still just enqueues.
#[inline]
fn flush_immediately(kind: &str) -> bool {
    matches!(
        kind,
        "fill"
            | "hedge_fill"
            | "net"
            | "fill_hedge_context"
            | "hedge_retry_context"
            | "freeze"
            | "unfreeze"
            | "flatten_pending"
            | "recover_hedge"
            | "recover_flatten"
            | "circuit_trip"
    )
}

/// Drain `rx` into `writer` as one JSON object per line until the channel closes. Runs on a
/// cold task; never touches the hot path. Significant records flush at once (see
/// [`flush_immediately`]); the place/replace/cancel churn is batched.
pub async fn run_journal_writer<W: Write>(mut rx: Receiver<JournalRecord>, mut writer: W) {
    let mut since_flush = 0u32;
    while let Some(rec) = rx.recv().await {
        match serde_json::to_string(&rec) {
            Ok(line) => {
                if writeln!(writer, "{line}").is_err() {
                    warn!("journal write failed; dropping further records");
                    break;
                }
                since_flush += 1;
                if flush_immediately(rec.kind) || since_flush >= 64 {
                    writer.flush().ok();
                    since_flush = 0;
                }
            }
            Err(e) => warn!("journal serialize failed: {e}"),
        }
    }
    writer.flush().ok();
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn null_journal_drops_without_panicking() {
        let j = Journal::null();
        j.record(1, "place", Some("BTC".into()), json!({"px": 100}));
        // nothing to assert beyond "doesn't panic / doesn't block".
    }

    #[tokio::test]
    async fn records_are_written_as_jsonl() {
        let (j, rx) = Journal::channel();
        j.record(1, "place", Some("BTC".into()), json!({"px": 100}));
        j.record(2, "cancel", None, json!({"reason": "stale"}));
        drop(j); // close the channel so the writer loop ends
        let mut buf: Vec<u8> = Vec::new();
        run_journal_writer(rx, &mut buf).await;
        let text = String::from_utf8(buf).unwrap();
        let lines: Vec<&str> = text.lines().collect();
        assert_eq!(lines.len(), 2);
        let first: serde_json::Value = serde_json::from_str(lines[0]).unwrap();
        assert_eq!(first["kind"], "place");
        assert_eq!(first["market"], "BTC");
        assert_eq!(first["detail"]["px"], 100);
        // Every record must carry a wall-clock stamp for PnL/history windowing.
        assert!(first["ts_ms"].as_i64().unwrap() > 1_500_000_000_000);
        // the second record omits market (skip_serializing_if None)
        let second: serde_json::Value = serde_json::from_str(lines[1]).unwrap();
        assert!(second.get("market").is_none());
    }

    #[test]
    fn money_path_kinds_flush_immediately_quoting_churn_batches() {
        // Money-path + exceptional records must hit disk at once so a live `tail` shows them.
        for k in [
            "fill",
            "hedge_fill",
            "net",
            "fill_hedge_context",
            "hedge_retry_context",
            "freeze",
            "unfreeze",
            "flatten_pending",
            "recover_hedge",
            "recover_flatten",
        ] {
            assert!(flush_immediately(k), "{k} must flush immediately for live tail visibility");
        }
        // High-frequency quoting churn stays batched to avoid fsync thrash.
        for k in ["place", "replace", "cancel"] {
            assert!(!flush_immediately(k), "{k} must stay batched");
        }
    }
}

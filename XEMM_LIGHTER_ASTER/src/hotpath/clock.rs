//! Monotonic clock for hot-path staleness stamps.
//!
//! Connection/book liveness must be immune to wall-clock jumps (an NTP step or a
//! leap-second smear): a *backward* `Utc::now()` jump would make `age_ms` /
//! `book_age_ms` negative and let a dead feed read as fresh, suppressing the
//! watchdog's reconnect and holding the trading gate open over stale data. So the
//! staleness atomics (`VenueBook::last_msg_ns` / `last_book_ns`) and the watchdog
//! scan are stamped from a process-start monotonic `Instant`, NOT `Utc::now()`.
//!
//! This is a side-channel for liveness only. It never touches the recorder's
//! `local_recv_ts` (the tape's deterministic *wall-clock* anchor, intentionally
//! still `Utc::now()`), so replay stays a pure function of (tape, code).

use std::sync::OnceLock;
use std::time::Instant;

/// Nanoseconds since a fixed process-start epoch — monotonic and jump-immune.
/// Only differences between two `mono_now_ns()` values are meaningful (it is not a
/// wall-clock time). `.max(1)` keeps a genuine stamp distinct from the `last == 0`
/// "never stamped" sentinel that `age_ms`/`book_age_ms` rely on.
#[inline]
pub(crate) fn mono_now_ns() -> i64 {
    static EPOCH: OnceLock<Instant> = OnceLock::new();
    let ns = EPOCH.get_or_init(Instant::now).elapsed().as_nanos();
    i64::try_from(ns).unwrap_or(i64::MAX).max(1)
}

//! # Hot path — the lock-free real-time substrate (live-bot foundation)
//!
//! This module is the latency-critical, real-time side of the system and the
//! foundation a future live trading bot plugs into. It is **strictly separated**
//! from the deterministic cold path (`record`, `replay`, `report`, `sim`).
//!
//! ## Determinism contract
//!
//! Each venue WS reader fans out to **two independent consumers**:
//!
//! 1. **Canonical (cold / determinism):** the existing
//!    `connectors::EventSink` → single-threaded recorder dequeue → monotonic
//!    `(local_recv_ts, seq)` stamp → JSONL. `record` uses the lossless sink; livebot
//!    uses a bounded lossy sink so cold recording cannot backpressure or OOM the hot path.
//!    This is the *only* thing `replay` ever sees, and it is byte-for-byte unchanged by
//!    anything here.
//! 2. **Hot (live / strategy):** a [`VenueBook`] — an `ArcSwapOption<OrderBook>`
//!    latest-book cell plus an atomic liveness stamp — published directly by ingest,
//!    independently of the cold sink, and read wait-free by the [`watchdog`] and strategy
//!    loop. Integer hot snapshots may publish before the slower raw Decimal book, but only
//!    for cancel-only prechecks until the raw book catches up.
//!
//! The hot substrate is **write-only from the reader and non-persistent**: nothing
//! here flows into `Event`, JSONL, SQLite, or `SimEngine`. There is no data path
//! from the lock-free world into the deterministic world, so a given recorded log
//! always replays identically. The whole module is feature-gated (`hotpath`,
//! default-on) so the deterministic core builds with zero lock-free code under
//! `--no-default-features`.
//!
//! ## Separation rules (hold these when changing ANY hot-path code)
//!
//! The two latency-critical paths are (1) market-data tick → quote decision →
//! cancel/place dispatch and (2) maker fill → hedge dispatch. On those paths:
//!
//! * **No locks** shared with cold tasks — reads are ArcSwap loads / atomics only.
//! * **No syscalls or I/O** — journal/telemetry is a bounded `try_send` enqueue; the
//!   writer drains on a cold task.
//! * **No unbounded sends** — every channel touched is bounded, `try_send`-only, with
//!   an explicit fail-closed action (freeze) on dispatch failure of a safety command.
//! * **No REST / signing** — signing and wire sends live in the exec workers; the
//!   strategy loop only enqueues commands.
//! * Nonce and tx-socket access stay strictly serialized in the hedge worker's send
//!   phase; only fill-WAITING may run concurrently (see `exec::hyperliquid`).
//!
//! ## Pieces
//!
//! - [`book_cell`] — `VenueBook`: the lock-free latest-book cell + liveness stamp.
//! - [`registry`] — `VenueRegistry`: the shared map of cells, keyed (market, venue).
//! - [`watchdog`] — `TradingGate` + `ReconnectHandle` + the staleness scan that
//!   forces reconnects on silent streams and signals "stop trading" lock-free.
//! - [`book_check`] — the slow REST cross-check that confirms each WS book matches a
//!   REST snapshot; on sustained divergence it flags the cell + resets the socket.
//! - [`exec`] — the `Execution` trait seam (place/cancel/replace maker, market
//!   hedge) and its unimplemented `LiveExecution` stub for the future real bot.

pub mod book_cell;
pub mod book_check;
pub mod clock;
pub mod dirty;
pub mod exec;
pub mod registry;
pub mod venue_thread;
pub mod watchdog;

pub use book_cell::{VenueBook, VenueTag};
pub use book_check::{run_book_check, BookCheckParams, BookCheckTarget};
pub use exec::{ExecError, Execution, LiveExecution, MakerOrder, OrderHandle};
pub use registry::VenueRegistry;
pub use venue_thread::{spawn_venue_thread, maybe_pin_core};
pub use watchdog::{run_watchdog, scan_once, ReconnectHandle, TradingGate};

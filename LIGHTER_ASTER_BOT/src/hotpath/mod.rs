//! Market-data publication and strategy wakeups, separated from recording and execution.
//!
//! Ingest publishes immutable books plus monotonic/source-age metadata. Strategy
//! decisions read snapshots and enqueue bounded commands without filesystem,
//! REST or shared cold-task locks. Venue signing and ordered writes live in
//! livebot::exec; private fills have priority over repricing and diagnostics.
//! The recorder owns its independent event stream and simulation state. A slow
//! recorder cannot block live publication; recording gaps are explicit.

pub mod book_cell;
pub mod book_check;
pub mod clock;
pub mod dirty;
pub mod registry;
pub mod venue_thread;
pub mod watchdog;

pub use book_cell::{VenueBook, VenueTag};
pub use book_check::{run_book_check, BookCheckParams, BookCheckTarget};
pub use registry::VenueRegistry;
pub use venue_thread::{spawn_venue_thread, maybe_pin_core};
pub use watchdog::{run_watchdog, scan_once, ReconnectHandle, TradingGate};

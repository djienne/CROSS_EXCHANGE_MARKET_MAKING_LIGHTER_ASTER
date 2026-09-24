//! Market-data publication and strategy wakeups, separated from execution.
//!
//! Ingest publishes immutable books plus monotonic/source-age metadata. Strategy
//! decisions read snapshots and enqueue bounded commands without filesystem,
//! REST or shared cold-task locks. Venue signing and ordered writes live in
//! livebot::exec; private fills have priority over repricing and diagnostics.

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
pub use watchdog::{run_watchdog, ReconnectHandle, TradingGate};

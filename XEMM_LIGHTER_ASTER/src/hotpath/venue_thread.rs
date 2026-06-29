//! Per-venue dedicated ingest thread. Each venue's WS reader gets its own OS thread
//! hosting a single-threaded tokio runtime, so Aster ingest latency/jitter is
//! isolated from Hyperliquid's (and from the recorder/sim) instead of sharing the
//! default multi-thread pool. The reader still feeds BOTH the canonical recorder
//! channel (`tx`) AND the lock-free [`VenueBook`] (via the connector `Tap`).

use std::sync::Arc;
use std::thread::{self, JoinHandle};

use tokio::sync::Notify;
use tokio_util::sync::CancellationToken;

use crate::connectors::{aster, lighter, BookTap, EventSink, Tap};
use crate::livebot::scale::HotQtyScale;
use crate::types::MarketId;

use super::book_cell::{VenueBook, VenueTag};

/// Spawn one dedicated OS thread running the venue's reconnecting WS reader. The
/// thread exits (so `join()` returns) when `shutdown` is cancelled.
///
/// `core_hint` optionally pins the thread to a CPU core (index taken modulo the
/// available cores) — only honored under the `core-pin` feature (default off); see
/// [`maybe_pin_core`]. Wired so a future live bot flips the feature on with no code
/// change.
#[allow(clippy::too_many_arguments)]
pub fn spawn_venue_thread(
    venue: VenueTag,
    symbol: String,
    market: MarketId,
    tx: EventSink,
    cell: Arc<VenueBook>,
    reconnect: Arc<Notify>,
    shutdown: CancellationToken,
    core_hint: Option<usize>,
    scale: Option<crate::livebot::scale::MarketScale>,
) -> JoinHandle<()> {
    let name = format!("ingest-{}-{}", venue.as_str(), market.0);
    thread::Builder::new()
        .name(name)
        .spawn(move || {
            maybe_pin_core(core_hint);
            let rt = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .expect("current-thread ingest runtime");
            let book: Arc<dyn BookTap> = cell;
            let qty_scale = match venue {
                VenueTag::Aster => HotQtyScale::Aster,
                VenueTag::Hyperliquid => HotQtyScale::Hyperliquid,
            };
            let tap = Tap { book: Some(book), reconnect: Some(reconnect), scale, qty_scale };
            rt.block_on(async move {
                tokio::select! {
                    // The reader loops forever (reconnecting); it only returns if the
                    // task is dropped. The shutdown arm is what ends the thread cleanly.
                    _ = run_reader(venue, symbol, market, tx, tap) => {}
                    _ = shutdown.cancelled() => {}
                }
            });
        })
        .expect("spawn venue ingest thread")
}

async fn run_reader(
    venue: VenueTag,
    symbol: String,
    market: MarketId,
    tx: EventSink,
    tap: Tap,
) {
    match venue {
        VenueTag::Aster => aster::run_with_tap(symbol, market, tx, tap).await,
        VenueTag::Hyperliquid => {
            let (market_id, label) = symbol
                .split_once(':')
                .and_then(|(id, label)| Some((id.parse::<u32>().ok()?, label.to_string())))
                .unwrap_or((0, symbol));
            lighter::run_with_tap(market_id, label, market, tx, tap).await
        }
    }
}

/// Pin the current thread to a core (index modulo available cores) when the
/// `core-pin` feature is on; a no-op otherwise. Default-off: core pinning buys
/// tail-latency determinism that only matters once real orders are placed, and on
/// Windows it can interact poorly with the scheduler. The plumbing is here so the
/// future live bot enables it with a feature flag, not a refactor.
#[cfg(feature = "core-pin")]
fn maybe_pin_core(hint: Option<usize>) {
    if let Some(idx) = hint {
        if let Some(ids) = core_affinity::get_core_ids() {
            if !ids.is_empty() {
                core_affinity::set_for_current(ids[idx % ids.len()]);
            }
        }
    }
}

#[cfg(not(feature = "core-pin"))]
fn maybe_pin_core(_hint: Option<usize>) {}

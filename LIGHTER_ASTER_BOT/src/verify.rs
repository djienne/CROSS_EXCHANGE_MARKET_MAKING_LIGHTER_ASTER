//! `verify-books`: a one-shot confidence check that the websocket-built order books
//! match the venues' REST snapshots. It connects both feeds for a few seconds,
//! captures the latest book per (market, venue) straight off the canonical recorder
//! channel (no hot-path / lock-free machinery needed — so it builds under
//! `--no-default-features`), then fetches REST depth for each and prints a
//! side-by-side top-of-book comparison. Purely diagnostic; it places nothing.

use std::collections::HashMap;

use anyhow::Result;
use rust_decimal::Decimal;
use tokio::sync::mpsc;
use tokio::time::{Duration, Instant};
use tracing::info;

use crate::book::OrderBook;
use crate::config::{Config, MarketCfg};
use crate::connectors::rest_book::{self, BookComparison};
use crate::connectors::{aster, lighter, rest_specs};
use crate::events::EventKind;
use crate::types::MarketId;

/// Mid divergence (bps) under which a WS book is considered to match REST for this
/// diagnostic. Looser than the live reconciler's threshold because the WS snapshot
/// and the REST fetch are taken a moment apart (small timing skew is expected).
const VERDICT_TOLERANCE_BPS: i64 = 25;

/// Notional (USD) of the VWAP probe used to compare hedge-depth, not just top-of-book.
/// Sized to span several levels on a liquid book; a thin book that cannot fill it simply
/// skips the VWAP comparison (reported as `-`).
const VWAP_PROBE_NOTIONAL: i64 = 2000;

pub async fn run(cfg: &Config, markets: Vec<MarketCfg>, secs: u64) -> Result<()> {
    if markets.is_empty() {
        anyhow::bail!("no markets selected for verify-books");
    }
    let specs = rest_specs::build_market_specs_with_bases(
        &markets,
        cfg.partials.hyperliquid_min_notional,
        &cfg.live.aster.base_url,
        &cfg.live.hyperliquid.base_url,
    )
    .await?;

    let (tx, mut rx) = mpsc::unbounded_channel::<(MarketId, EventKind)>();
    let mut tasks = Vec::new();
    for s in &specs {
        let id = s.market_id.clone();
        tasks.push(tokio::spawn(aster::run(s.aster_symbol.to_lowercase(), id.clone(), tx.clone())));
        tasks.push(tokio::spawn(lighter::run(s.lighter_market_id, s.hl_coin.clone(), id.clone(), tx.clone())));
    }
    drop(tx);

    info!("verify-books: collecting websocket books for {secs}s across {} market(s)...", markets.len());

    // Keep the latest WS book per (market, venue) over the collection window.
    let mut aster_books: HashMap<MarketId, OrderBook> = HashMap::new();
    let mut hl_books: HashMap<MarketId, OrderBook> = HashMap::new();
    let deadline = Instant::now() + Duration::from_secs(secs);
    loop {
        tokio::select! {
            _ = tokio::time::sleep_until(deadline) => break,
            msg = rx.recv() => {
                let Some((market, kind)) = msg else { break };
                let now = chrono::Utc::now();
                match kind {
                    EventKind::AsterDepth { bids, asks, exch_ts } => {
                        aster_books.insert(market, OrderBook::from_levels(bids, asks, exch_ts, now));
                    }
                    EventKind::HlL2Book { bids, asks, exch_ts } => {
                        hl_books.insert(market, OrderBook::from_levels(bids, asks, exch_ts, now));
                    }
                    _ => {}
                }
            }
        }
    }
    for t in tasks {
        t.abort();
    }

    // Fetch REST snapshots and compare each venue's WS book against its OWN REST book
    // (never cross-venue — the two venues legitimately differ; that is the arb).
    let client = rest_book::client()?;
    println!(
        "\n{:<6} {:<5} {:>14} {:>14} {:>14} {:>14} {:>9} {:>9}  verdict",
        "market", "venue", "ws_bid", "ws_ask", "rest_bid", "rest_ask", "mid_bps", "vwap_bps"
    );
    let mut all_ok = true;
    for s in &specs {
        let id = s.market_id.clone();
        match aster_books.get(&id) {
            Some(ws) => match rest_book::fetch_aster_book_from_base(
                &client,
                &cfg.live.aster.base_url,
                &s.aster_symbol.to_uppercase(),
                20,
            )
            .await
            {
                Ok(rest) => all_ok &= print_row(&id, "aster", ws, &rest),
                Err(e) => {
                    println!("{:<6} {:<5} REST fetch failed: {e:#}", id.0, "aster");
                    all_ok = false;
                }
            },
            None => {
                println!("{:<6} {:<5} no websocket book captured in {secs}s", id.0, "aster");
                all_ok = false;
            }
        }
        match hl_books.get(&id) {
            Some(ws) => match rest_book::fetch_lighter_book_from_base(
                &client,
                &cfg.live.hyperliquid.base_url,
                s.lighter_market_id,
                20,
            )
            .await
            {
                Ok(rest) => all_ok &= print_row(&id, "lighter", ws, &rest),
                Err(e) => {
                    println!("{:<6} {:<5} REST fetch failed: {e:#}", id.0, "lighter");
                    all_ok = false;
                }
            },
            None => {
                println!("{:<6} {:<5} no websocket book captured in {secs}s", id.0, "lighter");
                all_ok = false;
            }
        }
    }
    println!();
    if all_ok {
        println!(
            "verify-books: all websocket books agree with REST within {VERDICT_TOLERANCE_BPS} bps. \
             Books are being built correctly."
        );
    } else {
        println!(
            "verify-books: some rows need a look (DIVERGENT/CROSSED or no book). A few bps of mid \
             difference is normal timing skew; large or crossed differences are not."
        );
    }
    Ok(())
}

fn print_row(market: &MarketId, venue: &str, ws: &OrderBook, rest: &OrderBook) -> bool {
    // Probe size for the deep-book VWAP check: a fixed notional converted at the mid.
    let size = ws
        .mid()
        .or_else(|| rest.mid())
        .filter(|m| *m > Decimal::ZERO)
        .map(|m| Decimal::from(VWAP_PROBE_NOTIONAL) / m)
        .unwrap_or(Decimal::ZERO);
    let cmp = BookComparison::compute(ws, rest, size);
    let tol = Decimal::from(VERDICT_TOLERANCE_BPS);
    let bps = cmp.mid_diff_bps.map(|d| d.round_dp(2));
    let vbps = cmp.vwap_diff_bps.map(|d| d.round_dp(2));
    let mid_ok = bps.is_some_and(|b| b <= tol);
    let vwap_ok = vbps.is_none_or(|b| b <= tol); // an absent probe (thin book) doesn't fail
    let ok = !cmp.ws_crossed && mid_ok && vwap_ok;
    let verdict = if cmp.ws_crossed {
        "CROSSED"
    } else if bps.is_none() {
        "NO_MID"
    } else if ok {
        "ok"
    } else {
        "DIVERGENT"
    };
    println!(
        "{:<6} {:<5} {:>14} {:>14} {:>14} {:>14} {:>9} {:>9}  {}",
        market.0,
        venue,
        opt(cmp.ws_bid),
        opt(cmp.ws_ask),
        opt(cmp.rest_bid),
        opt(cmp.rest_ask),
        bps.map(|b| b.to_string()).unwrap_or_else(|| "-".to_string()),
        vbps.map(|b| b.to_string()).unwrap_or_else(|| "-".to_string()),
        verdict
    );
    ok
}

fn opt(d: Option<Decimal>) -> String {
    d.map(|x| x.to_string()).unwrap_or_else(|| "-".to_string())
}

//! Paper executor (plan Phase 2). Drives the exact same order/hedge lifecycle as a live
//! venue but fabricates the acknowledgements locally — NO network order I/O. This is what
//! `mode = "paper"` runs, and what the live state
//! machine is validated against before any real execution.
//!
//! It is a pure synchronous processor: command in → events out. The worker task just loops
//! `recv → process → emit`, so the logic is unit-testable without async or sockets.

use rust_decimal::Decimal;

use super::command::{ExecCommand, ExecEvent, HedgeCommand};

/// Stateless paper executor. (State, if ever needed, lives in the strategy's order book.)
#[derive(Debug, Default)]
pub struct PaperExec;

impl PaperExec {
    pub fn new() -> Self {
        PaperExec
    }

    /// Process one Aster-side command, returning the events to publish back to the strategy.
    /// A paper place/cancel/replace acks instantly; bulk cancels are confirmed locally by the
    /// strategy (it owns the slot state), so they emit nothing here.
    pub fn on_exec_command(&self, cmd: ExecCommand) -> Vec<ExecEvent> {
        match cmd {
            ExecCommand::Barrier { completion } => {
                completion.complete(crate::hotpath::clock::mono_now_ns());
                Vec::new()
            }
            ExecCommand::Place { client_id, permit, .. } => {
                if !permit.try_claim(crate::hotpath::clock::mono_now_ns()) {
                    return if permit.is_cancelled() { vec![ExecEvent::PlaceReject { client_id, reason: "not sent: stale or revoked quote".into() }] } else { Vec::new() };
                }
                let venue_order_id = format!("paper-{client_id}");
                vec![ExecEvent::PlaceAck { client_id, venue_order_id }]
            }
            ExecCommand::Cancel { client_id, .. } => {
                vec![ExecEvent::CancelAck { client_id }]
            }
            ExecCommand::Replace { old_client_id, new_client_id, permit, .. } => {
                if !permit.try_claim(crate::hotpath::clock::mono_now_ns()) {
                    return if permit.is_cancelled() { vec![ExecEvent::CancelAck { client_id: old_client_id }, ExecEvent::PlaceReject { client_id: new_client_id, reason: "not sent: stale or revoked quote".into() }] } else { Vec::new() };
                }
                let venue_order_id = format!("paper-{new_client_id}");
                vec![
                    ExecEvent::CancelAck { client_id: old_client_id },
                    ExecEvent::PlaceAck { client_id: new_client_id, venue_order_id },
                ]
            }
            ExecCommand::FlattenAster { intent, .. } => {
                if !intent.admission.try_claim(crate::hotpath::clock::mono_now_ns()) {
                    return if intent.admission.is_cancelled() { vec![ExecEvent::AttemptNotSent { cloid: intent.cloid, reason: "expired or cancelled before send".into() }] } else { Vec::new() };
                }
                vec![ExecEvent::ExecutionProgress { cloid: intent.cloid, cumulative_qty: intent.qty,
                    cumulative_quote_usd: Some(intent.qty * intent.aster_fill_px), cumulative_fee_usd: Some(Decimal::ZERO),
                    terminal: true, venue_order_id: None, event_time_ms: Some(chrono::Utc::now().timestamp_millis()) }]
            }
            // Bulk safety cancels + the dead-man heartbeat have no per-order paper ack.
            ExecCommand::CancelMarket { .. }
            | ExecCommand::CancelAllBot
            | ExecCommand::RefreshDeadman { .. }
            | ExecCommand::Shutdown => Vec::new(),
        }
    }

    /// Process one hedge command. A paper hedge fully fills immediately at the aggressive
    /// price (the optimistic-but-bounded model; slippage was already capped by the caller).
    pub fn on_hedge_command(&self, cmd: HedgeCommand) -> Vec<ExecEvent> {
        match cmd {
            HedgeCommand::Hedge { intent, aggressive_px, .. } => {
                let cloid = intent.cloid;
                if !intent.admission.try_claim(crate::hotpath::clock::mono_now_ns()) {
                    return if intent.admission.is_cancelled() { vec![ExecEvent::AttemptNotSent { cloid, reason: "expired or cancelled before send".into() }] } else { Vec::new() };
                }
                let hl_oid = format!("paper-hedge-{}", cloid.to_hex());
                vec![
                    ExecEvent::HedgeAck { cloid, hl_oid },
                    ExecEvent::ExecutionProgress { cloid, cumulative_qty: intent.qty,
                        cumulative_quote_usd: Some(intent.qty * aggressive_px), cumulative_fee_usd: Some(Decimal::ZERO),
                        terminal: true, venue_order_id: None, event_time_ms: Some(chrono::Utc::now().timestamp_millis()) },
                ]
            }
            HedgeCommand::Shutdown => Vec::new(),
        }
    }
}

/// Sanity helper used by both paper and live workers: clamp an aggressive hedge price to the
/// slippage cap relative to a reference, so a paper fill price is never more aggressive than
/// the live worker would accept. `side` is the HEDGE side (buy pays up, sell concedes down).
pub fn cap_aggressive_px(reference: Decimal, side: crate::types::Side, slippage_bps: Decimal) -> Decimal {
    let factor = slippage_bps / Decimal::from(10_000);
    match side {
        crate::types::Side::Buy => reference * (Decimal::ONE + factor),
        crate::types::Side::Sell => reference * (Decimal::ONE - factor),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::livebot::fills::{AsterFill, HedgeIntent};
    use crate::types::Side;
    use rust_decimal_macros::dec;

    #[test]
    fn place_acks_instantly() {
        let p = PaperExec::new();
        let evs = p.on_exec_command(ExecCommand::Place {
            permit: super::super::command::MakerPermit::for_test(),
            market: "BTC".into(),
            side: Side::Buy,
            price_ticks: 1000,
            qty_lots: 5,
            client_id: "Xs-BTC-B-0".into(),
        });
        assert_eq!(evs.len(), 1);
        match &evs[0] {
            ExecEvent::PlaceAck { client_id, venue_order_id } => {
                assert_eq!(client_id, "Xs-BTC-B-0");
                assert_eq!(venue_order_id, "paper-Xs-BTC-B-0");
            }
            _ => panic!("expected PlaceAck"),
        }
    }

    #[test]
    fn replace_acks_cancel_then_place() {
        let p = PaperExec::new();
        let evs = p.on_exec_command(ExecCommand::Replace {
            permit: super::super::command::MakerPermit::for_test(),
            market: "BTC".into(),
            side: Side::Buy,
            old_client_id: "old".into(),
            old_venue_order_id: Some("paper-old".into()),
            new_client_id: "new".into(),
            price_ticks: 1001,
            qty_lots: 5,
        });
        assert!(matches!(&evs[0], ExecEvent::CancelAck { client_id } if client_id == "old"));
        assert!(matches!(&evs[1], ExecEvent::PlaceAck { client_id, .. } if client_id == "new"));
    }

    #[test]
    fn bulk_cancels_emit_nothing() {
        let p = PaperExec::new();
        assert!(p.on_exec_command(ExecCommand::CancelAllBot).is_empty());
        assert!(p.on_exec_command(ExecCommand::CancelMarket { market: "BTC".into() }).is_empty());
    }

    #[test]
    fn paper_hedge_fully_fills_at_aggressive_px() {
        let p = PaperExec::new();
        let fill = AsterFill {
            market: "BTC".into(),
            aster_side: Side::Buy,
            order_id: "100".into(),
            trade_id: "T1".into(),
            client_id: "Xs-BTC-B-0".into(),
            last_fill_qty: dec!(0.5),
            last_fill_px: dec!(100),
            cum_filled_qty: dec!(0.5),
            event_time_ms: 1,
            reduce_only: false,
            commission: None,
            commission_asset: None,
        };
        let intent = HedgeIntent::from_fill(&fill, 0);
        let cloid = intent.cloid;
        let evs = p.on_hedge_command(HedgeCommand::Hedge {
            intent,
            aggressive_px: dec!(99.9),
            slippage_bps: dec!(5),
            emergency: false,
        });
        assert!(matches!(&evs[0], ExecEvent::HedgeAck { cloid: c, .. } if *c == cloid));
        match &evs[1] {
            ExecEvent::ExecutionProgress { cloid: c, cumulative_qty, cumulative_quote_usd, terminal, .. } => {
                assert_eq!(*c, cloid);
                assert_eq!(*cumulative_qty, dec!(0.5));
                assert_eq!(*cumulative_quote_usd, Some(dec!(49.95)));
                assert!(*terminal);
            }
            _ => panic!("expected terminal execution progress"),
        }
    }

    #[test]
    fn aggressive_px_cap_directions() {
        // hedge buy pays up to +5bps; hedge sell concedes down 5bps.
        assert_eq!(cap_aggressive_px(dec!(100), Side::Buy, dec!(5)), dec!(100.05));
        assert_eq!(cap_aggressive_px(dec!(100), Side::Sell, dec!(5)), dec!(99.95));
    }
}

//! `live-report`: summarize livebot journal fills into realized two-leg PnL.

use std::collections::{BTreeMap, BTreeSet};
use std::fs::File;
use std::io::{BufRead, BufReader};
use std::path::{Path, PathBuf};

use anyhow::{Context, Result};
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::config::Config;
use crate::types::Side;

#[derive(Debug, Clone)]
struct FillRec {
    market: String,
    mono_ns: i64,
    hedge_side: Side,
    qty: Decimal,
    aster_px: Decimal,
}

#[derive(Debug, Clone)]
struct HedgeRec {
    market: String,
    first_mono_ns: i64,
    last_mono_ns: i64,
    hedge_side: Side,
    qty: Decimal,
    notional: Decimal,
    fee_usd: Decimal,
}

#[derive(Debug, Clone, Serialize)]
pub struct TradeSummary {
    pub cloid: String,
    pub market: String,
    pub first_mono_ns: i64,
    pub last_mono_ns: i64,
    pub hedge_side: Side,
    pub qty: Decimal,
    pub aster_px: Decimal,
    pub lighter_px: Decimal,
    pub gross_pnl: Decimal,
    pub aster_fee: Decimal,
    pub lighter_fee: Decimal,
    pub net_pnl: Decimal,
}

#[derive(Debug, Clone, Default, Serialize)]
pub struct LiveReportSummary {
    pub trades: Vec<TradeSummary>,
    pub unmatched_fills: usize,
    pub unmatched_hedges: usize,
    pub qty_mismatches: usize,
    pub gross_pnl: Decimal,
    pub aster_fees: Decimal,
    pub lighter_fees: Decimal,
    pub net_pnl: Decimal,
    pub total_qty: Decimal,
}

#[derive(Debug, Serialize)]
struct LiveReportJson<'a> {
    journal_path: &'a str,
    summary: &'a LiveReportSummary,
}

#[derive(Debug, Deserialize)]
struct JournalLine {
    #[serde(default)]
    mono_ns: i64,
    kind: String,
    market: Option<String>,
    detail: Value,
}

pub fn inferred_journal_path(db: &Path) -> PathBuf {
    let stem = db.file_stem().and_then(|s| s.to_str()).unwrap_or("livebot");
    let dir = db
        .parent()
        .filter(|p| !p.as_os_str().is_empty())
        .map(|p| p.to_path_buf())
        .unwrap_or_else(|| PathBuf::from("runs"));
    dir.join(format!("{stem}-journal.jsonl"))
}

pub fn summarize_path(path: &Path, cfg: &Config, market_filter: Option<&str>) -> Result<LiveReportSummary> {
    let file = File::open(path).with_context(|| format!("opening {}", path.display()))?;
    summarize_reader(BufReader::new(file), cfg, market_filter)
}

pub fn summarize_reader<R: BufRead>(reader: R, cfg: &Config, market_filter: Option<&str>) -> Result<LiveReportSummary> {
    summarize_reader_with_aster_fee(reader, cfg.edge.aster_maker_fee_rate(), market_filter)
}

fn summarize_reader_with_aster_fee<R: BufRead>(
    reader: R,
    aster_fee_rate: Decimal,
    market_filter: Option<&str>,
) -> Result<LiveReportSummary> {
    let mut fills: BTreeMap<String, FillRec> = BTreeMap::new();
    let mut hedges: BTreeMap<String, HedgeRec> = BTreeMap::new();

    for (idx, line) in reader.lines().enumerate() {
        let line = line.with_context(|| format!("reading journal line {}", idx + 1))?;
        if line.trim().is_empty() {
            continue;
        }
        let rec: JournalLine = serde_json::from_str(&line).with_context(|| format!("parsing journal line {}", idx + 1))?;
        let Some(market) = rec.market else {
            continue;
        };
        if market_filter.is_some_and(|want| want != market.as_str()) {
            continue;
        }
        match rec.kind.as_str() {
            "fill" => {
                let cloid = detail_str(&rec.detail, "cloid")?.to_string();
                fills.insert(
                    cloid,
                    FillRec {
                        market,
                        mono_ns: rec.mono_ns,
                        hedge_side: parse_side(detail_str(&rec.detail, "side")?)?,
                        qty: detail_dec(&rec.detail, "qty")?,
                        aster_px: detail_dec(&rec.detail, "avg_aster_px")?,
                    },
                );
            }
            "hedge_fill" => {
                let cloid = detail_str(&rec.detail, "cloid")?.to_string();
                let qty = detail_dec(&rec.detail, "qty")?;
                let px = detail_dec(&rec.detail, "px")?;
                let fee_usd = detail_dec(&rec.detail, "fee_usd").unwrap_or(Decimal::ZERO);
                hedges
                    .entry(cloid)
                    .and_modify(|h| {
                        if h.first_mono_ns == 0 || (rec.mono_ns > 0 && rec.mono_ns < h.first_mono_ns) {
                            h.first_mono_ns = rec.mono_ns;
                        }
                        h.last_mono_ns = h.last_mono_ns.max(rec.mono_ns);
                        h.qty += qty;
                        h.notional += qty * px;
                        h.fee_usd += fee_usd;
                    })
                    .or_insert(HedgeRec {
                        market,
                        first_mono_ns: rec.mono_ns,
                        last_mono_ns: rec.mono_ns,
                        hedge_side: parse_side(detail_str(&rec.detail, "side")?)?,
                        qty,
                        notional: qty * px,
                        fee_usd,
                    });
            }
            _ => {}
        }
    }

    let keys: BTreeSet<String> = fills.keys().chain(hedges.keys()).cloned().collect();
    let mut out = LiveReportSummary::default();

    for cloid in keys {
        match (fills.get(&cloid), hedges.get(&cloid)) {
            (Some(fill), Some(hedge)) => {
                let lighter_px = if hedge.qty > Decimal::ZERO { hedge.notional / hedge.qty } else { Decimal::ZERO };
                if fill.qty != hedge.qty || fill.hedge_side != hedge.hedge_side || fill.market != hedge.market {
                    out.qty_mismatches += 1;
                }
                let gross = match fill.hedge_side {
                    // The journal side is the Lighter hedge side. BUY hedge means Aster sold.
                    Side::Buy => fill.qty * (fill.aster_px - lighter_px),
                    Side::Sell => fill.qty * (lighter_px - fill.aster_px),
                };
                let aster_fee = fill.qty * fill.aster_px * aster_fee_rate;
                let lighter_fee = hedge.fee_usd;
                let net = gross - aster_fee - lighter_fee;
                out.total_qty += fill.qty;
                out.gross_pnl += gross;
                out.aster_fees += aster_fee;
                out.lighter_fees += lighter_fee;
                out.net_pnl += net;
                out.trades.push(TradeSummary {
                    cloid,
                    market: fill.market.clone(),
                    first_mono_ns: [fill.mono_ns, hedge.first_mono_ns]
                        .into_iter()
                        .filter(|v| *v > 0)
                        .min()
                        .unwrap_or(0),
                    last_mono_ns: fill.mono_ns.max(hedge.last_mono_ns),
                    hedge_side: fill.hedge_side,
                    qty: fill.qty,
                    aster_px: fill.aster_px,
                    lighter_px,
                    gross_pnl: gross,
                    aster_fee,
                    lighter_fee,
                    net_pnl: net,
                });
            }
            (Some(_), None) => out.unmatched_fills += 1,
            (None, Some(_)) => out.unmatched_hedges += 1,
            (None, None) => {}
        }
    }

    Ok(out)
}

pub fn print_summary(path: &Path, summary: &LiveReportSummary, details: bool) {
    println!("live-report: {}", path.display());
    println!("completed trades: {}", summary.trades.len());
    println!("unmatched fills: {}", summary.unmatched_fills);
    println!("unmatched hedge fills: {}", summary.unmatched_hedges);
    println!("qty/side mismatches: {}", summary.qty_mismatches);
    println!("total qty: {}", dec6(summary.total_qty));
    println!("gross pnl: {} USDC", dec6(summary.gross_pnl));
    println!("aster fees (configured): {} USDC", dec6(summary.aster_fees));
    println!("lighter fees (journal): {} USDC", dec6(summary.lighter_fees));
    println!("net pnl: {} USDC", dec6(summary.net_pnl));

    if details {
        println!();
        println!(
            "{:<6} {:<4} {:>10} {:>12} {:>12} {:>12} {:>12}",
            "market", "hedge", "qty", "aster_px", "lighter_px", "gross", "net"
        );
        for t in &summary.trades {
            println!(
                "{:<6} {:<4} {:>10} {:>12} {:>12} {:>12} {:>12}",
                t.market,
                t.hedge_side.as_str(),
                dec6(t.qty),
                dec6(t.aster_px),
                dec6(t.lighter_px),
                dec6(t.gross_pnl),
                dec6(t.net_pnl)
            );
        }
    }
}

pub fn print_summary_json(path: &Path, summary: &LiveReportSummary) -> Result<()> {
    let path_s = path.to_string_lossy();
    let out = LiveReportJson {
        journal_path: &path_s,
        summary,
    };
    println!("{}", serde_json::to_string_pretty(&out)?);
    Ok(())
}

fn parse_side(s: &str) -> Result<Side> {
    match s.trim().to_ascii_uppercase().as_str() {
        "BUY" => Ok(Side::Buy),
        "SELL" => Ok(Side::Sell),
        other => anyhow::bail!("unknown side {other:?}"),
    }
}

fn detail_str<'a>(v: &'a Value, key: &str) -> Result<&'a str> {
    v.get(key)
        .and_then(Value::as_str)
        .with_context(|| format!("journal detail missing string field {key:?}"))
}

fn detail_dec(v: &Value, key: &str) -> Result<Decimal> {
    match v.get(key) {
        Some(Value::String(s)) => s.parse().with_context(|| format!("parsing decimal field {key:?}: {s:?}")),
        Some(Value::Number(n)) => n.to_string().parse().with_context(|| format!("parsing decimal field {key:?}: {n}")),
        _ => anyhow::bail!("journal detail missing decimal field {key:?}"),
    }
}

fn dec6(d: Decimal) -> String {
    d.round_dp(6).normalize().to_string()
}

#[cfg(test)]
mod tests {
    use super::*;
    use rust_decimal_macros::dec;
    use std::io::Cursor;

    #[test]
    fn summarizes_completed_live_trades_from_hedge_side() {
        let text = r#"
{"mono_ns":1,"kind":"fill","market":"HYPE","detail":{"avg_aster_px":"60.65200","cloid":"a","qty":"0.20","side":"BUY"}}
{"mono_ns":2,"kind":"hedge_fill","market":"HYPE","detail":{"cloid":"a","fee_usd":"0.000028","px":"60.5944","qty":"0.20","side":"BUY"}}
{"mono_ns":3,"kind":"fill","market":"HYPE","detail":{"avg_aster_px":"60.46600","cloid":"b","qty":"0.21","side":"SELL"}}
{"mono_ns":4,"kind":"hedge_fill","market":"HYPE","detail":{"cloid":"b","fee_usd":"0.000032","px":"60.4967","qty":"0.21","side":"SELL"}}
"#;
        let s = summarize_reader_with_aster_fee(Cursor::new(text), Decimal::ZERO, None).unwrap();
        assert_eq!(s.trades.len(), 2);
        assert_eq!(s.gross_pnl.round_dp(6), dec!(0.017967));
        assert_eq!(s.lighter_fees, dec!(0.000060));
        assert_eq!(s.net_pnl.round_dp(6), dec!(0.017907));
    }

    #[test]
    fn reports_unmatched_records_and_filters_markets() {
        let text = r#"
{"mono_ns":1,"kind":"fill","market":"HYPE","detail":{"avg_aster_px":"60","cloid":"a","qty":"1","side":"BUY"}}
{"mono_ns":2,"kind":"hedge_fill","market":"ETH","detail":{"cloid":"b","fee_usd":"0","px":"10","qty":"1","side":"SELL"}}
{"mono_ns":3,"kind":"hedge_fill","market":"HYPE","detail":{"cloid":"c","fee_usd":"0","px":"61","qty":"1","side":"BUY"}}
"#;
        let s = summarize_reader_with_aster_fee(Cursor::new(text), Decimal::ZERO, Some("HYPE")).unwrap();
        assert_eq!(s.trades.len(), 0);
        assert_eq!(s.unmatched_fills, 1);
        assert_eq!(s.unmatched_hedges, 1);
    }
}

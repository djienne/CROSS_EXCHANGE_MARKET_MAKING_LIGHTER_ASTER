use std::collections::{BTreeMap, HashMap};
use std::sync::Arc;
use std::time::Duration;

use anyhow::{anyhow, Context, Result};
use reqwest::Method;
use rust_decimal::Decimal;
use serde::Deserialize;

use crate::aster::sign::{AsterNonce, AsterSigner, MonotonicMs};
use crate::decimal::trim_dec;
use crate::markets::MarketSpec;
use crate::types::{FillSummary, MarketId, Side};

const ASTER_ORDER_PATH: &str = "/fapi/v3/order";
const ASTER_RECV_WINDOW: &str = "50000";
const USER_AGENT: &str = "lighter-aster-taker-arb";

#[derive(Clone)]
struct MarketWire {
    symbol: String,
    step: Decimal,
    tick: Decimal,
}

#[derive(Debug, Clone)]
pub enum SubmitOutcome {
    Accepted {
        venue_order_id: Option<i64>,
        raw: String,
    },
    Rejected {
        reason: String,
    },
    Unknown {
        reason: String,
    },
}

#[derive(Debug, Clone, Deserialize)]
pub struct AsterPositionRow {
    pub symbol: String,
    #[serde(rename = "positionAmt")]
    pub position_amt: String,
}

#[derive(Debug, Clone, Deserialize)]
pub struct AsterBalanceRow {
    pub asset: String,
    #[serde(rename = "availableBalance", default)]
    pub available_balance: String,
    #[serde(default)]
    pub balance: String,
    #[serde(rename = "crossWalletBalance", default)]
    pub cross_wallet_balance: String,
    #[serde(rename = "crossUnPnl", default)]
    pub cross_un_pnl: String,
}

#[derive(Debug, Clone, Copy)]
pub struct AsterBalanceSnapshot {
    pub available_usd: Decimal,
    pub wallet_balance_usd: Option<Decimal>,
    pub cross_wallet_balance_usd: Option<Decimal>,
    pub cross_unrealized_pnl_usd: Option<Decimal>,
}

impl AsterBalanceSnapshot {
    pub fn equity_usd(self) -> Option<Decimal> {
        match (self.cross_wallet_balance_usd, self.cross_unrealized_pnl_usd) {
            (Some(wallet), Some(unpnl)) => Some(wallet + unpnl),
            _ => self.wallet_balance_usd,
        }
    }
}

#[derive(Debug, Clone, Deserialize)]
pub struct AsterOpenOrder {
    #[serde(rename = "orderId")]
    pub order_id: i64,
    #[serde(rename = "clientOrderId", default)]
    pub client_order_id: String,
    #[serde(default)]
    pub symbol: String,
    #[serde(default)]
    pub status: String,
}

#[derive(Debug, Deserialize)]
struct AsterOrderResp {
    #[serde(rename = "orderId")]
    order_id: Option<i64>,
    status: Option<String>,
    #[serde(rename = "executedQty")]
    executed_qty: Option<String>,
    #[serde(rename = "cumQty")]
    cum_qty: Option<String>,
    #[serde(rename = "cumQuote")]
    cum_quote: Option<String>,
    #[serde(rename = "avgPrice")]
    avg_price: Option<String>,
    code: Option<i64>,
    msg: Option<String>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct AsterUserTrade {
    #[serde(default)]
    pub id: i64,
    #[serde(rename = "orderId")]
    pub order_id: i64,
    pub price: String,
    pub qty: String,
    #[serde(rename = "quoteQty", default)]
    pub quote_qty: String,
    #[serde(default)]
    pub commission: String,
    #[serde(rename = "commissionAsset", default)]
    pub commission_asset: String,
    #[serde(default)]
    pub time: i64,
    #[serde(default)]
    pub buyer: bool,
    #[serde(default)]
    pub maker: bool,
}

#[derive(Debug, Clone, Copy)]
pub struct AsterImmediateFill {
    pub qty: Decimal,
    pub vwap: Decimal,
    pub notional: Decimal,
}

pub struct AsterRest {
    client: reqwest::Client,
    base_url: String,
    signer: Arc<dyn AsterSigner>,
    nonce: AsterNonce,
    timestamp: MonotonicMs,
    markets: HashMap<MarketId, MarketWire>,
}

impl AsterRest {
    pub fn new(
        base_url: String,
        signer: Arc<dyn AsterSigner>,
        specs: &[MarketSpec],
    ) -> Result<Self> {
        let client = reqwest::Client::builder()
            .timeout(Duration::from_secs(5))
            .tcp_nodelay(true)
            .pool_idle_timeout(Some(Duration::from_secs(120)))
            .pool_max_idle_per_host(4)
            .tcp_keepalive(Some(Duration::from_secs(30)))
            .build()?;
        let markets = specs
            .iter()
            .map(|s| {
                (
                    s.market_id.clone(),
                    MarketWire {
                        symbol: s.aster_symbol.clone(),
                        step: s.step,
                        tick: s.tick,
                    },
                )
            })
            .collect();
        Ok(AsterRest {
            client,
            base_url,
            signer,
            nonce: AsterNonce::new(),
            timestamp: MonotonicMs::new(),
            markets,
        })
    }

    fn wire(&self, market: &MarketId) -> Result<&MarketWire> {
        self.markets
            .get(market)
            .ok_or_else(|| anyhow!("no Aster wire context for {market}"))
    }

    async fn signed_request(
        &self,
        method: Method,
        path: &str,
        business: Vec<(String, String)>,
    ) -> Result<String> {
        let mut params = business;
        params.push(("recvWindow".into(), ASTER_RECV_WINDOW.into()));
        params.push(("timestamp".into(), self.timestamp.next().to_string()));
        let json_map: BTreeMap<&str, &str> = params
            .iter()
            .map(|(k, v)| (k.as_str(), v.as_str()))
            .collect();
        let json_str = serde_json::to_string(&json_map)?;
        let nonce = self.nonce.next();
        let sig = self.signer.sign_v3(&json_str, nonce)?;
        params.push(("nonce".into(), nonce.to_string()));
        params.push(("user".into(), self.signer.user_address().to_string()));
        params.push(("signer".into(), self.signer.signer_address().to_string()));
        params.push(("signature".into(), sig.0));

        let url = format!("{}{}", self.base_url.trim_end_matches('/'), path);
        let builder = match method {
            Method::GET => self.client.get(&url).query(&params),
            Method::POST => self.client.post(&url).form(&params),
            Method::DELETE => self.client.delete(&url).form(&params),
            other => return Err(anyhow!("unsupported Aster method {other}")),
        };
        let resp = builder.header("User-Agent", USER_AGENT).send().await?;
        let status = resp.status();
        let text = resp.text().await?;
        if !status.is_success() {
            return Err(anyhow!("Aster {path} HTTP {}: {}", status.as_u16(), text));
        }
        Ok(text)
    }

    pub async fn submit_market_order(
        &self,
        market: &MarketId,
        side: Side,
        qty: Decimal,
        reduce_only: bool,
    ) -> SubmitOutcome {
        let params = match self.market_params(market, side, qty, reduce_only) {
            Ok(p) => p,
            Err(e) => {
                return SubmitOutcome::Rejected {
                    reason: e.to_string(),
                }
            }
        };
        match self
            .signed_request(Method::POST, ASTER_ORDER_PATH, params)
            .await
        {
            Ok(body) => classify_order_response(&body),
            Err(e) => SubmitOutcome::Unknown {
                reason: e.to_string(),
            },
        }
    }

    pub async fn submit_ioc_order(
        &self,
        market: &MarketId,
        side: Side,
        qty: Decimal,
        price_bound: Decimal,
        reduce_only: bool,
    ) -> SubmitOutcome {
        let params = match self.limit_ioc_params(market, side, qty, price_bound, reduce_only) {
            Ok(p) => p,
            Err(e) => {
                return SubmitOutcome::Rejected {
                    reason: e.to_string(),
                }
            }
        };
        match self
            .signed_request(Method::POST, ASTER_ORDER_PATH, params)
            .await
        {
            Ok(body) => classify_order_response(&body),
            Err(e) => SubmitOutcome::Unknown {
                reason: e.to_string(),
            },
        }
    }

    fn market_params(
        &self,
        market: &MarketId,
        side: Side,
        qty: Decimal,
        reduce_only: bool,
    ) -> Result<Vec<(String, String)>> {
        let w = self.wire(market)?;
        let qty = floor_to_step(qty, w.step);
        if qty <= Decimal::ZERO {
            anyhow::bail!("Aster quantity rounds to zero");
        }
        let mut p = vec![
            ("symbol".into(), w.symbol.clone()),
            ("side".into(), side.as_str().to_string()),
            ("type".into(), "MARKET".into()),
            ("quantity".into(), trim_dec(qty)),
            ("positionSide".into(), "BOTH".into()),
        ];
        if reduce_only {
            p.push(("reduceOnly".into(), "true".into()));
        }
        Ok(p)
    }

    fn limit_ioc_params(
        &self,
        market: &MarketId,
        side: Side,
        qty: Decimal,
        price_bound: Decimal,
        reduce_only: bool,
    ) -> Result<Vec<(String, String)>> {
        let w = self.wire(market)?;
        let qty = floor_to_step(qty, w.step);
        if qty <= Decimal::ZERO {
            anyhow::bail!("Aster quantity rounds to zero");
        }
        let price = match side {
            Side::Buy => ceil_to_step(price_bound, w.tick),
            Side::Sell => floor_to_step(price_bound, w.tick),
        };
        if price <= Decimal::ZERO {
            anyhow::bail!("Aster price bound rounds to zero");
        }
        let mut p = vec![
            ("symbol".into(), w.symbol.clone()),
            ("side".into(), side.as_str().to_string()),
            ("type".into(), "LIMIT".into()),
            ("timeInForce".into(), "IOC".into()),
            ("quantity".into(), trim_dec(qty)),
            ("price".into(), trim_dec(price)),
            ("positionSide".into(), "BOTH".into()),
        ];
        if reduce_only {
            p.push(("reduceOnly".into(), "true".into()));
        }
        Ok(p)
    }

    pub async fn position_qty(&self, market: &MarketId) -> Result<Decimal> {
        let symbol = self.wire(market)?.symbol.clone();
        let body = self
            .signed_request(
                Method::GET,
                "/fapi/v3/positionRisk",
                vec![("symbol".into(), symbol.clone())],
            )
            .await?;
        let rows: Vec<AsterPositionRow> = serde_json::from_str(&body)
            .map_err(|e| anyhow!("parse Aster positionRisk: {e}: {body}"))?;
        Ok(rows
            .into_iter()
            .find(|r| r.symbol.eq_ignore_ascii_case(&symbol))
            .and_then(|r| r.position_amt.parse::<Decimal>().ok())
            .unwrap_or(Decimal::ZERO))
    }

    pub async fn available_usdc(&self) -> Result<Decimal> {
        Ok(self.balance_snapshot().await?.available_usd)
    }

    pub async fn balance_snapshot(&self) -> Result<AsterBalanceSnapshot> {
        let body = self
            .signed_request(Method::GET, "/fapi/v3/balance", vec![])
            .await?;
        let rows: Vec<AsterBalanceRow> =
            serde_json::from_str(&body).map_err(|e| anyhow!("parse Aster balance: {e}: {body}"))?;
        // ALL USD-pegged rows, not just USDT/USDC: cross-margin settles funding/PnL per
        // asset, so an account collateralized in USDC can carry a NEGATIVE USDT row
        // (real debt — observed live 2026-07-04).
        let stable_rows: Vec<AsterBalanceRow> = rows
            .into_iter()
            .filter(|r| !r.asset.is_empty() && is_usd_stable_asset(&r.asset))
            .collect();
        if stable_rows.is_empty() {
            return Ok(AsterBalanceSnapshot {
                available_usd: Decimal::ZERO,
                wallet_balance_usd: None,
                cross_wallet_balance_usd: None,
                cross_unrealized_pnl_usd: None,
            });
        }
        let available_usd = stable_rows
            .iter()
            .filter_map(|r| parse_optional_dec(&r.available_balance))
            .max()
            .unwrap_or(Decimal::ZERO);
        // SIGNED sum: a negative stablecoin row is debt and must reduce the wallet total
        // (the old `> 0` filter overstated it by the debt).
        let wallet_balance_usd: Decimal = stable_rows
            .iter()
            .filter_map(|r| parse_optional_dec(&r.balance))
            .sum();
        // SIGNED sums across ALL stable rows for the equity terms: the old
        // positive-only `.max()` picked the USDC collateral row and dropped the negative
        // USDT debt row, so `equity_usd()` overstated equity by the debt — and per-asset
        // crossUnPnl on rows other than the single positive one was silently dropped.
        // Absent-on-every-row keeps the Option at None (equity falls back to the wallet sum).
        let cross_vals: Vec<Decimal> = stable_rows
            .iter()
            .filter_map(|r| parse_optional_dec(&r.cross_wallet_balance))
            .collect();
        let cross_wallet_balance_usd =
            (!cross_vals.is_empty()).then(|| cross_vals.into_iter().sum::<Decimal>());
        let unpnl_vals: Vec<Decimal> = stable_rows
            .iter()
            .filter_map(|r| parse_optional_dec(&r.cross_un_pnl))
            .collect();
        let cross_unrealized_pnl_usd =
            (!unpnl_vals.is_empty()).then(|| unpnl_vals.into_iter().sum::<Decimal>());
        Ok(AsterBalanceSnapshot {
            available_usd,
            // None here means "unknown", so a net-NEGATIVE wallet (fully underwater
            // account) is hidden from the equity fallback; acceptable while the cross
            // fields above (signed) are the primary equity terms.
            wallet_balance_usd: (wallet_balance_usd > Decimal::ZERO).then_some(wallet_balance_usd),
            cross_wallet_balance_usd,
            cross_unrealized_pnl_usd,
        })
    }

    pub async fn open_orders(&self, market: &MarketId) -> Result<Vec<AsterOpenOrder>> {
        let symbol = self.wire(market)?.symbol.clone();
        let body = self
            .signed_request(
                Method::GET,
                "/fapi/v3/openOrders",
                vec![("symbol".into(), symbol)],
            )
            .await
            .context("Aster openOrders")?;
        serde_json::from_str(&body).map_err(|e| anyhow!("parse Aster openOrders: {e}: {body}"))
    }

    pub async fn order_trades(
        &self,
        market: &MarketId,
        order_id: i64,
    ) -> Result<Vec<AsterUserTrade>> {
        let symbol = self.wire(market)?.symbol.clone();
        let body = self
            .signed_request(
                Method::GET,
                "/fapi/v3/userTrades",
                vec![
                    ("symbol".into(), symbol),
                    ("orderId".into(), order_id.to_string()),
                    ("limit".into(), "1000".into()),
                ],
            )
            .await
            .context("Aster userTrades")?;
        let mut rows: Vec<AsterUserTrade> = serde_json::from_str(&body)
            .map_err(|e| anyhow!("parse Aster userTrades: {e}: {body}"))?;
        rows.retain(|r| r.order_id == order_id);
        Ok(rows)
    }

    pub async fn wait_order_fill_summary(
        &self,
        market: &MarketId,
        order_id: i64,
        expected_qty: Decimal,
        timeout: Duration,
    ) -> Result<FillSummary> {
        let deadline = tokio::time::Instant::now() + timeout;
        let min_expected = expected_qty * Decimal::from(999u32) / Decimal::from(1000u32);
        let mut last = None;
        // Assigned by every match arm below before any read, so no initializer.
        let mut last_err: Option<anyhow::Error>;
        loop {
            // Retry ALL order_trades errors inside the deadline (mirrors
            // wait_post_trade_reconciled): a single transient REST failure used to
            // abort the whole wait and could leave a filled trade unbooked. A
            // persistent error just costs the deadline it already cost.
            match self.order_trades(market, order_id).await {
                Ok(trades) => {
                    last_err = None;
                    if let Some(summary) = summarize_user_trades(&trades) {
                        if summary.qty >= min_expected {
                            return Ok(summary);
                        }
                        last = Some(summary);
                    }
                }
                Err(e) => {
                    tracing::warn!(
                        "Aster userTrades poll failed for orderId={order_id} (retrying within deadline): {e:#}"
                    );
                    last_err = Some(e);
                }
            }
            if tokio::time::Instant::now() >= deadline {
                // Best partial summary beats an error; an error beats nothing.
                if let Some(summary) = last {
                    return Ok(summary);
                }
                if let Some(e) = last_err {
                    return Err(e.context(format!(
                        "Aster userTrades kept failing for orderId={order_id} until the deadline"
                    )));
                }
                anyhow::bail!("no Aster userTrades found for orderId={order_id}");
            }
            tokio::time::sleep(Duration::from_millis(250)).await;
        }
    }
}

fn parse_optional_dec(raw: &str) -> Option<Decimal> {
    let s = raw.trim();
    if s.is_empty() {
        None
    } else {
        s.parse::<Decimal>().ok()
    }
}

/// USD-pegged commission assets that need no conversion.
fn is_usd_stable_asset(asset: &str) -> bool {
    matches!(
        asset.to_ascii_uppercase().as_str(),
        "" | "USD" | "USDT" | "USDC" | "BUSD" | "USDF" | "FDUSD" | "DAI"
    )
}

fn summarize_user_trades(rows: &[AsterUserTrade]) -> Option<FillSummary> {
    let mut qty = Decimal::ZERO;
    let mut notional = Decimal::ZERO;
    let mut fee = Decimal::ZERO;
    for row in rows {
        let q = row.qty.parse::<Decimal>().unwrap_or(Decimal::ZERO).abs();
        let quote = row
            .quote_qty
            .parse::<Decimal>()
            .unwrap_or(Decimal::ZERO)
            .abs();
        let px = row.price.parse::<Decimal>().unwrap_or(Decimal::ZERO);
        if q <= Decimal::ZERO {
            continue;
        }
        qty += q;
        notional += if quote > Decimal::ZERO { quote } else { q * px };
        let commission = row
            .commission
            .parse::<Decimal>()
            .unwrap_or(Decimal::ZERO)
            .abs();
        // Commission is only USD when the commission asset is a USD stablecoin; a
        // base-asset commission must be valued at the trade price or per-trade fees
        // (hence net PnL feeding the breaker) are mis-valued.
        if is_usd_stable_asset(&row.commission_asset) {
            fee += commission;
        } else {
            tracing::warn!(
                "Aster commission in non-USD asset {:?}; valuing at trade price {}",
                row.commission_asset,
                px
            );
            fee += commission * px;
        }
    }
    FillSummary::from_qty_notional(qty, notional, fee)
}

pub fn immediate_fill_from_order_response(body: &str) -> Result<AsterImmediateFill> {
    let r: AsterOrderResp = serde_json::from_str(body)
        .map_err(|e| anyhow!("parse Aster order response immediate fill: {e}: {body}"))?;
    Ok(immediate_fill_from_order(&r))
}

fn immediate_fill_from_order(r: &AsterOrderResp) -> AsterImmediateFill {
    let qty = r.executed_qty
        .as_deref()
        .and_then(parse_optional_dec)
        .or_else(|| r.cum_qty.as_deref().and_then(parse_optional_dec))
        .unwrap_or(Decimal::ZERO)
        .abs();
    let avg_price = r.avg_price
        .as_deref()
        .and_then(parse_optional_dec)
        .unwrap_or(Decimal::ZERO)
        .abs();
    let notional = r.cum_quote
        .as_deref()
        .and_then(parse_optional_dec)
        .map(|v| v.abs())
        .filter(|v| *v > Decimal::ZERO)
        .unwrap_or_else(|| qty * avg_price);
    let vwap = if qty > Decimal::ZERO {
        if notional > Decimal::ZERO {
            notional / qty
        } else {
            avg_price
        }
    } else {
        Decimal::ZERO
    };
    AsterImmediateFill {
        qty,
        vwap,
        notional,
    }
}

fn classify_order_response(body: &str) -> SubmitOutcome {
    match serde_json::from_str::<AsterOrderResp>(body) {
        Ok(r) => {
            if let Some(code) = r.code {
                return SubmitOutcome::Rejected {
                    reason: format!("code {code}: {}", r.msg.unwrap_or_default()),
                };
            }
            match r.status.as_deref() {
                Some("NEW") | Some("PARTIALLY_FILLED") | Some("FILLED") => {
                    SubmitOutcome::Accepted {
                        venue_order_id: r.order_id,
                        raw: body.to_string(),
                    }
                }
                Some("EXPIRED") if expired_with_fill(&r) => SubmitOutcome::Accepted {
                    venue_order_id: r.order_id,
                    raw: body.to_string(),
                },
                Some("EXPIRED") | Some("REJECTED") => SubmitOutcome::Rejected {
                    reason: format!("status {}", r.status.unwrap_or_default()),
                },
                Some(other) => SubmitOutcome::Unknown {
                    reason: format!("unexpected Aster order status {other}: {body}"),
                },
                None => SubmitOutcome::Unknown {
                    reason: format!("missing Aster order status: {body}"),
                },
            }
        }
        Err(e) => SubmitOutcome::Unknown {
            reason: format!("unparseable Aster order response: {e}: {body}"),
        },
    }
}

fn expired_with_fill(r: &AsterOrderResp) -> bool {
    r.order_id.is_some() && immediate_fill_from_order(r).qty > Decimal::ZERO
}

fn floor_to_step(qty: Decimal, step: Decimal) -> Decimal {
    if qty <= Decimal::ZERO || step <= Decimal::ZERO {
        return Decimal::ZERO;
    }
    (qty / step).floor() * step
}

fn ceil_to_step(qty: Decimal, step: Decimal) -> Decimal {
    if qty <= Decimal::ZERO || step <= Decimal::ZERO {
        return Decimal::ZERO;
    }
    (qty / step).ceil() * step
}

#[cfg(test)]
mod tests {
    use super::*;
    use rust_decimal_macros::dec;

    #[test]
    fn immediate_fill_parses_zero_ioc_fill() {
        let body = r#"{"orderId":2075571341,"status":"NEW","executedQty":"0","cumQty":"0","cumQuote":"0","avgPrice":"0"}"#;
        let fill = immediate_fill_from_order_response(body).expect("fill parse");
        assert_eq!(fill.qty, Decimal::ZERO);
        assert_eq!(fill.notional, Decimal::ZERO);
        assert_eq!(fill.vwap, Decimal::ZERO);
    }

    #[test]
    fn immediate_fill_prefers_cum_quote_for_vwap() {
        let body = r#"{"orderId":1,"status":"FILLED","executedQty":"0.21","cumQty":"0.21","cumQuote":"12.931107","avgPrice":"61.5767"}"#;
        let fill = immediate_fill_from_order_response(body).expect("fill parse");
        assert_eq!(fill.qty, dec!(0.21));
        assert_eq!(fill.notional, dec!(12.931107));
        assert_eq!(fill.vwap, dec!(61.5767));
    }

    #[test]
    fn immediate_fill_uses_cum_qty_when_executed_qty_missing() {
        let body = r#"{"orderId":1,"status":"PARTIALLY_FILLED","cumQty":"0.07","cumQuote":"4.305","avgPrice":"61.5"}"#;
        let fill = immediate_fill_from_order_response(body).expect("fill parse");
        assert_eq!(fill.qty, dec!(0.07));
        assert_eq!(fill.notional, dec!(4.305));
        assert_eq!(fill.vwap, dec!(61.5));
    }
}

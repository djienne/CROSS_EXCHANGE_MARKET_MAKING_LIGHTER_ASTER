//! One-shot REST fetch of market specifications: Aster `exchangeInfo` (tick /
//! step / min-qty / min-notional) and Lighter `orderBooks` metadata. Combined
//! into `MarketSpec`s that are written into the run-log header so replay is
//! fully offline.

use std::collections::HashMap;
use std::time::Duration;

use anyhow::{anyhow, Context, Result};
use rust_decimal::Decimal;
use serde::Deserialize;

use crate::config::MarketCfg;
use crate::decimal::parse_dec;
use crate::markets::MarketSpec;

use super::rest_book::{DEFAULT_ASTER_BASE_URL, DEFAULT_LIGHTER_BASE_URL};

fn endpoint(base_url: &str, path: &str) -> String {
    format!("{}{}", base_url.trim_end_matches('/'), path)
}

#[derive(Deserialize)]
struct ExchangeInfo {
    symbols: Vec<SymbolInfo>,
}

#[derive(Deserialize)]
struct SymbolInfo {
    symbol: String,
    #[serde(default)]
    filters: Vec<serde_json::Value>,
}

fn client() -> Result<reqwest::Client> {
    reqwest::Client::builder()
        .timeout(Duration::from_secs(20))
        .build()
        .context("building http client")
}

/// Map of Aster symbol -> (tick, step, min_qty, min_notional).
pub async fn fetch_aster_exchange_info(
    client: &reqwest::Client,
) -> Result<HashMap<String, (Decimal, Decimal, Decimal, Decimal)>> {
    fetch_aster_exchange_info_from_base(client, DEFAULT_ASTER_BASE_URL).await
}

/// Same as [`fetch_aster_exchange_info`], but against a configured REST base URL.
pub async fn fetch_aster_exchange_info_from_base(
    client: &reqwest::Client,
    base_url: &str,
) -> Result<HashMap<String, (Decimal, Decimal, Decimal, Decimal)>> {
    let url = endpoint(base_url, "/fapi/v3/exchangeInfo");
    let info: ExchangeInfo = client
        .get(&url)
        .send()
        .await
        .with_context(|| format!("GET Aster exchangeInfo from {base_url}"))?
        .error_for_status()?
        .json()
        .await
        .context("parsing Aster exchangeInfo")?;

    let mut out = HashMap::new();
    for s in info.symbols {
        let mut tick = None;
        let mut step = None;
        let mut min_qty = None;
        let mut min_notional = None;
        for f in &s.filters {
            match f.get("filterType").and_then(|v| v.as_str()) {
                Some("PRICE_FILTER") => tick = field_dec(f, "tickSize"),
                Some("LOT_SIZE") => {
                    step = field_dec(f, "stepSize");
                    min_qty = field_dec(f, "minQty");
                }
                Some("MIN_NOTIONAL") => min_notional = field_dec(f, "notional"),
                _ => {}
            }
        }
        if let (Some(tick), Some(step)) = (tick, step) {
            out.insert(
                s.symbol,
                (
                    tick,
                    step,
                    min_qty.unwrap_or(step),
                    min_notional.unwrap_or(Decimal::from(5)),
                ),
            );
        }
    }
    Ok(out)
}

fn field_dec(f: &serde_json::Value, key: &str) -> Option<Decimal> {
    f.get(key).and_then(|v| v.as_str()).and_then(|s| parse_dec(s).ok())
}

#[derive(Debug, Clone)]
pub struct LighterMarketMeta {
    pub market_id: u32,
    pub symbol: String,
    pub size_decimals: u32,
    pub price_decimals: u32,
    pub min_base_amount: Decimal,
    pub min_quote_amount: Decimal,
}

/// Map of Lighter symbol -> market metadata.
pub async fn fetch_lighter_meta(client: &reqwest::Client) -> Result<HashMap<String, LighterMarketMeta>> {
    fetch_lighter_meta_from_base(client, DEFAULT_LIGHTER_BASE_URL).await
}

/// Same as [`fetch_lighter_meta`], but against a configured REST base URL.
pub async fn fetch_lighter_meta_from_base(client: &reqwest::Client, base_url: &str) -> Result<HashMap<String, LighterMarketMeta>> {
    let url = endpoint(base_url, "/api/v1/orderBooks");
    let resp: crate::lighter::messages::OrderBooksResponse = client
        .get(&url)
        .send()
        .await
        .with_context(|| format!("GET Lighter orderBooks from {base_url}"))?
        .error_for_status()?
        .json()
        .await
        .context("parsing Lighter orderBooks")?;
    let mut out = HashMap::new();
    for b in resp.order_books {
        let min_base_amount = parse_dec(&b.min_base_amount).unwrap_or(Decimal::ZERO);
        let min_quote_amount = parse_dec(&b.min_quote_amount).unwrap_or(Decimal::ZERO);
        out.insert(
            b.symbol.to_ascii_uppercase(),
            LighterMarketMeta {
                market_id: b.market_id,
                symbol: b.symbol,
                size_decimals: b.supported_size_decimals,
                price_decimals: b.supported_price_decimals,
                min_base_amount,
                min_quote_amount,
            },
        );
    }
    Ok(out)
}

/// Resolve `MarketSpec`s for the configured markets from both venues.
pub async fn build_market_specs(markets: &[MarketCfg], hl_min_notional: Decimal) -> Result<Vec<MarketSpec>> {
    build_market_specs_with_bases(markets, hl_min_notional, DEFAULT_ASTER_BASE_URL, DEFAULT_LIGHTER_BASE_URL).await
}

/// Resolve `MarketSpec`s against configured REST base URLs (live/testnet/custom).
pub async fn build_market_specs_with_bases(
    markets: &[MarketCfg],
    hl_min_notional: Decimal,
    aster_base_url: &str,
    hl_base_url: &str,
) -> Result<Vec<MarketSpec>> {
    let client = client()?;
    let aster = fetch_aster_exchange_info_from_base(&client, aster_base_url).await?;
    let lighter = fetch_lighter_meta_from_base(&client, hl_base_url).await?;

    let mut specs = Vec::new();
    for m in markets {
        let (tick, step, min_qty, min_notional) = aster
            .get(&m.aster_symbol)
            .copied()
            .ok_or_else(|| anyhow!("Aster symbol {} not found in exchangeInfo", m.aster_symbol))?;
        let lm = lighter
            .get(&m.hl_coin.to_ascii_uppercase())
            .ok_or_else(|| anyhow!("Lighter symbol {} not found in orderBooks", m.hl_coin))?;
        let hl_qty_step = Decimal::new(1, lm.size_decimals);
        let lighter_price_tick = Decimal::new(1, lm.price_decimals);
        let hedge_min_notional = if lm.min_quote_amount > Decimal::ZERO {
            lm.min_quote_amount
        } else {
            hl_min_notional
        };
        specs.push(MarketSpec {
            market_id: m.id(),
            aster_symbol: m.aster_symbol.clone(),
            hl_coin: lm.symbol.clone(),
            lighter_market_id: lm.market_id,
            lighter_price_decimals: lm.price_decimals,
            lighter_size_decimals: lm.size_decimals,
            lighter_price_tick,
            tick,
            step,
            aster_min_qty: min_qty,
            aster_min_notional: min_notional,
            hl_sz_decimals: lm.size_decimals as i32,
            hl_qty_step,
            hl_min_notional: hedge_min_notional,
        });
    }
    Ok(specs)
}

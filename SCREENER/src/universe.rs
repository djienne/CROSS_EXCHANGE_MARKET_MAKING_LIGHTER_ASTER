//! The pairs to watch: perps listed on both venues, matched by name, confirmed by price and
//! filtered by 24 h volume.

use std::collections::HashMap;
use std::time::Duration;

use anyhow::{Context, Result};
use serde::{Deserialize, Serialize};

use crate::config::Collect;

pub const ASTER_REST: &str = "https://fapi.asterdex.com";
pub const LIGHTER_REST: &str = "https://mainnet.zklighter.elliot.ai";
/// A name match counts only if the prices agree this closely (Aster `BB` and Lighter `BB` differ
/// ~7.9M-fold: different assets).
const MAX_PRICE_MISMATCH: f64 = 0.02;

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Pair {
    /// The Lighter symbol, e.g. `HYPE` or `1000NOT`: the pair's name everywhere.
    pub name: String,
    pub aster: String,
    pub lighter_id: u32,
    /// Lighter price = `scale` x Aster price (1000 for Lighter `1000NOT` vs Aster `NOTUSDT`).
    pub scale: f64,
    /// Aster exchangeInfo `underlyingSubType`, which decides its fee (`Report::aster_taker_bps`).
    pub aster_subtypes: Vec<String>,
    pub aster_volume_usd: f64,
    pub lighter_volume_usd: f64,
}

#[derive(Debug, Clone)]
pub struct AsterListing {
    pub symbol: String,
    pub price: f64,
    pub volume_usd: f64,
    pub subtypes: Vec<String>,
}

#[derive(Debug, Clone)]
pub struct LighterListing {
    pub symbol: String,
    pub id: u32,
    pub price: f64,
    pub volume_usd: f64,
}

/// The pairs (most liquid first, at most `max_pairs`), and the name matches dropped by the price
/// check with their price ratio.
pub fn match_pairs(aster: &[AsterListing], lighter: &[LighterListing], min_volume_usd: f64, max_pairs: usize) -> (Vec<Pair>, Vec<(String, f64)>) {
    let by_symbol: HashMap<&str, &AsterListing> = aster.iter().map(|a| (a.symbol.as_str(), a)).collect();
    let (mut pairs, mut mismatched) = (Vec::new(), Vec::new());
    for l in lighter {
        let s = &l.symbol;
        let candidates = [
            Some((format!("{s}USDT"), 1.0)),
            s.ends_with("USD").then(|| (format!("{s}T"), 1.0)),
            s.strip_prefix("1000").map(|base| (format!("{base}USDT"), 1000.0)),
        ];
        let Some((a, scale)) = candidates.into_iter().flatten().find_map(|(sym, scale)| by_symbol.get(sym.as_str()).map(|a| (*a, scale))) else {
            continue;
        };
        let ratio = l.price / (a.price * scale);
        if !ratio.is_finite() || (ratio - 1.0).abs() > MAX_PRICE_MISMATCH {
            mismatched.push((s.clone(), ratio));
            continue;
        }
        if a.volume_usd.min(l.volume_usd) >= min_volume_usd {
            pairs.push(Pair {
                name: s.clone(),
                aster: a.symbol.clone(),
                lighter_id: l.id,
                scale,
                aster_subtypes: a.subtypes.clone(),
                aster_volume_usd: a.volume_usd,
                lighter_volume_usd: l.volume_usd,
            });
        }
    }
    pairs.sort_by(|x, y| y.aster_volume_usd.min(y.lighter_volume_usd).total_cmp(&x.aster_volume_usd.min(x.lighter_volume_usd)));
    pairs.truncate(max_pairs);
    (pairs, mismatched)
}

#[derive(Deserialize)]
struct ExchangeInfo {
    symbols: Vec<AsterSymbol>,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct AsterSymbol {
    symbol: String,
    status: String,
    contract_type: String,
    quote_asset: String,
    #[serde(default)]
    underlying_sub_type: Vec<String>,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct AsterTicker {
    symbol: String,
    last_price: String,
    quote_volume: String,
}

#[derive(Deserialize)]
struct LighterDetails {
    order_book_details: Vec<LighterMarket>,
}

#[derive(Deserialize)]
struct LighterMarket {
    symbol: String,
    market_id: u32,
    status: String,
    market_type: String,
    last_trade_price: f64,
    daily_quote_token_volume: f64,
}

/// Today's pairs, from Aster exchangeInfo and 24 h tickers and Lighter orderBookDetails.
pub async fn discover(cfg: &Collect) -> Result<(Vec<Pair>, Vec<(String, f64)>)> {
    let http = reqwest::Client::builder().timeout(Duration::from_secs(20)).build()?;
    let get = |url: String| {
        let request = http.get(url);
        async move { anyhow::Ok(request.send().await?.error_for_status()?.text().await?) }
    };
    let info: ExchangeInfo = serde_json::from_str(&get(format!("{ASTER_REST}/fapi/v3/exchangeInfo")).await?).context("Aster exchangeInfo")?;
    let tickers: Vec<AsterTicker> = serde_json::from_str(&get(format!("{ASTER_REST}/fapi/v3/ticker/24hr")).await?).context("Aster ticker/24hr")?;
    let details: LighterDetails =
        serde_json::from_str(&get(format!("{LIGHTER_REST}/api/v1/orderBookDetails")).await?).context("Lighter orderBookDetails")?;

    let tickers: HashMap<String, AsterTicker> = tickers.into_iter().map(|t| (t.symbol.clone(), t)).collect();
    let aster: Vec<AsterListing> = info
        .symbols
        .into_iter()
        .filter(|s| s.status == "TRADING" && s.contract_type == "PERPETUAL" && s.quote_asset == "USDT")
        .filter_map(|s| {
            let t = tickers.get(&s.symbol)?;
            Some(AsterListing {
                price: t.last_price.parse().ok()?,
                volume_usd: t.quote_volume.parse().ok()?,
                symbol: s.symbol,
                subtypes: s.underlying_sub_type,
            })
        })
        .collect();
    let lighter: Vec<LighterListing> = details
        .order_book_details
        .into_iter()
        .filter(|m| m.status == "active" && m.market_type == "perp")
        .map(|m| LighterListing { symbol: m.symbol, id: m.market_id, price: m.last_trade_price, volume_usd: m.daily_quote_token_volume })
        .collect();
    Ok(match_pairs(&aster, &lighter, cfg.min_volume_usd, cfg.max_pairs))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn aster(symbol: &str, price: f64, volume_usd: f64) -> AsterListing {
        AsterListing { symbol: symbol.into(), price, volume_usd, subtypes: vec![] }
    }

    fn lighter(symbol: &str, id: u32, price: f64, volume_usd: f64) -> LighterListing {
        LighterListing { symbol: symbol.into(), id, price, volume_usd }
    }

    #[test]
    fn names_match_by_rule_prices_confirm_and_volume_filters() {
        let a = [
            aster("HYPEUSDT", 92.4, 24e6),
            aster("NOTUSDT", 0.0021, 3e6),
            aster("SKHYNIXUSDT", 610.0, 1e6),
            aster("BBUSDT", 0.5, 5e6),
            aster("DOGEUSDT", 0.2, 50_000.0),
        ];
        let l = [
            lighter("HYPE", 24, 92.41, 45e6),
            lighter("1000NOT", 7, 2.1, 2e6),
            lighter("SKHYNIXUSD", 90, 612.0, 4e6),
            lighter("BB", 50, 3_950_000.0, 5e6),
            lighter("DOGE", 3, 0.2, 9e6),
            lighter("ZZZ", 99, 1.0, 9e6),
        ];
        let (pairs, mismatched) = match_pairs(&a, &l, 200_000.0, 10);
        let names: Vec<(&str, &str, f64)> = pairs.iter().map(|p| (p.name.as_str(), p.aster.as_str(), p.scale)).collect();
        // Ordered by the smaller venue volume: HYPE 24M, NOT 2M, SKHYNIX 1M; DOGE's Aster side is thin.
        assert_eq!(names, [("HYPE", "HYPEUSDT", 1.0), ("1000NOT", "NOTUSDT", 1000.0), ("SKHYNIXUSD", "SKHYNIXUSDT", 1.0)]);
        assert_eq!(mismatched, [("BB".to_string(), 7_900_000.0)]);
        assert_eq!(match_pairs(&a, &l, 200_000.0, 1).0.len(), 1);
    }
}

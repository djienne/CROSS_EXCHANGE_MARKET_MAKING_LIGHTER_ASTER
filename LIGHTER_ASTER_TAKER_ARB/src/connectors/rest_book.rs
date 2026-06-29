use std::time::Duration;

use anyhow::{Context, Result};
use chrono::Utc;
use rust_decimal::Decimal;
use serde::Deserialize;

use crate::book::OrderBook;
use crate::decimal::parse_dec;

fn endpoint(base_url: &str, path: &str) -> String {
    format!("{}{}", base_url.trim_end_matches('/'), path)
}

pub fn client() -> Result<reqwest::Client> {
    reqwest::Client::builder()
        .timeout(Duration::from_secs(5))
        .tcp_nodelay(true)
        .pool_idle_timeout(Some(Duration::from_secs(120)))
        .pool_max_idle_per_host(4)
        .tcp_keepalive(Some(Duration::from_secs(30)))
        .build()
        .context("building REST book client")
}

#[derive(Deserialize)]
struct AsterDepthResp {
    #[serde(rename = "E", default)]
    event_time: i64,
    #[serde(default)]
    bids: Vec<[String; 2]>,
    #[serde(default)]
    asks: Vec<[String; 2]>,
}

pub async fn fetch_aster_book(
    client: &reqwest::Client,
    base_url: &str,
    symbol_upper: &str,
    limit: u32,
) -> Result<OrderBook> {
    let limit_s = limit.to_string();
    let url = endpoint(base_url, "/fapi/v3/depth");
    let resp: AsterDepthResp = client
        .get(&url)
        .query(&[("symbol", symbol_upper), ("limit", limit_s.as_str())])
        .send()
        .await
        .with_context(|| format!("GET Aster depth {symbol_upper}"))?
        .error_for_status()
        .with_context(|| format!("Aster depth status {symbol_upper}"))?
        .json()
        .await
        .with_context(|| format!("parse Aster depth {symbol_upper}"))?;
    Ok(OrderBook::from_levels(
        parse_pairs(&resp.bids),
        parse_pairs(&resp.asks),
        ms_to_dt(resp.event_time),
        Utc::now(),
    ))
}

pub async fn fetch_lighter_book(
    client: &reqwest::Client,
    base_url: &str,
    market_id: u32,
    limit: u32,
) -> Result<OrderBook> {
    let url = endpoint(base_url, "/api/v1/orderBookOrders");
    let resp: serde_json::Value = client
        .get(&url)
        .query(&[
            ("market_id", market_id.to_string()),
            ("limit", limit.to_string()),
        ])
        .send()
        .await
        .with_context(|| format!("GET Lighter orderBookOrders {market_id}"))?
        .error_for_status()
        .with_context(|| format!("Lighter orderBookOrders status {market_id}"))?
        .json()
        .await
        .with_context(|| format!("parse Lighter orderBookOrders {market_id}"))?;
    Ok(OrderBook::from_levels(
        lighter_rows(
            resp.get("bids")
                .or_else(|| resp.pointer("/order_book/bids")),
        ),
        lighter_rows(
            resp.get("asks")
                .or_else(|| resp.pointer("/order_book/asks")),
        ),
        Utc::now(),
        Utc::now(),
    ))
}

fn parse_pairs(rows: &[[String; 2]]) -> Vec<(Decimal, Decimal)> {
    rows.iter()
        .filter_map(|r| match (parse_dec(&r[0]), parse_dec(&r[1])) {
            (Ok(p), Ok(q)) => Some((p, q)),
            _ => None,
        })
        .collect()
}

fn lighter_rows(v: Option<&serde_json::Value>) -> Vec<(Decimal, Decimal)> {
    let Some(rows) = v.and_then(|v| v.as_array()) else {
        return Vec::new();
    };
    rows.iter()
        .filter_map(|r| {
            let p = r.get("price").or_else(|| r.get("px")).and_then(value_dec)?;
            let q = r
                .get("remaining_base_amount")
                .or_else(|| r.get("size"))
                .or_else(|| r.get("sz"))
                .and_then(value_dec)?;
            (p > Decimal::ZERO && q > Decimal::ZERO).then_some((p, q))
        })
        .collect()
}

fn value_dec(v: &serde_json::Value) -> Option<Decimal> {
    match v {
        serde_json::Value::String(s) => parse_dec(s).ok(),
        serde_json::Value::Number(n) => parse_dec(&n.to_string()).ok(),
        _ => None,
    }
}

fn ms_to_dt(ms: i64) -> chrono::DateTime<chrono::Utc> {
    if ms > 0 {
        chrono::DateTime::from_timestamp_millis(ms).unwrap_or_else(Utc::now)
    } else {
        Utc::now()
    }
}

//! Lighter REST client (reqwest). Endpoints + param encodings verified against the SDK:
//!   GET  /api/v1/orderBooks
//!   GET  /api/v1/nextNonce            ?account_index&api_key_index
//!   GET  /api/v1/accountActiveOrders  ?account_index&market_id (authorization header)
//!   GET  /api/v1/orderBookOrders      ?market_id&limit
//!   POST /api/v1/sendTx               form: tx_type, tx_info
//!   POST /api/v1/sendTxBatch          form: tx_types(json), tx_infos(json)

use crate::lighter::messages::{
    AccountActiveOrdersResponse, NextNonceResponse, OrderBookDetail, OrderBooksResponse,
    RemoteOrder, TxResponse,
};
use anyhow::{bail, Context, Result};
use rust_decimal::Decimal;
use std::time::Duration;

/// History pages (100 rows each, newest first) one order lookup reads per pass. A just-sent
/// IOC is among the newest rows; walking a lagging history deeper would spend a Standard
/// account's 60 REST calls a minute in one pass. An older order is resolved by hand (RUNBOOK).
pub const HISTORY_PAGES: usize = 2;

#[derive(Clone)]
pub struct RestClient {
    base: String,
    http: reqwest::Client,
}

impl RestClient {
    /// `max_idle_per_host` warm connections let latency-adjacent REST calls (nonce
    /// hard_refresh on reject recovery, reconciler fallback) skip the TLS handshake; the
    /// XEMM hedge worker keeps 2, the taker keeps none (its standalone behaviour).
    pub fn new(base_url: &str, max_idle_per_host: usize) -> Result<Self> {
        let http = reqwest::Client::builder()
            .timeout(Duration::from_secs(10))
            .tcp_nodelay(true)
            // Idle timeout stays below typical CDN idle-close windows so hyper retires
            // connections before the server can slam them mid-request.
            .pool_max_idle_per_host(max_idle_per_host)
            .pool_idle_timeout(Some(Duration::from_secs(60)))
            .tcp_keepalive(Some(Duration::from_secs(30)))
            .build()
            .context("build reqwest client")?;
        Ok(Self {
            base: base_url.trim_end_matches('/').to_string(),
            http,
        })
    }

    fn url(&self, path: &str) -> String {
        format!("{}{}", self.base, path)
    }

    async fn authenticated_history(
        &self, path: &str, auth: &str, mut params: Vec<(&str, String)>, cursor: Option<&str>,
    ) -> Result<serde_json::Value> {
        params.push(("limit", "100".into()));
        if let Some(cursor) = cursor { params.push(("cursor", cursor.to_string())); }
        let value: serde_json::Value = self.http.get(self.url(path))
            .header("authorization", auth).query(&params).send().await?
            .error_for_status()?.json().await?;
        if !value.get("code").and_then(|c| c.as_i64()).is_some_and(|c| c == 0 || c == 200) {
            bail!("Lighter history returned an error envelope: {value}");
        }
        Ok(value)
    }

    pub async fn account_inactive_orders(
        &self, account_index: i64, market_id: u32, auth: &str, cursor: Option<&str>,
    ) -> Result<serde_json::Value> {
        self.authenticated_history("/api/v1/accountInactiveOrders", auth, vec![
            ("account_index", account_index.to_string()), ("market_id", market_id.to_string()),
        ], cursor).await
    }

    pub async fn trades_by_order(
        &self, account_index: i64, order_index: i64, auth: &str, cursor: Option<&str>,
    ) -> Result<serde_json::Value> {
        self.authenticated_history("/api/v1/trades", auth, vec![
            ("account_index", account_index.to_string()), ("order_index", order_index.to_string()),
            ("sort_by", "trade_id".into()), ("sort_dir", "desc".into()),
        ], cursor).await
    }

    pub async fn tx_by_hash(&self, tx_hash: &str) -> Result<serde_json::Value> {
        let value: serde_json::Value = self.http.get(self.url("/api/v1/tx"))
            .query(&[("by", "hash"), ("value", tx_hash)]).send().await?
            .error_for_status()?.json().await?;
        if value.get("code").and_then(|c| c.as_i64()).is_some_and(|c| c != 0 && c != 200) {
            bail!("Lighter transaction query returned an error envelope: {value}");
        }
        Ok(value)
    }

    pub async fn order_books(&self) -> Result<Vec<OrderBookDetail>> {
        let resp: OrderBooksResponse = self
            .http
            .get(self.url("/api/v1/orderBooks"))
            .send()
            .await?
            .error_for_status()?
            .json()
            .await
            .context("parse orderBooks")?;
        Ok(resp.order_books)
    }

    /// Resolve a symbol -> its market detail (ticks via decimals, min amounts).
    pub async fn market_detail(&self, symbol: &str) -> Result<OrderBookDetail> {
        let books = self.order_books().await?;
        books
            .into_iter()
            .find(|b| b.symbol.eq_ignore_ascii_case(symbol))
            .with_context(|| format!("symbol {symbol} not found in orderBooks"))
    }

    pub async fn next_nonce(&self, account_index: i64, api_key_index: i32) -> Result<i64> {
        let resp: NextNonceResponse = self
            .http
            .get(self.url("/api/v1/nextNonce"))
            .query(&[
                ("account_index", account_index.to_string()),
                ("api_key_index", api_key_index.to_string()),
            ])
            .send()
            .await?
            .error_for_status()?
            .json()
            .await
            .context("parse nextNonce")?;
        Ok(resp.nonce)
    }

    pub async fn account_active_orders(
        &self,
        account_index: i64,
        market_id: u32,
        auth: &str,
    ) -> Result<Vec<RemoteOrder>> {
        let resp: AccountActiveOrdersResponse = self
            .http
            .get(self.url("/api/v1/accountActiveOrders"))
            .header("authorization", auth)
            .query(&[
                ("account_index", account_index.to_string()),
                ("market_id", market_id.to_string()),
            ])
            .send()
            .await?
            .error_for_status()?
            .json()
            .await
            .context("parse accountActiveOrders")?;
        if resp.code != 0 && resp.code != 200 { bail!("Lighter active-orders error code {}", resp.code); }
        Ok(resp.orders)
    }

    /// Raw top-of-book via REST (sanity check). Returns the JSON value.
    pub async fn order_book_orders(&self, market_id: u32, limit: u32) -> Result<serde_json::Value> {
        let v: serde_json::Value = self
            .http
            .get(self.url("/api/v1/orderBookOrders"))
            .query(&[
                ("market_id", market_id.to_string()),
                ("limit", limit.to_string()),
            ])
            .send()
            .await?
            .error_for_status()?
            .json()
            .await
            .context("parse orderBookOrders")?;
        Ok(v)
    }

    pub async fn send_tx(&self, tx_type: u8, tx_info: &str) -> Result<TxResponse> {
        let resp = self
            .http
            .post(self.url("/api/v1/sendTx"))
            .form(&[
                ("tx_type", tx_type.to_string()),
                ("tx_info", tx_info.to_string()),
            ])
            .send()
            .await?;
        Self::parse_tx_response(resp).await
    }

    pub async fn send_tx_batch(&self, tx_types: &[u8], tx_infos: &[String]) -> Result<TxResponse> {
        let types_json = serde_json::to_string(tx_types)?;
        let infos_json = serde_json::to_string(tx_infos)?;
        let resp = self
            .http
            .post(self.url("/api/v1/sendTxBatch"))
            .form(&[("tx_types", types_json), ("tx_infos", infos_json)])
            .send()
            .await?;
        Self::parse_tx_response(resp).await
    }

    /// Signed position (base units) for a market via REST — authoritative and independent of
    /// the account WS (so position is never stale even if that WS dies).
    pub async fn account_position(&self, account_index: i64, market_id: u32) -> Result<Decimal> {
        let value = self.account_raw(account_index).await?;
        let account = value.get("accounts").and_then(|v| v.as_array()).and_then(|v| v.first())
            .context("Lighter account response is missing its account row")?;
        let positions = account.get("positions").and_then(|v| v.as_array())
            .context("Lighter account response is missing positions")?;
        for position in positions {
            if position.get("market_id").and_then(|v| v.as_u64()) == Some(market_id as u64) {
                return signed_position_decimal(position.get("position"), position.get("sign").and_then(|v| v.as_i64()));
            }
        }
        Ok(Decimal::ZERO)
    }

    pub async fn account_raw(&self, account_index: i64) -> Result<serde_json::Value> {
        let value: serde_json::Value = self.http.get(self.url("/api/v1/account"))
            .query(&[("by", "index".to_string()), ("value", account_index.to_string())])
            .send().await?.error_for_status()?.json().await.context("parse account")?;
        if !value.get("code").and_then(|code| code.as_i64()).is_some_and(|code| code == 0 || code == 200) {
            bail!("Lighter account query returned an error or missing response code");
        }
        if !value.get("accounts").and_then(|rows| rows.as_array()).is_some_and(|rows| !rows.is_empty()) {
            bail!("Lighter account query has no account row");
        }
        Ok(value)
    }

    /// Parse a sendTx[Batch] response body even when the HTTP status is an error
    /// (the body still carries code/message useful for rejection classification).
    async fn parse_tx_response(resp: reqwest::Response) -> Result<TxResponse> {
        let status = resp.status();
        let text = resp.text().await.unwrap_or_default();
        match serde_json::from_str::<TxResponse>(&text) {
            Ok(tx) => Ok(tx),
            Err(_) if status.is_success() => bail!("Lighter transaction response has no valid outcome: {text}"),
            Err(e) => bail!("tx response {} not JSON: {} ({})", status, text, e),
        }
    }
}

fn value_decimal(v: Option<&serde_json::Value>) -> Option<Decimal> {
    match v {
        Some(serde_json::Value::String(s)) => s.parse::<Decimal>().ok(),
        Some(serde_json::Value::Number(n)) => n.to_string().parse::<Decimal>().ok(),
        _ => None,
    }
}

fn signed_position_decimal(position: Option<&serde_json::Value>, sign: Option<i64>) -> Result<Decimal> {
    let quantity = value_decimal(position).context("invalid Lighter position quantity")?;
    if quantity < Decimal::ZERO { bail!("negative Lighter unsigned position magnitude"); }
    if quantity.is_zero() { return Ok(Decimal::ZERO); }
    match sign {
        Some(1) => Ok(quantity),
        Some(-1) => Ok(-quantity),
        _ => bail!("invalid or missing Lighter position sign"),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use rust_decimal_macros::dec;


    #[tokio::test]
    async fn authenticated_order_reads_use_headers_and_reject_error_envelopes() {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let client = RestClient::new(&format!("http://{}", listener.local_addr().unwrap()), 0).unwrap();
        let server = tokio::spawn(async move {
            let mut requests = Vec::new();
            for body in [
                r#"{"code":200,"orders":[]}"#,
                r#"{"code":200,"orders":[],"next_cursor":"next"}"#,
                r#"{"code":200,"trades":[]}"#,
                r#"{"code":20001,"orders":[]}"#,
            ] {
                let (mut stream, _) = listener.accept().await.unwrap();
                let mut bytes = Vec::new();
                while !bytes.windows(4).any(|part| part == b"\r\n\r\n") {
                    let mut chunk = [0u8; 1024];
                    let count = stream.read(&mut chunk).await.unwrap();
                    assert!(count > 0);
                    bytes.extend_from_slice(&chunk[..count]);
                }
                requests.push(String::from_utf8(bytes).unwrap());
                let response = format!("HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}", body.len());
                stream.write_all(response.as_bytes()).await.unwrap();
            }
            requests
        });
        assert!(client.account_active_orders(7, 24, "test-auth").await.unwrap().is_empty());
        assert_eq!(client.account_inactive_orders(7, 24, "test-auth", Some("page/2")).await.unwrap()["next_cursor"], "next");
        assert!(client.trades_by_order(7, 99, "test-auth", None).await.unwrap()["trades"].as_array().unwrap().is_empty());
        assert!(client.account_active_orders(7, 24, "test-auth").await.is_err());
        let requests = server.await.unwrap();
        for request in &requests {
            assert!(request.to_ascii_lowercase().contains("authorization: test-auth"));
            assert!(!request.lines().next().unwrap().contains("auth="));
        }
        assert!(requests[1].contains("cursor=page%2F2"));
        assert!(requests[2].contains("order_index=99") && requests[2].contains("limit=100"));
    }

    #[test]
    fn position_magnitude_and_sign_are_validated_without_inventing_flat() {
        for value in [serde_json::json!("0.85"), serde_json::json!(0.85)] {
            assert_eq!(signed_position_decimal(Some(&value), Some(1)).unwrap(), dec!(0.85));
            assert_eq!(signed_position_decimal(Some(&value), Some(-1)).unwrap(), dec!(-0.85));
            assert!(signed_position_decimal(Some(&value), None).is_err());
            assert!(signed_position_decimal(Some(&value), Some(0)).is_err());
        }
        assert!(signed_position_decimal(None, Some(1)).is_err());
        assert!(signed_position_decimal(Some(&serde_json::json!("garbage")), Some(1)).is_err());
        assert!(signed_position_decimal(Some(&serde_json::json!("-0.85")), Some(1)).is_err());
        assert_eq!(signed_position_decimal(Some(&serde_json::json!("0")), None).unwrap(), Decimal::ZERO);
    }
}

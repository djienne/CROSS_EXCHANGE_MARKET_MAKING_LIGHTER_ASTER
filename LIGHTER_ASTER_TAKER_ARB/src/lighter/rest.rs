//! Lighter REST client (reqwest). Endpoints + param encodings verified against the SDK:
//!   GET  /api/v1/orderBooks
//!   GET  /api/v1/nextNonce            ?account_index&api_key_index
//!   GET  /api/v1/accountActiveOrders  ?account_index&market_id&auth
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

pub const BASE_URL: &str = "https://mainnet.zklighter.elliot.ai";

#[derive(Clone)]
pub struct RestClient {
    base: String,
    http: reqwest::Client,
}

impl RestClient {
    pub fn new(base_url: &str) -> Result<Self> {
        let http = reqwest::Client::builder()
            .timeout(Duration::from_secs(10))
            .tcp_nodelay(true)
            .pool_max_idle_per_host(0)
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
            .query(&[
                ("account_index", account_index.to_string()),
                ("market_id", market_id.to_string()),
                ("auth", auth.to_string()),
            ])
            .send()
            .await?
            .error_for_status()?
            .json()
            .await
            .context("parse accountActiveOrders")?;
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
        let v = self.account_raw(account_index).await?;
        if let Some(acc) = v
            .get("accounts")
            .and_then(|a| a.as_array())
            .and_then(|a| a.first())
        {
            if let Some(poss) = acc.get("positions").and_then(|p| p.as_array()) {
                for p in poss {
                    if p.get("market_id").and_then(|m| m.as_u64()) == Some(market_id as u64) {
                        let sign = p.get("sign").and_then(|x| x.as_i64()).unwrap_or(1);
                        return Ok(signed_position_decimal(p.get("position"), sign));
                    }
                }
            }
        }
        Ok(Decimal::ZERO)
    }

    pub async fn account_raw(&self, account_index: i64) -> Result<serde_json::Value> {
        self.http
            .get(self.url("/api/v1/account"))
            .query(&[
                ("by", "index".to_string()),
                ("value", account_index.to_string()),
            ])
            .send()
            .await?
            .error_for_status()?
            .json()
            .await
            .context("parse account")
    }

    /// GET /api/v1/getMakerOnlyApiKeys (maker-only restriction detection).
    pub async fn maker_only_api_keys(&self, account_index: i64) -> Result<serde_json::Value> {
        let v: serde_json::Value = self
            .http
            .get(self.url("/api/v1/getMakerOnlyApiKeys"))
            .query(&[("account_index", account_index.to_string())])
            .send()
            .await?
            .error_for_status()?
            .json()
            .await
            .context("parse getMakerOnlyApiKeys")?;
        Ok(v)
    }

    /// Parse a sendTx[Batch] response body even when the HTTP status is an error
    /// (the body still carries code/message useful for rejection classification).
    async fn parse_tx_response(resp: reqwest::Response) -> Result<TxResponse> {
        let status = resp.status();
        let text = resp.text().await.unwrap_or_default();
        match serde_json::from_str::<TxResponse>(&text) {
            Ok(tx) => Ok(tx),
            Err(_) if status.is_success() => Ok(TxResponse::default()),
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

fn signed_position_decimal(position: Option<&serde_json::Value>, sign: i64) -> Decimal {
    let mag = value_decimal(position).unwrap_or(Decimal::ZERO).abs();
    let signed = if sign < 0 { -mag } else { mag };
    signed.normalize()
}

#[cfg(test)]
mod tests {
    use super::*;
    use rust_decimal_macros::dec;

    #[test]
    fn signed_position_decimal_parses_string_without_float_tail() {
        let value = serde_json::json!("0.850000");
        assert_eq!(signed_position_decimal(Some(&value), 1), dec!(0.85));
        assert_eq!(signed_position_decimal(Some(&value), -1), dec!(-0.85));
    }

    #[test]
    fn signed_position_decimal_parses_json_number_without_float_tail() {
        let value = serde_json::json!(0.85);
        assert_eq!(signed_position_decimal(Some(&value), 1).to_string(), "0.85");
    }

    #[test]
    fn signed_position_decimal_missing_is_zero() {
        assert_eq!(signed_position_decimal(None, 1), Decimal::ZERO);
    }
}

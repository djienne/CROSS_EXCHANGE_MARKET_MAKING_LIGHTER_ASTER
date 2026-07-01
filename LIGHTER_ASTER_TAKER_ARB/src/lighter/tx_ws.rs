//! Persistent tx WebSocket — port of `TxWebSocket`. Sends `jsonapi/sendtxbatch` frames
//! (excluded from the 200 msg/min limit). Preserves the critical outcome semantics:
//!   * NotSent  — no frame written; REST fallback is SAFE.
//!   * Unknown  — a frame may have reached Lighter; caller must NOT retry (pause+reconcile).
//!   * Ok/Rejected — definite outcome from the response code (200 normalized to 0).
//!
//! KEEPALIVE (matches the Python `TxWebSocket`): the connection runs a **continuous background
//! recv loop** plus a **proactive pinger**. Lighter closes any connection that sends no frame
//! for 2 minutes and disconnects clients that fall behind on reading
//! (https://apidocs.lighter.xyz/docs/websocket-reference), so an idle connection that is only
//! read inline during a send (the previous design) gets dropped during quiet periods / the warmup
//! window — surfacing as `disconnected_after_send` Unknown outcomes. Here:
//!   * the recv loop ALWAYS reads (so tungstenite flushes auto-pongs, we reply `{"type":"pong"}`
//!     to Lighter's app-level `{"type":"ping"}`, and we never "fall behind on reading"), and
//!   * the pinger sends a WS Ping every `PING_INTERVAL` (< the 2-min server timeout).
//! Sends are serialized upstream (the sign+send Mutex) and a response is correlated 1:1 via the
//! recv channel (single in-flight request).

use crate::types::{TxSendResult, TxSendStatus};
use anyhow::{Context, Result};
use futures_util::stream::{SplitSink, SplitStream};
use futures_util::{SinkExt, StreamExt};
use serde_json::Value;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Duration;
use tokio::net::TcpStream;
use tokio::sync::{mpsc, Mutex};
use tokio::task::JoinHandle;
use tokio::time::timeout;
use tokio_tungstenite::tungstenite::Message;
use tokio_tungstenite::{connect_async, MaybeTlsStream, WebSocketStream};

type Ws = WebSocketStream<MaybeTlsStream<TcpStream>>;
type WsSink = SplitSink<Ws, Message>;
type WsStream = SplitStream<Ws>;

/// Proactive client-ping interval. Lighter closes connections idle for 2 minutes, so this must
/// be comfortably under 120s (the Python uses `ping_interval=20`).
const PING_INTERVAL: Duration = Duration::from_secs(20);

/// Bound on any single sink write. A half-open TCP connection with a full send buffer
/// otherwise blocks `send()` until the OS gives up (potentially 10+ minutes) — while the
/// hedge-submission path waits behind this socket. On timeout the connection is marked
/// dead and the outcome is Unknown (the frame may or may not have left the socket).
const WRITE_TIMEOUT: Duration = Duration::from_secs(3);
/// Max wait for a tx response after the frame is written before declaring the outcome Unknown.
const RESPONSE_TIMEOUT: Duration = Duration::from_secs(10);

/// One live connection: the shared write half plus the response channel fed by the recv loop,
/// and the background task handles (aborted on reconnect/close).
struct Conn {
    /// Write half, shared between `send_batch`, the pinger, and the recv loop's pong replies.
    write: Arc<Mutex<WsSink>>,
    /// Real (non-ping / non-info) responses routed from the recv loop, in order. Single in-flight
    /// request upstream, so the next item after a send is that send's response.
    resp_rx: mpsc::UnboundedReceiver<Value>,
    /// False once the recv loop or pinger observes a dead socket.
    alive: Arc<AtomicBool>,
    recv_task: JoinHandle<()>,
    ping_task: JoinHandle<()>,
}

impl Conn {
    fn is_alive(&self) -> bool {
        self.alive.load(Ordering::Acquire)
    }
}

impl Drop for Conn {
    fn drop(&mut self) {
        self.recv_task.abort();
        self.ping_task.abort();
    }
}

pub struct TxWebSocket {
    url: String,
    conn: Mutex<Option<Conn>>,
}

impl TxWebSocket {
    pub fn new(url: &str) -> Self {
        Self {
            url: url.to_string(),
            conn: Mutex::new(None),
        }
    }

    /// Pre-connect (best effort). Safe to call at startup.
    pub async fn connect(&self) -> Result<()> {
        let conn = self.open().await?;
        *self.conn.lock().await = Some(conn);
        Ok(())
    }

    /// Open a fresh connection, split it, and spawn the recv + ping background tasks.
    async fn open(&self) -> Result<Conn> {
        let (ws, _) = connect_async(&self.url).await.context("tx ws connect")?;
        let (sink, stream) = ws.split();
        let write = Arc::new(Mutex::new(sink));
        let alive = Arc::new(AtomicBool::new(true));
        let (resp_tx, resp_rx) = mpsc::unbounded_channel::<Value>();

        let recv_task = tokio::spawn(recv_loop(stream, write.clone(), alive.clone(), resp_tx));
        let ping_task = tokio::spawn(ping_loop(write.clone(), alive.clone()));

        tracing::info!(
            "TxWebSocket connected to {} (keepalive recv-loop + {}s pinger)",
            self.url,
            PING_INTERVAL.as_secs()
        );
        Ok(Conn {
            write,
            resp_rx,
            alive,
            recv_task,
            ping_task,
        })
    }

    fn response_payload(resp: &Value) -> &Value {
        resp.get("data")
            .and_then(|v| v.as_object().map(|_| v))
            .unwrap_or(resp)
    }

    /// Extract (code, message) from a frame. Returns `None` when the frame carries NO
    /// recognizable outcome field (`error`/`code`/`status_code`) — such a frame must map
    /// to Unknown, never default to code 0 (= Ok): treating an unrecognized frame as a
    /// success would track a possibly-rejected order as resting.
    fn code_message(resp: &Value) -> Option<(i64, String)> {
        let payload = Self::response_payload(resp);
        if let Some(err) = payload.get("error") {
            if let Some(obj) = err.as_object() {
                let code = obj.get("code").and_then(|c| c.as_i64()).unwrap_or(-1);
                let msg = Self::extract_message(obj.get("message"));
                return Some((if code == 200 { 0 } else { code }, msg));
            } else if !err.is_null() {
                return Some((-1, err.to_string()));
            }
        }
        let code = payload
            .get("code")
            .or_else(|| payload.get("status_code"))
            .and_then(|c| c.as_i64())?;
        let msg = Self::extract_message(payload.get("message"));
        Some((if code == 200 { 0 } else { code }, msg))
    }

    /// A frame counts as THE tx outcome only if it structurally looks like one: the
    /// observed success shape always carries `code` (200) and rejects carry `code`/`error`.
    /// Anything else (new informational frame types, notices) must not be consumed as the
    /// in-flight request's outcome.
    fn looks_like_tx_outcome(v: &Value) -> bool {
        if v.get("type")
            .and_then(|t| t.as_str())
            .is_some_and(|t| t.contains("sendtx"))
        {
            return true;
        }
        let payload = Self::response_payload(v);
        payload.get("code").is_some()
            || payload.get("status_code").is_some()
            || payload.get("error").is_some_and(|e| !e.is_null())
    }

    /// Extract a message field as its RAW string content (empty `""` stays empty, NOT `"\"\""`) so
    /// the reject classifier and empty-message code-fallback work (codex).
    fn extract_message(v: Option<&Value>) -> String {
        match v {
            Some(Value::String(s)) => s.clone(),
            Some(other) if !other.is_null() => other.to_string(),
            _ => String::new(),
        }
    }

    /// Send a batch. `tx_types`/`tx_infos` are JSON-encoded into strings inside `data`.
    pub async fn send_batch(&self, tx_types: &[u8], tx_infos: &[String]) -> TxSendResult {
        let mut guard = self.conn.lock().await;

        // (Re)connect if there is no live connection.
        if !guard.as_ref().map(|c| c.is_alive()).unwrap_or(false) {
            match self.open().await {
                Ok(c) => *guard = Some(c),
                Err(_) => return TxSendResult::not_sent("connect_failed"),
            }
        }
        let conn = guard.as_mut().unwrap();

        let frame = serde_json::json!({
            "type": "jsonapi/sendtxbatch",
            "data": {
                "tx_types": serde_json::to_string(tx_types).unwrap_or_default(),
                "tx_infos": serde_json::to_string(tx_infos).unwrap_or_default(),
            }
        })
        .to_string();

        // Drop any stale responses left over from a previous send before issuing this one.
        while conn.resp_rx.try_recv().is_ok() {}

        // Write the frame, bounded. A write error/timeout means the frame may or may not
        // have reached the exchange — treat as Unknown (the safe, no-retry outcome).
        {
            let mut w = conn.write.lock().await;
            match timeout(WRITE_TIMEOUT, w.send(Message::Text(frame))).await {
                Ok(Ok(())) => {}
                Ok(Err(e)) => {
                    conn.alive.store(false, Ordering::Release);
                    return TxSendResult::unknown(format!("send_failed:{e}"));
                }
                Err(_) => {
                    conn.alive.store(false, Ordering::Release);
                    return TxSendResult::unknown("send_timeout");
                }
            }
        }

        // Await the response routed by the recv loop (single in-flight request).
        match timeout(RESPONSE_TIMEOUT, conn.resp_rx.recv()).await {
            Ok(Some(resp)) => {
                let Some((code, message)) = Self::code_message(&resp) else {
                    // Outcome-shaped gate passed but no code/error field: ambiguous.
                    return TxSendResult::unknown("unrecognized_response");
                };
                let payload = Self::response_payload(&resp);
                let quota = payload
                    .get("volume_quota_remaining")
                    .and_then(|q| q.as_i64());
                let status = if code == 0 {
                    TxSendStatus::Ok
                } else {
                    TxSendStatus::Rejected
                };
                TxSendResult {
                    status,
                    code,
                    message,
                    quota_remaining: quota,
                }
            }
            // recv loop ended (socket closed) — a frame was written, outcome unknown.
            Ok(None) => {
                conn.alive.store(false, Ordering::Release);
                TxSendResult::unknown("disconnected_after_send")
            }
            Err(_) => {
                conn.alive.store(false, Ordering::Release);
                TxSendResult::unknown("response_timeout")
            }
        }
    }
}

/// Background reader: drains the socket forever, replies to Lighter's app-level `{"type":"ping"}`,
/// drops info frames, and routes everything else to `resp_tx`. Exits (dropping `resp_tx`, which
/// unblocks a waiting `send_batch` with `None`) when the socket closes/errors.
async fn recv_loop(
    mut stream: WsStream,
    write: Arc<Mutex<WsSink>>,
    alive: Arc<AtomicBool>,
    resp_tx: mpsc::UnboundedSender<Value>,
) {
    while let Some(msg) = stream.next().await {
        match msg {
            Ok(Message::Text(t)) => {
                let v: Value = match serde_json::from_str(&t) {
                    Ok(v) => v,
                    Err(_) => continue,
                };
                match v.get("type").and_then(|x| x.as_str()) {
                    Some("ping") => {
                        // Lighter application-level ping -> must reply with a pong frame.
                        let mut w = write.lock().await;
                        match timeout(WRITE_TIMEOUT, w.send(Message::Text(r#"{"type":"pong"}"#.into()))).await {
                            Ok(Ok(())) => {}
                            _ => break, // write failed or wedged: connection is dead
                        }
                    }
                    Some("connected") | Some("subscribed") => {} // informational; drop
                    _ if TxWebSocket::looks_like_tx_outcome(&v) => {
                        // Real response (tx outcome). Channel closed => no receiver; stop.
                        if resp_tx.send(v).is_err() {
                            break;
                        }
                    }
                    other => {
                        // NOT outcome-shaped: never consume it as the in-flight request's
                        // response (that would report a possibly-rejected order as Ok).
                        tracing::debug!("tx socket: dropping non-outcome frame type={other:?}");
                    }
                }
            }
            // Tungstenite auto-queues a Pong for an incoming Ping (flushed on the next write); we
            // also reply explicitly to be safe. Pongs (from our pings) are discarded.
            Ok(Message::Ping(p)) => {
                let mut w = write.lock().await;
                if timeout(WRITE_TIMEOUT, w.send(Message::Pong(p))).await.is_err() {
                    break; // write wedged: connection is dead
                }
            }
            Ok(Message::Pong(_)) => {}
            Ok(Message::Close(_)) => break,
            Ok(_) => {} // Binary/Frame — unused
            Err(_) => break,
        }
    }
    alive.store(false, Ordering::Release);
    // resp_tx drops here -> any send_batch awaiting resp_rx.recv() gets None (Unknown).
}

/// Background pinger: sends a WS Ping every `PING_INTERVAL` so the connection keeps emitting a
/// client frame well within Lighter's 2-minute idle-close window. Exits when the socket dies.
async fn ping_loop(write: Arc<Mutex<WsSink>>, alive: Arc<AtomicBool>) {
    let mut tick = tokio::time::interval(PING_INTERVAL);
    tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    tick.tick().await; // first tick fires immediately; skip it (just connected)
    loop {
        tick.tick().await;
        if !alive.load(Ordering::Acquire) {
            break;
        }
        let mut w = write.lock().await;
        match timeout(WRITE_TIMEOUT, w.send(Message::Ping(Vec::new()))).await {
            Ok(Ok(())) => {}
            _ => {
                alive.store(false, Ordering::Release);
                break;
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::TxSendStatus;
    use tokio::net::TcpListener;
    use tokio::time::timeout;
    use tokio_tungstenite::accept_async;

    #[tokio::test]
    async fn send_batch_drains_info_replies_to_app_ping_and_routes_response() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let url = format!("ws://{}", listener.local_addr().unwrap());
        let server = tokio::spawn(async move {
            let (stream, _) = listener.accept().await.unwrap();
            let mut ws = accept_async(stream).await.unwrap();

            ws.send(Message::Text(r#"{"type":"connected"}"#.into()))
                .await
                .unwrap();
            ws.send(Message::Text(r#"{"type":"ping"}"#.into()))
                .await
                .unwrap();

            let mut saw_pong = false;
            let mut frame = None;
            for _ in 0..2 {
                let msg = timeout(Duration::from_secs(2), ws.next())
                    .await
                    .unwrap()
                    .unwrap()
                    .unwrap();
                let Message::Text(text) = msg else {
                    panic!("expected text frame");
                };
                if text == r#"{"type":"pong"}"# {
                    saw_pong = true;
                } else {
                    frame = Some(serde_json::from_str::<Value>(&text).unwrap());
                }
            }
            assert!(saw_pong);
            let frame = frame.expect("sendtxbatch frame");
            assert_eq!(
                frame.get("type").and_then(|v| v.as_str()),
                Some("jsonapi/sendtxbatch")
            );
            assert_eq!(
                frame.pointer("/data/tx_types").and_then(|v| v.as_str()),
                Some("[14]")
            );
            assert_eq!(
                frame.pointer("/data/tx_infos").and_then(|v| v.as_str()),
                Some(r#"["signed-tx"]"#)
            );

            ws.send(Message::Text(
                r#"{"code":200,"message":"","volume_quota_remaining":42}"#.into(),
            ))
            .await
            .unwrap();
        });

        let tx_ws = TxWebSocket::new(&url);
        let result = tx_ws.send_batch(&[14], &[String::from("signed-tx")]).await;

        assert_eq!(result.status, TxSendStatus::Ok);
        assert_eq!(result.code, 0);
        assert_eq!(result.quota_remaining, Some(42));
        server.await.unwrap();
    }

    #[tokio::test]
    async fn send_batch_reports_unknown_if_server_closes_after_write() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let url = format!("ws://{}", listener.local_addr().unwrap());
        let server = tokio::spawn(async move {
            let (stream, _) = listener.accept().await.unwrap();
            let mut ws = accept_async(stream).await.unwrap();
            let frame = timeout(Duration::from_secs(2), ws.next())
                .await
                .unwrap()
                .unwrap()
                .unwrap();
            assert!(matches!(frame, Message::Text(_)));
            ws.close(None).await.unwrap();
        });

        let tx_ws = TxWebSocket::new(&url);
        let result = tx_ws.send_batch(&[14], &[String::from("signed-tx")]).await;

        assert_eq!(result.status, TxSendStatus::Unknown);
        assert_eq!(result.message, "disconnected_after_send");
        server.await.unwrap();
    }

    #[test]
    fn code_message_reads_nested_data_payload() {
        let ok = serde_json::json!({
            "type": "jsonapi/sendtxbatch",
            "data": {"code": 200, "message": "", "volume_quota_remaining": 7}
        });
        assert_eq!(TxWebSocket::code_message(&ok), Some((0, String::new())));
        assert_eq!(
            TxWebSocket::response_payload(&ok)
                .get("volume_quota_remaining")
                .and_then(|q| q.as_i64()),
            Some(7)
        );

        let reject = serde_json::json!({
            "type": "jsonapi/sendtxbatch",
            "data": {"code": 42, "message": "bad nonce"}
        });
        assert_eq!(
            TxWebSocket::code_message(&reject),
            Some((42, "bad nonce".to_string()))
        );
    }

    #[test]
    fn frames_without_outcome_fields_are_not_tx_outcomes() {
        // A frame with no code/status_code/error must neither pass the recv-loop gate nor
        // default to code 0 (= Ok) — an unsolicited notice consumed as a success would
        // track a possibly-rejected order as resting.
        let notice = serde_json::json!({"type": "notice", "data": {"info": "maintenance"}});
        assert!(!TxWebSocket::looks_like_tx_outcome(&notice));
        assert_eq!(TxWebSocket::code_message(&notice), None);

        let success = serde_json::json!({"code": 200, "message": "", "volume_quota_remaining": 42});
        assert!(TxWebSocket::looks_like_tx_outcome(&success));

        let err_only = serde_json::json!({"error": {"code": 7, "message": "nope"}});
        assert!(TxWebSocket::looks_like_tx_outcome(&err_only));
        assert_eq!(TxWebSocket::code_message(&err_only), Some((7, "nope".to_string())));
    }
}

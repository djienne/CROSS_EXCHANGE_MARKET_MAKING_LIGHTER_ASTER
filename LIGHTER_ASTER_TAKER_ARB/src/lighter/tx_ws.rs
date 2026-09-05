//! Lighter transaction socket. Handshakes/reconnection run in a cold task.
//! The send path never connects: an unready connection returns NotSent immediately.
//! A write/response failure is Unknown because the venue may already have executed it.

use anyhow::{Context, Result};
use futures_util::{stream::{SplitSink, SplitStream}, SinkExt, StreamExt};
use serde_json::Value;
use std::sync::{atomic::{AtomicBool, Ordering}, Arc, RwLock};
use std::time::Duration;
use tokio::net::TcpStream;
use tokio::sync::{mpsc, Mutex, Notify};
use tokio::task::JoinHandle;
use tokio::time::timeout;
use tokio_tungstenite::{connect_async, MaybeTlsStream, WebSocketStream};
use tokio_tungstenite::tungstenite::Message;
use crate::types::{TxSendResult, TxSendStatus};

type Ws = WebSocketStream<MaybeTlsStream<TcpStream>>;
type WsSink = SplitSink<Ws, Message>;
type WsStream = SplitStream<Ws>;
const PING_INTERVAL: Duration = Duration::from_secs(20);
const WRITE_TIMEOUT: Duration = Duration::from_secs(3);
const RESPONSE_TIMEOUT: Duration = Duration::from_secs(10);
const CONNECT_TIMEOUT: Duration = Duration::from_secs(3);
const READ_IDLE_TIMEOUT: Duration = Duration::from_secs(60);

struct Conn {
    write: Arc<Mutex<WsSink>>,
    resp_rx: mpsc::UnboundedReceiver<Value>,
    alive: Arc<AtomicBool>,
    recv_task: JoinHandle<()>,
    ping_task: JoinHandle<()>,
}
impl Conn {
    fn is_alive(&self) -> bool { self.alive.load(Ordering::Acquire) }
}
impl Drop for Conn {
    fn drop(&mut self) {
        self.recv_task.abort();
        self.ping_task.abort();
    }
}

struct State {
    url: String,
    conn: Mutex<Option<Conn>>,
    // Publish the CURRENT connection's own flag, so an old reader cannot invalidate
    // a newer connection. The hot read is try_read and never waits behind a writer.
    published_alive: RwLock<Option<Arc<AtomicBool>>>,
    reconnect: Arc<Notify>,
}
impl State {
    fn is_ready(&self) -> bool {
        self.published_alive.try_read().ok().is_some_and(|guard|
            guard.as_ref().is_some_and(|alive| alive.load(Ordering::Acquire)))
    }
}

pub struct TxWebSocket {
    inner: Arc<State>,
    reconnect_task: std::sync::Mutex<Option<JoinHandle<()>>>,
}
impl Drop for TxWebSocket {
    fn drop(&mut self) {
        if let Some(task) = self.reconnect_task.get_mut().unwrap_or_else(|e| e.into_inner()).take() {
            task.abort();
        }
    }
}

impl TxWebSocket {
    pub fn new(url: &str) -> Self {
        Self {
            inner: Arc::new(State {
                url: url.into(), conn: Mutex::new(None),
                published_alive: RwLock::new(None), reconnect: Arc::new(Notify::new()),
            }),
            reconnect_task: std::sync::Mutex::new(None),
        }
    }

    pub fn is_ready(&self) -> bool { self.inner.is_ready() }

    pub fn request_reconnect(&self) {
        self.inner.reconnect.notify_one();
    }

    /// Cold startup entry point. Starts the reconnect task once and bounds readiness.
    pub async fn connect(&self) -> Result<()> {
        {
            let mut task = self.reconnect_task.lock().unwrap_or_else(|e| e.into_inner());
            if task.is_none() {
                *task = Some(tokio::spawn(Self::reconnect_loop(self.inner.clone())));
            }
        }
        self.request_reconnect();
        timeout(CONNECT_TIMEOUT, async {
            while !self.is_ready() {
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        }).await.context("Lighter tx websocket connect deadline")?;
        Ok(())
    }

    async fn reconnect_loop(state: Arc<State>) {
        let mut backoff = Duration::from_millis(250);
        loop {
            if state.is_ready() {
                tokio::select! {
                    _ = state.reconnect.notified() => {}
                    _ = tokio::time::sleep(Duration::from_secs(1)) => {}
                }
                if state.is_ready() { continue; }
            }
            let mut guard = state.conn.lock().await;
            if guard.as_ref().is_some_and(Conn::is_alive) { continue; }
            *guard = None; // drop old readers before publishing a replacement
            *state.published_alive.write().unwrap_or_else(|e| e.into_inner()) = None;
            match timeout(CONNECT_TIMEOUT, Self::open(&state.url, state.reconnect.clone())).await {
                Ok(Ok(conn)) => {
                    let alive = conn.alive.clone();
                    *guard = Some(conn);
                    *state.published_alive.write().unwrap_or_else(|e| e.into_inner()) = Some(alive);
                    backoff = Duration::from_millis(250);
                    continue;
                }
                Ok(Err(error)) => tracing::warn!("Lighter tx reconnect failed: {error:#}"),
                Err(_) => tracing::warn!("Lighter tx reconnect exceeded 3 seconds"),
            }
            drop(guard);
            tokio::time::sleep(backoff).await;
            backoff = (backoff * 2).min(Duration::from_secs(10));
        }
    }

    async fn open(url: &str, reconnect: Arc<Notify>) -> Result<Conn> {
        let (ws, _) = connect_async(url).await.context("tx ws connect")?;
        let (sink, stream) = ws.split();
        let write = Arc::new(Mutex::new(sink));
        let alive = Arc::new(AtomicBool::new(true));
        let (resp_tx, resp_rx) = mpsc::unbounded_channel();
        let recv_task = tokio::spawn(recv_loop(stream, write.clone(), alive.clone(), resp_tx, reconnect.clone()));
        let ping_task = tokio::spawn(ping_loop(write.clone(), alive.clone(), reconnect));
        Ok(Conn { write, resp_rx, alive, recv_task, ping_task })
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

    pub async fn send_batch(&self, tx_types: &[u8], tx_infos: &[String]) -> TxSendResult {
        if !self.is_ready() {
            self.request_reconnect();
            return TxSendResult::not_sent("transport_not_ready");
        }
        let mut guard = match self.inner.conn.try_lock() {
            Ok(guard) => guard,
            Err(_) => return TxSendResult::not_sent("transport_busy"),
        };
        let Some(conn) = guard.as_mut().filter(|conn| conn.is_alive()) else {
            self.request_reconnect();
            return TxSendResult::not_sent("transport_not_ready");
        };
        let frame = serde_json::json!({
            "type": "jsonapi/sendtxbatch",
            "data": {
                "tx_types": serde_json::to_string(tx_types).expect("serialize tx types"),
                "tx_infos": serde_json::to_string(tx_infos).expect("serialize signed tx strings"),
            }
        }).to_string();
        while conn.resp_rx.try_recv().is_ok() {}
        {
            // The pinger/reader hold this only for bounded writes.
            let result = timeout(WRITE_TIMEOUT, async {
                conn.write.lock().await.send(Message::Text(frame)).await
            }).await;
            if !matches!(result, Ok(Ok(()))) {
                conn.alive.store(false, Ordering::Release);
                self.request_reconnect();
                return TxSendResult::unknown("send_failed_or_timeout");
            }
        }
        match timeout(RESPONSE_TIMEOUT, conn.resp_rx.recv()).await {
            Ok(Some(response)) => {
                let Some((code, message)) = Self::code_message(&response) else {
                    conn.alive.store(false, Ordering::Release);
                    self.request_reconnect();
                    return TxSendResult::unknown("unrecognized_response");
                };
                TxSendResult {
                    status: if code == 0 { TxSendStatus::Ok } else { TxSendStatus::Rejected },
                    code, message,
                    quota_remaining: Self::response_payload(&response)
                        .get("volume_quota_remaining").and_then(|v| v.as_i64()),
                }
            }
            outcome => {
                conn.alive.store(false, Ordering::Release);
                self.request_reconnect();
                TxSendResult::unknown(if outcome.is_err() { "response_timeout" } else { "disconnected_after_send" })
            }
        }
    }
}

async fn recv_loop(
    mut stream: WsStream, write: Arc<Mutex<WsSink>>, alive: Arc<AtomicBool>,
    resp_tx: mpsc::UnboundedSender<Value>, reconnect: Arc<Notify>,
) {
    loop {
        let message = match timeout(READ_IDLE_TIMEOUT, stream.next()).await {
            Ok(Some(Ok(message))) => message,
            _ => break,
        };
        match message {
            Message::Text(text) => {
                let Ok(value) = serde_json::from_str::<Value>(&text) else { continue; };
                match value.get("type").and_then(|v| v.as_str()) {
                    Some("ping") => {
                        let result = timeout(WRITE_TIMEOUT, async {
                            write.lock().await.send(Message::Text(r#"{"type":"pong"}"#.into())).await
                        }).await;
                        if !matches!(result, Ok(Ok(()))) { break; }
                    }
                    Some("connected" | "subscribed") => {}
                    _ if TxWebSocket::looks_like_tx_outcome(&value) => {
                        if resp_tx.send(value).is_err() { break; }
                    }
                    _ => {}
                }
            }
            Message::Ping(payload) => {
                let result = timeout(WRITE_TIMEOUT, async {
                    write.lock().await.send(Message::Pong(payload)).await
                }).await;
                if !matches!(result, Ok(Ok(()))) { break; }
            }
            Message::Close(_) => break,
            _ => {}
        }
    }
    alive.store(false, Ordering::Release);
    reconnect.notify_one();
}

async fn ping_loop(write: Arc<Mutex<WsSink>>, alive: Arc<AtomicBool>, reconnect: Arc<Notify>) {
    let mut tick = tokio::time::interval(PING_INTERVAL);
    tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    tick.tick().await;
    while alive.load(Ordering::Acquire) {
        tick.tick().await;
        let result = timeout(WRITE_TIMEOUT, async {
            write.lock().await.send(Message::Ping(Vec::new())).await
        }).await;
        if !matches!(result, Ok(Ok(()))) {
            alive.store(false, Ordering::Release);
            reconnect.notify_one();
            break;
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
        tx_ws.connect().await.unwrap();
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
        tx_ws.connect().await.unwrap();
        let result = tx_ws.send_batch(&[14], &[String::from("signed-tx")]).await;

        assert_eq!(result.status, TxSendStatus::Unknown);
        assert_eq!(result.message, "disconnected_after_send");
        server.await.unwrap();
    }

    #[tokio::test]
    async fn cold_connect_is_bounded_and_hot_unready_send_never_connects() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let url = format!("ws://{}", listener.local_addr().unwrap());
        let socket = TxWebSocket::new(&url);
        let before = tokio::time::Instant::now();
        let result = socket.send_batch(&[14], &["signed-tx".into()]).await;
        assert_eq!(result.status, TxSendStatus::NotSent);
        assert!(before.elapsed() < Duration::from_millis(50));
        assert!(timeout(Duration::from_secs(4), socket.connect()).await.unwrap().is_err());
        drop(listener);
    }

    #[tokio::test]
    async fn recv_loop_flags_half_open_socket_via_idle_watchdog() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let url = format!("ws://{}", listener.local_addr().unwrap());
        // Server: complete the WS handshake, then go silent forever — never reads
        // (so no auto-pongs) and never writes. Half-open from the client's view.
        let server = tokio::spawn(async move {
            let (stream, _) = listener.accept().await.unwrap();
            let _ws = accept_async(stream).await.unwrap();
            std::future::pending::<()>().await
        });

        let tx_ws = TxWebSocket::new(&url);
        // Connect under the REAL clock: a paused clock auto-advances through
        // CONNECT_TIMEOUT while the (real) local handshake I/O is still pending.
        tx_ws.connect().await.unwrap();
        // Then pause so auto-advance rushes through the 60s idle window instead of
        // the test waiting it out.
        tokio::time::pause();
        let old_alive = tx_ws.inner.conn.lock().await.as_ref().unwrap().alive.clone();
        assert!(old_alive.load(Ordering::Acquire));

        // The paused clock auto-advances through READ_IDLE_TIMEOUT; poll until the
        // watchdog flips `alive` (bounded so a regression fails instead of hanging).
        let deadline = tokio::time::Instant::now() + READ_IDLE_TIMEOUT * 3;
        loop {
            if !old_alive.load(Ordering::Acquire) {
                break;
            }
            assert!(
                tokio::time::Instant::now() < deadline,
                "idle watchdog never flipped alive=false"
            );
            tokio::time::sleep(Duration::from_millis(250)).await;
        }
        server.abort();
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

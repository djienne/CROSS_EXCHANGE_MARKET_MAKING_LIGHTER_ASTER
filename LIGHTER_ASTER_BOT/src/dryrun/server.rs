//! A minimal HTTP/1.1 server with WebSocket upgrade: just enough for the bot's own clients
//! (reqwest, tokio-tungstenite and the Lighter signer library) to reach the simulated venues on
//! loopback. Keep-alive and Content-Length bodies only; no TLS, no compression.

use std::collections::BTreeMap;
use std::future::Future;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;

use anyhow::{bail, Context, Result};
use tokio::io::{AsyncBufReadExt, AsyncReadExt, AsyncWriteExt, BufReader};
use tokio::net::{TcpListener, TcpStream};
use tokio_tungstenite::tungstenite::handshake::derive_accept_key;
use tokio_tungstenite::tungstenite::protocol::Role;
use tokio_tungstenite::WebSocketStream;

#[derive(Debug, Clone, Default)]
pub struct Request {
    pub method: String,
    pub path: String,
    /// Raw query string, without the `?`.
    pub query: String,
    pub headers: Vec<(String, String)>,
    pub body: Vec<u8>,
}

impl Request {
    pub fn header(&self, name: &str) -> Option<&str> {
        self.headers.iter().find(|(key, _)| key.eq_ignore_ascii_case(name)).map(|(_, value)| value.as_str())
    }

    /// Decoded query-string and form-body parameters (clients put them in either).
    pub fn params(&self) -> BTreeMap<String, String> {
        let mut params = form_pairs(&self.query);
        let form = self.header("content-type").is_some_and(|t| t.starts_with("application/x-www-form-urlencoded"));
        if form {
            params.extend(form_pairs(&String::from_utf8_lossy(&self.body)));
        }
        params
    }
}

fn form_pairs(raw: &str) -> BTreeMap<String, String> {
    reqwest::Url::parse(&format!("http://form/?{raw}"))
        .map(|url| url.query_pairs().into_owned().collect())
        .unwrap_or_default()
}

#[derive(Debug, Clone, PartialEq)]
pub struct Response {
    pub status: u16,
    pub body: String,
}

impl Response {
    pub fn json(status: u16, body: impl Into<String>) -> Self {
        Self { status, body: body.into() }
    }
}

pub trait Handler: Send + Sync + 'static {
    /// Answers one REST request that arrived on connection `lane`.
    fn rest(&self, lane: u64, request: Request) -> impl Future<Output = Response> + Send;
    /// Runs one upgraded websocket connection until it closes.
    fn websocket(&self, lane: u64, request: Request, ws: WebSocketStream<TcpStream>) -> impl Future<Output = ()> + Send;
}

/// Connection ids, unique across both venues: each connection is one lane, answered in order.
static LANES: AtomicU64 = AtomicU64::new(1);

pub async fn serve<H: Handler>(listener: TcpListener, handler: Arc<H>) {
    loop {
        match listener.accept().await {
            Ok((stream, _)) => {
                let _ = stream.set_nodelay(true);
                let handler = handler.clone();
                let lane = LANES.fetch_add(1, Ordering::Relaxed);
                tokio::spawn(async move {
                    if let Err(e) = connection(stream, lane, handler).await {
                        tracing::debug!(lane, "dry-run connection closed: {e:#}");
                    }
                });
            }
            Err(e) => {
                tracing::warn!("dry-run accept failed: {e}");
                tokio::time::sleep(Duration::from_millis(100)).await;
            }
        }
    }
}

async fn connection<H: Handler>(stream: TcpStream, lane: u64, handler: Arc<H>) -> Result<()> {
    let mut reader = BufReader::new(stream);
    while let Some(request) = read_request(&mut reader).await? {
        if request.header("upgrade").is_some_and(|v| v.eq_ignore_ascii_case("websocket")) {
            let key = request.header("sec-websocket-key").context("websocket upgrade without a key")?;
            let head = format!(
                "HTTP/1.1 101 Switching Protocols\r\nUpgrade: websocket\r\nConnection: Upgrade\r\nSec-WebSocket-Accept: {}\r\n\r\n",
                derive_accept_key(key.as_bytes())
            );
            reader.get_mut().write_all(head.as_bytes()).await?;
            let early = reader.buffer().to_vec();
            let ws = WebSocketStream::from_partially_read(reader.into_inner(), early, Role::Server, None).await;
            handler.websocket(lane, request, ws).await;
            return Ok(());
        }
        let close = request.header("connection").is_some_and(|v| v.eq_ignore_ascii_case("close"));
        let response = handler.rest(lane, request).await;
        let reason = match response.status {
            200 => "OK",
            400 => "Bad Request",
            404 => "Not Found",
            429 => "Too Many Requests",
            503 => "Service Unavailable",
            _ => "Status",
        };
        let head = format!(
            "HTTP/1.1 {} {reason}\r\nContent-Type: application/json\r\nContent-Length: {}\r\n\r\n",
            response.status,
            response.body.len()
        );
        let out = reader.get_mut();
        out.write_all(head.as_bytes()).await?;
        out.write_all(response.body.as_bytes()).await?;
        if close {
            break;
        }
    }
    Ok(())
}

async fn read_request(reader: &mut BufReader<TcpStream>) -> Result<Option<Request>> {
    let mut line = String::new();
    if reader.read_line(&mut line).await? == 0 {
        return Ok(None);
    }
    let mut parts = line.split_whitespace();
    let (Some(method), Some(target)) = (parts.next(), parts.next()) else { bail!("bad request line {line:?}") };
    let (path, query) = target.split_once('?').unwrap_or((target, ""));
    let mut request = Request { method: method.into(), path: path.into(), query: query.into(), ..Default::default() };
    loop {
        line.clear();
        if reader.read_line(&mut line).await? == 0 {
            bail!("connection closed inside the headers");
        }
        let Some((key, value)) = line.trim_end().split_once(':') else { break };
        request.headers.push((key.trim().to_string(), value.trim().to_string()));
    }
    if request.header("transfer-encoding").is_some() {
        bail!("chunked request bodies are not supported ({} {})", request.method, request.path);
    }
    let length = request.header("content-length").map_or(Ok(0), str::parse::<usize>)?;
    request.body = vec![0; length];
    reader.read_exact(&mut request.body).await?;
    Ok(Some(request))
}

#[cfg(test)]
mod tests {
    use super::*;
    use futures_util::{SinkExt, StreamExt};
    use tokio_tungstenite::tungstenite::Message;

    struct Echo;

    impl Handler for Echo {
        async fn rest(&self, lane: u64, request: Request) -> Response {
            let a = request.params().get("a").cloned().unwrap_or_default();
            Response::json(200, serde_json::json!({ "lane": lane, "path": request.path, "a": a }).to_string())
        }

        async fn websocket(&self, _lane: u64, _request: Request, mut ws: WebSocketStream<TcpStream>) {
            while let Some(Ok(message)) = ws.next().await {
                if message.is_text() && ws.send(message).await.is_err() {
                    break;
                }
            }
        }
    }

    #[tokio::test]
    async fn the_bots_own_clients_get_through() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(serve(listener, Arc::new(Echo)));
        let client = reqwest::Client::new();
        let get: serde_json::Value =
            client.get(format!("http://{addr}/x?a=1%202")).send().await.unwrap().json().await.unwrap();
        let post: serde_json::Value =
            client.post(format!("http://{addr}/y")).form(&[("a", "3")]).send().await.unwrap().json().await.unwrap();
        assert_eq!((get["path"].as_str(), get["a"].as_str()), (Some("/x"), Some("1 2")));
        assert_eq!(post["a"].as_str(), Some("3"));
        assert_eq!(get["lane"], post["lane"], "keep-alive reuses one connection, so one lane");
        let (mut ws, _) = tokio_tungstenite::connect_async(format!("ws://{addr}/stream")).await.unwrap();
        ws.send(Message::Text("hi".into())).await.unwrap();
        assert_eq!(ws.next().await.unwrap().unwrap(), Message::Text("hi".into()));
    }
}

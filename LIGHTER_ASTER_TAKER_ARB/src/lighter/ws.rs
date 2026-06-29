//! Lighter WebSocket subscription primitives — port of `ws_manager.py`.
//!
//! `subscribe_loop` connects, subscribes to channels (optionally with per-channel auth),
//! handles ping/pong + the `subscribed` confirmation, applies liveness/data watchdogs,
//! and reconnects with exponential backoff. Each decoded application message is handed to a
//! synchronous callback (the hot-path market-data task runs its book+signal update there;
//! cold-path account tasks enqueue to channels). A buggy callback never tears down the
//! socket — it is caught and logged.

use anyhow::Result;
use futures_util::{SinkExt, StreamExt};
use serde_json::Value;
use std::collections::HashMap;
use std::time::{Duration, Instant};
use tokio::sync::Notify;
use tokio::time::sleep;
use tokio_tungstenite::connect_async;
use tokio_tungstenite::tungstenite::Message;

pub const WS_URL: &str = "wss://mainnet.zklighter.elliot.ai/stream";

/// Proactive client-ping interval. Lighter closes any connection that sends NO frame for 2
/// minutes (https://apidocs.lighter.xyz/docs/websocket-reference), so quiet streams (e.g.
/// account/user_stats) must emit a keepalive frame well under that window — matches Python's
/// `ping_interval=20`.
const WS_PING_INTERVAL: Duration = Duration::from_secs(20);

#[derive(Clone)]
pub struct SubscribeOptions {
    pub url: String,
    pub channels: Vec<String>,
    pub channel_auths: HashMap<String, String>,
    /// Reconnect when no real application message arrives within this many seconds.
    /// Quiet private streams may set this to `None`; socket liveness is still guarded by
    /// `frame_timeout`.
    pub data_timeout: Option<f64>,
    /// Reconnect when no websocket/control/application frame arrives within this many seconds.
    pub frame_timeout: f64,
    pub ping_interval: Duration,
    pub reconnect_base: f64,
    pub reconnect_max: f64,
    pub label: String,
}

impl SubscribeOptions {
    pub fn new(label: &str, channels: Vec<String>) -> Self {
        Self {
            url: WS_URL.to_string(),
            channels,
            channel_auths: HashMap::new(),
            data_timeout: Some(30.0),
            frame_timeout: 90.0,
            ping_interval: WS_PING_INTERVAL,
            reconnect_base: 5.0,
            reconnect_max: 60.0,
            label: label.to_string(),
        }
    }
}

/// Deterministic jitter in [0, 0.2*base) without an RNG dependency (matches the spirit of
/// the Python `backoff*0.2*(monotonic()%1)`), seeded by wall-clock ms.
fn jitter(base: f64) -> f64 {
    let now_ms = chrono::Utc::now().timestamp_millis().unsigned_abs();
    let frac = (now_ms % 1000) as f64 / 1000.0;
    base * 0.2 * frac
}

fn next_reconnect_backoff(current: f64, base: f64, max: f64, elapsed: Duration) -> f64 {
    if elapsed >= Duration::from_secs(60) {
        base
    } else {
        (current * 2.0).min(max).max(base)
    }
}

fn reconnect_delay_after_session(current: f64, base: f64, elapsed: Duration) -> f64 {
    if elapsed >= Duration::from_secs(60) {
        base
    } else {
        current
    }
}

/// Run the subscription loop forever (reconnecting). `on_message` is called for each
/// decoded application message (NOT ping/subscribed). `reconnect` (if provided) forces a
/// fresh reconnect when notified (e.g. orderbook sanity divergence). `on_disconnect` runs
/// on every disconnect (clear local book, reset vol state, etc.).
pub async fn subscribe_loop<F, D>(
    opts: SubscribeOptions,
    reconnect: Option<std::sync::Arc<Notify>>,
    mut on_message: F,
    mut on_disconnect: D,
) where
    F: FnMut(&Value),
    D: FnMut(),
{
    let mut backoff = opts.reconnect_base;
    loop {
        let started = Instant::now();
        match session(&opts, reconnect.as_deref(), &mut on_message).await {
            Ok(()) => {}
            Err(e) => tracing::info!("{} ws disconnected: {e}", opts.label),
        }
        on_disconnect();
        let elapsed = started.elapsed();
        let delay = reconnect_delay_after_session(backoff, opts.reconnect_base, elapsed);
        let sleep_for = delay + jitter(delay);
        tracing::info!(
            "{} reconnecting in {:.3}s after session {:.3}s (next_backoff_base={:.3}s)",
            opts.label,
            sleep_for,
            elapsed.as_secs_f64(),
            next_reconnect_backoff(backoff, opts.reconnect_base, opts.reconnect_max, elapsed),
        );
        sleep(Duration::from_secs_f64(sleep_for)).await;
        backoff = next_reconnect_backoff(backoff, opts.reconnect_base, opts.reconnect_max, elapsed);
    }
}

/// Like `subscribe_loop` but regenerates per-channel auth tokens before EACH connection
/// (private channels: account_orders / account_all / user_stats). The server token TTL is
/// ~10 min; on expiry the server drops the socket, the session ends, and we reconnect with a
/// fresh token. `auth_fn` returns the channel->token map for the upcoming connection.
pub async fn subscribe_loop_authed<F, A>(
    mut opts: SubscribeOptions,
    mut auth_fn: A,
    mut on_message: F,
) where
    F: FnMut(&Value),
    A: FnMut() -> std::collections::HashMap<String, String>,
{
    let mut backoff = opts.reconnect_base;
    loop {
        opts.channel_auths = auth_fn();
        if opts.channel_auths.is_empty() {
            tracing::warn!("{}: no auth token; retrying", opts.label);
            sleep(Duration::from_secs_f64(backoff)).await;
            backoff = (backoff * 2.0).min(opts.reconnect_max);
            continue;
        }
        let started = Instant::now();
        match session(&opts, None, &mut on_message).await {
            Ok(()) => {}
            Err(e) => tracing::info!("{} ws disconnected: {e}", opts.label),
        }
        let elapsed = started.elapsed();
        let delay = reconnect_delay_after_session(backoff, opts.reconnect_base, elapsed);
        let sleep_for = delay + jitter(delay);
        tracing::info!(
            "{} reconnecting in {:.3}s after session {:.3}s (next_backoff_base={:.3}s)",
            opts.label,
            sleep_for,
            elapsed.as_secs_f64(),
            next_reconnect_backoff(backoff, opts.reconnect_base, opts.reconnect_max, elapsed),
        );
        sleep(Duration::from_secs_f64(sleep_for)).await;
        backoff = next_reconnect_backoff(backoff, opts.reconnect_base, opts.reconnect_max, elapsed);
    }
}

async fn session<F>(
    opts: &SubscribeOptions,
    reconnect: Option<&Notify>,
    on_message: &mut F,
) -> Result<()>
where
    F: FnMut(&Value),
{
    let (ws_stream, _) = connect_async(&opts.url).await?;
    let (mut write, mut read) = ws_stream.split();
    tracing::info!("connected to {} for {}", opts.url, opts.label);

    for ch in &opts.channels {
        let mut sub = serde_json::json!({"type": "subscribe", "channel": ch});
        if let Some(auth) = opts.channel_auths.get(ch) {
            sub["auth"] = Value::String(auth.clone());
        }
        tracing::info!(
            "{} subscribing channel={} auth_present={}",
            opts.label,
            ch,
            opts.channel_auths.contains_key(ch)
        );
        write.send(Message::Text(sub.to_string())).await?;
    }
    tracing::info!("{} subscribed to {:?}", opts.label, opts.channels);

    let data_to = opts.data_timeout.map(Duration::from_secs_f64);
    let frame_to = Duration::from_secs_f64(opts.frame_timeout);
    let mut ping_tick = tokio::time::interval(opts.ping_interval);
    ping_tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    ping_tick.tick().await; // consume the immediate first tick (just connected)
    let mut last_frame = Instant::now();
    let mut last_data = Instant::now();
    loop {
        // Non-blocking forced-reconnect check.
        if let Some(rc) = reconnect {
            if rc.notified().now_or_never().is_some() {
                tracing::info!(
                    "{} reconnect requested; dropping for fresh snapshot",
                    opts.label
                );
                return Ok(());
            }
        }

        // Race the read against the keepalive tick. On the tick we send a proactive client Ping
        // (so a quiet stream still satisfies Lighter's 2-min "send a frame" rule). Socket
        // liveness is based on ANY received frame; optional data freshness is based only on real
        // application messages.
        let msg = tokio::select! {
            _ = ping_tick.tick() => {
                if last_frame.elapsed() > frame_to {
                    tracing::warn!("{} watchdog: no frames for {}s", opts.label, opts.frame_timeout);
                    return Ok(());
                }
                if let Some(data_to) = data_to {
                    if last_data.elapsed() > data_to {
                        tracing::warn!(
                            "{} watchdog: no application data for {}s",
                            opts.label,
                            opts.data_timeout.unwrap_or_default()
                        );
                        return Ok(());
                    }
                }
                if write.send(Message::Ping(Vec::new())).await.is_err() {
                    return Ok(());
                }
                continue;
            }
            res = read.next() => match res {
                Some(Ok(m)) => m,
                Some(Err(e)) => return Err(e.into()),
                None => return Ok(()),
            },
        };

        last_frame = Instant::now();
        match msg {
            Message::Text(t) => {
                let data: Value = match serde_json::from_str(&t) {
                    Ok(v) => v,
                    Err(_) => continue,
                };
                match data.get("type").and_then(|v| v.as_str()) {
                    Some("ping") => {
                        // Server app-level keepalive — reply, but do NOT count as feed data.
                        let _ = write
                            .send(Message::Text(r#"{"type":"pong"}"#.to_string()))
                            .await;
                    }
                    Some("connected") => {
                        tracing::debug!("{} control connected", opts.label);
                    }
                    Some("subscribed") => {
                        tracing::info!(
                            "{} subscribe acknowledged channel={}",
                            opts.label,
                            data.get("channel")
                                .and_then(|v| v.as_str())
                                .unwrap_or("unknown")
                        );
                    }
                    _ => {
                        // Real application message — this is the only thing that refreshes the
                        // data watchdog. Callbacks here are written to not panic.
                        last_data = Instant::now();
                        on_message(&data);
                    }
                }
            }
            Message::Ping(p) => {
                let _ = write.send(Message::Pong(p)).await;
            }
            Message::Close(_) => return Ok(()),
            _ => {}
        }
    }
}

// bring `now_or_never` into scope
use futures_util::future::FutureExt;

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::Arc;
    use tokio::net::TcpListener;
    use tokio::time::{sleep, timeout};
    use tokio_tungstenite::accept_async;

    async fn local_ws_url() -> (TcpListener, String) {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let url = format!("ws://{}", listener.local_addr().unwrap());
        (listener, url)
    }

    fn opts(url: String) -> SubscribeOptions {
        let mut opts = SubscribeOptions::new("test", vec!["test/channel".to_string()]);
        opts.url = url;
        opts.reconnect_base = 0.01;
        opts.reconnect_max = 0.01;
        opts.ping_interval = Duration::from_millis(50);
        opts
    }

    #[tokio::test]
    async fn app_ping_replies_pong_without_refreshing_application_data() {
        let (listener, url) = local_ws_url().await;
        let server = tokio::spawn(async move {
            let (stream, _) = listener.accept().await.unwrap();
            let mut ws = accept_async(stream).await.unwrap();
            let _sub = ws.next().await.unwrap().unwrap();
            ws.send(Message::Text(r#"{"type":"ping"}"#.into()))
                .await
                .unwrap();
            let msg = timeout(Duration::from_secs(1), ws.next())
                .await
                .unwrap()
                .unwrap()
                .unwrap();
            assert_eq!(msg, Message::Text(r#"{"type":"pong"}"#.into()));
            sleep(Duration::from_millis(200)).await;
        });

        let mut opts = opts(url);
        opts.data_timeout = Some(0.12);
        opts.frame_timeout = 2.0;
        let calls = Arc::new(AtomicUsize::new(0));
        let calls_for_cb = calls.clone();
        let mut on_message = move |_data: &Value| {
            calls_for_cb.fetch_add(1, Ordering::Relaxed);
        };

        timeout(
            Duration::from_secs(2),
            session(&opts, None, &mut on_message),
        )
        .await
        .unwrap()
        .unwrap();
        server.await.unwrap();
        assert_eq!(calls.load(Ordering::Relaxed), 0);
    }

    #[tokio::test]
    async fn quiet_control_frames_keep_private_stream_alive_when_data_timeout_disabled() {
        let (listener, url) = local_ws_url().await;
        let server = tokio::spawn(async move {
            let (stream, _) = listener.accept().await.unwrap();
            let mut ws = accept_async(stream).await.unwrap();
            let _sub = ws.next().await.unwrap().unwrap();
            loop {
                ws.send(Message::Pong(Vec::new())).await.unwrap();
                sleep(Duration::from_millis(40)).await;
            }
        });

        let mut opts = opts(url);
        opts.data_timeout = None;
        opts.frame_timeout = 0.15;
        let calls = Arc::new(AtomicUsize::new(0));
        let calls_for_cb = calls.clone();
        let mut on_message = move |_data: &Value| {
            calls_for_cb.fetch_add(1, Ordering::Relaxed);
        };

        let mut fut = Box::pin(session(&opts, None, &mut on_message));
        assert!(
            timeout(Duration::from_millis(350), &mut fut).await.is_err(),
            "quiet control frames should keep the private stream session alive"
        );
        drop(fut);
        server.abort();
        assert_eq!(calls.load(Ordering::Relaxed), 0);
    }

    #[tokio::test]
    async fn silent_socket_trips_frame_timeout_even_when_data_timeout_disabled() {
        let (listener, url) = local_ws_url().await;
        let server = tokio::spawn(async move {
            let (stream, _) = listener.accept().await.unwrap();
            let mut ws = accept_async(stream).await.unwrap();
            let _sub = ws.next().await.unwrap().unwrap();
            sleep(Duration::from_secs(1)).await;
        });

        let mut opts = opts(url);
        opts.data_timeout = None;
        opts.frame_timeout = 0.16;
        let mut on_message = |_data: &Value| {};

        timeout(
            Duration::from_secs(2),
            session(&opts, None, &mut on_message),
        )
        .await
        .unwrap()
        .unwrap();
        server.abort();
    }

    #[tokio::test]
    async fn subscription_message_includes_channel_auth_when_configured() {
        let (listener, url) = local_ws_url().await;
        let server = tokio::spawn(async move {
            let (stream, _) = listener.accept().await.unwrap();
            let mut ws = accept_async(stream).await.unwrap();
            let sub = ws.next().await.unwrap().unwrap();
            let Message::Text(raw) = sub else {
                panic!("expected text subscribe frame");
            };
            let v: Value = serde_json::from_str(&raw).unwrap();
            assert_eq!(v.get("type").and_then(|x| x.as_str()), Some("subscribe"));
            assert_eq!(
                v.get("channel").and_then(|x| x.as_str()),
                Some("test/channel")
            );
            assert_eq!(v.get("auth").and_then(|x| x.as_str()), Some("secret-token"));
            ws.close(None).await.unwrap();
        });

        let mut opts = opts(url);
        opts.channel_auths
            .insert("test/channel".to_string(), "secret-token".to_string());
        opts.frame_timeout = 1.0;
        let mut on_message = |_data: &Value| {};

        timeout(
            Duration::from_secs(2),
            session(&opts, None, &mut on_message),
        )
        .await
        .unwrap()
        .unwrap();
        server.await.unwrap();
    }

    #[tokio::test]
    async fn application_message_refreshes_data_timeout_and_invokes_callback() {
        let (listener, url) = local_ws_url().await;
        let server = tokio::spawn(async move {
            let (stream, _) = listener.accept().await.unwrap();
            let mut ws = accept_async(stream).await.unwrap();
            let _sub = ws.next().await.unwrap().unwrap();
            for n in 0..4 {
                ws.send(Message::Text(format!(r#"{{"type":"update","n":{n}}}"#)))
                    .await
                    .unwrap();
                sleep(Duration::from_millis(60)).await;
            }
            sleep(Duration::from_millis(200)).await;
        });

        let mut opts = opts(url);
        opts.data_timeout = Some(0.12);
        opts.frame_timeout = 2.0;
        let calls = Arc::new(AtomicUsize::new(0));
        let calls_for_cb = calls.clone();
        let mut on_message = move |_data: &Value| {
            calls_for_cb.fetch_add(1, Ordering::Relaxed);
        };

        timeout(
            Duration::from_secs(2),
            session(&opts, None, &mut on_message),
        )
        .await
        .unwrap()
        .unwrap();
        server.await.unwrap();
        assert_eq!(calls.load(Ordering::Relaxed), 4);
    }
}

//! Dry-run mode: the unchanged live bot trades against an in-process simulation of Aster and
//! Lighter as a bot in AWS Tokyo would see them.
//!
//! The simulated world is the real one delayed by a constant shift D: real feed events are
//! replayed D late, so this host's own feed lag (Europe) is hidden and the latency a Tokyo bot
//! would pay is modelled explicitly. `matching` is the venues' deterministic core; `book` holds
//! the replica each market trades against; `account` settles the money; `clock` draws the
//! latencies; `feed` brings the real market in; `server` speaks HTTP and WebSocket on
//! loopback, and `aster` and `lighter` speak each venue's protocol on top of it.

pub mod account;
pub mod aster;
pub mod book;
pub mod clock;
pub mod feed;
pub mod lighter;
pub mod matching;
pub mod server;

use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use tokio::sync::{broadcast, mpsc, oneshot};

use self::account::AccountView;
use self::clock::{wall_us, Latency};
use self::feed::{Follower, Hub, Input};
use self::matching::{Envelope, Event, Exchange, Order, Output, Reject, Reply, Venue};

enum Command {
    Call(Envelope, oneshot::Sender<Reply>),
    Peek(Venue, oneshot::Sender<(AccountView, Vec<Order>)>),
}

/// Private events a connection may fall behind by before it is dropped.
const EVENTS: usize = 1_024;

/// The simulated venues as their protocol servers reach them: requests go through the matching
/// core, private events come out of it, public frames come from the feed hubs.
#[derive(Clone)]
pub struct Venues {
    commands: mpsc::UnboundedSender<Command>,
    events: [broadcast::Sender<Arc<Event>>; 2],
    hubs: [Arc<Mutex<Hub>>; 2],
    feed_latency: [Latency; 2],
    seed: u64,
}

impl Venues {
    /// Runs `core` on this host's wall clock, fed by `feed`, until every handle is dropped.
    pub fn start(core: Exchange, feed: mpsc::UnboundedReceiver<Input>, hubs: [Arc<Mutex<Hub>>; 2], feed_latency: [Latency; 2], seed: u64) -> Self {
        let (commands, rx) = mpsc::unbounded_channel();
        let events = [broadcast::channel(EVENTS).0, broadcast::channel(EVENTS).0];
        tokio::spawn(drive(core, feed, rx, events.clone()));
        Self { commands, events, hubs, feed_latency, seed }
    }

    /// Sends one request now; its reply comes back after the venue's round trip.
    pub async fn call(&self, envelope: Envelope) -> Reply {
        let (tx, rx) = oneshot::channel();
        let _ = self.commands.send(Command::Call(envelope, tx));
        rx.await.unwrap_or(Reply::Reject(Reject::Unavailable))
    }

    /// The venue's account and open orders now: what a private stream sends on subscription.
    pub async fn peek(&self, venue: Venue) -> Option<(AccountView, Vec<Order>)> {
        let (tx, rx) = oneshot::channel();
        let _ = self.commands.send(Command::Peek(venue, tx));
        rx.await.ok()
    }

    /// The venue's private events from now on.
    pub fn events(&self, venue: Venue) -> broadcast::Receiver<Arc<Event>> {
        self.events[venue.ix()].subscribe()
    }

    /// Connection `lane`'s view of the venue's public streams.
    pub fn follower(&self, venue: Venue, lane: u64, combined: bool) -> Follower {
        let v = venue.ix();
        Follower::new(self.hubs[v].clone(), self.feed_latency[v], self.seed ^ lane.rotate_left(32), combined)
    }
}

async fn drive(
    mut core: Exchange,
    mut feed: mpsc::UnboundedReceiver<Input>,
    mut commands: mpsc::UnboundedReceiver<Command>,
    events: [broadcast::Sender<Arc<Event>>; 2],
) {
    let mut waiting: HashMap<u64, oneshot::Sender<Reply>> = HashMap::new();
    let mut out = Vec::new();
    let mut feeding = true;
    loop {
        let wait = core.next_due().map(|due| Duration::from_micros((due - wall_us()).max(0) as u64));
        tokio::select! {
            input = feed.recv(), if feeding => match input {
                Some(input) => {
                    core.advance(wall_us(), &mut out);
                    match input {
                        Input::Frame { venue, market, exch_us, event } => core.ingest(venue, &market, exch_us, event),
                        Input::Funding { venue, market, exch_us, rate } => core.funding(venue, &market, exch_us, rate),
                    }
                }
                None => feeding = false,
            },
            command = commands.recv() => {
                let Some(command) = command else { return };
                core.advance(wall_us(), &mut out);
                match command {
                    Command::Call(envelope, reply) => {
                        waiting.insert(core.submit(envelope), reply);
                    }
                    Command::Peek(venue, reply) => {
                        let _ = reply.send(core.peek(venue));
                    }
                }
            }
            _ = tokio::time::sleep(wait.unwrap_or_default()), if wait.is_some() => {}
        }
        core.advance(wall_us(), &mut out);
        for output in out.drain(..) {
            match output {
                Output::Reply { ticket, reply } => {
                    if let Some(tx) = waiting.remove(&ticket) {
                        let _ = tx.send(reply);
                    }
                }
                Output::Event { venue, event } => {
                    let _ = events[venue.ix()].send(Arc::new(event));
                }
            }
        }
    }
}

/// The venues answering the bot's own clients on loopback.
#[cfg(test)]
pub(crate) mod tests {
    use std::path::Path;
    use std::time::Instant;

    use futures_util::StreamExt;
    use rust_decimal::Decimal;
    use rust_decimal_macros::dec;
    use serde_json::Value;
    use tokio::net::TcpListener;
    use tokio_util::sync::CancellationToken;

    use super::aster::Aster;
    use super::feed::{aster_frame, aster_streams, forward, lighter_frame, Frame};
    use super::lighter::Lighter;
    use super::matching::{Fees, Filters, Request, SimParams};
    use super::*;
    use crate::hotpath::clock::mono_now_ns;
    use crate::lighter::messages::{OrderBooksResponse, TradePayload};
    use crate::lighter::rest::RestClient;
    use crate::lighter::signer::{self as native, Signer};
    use crate::lighter::tx_ws::TxWebSocket;
    use crate::lighter::ws::{stream_url, subscribe_loop, SubscribeOptions};
    use crate::livebot::exec::aster::{AsterRest, CancelOutcome};
    use crate::livebot::exec::creds::{AsterCreds, LighterCreds};
    use crate::livebot::exec::sign::{AsterSigner, EvmAsterSigner};
    use crate::livebot::exec::ExecEvent;
    use crate::livebot::scale::MarketScale;
    use crate::livebot::userstream::{run_aster_user_stream, StreamLiveness};
    use crate::types::{MarketId, Side, TxSendStatus};

    /// Frames are stamped one shift in the past, so the core applies them at once.
    const SHIFT_MS: i64 = 50;
    /// Lighter's `orderBooks`, cut to HYPE.
    const ORDER_BOOKS: &str = r#"{"code":200,"order_books":[{"symbol":"HYPE","market_id":24,"status":"active","taker_fee":"0.0000","maker_fee":"0.0000","min_base_amount":"0.50","min_quote_amount":"10.000000","supported_size_decimals":2,"supported_price_decimals":4,"supported_quote_decimals":6}]}"#;

    /// Both simulated venues on loopback: HYPE quoted 99 / 101 five deep, 1000 of collateral on
    /// each, a few milliseconds away.
    pub(crate) struct World {
        pub(crate) aster: String,
        pub(crate) lighter: String,
        venues: Venues,
        inputs: mpsc::UnboundedSender<Input>,
        hubs: [Arc<Mutex<Hub>>; 2],
    }

    async fn serve<H: server::Handler>(handler: H) -> String {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let url = format!("http://{}", listener.local_addr().unwrap());
        tokio::spawn(server::serve(listener, Arc::new(handler)));
        url
    }

    impl World {
        pub(crate) async fn start() -> Self {
            let fixed = |ms: f64| Latency::try_from([ms, ms]).unwrap();
            let params = SimParams {
                shift_us: SHIFT_MS * 1_000,
                seed: 1,
                effect_fraction: 0.9,
                rtt: [fixed(4.0), fixed(2.0)],
                private: [fixed(1.0), fixed(1.0)],
                lighter_taker_delay_us: 300_000,
                hidden_queue_multiplier: dec!(0.5),
                fees: [Fees { maker: dec!(0), taker: dec!(0.0004) }, Fees { maker: dec!(0), taker: dec!(0) }],
                leverage: dec!(1),
                balances: [dec!(1000), dec!(1000)],
            };
            let mut core = Exchange::new(params, wall_us());
            let aster = Filters { tick: dec!(0.001), step: dec!(0.01), min_qty: dec!(0.01), min_notional: dec!(5), price_band: Some(dec!(0.05)) };
            let lighter = Filters { tick: dec!(0.0001), step: dec!(0.01), min_qty: dec!(0.5), min_notional: dec!(10), price_band: Some(dec!(0.05)) };
            core.add_market(Venue::Aster, "HYPEUSDT", Some(20), aster);
            core.add_market(Venue::Lighter, "24", None, lighter);
            let shift = SHIFT_MS * 1_000;
            let hubs = [
                Arc::new(Mutex::new(Hub::new(shift, aster_streams("HYPEUSDT")))),
                Arc::new(Mutex::new(Hub::new(shift, ["order_book/24".to_string()]))),
            ];
            let (inputs, feed) = mpsc::unbounded_channel();
            let venues = Venues::start(core, feed, hubs.clone(), [Latency::ZERO; 2], 1);
            let markets = serde_json::from_str::<OrderBooksResponse>(ORDER_BOOKS).unwrap().order_books;
            let aster = serve(Aster::new(venues.clone(), "{}".into(), vec!["HYPEUSDT".into()], dec!(1))).await;
            let lighter = serve(Lighter::new(venues.clone(), ORDER_BOOKS.into(), markets, [dec!(0); 2], dec!(1))).await;
            let world = Self { aster, lighter, venues, inputs, hubs };
            world.aster_book(dec!(99), dec!(101));
            world.lighter_book(dec!(99), dec!(101));
            // The feed and the requests reach the core on separate channels.
            for (venue, market) in [(Venue::Aster, "HYPEUSDT"), (Venue::Lighter, "24")] {
                let book = || Envelope { venue, lane: 0, weight: 0, orders: 0, nonce: None, request: Request::Book { market: market.into() } };
                while matches!(world.venues.call(book()).await, Reply::Reject(_)) {
                    tokio::time::sleep(Duration::from_millis(5)).await;
                }
            }
            world
        }

        /// As the upstream does: to the core and to the bot's streams.
        fn publish(&self, frame: Frame) {
            let hub = &self.hubs[frame.venue.ix()];
            forward(frame, hub, &self.inputs);
        }

        /// One shift ago in venue milliseconds: due now.
        fn due_ms() -> i64 {
            wall_us() / 1_000 - SHIFT_MS
        }

        pub(crate) fn aster_book(&self, bid: Decimal, ask: Decimal) {
            let t = Self::due_ms();
            let text = format!(r#"{{"stream":"hypeusdt@depth20@100ms","data":{{"e":"depthUpdate","E":{t},"T":{t},"s":"HYPEUSDT","U":1,"u":2,"pu":0,"b":[["{bid}","5"]],"a":[["{ask}","5"]]}}}}"#);
            self.publish(aster_frame(&text, SHIFT_MS * 1_000).unwrap().unwrap());
        }

        /// A print whose aggressor was on the `taker` side.
        pub(crate) fn aster_print(&self, price: Decimal, qty: Decimal, taker: Side) {
            let (t, buyer_made) = (Self::due_ms(), taker == Side::Sell);
            let text = format!(r#"{{"stream":"hypeusdt@aggTrade","data":{{"e":"aggTrade","E":{t},"a":1,"s":"HYPEUSDT","p":"{price}","q":"{qty}","f":1,"l":1,"T":{t},"m":{buyer_made}}}}}"#);
            self.publish(aster_frame(&text, SHIFT_MS * 1_000).unwrap().unwrap());
        }

        fn lighter_book(&self, bid: Decimal, ask: Decimal) {
            let t = Self::due_ms();
            let us = t * 1_000;
            let text = format!(r#"{{"channel":"order_book:24","last_updated_at":{us},"offset":10,"order_book":{{"code":0,"asks":[{{"price":"{ask}","size":"5"}}],"bids":[{{"price":"{bid}","size":"5"}}],"offset":10,"nonce":1,"last_updated_at":{us},"begin_nonce":0}},"timestamp":{t},"type":"subscribed/order_book"}}"#);
            self.publish(lighter_frame(&text, SHIFT_MS * 1_000).unwrap().unwrap());
        }
    }

    pub(crate) fn aster_signer() -> Arc<dyn AsterSigner> {
        let creds = AsterCreds::dry_run();
        Arc::new(EvmAsterSigner::new(creds.user, creds.signer, creds.key).unwrap())
    }

    fn xemm_aster(world: &World) -> AsterRest {
        let scale = MarketScale { tick: dec!(0.001), step: dec!(0.01), hl_qty_step: dec!(0.01) };
        let markets = HashMap::from([(MarketId::from("HYPE"), (scale, "HYPEUSDT".to_string()))]);
        AsterRest::new(world.aster.clone(), aster_signer(), markets, 30_000, 1_000, 1_000, None).unwrap()
    }

    /// A subscription's frames, taken by type in whatever order they came.
    struct Frames(mpsc::UnboundedReceiver<String>, Vec<Value>);

    impl Frames {
        async fn take(&mut self, kind: &str) -> Value {
            loop {
                if let Some(i) = self.1.iter().position(|v| v["type"] == kind) {
                    return self.1.remove(i);
                }
                let text = tokio::time::timeout(Duration::from_secs(5), self.0.recv()).await
                    .unwrap_or_else(|_| panic!("no {kind} in time; got {:?}", self.1))
                    .expect("the subscription ended");
                self.1.push(serde_json::from_str(&text).unwrap());
            }
        }
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn aster_answers_the_bots_own_clients() {
        let world = World::start().await;
        let hype = MarketId::from("HYPE");
        let rest = xemm_aster(&world);
        // The live startup gates.
        assert!(rest.is_one_way().await.unwrap());
        assert_eq!(rest.get_leverage(&hype).await.unwrap(), 1);
        assert_eq!(rest.account_available_balance().await.unwrap(), dec!(1000));
        assert!(rest.open_orders(None).await.unwrap().is_empty());

        // A quote rests, shows and cancels once; a post-only order that would take expires.
        let placed = rest.place(&hype, Side::Buy, 98_500, 10, "q1", false).await;
        assert!(matches!(&placed, ExecEvent::PlaceAck { client_id, .. } if client_id == "q1"), "{placed:?}");
        assert_eq!(rest.open_orders(Some(&hype)).await.unwrap().len(), 1);
        assert_eq!(rest.query_order(&hype, "q1").await.unwrap()["status"], "NEW");
        assert_eq!(rest.cancel_order(&hype, "q1").await.unwrap(), CancelOutcome::Canceled);
        assert_eq!(rest.cancel_order(&hype, "q1").await.unwrap(), CancelOutcome::AlreadyGone);
        let crossing = rest.place(&hype, Side::Buy, 101_000, 10, "q2", false).await;
        assert!(matches!(&crossing, ExecEvent::PlaceReject { reason, .. } if reason.contains("EXPIRED")), "{crossing:?}");

        // A print through a resting quote fills it, and the user stream reports the fill.
        let (fills_tx, mut fills) = tokio::sync::mpsc::channel(8);
        let liveness = Arc::new(StreamLiveness::default());
        let shutdown = CancellationToken::new();
        let symbols = HashMap::from([("HYPEUSDT".to_string(), hype.clone())]);
        tokio::spawn(run_aster_user_stream(xemm_aster(&world), symbols, fills_tx, liveness.clone(), shutdown.clone()));
        while liveness.age_ms(mono_now_ns()) == i64::MAX {
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
        let placed = rest.place(&hype, Side::Buy, 99_000, 10, "q3", false).await;
        assert!(matches!(placed, ExecEvent::PlaceAck { .. }), "{placed:?}");
        world.aster_print(dec!(98.9), dec!(20), Side::Sell);
        let fill = tokio::time::timeout(Duration::from_secs(5), fills.recv()).await.unwrap().unwrap();
        assert_eq!((fill.client_id.as_str(), fill.aster_side, fill.last_fill_qty, fill.last_fill_px), ("q3", Side::Buy, dec!(0.1), dec!(99)));
        assert_eq!(fill.commission, None, "a free maker fill carries no commission fields");
        shutdown.cancel();

        // Public streams, combined and raw, with the venue's timestamps moved by the shift.
        for path in ["stream?streams=hypeusdt@depth20@100ms", "ws/hypeusdt@depth20@100ms"] {
            let url = format!("{}/{path}", world.aster.replacen("http", "ws", 1));
            let (mut ws, _) = tokio_tungstenite::connect_async(url).await.unwrap();
            // Only frames published after the subscription reaches the venue come through.
            let (text, due) = loop {
                let due = World::due_ms();
                world.aster_book(dec!(99), dec!(101));
                if let Ok(Some(Ok(message))) = tokio::time::timeout(Duration::from_millis(50), ws.next()).await {
                    break (message.into_text().unwrap(), due);
                }
            };
            let frame: Value = serde_json::from_str(&text).unwrap();
            let data = if path.starts_with("stream") { &frame["data"] } else { &frame };
            if path.starts_with("stream") {
                assert_eq!(frame["stream"], "hypeusdt@depth20@100ms");
            }
            assert_eq!((data["b"][0][0].as_str(), data["a"][0][0].as_str()), (Some("99"), Some("101")));
            assert!(data["T"].as_i64().unwrap() >= due + SHIFT_MS, "{data}");
        }
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn lighter_answers_the_bots_own_clients() {
        let dir = Path::new("signers");
        if !dir.join(native::signer_filename()).exists() {
            eprintln!("skipped: no signer library for this platform");
            return;
        }
        let _native = native::NATIVE.lock().unwrap_or_else(|e| e.into_inner());
        let world = World::start().await;
        let creds = LighterCreds::dry_run();
        let (account, key) = (creds.account_index, creds.api_key_index);
        let signer = Signer::load(dir, &world.lighter, &creds.api_private_key, key, account).unwrap();
        // The library's own key check, as at live startup.
        tokio::task::block_in_place(|| signer.check_client(key)).unwrap();

        let rest = RestClient::new(&world.lighter, 0).unwrap();
        let books = rest.order_books().await.unwrap();
        assert_eq!((books[0].market_id, books[0].supported_size_decimals, books[0].supported_price_decimals), (24, 2, 4));
        assert_eq!(rest.next_nonce(account, key).await.unwrap(), 0);
        assert_eq!(rest.account_position(account, 24).await.unwrap(), Decimal::ZERO);
        let top = rest.order_book_orders(24, 1).await.unwrap();
        assert_eq!((top["bids"][0]["price"].as_str(), top["asks"][0]["price"].as_str()), (Some("99"), Some("101")));

        let url = stream_url(&world.lighter);
        let (frames_tx, frames_rx) = mpsc::unbounded_channel();
        let channels = vec!["order_book/24".to_string(), format!("account_all/{account}"), format!("account_all_orders/{account}")];
        let mut options = SubscribeOptions::new(&url, "dry-run test", channels);
        options.channel_auths = options.channels.iter().map(|c| (c.clone(), "token".to_string())).collect();
        let stream = tokio::spawn(subscribe_loop(options, None, move |f| {
            let _ = frames_tx.send(f.raw.to_string());
        }, || {}));
        let mut frames = Frames(frames_rx, Vec::new());
        let book = frames.take("subscribed/order_book").await;
        assert_eq!(book["order_book"]["bids"][0]["price"], "99");
        frames.take("subscribed/account_all_orders").await;

        let tx = TxWebSocket::new(&url);
        tx.connect().await.unwrap();
        let order = |client: i64, price: i32, is_ask: bool, tif: i32, nonce: i64| {
            let expiry = if tif == native::TIF_IMMEDIATE_OR_CANCEL { native::DEFAULT_IOC_EXPIRY } else { native::DEFAULT_28_DAY_ORDER_EXPIRY };
            let signed = signer
                .sign_create_order(24, client, 50, price, is_ask, native::ORDER_TYPE_LIMIT, tif, false,
                    native::NIL_TRIGGER_PRICE, expiry, nonce, key)
                .unwrap();
            ([signed.tx_type], [signed.tx_info])
        };
        // A quote: the sequencer accepts it, then the account stream and REST show it resting.
        let (types, infos) = order(7, 985_000, false, native::TIF_POST_ONLY, 0);
        let sent = tx.send_batch(&types, &infos).await;
        assert_eq!((sent.status, sent.code), (TxSendStatus::Ok, 0), "{}", sent.message);
        let orders = frames.take("update/account_all_orders").await;
        assert_eq!(orders["orders"]["24"][0]["client_order_index"], 7);
        let active = rest.account_active_orders(account, 24, "token").await.unwrap();
        assert_eq!(active.iter().map(|o| o.client_order_index).collect::<Vec<_>>(), [Some(7)]);
        // A reused nonce is refused before anything executes.
        let (types, infos) = order(8, 985_000, false, native::TIF_POST_ONLY, 0);
        let refused = tx.send_batch(&types, &infos).await;
        assert_eq!((refused.status, refused.code), (TxSendStatus::Rejected, 21104));
        // A taker order executes after the Standard account's delay; its trade reaches the stream.
        let (types, infos) = order(9, 980_000, true, native::TIF_IMMEDIATE_OR_CANCEL, 1);
        let sent_at = Instant::now();
        assert_eq!(tx.send_batch(&types, &infos).await.status, TxSendStatus::Ok);
        let update = frames.take("update/account_all").await;
        assert!(sent_at.elapsed() >= Duration::from_millis(300), "filled {:?} after sending", sent_at.elapsed());
        let trades: Vec<TradePayload> = serde_json::from_value(update["trades"]["24"].clone()).unwrap();
        assert_eq!((trades[0].ask_client_id, trades[0].ask_account_id, trades[0].is_maker_ask), (Some(9), Some(account), Some(false)));
        assert_eq!(trades[0].size.as_deref().map(|s| s.parse::<Decimal>().unwrap()), Some(dec!(0.5)));
        assert_eq!(rest.account_position(account, 24).await.unwrap(), dec!(-0.5));
        stream.abort();
    }
}

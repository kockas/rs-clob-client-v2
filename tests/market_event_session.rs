#![cfg(all(feature = "clob", feature = "ws"))]
use futures_util::{FutureExt, SinkExt, StreamExt};
use polymarket_client_sdk_v2::{
    clob::ws::{
        ChannelType, Client, MarketEventSession, MarketSessionEvent, ScopeChange, WsMessage,
    },
    types::U256,
    ws::config::Config,
};
use serde_json::{Value, json};
use std::{
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
    time::Duration,
};
use tokio::sync::{mpsc, oneshot};
use tokio_tungstenite::tungstenite::Message;

const TS: i64 = 1_800_000_000_000;
fn ids(values: &[u64]) -> Vec<U256> {
    values.iter().map(|value| U256::from(*value)).collect()
}
fn book(token: u64, offset: i64) -> Value {
    json!({"event_type":"book", "asset_id":token.to_string(), "market":format!("0x{}", "ab".repeat(32)),
        "timestamp":(TS+offset).to_string(), "bids":[{"price":"0.4","size":"5"}],
        "asks":[{"price":"0.6","size":"5"}], "hash":"session-fixture"})
}
fn mixed(tokens: &[u64]) -> Value {
    json!({"event_type":"price_change", "market":format!("0x{}", "ab".repeat(32)),
        "timestamp":(TS+10).to_string(), "price_changes":tokens.iter().map(|token|json!({
            "asset_id":token.to_string(), "price":"0.41","size":"7","side":"BUY",
            "best_bid":"0.41","best_ask":"0.6"
        })).collect::<Vec<_>>()})
}
fn trade(token: u64) -> Value {
    json!({"event_type":"last_trade_price", "asset_id":token.to_string(),
        "market":format!("0x{}", "ab".repeat(32)), "timestamp":(TS+11).to_string(),
        "price":"0.41","size":"5","side":"SELL","fee_rate_bps":"0"})
}

enum Command {
    Frames(Vec<String>, oneshot::Sender<()>),
    Close(oneshot::Sender<()>),
}
struct Harness {
    client: Client,
    session: MarketEventSession,
    commands: mpsc::UnboundedSender<Command>,
    requests: mpsc::UnboundedReceiver<Value>,
    connections: Arc<AtomicUsize>,
    server: tokio::task::JoinHandle<()>,
}
impl Harness {
    async fn new(initial: &[u64], immediate_snapshot: bool) -> Self {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let endpoint = format!("ws://{}", listener.local_addr().unwrap());
        let (commands, mut command_rx) = mpsc::unbounded_channel();
        let (requests_tx, requests) = mpsc::unbounded_channel();
        let connections = Arc::new(AtomicUsize::new(0));
        let count = connections.clone();
        let server = tokio::spawn(async move {
            loop {
                let (tcp, _) = listener.accept().await.unwrap();
                let mut socket = tokio_tungstenite::accept_async(tcp).await.unwrap();
                count.fetch_add(1, Ordering::SeqCst);
                loop {
                    tokio::select! {
                        command = command_rx.recv() => match command {
                            Some(Command::Frames(frames, done)) => {
                                for (ordinal, frame) in frames.into_iter().enumerate() {
                                    socket.send(Message::Text(frame.into())).await.unwrap();
                                    if ordinal % 64 == 0 { tokio::task::yield_now().await; }
                                }
                                let _ = done.send(());
                            }
                            Some(Command::Close(done)) => {
                                socket.close(None).await.unwrap();
                                let _ = done.send(());
                                break;
                            }
                            None => return,
                        },
                        message = socket.next() => match message {
                            Some(Ok(Message::Text(text))) if text == "PING" => {
                                socket.send(Message::Text("PONG".into())).await.unwrap();
                            }
                            Some(Ok(Message::Text(text))) => {
                                let request: Value = serde_json::from_str(&text).unwrap();
                                if immediate_snapshot && request["operation"] == "subscribe" {
                                    for token in request["assets_ids"].as_array().unwrap() {
                                        let token = token.as_str().unwrap().parse::<u64>().unwrap();
                                        socket.send(Message::Text(book(token,0).to_string().into())).await.unwrap();
                                    }
                                }
                                let _ = requests_tx.send(request);
                            }
                            Some(Ok(_)) => {},
                            None | Some(Err(_)) => break,
                        },
                    }
                }
            }
        });
        let mut config = Config::default();
        config.reconnect.initial_backoff = Duration::from_millis(1);
        config.reconnect.max_backoff = Duration::from_millis(5);
        let client = Client::new(&endpoint, config).unwrap();
        let session = client.market_event_session(ids(initial)).unwrap();
        let harness = Self {
            client,
            session,
            commands,
            requests,
            connections,
            server,
        };
        harness.connected(1).await;
        harness
    }
    async fn connected(&self, count: usize) {
        tokio::time::timeout(Duration::from_secs(3), async {
            while self.connections.load(Ordering::SeqCst) < count
                || !self
                    .client
                    .connection_state(ChannelType::Market)
                    .is_connected()
            {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("loopback connection prerequisite");
    }
    async fn raw(&self, frames: Vec<String>) {
        let (done, wait) = oneshot::channel();
        self.commands.send(Command::Frames(frames, done)).unwrap();
        wait.await.unwrap();
    }
    async fn frames(&self, values: Vec<Value>) {
        let target = self
            .client
            .connection_health(ChannelType::Market)
            .unwrap()
            .messages
            + values.len() as u64;
        self.raw(values.into_iter().map(|value| value.to_string()).collect())
            .await;
        tokio::time::timeout(Duration::from_secs(5), async {
            while self
                .client
                .connection_health(ChannelType::Market)
                .unwrap()
                .messages
                < target
            {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("real parsed messages must reach SDK publication");
    }
    async fn next(&mut self) -> MarketSessionEvent {
        tokio::time::timeout(Duration::from_secs(3), self.session.next())
            .await
            .expect("ordered event must arrive")
            .expect("session ended")
            .expect("session faulted")
    }
    async fn scope(&mut self) -> ScopeChange {
        let MarketSessionEvent::Scope(change) = self.next().await else {
            panic!("expected ordered scope marker")
        };
        change
    }
    async fn request(&mut self) -> Value {
        tokio::time::timeout(Duration::from_secs(3), self.requests.recv())
            .await
            .unwrap()
            .unwrap()
    }
    async fn close_and_reconnect(&self) {
        let target = self.connections.load(Ordering::SeqCst) + 1;
        let (done, wait) = oneshot::channel();
        self.commands.send(Command::Close(done)).unwrap();
        wait.await.unwrap();
        self.connected(target).await;
    }
}
impl Drop for Harness {
    fn drop(&mut self) {
        self.server.abort();
    }
}

#[tokio::test]
async fn immediate_snapshot_has_initial_scope_then_first_epoch_binding() {
    let mut h = Harness::new(&[1], true).await;
    let scope = h.scope().await;
    assert_eq!(
        (scope.revision, scope.sequence_cut, scope.transport_epoch),
        (1, 0, 1)
    );
    let MarketSessionEvent::Message {
        sequence,
        transport_epoch,
        token_bindings,
        message,
    } = h.next().await
    else {
        panic!("snapshot")
    };
    assert_eq!((sequence, transport_epoch), (1, 1));
    assert_eq!(token_bindings, scope.added);
    assert!(matches!(message, WsMessage::Book(_)));
    assert_eq!(
        h.session.membership().binding(U256::from(1)).unwrap(),
        token_bindings.get(&U256::from(1)).copied()
    );
}

#[tokio::test]
async fn queued_six_to_six_preserves_mixed_frame_and_filtered_tail_cut() {
    let mut h = Harness::new(&[1, 2, 3, 4, 5, 6], false).await;
    let initial = h.scope().await;
    h.frames((1..=6).map(|token| book(token, 0)).collect())
        .await;
    for sequence in 1..=6 {
        assert!(
            matches!(h.next().await, MarketSessionEvent::Message { sequence: actual, .. } if actual == sequence)
        );
    }
    h.frames(vec![book(1, 1), mixed(&[1, 3, 7]), trade(2), book(999, 12)])
        .await;
    assert_eq!(
        h.client
            .connection_health(ChannelType::Market)
            .unwrap()
            .pending,
        4
    );
    let change = h.session.replace_assets(ids(&[1, 2, 5, 6, 7, 8])).unwrap();
    assert_eq!(change.sequence_cut, 10);
    let view = h.session.membership();
    assert_eq!(
        view.binding(U256::from(1)).unwrap(),
        initial.added.get(&U256::from(1)).copied()
    );
    assert!(
        view.binding(U256::from(3)).unwrap().is_none(),
        "retirement visible before marker consumption"
    );
    assert!(matches!(
        h.next().await,
        MarketSessionEvent::Message { sequence: 7, .. }
    ));
    let MarketSessionEvent::Message {
        sequence,
        token_bindings,
        message,
        ..
    } = h.next().await
    else {
        panic!("mixed message")
    };
    assert_eq!(sequence, 8);
    assert_eq!(
        token_bindings.keys().copied().collect::<Vec<_>>(),
        ids(&[1, 3])
    );
    assert_eq!(
        token_bindings[&U256::from(3)],
        initial.added[&U256::from(3)],
        "old queued identity cannot be relabeled"
    );
    let WsMessage::PriceChange(message) = message else {
        panic!("one native mixed frame")
    };
    assert_eq!(
        message.price_changes.len(),
        3,
        "native mixed frame remains intact; bindings govern each token"
    );
    assert!(matches!(
        h.next().await,
        MarketSessionEvent::Message {
            sequence: 9,
            message: WsMessage::LastTradePrice(_),
            ..
        }
    ));
    assert_eq!(
        h.scope().await,
        change,
        "filtered-only tail must advance cut without another data message"
    );
    assert_eq!(h.connections.load(Ordering::SeqCst), 1);
    assert_eq!(
        h.client
            .connection_health(ChannelType::Market)
            .unwrap()
            .faults,
        0
    );
    h.frames(vec![book(7, 20)]).await;
    let MarketSessionEvent::Message {
        sequence,
        token_bindings,
        ..
    } = h.next().await
    else {
        panic!("new full snapshot")
    };
    assert_eq!(sequence, 11);
    assert_eq!(token_bindings[&U256::from(7)], change.added[&U256::from(7)]);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn receiver_registration_racing_publication_has_exact_first_sequence() {
    let mut h = Harness::new(&[1], false).await;
    h.scope().await;
    let (done, wait) = oneshot::channel();
    h.commands
        .send(Command::Frames(
            (1..=128)
                .map(|offset| book(1, offset).to_string())
                .collect(),
            done,
        ))
        .unwrap();
    // Existing socket publishes concurrently with this receiver registration.
    let mut second = h.client.market_event_session(ids(&[1])).unwrap();
    wait.await.unwrap();
    h.raw(vec![book(1, 129).to_string()]).await;
    tokio::time::timeout(Duration::from_secs(3), async {
        while h
            .client
            .connection_health(ChannelType::Market)
            .unwrap()
            .messages
            != 129
        {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    let MarketSessionEvent::Scope(initial) = second.next().await.unwrap().unwrap() else {
        panic!("initial scope")
    };
    assert!(initial.sequence_cut <= 128);
    for expected in initial.sequence_cut + 1..=129 {
        let MarketSessionEvent::Message {
            sequence,
            token_bindings,
            message,
            ..
        } = second.next().await.unwrap().unwrap()
        else {
            panic!("message after registration")
        };
        assert_eq!(
            sequence, expected,
            "no missed/relabelled registration-boundary message"
        );
        assert_eq!(token_bindings, initial.added);
        let WsMessage::Book(book) = message else {
            panic!("book")
        };
        assert_eq!(book.timestamp, TS + expected as i64);
    }
    assert!(second.next().now_or_never().is_none());
    assert_eq!(h.connections.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn same_cut_remove_readd_keeps_old_queue_identity_and_fifo_markers() {
    let mut h = Harness::new(&[1], false).await;
    let initial = h.scope().await;
    h.frames(vec![book(1, 1)]).await;
    let removed = h.session.replace_assets(vec![]).unwrap();
    let added = h.session.replace_assets(ids(&[1])).unwrap();
    assert_eq!(removed.sequence_cut, added.sequence_cut);
    assert_ne!(initial.added[&U256::from(1)], added.added[&U256::from(1)]);
    let MarketSessionEvent::Message { token_bindings, .. } = h.next().await else {
        panic!("old queued snapshot")
    };
    assert_eq!(token_bindings, initial.added);
    assert_ne!(
        h.session.membership().binding(U256::from(1)).unwrap(),
        token_bindings.get(&U256::from(1)).copied()
    );
    assert_eq!(h.scope().await, removed);
    assert_eq!(h.scope().await, added);
    h.frames(vec![book(1, 2)]).await;
    let MarketSessionEvent::Message {
        sequence,
        token_bindings,
        ..
    } = h.next().await
    else {
        panic!("new snapshot")
    };
    assert_eq!(sequence, 2);
    assert_eq!(token_bindings, added.added);
}

#[tokio::test]
async fn cancelled_next_preserves_receiver_progress_and_scope_order() {
    let mut h = Harness::new(&[1], false).await;
    h.scope().await;
    for _ in 0..8 {
        assert!(h.session.next().now_or_never().is_none());
    }
    h.frames(vec![book(999, 1), book(1, 2)]).await;
    let change = h.session.replace_assets(ids(&[1, 2])).unwrap();
    let MarketSessionEvent::Message { sequence, .. } = h.next().await else {
        panic!("retained book")
    };
    assert_eq!(sequence, 2);
    assert_eq!(h.scope().await, change);
    assert!(
        h.session.next().now_or_never().is_none(),
        "no duplicate book or marker"
    );
}

#[tokio::test]
async fn empty_scope_pins_socket_and_legacy_unsubscribe_cannot_steal_session_owner() {
    let mut h = Harness::new(&[1], false).await;
    h.scope().await;
    h.request().await;
    let legacy = h.client.subscribe_market_events(ids(&[1])).unwrap();
    h.client.unsubscribe_orderbook(&ids(&[1])).unwrap();
    drop(legacy);
    let empty = h.session.replace_assets(vec![]).unwrap();
    assert_eq!(h.scope().await, empty);
    let request = h.request().await;
    assert_eq!(request["operation"], "unsubscribe");
    assert_eq!(request["assets_ids"], json!(["1"]));
    // Unpaired legacy unsubscribe must not remove the empty session's channel.
    h.client.unsubscribe_orderbook(&ids(&[1])).unwrap();
    assert!(h.session.membership().snapshot().unwrap().connected);
    let added = h.session.replace_assets(ids(&[2])).unwrap();
    assert_eq!(h.scope().await, added);
    let request = h.request().await;
    assert_eq!(request["operation"], "subscribe");
    assert_eq!(request["assets_ids"], json!(["2"]));
    assert_eq!(h.connections.load(Ordering::SeqCst), 1);
    h.frames(vec![book(2, 1)]).await;
    assert!(matches!(
        h.next().await,
        MarketSessionEvent::Message {
            transport_epoch: 1,
            ..
        }
    ));
}

#[tokio::test]
async fn parser_fault_wakes_pending_next_without_any_data_message() {
    let mut h = Harness::new(&[1], false).await;
    h.scope().await;
    assert!(h.session.next().now_or_never().is_none());
    h.raw(vec![r#"{"event_type":"book","asset_id":"1"}"#.into()])
        .await;
    let result = tokio::time::timeout(Duration::from_secs(3), h.session.next())
        .await
        .unwrap()
        .unwrap();
    assert!(result.is_err());
    assert_eq!(
        h.client
            .connection_health(ChannelType::Market)
            .unwrap()
            .messages,
        0
    );
    assert!(h.session.membership().snapshot().is_err());
    assert!(h.session.replace_assets(ids(&[1, 2])).is_err());
    assert!(h.session.next().await.is_none());
}

#[tokio::test]
async fn failed_scope_keeps_registry_ownership_and_cannot_revive_after_reconnect() {
    let mut h = Harness::new(&[1], false).await;
    let original = h.scope().await;
    assert_eq!(h.request().await["assets_ids"], json!(["1"]));
    let original_binding = h.session.membership().binding(U256::from(1)).unwrap();
    assert_eq!(
        original_binding,
        original.added.get(&U256::from(1)).copied()
    );
    h.raw(vec![r#"{"event_type":"book","asset_id":"1"}"#.into()])
        .await;
    tokio::time::timeout(Duration::from_secs(3), async {
        while h
            .client
            .connection_health(ChannelType::Market)
            .unwrap()
            .faults
            == 0
        {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    assert!(h.session.replace_assets(ids(&[2])).is_err());
    assert_eq!(h.client.subscription_count(), 1);
    assert!(h.session.membership().snapshot().is_err());
    assert!(
        h.session.next().await.unwrap().is_err(),
        "no successful replacement Scope marker"
    );
    assert!(h.session.next().await.is_none());
    // Actual shared refcounts: token1 must still be owned; token2 must not
    // have been committed by the failed replace. Token3 is a positive wire
    // sentinel, so an erroneous token2 commit fails by content, not timeout.
    let legacy1 = h.client.subscribe_market_events(ids(&[1])).unwrap();
    let legacy2 = h.client.subscribe_market_events(ids(&[2])).unwrap();
    let legacy3 = h.client.subscribe_market_events(ids(&[3])).unwrap();
    assert_eq!(h.request().await["assets_ids"], json!(["2"]));
    assert_eq!(h.request().await["assets_ids"], json!(["3"]));
    h.close_and_reconnect().await;
    assert!(h.session.replace_assets(ids(&[1])).is_err());
    assert!(h.session.membership().snapshot().is_err());
    assert!(h.session.next().await.is_none());
    drop((legacy1, legacy2, legacy3));
}

#[tokio::test]
async fn hidden_reconnect_revokes_queued_evidence_even_before_baseline_or_while_empty() {
    for (empty, baseline) in [(false, true), (false, false), (true, false)] {
        let mut h = Harness::new(if empty { &[] } else { &[1] }, false).await;
        if baseline {
            h.scope().await;
            h.frames(vec![book(1, 0)]).await;
            assert!(matches!(h.next().await, MarketSessionEvent::Message { .. }));
        }
        // Do not poll the session/watch while the SDK reconnects. The latest
        // visible connection state is Connected again when next is polled.
        h.frames(vec![book(1, 1)]).await;
        h.close_and_reconnect().await;
        h.frames(vec![book(1, 2)]).await;
        assert!(
            h.client
                .connection_state(ChannelType::Market)
                .is_connected()
        );
        assert!(
            h.client
                .connection_health(ChannelType::Market)
                .unwrap()
                .faults
                > 0
        );
        let next = tokio::time::timeout(Duration::from_secs(3), h.session.next())
            .await
            .unwrap()
            .unwrap();
        assert!(
            next.is_err(),
            "old/new socket evidence cannot be admitted as continuous (empty={empty},baseline={baseline})"
        );
        assert!(h.session.membership().snapshot().is_err());
        assert!(h.session.replace_assets(ids(&[2])).is_err());
    }
}

#[tokio::test]
async fn queue_overflow_faults_session_before_exposing_buffered_evidence() {
    let mut h = Harness::new(&[1], false).await;
    h.scope().await;
    h.frames((0..9000).map(|offset| book(1, offset)).collect())
        .await;
    assert!(
        h.client
            .connection_health(ChannelType::Market)
            .unwrap()
            .faults
            > 0
    );
    assert!(h.session.next().await.unwrap().is_err());
    assert!(h.session.next().await.is_none());
    assert!(h.session.membership().snapshot().is_err());
}

#[tokio::test]
async fn undrained_scope_history_has_a_fail_closed_bound() {
    let mut h = Harness::new(&[1], false).await;
    h.scope().await;
    for ordinal in 0..64 {
        h.session.replace_assets(ids(&[2 - ordinal % 2])).unwrap();
    }
    let error = h.session.replace_assets(ids(&[2])).unwrap_err();
    assert!(error.to_string().contains("history bound"));
    assert!(h.session.membership().snapshot().is_err());
    assert!(h.session.next().await.unwrap().is_err());
}

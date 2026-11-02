#![cfg(all(feature = "clob", feature = "ws"))]
//! Read-only scope views must not extend the lifetime of network owners.
use futures_util::{SinkExt, StreamExt};
use polymarket_client_sdk_v2::{
    clob::ws::{Client, MarketSessionEvent},
    types::U256,
};
use std::{io::ErrorKind, time::Duration};
use tokio::sync::oneshot;
use tokio_tungstenite::tungstenite::{Error, Message, error::ProtocolError};

#[derive(Debug, PartialEq, Eq)]
enum AfterDrop {
    SocketClosed,
    FreshProbePong,
}

fn socket_closed(error: &Error) -> bool {
    match error {
        Error::ConnectionClosed | Error::AlreadyClosed => true,
        Error::Io(error) => matches!(
            error.kind(),
            ErrorKind::BrokenPipe
                | ErrorKind::ConnectionReset
                | ErrorKind::ConnectionAborted
                | ErrorKind::UnexpectedEof
        ),
        Error::Protocol(ProtocolError::ResetWithoutClosingHandshake) => true,
        _ => false,
    }
}

#[tokio::test]
async fn retained_readonly_view_does_not_keep_socket_alive_after_last_owner_drop() {
    tokio::time::timeout(Duration::from_secs(5), async {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let endpoint = format!("ws://{}",listener.local_addr().unwrap());
        let (subscribed, subscription_seen) = oneshot::channel();
        let (owners_dropped, wait_for_drop) = oneshot::channel();
        let nonce = uuid::Uuid::new_v4().as_bytes().to_vec();
        let server = tokio::spawn(async move {
            let (tcp,_) = listener.accept().await.unwrap();
            let mut socket = tokio_tungstenite::accept_async(tcp).await.unwrap();
            loop {
                let message = socket.next().await.expect("initial socket ended").unwrap();
                if let Message::Text(text) = message {
                    if text == "PING" {
                        socket.send(Message::Text("PONG".into())).await.unwrap();
                    } else {
                        let value:serde_json::Value = serde_json::from_str(&text).unwrap();
                        if value["operation"] == "subscribe" {
                            assert_eq!(value["assets_ids"],serde_json::json!(["1"]));
                            break;
                        }
                    }
                }
            }
            subscribed.send(()).unwrap();
            wait_for_drop.await.unwrap();

            // This nonce has never appeared on the connection before the
            // owners-dropped barrier. Its Pong is positive task-liveness proof.
            if let Err(error) = socket.send(Message::Ping(nonce.clone().into())).await {
                assert!(socket_closed(&error),"unexpected probe send error: {error}");
                return AfterDrop::SocketClosed;
            }
            loop {
                match socket.next().await {
                    Some(Ok(Message::Pong(value))) if value.as_ref() == nonce.as_slice() => {
                        return AfterDrop::FreshProbePong;
                    }
                    None | Some(Ok(Message::Close(_))) => return AfterDrop::SocketClosed,
                    Some(Err(error)) => {
                        assert!(socket_closed(&error),"unexpected post-drop socket error: {error}");
                        return AfterDrop::SocketClosed;
                    }
                    // Ignore old unsubscribe/text/heartbeat traffic; none of
                    // it can satisfy this newly issued probe's liveness guard.
                    Some(Ok(_)) => {},
                }
            }
        });

        let client = Client::new(&endpoint,Default::default()).unwrap();
        let mut session = client.market_event_session(vec![U256::from(1)]).unwrap();
        let view = session.membership();
        subscription_seen.await.unwrap();
        assert!(matches!(session.next().await.unwrap().unwrap(),MarketSessionEvent::Scope(_)));
        assert!(view.snapshot().unwrap().connected);
        assert!(view.binding(U256::from(1)).unwrap().is_some());
        drop(session);
        drop(client);
        assert!(view.snapshot().is_err(),"retained view must acknowledge closed session");
        assert!(view.binding(U256::from(1)).is_err());
        // Let already-requested task cancellation run before asking the peer
        // for a fresh reply. No timer, grace extension or fake transport close.
        tokio::task::yield_now().await;
        owners_dropped.send(()).unwrap();
        let outcome = server.await.unwrap();
        eprintln!("readonly-view lifetime witness after both owner drops: {outcome:?}");
        assert_eq!(outcome,AfterDrop::SocketClosed,
            "fresh nonce Pong after Client + MarketEventSession drop proves the read-only view still owns the socket task");
        assert!(view.snapshot().is_err());
        drop(view);
    }).await.expect("fixture did not observe socket closure or the fresh probe Pong");
}

#[tokio::test]
async fn session_drop_preserves_overlapping_session_and_legacy_until_final_owner_drop() {
    tokio::time::timeout(Duration::from_secs(5), async {
        use polymarket_client_sdk_v2::clob::ws::WsMessage;
        use serde_json::json;
        use std::collections::BTreeSet;
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let endpoint = format!("ws://{}",listener.local_addr().unwrap());
        let (all_subscribed, all_subscribed_rx) = oneshot::channel();
        let (first_dropped, first_dropped_rx) = oneshot::channel();
        let (second_unsubscribed, second_unsubscribed_rx) = oneshot::channel();
        let (last_owner_dropped, last_owner_dropped_rx) = oneshot::channel();
        let nonce = uuid::Uuid::new_v4().as_bytes().to_vec();
        let server = tokio::spawn(async move {
            let (tcp,_) = listener.accept().await.unwrap();
            let mut socket = tokio_tungstenite::accept_async(tcp).await.unwrap();
            let mut assets = BTreeSet::new();
            while assets != BTreeSet::from(["1".to_string(),"2".to_string(),"3".to_string()]) {
                let message = socket.next().await.unwrap().unwrap();
                if let Message::Text(text) = message {
                    if text == "PING" {
                        socket.send(Message::Text("PONG".into())).await.unwrap();
                    } else {
                        let request:serde_json::Value = serde_json::from_str(&text).unwrap();
                        assert_eq!(request["operation"],"subscribe");
                        assets.extend(request["assets_ids"].as_array().unwrap().iter()
                            .map(|value|value.as_str().unwrap().to_string()));
                    }
                }
            }
            all_subscribed.send(()).unwrap();
            first_dropped_rx.await.unwrap();
            // The client queues this ordinary new subscription AFTER DropA.
            // FIFO makes any premature DropA unsubscribe visible before it.
            loop {
                let message = socket.next().await.unwrap().unwrap();
                if let Message::Text(text) = message {
                    if text == "PING" {
                        socket.send(Message::Text("PONG".into())).await.unwrap();
                    } else {
                        let request:serde_json::Value = serde_json::from_str(&text).unwrap();
                        assert_eq!(request["operation"],"subscribe",
                            "DropA must not unsubscribe any asset still owned by B or legacy");
                        assert_eq!(request["assets_ids"],json!(["99"]),"post-DropA wire sentinel");
                        break;
                    }
                }
            }
            let frame = |token:u64, offset:i64| json!({"event_type":"book","asset_id":token.to_string(),
                "market":format!("0x{}","ab".repeat(32)), "timestamp":(1_800_000_000_000_i64+offset).to_string(),
                "bids":[{"price":"0.4","size":"5"}],"asks":[{"price":"0.6","size":"5"}],"hash":"drop-overlap"});
            for token in [1,2,3] {
                socket.send(Message::Text(frame(token,1).to_string().into())).await.unwrap();
            }
            // Dropping session A releases no wire interest: all its assets
            // still belong to B or the legacy stream. The first unsubscribe
            // must therefore be exactly token2, after session B is dropped.
            loop {
                let message = socket.next().await.unwrap().unwrap();
                if let Message::Text(text) = message {
                    if text == "PING" {
                        socket.send(Message::Text("PONG".into())).await.unwrap();
                    } else {
                        let request:serde_json::Value = serde_json::from_str(&text).unwrap();
                        assert_eq!(request["operation"],"unsubscribe");
                        assert_eq!(request["assets_ids"],json!(["2"]),"session Drop must preserve overlapping owners");
                        break;
                    }
                }
            }
            second_unsubscribed.send(()).unwrap();
            for token in [1,3] {
                socket.send(Message::Text(frame(token,2).to_string().into())).await.unwrap();
            }
            last_owner_dropped_rx.await.unwrap();
            if let Err(error) = socket.send(Message::Ping(nonce.clone().into())).await {
                assert!(socket_closed(&error),"unexpected final probe error: {error}");
                return AfterDrop::SocketClosed;
            }
            loop {
                match socket.next().await {
                    Some(Ok(Message::Pong(value))) if value.as_ref() == nonce.as_slice() => return AfterDrop::FreshProbePong,
                    None | Some(Ok(Message::Close(_))) => return AfterDrop::SocketClosed,
                    Some(Err(error)) => {
                        assert!(socket_closed(&error),"unexpected final socket error: {error}");
                        return AfterDrop::SocketClosed;
                    }
                    Some(Ok(_)) => {},
                }
            }
        });

        let client = Client::new(&endpoint,Default::default()).unwrap();
        let mut first = client.market_event_session(vec![U256::from(1),U256::from(2)]).unwrap();
        let mut second = client.market_event_session(vec![U256::from(2),U256::from(3)]).unwrap();
        let mut legacy = Box::pin(client.subscribe_market_events(vec![U256::from(1),U256::from(3)]).unwrap());
        let first_view = first.membership();
        let second_view = second.membership();
        all_subscribed_rx.await.unwrap();
        assert!(matches!(first.next().await.unwrap().unwrap(),MarketSessionEvent::Scope(_)));
        let MarketSessionEvent::Scope(second_scope) = second.next().await.unwrap().unwrap() else { panic!("second initial scope") };
        drop(first);
        assert!(first_view.snapshot().is_err());
        assert!(first_view.binding(U256::from(1)).is_err());
        assert_eq!(second_view.snapshot().unwrap().assets,second_scope.added);
        let mut sentinel = Box::pin(client.subscribe_market_events(vec![U256::from(99)]).unwrap());
        first_dropped.send(()).unwrap();
        for token in [2,3] {
            let MarketSessionEvent::Message {transport_epoch,token_bindings,message,..} = second.next().await.unwrap().unwrap() else {panic!("retained session book")};
            let WsMessage::Book(book) = message else {panic!("retained session book")};
            assert_eq!(book.asset_id,U256::from(token));
            assert_eq!(book.timestamp,1_800_000_000_001);
            assert_eq!(transport_epoch,second_scope.transport_epoch);
            assert_eq!(token_bindings[&book.asset_id],second_scope.added[&book.asset_id]);
        }
        for token in [1,3] {
            let WsMessage::Book(book) = legacy.next().await.unwrap().unwrap() else {panic!("legacy retained book")};
            assert_eq!((book.asset_id,book.timestamp),(U256::from(token),1_800_000_000_001));
        }
        drop(second);
        assert!(second_view.snapshot().is_err());
        assert!(second_view.binding(U256::from(2)).is_err());
        second_unsubscribed_rx.await.unwrap();
        assert_eq!(client.subscription_count(),2,"legacy and sentinel owners must remain");
        for token in [1,3] {
            let WsMessage::Book(book) = legacy.next().await.unwrap().unwrap() else {panic!("legacy after both session drops")};
            assert_eq!((book.asset_id,book.timestamp),(U256::from(token),1_800_000_000_002));
        }
        drop(client);
        assert!(first_view.snapshot().is_err());
        assert!(second_view.snapshot().is_err());
        tokio::task::yield_now().await;
        last_owner_dropped.send(()).unwrap();
        assert_eq!(server.await.unwrap(),AfterDrop::SocketClosed,
            "held closed views and legacy receiver must not retain the final network owner");
        assert!(legacy.next().await.is_none());
        assert!(sentinel.next().await.is_none());
        assert!(first_view.binding(U256::from(1)).is_err());
        assert!(second_view.binding(U256::from(2)).is_err());
        drop((first_view,second_view,legacy,sentinel));
    }).await.expect("overlap fixture did not observe its real delivery/teardown barriers");
}

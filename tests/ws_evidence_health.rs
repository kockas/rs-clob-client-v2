#![cfg(feature = "ws")]
use futures_util::{SinkExt, StreamExt};
use polymarket_client_sdk_v2::{
    clob::ws::{ChannelType, Client},
    types::U256,
};
use serde_json::json;
use std::time::Duration;

async fn burst(count: i64) {
    tokio::time::timeout(Duration::from_secs(20), async {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let (done, hold) = tokio::sync::oneshot::channel::<()>();
        let server = tokio::spawn(async move {
            let (tcp, _) = listener.accept().await.unwrap();
            let mut ws = tokio_tungstenite::accept_async(tcp).await.unwrap();
            while let Some(Ok(msg)) = ws.next().await {
                if msg.is_text() && msg.to_text().unwrap().contains("assets_ids") { break; }
            }
            for seq in 0..count {
                let frame = json!({"event_type":"book","asset_id":"111",
                    "market":format!("0x{}", "ab".repeat(32)),"timestamp":(1_800_000_000_000_i64+seq).to_string(),
                    "bids":[],"asks":[],"hash":"fixture"});
                ws.send(tokio_tungstenite::tungstenite::Message::Text(frame.to_string().into())).await.unwrap();
                if seq % 64 == 0 { tokio::task::yield_now().await; }
            }
            let _ = hold.await;
        });
        let client = Client::new(&format!("ws://{addr}"), Default::default()).unwrap();
        let mut slow = Box::pin(client.subscribe_market_events(vec![U256::from(111)]).unwrap());
        let mut witness = Box::pin(client.subscribe_orderbook(vec![U256::from(111)]).unwrap());
        while witness.next().await.unwrap().unwrap().timestamp != 1_800_000_000_000 + count - 1 {}
        let health = client.connection_health(ChannelType::Market).unwrap();
        assert_eq!(health.messages, count as u64);
        assert_eq!(health.pending, (count as usize).min(8192));
        assert!(health.oldest_pending_age > Duration::ZERO);
        if count > 8192 {
            // Sticky evidence is observable BEFORE the slow receiver is polled.
            assert_eq!(health.faults, (count - 8192) as u64);
            let error = slow.next().await.unwrap().unwrap_err();
            assert!(error.to_string().contains("evidence gap"));
            assert!(slow.next().await.is_none());
            assert!(client.connection_health(ChannelType::Market).unwrap().faults > 0);
        } else {
            assert_eq!(health.faults, 0);
            for _ in 0..count { slow.next().await.unwrap().unwrap(); }
            assert_eq!(client.connection_health(ChannelType::Market).unwrap().pending, 0);
        }
        done.send(()).unwrap();
        server.await.unwrap();
    }).await.unwrap();
}

#[tokio::test]
async fn ring_holds_8192_without_loss() {
    burst(8192).await;
}

#[tokio::test]
async fn overflow_is_sticky_and_terminates_subscription_explicitly() {
    burst(9000).await;
}

#[test]
fn malformed_subscribed_batch_is_not_silently_filtered() {
    use polymarket_client_sdk_v2::clob::ws::{
        interest::MessageInterest, types::response::parse_if_interested,
    };
    assert!(
        parse_if_interested(
            br#"[{"event_type":"book","asset_id":"111"}]"#,
            &MessageInterest::MARKET
        )
        .is_err()
    );
    assert!(
        parse_if_interested(
            br#"[{"event_type":"future_unknown_kind"}]"#,
            &MessageInterest::MARKET
        )
        .unwrap()
        .is_empty()
    );
}

#[tokio::test]
async fn heartbeat_timeout_is_a_sticky_fault_even_if_socket_does_not_close() {
    tokio::time::timeout(Duration::from_secs(3), async {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let endpoint = format!("ws://{}", listener.local_addr().unwrap());
        let server = tokio::spawn(async move {
            let (tcp, _) = listener.accept().await.unwrap();
            let mut ws = tokio_tungstenite::accept_async(tcp).await.unwrap();
            // Read everything, deliberately never answer application PING.
            while let Some(Ok(_)) = ws.next().await {}
        });
        let mut cfg = polymarket_client_sdk_v2::ws::config::Config::default();
        cfg.heartbeat_interval = Duration::from_millis(10);
        cfg.heartbeat_timeout = Duration::from_millis(30);
        let client = Client::new(&endpoint, cfg).unwrap();
        let _stream = client.subscribe_orderbook(vec![U256::from(111)]).unwrap();
        loop {
            if client
                .connection_health(ChannelType::Market)
                .unwrap()
                .faults
                > 0
            {
                break;
            }
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
        assert!(!client.connection_state(ChannelType::Market).is_connected());
        server.await.unwrap();
    })
    .await
    .unwrap();
}

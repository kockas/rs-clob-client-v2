#![cfg(all(feature = "clob", feature = "ws"))]
//! Private transport transactions, compiled from the exact production file.
//! The public Client loopbacks in market_event_session prove owner propagation.
pub use polymarket_client_sdk_v2::{Result, auth};
pub mod error {
    pub use polymarket_client_sdk_v2::{error::*, ws::WsError};
}
pub mod config {
    pub use polymarket_client_sdk_v2::ws::config::*;
}
pub mod traits {
    pub use polymarket_client_sdk_v2::ws::traits::*;
}
pub mod ws {
    pub use polymarket_client_sdk_v2::ws::WithCredentials;
}
#[path = "../src/ws/connection.rs"]
mod connection;

use serde_json::Value;
use std::{
    sync::{Arc, Mutex},
    time::Duration,
};
use tokio_tungstenite::tungstenite::Message;

#[derive(Clone)]
struct Parser;
impl traits::MessageParser<Value> for Parser {
    fn parse(&self, bytes: &[u8]) -> Result<Vec<Value>> {
        Ok(vec![serde_json::from_slice(bytes)?])
    }
}

#[tokio::test]
async fn actual_closed_sender_rejects_entire_batch_and_latches_fault_without_a_cut() {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let endpoint = format!("ws://{}", listener.local_addr().unwrap());
    drop(listener);
    let mut config = config::Config::default();
    config.reconnect.max_attempts = Some(1);
    let connection = connection::ConnectionManager::new(endpoint, config, Parser).unwrap();
    tokio::time::timeout(Duration::from_secs(3), async {
        while connection.health().faults == 0
            || !matches!(
                connection.state(),
                connection::ConnectionState::Disconnected
            )
        {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    let changes = connection.evidence_changes();
    let mut publication = connection.publication();
    let before = publication.stamp();
    assert!(
        publication
            .enqueue_batch(vec!["first".into(), "second".into()])
            .is_err()
    );
    let after = publication.stamp();
    assert_eq!(
        after.messages, before.messages,
        "no successful scope cut advanced"
    );
    assert_eq!(after.transport_epoch, before.transport_epoch);
    assert_eq!(after.faults, before.faults + 1);
    drop(publication);
    assert!(
        changes.has_changed().unwrap(),
        "failure wakes observers only after publication unlock"
    );
}

#[tokio::test]
async fn actual_batch_writer_stops_after_second_write_failure_without_tail_or_retry() {
    let attempted = Arc::new(Mutex::new(Vec::new()));
    let observed = attempted.clone();
    let sink = futures_util::sink::unfold(0, move |ordinal, message: Message| {
        observed
            .lock()
            .unwrap()
            .push(message.into_text().unwrap().to_string());
        async move {
            if ordinal == 1 {
                Err(std::io::Error::other(
                    "scripted second socket write failure",
                ))
            } else {
                Ok(ordinal + 1)
            }
        }
    });
    let mut sink = Box::pin(sink);
    let failure = connection::write_batch(
        &mut sink,
        vec![
            "subscribe".into(),
            "unsubscribe".into(),
            "must-not-write".into(),
        ],
    )
    .await
    .unwrap_err();
    assert!(failure.to_string().contains("second socket write"));
    assert_eq!(*attempted.lock().unwrap(), vec!["subscribe", "unsubscribe"]);
}

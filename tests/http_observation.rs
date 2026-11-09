//! Synthetic loopback contracts; no network credentials or remote orders.
use alloy::signers::{Signer, local::PrivateKeySigner};
use polymarket_client_sdk_v2::{
    auth::{Credentials, Normal, state::Authenticated},
    clob::{Client, Config},
    error::{Error, Method, Status, StatusCode},
};
use serde_json::{Value, json};
use std::sync::{Arc, Mutex};
use tokio::io::{AsyncReadExt, AsyncWriteExt};

struct Server {
    url: String,
    seen: Arc<Mutex<Vec<(String, String, String, bool)>>>,
    task: tokio::task::JoinHandle<()>,
}
impl Drop for Server {
    fn drop(&mut self) {
        self.task.abort();
    }
}
impl Server {
    async fn new(status: u16, body: Value, headers: &str) -> Self {
        Self::raw(status, body.to_string(), headers).await
    }
    async fn raw(status: u16, body: String, headers: &str) -> Self {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let url = format!("http://{}", listener.local_addr().unwrap());
        let seen = Arc::new(Mutex::new(Vec::new()));
        let records = seen.clone();
        let headers = headers.to_owned();
        let task = tokio::spawn(async move {
            loop {
                let (mut socket, _) = listener.accept().await.unwrap();
                let mut bytes = Vec::new();
                let mut buf = [0; 4096];
                let (method, path, payload, authenticated) = loop {
                    let n = socket.read(&mut buf).await.unwrap();
                    if n == 0 {
                        break (String::new(), String::new(), String::new(), false);
                    }
                    bytes.extend_from_slice(&buf[..n]);
                    assert!(bytes.len() < 128 * 1024);
                    let Some(end) = bytes.windows(4).position(|b| b == b"\r\n\r\n") else {
                        continue;
                    };
                    let head = std::str::from_utf8(&bytes[..end]).unwrap();
                    let len = head
                        .lines()
                        .find_map(|l| {
                            l.to_ascii_lowercase()
                                .strip_prefix("content-length:")
                                .map(|n| n.trim().parse::<usize>().unwrap())
                        })
                        .unwrap_or(0);
                    if bytes.len() < end + 4 + len {
                        continue;
                    }
                    let mut line = head.lines().next().unwrap().split_whitespace();
                    let authenticated = [
                        "poly_address:",
                        "poly_signature:",
                        "poly_api_key:",
                        "poly_timestamp:",
                    ]
                    .iter()
                    .all(|k| head.to_ascii_lowercase().contains(k));
                    break (
                        line.next().unwrap().into(),
                        line.next().unwrap().into(),
                        String::from_utf8(bytes[end + 4..end + 4 + len].to_vec()).unwrap(),
                        authenticated,
                    );
                };
                if method.is_empty() {
                    continue;
                }
                let metadata = match path.as_str() {
                    "/version" => Some(json!({"version":2})),
                    "/tick-size?token_id=123" => Some(json!({"minimum_tick_size":"0.01"})),
                    "/neg-risk?token_id=123" => Some(json!({"neg_risk":false})),
                    _ => None,
                };
                let status = if metadata.is_some() { 200 } else { status };
                let body = metadata
                    .map(|m| m.to_string())
                    .unwrap_or_else(|| body.clone());
                records
                    .lock()
                    .unwrap()
                    .push((method, path, payload, authenticated));
                socket.write_all(format!("HTTP/1.1 {status} Test\r\nContent-Type: application/json\r\nContent-Length: {}\r\n{headers}Connection: close\r\n\r\n{body}",body.len()).as_bytes()).await.unwrap();
            }
        });
        Self { url, seen, task }
    }
    async fn client(&self) -> Client<Authenticated<Normal>> {
        let signer = format!("{:064x}", 1)
            .parse::<PrivateKeySigner>()
            .unwrap()
            .with_chain_id(Some(137));
        Client::new(&self.url, Config::default())
            .unwrap()
            .authentication_builder(&signer)
            .credentials(Credentials::new(
                uuid::Uuid::nil(),
                "dGVzdA==".into(),
                "test".into(),
            ))
            .authenticate()
            .await
            .unwrap()
    }
}
const RATE_HEADERS: &str = "Retry-After: 1\r\nPoly-RateLimit-Remaining: 0\r\nPoly-RateLimit-Reset: 1\r\nPoly-RateLimit-Tier: synthetic\r\nSet-Cookie: DO_NOT_RETAIN\r\n";

#[tokio::test]
async fn actual_tier_uses_authenticated_endpoint_and_response_details() {
    let server=Server::new(200,json!({"tier":"synthetic","tier_details":{"order_rate_per_sec":2,"order_burst":3,"cancel_rate_per_sec":4,"cancel_burst":5,"allow_negative":true,"volume_threshold":0},"period":30}),"").await;
    let client = server.client().await;
    let reply = client
        .rate_limit_tier()
        .await
        .expect("actual authenticated tier read required");
    assert_eq!(reply.data.tier, "synthetic");
    assert_eq!(reply.data.tier_details.order_burst, 3);
    let observation = reply
        .observation
        .expect("successful read must preserve provenance");
    assert_eq!(observation.authenticated_signer(), Some(client.address()));
    assert!(observation.same_endpoint());
    assert_eq!(
        server.seen.lock().unwrap().as_slice(),
        &[(
            "GET".into(),
            "/v1/user/rate-limit-tier".into(),
            "".into(),
            true
        )]
    );
}

#[tokio::test]
async fn genuine_error_preserves_status_and_bounded_transport_metadata() {
    let server = Server::new(
        429,
        json!({"error":"too many post orders requests"}),
        RATE_HEADERS,
    )
    .await;
    let client = server.client().await;
    let error = client.post_orders_observed(vec![]).await.unwrap_err();
    let status = error
        .downcast_ref::<Status>()
        .expect("typed Status must remain directly downcastable");
    assert_eq!(status.status_code, StatusCode::TOO_MANY_REQUESTS);
    let m = status
        .response_metadata()
        .expect("real response headers were lost");
    assert_eq!(m.method(), &Method::POST);
    assert_eq!(m.path(), "/orders");
    assert_eq!(m.authenticated_signer(), Some(client.address()));
    assert!(m.same_endpoint());
    assert!(m.matches_body(&status.message));
    assert!(!m.matches_body("tampered"));
    assert_eq!(m.retry_after(), Some("1"));
    assert_eq!(m.rate_remaining(), Some("0"));
    assert_eq!(m.rate_reset(), Some("1"));
    assert_eq!(m.rate_tier(), Some("synthetic"));
    assert!(!format!("{m:?}").contains("DO_NOT_RETAIN"));
    let synthetic = Error::status(
        StatusCode::TOO_MANY_REQUESTS,
        Method::POST,
        "/orders".into(),
        status.message.clone(),
    );
    assert!(
        synthetic
            .downcast_ref::<Status>()
            .unwrap()
            .response_metadata()
            .is_none()
    );
}

#[tokio::test]
async fn ordinary_and_observed_methods_have_identical_wire_contracts() {
    let posts = Server::new(200, json!([]), RATE_HEADERS).await;
    let client = posts.client().await;
    assert!(client.post_orders(vec![]).await.unwrap().is_empty());
    let observed = client.post_orders_observed(vec![]).await.unwrap();
    assert!(observed.data.is_empty());
    assert!(observed.observation.is_some());
    let seen = posts.seen.lock().unwrap();
    assert_eq!(seen.len(), 2);
    assert_eq!(seen[0], seen[1]);
    drop(seen);
    let cancels = Server::new(
        200,
        json!({"canceled":["id"],"not_canceled":{}}),
        RATE_HEADERS,
    )
    .await;
    let client = cancels.client().await;
    assert_eq!(
        client.cancel_orders(&["id"]).await.unwrap().canceled,
        vec!["id"]
    );
    assert!(
        client
            .cancel_orders_observed(&["id"])
            .await
            .unwrap()
            .observation
            .is_some()
    );
    let seen = cancels.seen.lock().unwrap();
    assert_eq!(seen.len(), 2);
    assert_eq!(seen[0], seen[1]);
}

#[tokio::test]
async fn redirects_never_replay_an_authenticated_post_or_gain_endpoint_authority() {
    let target = Server::new(
        429,
        json!({"error":"too many post orders requests"}),
        RATE_HEADERS,
    )
    .await;
    let redirect = Server::new(
        307,
        json!({}),
        &format!("Location: {}/orders\r\n", target.url),
    )
    .await;
    let error = redirect
        .client()
        .await
        .post_orders_observed(vec![])
        .await
        .unwrap_err();
    assert_eq!(
        error.downcast_ref::<Status>().unwrap().status_code.as_u16(),
        307,
        "a redirect is not a second authenticated POST or a venue limiter response"
    );
    assert!(target.seen.lock().unwrap().is_empty());
    assert_eq!(redirect.seen.lock().unwrap().len(), 1);
}

#[tokio::test]
async fn duplicate_or_oversized_rate_headers_are_not_authoritative() {
    for headers in [
        format!("{RATE_HEADERS}Retry-After: 2\r\n"),
        format!("Retry-After: {}\r\n", "1".repeat(257)),
    ] {
        let server = Server::new(
            429,
            json!({"error":"too many post orders requests"}),
            &headers,
        )
        .await;
        let error = server
            .client()
            .await
            .post_orders_observed(vec![])
            .await
            .unwrap_err();
        let observation = error
            .downcast_ref::<Status>()
            .unwrap()
            .response_metadata()
            .unwrap();
        assert!(observation.retry_after().is_none());
    }
}

#[derive(Clone, Default)]
struct Logs(Arc<Mutex<Vec<u8>>>);
impl std::io::Write for Logs {
    fn write(&mut self, b: &[u8]) -> std::io::Result<usize> {
        self.0.lock().unwrap().extend_from_slice(b);
        Ok(b.len())
    }
    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}
impl<'a> tracing_subscriber::fmt::MakeWriter<'a> for Logs {
    type Writer = Self;
    fn make_writer(&'a self) -> Self {
        self.clone()
    }
}
#[tokio::test(flavor = "current_thread")]
async fn missing_authenticated_order_is_debug_but_unrelated_errors_stay_warn() {
    let logs = Logs::default();
    let subscriber = tracing_subscriber::fmt()
        .with_max_level(tracing::Level::DEBUG)
        .with_ansi(false)
        .without_time()
        .with_writer(logs.clone())
        .finish();
    tracing::subscriber::set_global_default(subscriber).unwrap();
    for body in [json!({"error":"missing"}), Value::Null] {
        let server = Server::new(if body.is_null() { 200 } else { 404 }, body, "").await;
        let client = server.client().await;
        let id = format!("0x{}", "ab".repeat(32));
        logs.0.lock().unwrap().clear();
        let error = client.order(&id).await.unwrap_err();
        assert_eq!(
            error.downcast_ref::<Status>().unwrap().status_code,
            StatusCode::NOT_FOUND
        );
        let text = String::from_utf8(logs.0.lock().unwrap().clone()).unwrap();
        assert!(
            !text
                .lines()
                .any(|l| l.contains("WARN") && l.contains(&format!("path=/data/order/{id}"))),
            "expected order lookup spam: {text}"
        );
        assert!(
            text.lines()
                .any(|l| l.contains("DEBUG") && l.contains(&format!("path=/data/order/{id}")))
        );
    }
    for status in [401, 403, 404, 500] {
        let server = Server::new(status, json!({"error":"failure"}), "").await;
        let client = server.client().await;
        logs.0.lock().unwrap().clear();
        let error = client.api_keys().await.unwrap_err();
        assert_eq!(
            error.downcast_ref::<Status>().unwrap().status_code.as_u16(),
            status
        );
        assert!(
            String::from_utf8(logs.0.lock().unwrap().clone())
                .unwrap()
                .lines()
                .any(|l| l.contains("WARN") && l.contains("path=/auth/api-keys"))
        );
    }
}

#[tokio::test]
async fn received_decode_schema_and_null_errors_keep_observation_and_original_source() {
    for (body, kind) in [("{", "decode"), ("{}", "schema"), ("null", "null")] {
        let server = Server::raw(200, body.into(), RATE_HEADERS).await;
        let error = server
            .client()
            .await
            .post_orders_observed(vec![])
            .await
            .unwrap_err();
        let m = error
            .response_metadata()
            .expect("received-response evidence lost on decode/null failure");
        assert_eq!(m.status(), StatusCode::OK);
        assert_eq!(m.path(), "/orders");
        assert_eq!(m.retry_after(), Some("1"));
        assert!(m.authenticated_signer().is_some());
        match kind {
            "decode" => assert!(error.downcast_ref::<reqwest::Error>().unwrap().is_decode()),
            "schema" => assert!(error.downcast_ref::<serde_json::Error>().is_some()),
            _ => assert_eq!(
                error.downcast_ref::<Status>().unwrap().status_code,
                StatusCode::NOT_FOUND
            ),
        }
    }
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!("http://{}", listener.local_addr().unwrap());
    drop(listener);
    let client = Client::new(&url, Config::default()).unwrap();
    assert!(
        client
            .server_time()
            .await
            .unwrap_err()
            .response_metadata()
            .is_none()
    );
}
#[tokio::test]
async fn nonempty_signed_single_and_batch_posts_have_parity_and_never_poll_hashes() {
    use polymarket_client_sdk_v2::clob::types::{OrderType, Side};
    use polymarket_client_sdk_v2::types::{Decimal, U256};
    let response = json!({"success":true,"orderID":format!("0x{}","ab".repeat(32)),"status":"live","errorMsg":"","makingAmount":"0","takingAmount":"0","tradeIDs":[],"transactionsHashes":[]});
    for batch in [false, true] {
        let server = Server::new(
            200,
            if batch {
                json!([response.clone()])
            } else {
                response.clone()
            },
            RATE_HEADERS,
        )
        .await;
        let client = server.client().await;
        let signer = format!("{:064x}", 1)
            .parse::<PrivateKeySigner>()
            .unwrap()
            .with_chain_id(Some(137));
        let signable = client
            .limit_order()
            .token_id(U256::from(123))
            .side(Side::Buy)
            .price("0.40".parse::<Decimal>().unwrap())
            .size(Decimal::from(5))
            .order_type(OrderType::GTC)
            .post_only(true)
            .build()
            .await
            .unwrap();
        let first = client.sign(&signer, signable.clone()).await.unwrap();
        let second = client.sign(&signer, signable).await.unwrap();
        let before = server.seen.lock().unwrap().len();
        if batch {
            assert_eq!(client.post_orders(vec![first]).await.unwrap().len(), 1);
            let r = client.post_orders_observed(vec![second]).await.unwrap();
            assert_eq!(r.data.len(), 1);
            assert!(r.observation.is_some());
        } else {
            assert!(
                client
                    .post_order(first)
                    .await
                    .unwrap()
                    .transaction_hashes
                    .is_empty()
            );
            let r = client.post_order_observed(second).await.unwrap();
            assert!(r.data.success);
            assert!(r.observation.is_some());
        }
        let seen = server.seen.lock().unwrap();
        assert_eq!(seen.len(), before + 2, "no follow-up hash/trade polling");
        assert_eq!(seen[before], seen[before + 1]);
        assert!(seen[before].3);
        assert!(seen[before].2.contains("signature"));
    }
}
#[tokio::test]
async fn ordinary_reads_also_surface_redirect_without_following() {
    let target = Server::new(200, json!(123), "").await;
    let source = Server::new(
        302,
        json!({}),
        &format!("Location: {}/time\r\n", target.url),
    )
    .await;
    let error = Client::new(&source.url, Config::default())
        .unwrap()
        .server_time()
        .await
        .unwrap_err();
    assert_eq!(
        error.downcast_ref::<Status>().unwrap().status_code.as_u16(),
        302
    );
    assert!(target.seen.lock().unwrap().is_empty());
}

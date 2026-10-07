#![cfg(feature = "clob")]
//! Deterministic offline regression tests for revision-fenced SDK metadata.
use alloy::{
    primitives::U256,
    signers::{Signer, local::PrivateKeySigner},
};
use polymarket_client_sdk_v2::clob::{metadata::MetadataError, types::Amount};
use polymarket_client_sdk_v2::{
    auth::{Credentials, Normal, state::Authenticated},
    clob::{
        Client, Config,
        types::{OrderType, Side, SignableOrder, TickSize},
    },
};
use rust_decimal::Decimal;
use serde_json::{Value, json};
use std::{
    collections::{HashMap, VecDeque},
    future::Future,
    sync::{Arc, Mutex},
    time::Duration,
};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    sync::Semaphore,
    task::JoinHandle,
};

type Sdk = Client<Authenticated<Normal>>;
const TOKEN: u64 = 123;
const OTHER: u64 = 456;
const CONDITION: &str = "0x0000000000000000000000000000000000000000000000000000000000000001";
fn bulk(tick: &str, neg: bool, tokens: &[u64], fee: Option<&str>) -> Value {
    let mut body = json!({"c":CONDITION,"mts":tick,"nr":neg,"mos":"5",
        "t":tokens.iter().map(|id|json!({"t":id.to_string(),"o":"test"})).collect::<Vec<_>>()});
    if let Some(fee) = fee {
        body["fd"] = json!({"r":fee,"e":1,"to":true});
    }
    body
}

async fn bounded<T>(future: impl Future<Output = T>) -> T {
    tokio::time::timeout(Duration::from_secs(5), future)
        .await
        .expect("offline operation hung")
}

#[derive(Clone)]
struct Gate {
    entered: Arc<Semaphore>,
    release: Arc<Semaphore>,
}
impl Gate {
    fn new() -> Self {
        Self {
            entered: Arc::new(Semaphore::new(0)),
            release: Arc::new(Semaphore::new(0)),
        }
    }
    async fn entered(&self) {
        bounded(self.entered.acquire()).await.unwrap().forget();
    }
    fn release(&self) {
        self.release.add_permits(1);
    }
}
struct Reply {
    status: u16,
    body: String,
    gate: Option<Gate>,
}
impl Reply {
    fn ok(body: Value) -> Self {
        Self {
            status: 200,
            body: body.to_string(),
            gate: None,
        }
    }
    fn held(body: Value, gate: Gate) -> Self {
        Self {
            gate: Some(gate),
            ..Self::ok(body)
        }
    }
}
#[derive(Clone, Debug)]
struct Request {
    method: String,
    path: String,
    headers: Vec<(String, String)>,
    body: String,
}

async fn read_request(socket: &mut tokio::net::TcpStream) -> Request {
    let mut raw = Vec::new();
    let mut buf = [0; 4096];
    loop {
        let n = socket.read(&mut buf).await.unwrap();
        assert_ne!(n, 0);
        raw.extend_from_slice(&buf[..n]);
        assert!(raw.len() <= 16 * 1024, "unexpectedly large offline request");
        if let Some(end) = raw.windows(4).position(|s| s == b"\r\n\r\n") {
            let head = std::str::from_utf8(&raw[..end]).unwrap();
            let headers: Vec<_> = head
                .lines()
                .skip(1)
                .map(|s| {
                    let (k, v) = s.split_once(':').unwrap();
                    (k.to_ascii_lowercase(), v.trim().to_string())
                })
                .collect();
            let size = headers
                .iter()
                .find(|(k, _)| k == "content-length")
                .map_or(0, |(_, v)| v.parse::<usize>().unwrap());
            assert!(size <= 16 * 1024);
            if raw.len() >= end + 4 + size {
                let mut line = head.lines().next().unwrap().split_whitespace();
                return Request {
                    method: line.next().unwrap().into(),
                    path: line.next().unwrap().into(),
                    headers,
                    body: String::from_utf8(raw[end + 4..end + 4 + size].to_vec()).unwrap(),
                };
            }
        }
    }
}

struct Server {
    url: String,
    requests: Arc<Mutex<Vec<Request>>>,
    replies: Arc<Mutex<HashMap<String, VecDeque<Reply>>>>,
    task: JoinHandle<()>,
}
impl Drop for Server {
    fn drop(&mut self) {
        self.task.abort();
    }
}
impl Server {
    async fn new(version: u32, neg_risk: bool) -> Self {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let url = format!("http://{}", listener.local_addr().unwrap());
        let requests = Arc::new(Mutex::new(Vec::<Request>::new()));
        let replies = Arc::new(Mutex::new(HashMap::<String, VecDeque<Reply>>::new()));
        let (seen, plans) = (requests.clone(), replies.clone());
        let task = tokio::spawn(async move {
            let mut handlers = tokio::task::JoinSet::new();
            loop {
                tokio::select! {
                    incoming = listener.accept() => {
                        let (mut socket, _) = incoming.unwrap();
                        let (seen, plans) = (seen.clone(), plans.clone());
                        handlers.spawn(async move {
                            let req = bounded(read_request(&mut socket)).await;
                            if req.method == "POST" || req.path.starts_with("/fees/builder-fees/") {
                                assert_auth(&req);
                            }
                            let key = format!("{} {}", req.method, req.path);
                            seen.lock().unwrap().push(req.clone());
                            let plan = plans.lock().unwrap().get_mut(&key).and_then(VecDeque::pop_front);
                            let reply = plan.unwrap_or_else(|| {
                                match (req.method.as_str(), req.path.split('?').next().unwrap()) {
                                    ("GET", "/version") => Reply::ok(json!({"version":version})),
                                    ("GET", "/tick-size") => Reply::ok(json!({"minimum_tick_size":"0.01"})),
                                    ("GET", "/neg-risk") => Reply::ok(json!({"neg_risk":neg_risk})),
                                    ("GET", "/fee-rate") => Reply::ok(json!({"base_fee":25})),
                                    ("GET", p) if p.starts_with("/markets-by-token/") => Reply::ok(json!({"condition_id":CONDITION,"primary_token_id":"123","secondary_token_id":"456"})),
                                    ("GET", p) if p.starts_with("/clob-markets/") => Reply::ok(bulk("0.01", false, &[TOKEN, OTHER], Some("0.07"))),
                                    ("GET", p) if p.starts_with("/fees/builder-fees/") => Reply::ok(json!({"builderMakerFeeRateBps":0,"builderTakerFeeRateBps":10})),
                                    ("POST", "/order") => Reply { status:400, body:json!({"error":"order_version_mismatch"}).to_string(), gate:None },
                                    ("POST", "/v1/heartbeats") => Reply::ok(json!({"heartbeat_id":uuid::Uuid::nil()})),
                                    _ => panic!("unexpected offline request: {key}"),
                                }
                            });
                            if let Some(gate) = reply.gate {
                                gate.entered.add_permits(1);
                                bounded(gate.release.acquire()).await.unwrap().forget();
                            }
                            // A deliberately cancelled client may close before a held reply.
                            let _ = socket.write_all(format!(
                                "HTTP/1.1 {} Test\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}",
                                reply.status, reply.body.len(), reply.body
                            ).as_bytes()).await;
                        });
                    }
                    joined = handlers.join_next(), if !handlers.is_empty() => { joined.unwrap().unwrap(); }
                }
            }
        });
        Self {
            url,
            requests,
            replies,
            task,
        }
    }
    fn plan(&self, path: &str, reply: Reply) {
        self.replies
            .lock()
            .unwrap()
            .entry(format!("GET {path}"))
            .or_default()
            .push_back(reply);
    }
    fn seen(&self) -> Vec<String> {
        assert!(!self.task.is_finished(), "offline server failed");
        self.requests
            .lock()
            .unwrap()
            .iter()
            .map(|r| format!("{} {}", r.method, r.path))
            .collect()
    }
    fn assert_seen(&self, expected: &[&str]) {
        assert_eq!(self.seen(), expected);
    }
}

fn signer() -> PrivateKeySigner {
    format!("{:064x}", 1)
        .parse::<PrivateKeySigner>()
        .unwrap()
        .with_chain_id(Some(137))
}
async fn client(server: &Server) -> Sdk {
    bounded(
        Client::new(&server.url, Config::default())
            .unwrap()
            .authentication_builder(&signer())
            .credentials(Credentials::new(
                uuid::Uuid::nil(),
                "dGVzdA==".into(),
                "test".into(),
            ))
            .salt_generator(|| 42)
            .authenticate(),
    )
    .await
    .unwrap()
}
async fn build(sdk: &Sdk, token: u64) -> polymarket_client_sdk_v2::Result<SignableOrder> {
    bounded(
        sdk.limit_order()
            .token_id(U256::from(token))
            .side(Side::Buy)
            .price(Decimal::new(4, 1))
            .size(Decimal::new(5, 0))
            .order_type(OrderType::FAK)
            .post_only(false)
            .build(),
    )
    .await
}
async fn build_sign_hash(sdk: &Sdk, token: u64) {
    let signable = build(sdk, token).await.unwrap();
    let signed = bounded(sdk.sign(&signer(), signable)).await.unwrap();
    bounded(sdk.order_signing_hash(&signer(), &signed.payload))
        .await
        .unwrap();
}
async fn warm(sdk: &Sdk, token: u64) {
    let version = bounded(sdk.version()).await.unwrap();
    bounded(sdk.tick_size(U256::from(token))).await.unwrap();
    bounded(sdk.neg_risk(U256::from(token))).await.unwrap();
    if version == 1 {
        bounded(sdk.fee_rate_bps(U256::from(token))).await.unwrap();
    }
}

fn assert_auth(request: &Request) {
    use base64::{Engine, engine::general_purpose::URL_SAFE};
    use hmac::{Hmac, Mac};
    let header = |name: &str| {
        request
            .headers
            .iter()
            .find(|(k, _)| k == name)
            .unwrap()
            .1
            .as_str()
    };
    let message = format!(
        "{}{}{}{}",
        header("poly_timestamp"),
        request.method,
        request.path,
        request.body
    );
    let mut mac = Hmac::<sha2::Sha256>::new_from_slice(b"test").unwrap();
    mac.update(message.as_bytes());
    assert_eq!(
        header("poly_signature"),
        URL_SAFE.encode(mac.finalize().into_bytes())
    );
    assert_eq!(header("poly_api_key"), uuid::Uuid::nil().to_string());
    assert_eq!(header("poly_passphrase"), "test");
}

#[derive(Clone, Copy, Debug)]
enum Field {
    Tick,
    Neg,
    Fee,
    Builder,
    Version,
}
impl Field {
    async fn read_view(
        self,
        view: &polymarket_client_sdk_v2::clob::metadata_view::MetadataView<Authenticated<Normal>>,
    ) -> polymarket_client_sdk_v2::Result<()> {
        match self {
            Self::Tick => {
                view.tick_size(U256::from(TOKEN)).await?;
            }
            Self::Neg => {
                view.neg_risk(U256::from(TOKEN)).await?;
            }
            Self::Fee => {
                view.fee_rate_bps(U256::from(TOKEN)).await?;
            }
            Self::Builder => {
                view.builder_fee_rate(alloy::primitives::B256::ZERO).await?;
            }
            Self::Version => {
                view.version().await?;
            }
        }
        Ok(())
    }
    fn path(self) -> String {
        match self {
            Self::Tick => "/tick-size?token_id=123".into(),
            Self::Neg => "/neg-risk?token_id=123".into(),
            Self::Fee => "/fee-rate?token_id=123".into(),
            Self::Builder => format!("/fees/builder-fees/{}", alloy::primitives::B256::ZERO),
            Self::Version => "/version".into(),
        }
    }
    fn reply(self, newer: bool) -> Value {
        match self {
            Self::Tick => json!({"minimum_tick_size":if newer {"0.001"} else {"0.01"}}),
            Self::Neg => json!({"neg_risk":newer}),
            Self::Fee => json!({"base_fee":if newer {10} else {25}}),
            Self::Builder => {
                json!({"builderMakerFeeRateBps":0,"builderTakerFeeRateBps":if newer {50} else {10}})
            }
            Self::Version => json!({"version":if newer {2} else {1}}),
        }
    }
    async fn read(self, sdk: &Sdk) -> polymarket_client_sdk_v2::Result<Value> {
        Ok(match self {
            Self::Tick => json!(
                sdk.tick_size(U256::from(TOKEN))
                    .await?
                    .minimum_tick_size
                    .as_decimal()
                    .to_string()
            ),
            Self::Neg => json!(sdk.neg_risk(U256::from(TOKEN)).await?.neg_risk),
            Self::Fee => json!(sdk.fee_rate_bps(U256::from(TOKEN)).await?.base_fee),
            Self::Builder => json!(
                sdk.builder_fee_rate(alloy::primitives::B256::ZERO)
                    .await?
                    .builder_taker_fee_rate_bps
            ),
            Self::Version => json!(sdk.version().await?),
        })
    }
    fn set(self, sdk: &Sdk) {
        match self {
            Self::Tick => sdk.set_tick_size(U256::from(TOKEN), TickSize::Thousandth),
            Self::Neg => sdk.set_neg_risk(U256::from(TOKEN), true),
            Self::Fee => sdk.set_fee_rate_bps(U256::from(TOKEN), 10),
            Self::Builder | Self::Version => unreachable!(),
        }
    }
}
const FIELDS: [Field; 5] = [
    Field::Tick,
    Field::Neg,
    Field::Fee,
    Field::Builder,
    Field::Version,
];
macro_rules! enter {
    ($future:ident, $gate:expr) => {
        tokio::select! {
            _ = $gate.entered() => {},
            _ = &mut $future => panic!("held request completed before release"),
        }
    };
}
fn changed<T: std::fmt::Debug>(result: polymarket_client_sdk_v2::Result<T>) {
    assert_eq!(
        result.unwrap_err().downcast_ref::<MetadataError>(),
        Some(&MetadataError::Changed)
    );
}
async fn market(sdk: &Sdk) -> polymarket_client_sdk_v2::Result<SignableOrder> {
    bounded(
        sdk.market_order()
            .token_id(U256::from(TOKEN))
            .side(Side::Buy)
            .price(Decimal::new(4, 1))
            .amount(Amount::usdc(Decimal::new(2, 0)).unwrap())
            .user_usdc_balance(Decimal::new(2, 0))
            .build(),
    )
    .await
}

#[tokio::test]
async fn setters_reject_held_obsolete_values_and_clone_observes_new_revision() {
    for field in [Field::Tick, Field::Neg, Field::Fee] {
        let server = Server::new(2, false).await;
        let sdk = client(&server).await;
        let revision = sdk.metadata_revision().unwrap();
        let gate = Gate::new();
        server.plan(&field.path(), Reply::held(field.reply(false), gate.clone()));
        let mut old = Box::pin(field.read(&sdk));
        enter!(old, gate);
        let clone = sdk.clone();
        field.set(&clone);
        assert!(!sdk.metadata_is_current(&revision));
        let new = bounded(field.read(&sdk)).await.unwrap();
        gate.release();
        changed(bounded(old).await);
        assert_eq!(bounded(field.read(&clone)).await.unwrap(), new);
        assert_eq!(server.seen().len(), 1);
    }
}

#[tokio::test]
async fn invalidation_fences_every_single_field_getter_and_recovers() {
    for field in FIELDS {
        let server = Server::new(2, false).await;
        let sdk = client(&server).await;
        let gate = Gate::new();
        server.plan(&field.path(), Reply::held(field.reply(true), gate.clone()));
        let mut old = Box::pin(field.read(&sdk));
        enter!(old, gate);
        sdk.invalidate_internal_caches();
        gate.release();
        changed(bounded(old).await);
        let fresh = bounded(field.read(&sdk)).await.unwrap();
        assert_eq!(bounded(field.read(&sdk)).await.unwrap(), fresh);
        assert_eq!(server.seen().len(), 2);
    }
}

#[tokio::test]
async fn duplicate_same_revision_requests_return_first_committed_value_without_epoch_change() {
    for field in FIELDS {
        let server = Server::new(2, false).await;
        let sdk = client(&server).await;
        let revision = sdk.metadata_revision().unwrap();
        let a = Gate::new();
        let b = Gate::new();
        server.plan(&field.path(), Reply::held(field.reply(false), a.clone()));
        server.plan(&field.path(), Reply::held(field.reply(true), b.clone()));
        let mut first = Box::pin(field.read(&sdk));
        enter!(first, a);
        let mut second = Box::pin(field.read(&sdk));
        enter!(second, b);
        b.release();
        let newer = bounded(second).await.unwrap();
        a.release();
        assert_eq!(bounded(first).await.unwrap(), newer);
        assert_eq!(bounded(field.read(&sdk)).await.unwrap(), newer);
        assert!(sdk.metadata_is_current(&revision));
        assert_eq!(server.seen().len(), 2);
    }
}

#[tokio::test]
async fn bulk_refresh_is_atomic_epoch_change_and_fences_pending_single_field_getters() {
    for field in [Field::Tick, Field::Neg, Field::Fee] {
        let server = Server::new(2, false).await;
        let sdk = client(&server).await;
        let revision = sdk.metadata_revision().unwrap();
        let gate = Gate::new();
        server.plan(&field.path(), Reply::held(field.reply(true), gate.clone()));
        let mut old = Box::pin(field.read(&sdk));
        enter!(old, gate);
        bounded(sdk.clob_market_info(CONDITION)).await.unwrap();
        assert!(!sdk.metadata_is_current(&revision));
        gate.release();
        changed(bounded(old).await);
        for token in [TOKEN, OTHER] {
            assert_eq!(
                bounded(sdk.tick_size(U256::from(token)))
                    .await
                    .unwrap()
                    .minimum_tick_size,
                TickSize::Hundredth
            );
            assert!(
                !bounded(sdk.neg_risk(U256::from(token)))
                    .await
                    .unwrap()
                    .neg_risk
            );
        }
        assert_eq!(server.seen().len(), 2);
        let revision = sdk.metadata_revision().unwrap();
        server.plan(
            &format!("/clob-markets/{CONDITION}"),
            Reply::ok(bulk("0.001", true, &[TOKEN, OTHER], None)),
        );
        bounded(sdk.clob_market_info(CONDITION)).await.unwrap();
        assert!(!sdk.metadata_is_current(&revision));
        for token in [TOKEN, OTHER] {
            assert_eq!(
                bounded(sdk.tick_size(U256::from(token)))
                    .await
                    .unwrap()
                    .minimum_tick_size,
                TickSize::Thousandth
            );
            assert!(
                bounded(sdk.neg_risk(U256::from(token)))
                    .await
                    .unwrap()
                    .neg_risk
            );
        }
        assert_eq!(server.seen().len(), 3);
    }
}

#[tokio::test]
async fn obsolete_bulk_response_cannot_partially_install_other_tokens() {
    let server = Server::new(2, false).await;
    let sdk = client(&server).await;
    let gate = Gate::new();
    server.plan(
        &format!("/clob-markets/{CONDITION}"),
        Reply::held(bulk("0.001", true, &[TOKEN, OTHER], None), gate.clone()),
    );
    let mut old = Box::pin(sdk.clob_market_info(CONDITION));
    enter!(old, gate);
    sdk.set_tick_size(U256::from(TOKEN), TickSize::Hundredth);
    gate.release();
    changed(bounded(old).await);
    assert_eq!(
        bounded(sdk.tick_size(U256::from(TOKEN)))
            .await
            .unwrap()
            .minimum_tick_size,
        TickSize::Hundredth
    );
    assert_eq!(
        bounded(sdk.tick_size(U256::from(OTHER)))
            .await
            .unwrap()
            .minimum_tick_size,
        TickSize::Hundredth
    );
    assert!(
        !bounded(sdk.neg_risk(U256::from(TOKEN)))
            .await
            .unwrap()
            .neg_risk
    );
    server.assert_seen(&[
        &format!("GET /clob-markets/{CONDITION}"),
        "GET /tick-size?token_id=456",
        "GET /neg-risk?token_id=123",
    ]);
}

#[tokio::test]
async fn competing_bulk_refreshes_do_not_overwrite_first_accepted_response() {
    let server = Server::new(2, false).await;
    let sdk = client(&server).await;
    let a = Gate::new();
    let b = Gate::new();
    let path = format!("/clob-markets/{CONDITION}");
    server.plan(
        &path,
        Reply::held(bulk("0.01", false, &[TOKEN, OTHER], None), a.clone()),
    );
    server.plan(
        &path,
        Reply::held(bulk("0.001", true, &[TOKEN, OTHER], None), b.clone()),
    );
    let mut first = Box::pin(sdk.clob_market_info(CONDITION));
    enter!(first, a);
    let mut second = Box::pin(sdk.clob_market_info(CONDITION));
    enter!(second, b);
    b.release();
    bounded(second).await.unwrap();
    a.release();
    changed(bounded(first).await);
    assert_eq!(
        bounded(sdk.tick_size(U256::from(TOKEN)))
            .await
            .unwrap()
            .minimum_tick_size,
        TickSize::Thousandth
    );
    assert_eq!(server.seen().len(), 2);
}

#[tokio::test]
async fn v2_market_fee_lookup_cannot_cross_invalidation_at_either_network_boundary() {
    for at_bulk in [false, true] {
        let server = Server::new(2, false).await;
        let sdk = client(&server).await;
        let gate = Gate::new();
        let path = if at_bulk {
            format!("/clob-markets/{CONDITION}")
        } else {
            "/markets-by-token/123".into()
        };
        let body = if at_bulk {
            bulk("0.001", true, &[TOKEN, OTHER], None)
        } else {
            json!({"condition_id":CONDITION,"primary_token_id":"123","secondary_token_id":"456"})
        };
        server.plan(&path, Reply::held(body, gate.clone()));
        let mut pending = Box::pin(market(&sdk));
        enter!(pending, gate);
        sdk.invalidate_internal_caches();
        gate.release();
        changed(bounded(pending).await);
        let before = server.seen().len();
        let fresh = market(&sdk).await.unwrap();
        assert!(fresh.order().makerAmount < U256::from(2_000_000));
        let after = server.seen();
        assert!(
            after[before..].contains(&"GET /markets-by-token/123".to_string()),
            "stale mapping must not survive"
        );
        assert!(after[before..].contains(&format!("GET /clob-markets/{CONDITION}")));
        assert_eq!(
            bounded(sdk.tick_size(U256::from(TOKEN)))
                .await
                .unwrap()
                .minimum_tick_size,
            TickSize::Hundredth
        );
    }
}

#[tokio::test]
async fn omitted_token_is_unavailable_not_zero_fee_and_bulk_installs_nothing() {
    let server = Server::new(2, false).await;
    let sdk = client(&server).await;
    server.plan(
        &format!("/clob-markets/{CONDITION}"),
        Reply::ok(bulk("0.001", true, &[OTHER], None)),
    );
    let error = market(&sdk).await.unwrap_err();
    assert_eq!(
        error.downcast_ref::<MetadataError>(),
        Some(&MetadataError::Unavailable)
    );
    let before = server.seen().len();
    assert_eq!(
        bounded(sdk.tick_size(U256::from(OTHER)))
            .await
            .unwrap()
            .minimum_tick_size,
        TickSize::Hundredth
    );
    assert_eq!(server.seen().len(), before + 1);
    assert!(
        !bounded(sdk.neg_risk(U256::from(TOKEN)))
            .await
            .unwrap()
            .neg_risk
    );
}

#[tokio::test]
async fn wrong_condition_response_is_rejected_without_revision_change_or_cache_install() {
    let server = Server::new(2, false).await;
    let sdk = client(&server).await;
    let before = sdk.metadata_revision().unwrap();
    let mut response = bulk("0.001", true, &[TOKEN, OTHER], None);
    response["c"] = json!(alloy::primitives::B256::ZERO.to_string());
    server.plan(&format!("/clob-markets/{CONDITION}"), Reply::ok(response));
    let error = bounded(sdk.clob_market_info(CONDITION)).await.unwrap_err();
    assert_eq!(
        error.downcast_ref::<MetadataError>(),
        Some(&MetadataError::Unavailable)
    );
    assert!(sdk.metadata_is_current(&before));
    assert_eq!(
        bounded(sdk.tick_size(U256::from(TOKEN)))
            .await
            .unwrap()
            .minimum_tick_size,
        TickSize::Hundredth
    );
    assert!(
        !bounded(sdk.neg_risk(U256::from(OTHER)))
            .await
            .unwrap()
            .neg_risk
    );
    assert_eq!(server.seen().len(), 3);
}

#[tokio::test]
async fn wrong_token_mapping_is_not_cached_and_never_starts_bulk_refresh() {
    let server = Server::new(2, false).await;
    let sdk = client(&server).await;
    let before = sdk.metadata_revision().unwrap();
    server.plan(
        "/markets-by-token/123",
        Reply::ok(json!({"condition_id":CONDITION,
        "primary_token_id":"456","secondary_token_id":"789"})),
    );
    let error = market(&sdk).await.unwrap_err();
    assert_eq!(
        error.downcast_ref::<MetadataError>(),
        Some(&MetadataError::Unavailable)
    );
    assert!(sdk.metadata_is_current(&before));
    server.assert_seen(&["GET /tick-size?token_id=123", "GET /markets-by-token/123"]);
    market(&sdk).await.unwrap();
    assert_eq!(
        server
            .seen()
            .iter()
            .filter(|r| r.as_str() == "GET /markets-by-token/123")
            .count(),
        2
    );
}

#[tokio::test]
async fn order_path_futures_remain_send_without_mutex_guards_crossing_awaits() {
    fn is_send<T: Send>(_: T) {}
    let server = Server::new(2, false).await;
    let sdk = client(&server).await;
    is_send(sdk.tick_size(U256::from(TOKEN)));
    is_send(sdk.neg_risk(U256::from(TOKEN)));
    is_send(sdk.fee_rate_bps(U256::from(TOKEN)));
    is_send(sdk.builder_fee_rate(alloy::primitives::B256::ZERO));
    is_send(sdk.version());
    is_send(sdk.clob_market_info(CONDITION));
    is_send(market(&sdk));
    is_send(build(&sdk, TOKEN));
    let payload = build(&sdk, TOKEN).await.unwrap();
    is_send(sdk.sign(&signer(), payload.clone()));
    is_send(sdk.order_signing_hash(&signer(), &payload.payload));
}

#[tokio::test]
async fn invalidate_clears_v2_fees_and_mapping_but_retains_global_version_and_wire_default() {
    let server = Server::new(2, false).await;
    let sdk = client(&server).await;
    let with_fee = market(&sdk).await.unwrap();
    assert!(with_fee.order().makerAmount < U256::from(2_000_000));
    let before = server.seen().len();
    market(&sdk).await.unwrap();
    assert_eq!(server.seen().len(), before);
    sdk.invalidate_internal_caches();
    server.plan(
        &format!("/clob-markets/{CONDITION}"),
        Reply::ok(bulk("0.01", false, &[TOKEN, OTHER], None)),
    );
    let no_fee = market(&sdk).await.unwrap();
    assert_eq!(no_fee.order().makerAmount, U256::from(2_000_000));
    assert!(server.seen()[before..].contains(&"GET /markets-by-token/123".to_string()));
    assert_eq!(
        server
            .seen()
            .iter()
            .filter(|r| r.as_str() == "GET /version")
            .count(),
        1
    );
}

#[tokio::test]
async fn forced_version_refresh_supersedes_normal_get_and_failed_refresh_clears_old_version() {
    let server = Server::new(2, false).await;
    let seed = client(&server).await;
    let payload = build(&seed, TOKEN).await.unwrap();
    let sdk = client(&server).await;
    let signed = bounded(sdk.sign(&signer(), payload.clone())).await.unwrap();
    let old_gate = Gate::new();
    let new_gate = Gate::new();
    server.plan(
        "/version",
        Reply::held(json!({"version":1}), old_gate.clone()),
    );
    server.plan(
        "/version",
        Reply::held(json!({"version":2}), new_gate.clone()),
    );
    let mut old = Box::pin(sdk.version());
    enter!(old, old_gate);
    let revision = sdk.metadata_revision().unwrap();
    let mut post = Box::pin(sdk.post_order(signed));
    enter!(post, new_gate);
    assert!(!sdk.metadata_is_current(&revision));
    new_gate.release();
    assert!(bounded(post).await.is_err());
    old_gate.release();
    changed(bounded(old).await);
    assert_eq!(bounded(sdk.version()).await.unwrap(), 2);
    let signed = bounded(sdk.sign(&signer(), payload)).await.unwrap();
    server.plan(
        "/version",
        Reply {
            status: 500,
            body: "{}".into(),
            gate: None,
        },
    );
    assert!(bounded(sdk.post_order(signed)).await.is_err());
    server.plan("/version", Reply::ok(json!({"version":1})));
    assert_eq!(
        bounded(sdk.version()).await.unwrap(),
        1,
        "failed force cannot retain old cached version"
    );
}

#[tokio::test]
async fn two_forced_version_refreshes_cannot_restore_first_obsolete_version() {
    let server = Server::new(2, false).await;
    let sdk = client(&server).await;
    let payload = build(&sdk, TOKEN).await.unwrap();
    let a = bounded(sdk.sign(&signer(), payload.clone())).await.unwrap();
    let b = bounded(sdk.sign(&signer(), payload)).await.unwrap();
    let ga = Gate::new();
    let gb = Gate::new();
    server.plan("/version", Reply::held(json!({"version":1}), ga.clone()));
    server.plan("/version", Reply::held(json!({"version":2}), gb.clone()));
    let mut first = Box::pin(sdk.post_order(a));
    enter!(first, ga);
    let mut second = Box::pin(sdk.post_order(b));
    enter!(second, gb);
    gb.release();
    assert!(bounded(second).await.is_err());
    ga.release();
    assert!(bounded(first).await.is_err());
    assert_eq!(bounded(sdk.version()).await.unwrap(), 2);
    assert_eq!(
        server
            .seen()
            .iter()
            .filter(|r| r.as_str() == "POST /order")
            .count(),
        2,
        "no implicit resend"
    );
}

#[tokio::test]
async fn revision_survives_authentication_state_transitions_and_clients_do_not_share_epochs() {
    let server = Server::new(2, false).await;
    let sdk = Client::new(&server.url, Config::default()).unwrap();
    sdk.set_tick_size(U256::from(TOKEN), TickSize::Hundredth);
    let revision = sdk.metadata_revision().unwrap();
    let sdk = bounded(
        sdk.authentication_builder(&signer())
            .credentials(Credentials::new(
                uuid::Uuid::nil(),
                "dGVzdA==".into(),
                "test".into(),
            ))
            .authenticate(),
    )
    .await
    .unwrap();
    assert!(sdk.metadata_is_current(&revision));
    let clone = sdk.clone();
    assert!(clone.metadata_is_current(&revision));
    drop(clone);
    let sdk = bounded(sdk.deauthenticate()).await.unwrap();
    assert!(sdk.metadata_is_current(&revision));
    let other = Client::new(&server.url, Config::default()).unwrap();
    assert!(!other.metadata_is_current(&revision));
    sdk.set_tick_size(U256::from(TOKEN), TickSize::Hundredth);
    assert!(
        !sdk.metadata_is_current(&revision),
        "even equal authoritative setters supersede held responses"
    );
    server.assert_seen(&[]);
}

#[tokio::test]
async fn same_complete_payload_signatures_and_hashes_stay_exact_for_all_domains_and_versions() {
    for version in [1, 2] {
        for neg_risk in [false, true] {
            let server = Server::new(version, neg_risk).await;
            let cold = client(&server).await;
            let warmed = client(&server).await;
            let payload = build(&cold, TOKEN).await.unwrap();
            warm(&warmed, TOKEN).await;
            let revision = warmed.metadata_revision().unwrap();
            let a = bounded(cold.sign(&signer(), payload.clone()))
                .await
                .unwrap();
            let b = bounded(warmed.sign(&signer(), payload)).await.unwrap();
            assert_eq!(a, b);
            assert_eq!(
                bounded(cold.order_signing_hash(&signer(), &a.payload))
                    .await
                    .unwrap(),
                bounded(warmed.order_signing_hash(&signer(), &b.payload))
                    .await
                    .unwrap()
            );
            let count = server.seen().len();
            build_sign_hash(&warmed, TOKEN).await;
            assert_eq!(server.seen().len(), count);
            assert!(warmed.metadata_is_current(&revision));
        }
    }
}

#[tokio::test]
async fn bound_delayed_first_poll_and_cache_hits_never_adopt_new_revision() {
    for warmed in [false, true] {
        for field in FIELDS {
            let server = Server::new(2, false).await;
            let sdk = client(&server).await;
            if warmed {
                field.read(&sdk).await.unwrap();
            }
            let revision = sdk.metadata_revision().unwrap();
            let view = sdk.metadata_view(&revision).unwrap();
            let clone = view.clone();
            let pending = field.read_view(&clone); // Deliberately not polled.
            sdk.invalidate_internal_caches();
            // Include a current cache hit: an old view cannot borrow its value.
            if warmed {
                field.read(&sdk).await.unwrap();
            }
            let count = server.seen().len();
            changed(pending.await);
            changed(view.metadata_revision());
            changed(sdk.metadata_view(&revision));
            assert!(!view.metadata_is_current(&sdk.metadata_revision().unwrap()));
            assert_eq!(server.seen().len(), count);
            let current = sdk
                .metadata_view(&sdk.metadata_revision().unwrap())
                .unwrap();
            field.read_view(&current).await.unwrap();
            assert_eq!(server.seen().len(), count + usize::from(!warmed));
        }
    }
}

#[tokio::test]
async fn bound_held_responses_cannot_repopulate_retired_cache() {
    for field in [Field::Tick, Field::Neg, Field::Fee, Field::Builder] {
        let server = Server::new(2, false).await;
        let sdk = client(&server).await;
        let view = sdk
            .metadata_view(&sdk.metadata_revision().unwrap())
            .unwrap();
        let gate = Gate::new();
        server.plan(&field.path(), Reply::held(field.reply(false), gate.clone()));
        let mut old = Box::pin(field.read_view(&view));
        enter!(old, gate);
        sdk.invalidate_internal_caches();
        server.plan(&field.path(), Reply::ok(field.reply(true)));
        let canonical = field.read(&sdk).await.unwrap();
        gate.release();
        changed(old.await);
        assert_eq!(field.read(&sdk).await.unwrap(), canonical);
        assert_eq!(server.seen().len(), 2);
    }
}

#[tokio::test]
async fn bound_builder_clones_and_sign_hash_reject_invalidation_between_steps() {
    for version in [1, 2] {
        let server = Server::new(version, false).await;
        let sdk = client(&server).await;
        let view = sdk
            .metadata_view(&sdk.metadata_revision().unwrap())
            .unwrap();
        let builder = view
            .limit_order()
            .token_id(U256::from(TOKEN))
            .side(Side::Buy)
            .price(Decimal::new(4, 1))
            .size(Decimal::new(5, 0))
            .order_type(OrderType::FAK);
        let clone = builder.clone();
        let signable = builder.build().await.unwrap();
        // Build's tick/version/(V1 fee) initialized under the same revision.
        assert!(view.metadata_revision().is_ok());
        sdk.invalidate_internal_caches();
        let count = server.seen().len();
        changed(clone.build().await);
        changed(view.sign(&signer(), signable.clone()).await);
        changed(view.order_signing_hash(&signer(), &signable.payload).await);
        assert_eq!(
            server.seen().len(),
            count,
            "no new negative-risk request from obsolete work"
        );
        let fresh = sdk
            .metadata_view(&sdk.metadata_revision().unwrap())
            .unwrap();
        fresh.sign(&signer(), signable).await.unwrap();
        assert_eq!(server.seen().len(), count + 1);
    }
}

#[tokio::test]
async fn bound_v2_fee_miss_requires_root_refresh_without_lookup_side_effects() {
    let server = Server::new(2, false).await;
    let sdk = client(&server).await;
    let revision = sdk.metadata_revision().unwrap();
    let view = sdk.metadata_view(&revision).unwrap();
    assert_eq!(
        view.fee_exponent(U256::from(TOKEN))
            .await
            .unwrap_err()
            .downcast_ref::<MetadataError>(),
        Some(&MetadataError::RefreshRequired)
    );
    server.assert_seen(&[]);
    assert!(sdk.metadata_is_current(&revision));
    // The regular market builder must preserve the bound view too.
    let error = view
        .market_order()
        .token_id(U256::from(TOKEN))
        .side(Side::Buy)
        .price(Decimal::new(4, 1))
        .amount(Amount::usdc(Decimal::new(2, 0)).unwrap())
        .user_usdc_balance(Decimal::new(2, 0))
        .build()
        .await
        .unwrap_err();
    assert_eq!(
        error.downcast_ref::<MetadataError>(),
        Some(&MetadataError::RefreshRequired)
    );
    assert!(
        !server
            .seen()
            .iter()
            .any(|r| r.contains("markets-by-token") || r.contains("clob-markets"))
    );
    sdk.clob_market_info(CONDITION).await.unwrap();
    changed(view.fee_exponent(U256::from(TOKEN)).await);
    let fresh = sdk
        .metadata_view(&sdk.metadata_revision().unwrap())
        .unwrap();
    let count = server.seen().len();
    assert_eq!(fresh.fee_exponent(U256::from(TOKEN)).await.unwrap(), 1);
    assert_eq!(server.seen().len(), count);
}

#[tokio::test]
async fn bound_convenience_methods_reject_before_any_io_or_refresh() {
    let server = Server::new(2, false).await;
    let sdk = client(&server).await;
    let revision = sdk.metadata_revision().unwrap();
    let view = sdk.metadata_view(&revision).unwrap();
    for error in [
        view.limit_order()
            .build_sign_and_post(&signer())
            .await
            .unwrap_err(),
        view.market_order()
            .build_sign_and_post(&signer())
            .await
            .unwrap_err(),
    ] {
        assert_eq!(
            error.downcast_ref::<MetadataError>(),
            Some(&MetadataError::PreparationOnly)
        );
    }
    assert!(sdk.metadata_is_current(&revision));
    server.assert_seen(&[]);
}

#[tokio::test]
async fn bound_and_unbound_sign_complete_payload_identically() {
    for version in [1, 2] {
        for neg in [false, true] {
            let server = Server::new(version, neg).await;
            let sdk = client(&server).await;
            let view = sdk
                .metadata_view(&sdk.metadata_revision().unwrap())
                .unwrap();
            let payload = view
                .limit_order()
                .token_id(U256::from(TOKEN))
                .side(Side::Buy)
                .price(Decimal::new(4, 1))
                .size(Decimal::new(5, 0))
                .order_type(OrderType::FAK)
                .nonce(42)
                .build()
                .await
                .unwrap();
            let a = view.sign(&signer(), payload.clone()).await.unwrap();
            let b = sdk.sign(&signer(), payload).await.unwrap();
            assert_eq!(a, b);
            assert_eq!(
                view.order_signing_hash(&signer(), &a.payload)
                    .await
                    .unwrap(),
                sdk.order_signing_hash(&signer(), &b.payload).await.unwrap()
            );
            let count = server.seen().len();
            view.tick_size(U256::from(TOKEN)).await.unwrap();
            assert_eq!(server.seen().len(), count);
            let _ = sdk.post_order(a).await; // Root POST remains available; mismatch refresh stays legacy.
            assert_eq!(
                server
                    .seen()
                    .iter()
                    .filter(|r| r.starts_with("POST "))
                    .count(),
                1
            );
        }
    }
}

#[tokio::test]
async fn bound_view_and_its_futures_are_send() {
    fn send<T: Send>(_: T) {}
    let server = Server::new(2, false).await;
    let sdk = client(&server).await;
    let view = sdk
        .metadata_view(&sdk.metadata_revision().unwrap())
        .unwrap();
    send(view.clone());
    send(view.version());
    send(view.tick_size(U256::from(TOKEN)));
    send(view.neg_risk(U256::from(TOKEN)));
    send(view.fee_rate_bps(U256::from(TOKEN)));
    send(view.fee_exponent(U256::from(TOKEN)));
    send(view.limit_order().build());
    server.assert_seen(&[]);
}

#[tokio::test]
async fn bound_cached_read_microbenchmark() {
    let server = Server::new(2, false).await;
    let sdk = client(&server).await;
    sdk.tick_size(U256::from(TOKEN)).await.unwrap();
    let view = sdk
        .metadata_view(&sdk.metadata_revision().unwrap())
        .unwrap();
    let iterations = 100_000;
    let mut unbound_ns = Vec::new();
    let mut bound_ns = Vec::new();
    for round in 0..6 {
        for use_view in if round % 2 == 0 {
            [false, true]
        } else {
            [true, false]
        } {
            let start = std::time::Instant::now();
            for _ in 0..iterations {
                let value = if use_view {
                    view.tick_size(U256::from(TOKEN)).await
                } else {
                    sdk.tick_size(U256::from(TOKEN)).await
                };
                std::hint::black_box(value.unwrap());
            }
            let per_read = start.elapsed().as_nanos() / iterations;
            if use_view {
                bound_ns.push(per_read);
            } else {
                unbound_ns.push(per_read);
            }
        }
    }
    // Time is diagnostic only. Request counts, not performance thresholds,
    // prove this is the cache-only path; release results are not live latency.
    println!(
        "metadata_cached_read_benchmark {}",
        json!({"iterations_per_sample":iterations,"unbound_ns":unbound_ns,"bound_ns":bound_ns})
    );
    server.assert_seen(&["GET /tick-size?token_id=123"]);
}

#[cfg(feature = "heartbeats")]
#[tokio::test]
async fn dropping_bound_views_and_builders_does_not_cancel_root_heartbeats() {
    let server = Server::new(2, false).await;
    let mut sdk = Client::new(
        &server.url,
        Config::builder()
            .heartbeat_interval(Duration::from_millis(20))
            .build(),
    )
    .unwrap()
    .authentication_builder(&signer())
    .credentials(Credentials::new(
        uuid::Uuid::nil(),
        "dGVzdA==".into(),
        "test".into(),
    ))
    .authenticate()
    .await
    .unwrap();
    let count = || {
        server
            .seen()
            .iter()
            .filter(|r| r.as_str() == "POST /v1/heartbeats")
            .count()
    };
    bounded(async {
        while count() < 1 {
            tokio::task::yield_now().await;
        }
    })
    .await;
    let view = sdk
        .metadata_view(&sdk.metadata_revision().unwrap())
        .unwrap();
    drop(view.clone());
    let builder = view.limit_order();
    drop(builder.clone());
    drop(builder);
    drop(view.market_order());
    drop(view);
    let before = count();
    bounded(async {
        while count() < before + 2 {
            tokio::task::yield_now().await;
        }
    })
    .await;
    assert!(sdk.heartbeats_active());
    sdk.stop_heartbeats().await.unwrap();
}

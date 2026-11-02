//! A mutable subscription on one ordered receiver. Sequence cuts are local
//! publication boundaries, never wire-write or server acknowledgment evidence.
use std::{
    collections::{BTreeMap, BTreeSet, VecDeque},
    pin::Pin,
    sync::{Arc, Mutex, PoisonError, Weak},
    task::{Context, Poll},
    time::Instant,
};

use futures::{Stream, future::BoxFuture};
use tokio::sync::{broadcast, watch};

use super::{
    interest::MessageInterest,
    subscription::{SubscriptionInfo, SubscriptionManager, SubscriptionTarget},
    types::{request::SubscriptionRequest, response::WsMessage},
};
use crate::{
    Result,
    error::Error,
    types::U256,
    ws::{WsError, connection::EvidenceStamp},
};

const MAX_ASSETS: usize = 8192;
const MAX_PENDING_SCOPES: usize = 64;

/// Immutable admission identity. Addition cuts do not prove subscription ACKs.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct TokenBinding {
    pub transport_epoch: u64,
    pub incarnation: u64,
    pub added_after_sequence: u64,
}

/// Successful local command enqueue. Only the ordered stream emits its marker.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ScopeChange {
    pub revision: u64,
    pub sequence_cut: u64,
    pub transport_epoch: u64,
    pub added: BTreeMap<U256, TokenBinding>,
    pub removed: BTreeMap<U256, TokenBinding>,
}

#[derive(Clone, Debug)]
pub enum MarketSessionEvent {
    Scope(ScopeChange),
    /// One native message, including mixed-token messages, exactly once.
    /// Consumers must validate each token's binding against current membership.
    Message {
        sequence: u64,
        transport_epoch: u64,
        token_bindings: BTreeMap<U256, TokenBinding>,
        message: WsMessage,
    },
}

#[derive(Clone, Debug)]
pub struct ScopeSnapshot {
    pub revision: u64,
    pub transport_epoch: u64,
    pub connected: bool,
    pub assets: BTreeMap<U256, TokenBinding>,
}

struct MembershipState {
    revision: u64,
    assets: BTreeMap<U256, TokenBinding>,
    fault: Option<String>,
    closed: bool,
}

/// Read-only retirement view. It confers no consumed-snapshot authority.
#[derive(Clone)]
pub struct MarketMembership {
    state: Arc<Mutex<MembershipState>>,
    manager: Weak<SubscriptionManager>,
    epoch: u64,
}

fn error(reason: impl Into<String>) -> Error {
    WsError::SubscriptionFailed(reason.into()).into()
}

fn transport(stamp: EvidenceStamp, expected: u64) -> Result<bool> {
    if stamp.faults != 0 {
        return Err(error("ordered market session lost transport evidence"));
    }
    if stamp.transport_epoch == 0 && expected == 1 {
        return Ok(false);
    }
    if stamp.transport_epoch != expected {
        return Err(error("ordered market session transport epoch changed"));
    }
    Ok(true)
}

fn membership_valid(state: &MembershipState) -> Result<()> {
    if state.closed {
        return Err(error("ordered market session closed"));
    }
    if let Some(reason) = &state.fault {
        return Err(error(reason.clone()));
    }
    Ok(())
}

impl MarketMembership {
    pub fn snapshot(&self) -> Result<ScopeSnapshot> {
        let manager = self
            .manager
            .upgrade()
            .ok_or_else(|| error("ordered market session owner gone"))?;
        // Same order as scope commit: Evidence, then the membership view.
        let publication = manager.connection.publication();
        let connected = transport(publication.stamp(), self.epoch)?;
        let state = self.state.lock().unwrap_or_else(PoisonError::into_inner);
        membership_valid(&state)?;
        Ok(ScopeSnapshot {
            revision: state.revision,
            transport_epoch: self.epoch,
            connected,
            assets: state.assets.clone(),
        })
    }

    pub fn binding(&self, token: U256) -> Result<Option<TokenBinding>> {
        let manager = self
            .manager
            .upgrade()
            .ok_or_else(|| error("ordered market session owner gone"))?;
        let publication = manager.connection.publication();
        let connected = transport(publication.stamp(), self.epoch)?;
        let state = self.state.lock().unwrap_or_else(PoisonError::into_inner);
        membership_valid(&state)?;
        Ok(if connected {
            state.assets.get(&token).copied()
        } else {
            None
        })
    }
}

enum Wake {
    Message(std::result::Result<WsMessage, broadcast::error::RecvError>),
    Control,
}
type Receiver = (broadcast::Receiver<WsMessage>, watch::Receiver<()>);
type Receive = BoxFuture<'static, (Receiver, Wake)>;

/// One receiver for its entire lifetime. A pending receive belongs to the
/// session itself, so dropping a `next()` future cannot discard input/cursors.
pub struct MarketEventSession {
    manager: Arc<SubscriptionManager>,
    id: u64,
    view: MarketMembership,
    receiver: Option<Receiver>,
    receive: Option<Receive>,
    message: Option<(u64, WsMessage)>,
    next_sequence: u64,
    applied: BTreeMap<U256, TokenBinding>,
    pending: VecDeque<ScopeChange>,
    last_cut: u64,
    ended: bool,
}

fn desired_assets(assets: Vec<U256>) -> Result<BTreeSet<U256>> {
    if assets.len() > MAX_ASSETS {
        return Err(error("market scope asset bound exceeded"));
    }
    Ok(assets.into_iter().collect())
}

fn commands(
    manager: &SubscriptionManager,
    added: &[U256],
    removed: &[U256],
) -> Result<Vec<String>> {
    let subscribe = added
        .iter()
        .filter(|id| !manager.subscribed_assets.contains_key(*id))
        .copied()
        .collect::<Vec<_>>();
    let unsubscribe = removed
        .iter()
        .filter(|id| {
            manager
                .subscribed_assets
                .get(*id)
                .is_some_and(|count| *count == 1)
        })
        .copied()
        .collect::<Vec<_>>();
    let mut batch = Vec::with_capacity(2);
    if !subscribe.is_empty() {
        batch.push(serde_json::to_string(&SubscriptionRequest::market(
            subscribe,
        ))?);
    }
    if !unsubscribe.is_empty() {
        batch.push(serde_json::to_string(
            &SubscriptionRequest::market_unsubscribe(unsubscribe),
        )?);
    }
    Ok(batch)
}

fn commit_refs(manager: &SubscriptionManager, added: &[U256], removed: &[U256]) {
    for token in added {
        *manager.subscribed_assets.entry(*token).or_default() += 1;
    }
    for token in removed {
        if let dashmap::Entry::Occupied(mut entry) = manager.subscribed_assets.entry(*token) {
            *entry.get_mut() -= 1;
            if *entry.get() == 0 {
                entry.remove();
            }
        }
    }
}

impl MarketEventSession {
    pub(super) fn new(manager: Arc<SubscriptionManager>, assets: Vec<U256>) -> Result<Self> {
        let desired = desired_assets(assets)?;
        let mut registry = manager
            .market_registry
            .lock()
            .unwrap_or_else(PoisonError::into_inner);
        let id = registry
            .next_session
            .checked_add(1)
            .ok_or_else(|| error("session identity exhausted"))?;
        let next_incarnation = registry
            .next_incarnation
            .checked_add(desired.len() as u64)
            .ok_or_else(|| error("token incarnation exhausted"))?;
        let added = desired.iter().copied().collect::<Vec<_>>();
        let batch = commands(&manager, &added, &[])?;
        manager.interest.add(MessageInterest::MARKET);
        // Install the wake observer BEFORE checking evidence to avoid lost wakes.
        let changes = manager.connection.evidence_changes();
        let mut publication = manager.connection.publication();
        let stamp = publication.stamp();
        let epoch = stamp.transport_epoch.max(1);
        transport(stamp, epoch)?;
        let next_sequence = stamp
            .messages
            .checked_add(1)
            .ok_or_else(|| error("sequence exhausted"))?;
        let receiver = publication.subscribe(); // Before initial command enqueue.
        publication.enqueue_batch(batch)?;
        let bindings = added
            .iter()
            .enumerate()
            .map(|(ordinal, token)| {
                (
                    *token,
                    TokenBinding {
                        transport_epoch: epoch,
                        incarnation: registry.next_incarnation + ordinal as u64 + 1,
                        added_after_sequence: stamp.messages,
                    },
                )
            })
            .collect::<BTreeMap<_, _>>();
        let change = ScopeChange {
            revision: 1,
            sequence_cut: stamp.messages,
            transport_epoch: epoch,
            added: bindings.clone(),
            removed: BTreeMap::new(),
        };
        let state = Arc::new(Mutex::new(MembershipState {
            revision: 1,
            assets: bindings,
            fault: None,
            closed: false,
        }));
        commit_refs(&manager, &added, &[]);
        registry.next_session = id;
        registry.next_incarnation = next_incarnation;
        registry.sessions.insert(id);
        manager.active_subs.insert(
            format!("market-session:{id}"),
            SubscriptionInfo {
                target: SubscriptionTarget::Assets(added),
                created_at: Instant::now(),
            },
        );
        drop(publication);
        drop(registry);
        let view = MarketMembership {
            state,
            manager: Arc::downgrade(&manager),
            epoch,
        };
        Ok(Self {
            manager,
            id,
            view,
            receiver: Some((receiver, changes)),
            receive: None,
            message: None,
            next_sequence,
            applied: BTreeMap::new(),
            pending: VecDeque::from([change]),
            last_cut: stamp.messages,
            ended: false,
        })
    }

    #[must_use]
    pub fn membership(&self) -> MarketMembership {
        self.view.clone()
    }

    /// Changes only this session's subscription references, preserving the
    /// receiver and old queued bindings. The returned value is local intent.
    pub fn replace_assets(&mut self, assets: Vec<U256>) -> Result<ScopeChange> {
        let result = self.replace_inner(assets);
        if let Err(error) = &result {
            self.view
                .state
                .lock()
                .unwrap_or_else(PoisonError::into_inner)
                .fault = Some(error.to_string());
        }
        result
    }

    fn replace_inner(&mut self, assets: Vec<U256>) -> Result<ScopeChange> {
        let desired = desired_assets(assets)?;
        let manager = &self.manager;
        let mut registry = manager
            .market_registry
            .lock()
            .unwrap_or_else(PoisonError::into_inner);
        let (revision, current) = {
            let state = self
                .view
                .state
                .lock()
                .unwrap_or_else(PoisonError::into_inner);
            membership_valid(&state)?;
            (state.revision, state.assets.clone())
        };
        let added = desired
            .iter()
            .filter(|token| !current.contains_key(*token))
            .copied()
            .collect::<Vec<_>>();
        let removed = current
            .keys()
            .filter(|token| !desired.contains(*token))
            .copied()
            .collect::<Vec<_>>();
        if added.is_empty() && removed.is_empty() {
            let publication = manager.connection.publication();
            transport(publication.stamp(), self.view.epoch)?;
            return Ok(ScopeChange {
                revision,
                sequence_cut: self.last_cut,
                transport_epoch: self.view.epoch,
                added: BTreeMap::new(),
                removed: BTreeMap::new(),
            });
        }
        if self.pending.len() >= MAX_PENDING_SCOPES {
            return Err(error("undrained market scope history bound exceeded"));
        }
        let revision = revision
            .checked_add(1)
            .ok_or_else(|| error("scope revision exhausted"))?;
        let next_incarnation = registry
            .next_incarnation
            .checked_add(added.len() as u64)
            .ok_or_else(|| error("token incarnation exhausted"))?;
        let batch = commands(manager, &added, &removed)?;
        let mut publication = manager.connection.publication();
        let stamp = publication.stamp();
        transport(stamp, self.view.epoch)?;
        let change = ScopeChange {
            revision,
            sequence_cut: stamp.messages,
            transport_epoch: self.view.epoch,
            added: added
                .iter()
                .enumerate()
                .map(|(ordinal, token)| {
                    (
                        *token,
                        TokenBinding {
                            transport_epoch: self.view.epoch,
                            incarnation: registry.next_incarnation + ordinal as u64 + 1,
                            added_after_sequence: stamp.messages,
                        },
                    )
                })
                .collect(),
            removed: removed
                .iter()
                .map(|token| (*token, current[token]))
                .collect(),
        };
        publication.enqueue_batch(batch)?;
        commit_refs(manager, &added, &removed);
        registry.next_incarnation = next_incarnation;
        let mut state = self
            .view
            .state
            .lock()
            .unwrap_or_else(PoisonError::into_inner);
        for token in &removed {
            state.assets.remove(token);
        }
        state.assets.extend(change.added.clone());
        state.revision = revision;
        manager.active_subs.insert(
            format!("market-session:{}", self.id),
            SubscriptionInfo {
                target: SubscriptionTarget::Assets(desired.into_iter().collect()),
                created_at: Instant::now(),
            },
        );
        self.pending.push_back(change.clone());
        self.last_cut = stamp.messages;
        Ok(change)
    }

    fn fail(&mut self, failure: Error) -> Poll<Option<Result<MarketSessionEvent>>> {
        self.view
            .state
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .fault = Some(failure.to_string());
        self.ended = true;
        self.receiver = None;
        self.receive = None;
        self.message = None;
        Poll::Ready(Some(Err(failure)))
    }
}

fn tokens(message: &WsMessage) -> Vec<U256> {
    match message {
        WsMessage::Book(book) => vec![book.asset_id],
        WsMessage::PriceChange(change) => change
            .price_changes
            .iter()
            .map(|entry| entry.asset_id)
            .collect(),
        WsMessage::LastTradePrice(trade) => vec![trade.asset_id],
        WsMessage::TickSizeChange(tick) => vec![tick.asset_id],
        WsMessage::BestBidAsk(quote) => vec![quote.asset_id],
        WsMessage::NewMarket(market) => market.asset_ids.clone(),
        WsMessage::MarketResolved(market) => market.asset_ids.clone(),
        _ => vec![],
    }
}

impl Stream for MarketEventSession {
    type Item = Result<MarketSessionEvent>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = self.get_mut();
        if this.ended {
            return Poll::Ready(None);
        }
        let connection = this.manager.connection.clone();
        // Bound per-poll work without dropping filtered progress or cut state.
        for _ in 0..128 {
            {
                let publication = connection.publication();
                let active = transport(publication.stamp(), this.view.epoch);
                let state_result = membership_valid(
                    &this
                        .view
                        .state
                        .lock()
                        .unwrap_or_else(PoisonError::into_inner),
                );
                let active = match active.and_then(|active| state_result.map(|()| active)) {
                    Ok(active) => active,
                    Err(failure) => {
                        drop(publication);
                        return this.fail(failure);
                    }
                };
                let drained_before = this
                    .message
                    .as_ref()
                    .map_or(this.next_sequence, |(sequence, _)| *sequence);
                if active
                    && this
                        .pending
                        .front()
                        .is_some_and(|change| change.sequence_cut < drained_before)
                {
                    let change = this.pending.pop_front().unwrap();
                    for token in change.removed.keys() {
                        this.applied.remove(token);
                    }
                    this.applied.extend(change.added.clone());
                    return Poll::Ready(Some(Ok(MarketSessionEvent::Scope(change))));
                }
            }
            if let Some((sequence, message)) = this.message.take() {
                let publication = connection.publication();
                let stamp = publication.stamp();
                if let Err(failure) = transport(stamp, this.view.epoch) {
                    drop(publication);
                    return this.fail(failure);
                }
                if stamp.transport_epoch == 0 || sequence < stamp.epoch_start_sequence {
                    drop(publication);
                    return this.fail(error(
                        "queued message does not belong to pinned transport epoch",
                    ));
                }
                let token_bindings = tokens(&message)
                    .into_iter()
                    .filter_map(|token| this.applied.get(&token).map(|binding| (token, *binding)))
                    .collect::<BTreeMap<_, _>>();
                if !token_bindings.is_empty() {
                    return Poll::Ready(Some(Ok(MarketSessionEvent::Message {
                        sequence,
                        transport_epoch: this.view.epoch,
                        token_bindings,
                        message,
                    })));
                }
                continue;
            }
            if this.receive.is_none() {
                let (mut raw, mut changes) = this.receiver.take().unwrap();
                this.receive = Some(Box::pin(async move {
                    let wake = tokio::select! {
                        message = raw.recv() => Wake::Message(message),
                        _ = changes.changed() => Wake::Control,
                    };
                    ((raw, changes), wake)
                }));
            }
            let Poll::Ready((receiver, wake)) = this.receive.as_mut().unwrap().as_mut().poll(cx)
            else {
                return Poll::Pending;
            };
            this.receive = None;
            this.receiver = Some(receiver);
            let message = match wake {
                Wake::Control => continue,
                Wake::Message(Ok(message)) => message,
                Wake::Message(Err(broadcast::error::RecvError::Lagged(missed))) => {
                    return this.fail(WsError::Lagged { missed }.into());
                }
                Wake::Message(Err(broadcast::error::RecvError::Closed)) => {
                    return this.fail(WsError::ConnectionClosed.into());
                }
            };
            // Persist EVERY raw receive before filtering, returning or pending.
            let sequence = this.next_sequence;
            let Some(next) = sequence.checked_add(1) else {
                return this.fail(error("broadcast sequence exhausted"));
            };
            this.next_sequence = next;
            this.message = Some((sequence, message));
        }
        cx.waker().wake_by_ref();
        Poll::Pending
    }
}

impl Drop for MarketEventSession {
    fn drop(&mut self) {
        let manager = &self.manager;
        let mut registry = manager
            .market_registry
            .lock()
            .unwrap_or_else(PoisonError::into_inner);
        let removed = self
            .view
            .state
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .assets
            .keys()
            .copied()
            .collect::<Vec<_>>();
        let batch = commands(manager, &[], &removed);
        let mut publication = manager.connection.publication();
        // Close the read view even if transport failure prevents cleanup writes.
        self.view
            .state
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .closed = true;
        if let Ok(batch) = batch {
            let _ = publication.enqueue_batch(batch);
        }
        commit_refs(manager, &[], &removed);
        registry.sessions.remove(&self.id);
        manager
            .active_subs
            .remove(&format!("market-session:{}", self.id));
    }
}

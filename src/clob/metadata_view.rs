//! Preparation-only view of one admitted metadata revision.
//!
//! Root clients own mutation and submission. Views share their existing caches,
//! authentication and HTTP pool but cannot adopt a newer revision. A caller must
//! still check admission immediately before submitting with the root client.
use alloy::signers::Signer;

use crate::{
    Result,
    auth::{
        Kind,
        state::{Authenticated, State, Unauthenticated},
    },
    clob::{
        Client,
        metadata::MetadataRevision,
        order_builder::{Limit, Market, OrderBuilder},
        types::{
            OrderPayload, SignableOrder, SignedOrder,
            response::{
                BuilderFeeRateResponse, FeeRateResponse, NegRiskResponse, TickSizeResponse,
            },
        },
    },
    types::{B256, U256},
};

/// Obtained from [`Client::metadata_view`]. Does not expose the inner client.
#[derive(Clone, Debug)]
pub struct MetadataView<S: State = Unauthenticated> {
    pub(super) client: Client<S>,
}

impl<S: State> MetadataView<S> {
    /// Check and return this view's original revision, never a new revision.
    pub fn metadata_revision(&self) -> Result<MetadataRevision> {
        self.client.metadata_revision()
    }
    pub fn metadata_is_current(&self, revision: &MetadataRevision) -> bool {
        self.client.metadata_is_current(revision)
    }
    pub async fn version(&self) -> Result<u32> {
        self.client.version().await
    }
    pub async fn tick_size(&self, token: U256) -> Result<TickSizeResponse> {
        self.client.tick_size(token).await
    }
    pub async fn neg_risk(&self, token: U256) -> Result<NegRiskResponse> {
        self.client.neg_risk(token).await
    }
    pub async fn fee_rate_bps(&self, token: U256) -> Result<FeeRateResponse> {
        self.client.fee_rate_bps(token).await
    }
    /// A cache miss requires a separate authoritative root-client refresh.
    pub async fn fee_exponent(&self, token: U256) -> Result<u32> {
        self.client.fee_exponent(token).await
    }
}

impl<K: Kind> MetadataView<Authenticated<K>> {
    pub async fn builder_fee_rate(&self, code: B256) -> Result<BuilderFeeRateResponse> {
        self.client.builder_fee_rate(code).await
    }
    /// The returned builder can build but its convenience POST method rejects
    /// with `MetadataError::PreparationOnly` before doing any I/O.
    pub fn limit_order(&self) -> OrderBuilder<Limit, K> {
        self.client.limit_order()
    }
    /// Cold V2 market-fee metadata returns `MetadataError::RefreshRequired`.
    pub fn market_order(&self) -> OrderBuilder<Market, K> {
        self.client.market_order()
    }
    pub async fn sign<T: Signer>(&self, signer: &T, order: SignableOrder) -> Result<SignedOrder> {
        self.client.sign(signer, order).await
    }
    pub async fn order_signing_hash<T: Signer>(
        &self,
        signer: &T,
        payload: &OrderPayload,
    ) -> Result<B256> {
        self.client.order_signing_hash(signer, payload).await
    }
}

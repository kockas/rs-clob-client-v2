//! Bounded, response-authored HTTP evidence. No credentials or request bodies.
use crate::types::Address;
use reqwest::{Method, StatusCode};

#[derive(Debug, Clone)]
pub struct ResponseMetadata {
    pub(crate) method: Method,
    pub(crate) path: String,
    pub(crate) request_origin: String,
    pub(crate) response_origin: String,
    pub(crate) response_path: String,
    pub(crate) status: StatusCode,
    pub(crate) signer: Option<Address>,
    pub(crate) content_type: Option<String>,
    pub(crate) retry_after: Option<String>,
    pub(crate) remaining: Option<String>,
    pub(crate) reset: Option<String>,
    pub(crate) tier: Option<String>,
    pub(crate) body_hash: Option<[u8; 32]>,
}
impl ResponseMetadata {
    pub fn method(&self) -> &Method {
        &self.method
    }
    pub fn path(&self) -> &str {
        &self.path
    }
    pub fn request_origin(&self) -> &str {
        &self.request_origin
    }
    pub fn response_origin(&self) -> &str {
        &self.response_origin
    }
    pub fn response_path(&self) -> &str {
        &self.response_path
    }
    pub fn status(&self) -> StatusCode {
        self.status
    }
    pub fn authenticated_signer(&self) -> Option<Address> {
        self.signer
    }
    pub fn content_type(&self) -> Option<&str> {
        self.content_type.as_deref()
    }
    pub fn retry_after(&self) -> Option<&str> {
        self.retry_after.as_deref()
    }
    pub fn rate_remaining(&self) -> Option<&str> {
        self.remaining.as_deref()
    }
    pub fn rate_reset(&self) -> Option<&str> {
        self.reset.as_deref()
    }
    pub fn rate_tier(&self) -> Option<&str> {
        self.tier.as_deref()
    }
    pub fn same_endpoint(&self) -> bool {
        self.request_origin == self.response_origin && self.path == self.response_path
    }
    pub fn matches_body(&self, body: &str) -> bool {
        use sha2::{Digest, Sha256};
        Some(<[u8; 32]>::from(Sha256::digest(body.as_bytes()))) == self.body_hash
    }
}

#[derive(Debug)]
pub struct ObservedResponse<T> {
    pub data: T,
    pub observation: Option<ResponseMetadata>,
}

#[derive(Clone, Debug, serde::Deserialize)]
pub struct RateLimitTier {
    pub tier: String,
    pub tier_details: RateLimitTierDetails,
    pub period: u64,
}
#[derive(Clone, Debug, serde::Deserialize)]
pub struct RateLimitTierDetails {
    pub order_rate_per_sec: u64,
    pub order_burst: u64,
    pub cancel_rate_per_sec: u64,
    pub cancel_burst: u64,
    pub allow_negative: bool,
    pub volume_threshold: u64,
}

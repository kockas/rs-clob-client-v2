//! Revision fences for cached order metadata. They are not order-submission locks.
use std::{
    fmt,
    sync::{Arc, Mutex, MutexGuard},
};

use crate::{
    Result,
    error::{Error, Kind},
};

/// Opaque, client-owned metadata identity. Holding an old revision prevents ABA:
/// it cannot compare equal to a newer allocation, and no integer can wrap.
#[derive(Clone, Debug)]
pub struct MetadataRevision(Arc<()>);
impl Default for MetadataRevision {
    fn default() -> Self {
        Self(Arc::new(()))
    }
}
impl PartialEq for MetadataRevision {
    fn eq(&self, other: &Self) -> bool {
        Arc::ptr_eq(&self.0, &other.0)
    }
}
impl Eq for MetadataRevision {}

/// A metadata operation could not supply a coherent current value. An order
/// may only be classified as unsent if it has not crossed POST admission.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum MetadataError {
    Changed,
    Unavailable,
    Poisoned,
    RefreshRequired,
    PreparationOnly,
}
impl fmt::Display for MetadataError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            Self::Changed => "order metadata changed while request was pending",
            Self::Unavailable => "requested order metadata is unavailable",
            Self::Poisoned => "order metadata synchronization is poisoned",
            Self::RefreshRequired => {
                "metadata requires an authoritative refresh before preparation"
            }
            Self::PreparationOnly => {
                "metadata view is preparation-only; submit through the root client"
            }
        })
    }
}
impl std::error::Error for MetadataError {}
impl From<MetadataError> for Error {
    fn from(value: MetadataError) -> Self {
        Self::with_source(Kind::Synchronization, value)
    }
}

/// Lock order: this state first, then cache guards. Never hold across an await.
#[derive(Debug, Default)]
pub(crate) struct MetadataState(Mutex<MetadataRevision>);
impl MetadataState {
    pub(crate) fn lock(&self) -> Result<MutexGuard<'_, MetadataRevision>> {
        self.0.lock().map_err(|_| MetadataError::Poisoned.into())
    }
    pub(crate) fn check(
        &self,
        revision: &MetadataRevision,
    ) -> Result<MutexGuard<'_, MetadataRevision>> {
        let guard = self.lock()?;
        if *guard != *revision {
            return Err(MetadataError::Changed.into());
        }
        Ok(guard)
    }
}

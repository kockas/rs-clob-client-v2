#![cfg(feature = "clob")]
//! Test the actual synchronization primitive without exposing a production
//! callback that could deliberately poison the client's private mutex.
pub use polymarket_client_sdk_v2::{Result, error};
#[path = "../src/clob/metadata.rs"]
mod metadata_impl;
use metadata_impl::{MetadataError, MetadataRevision, MetadataState};

#[test]
fn poisoning_is_permanent_and_cannot_make_old_or_new_revision_valid() {
    let state = MetadataState::default();
    let old = state.lock().unwrap().clone();
    let outcome = std::panic::catch_unwind(|| {
        let mut guard = state.lock().unwrap();
        *guard = MetadataRevision::default();
        panic!("deliberately interrupt a metadata transaction");
    });
    assert!(outcome.is_err());
    for revision in [old, MetadataRevision::default()] {
        assert_eq!(
            state
                .check(&revision)
                .unwrap_err()
                .downcast_ref::<MetadataError>(),
            Some(&MetadataError::Poisoned)
        );
    }
    assert_eq!(
        state.lock().unwrap_err().downcast_ref::<MetadataError>(),
        Some(&MetadataError::Poisoned)
    );
}

#[test]
fn held_revisions_do_not_alias_new_allocations_and_clones_keep_identity() {
    let state = MetadataState::default();
    let original = state.lock().unwrap().clone();
    assert_eq!(original, original.clone());
    for _ in 0..1024 {
        *state.lock().unwrap() = MetadataRevision::default();
        assert_eq!(
            state
                .check(&original)
                .unwrap_err()
                .downcast_ref::<MetadataError>(),
            Some(&MetadataError::Changed)
        );
        let current = state.lock().unwrap().clone();
        assert!(state.check(&current).is_ok());
    }
}

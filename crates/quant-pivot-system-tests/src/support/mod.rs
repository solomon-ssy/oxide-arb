//! Scenario-local fixtures for cross-crate system tests.

use std::sync::Arc;

use quant_pivot_core::ingest::book_store::BookStore;
use quant_pivot_models::{
    domain::{data_plane::pipeline::StreamSessionTicket, market::book::BookSnapshot},
    types::TokenId,
};

pub mod catalog_fixtures;
pub mod execution_pg_seed;
pub mod fact_sink;
pub mod factor_governance;
pub mod model_spec_fixtures;
pub mod pit;
pub mod policy_fixtures;
pub mod report_fixtures;
pub mod report_lifecycle_seed;
pub mod report_pipeline_harness;
pub mod research_fixtures;
pub mod storage;
pub mod trade_tape_fixtures;
pub mod ws;

use uuid::Uuid;

#[must_use]
pub fn seeded_uuid(name: &str) -> Uuid {
    Uuid::new_v5(&Uuid::NAMESPACE_OID, name.as_bytes())
}

/// Publish a coherent test snapshot through the same explicit session fence as
/// production ingest. Test fixtures never bypass Fresh/LastKnown semantics.
pub fn publish_fresh_book(
    book_store: &BookStore,
    token_id: &TokenId,
    snapshot: BookSnapshot,
    sequence: u64,
) {
    let token = book_store.resolve(token_id).expect("registered book token");
    let epoch = u64::try_from(token.index()).expect("token index fits") + 1;
    let session_id = seeded_uuid(&format!("book-session:{}", token_id.as_str()));
    let session = StreamSessionTicket::new(session_id, epoch).expect("valid book session ticket");
    assert!(
        book_store
            .session_directory()
            .open(session, Arc::from([token_id.clone()]))
    );
    assert!(book_store.publish_snapshot_session(token, snapshot, sequence, session, None));
}

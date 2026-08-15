//! Scenario-local fixtures for cross-crate system tests.

use std::sync::Arc;

use quant_pivot_core::ingest::book_store::BookStore;
use quant_pivot_models::{
    domain::{data_plane::pipeline::StreamSessionTicket, market::book::BookSnapshot},
    types::{ContentHash, SelectorHashEvidence, TokenId},
};

pub mod artifact_store;
pub mod catalog_fixtures;
pub mod execution_history_fixtures;
pub mod execution_pg_seed;
pub mod fact_sink;
pub mod factor_definitions;
pub mod feedback_closure_seed;
pub mod model_serving_fixtures;
pub mod model_serving_runtime;
pub mod model_spec_fixtures;
pub mod pit;
pub mod policy_fixtures;
pub mod portfolio_scenario_fixtures;
pub mod report_fixtures;
pub mod report_lifecycle_seed;
pub mod report_pipeline_harness;
pub mod research_browser_seed;
pub mod research_fixtures;
pub mod storage;
pub mod trade_policy_fixtures;
pub mod ws;

use uuid::Uuid;

#[must_use]
pub fn seeded_uuid(name: &str) -> Uuid {
    Uuid::new_v5(&Uuid::NAMESPACE_OID, name.as_bytes())
}

/// Owner of structurally valid selector commitments for persistence-only fixtures.
pub struct SelectorFixture;

impl SelectorFixture {
    #[must_use]
    pub const fn evidence(selector_hash: ContentHash) -> SelectorHashEvidence {
        SelectorHashEvidence {
            selector_hash,
            contract_hash: ContentHash::from_bytes([1; 32]),
            boundary_hash: ContentHash::from_bytes([2; 32]),
            selection_policy_hash: ContentHash::from_bytes([3; 32]),
            data_quality_policy_hash: ContentHash::from_bytes([4; 32]),
            feature_schema_hash: ContentHash::from_bytes([5; 32]),
            model_requirements_hash: ContentHash::from_bytes([6; 32]),
            candidates_hash: ContentHash::from_bytes([7; 32]),
            candidate_catalog_hash: ContentHash::from_bytes([8; 32]),
            candidate_book_hash: ContentHash::from_bytes([9; 32]),
            candidate_domain_hash: ContentHash::from_bytes([10; 32]),
            candidate_decision_hash: ContentHash::from_bytes([11; 32]),
            included_hash: ContentHash::from_bytes([12; 32]),
            excluded_hash: ContentHash::from_bytes([13; 32]),
            exclusion_summary_hash: ContentHash::from_bytes([14; 32]),
        }
    }
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

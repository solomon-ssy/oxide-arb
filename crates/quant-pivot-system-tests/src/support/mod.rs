//! Scenario-local fixtures for cross-crate system tests.

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

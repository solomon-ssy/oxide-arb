//! `ClickHouse` connection management, schema DDL, and write manager.

mod bootstrap;
mod deadline;
mod ensure;
mod pool;
mod query;
mod query_limits;
mod readiness;
mod schema;
#[cfg(test)]
mod test_support;
pub mod write_manager;

pub use bootstrap::{
    ClickHouseSchemaStatus, bootstrap_schema, generate_clean_schema_manifest, schema_contract_hash,
    verify_schema,
};
pub use ensure::{
    active_preproduction_query_count, database_object_count, reset_preproduction_database,
};
pub use pool::ClickHousePool;
pub use query::{ChReadMetrics, ClickHouseQueryLimits, GovernedQuery};
pub use readiness::{BookLatencyObservation, RawHistoryObservation};
pub use schema::extract_table_ttl;
pub use write_manager::{ChWriteManager, ChWriteMetrics, WritePermit};

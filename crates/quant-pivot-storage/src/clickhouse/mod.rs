//! `ClickHouse` connection management, schema DDL, and write manager.

mod ensure;
mod migration;
mod pool;
mod query;
mod query_limits;
mod readiness;
mod schema;
pub mod write_manager;

pub use ensure::{
    active_preproduction_query_count, database_object_count, reset_preproduction_database,
};
pub use migration::{
    ClickHouseMigrationSafety, ClickHouseSchemaMigrationInfo, ClickHouseSchemaPlan,
    ClickHouseSchemaStatus, apply_offline_schema_migrations, apply_online_schema_migrations,
    generate_clean_schema_manifest, plan_schema, render_schema_manifest, schema_contract_hash,
    verify_schema,
};
pub use pool::ClickHousePool;
pub use query::ClickHouseQueryLimits;
pub use readiness::{BookLatencyObservation, RawHistoryObservation};
pub use schema::extract_table_ttl;
pub use write_manager::{ChWriteManager, ChWriteMetrics, WritePermit};

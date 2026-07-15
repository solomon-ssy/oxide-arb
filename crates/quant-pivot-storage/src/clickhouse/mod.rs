//! `ClickHouse` connection management, schema DDL, and write manager.

mod ensure;
mod migration;
mod pool;
mod readiness;
mod schema;
pub mod write_manager;

pub use migration::{
    ClickHouseMigrationSafety, ClickHouseSchemaMigrationInfo, ClickHouseSchemaPlan,
    ClickHouseSchemaStatus, apply_online_schema_migrations, plan_schema, verify_schema,
};
pub use pool::ClickHousePool;
pub use readiness::{BookLatencyObservation, RawHistoryObservation};
pub use schema::{RAW_HISTORY_TABLES, RawHistoryTable, extract_table_ttl};
pub use write_manager::{ChWriteManager, ChWriteMetrics, WritePermit};

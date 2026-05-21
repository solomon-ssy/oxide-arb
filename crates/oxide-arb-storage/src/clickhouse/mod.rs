//! `ClickHouse` connection management, schema DDL, and batch inserter.

mod inserter;
mod pool;
mod schema;
pub mod write_manager;

pub mod rows {
    pub use oxide_arb_models::clickhouse::*;
}

pub use inserter::BatchInserter;
pub use pool::ClickHousePool;
pub use write_manager::{ChWriteManager, ChWriteMetrics, WriteManagerError, WritePermit};

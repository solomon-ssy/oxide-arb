//! Database initialization, connection management, schema migrations,
//! and unified cache layer for the quant-pivot platform.

pub mod cache;
pub mod clickhouse;
pub mod error;
pub mod evidence;
pub mod postgres;
pub mod sql_contract_registry;
pub mod write;

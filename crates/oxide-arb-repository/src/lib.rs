//! Data access layer for the oxide-arb platform.
//!
//! Provides async repository traits and implementations for:
//!
//! - **`PostgreSQL`** — CRUD for all domain entities via `SeaORM`
//! - **`ClickHouse`** — timeseries insert and query

pub mod batch;
pub mod clickhouse;
pub mod postgres;
pub mod traits;

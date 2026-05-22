//! Data access layer for the oxide-arb platform.
//!
//! Provides async repository traits and implementations for:
//!
//! - **`PostgreSQL`** — CRUD for all domain entities via `SeaORM`
//! - **`ClickHouse`** — timeseries insert and query
//! - **`Cached`** — tiered cache wrappers (L1 Moka + L2 Redis)

pub mod batch;
pub mod cached;
pub mod clickhouse;
pub mod postgres;
pub mod traits;

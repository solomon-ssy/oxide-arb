//! Data access layer for the quant-pivot platform.
//!
//! Provides async repository traits and implementations for:
//!
//! - **`PostgreSQL`** — CRUD for all domain entities via `SeaORM`
//! - **`ClickHouse`** — timeseries insert and query
//! - **`Cached`** — tiered cache wrappers (L1 Moka + L2 Redis)

pub mod batch;
pub mod postgres;
pub mod traits;

pub use postgres::arc_repo;

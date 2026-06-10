//! Domain models, types, enums, configuration, and persistence entities
//! for the oxide-arb platform.
//!
//! This crate is the single source of truth for all data definitions.
//! It contains zero business logic — only type definitions, serialization,
//! and database schema mappings.
//!
//! # Visibility
//!
//! The `entities` module (`SeaORM` `Model`/`ActiveModel`/`Entity`) is
//! crate-private by default. Enable the `repository` feature to re-export
//! it — only the `oxide-arb-repository` crate should do this. All other
//! crates interact with persistence through domain DTOs (`New*`, `Update*`,
//! `Upsert*`) and read models (`*Info`).

pub mod clickhouse;
pub mod config;
pub mod constants;
pub mod domain;
#[cfg(feature = "repository")]
pub mod entities;

#[cfg(not(feature = "repository"))]
pub(crate) mod entities;

pub mod enums;
pub mod hashing;
pub mod idens;
pub mod runtime_config;
pub mod schema;
pub mod security;
pub mod seed;
pub mod types;

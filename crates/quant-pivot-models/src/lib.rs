//! Domain models, types, enums, configuration, and persistence entities
//! for the quant-pivot platform.
//!
//! This crate is the single source of truth for all data definitions.
//! It contains zero business logic — only type definitions, serialization,
//! and database schema mappings.
//!
//! # Visibility
//!
//! The `entities` module (`SeaORM` `Model`/`ActiveModel`/`Entity`) is
//! crate-private by default. Enable the `persistence-entities` feature only in
//! crates that own database persistence (`quant-pivot-repository` and the
//! schema/seed portion of `quant-pivot-storage`). All other crates interact
//! with persistence through domain DTOs (`New*`, `Update*`, `Upsert*`) and read
//! models (`*Info`).

pub mod clickhouse;
pub mod config;
pub mod constants;
pub mod domain;
// SeaORM `Model` structs idiomatically repeat the table noun in their id
// columns (`model_version_id`, `model_run_id`, …); `struct_field_names` is a
// poor fit for this generated-style DB projection layer. Allowed only here.
#[cfg(feature = "persistence-entities")]
#[allow(clippy::struct_field_names)]
pub mod entities;

#[cfg(not(feature = "persistence-entities"))]
#[allow(clippy::struct_field_names)]
pub(crate) mod entities;

pub mod enums;
pub mod hashing;
pub mod runtime_config;
pub mod security;
pub mod seed;
pub mod types;

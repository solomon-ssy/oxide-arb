//! Domain models, types, enums, configuration, and persistence entities
//! for the oxide-arb platform.
//!
//! This crate is the single source of truth for all data definitions.
//! It contains zero business logic — only type definitions, serialization,
//! and database schema mappings.

pub mod clickhouse;
pub mod config;
pub mod constants;
pub mod domain;
pub mod entities;
pub mod enums;
pub mod idens;
pub mod seed;
pub mod types;

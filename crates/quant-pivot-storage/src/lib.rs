//! Database initialization, connection management, schema migrations,
//! and unified cache layer for the quant-pivot platform.

use quant_pivot_allocator as _;

pub mod cache;
pub mod clickhouse;
pub mod error;
pub mod evidence;
pub mod postgres;
pub mod write;

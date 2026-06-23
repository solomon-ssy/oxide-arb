//! `ClickHouse` fact repositories built on the storage write manager.

mod fact;

pub use fact::{ChFactWriter, ChQuantFactRepository};

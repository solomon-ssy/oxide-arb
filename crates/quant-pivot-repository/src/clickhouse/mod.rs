//! `ClickHouse` fact repositories built on the storage write manager.

mod fact;
mod fact_read;

pub use fact::{ChFactWriter, ChQuantFactRepository};
pub use fact_read::ChQuantFactReadRepository;

//! `ClickHouse` fact repositories built on the storage write manager.

mod fact;
mod fact_read;
mod feature_parity;

pub use fact::{ChFactWriter, ChQuantFactRepository};
pub use fact_read::ChQuantFactReadRepository;
pub use feature_parity::ChFeatureParityEventRepository;

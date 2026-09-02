//! `ClickHouse` fact repositories built on the storage write manager.

mod fact;
mod fact_read;
mod feature_parity;
mod native_read;
mod query_batch;
mod query_limits;

pub use fact::{ChCanonicalLedgerWriter, ChFactWriter, ChQuantFactRepository};
pub use fact_read::ChQuantFactReadRepository;
pub use feature_parity::ChFeatureParityEventRepository;
pub use native_read::ChNativeReadRepository;

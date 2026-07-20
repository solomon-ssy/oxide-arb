//! `ClickHouse` fact repositories built on the storage write manager.

mod fact;
mod fact_read;
mod feature_parity;
mod native_read;

pub use fact::{ChFactWriter, ChQuantFactRepository};
pub use fact_read::ChQuantFactReadRepository;
pub use feature_parity::ChFeatureParityEventRepository;
pub use native_read::{ChNativeReadRepository, FactEvidenceTable};

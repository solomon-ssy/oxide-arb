//! Durable-trade integrity: reservation rehydration, admission snapshot, recovery gates.

mod service;
mod store;

pub use service::TradeIntegrityStore;
pub use store::TradeIntegrityStoreHandle;

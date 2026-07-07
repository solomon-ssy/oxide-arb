//! Polymarket CTF/NegRisk exchange `OrderFilled` log ingestion helpers.

pub mod constants;
pub mod denylist;
pub mod log_client;
pub mod normalize;
pub mod order_filled_v1;
pub mod order_filled_v2;

pub use constants::{EXCHANGE_CONTRACTS, ExchangeContract, ExchangeVersion};
pub use log_client::{ExchangeLogClient, FetchedLog, LogFetchError};
pub use normalize::{DecodeRejectReason, NormalizedFillLegs, normalize_v1_log, normalize_v2_log};

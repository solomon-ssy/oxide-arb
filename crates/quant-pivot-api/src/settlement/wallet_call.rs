//! Common immutable wallet-call boundary for EOA and relayer envelopes.

use quant_pivot_models::types::{ContentHash, EvmAddress, EvmCalldataHash};

pub trait PreparedWalletCall: Send + Sync {
    fn funder(&self) -> &EvmAddress;
    fn call_target(&self) -> &EvmAddress;
    fn calldata(&self) -> &[u8];
    fn target_adapter(&self) -> &EvmAddress;
    fn deployment_digest(&self) -> ContentHash;
    fn calldata_hash(&self) -> &EvmCalldataHash;
}

//! Shared Alloy-to-domain settlement value conversions.
//!
//! The model crate intentionally does not depend on Alloy. These local extension
//! traits therefore own the unavoidable orphan-rule boundary while each caller
//! maps [`SettlementValueError`] into its operation-specific error taxonomy.

use std::str::FromStr;

use alloy::primitives::{Address, B256, U256};
use quant_pivot_models::types::{
    EvmAddress, EvmBlockHash, EvmCodeHash, EvmTransactionHash, EvmUint256,
};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SettlementValueKind {
    Address,
    Uint,
    BlockHash,
    TransactionHash,
    CodeHash,
}

impl SettlementValueKind {
    const fn label(self) -> &'static str {
        match self {
            Self::Address => "address",
            Self::Uint => "uint256",
            Self::BlockHash => "block hash",
            Self::TransactionHash => "transaction hash",
            Self::CodeHash => "code hash",
        }
    }
}

#[derive(Debug, thiserror::Error)]
#[error("invalid settlement {}: {detail}", kind.label())]
pub struct SettlementValueError {
    kind: SettlementValueKind,
    detail: String,
}

impl SettlementValueError {
    fn new(kind: SettlementValueKind, error: &impl ToString) -> Self {
        Self {
            kind,
            detail: error.to_string(),
        }
    }

    pub(crate) const fn kind(&self) -> SettlementValueKind {
        self.kind
    }

    pub(crate) fn detail(&self) -> &str {
        &self.detail
    }
}

pub trait IntoEvmAddress {
    fn into_evm_address(self) -> Result<EvmAddress, SettlementValueError>;
}

impl IntoEvmAddress for Address {
    fn into_evm_address(self) -> Result<EvmAddress, SettlementValueError> {
        EvmAddress::from_str(&format!("{self:#x}"))
            .map_err(|error| SettlementValueError::new(SettlementValueKind::Address, &error))
    }
}

pub trait IntoAlloyAddress {
    fn into_alloy_address(self) -> Result<Address, SettlementValueError>;
}

impl IntoAlloyAddress for &EvmAddress {
    fn into_alloy_address(self) -> Result<Address, SettlementValueError> {
        Address::from_str(self.as_str())
            .map_err(|error| SettlementValueError::new(SettlementValueKind::Address, &error))
    }
}

pub trait IntoEvmUint {
    fn into_evm_uint(self) -> Result<EvmUint256, SettlementValueError>;
}

impl IntoEvmUint for U256 {
    fn into_evm_uint(self) -> Result<EvmUint256, SettlementValueError> {
        EvmUint256::from_str(&self.to_string())
            .map_err(|error| SettlementValueError::new(SettlementValueKind::Uint, &error))
    }
}

impl IntoEvmUint for u64 {
    fn into_evm_uint(self) -> Result<EvmUint256, SettlementValueError> {
        EvmUint256::from_str(&self.to_string())
            .map_err(|error| SettlementValueError::new(SettlementValueKind::Uint, &error))
    }
}

pub trait IntoEvmBlockHash {
    fn into_evm_block_hash(self) -> Result<EvmBlockHash, SettlementValueError>;
}

impl IntoEvmBlockHash for B256 {
    fn into_evm_block_hash(self) -> Result<EvmBlockHash, SettlementValueError> {
        EvmBlockHash::from_str(&format!("{self:#x}"))
            .map_err(|error| SettlementValueError::new(SettlementValueKind::BlockHash, &error))
    }
}

pub trait IntoEvmTransactionHash {
    fn into_evm_transaction_hash(self) -> Result<EvmTransactionHash, SettlementValueError>;
}

impl IntoEvmTransactionHash for B256 {
    fn into_evm_transaction_hash(self) -> Result<EvmTransactionHash, SettlementValueError> {
        EvmTransactionHash::from_str(&format!("{self:#x}")).map_err(|error| {
            SettlementValueError::new(SettlementValueKind::TransactionHash, &error)
        })
    }
}

pub trait IntoEvmCodeHash {
    fn into_evm_code_hash(self) -> Result<EvmCodeHash, SettlementValueError>;
}

impl IntoEvmCodeHash for B256 {
    fn into_evm_code_hash(self) -> Result<EvmCodeHash, SettlementValueError> {
        EvmCodeHash::from_str(&format!("{self:#x}"))
            .map_err(|error| SettlementValueError::new(SettlementValueKind::CodeHash, &error))
    }
}

#[cfg(test)]
mod tests {
    use alloy::primitives::{Address, B256, U256};

    use super::{
        IntoAlloyAddress, IntoEvmAddress, IntoEvmBlockHash, IntoEvmCodeHash,
        IntoEvmTransactionHash, IntoEvmUint,
    };

    #[test]
    fn address_round_trip() {
        let alloy = Address::from([0x11; 20]);
        let typed = alloy.into_evm_address().expect("canonical address");

        assert_eq!(typed.as_str(), format!("{alloy:#x}"));
        assert_eq!((&typed).into_alloy_address().expect("Alloy address"), alloy);
    }

    #[test]
    fn hashes_remain_canonical() {
        let hash = B256::from([0xab; 32]);
        let canonical = format!("{hash:#x}");

        assert_eq!(
            hash.into_evm_block_hash()
                .expect("canonical block hash")
                .as_str(),
            canonical
        );
        assert_eq!(
            hash.into_evm_transaction_hash()
                .expect("canonical transaction hash")
                .as_str(),
            canonical
        );
        assert_eq!(
            hash.into_evm_code_hash()
                .expect("canonical code hash")
                .as_str(),
            canonical
        );
    }

    #[test]
    fn uint_uses_decimal_text() {
        let value = U256::from(42_u64);

        assert_eq!(
            value.into_evm_uint().expect("canonical uint256").as_str(),
            "42"
        );
        assert_eq!(
            42_u64
                .into_evm_uint()
                .expect("canonical u64 uint256")
                .as_str(),
            "42"
        );
    }
}

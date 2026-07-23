//! Immutable execution-account identity persisted before any account snapshot.

use chrono::{DateTime, Utc};
use quant_pivot_error::hashing::CanonicalDigestError;
use sea_orm::{DeriveIntoActiveModel, DerivePartialModel};
use serde::{Deserialize, Serialize};

use crate::{
    entities::quant_execution_account,
    enums::quant::ExecutionWalletKind,
    hashing::CanonicalDigest,
    types::{ContentHash, EvmAddress, EvmCodeHash, ExecutionAccountId},
};

const EXECUTION_ACCOUNT_DOMAIN: &str = "quant-pivot.execution-account";
const EXECUTION_ACCOUNT_SCHEMA_VERSION: u32 = 1;

/// Canonical execution-account row shared by reports, orders, and settlement.
#[derive(Debug, Clone, Serialize, Deserialize, DerivePartialModel)]
#[sea_orm(entity = "crate::entities::quant_execution_account::Entity")]
pub struct ExecutionAccountInfo {
    pub execution_account_id: ExecutionAccountId,
    pub chain_id: i64,
    pub funder_address: EvmAddress,
    pub wallet_kind: ExecutionWalletKind,
    pub owner_address: EvmAddress,
    pub controller_address: EvmAddress,
    pub wallet_factory_address: Option<EvmAddress>,
    pub wallet_implementation_code_hash: Option<EvmCodeHash>,
    pub identity_digest: ContentHash,
    pub created_at: DateTime<Utc>,
}

info_from_model!(
    ExecutionAccountInfo,
    quant_execution_account::Model,
    {
        execution_account_id,
        chain_id,
        funder_address,
        wallet_kind,
        owner_address,
        controller_address,
        wallet_factory_address,
        wallet_implementation_code_hash,
        identity_digest,
        created_at,
    }
);

/// Insert payload for one content-addressed execution account.
#[derive(Debug, Clone, Serialize, Deserialize, DeriveIntoActiveModel)]
#[sea_orm(active_model = "crate::entities::quant_execution_account::ActiveModel")]
pub struct NewExecutionAccount {
    pub execution_account_id: ExecutionAccountId,
    pub chain_id: i64,
    pub funder_address: EvmAddress,
    pub wallet_kind: ExecutionWalletKind,
    pub owner_address: EvmAddress,
    pub controller_address: EvmAddress,
    pub wallet_factory_address: Option<EvmAddress>,
    pub wallet_implementation_code_hash: Option<EvmCodeHash>,
    pub identity_digest: ContentHash,
}

#[derive(Serialize)]
struct ExecutionAccountIdentity<'a> {
    chain_id: i64,
    funder_address: &'a EvmAddress,
    wallet_kind: ExecutionWalletKind,
    owner_address: &'a EvmAddress,
    controller_address: &'a EvmAddress,
    wallet_factory_address: &'a Option<EvmAddress>,
    wallet_implementation_code_hash: &'a Option<EvmCodeHash>,
}

impl NewExecutionAccount {
    /// Build the deterministic account identity from boot-verified wallet facts.
    pub fn build(
        chain_id: i64,
        funder_address: EvmAddress,
        wallet_kind: ExecutionWalletKind,
        owner_address: EvmAddress,
        controller_address: EvmAddress,
        wallet_factory_address: Option<EvmAddress>,
        wallet_implementation_code_hash: Option<EvmCodeHash>,
    ) -> Result<Self, CanonicalDigestError> {
        let identity_digest = CanonicalDigest::content_hash_typed(
            EXECUTION_ACCOUNT_DOMAIN,
            EXECUTION_ACCOUNT_SCHEMA_VERSION,
            &ExecutionAccountIdentity {
                chain_id,
                funder_address: &funder_address,
                wallet_kind,
                owner_address: &owner_address,
                controller_address: &controller_address,
                wallet_factory_address: &wallet_factory_address,
                wallet_implementation_code_hash: &wallet_implementation_code_hash,
            },
        )?;
        Ok(Self {
            execution_account_id: ExecutionAccountId::from_content_hash(&identity_digest),
            chain_id,
            funder_address,
            wallet_kind,
            owner_address,
            controller_address,
            wallet_factory_address,
            wallet_implementation_code_hash,
            identity_digest,
        })
    }
}

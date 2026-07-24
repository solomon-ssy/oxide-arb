//! Immutable, account-scoped inventory snapshots for full-balance settlement.

use chrono::{DateTime, Utc};
use quant_pivot_error::hashing::CanonicalDigestError;
use sea_orm::{DeriveIntoActiveModel, DerivePartialModel};
use serde::{Deserialize, Serialize};

use crate::{
    entities::quant_settlement_inventory_lot,
    enums::{
        quant::{ExitSettlementMode, OutcomeSide, RedeemPolicy},
        settlement::{SettlementEffectivePolicy, SettlementRoute},
    },
    hashing::CanonicalDigest,
    types::{
        ContentHash, ExecutionAccountId, MarketId, OrderIntentId, PositionId,
        SettlementInventoryLotId, SettlementRedeemId, Shares, TokenId, Usd,
    },
};

const CONTRIBUTOR_LOTS_DOMAIN: &str = "quant-pivot.settlement-contributor-lots";
const INVENTORY_DOMAIN: &str = "quant-pivot.settlement-inventory";
const INVENTORY_SCHEMA_VERSION: u32 = 1;

/// One open lot joined to its immutable execution account and frozen intent policy.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SettlementDiscoveryLot {
    pub position_id: PositionId,
    pub order_intent_id: OrderIntentId,
    pub execution_account_id: ExecutionAccountId,
    pub token_id: TokenId,
    pub side: OutcomeSide,
    pub shares: Shares,
    pub cost_basis_usd: Usd,
    pub settlement_mode: ExitSettlementMode,
    pub redeem_policy: RedeemPolicy,
    pub position_version_at: DateTime<Utc>,
    pub intent_version_at: DateTime<Utc>,
}

/// One resolved market/account candidate sourced entirely from durable `PostgreSQL` truth.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SettlementDiscoveryCandidate {
    pub market_id: MarketId,
    pub yes_token_id: TokenId,
    pub no_token_id: TokenId,
    pub execution_account_id: ExecutionAccountId,
    pub route: SettlementRoute,
    pub resolution_outcome: String,
    pub resolved_at: DateTime<Utc>,
    pub resolution_content_hash: ContentHash,
    pub lots: Vec<SettlementDiscoveryLot>,
}

/// Canonical hashes and policy derived from an exact sorted contributor set.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FrozenSettlementInventory {
    pub effective_policy: SettlementEffectivePolicy,
    pub inventory_digest: ContentHash,
    pub contributor_lots_digest: ContentHash,
    pub lots: Vec<SettlementDiscoveryLot>,
}

#[derive(Serialize)]
struct InventoryDigestScope<'a> {
    market_id: &'a MarketId,
    yes_token_id: &'a TokenId,
    no_token_id: &'a TokenId,
    execution_account_id: ExecutionAccountId,
    route: SettlementRoute,
    outcome: &'a str,
    resolved_at: DateTime<Utc>,
    market_content_hash: ContentHash,
    contributor_lots_digest: ContentHash,
}

impl SettlementDiscoveryCandidate {
    /// Freeze a stable, sorted inventory. Empty or cross-account lots are rejected.
    pub fn freeze(mut self) -> Result<FrozenSettlementInventory, CanonicalDigestError> {
        self.lots.sort_by_key(|lot| lot.position_id.to_string());
        if self.lots.is_empty()
            || self.yes_token_id == self.no_token_id
            || self.lots.iter().any(|lot| {
                lot.execution_account_id != self.execution_account_id
                    || !lot.shares.is_positive()
                    || lot.cost_basis_usd.is_negative()
                    || match lot.side {
                        OutcomeSide::Yes => lot.token_id != self.yes_token_id,
                        OutcomeSide::No => lot.token_id != self.no_token_id,
                    }
            })
        {
            return Err(CanonicalDigestError::Serialize(
                "settlement inventory must contain positive, account-scoped lots".to_owned(),
            ));
        }
        let contributor_lots_digest = CanonicalDigest::content_hash_typed(
            CONTRIBUTOR_LOTS_DOMAIN,
            INVENTORY_SCHEMA_VERSION,
            &self.lots,
        )?;
        let inventory_digest = CanonicalDigest::content_hash_typed(
            INVENTORY_DOMAIN,
            INVENTORY_SCHEMA_VERSION,
            &InventoryDigestScope {
                market_id: &self.market_id,
                yes_token_id: &self.yes_token_id,
                no_token_id: &self.no_token_id,
                execution_account_id: self.execution_account_id,
                route: self.route,
                outcome: &self.resolution_outcome,
                resolved_at: self.resolved_at,
                market_content_hash: self.resolution_content_hash,
                contributor_lots_digest,
            },
        )?;
        let automatic = self.lots.iter().all(|lot| {
            lot.settlement_mode == ExitSettlementMode::HoldToResolution
                && lot.redeem_policy == RedeemPolicy::Auto
        });
        Ok(FrozenSettlementInventory {
            effective_policy: if automatic {
                SettlementEffectivePolicy::AutomaticEligible
            } else {
                SettlementEffectivePolicy::ManualOnly
            },
            inventory_digest,
            contributor_lots_digest,
            lots: self.lots,
        })
    }
}

/// Persisted immutable contributor row for one inventory digest.
#[derive(Debug, Clone, Serialize, Deserialize, DerivePartialModel)]
#[sea_orm(entity = "quant_settlement_inventory_lot::Entity")]
pub struct SettlementInventoryLotInfo {
    pub settlement_inventory_lot_id: SettlementInventoryLotId,
    pub settlement_redeem_id: SettlementRedeemId,
    pub inventory_digest: ContentHash,
    pub contributor_lots_digest: ContentHash,
    pub execution_account_id: ExecutionAccountId,
    pub position_id: PositionId,
    pub order_intent_id: OrderIntentId,
    pub token_id: TokenId,
    pub side: OutcomeSide,
    pub shares: Shares,
    pub cost_basis_usd: Usd,
    pub settlement_mode: ExitSettlementMode,
    pub redeem_policy: RedeemPolicy,
    pub position_version_at: DateTime<Utc>,
    pub intent_version_at: DateTime<Utc>,
    pub created_at: DateTime<Utc>,
}

info_from_model!(
    SettlementInventoryLotInfo,
    quant_settlement_inventory_lot::Model,
    {
        settlement_inventory_lot_id,
        settlement_redeem_id,
        inventory_digest,
        contributor_lots_digest,
        execution_account_id,
        position_id,
        order_intent_id,
        token_id,
        side,
        shares,
        cost_basis_usd,
        settlement_mode,
        redeem_policy,
        position_version_at,
        intent_version_at,
        created_at,
    }
);

/// Insert payload for one immutable contributor row.
#[derive(Debug, Clone, Serialize, Deserialize, DeriveIntoActiveModel)]
#[sea_orm(active_model = "quant_settlement_inventory_lot::ActiveModel")]
pub struct NewSettlementInventoryLot {
    pub settlement_inventory_lot_id: SettlementInventoryLotId,
    pub settlement_redeem_id: SettlementRedeemId,
    pub inventory_digest: ContentHash,
    pub contributor_lots_digest: ContentHash,
    pub execution_account_id: ExecutionAccountId,
    pub position_id: PositionId,
    pub order_intent_id: OrderIntentId,
    pub token_id: TokenId,
    pub side: OutcomeSide,
    pub shares: Shares,
    pub cost_basis_usd: Usd,
    pub settlement_mode: ExitSettlementMode,
    pub redeem_policy: RedeemPolicy,
    pub position_version_at: DateTime<Utc>,
    pub intent_version_at: DateTime<Utc>,
}

/// CAS refresh of a pre-submission case after late fills or reconciliation changed inventory.
#[derive(Debug, Clone)]
pub struct RefreshSettlementInventory {
    pub settlement_redeem_id: SettlementRedeemId,
    pub expected_inventory_digest: ContentHash,
    pub yes_token_id: TokenId,
    pub no_token_id: TokenId,
    pub resolution_content_hash: ContentHash,
    pub resolution_outcome: String,
    pub resolved_at: DateTime<Utc>,
    pub effective_policy: SettlementEffectivePolicy,
    pub inventory_digest: ContentHash,
    pub contributor_lots_digest: ContentHash,
    pub lots: Vec<NewSettlementInventoryLot>,
    pub observed_at: DateTime<Utc>,
}

/// CAS transition used when the durable open-lot inventory becomes empty.
#[derive(Debug, Clone)]
pub struct MarkSettlementInventoryAbsent {
    pub settlement_redeem_id: SettlementRedeemId,
    pub expected_inventory_digest: ContentHash,
    pub observed_at: DateTime<Utc>,
}

impl FrozenSettlementInventory {
    /// Materialize append-only rows for one case and digest.
    #[must_use]
    pub fn into_rows(
        self,
        settlement_redeem_id: SettlementRedeemId,
    ) -> Vec<NewSettlementInventoryLot> {
        self.lots
            .into_iter()
            .map(|lot| NewSettlementInventoryLot {
                settlement_inventory_lot_id: SettlementInventoryLotId::from_v7(),
                settlement_redeem_id,
                inventory_digest: self.inventory_digest,
                contributor_lots_digest: self.contributor_lots_digest,
                execution_account_id: lot.execution_account_id,
                position_id: lot.position_id,
                order_intent_id: lot.order_intent_id,
                token_id: lot.token_id,
                side: lot.side,
                shares: lot.shares,
                cost_basis_usd: lot.cost_basis_usd,
                settlement_mode: lot.settlement_mode,
                redeem_policy: lot.redeem_policy,
                position_version_at: lot.position_version_at,
                intent_version_at: lot.intent_version_at,
            })
            .collect()
    }
}

#[cfg(test)]
mod tests {
    use chrono::{DateTime, Utc};
    use rust_decimal::Decimal;

    use super::*;

    #[test]
    fn inventory_digest_order_wide() {
        let automatic = lot(
            ExecutionAccountId::from_v7(),
            ExitSettlementMode::HoldToResolution,
            RedeemPolicy::Auto,
        );
        let account = automatic.execution_account_id;
        let manual = lot(
            account,
            ExitSettlementMode::HoldToResolution,
            RedeemPolicy::Manual,
        );
        let left = candidate(account, vec![automatic.clone(), manual.clone()])
            .freeze()
            .expect("valid mixed inventory");
        let right = candidate(account, vec![manual, automatic])
            .freeze()
            .expect("same inventory in reverse order");
        assert_eq!(left.inventory_digest, right.inventory_digest);
        assert_eq!(left.effective_policy, SettlementEffectivePolicy::ManualOnly);
    }

    #[test]
    fn cross_account_contributor_rejects() {
        let account = ExecutionAccountId::from_v7();
        let foreign = lot(
            ExecutionAccountId::from_v7(),
            ExitSettlementMode::HoldToResolution,
            RedeemPolicy::Auto,
        );
        assert!(candidate(account, vec![foreign]).freeze().is_err());
    }

    #[test]
    fn only_hold_resolution_eligible() {
        let account = ExecutionAccountId::from_v7();
        let automatic = candidate(
            account,
            vec![lot(
                account,
                ExitSettlementMode::HoldToResolution,
                RedeemPolicy::Auto,
            )],
        )
        .freeze()
        .expect("valid automatic inventory");
        assert_eq!(
            automatic.effective_policy,
            SettlementEffectivePolicy::AutomaticEligible
        );

        for (settlement_mode, redeem_policy) in [
            (ExitSettlementMode::HoldToResolution, RedeemPolicy::Manual),
            (ExitSettlementMode::ExitBeforeResolution, RedeemPolicy::Auto),
            (
                ExitSettlementMode::ExitBeforeResolution,
                RedeemPolicy::Manual,
            ),
        ] {
            let frozen = candidate(account, vec![lot(account, settlement_mode, redeem_policy)])
                .freeze()
                .expect("valid manual-only inventory");
            assert_eq!(
                frozen.effective_policy,
                SettlementEffectivePolicy::ManualOnly
            );
        }
    }

    fn candidate(
        execution_account_id: ExecutionAccountId,
        lots: Vec<SettlementDiscoveryLot>,
    ) -> SettlementDiscoveryCandidate {
        SettlementDiscoveryCandidate {
            market_id: MarketId::new("settlement-inventory-test"),
            yes_token_id: TokenId::new("101"),
            no_token_id: TokenId::new("102"),
            execution_account_id,
            route: SettlementRoute::StandardV2,
            resolution_outcome: "Yes".to_owned(),
            resolved_at: timestamp(),
            resolution_content_hash: ContentHash::from_bytes([0x71; 32]),
            lots,
        }
    }

    fn lot(
        execution_account_id: ExecutionAccountId,
        settlement_mode: ExitSettlementMode,
        redeem_policy: RedeemPolicy,
    ) -> SettlementDiscoveryLot {
        SettlementDiscoveryLot {
            position_id: PositionId::from_v7(),
            order_intent_id: OrderIntentId::from_v7(),
            execution_account_id,
            token_id: TokenId::new("101"),
            side: OutcomeSide::Yes,
            shares: Shares::new(Decimal::from(10)),
            cost_basis_usd: Usd::new(Decimal::from(4)),
            settlement_mode,
            redeem_policy,
            position_version_at: timestamp(),
            intent_version_at: timestamp(),
        }
    }

    fn timestamp() -> DateTime<Utc> {
        DateTime::from_timestamp(1_700_000_000, 0).expect("valid fixture timestamp")
    }
}

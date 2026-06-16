//! Live portfolio preflight for pending on-chain redeem positions.
//!
//! Before entering Live or activating a runtime config, every open position with
//! `redeem_status = Pending` must have a valid immutable redeem snapshot. Legacy
//! rows missing a fill-time snapshot are backfilled from the candidate policy
//! when the market registry is authoritative.

use crate::pipeline::market_registry::MarketRegistry;
use oxide_arb_models::{
    domain::{RuntimeControlError, position::PositionRedeemSnapshot},
    enums::common::ExecutionMode,
    runtime_config::{RedeemRoutingPolicy, validation::validate_pending_redeem_snapshots},
};
use oxide_arb_repository::traits::PositionRepository;

/// Fail-closed gate: backfill legacy snapshots, then validate every pending
/// redeem position can execute under the candidate policy.
pub async fn ensure_live_pending_redeem_portfolio(
    position_repo: &dyn PositionRepository,
    market_registry: &MarketRegistry,
    policy: &RedeemRoutingPolicy,
    mode: ExecutionMode,
) -> Result<(), RuntimeControlError> {
    if mode != ExecutionMode::Live {
        return Ok(());
    }

    let pending = position_repo
        .find_open_pending_redeem()
        .await
        .map_err(|error| {
            RuntimeControlError::Precondition(format!(
                "failed to load pending redeem positions: {error}"
            ))
        })?;

    for position in &pending {
        if !position.redeem_snapshot_needs_backfill() {
            continue;
        }
        let neg_risk = market_registry
            .neg_risk(&position.market_id)
            .ok_or_else(|| {
                RuntimeControlError::Precondition(format!(
                    "market {} not in registry for pending redeem position {}",
                    position.market_id, position.position_id
                ))
            })?;
        let Some(plan) = policy.resolve(&position.market_id, neg_risk) else {
            return Err(RuntimeControlError::Precondition(format!(
                "settlement.redeem: cannot resolve route for pending position {} \
                 (market {}, neg_risk={neg_risk})",
                position.position_id, position.market_id
            )));
        };
        let snapshot = PositionRedeemSnapshot::from_plan(neg_risk, plan);
        position_repo
            .update_redeem_snapshot(&position.position_id, &snapshot)
            .await
            .map_err(|error| {
                RuntimeControlError::Precondition(format!(
                    "failed to backfill redeem snapshot for position {}: {error}",
                    position.position_id
                ))
            })?;
    }

    let pending = position_repo
        .find_open_pending_redeem()
        .await
        .map_err(|error| {
            RuntimeControlError::Precondition(format!(
                "failed to reload pending redeem positions: {error}"
            ))
        })?;
    let report = validate_pending_redeem_snapshots(&pending);
    if report.has_errors() {
        return Err(RuntimeControlError::Precondition(report.to_string()));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::pipeline::market_registry::MarketRegistry;
    use chrono::Utc;
    use oxide_arb_models::{
        domain::{
            market::{MarketRegistryInfo, TokenInfo},
            position::PositionInfo,
        },
        enums::common::{
            CategorySet, ExecutionMode, MarketCategory, PositionStatus, RedeemResolutionSource,
            RedeemStatus, SettlementAccountingStatus, Side, TickSize,
        },
        enums::market::MarketStatus,
        types::{EventId, MarketId, PositionId, Price, Shares, TokenId, TradeId, Usd},
    };
    use oxide_arb_test_support::mocks::MockPositionRepository;
    use rust_decimal_macros::dec;

    fn market_id() -> MarketId {
        MarketId::new("0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa")
    }

    fn pending_position() -> PositionInfo {
        let now = Utc::now();
        PositionInfo {
            position_id: PositionId::from_v7(),
            trade_id: TradeId::from_v7(),
            market_id: market_id(),
            token_id: TokenId::new("111"),
            side: Side::Buy,
            execution_mode: ExecutionMode::Live,
            shares: Shares::new(dec!(10)),
            avg_entry_price: Price::new(dec!(0.9)),
            total_cost_usd: Usd::new(dec!(9)),
            total_fees_usd: Usd::ZERO,
            unrealized_pnl: Usd::ZERO,
            realized_pnl: Usd::ZERO,
            status: PositionStatus::Open,
            opened_at: now,
            closed_at: None,
            settled_at: None,
            winning_token_id: None,
            settlement_payout_usd: None,
            redeem_tx_hash: None,
            redeem_status: RedeemStatus::Pending,
            redeem_attempts: 0,
            oracle_verdict: None,
            settlement_trigger: None,
            settlement_accounting_status: SettlementAccountingStatus::Pending,
            settlement_accounting_error: None,
            settlement_accounted_at: None,
            redeem_terminal_reason: None,
            redeem_neg_risk: true,
            redeem_route: "standard_ctf".into(),
            redeem_holder_address: None,
            redeem_resolution: RedeemResolutionSource::ClassStandard,
            redeem_gas_limit: 500_000,
        }
    }

    fn registry() -> MarketRegistry {
        let registry = MarketRegistry::new();
        registry.register_market(MarketRegistryInfo {
            market_id: market_id(),
            event_id: EventId::new("evt-preflight"),
            token_yes: TokenId::new("111"),
            token_no: TokenId::new("222"),
            question: "Preflight?".into(),
            slug: "preflight".into(),
            categories: CategorySet::from(MarketCategory::Politics),
            status: MarketStatus::Active,
            outcome: None,
            neg_risk: true,
            tick_size: TickSize::Hundredth,
            tokens: vec![
                TokenInfo {
                    token_id: TokenId::new("111"),
                    outcome: "Yes".into(),
                    neg_risk: true,
                },
                TokenInfo {
                    token_id: TokenId::new("222"),
                    outcome: "No".into(),
                    neg_risk: true,
                },
            ],
            best_bid: None,
            best_ask: None,
            depth_usd: None,
            min_order_size: dec!(5),
            volume_24h: Usd::ZERO,
            fee_schedule: None,
            end_date: None,
            resolved_at: None,
            created_at: Utc::now(),
            updated_at: Utc::now(),
        });
        registry
    }

    #[tokio::test]
    async fn live_preflight_backfills_invalid_pending_snapshot() {
        let repo = MockPositionRepository::default();
        repo.insert(pending_position());
        let registry = registry();

        ensure_live_pending_redeem_portfolio(
            &repo,
            &registry,
            &RedeemRoutingPolicy::default(),
            ExecutionMode::Live,
        )
        .await
        .expect("preflight");

        let positions = repo.positions_snapshot();
        assert_eq!(positions.len(), 1);
        assert!(positions[0].redeem_neg_risk);
        assert_eq!(positions[0].redeem_route, "neg_risk_legacy_adapter");
        assert_eq!(
            positions[0].redeem_resolution,
            RedeemResolutionSource::ClassNegRisk
        );
    }

    #[tokio::test]
    async fn simulated_mode_skips_pending_redeem_portfolio_preflight() {
        let repo = MockPositionRepository::default();
        repo.insert(pending_position());

        ensure_live_pending_redeem_portfolio(
            &repo,
            &MarketRegistry::new(),
            &RedeemRoutingPolicy::default(),
            ExecutionMode::Paper,
        )
        .await
        .expect("paper preflight");
    }
}

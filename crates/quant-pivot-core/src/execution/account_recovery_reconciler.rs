//! Deterministic account-recovery assessment with explicit ambiguous-lot allocation.

use std::collections::{BTreeMap, BTreeSet, HashSet, VecDeque};

use chrono::{DateTime, Utc};
use quant_pivot_error::{QuantError, QuantResult};
use quant_pivot_models::{
    domain::quant::{
        AccountRecoveryAssessment, AccountRecoveryAssessmentInput, AccountRecoveryCreatedLot,
        AccountRecoveryExecutionDelta, AccountRecoveryLotAllocation, AccountRecoveryMismatch,
        AccountRecoverySellAllocation, AccountRecoveryTokenBalance,
    },
    hashing::CanonicalDigest,
    types::{AccountRecoveryIncidentId, Shares, StrategyPositionLotId, TokenId, Usd},
};
use rust_decimal::Decimal;
use serde::Serialize;

const EVIDENCE_DOMAIN: &str = "quant-pivot/account-recovery-assessment";
const EVIDENCE_VERSION: u32 = 1;

#[derive(Debug)]
enum WorkingLot {
    Existing(AccountRecoveryLotAllocation),
    Created(AccountRecoveryCreatedLot),
}

impl WorkingLot {
    const fn id(&self) -> StrategyPositionLotId {
        match self {
            Self::Existing(lot) => lot.strategy_position_lot_id,
            Self::Created(lot) => lot.strategy_position_lot_id,
        }
    }

    const fn remaining(&self) -> Shares {
        match self {
            Self::Existing(lot) => lot.after_shares,
            Self::Created(lot) => lot.remaining_shares,
        }
    }

    const fn set_remaining(&mut self, shares: Shares) {
        match self {
            Self::Existing(lot) => lot.after_shares = shares,
            Self::Created(lot) => lot.remaining_shares = shares,
        }
    }

    const fn cost(&self) -> Usd {
        match self {
            Self::Existing(lot) => lot.after_cost_usd,
            Self::Created(lot) => lot.remaining_cost_usd,
        }
    }

    fn apply_exit(&mut self, shares: Shares, cost: Usd, pnl: Usd, exited_at: DateTime<Utc>) {
        self.set_remaining(Shares::new(self.remaining().inner() - shares.inner()));
        match self {
            Self::Existing(lot) => {
                lot.after_cost_usd -= cost;
                lot.realized_pnl_delta_usd += pnl;
                if lot.after_shares.is_zero() {
                    lot.closed_at = Some(exited_at);
                }
            }
            Self::Created(lot) => {
                lot.remaining_cost_usd -= cost;
                lot.realized_pnl_delta_usd += pnl;
                if lot.remaining_shares.is_zero() {
                    lot.closed_at = Some(exited_at);
                }
            }
        }
    }
}

#[derive(Serialize)]
struct AssessmentPreimage<'a> {
    input: &'a AccountRecoveryAssessmentInput,
    allocations: &'a [AccountRecoveryLotAllocation],
    created_lots: &'a [AccountRecoveryCreatedLot],
    mismatches: &'a [AccountRecoveryMismatch],
}

pub struct AccountRecoveryReconciler;

impl AccountRecoveryReconciler {
    pub fn assess(
        mut input: AccountRecoveryAssessmentInput,
    ) -> QuantResult<AccountRecoveryAssessment> {
        canonicalize(&mut input);
        let mut mismatches = source_mismatches(&input);
        let (allocations, created_lots, ledger_mismatches) = allocate_lots(&input);
        mismatches.extend(ledger_mismatches);
        let mut evidence_input = input.clone();
        evidence_input.observed_at = DateTime::<Utc>::UNIX_EPOCH;
        let evidence_hash = CanonicalDigest::content_hash_typed(
            EVIDENCE_DOMAIN,
            EVIDENCE_VERSION,
            &AssessmentPreimage {
                input: &evidence_input,
                allocations: &allocations,
                created_lots: &created_lots,
                mismatches: &mismatches,
            },
        )
        .map_err(QuantError::from)?;
        Ok(AccountRecoveryAssessment {
            allocations,
            created_lots,
            mismatches,
            evidence_hash,
        })
    }
}

fn canonicalize(input: &mut AccountRecoveryAssessmentInput) {
    input
        .open_order_ids
        .sort_by(|left, right| left.as_str().cmp(right.as_str()));
    input.open_order_ids.dedup();
    input
        .unmapped_token_ids
        .sort_by(|left, right| left.as_str().cmp(right.as_str()));
    input.unmapped_token_ids.dedup();
    input.invalid_execution_ids.sort_by_key(|id| id.as_uuid());
    input.invalid_execution_ids.dedup();
    sort_balances(&mut input.data_api_positions);
    sort_balances(&mut input.chain_positions);
    input.open_lots.sort_by(|left, right| {
        left.opened_at.cmp(&right.opened_at).then_with(|| {
            left.strategy_position_lot_id
                .to_string()
                .cmp(&right.strategy_position_lot_id.to_string())
        })
    });
    input.incident_executions.sort_by(|left, right| {
        left.available_at.cmp(&right.available_at).then_with(|| {
            left.account_chain_execution_id
                .to_string()
                .cmp(&right.account_chain_execution_id.to_string())
        })
    });
    input.explicit_sell_allocations.sort_by(|left, right| {
        left.account_chain_execution_id
            .as_uuid()
            .cmp(&right.account_chain_execution_id.as_uuid())
            .then_with(|| {
                left.strategy_position_lot_id
                    .as_uuid()
                    .cmp(&right.strategy_position_lot_id.as_uuid())
            })
    });
}

fn sort_balances(balances: &mut [AccountRecoveryTokenBalance]) {
    balances.sort_by(|left, right| left.token_id.as_str().cmp(right.token_id.as_str()));
}

fn source_mismatches(input: &AccountRecoveryAssessmentInput) -> Vec<AccountRecoveryMismatch> {
    let mut mismatches = Vec::new();
    if !input.pause_confirmed {
        mismatches.push(AccountRecoveryMismatch::PauseIncomplete);
    }
    if !input.venue_snapshot_stable {
        mismatches.push(AccountRecoveryMismatch::VenueSnapshotUnstable);
    }
    if !input.open_order_ids.is_empty() {
        mismatches.push(AccountRecoveryMismatch::OpenOrdersPresent {
            order_ids: input.open_order_ids.clone(),
        });
    }
    mismatches.extend(
        input
            .unmapped_token_ids
            .iter()
            .cloned()
            .map(|token_id| AccountRecoveryMismatch::TokenMetadataMissing { token_id }),
    );
    if let Some(blocker) = &input.clean_funder_blocker {
        mismatches.push(AccountRecoveryMismatch::CleanFunderRequired {
            account_chain_execution_id: blocker.account_chain_execution_id,
            role: blocker.role,
        });
    }
    mismatches.extend(input.invalid_execution_ids.iter().copied().map(
        |account_chain_execution_id| AccountRecoveryMismatch::IncidentExecutionIncomplete {
            account_chain_execution_id,
        },
    ));
    if !input.reserved_usd.is_zero() {
        mismatches.push(AccountRecoveryMismatch::ReservedCapitalPresent {
            reserved_usd: input.reserved_usd,
        });
    }
    if input.clob_collateral_usd != input.chain_collateral_usd {
        mismatches.push(AccountRecoveryMismatch::CollateralMismatch {
            clob_usd: input.clob_collateral_usd,
            chain_usd: input.chain_collateral_usd,
        });
    }
    compare_position_sources(input, &mut mismatches);
    if input.pending_settlement_count > 0 {
        mismatches.push(AccountRecoveryMismatch::PendingSettlement {
            count: input.pending_settlement_count,
        });
    }
    mismatches
}

fn compare_position_sources(
    input: &AccountRecoveryAssessmentInput,
    mismatches: &mut Vec<AccountRecoveryMismatch>,
) {
    let data_api = aggregate_balances(&input.data_api_positions);
    let chain = aggregate_balances(&input.chain_positions);
    let tokens = data_api
        .keys()
        .chain(chain.keys())
        .cloned()
        .collect::<BTreeSet<_>>();
    for token_id in tokens {
        let data_api_shares = Shares::new(*data_api.get(&token_id).unwrap_or(&Decimal::ZERO));
        let chain_shares = Shares::new(*chain.get(&token_id).unwrap_or(&Decimal::ZERO));
        if data_api_shares != chain_shares {
            mismatches.push(AccountRecoveryMismatch::PositionSourceMismatch {
                token_id,
                data_api_shares,
                chain_shares,
            });
        }
    }
}

fn allocate_lots(
    input: &AccountRecoveryAssessmentInput,
) -> (
    Vec<AccountRecoveryLotAllocation>,
    Vec<AccountRecoveryCreatedLot>,
    Vec<AccountRecoveryMismatch>,
) {
    let mut lots = BTreeMap::<TokenId, VecDeque<WorkingLot>>::new();
    for lot in &input.open_lots {
        lots.entry(lot.token_id.clone())
            .or_default()
            .push_back(WorkingLot::Existing(AccountRecoveryLotAllocation {
                strategy_position_lot_id: lot.strategy_position_lot_id,
                token_id: lot.token_id.clone(),
                before_shares: lot.shares,
                after_shares: lot.shares,
                before_cost_usd: lot.cost_usd,
                after_cost_usd: lot.cost_usd,
                realized_pnl_delta_usd: Usd::ZERO,
                closed_at: None,
            }));
    }
    let mut incomplete = input
        .explicit_sell_allocations
        .iter()
        .filter(|allocation| {
            !input.incident_executions.iter().any(|execution| {
                execution.account_chain_execution_id == allocation.account_chain_execution_id
                    && execution.shares_delta < Decimal::ZERO
            })
        })
        .map(|allocation| AccountRecoveryMismatch::LotAllocationInvalid {
            account_chain_execution_id: allocation.account_chain_execution_id,
        })
        .collect::<Vec<_>>();
    for execution in &input.incident_executions {
        apply_execution(
            &mut lots,
            input.recovery_incident_id,
            execution,
            &input.explicit_sell_allocations,
            &mut incomplete,
        );
    }
    let chain = aggregate_balances(&input.chain_positions);
    let mut allocations = Vec::new();
    let mut created_lots = Vec::new();
    let mut expected = BTreeMap::<TokenId, Decimal>::new();
    for (token_id, token_lots) in lots {
        for lot in token_lots {
            *expected.entry(token_id.clone()).or_default() += lot.remaining().inner();
            match lot {
                WorkingLot::Existing(lot) => allocations.push(lot),
                WorkingLot::Created(lot) => created_lots.push(lot),
            }
        }
    }
    let tokens = expected
        .keys()
        .chain(chain.keys())
        .cloned()
        .collect::<BTreeSet<_>>();
    let mut mismatches = incomplete;
    for token_id in tokens {
        let expected_shares = Shares::new(*expected.get(&token_id).unwrap_or(&Decimal::ZERO));
        let venue_shares = Shares::new(*chain.get(&token_id).unwrap_or(&Decimal::ZERO));
        if expected_shares != venue_shares {
            mismatches.push(AccountRecoveryMismatch::PositionLedgerMismatch {
                token_id,
                expected_shares,
                venue_shares,
            });
        }
    }
    (allocations, created_lots, mismatches)
}

fn apply_execution(
    lots: &mut BTreeMap<TokenId, VecDeque<WorkingLot>>,
    incident_id: AccountRecoveryIncidentId,
    execution: &AccountRecoveryExecutionDelta,
    explicit_allocations: &[AccountRecoverySellAllocation],
    mismatches: &mut Vec<AccountRecoveryMismatch>,
) {
    if execution.shares_delta > Decimal::ZERO {
        let shares = Shares::new(execution.shares_delta);
        let cost = execution.principal_usd + execution.exact_fee_usd;
        lots.entry(execution.token_id.clone())
            .or_default()
            .push_back(WorkingLot::Created(AccountRecoveryCreatedLot {
                strategy_position_lot_id: StrategyPositionLotId::from_recovery_execution(
                    incident_id,
                    execution.account_chain_execution_id,
                ),
                account_chain_execution_id: execution.account_chain_execution_id,
                token_id: execution.token_id.clone(),
                acquired_shares: shares,
                remaining_shares: shares,
                acquired_cost_usd: cost,
                remaining_cost_usd: cost,
                realized_pnl_delta_usd: Usd::ZERO,
                closed_at: None,
            }));
        return;
    }
    let sold_shares = -execution.shares_delta;
    if execution.exact_fee_usd > execution.principal_usd || sold_shares <= Decimal::ZERO {
        mismatches.push(AccountRecoveryMismatch::IncidentExecutionIncomplete {
            account_chain_execution_id: execution.account_chain_execution_id,
        });
        return;
    }
    let net_proceeds = execution.principal_usd - execution.exact_fee_usd;
    let token_lots = lots.entry(execution.token_id.clone()).or_default();
    let available = token_lots
        .iter()
        .filter(|lot| lot.remaining().is_positive())
        .map(|lot| (lot.id(), lot.remaining().inner()))
        .collect::<Vec<_>>();
    let total_available = available.iter().map(|(_, shares)| *shares).sum::<Decimal>();
    if sold_shares > total_available {
        mismatches.push(AccountRecoveryMismatch::IncidentExecutionIncomplete {
            account_chain_execution_id: execution.account_chain_execution_id,
        });
        return;
    }
    let selected = explicit_allocations
        .iter()
        .filter(|allocation| {
            allocation.account_chain_execution_id == execution.account_chain_execution_id
        })
        .collect::<Vec<_>>();
    let allocation = if available.len() == 1 || sold_shares == total_available {
        if !selected.is_empty() {
            mismatches.push(AccountRecoveryMismatch::LotAllocationInvalid {
                account_chain_execution_id: execution.account_chain_execution_id,
            });
            return;
        }
        let mut remaining = sold_shares;
        available
            .into_iter()
            .filter_map(|(lot_id, shares)| {
                if remaining <= Decimal::ZERO {
                    return None;
                }
                let allocated = shares.min(remaining);
                remaining -= allocated;
                Some((lot_id, allocated))
            })
            .collect::<Vec<_>>()
    } else {
        if selected.is_empty() {
            mismatches.push(AccountRecoveryMismatch::LotAllocationRequired {
                account_chain_execution_id: execution.account_chain_execution_id,
                token_id: execution.token_id.clone(),
                sold_shares: Shares::new(sold_shares),
                candidate_lot_ids: available.into_iter().map(|(id, _)| id).collect(),
            });
            return;
        }
        let mut seen = HashSet::new();
        let mut allocated = Decimal::ZERO;
        let mut result = Vec::with_capacity(selected.len());
        for selection in selected {
            let Some((_, available_shares)) = available
                .iter()
                .find(|(lot_id, _)| *lot_id == selection.strategy_position_lot_id)
            else {
                mismatches.push(AccountRecoveryMismatch::LotAllocationInvalid {
                    account_chain_execution_id: execution.account_chain_execution_id,
                });
                return;
            };
            if !selection.shares.is_positive()
                || selection.shares.inner() > *available_shares
                || !seen.insert(selection.strategy_position_lot_id)
            {
                mismatches.push(AccountRecoveryMismatch::LotAllocationInvalid {
                    account_chain_execution_id: execution.account_chain_execution_id,
                });
                return;
            }
            allocated += selection.shares.inner();
            result.push((selection.strategy_position_lot_id, selection.shares.inner()));
        }
        if allocated != sold_shares {
            mismatches.push(AccountRecoveryMismatch::LotAllocationInvalid {
                account_chain_execution_id: execution.account_chain_execution_id,
            });
            return;
        }
        result
    };
    for (lot_id, deducted) in allocation {
        let Some(lot) = token_lots.iter_mut().find(|lot| lot.id() == lot_id) else {
            mismatches.push(AccountRecoveryMismatch::LotAllocationInvalid {
                account_chain_execution_id: execution.account_chain_execution_id,
            });
            return;
        };
        let before_shares = lot.remaining().inner();
        let cost_removed = Usd::new(lot.cost().inner() * deducted / before_shares);
        let proceeds = Usd::new(net_proceeds.inner() * deducted / sold_shares);
        lot.apply_exit(
            Shares::new(deducted),
            cost_removed,
            proceeds - cost_removed,
            execution.available_at,
        );
    }
}

fn aggregate_balances(balances: &[AccountRecoveryTokenBalance]) -> BTreeMap<TokenId, Decimal> {
    let mut aggregate = BTreeMap::new();
    for balance in balances {
        *aggregate.entry(balance.token_id.clone()).or_default() += balance.shares.inner();
    }
    aggregate
}

#[cfg(test)]
mod tests {
    use chrono::{Duration, TimeZone, Utc};
    use quant_pivot_error::{QuantError, QuantResult};
    use quant_pivot_models::{
        domain::quant::{
            AccountRecoveryAssessmentInput, AccountRecoveryExecutionDelta,
            AccountRecoveryLotBalance, AccountRecoveryMismatch, AccountRecoverySellAllocation,
            AccountRecoveryTokenBalance,
        },
        types::{
            AccountChainExecutionId, AccountRecoveryIncidentId, ContentHash, EvmBlockHash,
            ExecutionAccountId, OrderId, Shares, StrategyPositionLotId, TokenId, Usd,
        },
    };
    use rust_decimal_macros::dec;

    use super::AccountRecoveryReconciler;

    use rust_decimal::Decimal;

    fn balance(token: &TokenId, shares: Decimal) -> AccountRecoveryTokenBalance {
        AccountRecoveryTokenBalance {
            token_id: token.clone(),
            shares: Shares::new(shares),
        }
    }

    fn input() -> QuantResult<AccountRecoveryAssessmentInput> {
        let token = TokenId::new("token-a");
        Ok(AccountRecoveryAssessmentInput {
            recovery_incident_id: AccountRecoveryIncidentId::from_v7(),
            execution_account_id: ExecutionAccountId::from_v7(),
            observed_at: Utc.timestamp_opt(30, 0).single().unwrap_or_default(),
            finalized_block_number: 100,
            finalized_block_hash: EvmBlockHash::parse(format!("0x{}", "11".repeat(32)))
                .map_err(|error| QuantError::config(error.to_string()))?,
            clob_snapshot_hash: ContentHash::from_bytes([1; 32]),
            data_api_snapshot_hash: ContentHash::from_bytes([2; 32]),
            chain_snapshot_hash: ContentHash::from_bytes([3; 32]),
            settlement_snapshot_hash: ContentHash::from_bytes([4; 32]),
            pause_confirmed: true,
            venue_snapshot_stable: true,
            clob_collateral_usd: Usd::new(dec!(100)),
            chain_collateral_usd: Usd::new(dec!(100)),
            reserved_usd: Usd::ZERO,
            open_order_ids: Vec::new(),
            unmapped_token_ids: Vec::new(),
            invalid_execution_ids: Vec::new(),
            clean_funder_blocker: None,
            data_api_positions: vec![balance(&token, dec!(9))],
            chain_positions: vec![balance(&token, dec!(9))],
            open_lots: vec![AccountRecoveryLotBalance {
                strategy_position_lot_id: StrategyPositionLotId::from_v7(),
                token_id: token.clone(),
                shares: Shares::new(dec!(10)),
                cost_usd: Usd::new(dec!(5)),
                opened_at: Utc.timestamp_opt(10, 0).single().unwrap_or_default(),
            }],
            incident_executions: vec![
                AccountRecoveryExecutionDelta {
                    account_chain_execution_id: AccountChainExecutionId::from_v7(),
                    token_id: token.clone(),
                    shares_delta: dec!(-3),
                    principal_usd: Usd::new(dec!(2.1)),
                    exact_fee_usd: Usd::new(dec!(0.1)),
                    available_at: Utc.timestamp_opt(20, 0).single().unwrap_or_default(),
                },
                AccountRecoveryExecutionDelta {
                    account_chain_execution_id: AccountChainExecutionId::from_v7(),
                    token_id: token,
                    shares_delta: dec!(2),
                    principal_usd: Usd::new(dec!(1)),
                    exact_fee_usd: Usd::new(dec!(0.02)),
                    available_at: Utc.timestamp_opt(21, 0).single().unwrap_or_default(),
                },
            ],
            explicit_sell_allocations: Vec::new(),
            pending_settlement_count: 0,
        })
    }

    #[test]
    fn unique_allocation_converges() -> QuantResult<()> {
        let assessment = AccountRecoveryReconciler::assess(input()?)?;

        assert!(assessment.converged());
        assert_eq!(assessment.allocations[0].after_shares, Shares::new(dec!(7)));
        assert_eq!(
            assessment.created_lots[0].remaining_shares,
            Shares::new(dec!(2))
        );
        assert_eq!(
            assessment.allocations[0].after_cost_usd,
            Usd::new(dec!(3.5))
        );
        assert_eq!(
            assessment.allocations[0].realized_pnl_delta_usd,
            Usd::new(dec!(0.5)),
        );
        assert_eq!(
            assessment.created_lots[0].remaining_cost_usd,
            Usd::new(dec!(1.02)),
        );
        Ok(())
    }

    #[test]
    fn ambiguous_sell_requires_allocation() -> QuantResult<()> {
        let mut input = input()?;
        let token = input.open_lots[0].token_id.clone();
        let second_lot_id = StrategyPositionLotId::from_v7();
        input.open_lots.push(AccountRecoveryLotBalance {
            strategy_position_lot_id: second_lot_id,
            token_id: token.clone(),
            shares: Shares::new(dec!(5)),
            cost_usd: Usd::new(dec!(3)),
            opened_at: Utc.timestamp_opt(11, 0).single().unwrap_or_default(),
        });
        input.data_api_positions = vec![balance(&token, dec!(14))];
        input.chain_positions = vec![balance(&token, dec!(14))];
        let execution_id = input.incident_executions[0].account_chain_execution_id;

        let blocked = AccountRecoveryReconciler::assess(input.clone())?;
        assert!(blocked.mismatches.iter().any(|mismatch| matches!(
            mismatch,
            AccountRecoveryMismatch::LotAllocationRequired { .. }
        )));

        input
            .explicit_sell_allocations
            .push(AccountRecoverySellAllocation {
                account_chain_execution_id: execution_id,
                strategy_position_lot_id: second_lot_id,
                shares: Shares::new(dec!(3)),
            });
        let allocated = AccountRecoveryReconciler::assess(input)?;
        assert!(allocated.converged());
        assert_eq!(
            allocated
                .allocations
                .iter()
                .find(|lot| lot.strategy_position_lot_id == second_lot_id)
                .map(|lot| lot.after_shares),
            Some(Shares::new(dec!(2)))
        );
        Ok(())
    }

    #[test]
    fn irrelevant_allocation_is_rejected() -> QuantResult<()> {
        let mut input = input()?;
        input
            .explicit_sell_allocations
            .push(AccountRecoverySellAllocation {
                account_chain_execution_id: input.incident_executions[1].account_chain_execution_id,
                strategy_position_lot_id: input.open_lots[0].strategy_position_lot_id,
                shares: Shares::new(dec!(1)),
            });

        let assessment = AccountRecoveryReconciler::assess(input)?;
        assert!(assessment.mismatches.iter().any(|mismatch| matches!(
            mismatch,
            AccountRecoveryMismatch::LotAllocationInvalid { .. }
        )));
        Ok(())
    }

    #[test]
    fn blockers_are_typed() -> QuantResult<()> {
        let mut input = input()?;
        input.pause_confirmed = false;
        input.open_order_ids = vec![OrderId::new("open-order")];
        input.reserved_usd = Usd::new(dec!(5));
        input.pending_settlement_count = 1;
        let assessment = AccountRecoveryReconciler::assess(input)?;

        assert!(
            assessment
                .mismatches
                .iter()
                .any(|mismatch| matches!(mismatch, AccountRecoveryMismatch::PauseIncomplete))
        );
        assert!(
            assessment.mismatches.iter().any(|mismatch| matches!(
                mismatch,
                AccountRecoveryMismatch::OpenOrdersPresent { .. }
            ))
        );
        assert!(assessment.mismatches.iter().any(|mismatch| matches!(
            mismatch,
            AccountRecoveryMismatch::ReservedCapitalPresent { .. }
        )));
        assert!(
            assessment.mismatches.iter().any(|mismatch| matches!(
                mismatch,
                AccountRecoveryMismatch::PendingSettlement { .. }
            ))
        );
        Ok(())
    }

    #[test]
    fn oversell_fails_closed() -> QuantResult<()> {
        let mut input = input()?;
        input.incident_executions[0].shares_delta = dec!(-30);
        let assessment = AccountRecoveryReconciler::assess(input)?;

        assert!(assessment.mismatches.iter().any(|mismatch| matches!(
            mismatch,
            AccountRecoveryMismatch::IncidentExecutionIncomplete { .. }
        )));
        Ok(())
    }

    #[test]
    fn capture_time_preserves_identity() -> QuantResult<()> {
        let first = input()?;
        let mut replay = first.clone();
        replay.observed_at += Duration::seconds(5);

        assert_eq!(
            AccountRecoveryReconciler::assess(first)?.evidence_hash,
            AccountRecoveryReconciler::assess(replay)?.evidence_hash,
        );
        Ok(())
    }
}

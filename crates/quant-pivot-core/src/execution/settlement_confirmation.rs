//! Exact settlement allocation and atomic-confirmation command construction.

use std::str::FromStr;

use chrono::{DateTime, Utc};
use quant_pivot_api::settlement::confirmation::{
    SettlementConfirmationError, VerifiedSettlementConfirmation,
};
use quant_pivot_error::{QuantError, QuantResult, execution::ExecutionError};
use quant_pivot_models::{
    domain::{
        data_plane::{
            DomainEventEnvelope, DomainEventPayload, DomainEventType, SettlementRedeemConfirmed,
        },
        quant::{
            PositionExit, PositionInfo,
            settlement::{
                ConfirmSettlementRedeem, NewSettlementRedeemLot, RequireSettlementReconciliation,
                SettlementChainSubmissionInfo, SettlementRedeemInfo, SettlementRedeemLotWrite,
            },
            settlement_inventory::SettlementInventoryLotInfo,
        },
    },
    enums::{
        execution::{ExitReason, PositionLedgerState},
        quant::OutcomeSide,
    },
    hashing::CanonicalDigest,
    types::{
        ContentHash, DomainEventId, DomainSourceId, EvmUint256, SettlementRedeemLotId, Shares,
        TokenId, Usd, WorkerId,
    },
};
use rust_decimal::{Decimal, prelude::ToPrimitive};
use serde::Serialize;

const PUSD_SCALE: u64 = 1_000_000;

/// Build the only command permitted to close settlement lots and release
/// capital. Every allocation is exact in pUSD micro-units.
pub fn build_settlement_confirmation(
    redeem: &SettlementRedeemInfo,
    submission: &SettlementChainSubmissionInfo,
    mut positions: Vec<PositionInfo>,
    mut inventory: Vec<SettlementInventoryLotInfo>,
    confirmation: VerifiedSettlementConfirmation,
    confirmed_at: DateTime<Utc>,
    owner: WorkerId,
) -> QuantResult<ConfirmSettlementRedeem> {
    if positions.is_empty() {
        return Err(invariant("settlement has no open strategy lots"));
    }
    positions.sort_by(|left, right| {
        left.order_intent_id
            .as_uuid()
            .as_bytes()
            .cmp(right.order_intent_id.as_uuid().as_bytes())
    });
    inventory.sort_by(|left, right| {
        left.position_id
            .as_uuid()
            .as_bytes()
            .cmp(right.position_id.as_uuid().as_bytes())
    });
    validate_positions(redeem, &positions, &inventory)?;
    let before = redeem
        .balance_before_json
        .as_ref()
        .ok_or_else(|| invariant("settlement has no frozen balance evidence"))?;
    let denominator = decimal_uint(&redeem.payout_vector_json.denominator)?;
    if denominator.is_zero() {
        return Err(invariant("settlement payout denominator is zero"));
    }

    let yes_raw = decimal_uint(&before.yes.raw_balance)?;
    let no_raw = decimal_uint(&before.no.raw_balance)?;
    require_side_balance(&positions, OutcomeSide::Yes, &before.yes.token_id, yes_raw)?;
    require_side_balance(&positions, OutcomeSide::No, &before.no.token_id, no_raw)?;
    let yes_payout_raw = payout_raw(
        yes_raw,
        decimal_uint(&redeem.payout_vector_json.yes)?,
        denominator,
    )?;
    let no_payout_raw = payout_raw(
        no_raw,
        decimal_uint(&redeem.payout_vector_json.no)?,
        denominator,
    )?;
    let actual_payout_raw = usd_to_raw(confirmation.actual_payout_usd)?;
    if yes_payout_raw + no_payout_raw != actual_payout_raw {
        return Err(invariant(
            "aggregate side payout does not equal verified pUSD transfer",
        ));
    }

    let mut lots = allocate_side(
        redeem,
        &positions,
        OutcomeSide::Yes,
        yes_raw,
        yes_payout_raw,
        confirmed_at,
    )?;
    lots.extend(allocate_side(
        redeem,
        &positions,
        OutcomeSide::No,
        no_raw,
        no_payout_raw,
        confirmed_at,
    )?);
    if lots.iter().map(|write| write.lot.payout_usd).sum::<Usd>() != confirmation.actual_payout_usd
    {
        return Err(invariant(
            "per-lot payout does not exactly equal verified pUSD transfer",
        ));
    }
    let outbox_event =
        settlement_outbox_event(redeem, submission, &confirmation, lots.len(), confirmed_at)?;
    Ok(ConfirmSettlementRedeem {
        settlement_redeem_id: redeem.settlement_redeem_id,
        settlement_chain_submission_id: submission.settlement_chain_submission_id,
        owner,
        receipt_evidence_json: confirmation.receipt,
        balance_after_json: confirmation.balances_after,
        actual_payout_usd: confirmation.actual_payout_usd,
        gas_fee_pol: Some(confirmation.gas_fee_pol),
        confirmed_at,
        lots,
        outbox_event,
    })
}

/// Convert a closed verifier failure into a durable reconciliation command.
#[must_use]
pub fn settlement_reconciliation_command(
    redeem: &SettlementRedeemInfo,
    submission: &SettlementChainSubmissionInfo,
    error: &SettlementConfirmationError,
    observed_at: DateTime<Utc>,
    owner: WorkerId,
) -> RequireSettlementReconciliation {
    RequireSettlementReconciliation {
        settlement_redeem_id: redeem.settlement_redeem_id,
        settlement_chain_submission_id: submission.settlement_chain_submission_id,
        owner,
        failure_code: error.failure_code(),
        detail: error.to_string(),
        observed_at,
    }
}

fn validate_positions(
    redeem: &SettlementRedeemInfo,
    positions: &[PositionInfo],
    inventory: &[SettlementInventoryLotInfo],
) -> QuantResult<()> {
    if inventory.is_empty() || positions.len() != inventory.len() {
        return Err(invariant(
            "current positions do not exactly match the frozen settlement inventory",
        ));
    }
    if positions.iter().any(|position| {
        position.market_id != redeem.market_id
            || position.execution_account_id != redeem.execution_account_id
            || !matches!(
                position.state,
                PositionLedgerState::Open | PositionLedgerState::Closing
            )
            || !position.shares.is_positive()
            || match position.side {
                OutcomeSide::Yes => position.token_id != redeem.yes_token_id,
                OutcomeSide::No => position.token_id != redeem.no_token_id,
            }
    }) {
        return Err(invariant(
            "settlement positions contain another market/account/token, terminal state, or non-positive lot",
        ));
    }
    for (position, lot) in positions.iter().zip(inventory) {
        let current_cost_basis = position.cost_usd;
        if lot.settlement_redeem_id != redeem.settlement_redeem_id
            || lot.inventory_digest != redeem.inventory_digest
            || lot.contributor_lots_digest != redeem.contributor_lots_digest
            || lot.execution_account_id != redeem.execution_account_id
            || lot.position_id != position.position_id
            || lot.order_intent_id != position.order_intent_id
            || lot.token_id != position.token_id
            || lot.side != position.side
            || lot.shares != position.shares
            || lot.cost_basis_usd != current_cost_basis
            || lot.position_version_at != position.updated_at
        {
            return Err(invariant(
                "current position lineage differs from the frozen settlement inventory",
            ));
        }
    }
    Ok(())
}

fn require_side_balance(
    positions: &[PositionInfo],
    side: OutcomeSide,
    expected_token: &TokenId,
    expected_raw: Decimal,
) -> QuantResult<()> {
    let side_positions: Vec<&PositionInfo> = positions
        .iter()
        .filter(|position| position.side == side)
        .collect();
    if side_positions
        .iter()
        .any(|position| &position.token_id != expected_token)
    {
        return Err(invariant(
            "settlement lot token does not match frozen side token",
        ));
    }
    let lot_raw = side_positions
        .iter()
        .try_fold(Decimal::ZERO, |sum, position| {
            Ok::<Decimal, QuantError>(sum + shares_to_raw(position.shares)?)
        })?;
    if lot_raw != expected_raw {
        return Err(invariant(
            "wallet frozen balance does not exactly equal open strategy lots",
        ));
    }
    Ok(())
}

fn allocate_side(
    redeem: &SettlementRedeemInfo,
    positions: &[PositionInfo],
    side: OutcomeSide,
    side_balance_raw: Decimal,
    side_payout_raw: Decimal,
    confirmed_at: DateTime<Utc>,
) -> QuantResult<Vec<SettlementRedeemLotWrite>> {
    let side_positions: Vec<&PositionInfo> = positions
        .iter()
        .filter(|position| position.side == side)
        .collect();
    if side_positions.is_empty() {
        if side_balance_raw.is_zero() && side_payout_raw.is_zero() {
            return Ok(Vec::new());
        }
        return Err(invariant("non-zero settlement side has no strategy lot"));
    }
    if side_balance_raw.is_zero() {
        return Err(invariant(
            "strategy lots exist for a zero frozen side balance",
        ));
    }

    let mut allocated_raw = Decimal::ZERO;
    let final_index = side_positions.len() - 1;
    side_positions
        .into_iter()
        .enumerate()
        .map(|(index, position)| {
            let lot_raw = shares_to_raw(position.shares)?;
            let payout_raw = if index == final_index {
                side_payout_raw - allocated_raw
            } else {
                let value = (side_payout_raw * lot_raw / side_balance_raw).floor();
                allocated_raw += value;
                value
            };
            if payout_raw.is_sign_negative() || payout_raw.fract() != Decimal::ZERO {
                return Err(invariant(
                    "per-lot payout is not a non-negative pUSD integer",
                ));
            }
            let payout_usd = raw_to_usd(payout_raw);
            let realized_pnl_usd = payout_usd - position.cost_usd;
            Ok(SettlementRedeemLotWrite {
                lot: NewSettlementRedeemLot {
                    settlement_redeem_lot_id: SettlementRedeemLotId::from_v7(),
                    settlement_redeem_id: redeem.settlement_redeem_id,
                    position_id: position.position_id,
                    order_intent_id: position.order_intent_id,
                    token_id: position.token_id.clone(),
                    side,
                    shares_redeemed: position.shares,
                    cost_basis_usd: position.cost_usd,
                    payout_usd,
                    realized_pnl_usd,
                },
                position_exit: PositionExit {
                    shares: position.shares,
                    avg_price: position.avg_price,
                    proceeds_usd: payout_usd,
                    realized_pnl_usd,
                    exited_at: confirmed_at,
                    reason: ExitReason::ResolutionRedeem,
                },
            })
        })
        .collect()
}

fn settlement_outbox_event(
    redeem: &SettlementRedeemInfo,
    submission: &SettlementChainSubmissionInfo,
    confirmation: &VerifiedSettlementConfirmation,
    lot_count: usize,
    confirmed_at: DateTime<Utc>,
) -> QuantResult<DomainEventEnvelope> {
    let lot_count = u32::try_from(lot_count)
        .map_err(|_| invariant("settlement lot count exceeds outbox representation"))?;
    let payload = DomainEventPayload::SettlementRedeemConfirmed(SettlementRedeemConfirmed {
        settlement_redeem_id: redeem.settlement_redeem_id,
        settlement_chain_submission_id: submission.settlement_chain_submission_id,
        market_id: redeem.market_id.clone(),
        funder_address: redeem.funder_address.clone(),
        wallet_kind: redeem.wallet_kind,
        route: redeem.route,
        target_adapter: submission.target_adapter.clone(),
        transaction_hash: confirmation.receipt.transaction_hash.clone(),
        block_number: confirmation.receipt.block_number,
        block_hash: confirmation.receipt.block_hash.clone(),
        deployment_digest: submission.deployment_digest,
        actual_payout_usd: confirmation.actual_payout_usd,
        lot_count,
    });
    let payload_hash = digest(&payload)?;
    let source = DomainSourceId::polymarket_settlement();
    let event_type = DomainEventType::SettlementRedeemConfirmed;
    let subject = redeem.settlement_redeem_id.to_string();
    let source_checkpoint_hash = digest(&(
        confirmation.receipt.block_number,
        &confirmation.receipt.block_hash,
        &confirmation.receipt.transaction_hash,
    ))?;
    let content_hash = digest(&(
        &source,
        event_type,
        &subject,
        confirmed_at,
        &Option::<DomainEventId>::None,
        &payload_hash,
        &source_checkpoint_hash,
    ))?;
    Ok(DomainEventEnvelope {
        id: DomainEventId::from_content_hash(&content_hash),
        source,
        event_type,
        subject,
        time: confirmed_at,
        schema_version: 1,
        published_at: confirmed_at,
        available_at: confirmed_at,
        revision: 0,
        supersedes_event_id: None,
        payload_hash,
        source_checkpoint_hash,
        payload,
    })
}

fn payout_raw(
    balance_raw: Decimal,
    numerator: Decimal,
    denominator: Decimal,
) -> QuantResult<Decimal> {
    let product = balance_raw
        .checked_mul(numerator)
        .ok_or_else(|| invariant("settlement payout multiplication overflow"))?;
    Ok((product / denominator).floor())
}

fn shares_to_raw(shares: Shares) -> QuantResult<Decimal> {
    let raw = shares.inner() * Decimal::from(PUSD_SCALE);
    if raw.is_sign_negative() || raw.fract() != Decimal::ZERO {
        return Err(invariant(
            "position shares are not exactly representable in outcome-token micro-units",
        ));
    }
    Ok(raw)
}

fn usd_to_raw(usd: Usd) -> QuantResult<Decimal> {
    let raw = usd.inner() * Decimal::from(PUSD_SCALE);
    if raw.is_sign_negative() || raw.fract() != Decimal::ZERO || raw.to_u128().is_none() {
        return Err(invariant(
            "verified payout is not an exact non-negative pUSD micro-unit amount",
        ));
    }
    Ok(raw)
}

fn raw_to_usd(raw: Decimal) -> Usd {
    Usd::new(raw / Decimal::from(PUSD_SCALE))
}

fn decimal_uint(value: &EvmUint256) -> QuantResult<Decimal> {
    Decimal::from_str(value.as_str()).map_err(|error| {
        invariant(&format!(
            "uint256 exceeds decimal accounting range: {error}"
        ))
    })
}

fn digest(value: &impl Serialize) -> QuantResult<ContentHash> {
    CanonicalDigest::content_hash_json(value).map_err(|error| invariant(&error.to_string()))
}

fn invariant(reason: &str) -> QuantError {
    ExecutionError::SettlementRedeemInvariant {
        reason: reason.to_owned(),
    }
    .into()
}

#[cfg(test)]
mod tests {
    use quant_pivot_models::types::Shares;
    use rust_decimal_macros::dec;

    // Full fixtures live in the PostgreSQL system contract; this unit test
    // isolates the micro-unit representability invariant.
    #[test]
    fn lot_shares_must_be_exact_outcome_token_micro_units() {
        let result = super::shares_to_raw(Shares::new(dec!(0.0000001)));
        assert!(result.is_err());
    }
}

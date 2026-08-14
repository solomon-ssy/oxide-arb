//! Leased signer-free settlement capability and inventory preflight.

use std::{str::FromStr, sync::Arc};

use alloy::primitives::U256;
use chrono::{DateTime, Utc};
use quant_pivot_api::{
    settlement::{
        adapter::{
            AlloySettlementAdapterReader, SettlementAdapterError, SettlementBinaryTokenPair,
            SettlementRedeemPreflight,
        },
        contracts::{
            AlloySettlementChainReader, ContractDeploymentVerifier,
            SettlementCredentialAvailability, SettlementDeploymentCatalog,
            VerifiedSettlementDeployment,
        },
    },
    wallet::WalletTopology,
};
use quant_pivot_error::{QuantError, QuantResult, execution::ExecutionError};
use quant_pivot_models::{
    config::SettlementDeployConfig,
    domain::quant::{
        settlement::{PersistSettlementPreflight, SettlementRedeemInfo},
        settlement_inventory::SettlementInventoryLotInfo,
        settlement_readiness::SettlementReadinessReason,
    },
    enums::{
        quant::OutcomeSide,
        settlement::{SettlementFailureCode, SettlementReadinessStatus},
    },
    types::{
        EvmUint256, Shares, Usd, WorkerId,
        settlement_payload::{SettlementPayoutVector, SettlementReadinessEvidence},
    },
};
use quant_pivot_repository::traits::quant::settlement_redeem::SettlementRedeemRepository;
use rust_decimal::Decimal;

use super::{
    SettlementLifecyclePublisher,
    settlement_timing::{deadline, retry_deadline},
};

/// Outcome-token / pUSD micro-unit scale used by inventory ↔ chain raw matching.
const OUTCOME_TOKEN_SCALE: u64 = 1_000_000;

/// One bounded preflight result for metrics and worker control flow.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SettlementPreflightOutcome {
    Idle,
    Ready,
    Blocked,
}

pub struct SettlementPreflightService {
    repository: Arc<dyn SettlementRedeemRepository>,
    verifier: Arc<ContractDeploymentVerifier<AlloySettlementChainReader>>,
    adapter_reader: AlloySettlementAdapterReader,
    catalog: SettlementDeploymentCatalog,
    topology: WalletTopology,
    credentials: SettlementCredentialAvailability,
    config: SettlementDeployConfig,
    worker_id: WorkerId,
    lifecycle: Arc<SettlementLifecyclePublisher>,
}

pub struct SettlementPreflightServiceDeps {
    pub repository: Arc<dyn SettlementRedeemRepository>,
    pub verifier: Arc<ContractDeploymentVerifier<AlloySettlementChainReader>>,
    pub adapter_reader: AlloySettlementAdapterReader,
    pub catalog: SettlementDeploymentCatalog,
    pub topology: WalletTopology,
    pub credentials: SettlementCredentialAvailability,
    pub config: SettlementDeployConfig,
    pub worker_id: WorkerId,
    pub lifecycle: Arc<SettlementLifecyclePublisher>,
}

impl SettlementPreflightService {
    #[must_use]
    pub fn new(deps: SettlementPreflightServiceDeps) -> Self {
        Self {
            repository: deps.repository,
            verifier: deps.verifier,
            adapter_reader: deps.adapter_reader,
            catalog: deps.catalog,
            topology: deps.topology,
            credentials: deps.credentials,
            config: deps.config,
            worker_id: deps.worker_id,
            lifecycle: deps.lifecycle,
        }
    }

    pub async fn run_once(
        &self,
        observed_at: DateTime<Utc>,
    ) -> QuantResult<SettlementPreflightOutcome> {
        let lease_expires_at = deadline(
            observed_at,
            self.config.claim_lease_secs,
            "polymarket.settlement.claim_lease_secs",
        )?;
        let Some(claim) = self
            .repository
            .claim_next_preflight(&self.worker_id, observed_at, lease_expires_at)
            .await?
        else {
            return Ok(SettlementPreflightOutcome::Idle);
        };
        let redeem = claim.redeem;
        let unsettled_execution_orders = self
            .repository
            .count_unsettled_execution_orders(&redeem.market_id, &redeem.execution_account_id)
            .await?;
        if unsettled_execution_orders > 0 {
            self.persist_blocked(
                &redeem,
                vec![SettlementReadinessReason::UnsettledExecutionOrders {
                    count: unsettled_execution_orders,
                }],
                SettlementFailureCode::ExecutionNotQuiescent,
                observed_at,
            )
            .await?;
            return Ok(SettlementPreflightOutcome::Blocked);
        }
        let deployment = match self
            .verifier
            .verify(redeem.route, &self.topology, self.credentials, observed_at)
            .await
        {
            Ok(deployment) => deployment,
            Err(readiness) => {
                self.persist_blocked(
                    &redeem,
                    readiness.reasons,
                    SettlementFailureCode::RouteNotReady,
                    observed_at,
                )
                .await?;
                return Ok(SettlementPreflightOutcome::Blocked);
            }
        };
        let preflight = match self
            .adapter_reader
            .verify_redeem_route(
                &deployment,
                &redeem.market_id,
                &SettlementBinaryTokenPair {
                    yes: redeem.yes_token_id.clone(),
                    no: redeem.no_token_id.clone(),
                },
            )
            .await
        {
            Ok(preflight) => preflight.preflight().clone(),
            Err(source) => {
                let (reason, code) = adapter_reason(&deployment, source);
                self.persist_blocked(&redeem, vec![reason], code, observed_at)
                    .await?;
                return Ok(SettlementPreflightOutcome::Blocked);
            }
        };
        let inventory = self
            .repository
            .list_current_inventory(&redeem.settlement_redeem_id)
            .await?;
        if let Err(reason) = validate_inventory(&redeem, &inventory, &preflight) {
            self.persist_blocked(
                &redeem,
                vec![reason],
                SettlementFailureCode::BalanceMismatch,
                observed_at,
            )
            .await?;
            return Ok(SettlementPreflightOutcome::Blocked);
        }
        let expected_payout = expected_payout(&preflight)?;
        let committed = self
            .repository
            .persist_preflight(PersistSettlementPreflight {
                settlement_redeem_id: redeem.settlement_redeem_id,
                owner: self.worker_id,
                expected_inventory_digest: redeem.inventory_digest,
                readiness_status: SettlementReadinessStatus::Ready,
                readiness_evidence: SettlementReadinessEvidence {
                    reasons: Vec::new(),
                    advisories: deployment.advisories().to_vec(),
                },
                target_adapter: Some(deployment.target().clone()),
                target_code_hash: Some(deployment.target_code_hash().clone()),
                deployment_digest: Some(deployment.deployment_digest()),
                deployment_evidence_version: Some(deployment.evidence_version().clone()),
                verified_block_number: Some(i64::try_from(deployment.verified_block()).map_err(
                    |source| ExecutionError::SettlementRedeemInvariant {
                        reason: format!("verified block exceeds PostgreSQL bigint: {source}"),
                    },
                )?),
                verified_block_hash: Some(deployment.verified_block_hash().clone()),
                payout_vector: preflight.payout_vector,
                balance_before: Some(preflight.balances),
                expected_payout_usd: Some(expected_payout),
                failure_code: None,
                next_attempt_at: None,
                observed_at,
            })
            .await?;
        self.lifecycle.committed(&committed);
        Ok(SettlementPreflightOutcome::Ready)
    }

    async fn persist_blocked(
        &self,
        redeem: &SettlementRedeemInfo,
        reasons: Vec<SettlementReadinessReason>,
        failure_code: SettlementFailureCode,
        observed_at: DateTime<Utc>,
    ) -> QuantResult<()> {
        let retry_at = retry_deadline(
            observed_at,
            redeem.retry_count,
            self.config.retry_initial_secs,
            self.config.retry_max_secs,
            &redeem.settlement_redeem_id.to_string(),
        )?;
        let committed = self
            .repository
            .persist_preflight(PersistSettlementPreflight {
                settlement_redeem_id: redeem.settlement_redeem_id,
                owner: self.worker_id,
                expected_inventory_digest: redeem.inventory_digest,
                readiness_status: SettlementReadinessStatus::Blocked,
                readiness_evidence: SettlementReadinessEvidence {
                    reasons,
                    advisories: self.catalog.advisories(redeem.route),
                },
                target_adapter: None,
                target_code_hash: None,
                deployment_digest: None,
                deployment_evidence_version: None,
                verified_block_number: None,
                verified_block_hash: None,
                payout_vector: SettlementPayoutVector::unresolved(),
                balance_before: None,
                expected_payout_usd: None,
                failure_code: Some(failure_code),
                next_attempt_at: Some(retry_at),
                observed_at,
            })
            .await?;
        self.lifecycle.committed(&committed);
        Ok(())
    }
}

fn validate_inventory(
    redeem: &SettlementRedeemInfo,
    inventory: &[SettlementInventoryLotInfo],
    preflight: &SettlementRedeemPreflight,
) -> Result<(), SettlementReadinessReason> {
    if inventory.is_empty()
        || inventory.iter().any(|lot| {
            lot.settlement_redeem_id != redeem.settlement_redeem_id
                || lot.inventory_digest != redeem.inventory_digest
                || lot.contributor_lots_digest != redeem.contributor_lots_digest
                || lot.execution_account_id != redeem.execution_account_id
        })
    {
        return Err(inventory_mismatch(
            "durable inventory identity does not match the settlement case",
        ));
    }
    let yes_raw = side_raw_balance(inventory, OutcomeSide::Yes)?;
    let no_raw = side_raw_balance(inventory, OutcomeSide::No)?;
    let wallet_yes = uint_reason(&preflight.balances.yes.raw_balance)?;
    let wallet_no = uint_reason(&preflight.balances.no.raw_balance)?;
    if yes_raw != wallet_yes
        || no_raw != wallet_no
        || preflight.balances.yes.token_id != redeem.yes_token_id
        || preflight.balances.no.token_id != redeem.no_token_id
    {
        return Err(inventory_mismatch(
            "wallet full-balance evidence differs from durable account lots",
        ));
    }
    Ok(())
}

fn side_raw_balance(
    inventory: &[SettlementInventoryLotInfo],
    side: OutcomeSide,
) -> Result<U256, SettlementReadinessReason> {
    inventory
        .iter()
        .filter(|lot| lot.side == side)
        .try_fold(U256::ZERO, |sum, lot| {
            let lot_raw = shares_to_raw_u256(lot.shares)?;
            sum.checked_add(lot_raw).ok_or_else(|| {
                inventory_mismatch("durable inventory raw-balance sum overflows uint256")
            })
        })
}

fn shares_to_raw_u256(shares: Shares) -> Result<U256, SettlementReadinessReason> {
    let raw = shares.inner() * Decimal::from(OUTCOME_TOKEN_SCALE);
    if raw.is_sign_negative() || raw.fract() != Decimal::ZERO {
        return Err(inventory_mismatch(
            "durable lot shares are not exactly representable in outcome-token micro-units",
        ));
    }
    U256::from_str(&raw.trunc().to_string()).map_err(|source| {
        inventory_mismatch(&format!(
            "durable lot shares exceed outcome-token raw uint256 range: {source}"
        ))
    })
}

fn expected_payout(preflight: &SettlementRedeemPreflight) -> QuantResult<Usd> {
    let denominator = uint(&preflight.payout_vector.denominator)?;
    if denominator.is_zero() {
        return Err(ExecutionError::SettlementRedeemInvariant {
            reason: "settlement payout denominator is zero after successful preflight".to_owned(),
        }
        .into());
    }
    let yes = uint(&preflight.balances.yes.raw_balance)?
        .checked_mul(uint(&preflight.payout_vector.yes)?)
        .ok_or_else(payout_overflow)?
        / denominator;
    let no = uint(&preflight.balances.no.raw_balance)?
        .checked_mul(uint(&preflight.payout_vector.no)?)
        .ok_or_else(payout_overflow)?
        / denominator;
    let raw = yes.checked_add(no).ok_or_else(payout_overflow)?;
    let decimal = Decimal::from_str_exact(&raw.to_string()).map_err(|source| {
        QuantError::from(ExecutionError::SettlementRedeemInvariant {
            reason: format!("pUSD payout exceeds exact decimal ledger range: {source}"),
        })
    })?;
    Ok(Usd::new(decimal / Decimal::from(OUTCOME_TOKEN_SCALE)))
}

fn uint(value: &EvmUint256) -> QuantResult<U256> {
    U256::from_str(value.as_str()).map_err(|source| {
        ExecutionError::SettlementRedeemInvariant {
            reason: format!("invalid durable uint256 evidence: {source}"),
        }
        .into()
    })
}

fn uint_reason(value: &EvmUint256) -> Result<U256, SettlementReadinessReason> {
    U256::from_str(value.as_str()).map_err(|source| {
        inventory_mismatch(&format!(
            "wallet balance raw uint256 is not canonical: {source}"
        ))
    })
}

fn payout_overflow() -> QuantError {
    ExecutionError::SettlementRedeemInvariant {
        reason: "settlement payout arithmetic overflow".to_owned(),
    }
    .into()
}

fn inventory_mismatch(detail: &str) -> SettlementReadinessReason {
    SettlementReadinessReason::OutcomeInventoryMismatch {
        detail: detail.to_owned(),
    }
}

fn adapter_reason(
    deployment: &VerifiedSettlementDeployment,
    source: SettlementAdapterError,
) -> (SettlementReadinessReason, SettlementFailureCode) {
    match source {
        SettlementAdapterError::MissingOperatorApproval => (
            SettlementReadinessReason::OperatorApprovalMissing {
                funder: deployment.funder().clone(),
                adapter: deployment.target().clone(),
            },
            SettlementFailureCode::RouteNotReady,
        ),
        SettlementAdapterError::ConditionNotResolved => (
            SettlementReadinessReason::ConditionNotResolved,
            SettlementFailureCode::RouteNotReady,
        ),
        SettlementAdapterError::InvalidPayoutVector {
            denominator,
            yes,
            no,
        } => (
            SettlementReadinessReason::InvalidBinaryPayoutVector {
                denominator,
                yes,
                no,
            },
            SettlementFailureCode::RouteNotReady,
        ),
        SettlementAdapterError::EmptyOutcomeBalances => (
            inventory_mismatch("wallet has no redeemable outcome balance"),
            SettlementFailureCode::BalanceMismatch,
        ),
        SettlementAdapterError::AdapterPaused => (
            SettlementReadinessReason::AdapterPaused {
                adapter: deployment.target().clone(),
                asset: deployment.usdce().clone(),
            },
            SettlementFailureCode::RouteNotReady,
        ),
        SettlementAdapterError::AdapterResidualUsdce { raw_balance } => {
            EvmUint256::parse(&raw_balance).map_or_else(
                |_| {
                    (
                        inventory_mismatch("adapter returned a non-canonical residual balance"),
                        SettlementFailureCode::RouteNotReady,
                    )
                },
                |raw_balance| {
                    (
                        SettlementReadinessReason::AdapterResidualUsdce {
                            adapter: deployment.target().clone(),
                            raw_balance,
                        },
                        SettlementFailureCode::RouteNotReady,
                    )
                },
            )
        }
        SettlementAdapterError::SimulationReverted { detail } => (
            SettlementReadinessReason::RedeemSimulationReverted { detail },
            SettlementFailureCode::SimulationReverted,
        ),
        SettlementAdapterError::WrongChain { actual } => (
            SettlementReadinessReason::WrongChain {
                expected: 137,
                actual,
            },
            SettlementFailureCode::RouteNotReady,
        ),
        SettlementAdapterError::CanonicalBlockChanged { block_number } => (
            SettlementReadinessReason::CanonicalBlockChanged {
                block_number,
                observed_hash: deployment.verified_block_hash().clone(),
                current_hash: None,
            },
            SettlementFailureCode::DeploymentChanged,
        ),
        SettlementAdapterError::RpcConnection { detail } => (
            SettlementReadinessReason::RpcUnavailable {
                operation: "settlement_redeem_preflight_connect".to_owned(),
                detail,
            },
            SettlementFailureCode::RouteNotReady,
        ),
        SettlementAdapterError::RpcCall { operation, detail } => (
            SettlementReadinessReason::RpcUnavailable {
                operation: operation.to_owned(),
                detail,
            },
            SettlementFailureCode::RouteNotReady,
        ),
        other => (
            inventory_mismatch(&other.to_string()),
            SettlementFailureCode::RouteNotReady,
        ),
    }
}

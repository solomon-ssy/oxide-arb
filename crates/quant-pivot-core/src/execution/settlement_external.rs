//! Durable current-deployment observation of redemptions initiated outside the runtime.

use std::sync::Arc;

use chrono::{DateTime, Utc};
use quant_pivot_api::{
    settlement::{
        adapter::SettlementAdapterGateway,
        contracts::{
            AlloySettlementChainReader, ContractDeploymentVerifier, VerifiedSettlementDeployment,
        },
        external::ExternalSettlementScanner,
    },
    wallet::WalletTopology,
};
use quant_pivot_error::{QuantError, QuantResult, execution::ExecutionError};
use quant_pivot_models::{
    config::SettlementDeployConfig,
    domain::quant::{
        settlement::{NewSettlementChainSubmission, SettlementRedeemInfo},
        settlement_governance::{
            AdvanceSettlementExternalCursor, NewSettlementExternalCursor,
            PersistExternalSettlementScan,
        },
    },
    enums::settlement::{
        SettlementFailureCode, SettlementReadinessStatus, SettlementRoute,
        SettlementSubmissionKind, SettlementSubmissionPurpose, SettlementSubmissionState,
    },
    types::{
        EvmTransactionHash, ExecutionAccountId, SettlementChainSubmissionId,
        SettlementExternalCursorId,
        settlement_payload::{SettlementFailureEvidence, SettlementFailureHistory},
    },
};
use quant_pivot_repository::traits::quant::{
    settlement_governance::SettlementExternalCursorRepository,
    settlement_redeem::SettlementRedeemRepository,
};

/// One bounded finalized-range observation result.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SettlementExternalPassOutcome {
    NotFinalized,
    Advanced {
        route: SettlementRoute,
        observations: usize,
        through_block: u64,
    },
}

pub struct SettlementExternalObservationServiceDeps {
    pub cases: Arc<dyn SettlementRedeemRepository>,
    pub cursors: Arc<dyn SettlementExternalCursorRepository>,
    pub verifier: Arc<ContractDeploymentVerifier<AlloySettlementChainReader>>,
    pub scanner: ExternalSettlementScanner,
    pub topology: WalletTopology,
    pub execution_account_id: ExecutionAccountId,
    pub config: SettlementDeployConfig,
}

/// Scanner and journal service. Cursor advancement and submission inserts are
/// one `PostgreSQL` transaction, so a crash can only replay a canonical range.
pub struct SettlementExternalObservationService {
    cases: Arc<dyn SettlementRedeemRepository>,
    cursors: Arc<dyn SettlementExternalCursorRepository>,
    verifier: Arc<ContractDeploymentVerifier<AlloySettlementChainReader>>,
    scanner: ExternalSettlementScanner,
    topology: WalletTopology,
    execution_account_id: ExecutionAccountId,
    config: SettlementDeployConfig,
}

impl SettlementExternalObservationService {
    #[must_use]
    pub fn new(deps: SettlementExternalObservationServiceDeps) -> Self {
        Self {
            cases: deps.cases,
            cursors: deps.cursors,
            verifier: deps.verifier,
            scanner: deps.scanner,
            topology: deps.topology,
            execution_account_id: deps.execution_account_id,
            config: deps.config,
        }
    }

    pub async fn run_once(
        &self,
        route: SettlementRoute,
        observed_at: DateTime<Utc>,
    ) -> QuantResult<SettlementExternalPassOutcome> {
        let deployment = self
            .verifier
            .verify_for_observation(route, &self.topology, observed_at)
            .await
            .map_err(|readiness| {
                external_invariant(format!(
                    "current deployment is not verifiable for external observation: {readiness:?}"
                ))
            })?;
        let initial_block = i64_value(deployment.verified_block(), "verified block")?;
        let cursor = self
            .cursors
            .ensure_cursor(NewSettlementExternalCursor {
                settlement_external_cursor_id: SettlementExternalCursorId::from_v7(),
                execution_account_id: self.execution_account_id,
                chain_id: 137,
                route,
                target_adapter: deployment.target().clone(),
                target_code_hash: deployment.target_code_hash().clone(),
                deployment_digest: deployment.deployment_digest(),
                deployment_evidence_version: deployment.evidence_version().clone(),
                next_block_number: initial_block,
                last_observed_block_number: None,
                last_observed_block_hash: None,
            })
            .await?;
        let from_block = u64_value(cursor.next_block_number, "cursor next block")?;
        let requested_to_block = from_block
            .checked_add(self.config.external_scan_block_span.saturating_sub(1))
            .ok_or_else(|| external_invariant("external scan block range overflow"))?;
        let Some(scan) = self
            .scanner
            .scan_finalized(&deployment, from_block, requested_to_block)
            .await
            .map_err(|source| external_invariant(source.to_string()))?
        else {
            return Ok(SettlementExternalPassOutcome::NotFinalized);
        };
        let mut submissions = Vec::with_capacity(scan.observations.len());
        for observation in scan.observations {
            let redeem = self
                .cases
                .find_by_market_account(&observation.market_id, &self.execution_account_id)
                .await?
                .ok_or_else(|| {
                    external_invariant(format!(
                        "external redemption {} has no durable account-scoped case",
                        observation.transaction_hash
                    ))
                })?;
            submissions.push(external_submission(
                &deployment,
                &redeem,
                observation.transaction_hash,
                observed_at,
            )?);
        }
        let observation_count = submissions.len();
        self.cursors
            .persist_scan(PersistExternalSettlementScan {
                cursor: AdvanceSettlementExternalCursor {
                    settlement_external_cursor_id: cursor.settlement_external_cursor_id,
                    expected_next_block_number: cursor.next_block_number,
                    next_block_number: i64_value(
                        scan.to_block
                            .checked_add(1)
                            .ok_or_else(|| external_invariant("external cursor block overflow"))?,
                        "next cursor block",
                    )?,
                    last_observed_block_number: i64_value(scan.to_block, "last observed block")?,
                    last_observed_block_hash: scan.to_block_hash,
                },
                submissions,
                observed_at,
            })
            .await?;
        Ok(SettlementExternalPassOutcome::Advanced {
            route,
            observations: observation_count,
            through_block: scan.to_block,
        })
    }
}

fn external_submission(
    deployment: &VerifiedSettlementDeployment,
    redeem: &SettlementRedeemInfo,
    transaction_hash: EvmTransactionHash,
    observed_at: DateTime<Utc>,
) -> QuantResult<NewSettlementChainSubmission> {
    if redeem.route != deployment.route() {
        return Err(external_invariant(
            "external redemption route differs from the durable case",
        ));
    }
    let exact_pre_redemption_scope = redeem.readiness_status == SettlementReadinessStatus::Ready
        && redeem.target_adapter.as_ref() == Some(deployment.target())
        && redeem.target_code_hash.as_ref() == Some(deployment.target_code_hash())
        && redeem.deployment_digest == Some(deployment.deployment_digest())
        && redeem.deployment_evidence_version.as_ref() == Some(deployment.evidence_version())
        && redeem.balance_before_json.is_some()
        && redeem.expected_payout_usd.is_some();
    let call = SettlementAdapterGateway
        .expected_external_redeem_call(deployment, &redeem.market_id)
        .map_err(|source| external_invariant(source.to_string()))?;
    let attempt_ordinal = redeem
        .attempt_count
        .checked_add(1)
        .ok_or_else(|| external_invariant("external submission attempt ordinal overflow"))?;
    let (state, failure_code, failure_history_json, last_error) = if exact_pre_redemption_scope {
        (
            SettlementSubmissionState::AwaitingFinality,
            None,
            SettlementFailureHistory::default(),
            None,
        )
    } else {
        let detail = "external redemption was mined before exact pre-redemption balance and payout evidence was frozen".to_owned();
        (
            SettlementSubmissionState::Failed,
            Some(SettlementFailureCode::BalanceMismatch),
            SettlementFailureHistory {
                entries: vec![SettlementFailureEvidence {
                    code: SettlementFailureCode::BalanceMismatch,
                    detail: detail.clone(),
                    observed_at,
                }],
            },
            Some(detail),
        )
    };
    Ok(NewSettlementChainSubmission {
        settlement_chain_submission_id: SettlementChainSubmissionId::from_v7(),
        settlement_redeem_id: Some(redeem.settlement_redeem_id),
        settlement_governed_action_id: None,
        canary_action_id: None,
        purpose: SettlementSubmissionPurpose::Redeem,
        kind: SettlementSubmissionKind::ExternallyObserved,
        state,
        route: call.route(),
        target_adapter: call.target_adapter().clone(),
        target_code_hash: call.target_code_hash().clone(),
        conditional_tokens: call.conditional_tokens().clone(),
        collateral_token: call.collateral_token().clone(),
        usdce: call.usdce().clone(),
        call_target: call.target_adapter().clone(),
        deployment_digest: call.deployment_digest(),
        deployment_evidence_version: call.deployment_evidence_version().clone(),
        verified_block_number: i64_value(call.verified_block_number(), "external verified block")?,
        verified_block_hash: call.verified_block_hash().clone(),
        prepared_block_number: None,
        prepared_block_hash: None,
        calldata_hash: call.calldata_hash().clone(),
        calldata: call.calldata().to_vec(),
        signed_envelope: None,
        signed_envelope_hash: None,
        prepared_nonce: None,
        gas_limit: None,
        relayer_transaction_id: None,
        transaction_hash: Some(transaction_hash),
        failure_code,
        failure_history_json,
        receipt_evidence_json: None,
        attempt_ordinal,
        last_error,
        dispatched_at: None,
        chain_hash_observed_at: Some(observed_at),
        confirmed_at: None,
    })
}

fn i64_value(value: u64, field: &'static str) -> QuantResult<i64> {
    i64::try_from(value)
        .map_err(|_| external_invariant(format!("{field} exceeds PostgreSQL BIGINT")))
}

fn u64_value(value: i64, field: &'static str) -> QuantResult<u64> {
    u64::try_from(value).map_err(|_| external_invariant(format!("{field} cannot be negative")))
}

fn external_invariant(reason: impl Into<String>) -> QuantError {
    ExecutionError::SettlementRedeemInvariant {
        reason: reason.into(),
    }
    .into()
}

//! Governed settlement submission orchestration.
//!
//! Recovery of an existing durable identity is deliberately separate from
//! admission of a new money-moving submission. Runtime mode, kill switch and
//! rollout changes may deny the latter but can never strand the former.

use std::sync::Arc;

use async_trait::async_trait;
use chrono::{DateTime, Utc};
use quant_pivot_api::settlement::confirmation::VerifiedSettlementConfirmation;
use quant_pivot_error::{QuantError, QuantResult, execution::ExecutionError};
use quant_pivot_models::{
    config::SettlementDeployConfig,
    domain::quant::{
        settlement::{
            BeginSettlementDispatch, NewSettlementChainSubmission,
            PersistPreparedSettlementSubmission, PersistSettlementPreflight,
            RecordEoaSettlementBroadcast, RecordRelayerSettlementAcceptance,
            RecordRelayerSettlementChainHash, RequireSettlementReconciliation,
            ScheduleSettlementRetry, ScheduleSettlementWork, SettlementAuthorizationScope,
            SettlementChainSubmissionInfo, SettlementRedeemInfo, SettlementWorkClaim,
            StageSettlementAuthorization,
        },
        settlement_readiness::SettlementReadinessReason,
    },
    enums::{
        execution::KillSwitchState,
        quant::QuantRuntimeMode,
        settlement::{
            SettlementAuthorizationState, SettlementEffectivePolicy, SettlementFailureCode,
            SettlementReadinessStatus, SettlementSubmissionState, SettlementWritePolicy,
        },
    },
    types::{
        ContentHash, EvmTransactionHash, RelayerTransactionId, SettlementChainSubmissionId,
        SettlementGovernedActionId, SettlementRedeemId, WorkerId,
        settlement_payload::{SettlementPayoutVector, SettlementReadinessEvidence},
    },
};
use quant_pivot_repository::traits::{
    PositionRepository,
    quant::{
        settlement_governance::SettlementGovernanceRepository,
        settlement_redeem::SettlementRedeemRepository,
    },
};

use crate::{
    execution::{
        settlement_confirmation::build_settlement_confirmation,
        settlement_timing::{deadline, elapsed_ms_since, retry_deadline},
    },
    governance::RuntimeControlsHandle,
    observability::metrics_hub::MetricsHub,
};

/// Closed typed reason that denied construction of a new submission.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SettlementAdmissionBlockReason {
    SettlementWritePolicyDisabled,
    RuntimeModeWritePolicyMismatch,
    ReportOnly,
    KillSwitchHalted,
    ManualOnlyInventory,
    ExecutionNotQuiescent,
    ReadinessNotReady,
    CurrentCapabilityMissing,
    AuthorizationPending,
    AuthorizationExpired,
    AuthorizationInvalid,
    CanaryGrantMissing,
    ConfirmedCanaryMissing,
}

/// Pure new-submission admission result.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SettlementAdmissionDecision {
    Admit,
    StageSemiAutoAuthorization,
    Blocked(SettlementAdmissionBlockReason),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct SettlementAdmissionContext {
    mode: QuantRuntimeMode,
    canary_action_id: Option<SettlementGovernedActionId>,
}

/// One bounded service pass outcome, suitable for worker metrics and tests.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SettlementPassOutcome {
    Idle,
    AuthorizationPending {
        settlement_redeem_id: SettlementRedeemId,
        digest: ContentHash,
        expires_at: DateTime<Utc>,
    },
    NewSubmissionBlocked {
        settlement_redeem_id: SettlementRedeemId,
        reason: SettlementAdmissionBlockReason,
    },
    DispatchAccepted {
        settlement_chain_submission_id: SettlementChainSubmissionId,
    },
    ExistingSubmissionTracked {
        settlement_chain_submission_id: SettlementChainSubmissionId,
    },
    SettlementConfirmed {
        settlement_chain_submission_id: SettlementChainSubmissionId,
    },
    ReconciliationRequired {
        settlement_chain_submission_id: SettlementChainSubmissionId,
        failure_code: SettlementFailureCode,
    },
    RetryScheduled {
        settlement_redeem_id: SettlementRedeemId,
        settlement_chain_submission_id: Option<SettlementChainSubmissionId>,
        failure_code: SettlementFailureCode,
        next_attempt_at: DateTime<Utc>,
    },
}

/// Result of sending exactly the persisted signed envelope.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SettlementDispatchResult {
    /// Transport ended without a trustworthy acknowledgement. The durable
    /// submission remains `Dispatching` for exact-byte replay.
    Ambiguous,
    /// Direct EOA broadcast accepted the already persisted local tx hash.
    EoaAccepted,
    /// Relayer accepted and returned its opaque identity, not an EVM hash.
    RelayerAccepted(RelayerTransactionId),
}

/// Read-only recovery result for one already durable submission identity.
#[derive(Debug)]
pub enum SettlementTrackingResult {
    Pending,
    ChainHashObserved(EvmTransactionHash),
    Confirmed(Box<VerifiedSettlementConfirmation>),
    ReconciliationRequired {
        failure_code: SettlementFailureCode,
        detail: String,
    },
}

/// Closed executor failure class. Transient failures are retryable without
///
/// changing durable identity; terminal failures require operator evidence;
/// invariants indicate corrupt local state and always fail closed.
#[derive(Debug, thiserror::Error)]
pub enum SettlementExecutorError {
    #[error("transient settlement {stage} failure: {detail}")]
    Transient { stage: &'static str, detail: String },
    #[error("terminal settlement failure ({failure_code:?}): {detail}")]
    Terminal {
        failure_code: SettlementFailureCode,
        detail: String,
    },
    #[error("settlement executor invariant failed: {detail}")]
    Invariant { detail: String },
}

/// Money-moving boundary. `prepare` may sign, so the service invokes it only
/// after all new-submission gates pass. `dispatch` receives only a durable row.
#[async_trait]
pub trait SettlementSubmissionExecutor: Send + Sync {
    async fn prepare(
        &self,
        redeem: &SettlementRedeemInfo,
    ) -> Result<NewSettlementChainSubmission, SettlementExecutorError>;

    async fn dispatch(
        &self,
        submission: &SettlementChainSubmissionInfo,
    ) -> Result<SettlementDispatchResult, SettlementExecutorError>;

    async fn track(
        &self,
        redeem: &SettlementRedeemInfo,
        submission: &SettlementChainSubmissionInfo,
    ) -> Result<SettlementTrackingResult, SettlementExecutorError>;
}

pub struct SettlementServiceDeps {
    pub repository: Arc<dyn SettlementRedeemRepository>,
    pub governance: Arc<dyn SettlementGovernanceRepository>,
    pub positions: Arc<dyn PositionRepository>,
    pub executor: Arc<dyn SettlementSubmissionExecutor>,
    pub runtime_controls: RuntimeControlsHandle,
    pub config: SettlementDeployConfig,
    pub worker_id: WorkerId,
    pub metrics: Arc<MetricsHub>,
}

/// Lease-based, restart-safe settlement orchestration service.
pub struct SettlementService {
    repository: Arc<dyn SettlementRedeemRepository>,
    governance: Arc<dyn SettlementGovernanceRepository>,
    positions: Arc<dyn PositionRepository>,
    executor: Arc<dyn SettlementSubmissionExecutor>,
    runtime_controls: RuntimeControlsHandle,
    config: SettlementDeployConfig,
    worker_id: WorkerId,
    metrics: Arc<MetricsHub>,
}

impl SettlementService {
    #[must_use]
    pub fn new(deps: SettlementServiceDeps) -> Self {
        Self {
            repository: deps.repository,
            governance: deps.governance,
            positions: deps.positions,
            executor: deps.executor,
            runtime_controls: deps.runtime_controls,
            config: deps.config,
            worker_id: deps.worker_id,
            metrics: deps.metrics,
        }
    }

    /// Process at most one case. Existing durable submissions are always
    /// claimed first and are never evaluated by the new-submission gates.
    pub async fn run_once(&self, now: DateTime<Utc>) -> QuantResult<SettlementPassOutcome> {
        let lease_expires_at = deadline(
            now,
            self.config.claim_lease_secs,
            "polymarket.settlement.claim_lease_secs",
        )?;
        if let Some(claim) = self
            .repository
            .claim_next_recovery(&self.worker_id, now, lease_expires_at)
            .await?
        {
            return self.recover_existing(claim, now).await;
        }
        let Some(claim) = self
            .repository
            .claim_next_new_submission(&self.worker_id, now, lease_expires_at)
            .await?
        else {
            return Ok(SettlementPassOutcome::Idle);
        };

        let (context, decision) = match self.admission_decision(&claim.redeem, now).await {
            Ok(decision) => decision,
            Err(error) => {
                self.release(&claim.redeem.settlement_redeem_id).await?;
                return Err(error);
            }
        };
        match decision {
            SettlementAdmissionDecision::Admit => self.prepare_and_dispatch(claim, context).await,
            SettlementAdmissionDecision::StageSemiAutoAuthorization => {
                self.stage_authorization(claim, now).await
            }
            SettlementAdmissionDecision::Blocked(reason) => {
                self.defer(
                    &claim.redeem.settlement_redeem_id,
                    None,
                    now,
                    self.config.submission_poll_secs,
                )
                .await?;
                Ok(SettlementPassOutcome::NewSubmissionBlocked {
                    settlement_redeem_id: claim.redeem.settlement_redeem_id,
                    reason,
                })
            }
        }
    }

    async fn stage_authorization(
        &self,
        claim: SettlementWorkClaim,
        now: DateTime<Utc>,
    ) -> QuantResult<SettlementPassOutcome> {
        let expires_at = deadline(
            now,
            self.config.semi_auto_authorization_ttl_secs,
            "polymarket.settlement.semi_auto_authorization_ttl_secs",
        )?;
        let scope = authorization_scope(&claim.redeem, expires_at)?;
        let digest = scope.digest()?;
        self.repository
            .stage_authorization(StageSettlementAuthorization {
                settlement_redeem_id: claim.redeem.settlement_redeem_id,
                owner: self.worker_id,
                digest,
                expires_at,
                expected_target_adapter: scope.target_adapter,
                expected_deployment_digest: scope.deployment_digest,
                staged_at: now,
            })
            .await?;
        self.release(&claim.redeem.settlement_redeem_id).await?;
        Ok(SettlementPassOutcome::AuthorizationPending {
            settlement_redeem_id: claim.redeem.settlement_redeem_id,
            digest,
            expires_at,
        })
    }

    async fn prepare_and_dispatch(
        &self,
        claim: SettlementWorkClaim,
        admitted: SettlementAdmissionContext,
    ) -> QuantResult<SettlementPassOutcome> {
        if let Some(outcome) = self
            .deny_if_admission_changed(&claim.redeem, admitted, Utc::now())
            .await?
        {
            return Ok(outcome);
        }
        let prepare = self.executor.prepare(&claim.redeem).await;
        let mut prepared = match prepare {
            Ok(prepared) => prepared,
            Err(error) => {
                return self
                    .handle_executor_failure(&claim.redeem, None, error, Utc::now())
                    .await;
            }
        };
        let persisted_at = Utc::now();
        let lease_expires_at = deadline(
            persisted_at,
            self.config.claim_lease_secs,
            "polymarket.settlement.claim_lease_secs",
        )?;
        if !self
            .repository
            .renew_claim(
                &claim.redeem.settlement_redeem_id,
                &self.worker_id,
                persisted_at,
                lease_expires_at,
            )
            .await?
        {
            self.metrics.record_settlement_lease_lost("redeem");
            return Err(ExecutionError::SettlementRedeemInvariant {
                reason: "settlement case lease expired while preparing signed envelope".to_owned(),
            }
            .into());
        }
        if let Some(outcome) = self
            .deny_if_admission_changed(&claim.redeem, admitted, persisted_at)
            .await?
        {
            return Ok(outcome);
        }
        prepared.canary_action_id = admitted.canary_action_id;
        let expected_authorization_digest = (admitted.mode == QuantRuntimeMode::SemiAuto)
            .then_some(claim.redeem.authorization_digest)
            .flatten();
        let durable = self
            .repository
            .persist_prepared_submission(PersistPreparedSettlementSubmission {
                owner: self.worker_id,
                expected_authorization_digest,
                expected_canary_action_id: admitted.canary_action_id,
                submission: prepared,
                persisted_at,
            })
            .await?;
        let dispatching = self.begin_dispatch(&durable, persisted_at).await?;
        self.dispatch_durable(&claim.redeem, &dispatching).await
    }

    async fn admission_decision(
        &self,
        redeem: &SettlementRedeemInfo,
        now: DateTime<Utc>,
    ) -> QuantResult<(SettlementAdmissionContext, SettlementAdmissionDecision)> {
        let controls = self.runtime_controls.snapshot();
        let mode = controls.quant_runtime_mode;
        let mut context = SettlementAdmissionContext {
            mode,
            canary_action_id: None,
        };
        let decision = evaluate_new_submission_admission(
            redeem,
            mode,
            controls.settlement_write_policy,
            controls.kill_switch_state,
            now,
        );
        if decision != SettlementAdmissionDecision::Admit {
            return Ok((context, decision));
        }
        if self
            .repository
            .count_unsettled_execution_orders(&redeem.market_id, &redeem.execution_account_id)
            .await?
            > 0
        {
            return Ok((
                context,
                SettlementAdmissionDecision::Blocked(
                    SettlementAdmissionBlockReason::ExecutionNotQuiescent,
                ),
            ));
        }
        match controls.settlement_write_policy {
            SettlementWritePolicy::GovernedCanary => {
                let authorization_digest = redeem.authorization_digest.ok_or_else(|| {
                    invariant("governed canary admission has no authorization digest")
                })?;
                let target_adapter = redeem
                    .target_adapter
                    .as_ref()
                    .ok_or_else(|| invariant("governed canary admission has no target adapter"))?;
                let deployment_digest = redeem.deployment_digest.ok_or_else(|| {
                    invariant("governed canary admission has no deployment digest")
                })?;
                let canary = self
                    .governance
                    .find_authorized_canary(
                        &redeem.settlement_redeem_id,
                        authorization_digest,
                        redeem.route,
                        target_adapter,
                        deployment_digest,
                        now,
                    )
                    .await?;
                let Some(canary) = canary.filter(|canary| {
                    canary.execution_account_id == redeem.execution_account_id
                        && canary
                            .payout_ceiling_usd
                            .zip(redeem.expected_payout_usd)
                            .is_some_and(|(ceiling, expected)| ceiling >= expected)
                }) else {
                    return Ok((
                        context,
                        SettlementAdmissionDecision::Blocked(
                            SettlementAdmissionBlockReason::CanaryGrantMissing,
                        ),
                    ));
                };
                context.canary_action_id = Some(canary.settlement_governed_action_id);
            }
            SettlementWritePolicy::Auto => {
                let deployment_digest = redeem.deployment_digest.ok_or_else(|| {
                    invariant("auto settlement admission has no deployment digest")
                })?;
                if !self
                    .governance
                    .has_confirmed_canary(
                        &redeem.execution_account_id,
                        redeem.route,
                        deployment_digest,
                    )
                    .await?
                {
                    return Ok((
                        context,
                        SettlementAdmissionDecision::Blocked(
                            SettlementAdmissionBlockReason::ConfirmedCanaryMissing,
                        ),
                    ));
                }
            }
            SettlementWritePolicy::Disabled | SettlementWritePolicy::SemiAuto => {}
        }
        Ok((context, decision))
    }

    async fn deny_if_admission_changed(
        &self,
        redeem: &SettlementRedeemInfo,
        admitted: SettlementAdmissionContext,
        now: DateTime<Utc>,
    ) -> QuantResult<Option<SettlementPassOutcome>> {
        let (current, decision) = self.admission_decision(redeem, now).await?;
        if current == admitted && decision == SettlementAdmissionDecision::Admit {
            return Ok(None);
        }
        let reason = match decision {
            SettlementAdmissionDecision::Blocked(reason) => reason,
            SettlementAdmissionDecision::StageSemiAutoAuthorization => {
                SettlementAdmissionBlockReason::AuthorizationPending
            }
            SettlementAdmissionDecision::Admit => {
                SettlementAdmissionBlockReason::AuthorizationInvalid
            }
        };
        self.defer(
            &redeem.settlement_redeem_id,
            None,
            now,
            self.config.submission_poll_secs,
        )
        .await?;
        Ok(Some(SettlementPassOutcome::NewSubmissionBlocked {
            settlement_redeem_id: redeem.settlement_redeem_id,
            reason,
        }))
    }

    async fn recover_existing(
        &self,
        claim: SettlementWorkClaim,
        now: DateTime<Utc>,
    ) -> QuantResult<SettlementPassOutcome> {
        let submission =
            claim
                .active_submission
                .ok_or_else(|| ExecutionError::SettlementRedeemInvariant {
                    reason: "recovery claim has no active durable submission".to_owned(),
                })?;
        self.metrics
            .observe_submission_age_ms("redeem", elapsed_ms_since(now, submission.created_at));
        if submission.state == SettlementSubmissionState::AwaitingFinality {
            self.metrics.observe_finality_lag_ms(
                "redeem",
                elapsed_ms_since(
                    now,
                    submission.dispatched_at.unwrap_or(submission.created_at),
                ),
            );
        }
        match submission.state {
            SettlementSubmissionState::Prepared => {
                let dispatching = self.begin_dispatch(&submission, now).await?;
                self.dispatch_durable(&claim.redeem, &dispatching).await
            }
            SettlementSubmissionState::Dispatching => {
                self.dispatch_durable(&claim.redeem, &submission).await
            }
            SettlementSubmissionState::AwaitingChainHash
            | SettlementSubmissionState::AwaitingFinality => {
                self.track_existing(claim.redeem, submission, now).await
            }
            SettlementSubmissionState::Confirmed | SettlementSubmissionState::Failed => {
                Err(ExecutionError::SettlementRedeemInvariant {
                    reason: "recovery claim carried a terminal submission".to_owned(),
                }
                .into())
            }
        }
    }

    async fn track_existing(
        &self,
        redeem: SettlementRedeemInfo,
        submission: SettlementChainSubmissionInfo,
        observed_at: DateTime<Utc>,
    ) -> QuantResult<SettlementPassOutcome> {
        let tracking = match self.executor.track(&redeem, &submission).await {
            Ok(tracking) => tracking,
            Err(error) => {
                return self
                    .handle_executor_failure(&redeem, Some(&submission), error, observed_at)
                    .await;
            }
        };
        match tracking {
            SettlementTrackingResult::Pending => {
                self.defer(
                    &redeem.settlement_redeem_id,
                    Some(submission.settlement_chain_submission_id),
                    observed_at,
                    self.config.submission_poll_secs,
                )
                .await?;
                Ok(SettlementPassOutcome::ExistingSubmissionTracked {
                    settlement_chain_submission_id: submission.settlement_chain_submission_id,
                })
            }
            SettlementTrackingResult::ChainHashObserved(transaction_hash) => {
                if submission.state != SettlementSubmissionState::AwaitingChainHash {
                    self.release(&redeem.settlement_redeem_id).await?;
                    return Err(invariant("chain hash observed outside AwaitingChainHash"));
                }
                let relayer_transaction_id =
                    submission.relayer_transaction_id.clone().ok_or_else(|| {
                        invariant("AwaitingChainHash submission has no opaque relayer ID")
                    })?;
                self.repository
                    .record_relayer_chain_hash(RecordRelayerSettlementChainHash {
                        settlement_redeem_id: redeem.settlement_redeem_id,
                        settlement_chain_submission_id: submission.settlement_chain_submission_id,
                        owner: self.worker_id,
                        expected_relayer_transaction_id: relayer_transaction_id,
                        transaction_hash,
                        observed_at,
                    })
                    .await?;
                self.defer(
                    &redeem.settlement_redeem_id,
                    Some(submission.settlement_chain_submission_id),
                    observed_at,
                    self.config.submission_poll_secs,
                )
                .await?;
                Ok(SettlementPassOutcome::ExistingSubmissionTracked {
                    settlement_chain_submission_id: submission.settlement_chain_submission_id,
                })
            }
            SettlementTrackingResult::Confirmed(confirmation) => {
                if submission.state != SettlementSubmissionState::AwaitingFinality {
                    self.release(&redeem.settlement_redeem_id).await?;
                    return Err(invariant("confirmation observed outside AwaitingFinality"));
                }
                let positions = match self
                    .positions
                    .find_open_position(&redeem.market_id, &redeem.execution_account_id)
                    .await
                {
                    Ok(positions) => positions,
                    Err(source) => {
                        return self
                            .schedule_retry(
                                &redeem,
                                Some(&submission),
                                SettlementFailureCode::LedgerUnavailable,
                                source.to_string(),
                                observed_at,
                            )
                            .await;
                    }
                };
                let inventory = match self
                    .repository
                    .list_current_inventory(&redeem.settlement_redeem_id)
                    .await
                {
                    Ok(inventory) => inventory,
                    Err(source) => {
                        return self
                            .schedule_retry(
                                &redeem,
                                Some(&submission),
                                SettlementFailureCode::LedgerUnavailable,
                                source.to_string(),
                                observed_at,
                            )
                            .await;
                    }
                };
                let write = match build_settlement_confirmation(
                    &redeem,
                    &submission,
                    positions,
                    inventory,
                    *confirmation,
                    observed_at,
                    self.worker_id,
                ) {
                    Ok(write) => write,
                    Err(source) => {
                        return self
                            .require_reconciliation(
                                &redeem,
                                &submission,
                                SettlementFailureCode::ReceiptEvidenceMismatch,
                                source.to_string(),
                                observed_at,
                            )
                            .await;
                    }
                };
                if let Err(source) = self.repository.confirm(write).await {
                    return self
                        .schedule_retry(
                            &redeem,
                            Some(&submission),
                            SettlementFailureCode::LedgerUnavailable,
                            source.to_string(),
                            observed_at,
                        )
                        .await;
                }
                Ok(SettlementPassOutcome::SettlementConfirmed {
                    settlement_chain_submission_id: submission.settlement_chain_submission_id,
                })
            }
            SettlementTrackingResult::ReconciliationRequired {
                failure_code,
                detail,
            } => {
                self.require_reconciliation(&redeem, &submission, failure_code, detail, observed_at)
                    .await
            }
        }
    }

    async fn begin_dispatch(
        &self,
        submission: &SettlementChainSubmissionInfo,
        now: DateTime<Utc>,
    ) -> QuantResult<SettlementChainSubmissionInfo> {
        let settlement_redeem_id = submission.settlement_redeem_id.ok_or_else(|| {
            ExecutionError::SettlementRedeemInvariant {
                reason: "redeem service received a governed-action submission".to_owned(),
            }
        })?;
        let envelope_hash = submission.signed_envelope_hash.ok_or_else(|| {
            ExecutionError::SettlementRedeemInvariant {
                reason: "durable submission has no signed envelope hash".to_owned(),
            }
        })?;
        self.repository
            .begin_dispatch(BeginSettlementDispatch {
                settlement_redeem_id,
                settlement_chain_submission_id: submission.settlement_chain_submission_id,
                owner: self.worker_id,
                expected_target_adapter: submission.target_adapter.clone(),
                expected_deployment_digest: submission.deployment_digest,
                expected_calldata_hash: submission.calldata_hash.clone(),
                expected_signed_envelope_hash: envelope_hash,
                dispatching_at: now,
            })
            .await
            .map_err(Into::into)
    }

    async fn dispatch_durable(
        &self,
        redeem: &SettlementRedeemInfo,
        submission: &SettlementChainSubmissionInfo,
    ) -> QuantResult<SettlementPassOutcome> {
        let settlement_redeem_id = submission.settlement_redeem_id.ok_or_else(|| {
            ExecutionError::SettlementRedeemInvariant {
                reason: "redeem service received a governed-action submission".to_owned(),
            }
        })?;
        let dispatch = self.executor.dispatch(submission).await;
        let result = match dispatch {
            Ok(result) => result,
            Err(error) => {
                return self
                    .handle_executor_failure(redeem, Some(submission), error, Utc::now())
                    .await;
            }
        };
        let envelope_hash = submission.signed_envelope_hash.ok_or_else(|| {
            ExecutionError::SettlementRedeemInvariant {
                reason: "dispatching submission has no signed envelope hash".to_owned(),
            }
        })?;
        let observed_at = Utc::now();
        let outcome = match result {
            SettlementDispatchResult::Ambiguous => {
                return self
                    .schedule_retry(
                        redeem,
                        Some(submission),
                        SettlementFailureCode::TransportUncertain,
                        "transport returned no trustworthy submission acknowledgement".to_owned(),
                        observed_at,
                    )
                    .await;
            }
            SettlementDispatchResult::EoaAccepted => {
                self.repository
                    .record_eoa_broadcast(RecordEoaSettlementBroadcast {
                        settlement_redeem_id,
                        settlement_chain_submission_id: submission.settlement_chain_submission_id,
                        owner: self.worker_id,
                        expected_signed_envelope_hash: envelope_hash,
                        observed_at,
                    })
                    .await?;
                SettlementPassOutcome::DispatchAccepted {
                    settlement_chain_submission_id: submission.settlement_chain_submission_id,
                }
            }
            SettlementDispatchResult::RelayerAccepted(relayer_transaction_id) => {
                self.repository
                    .record_relayer_acceptance(RecordRelayerSettlementAcceptance {
                        settlement_redeem_id,
                        settlement_chain_submission_id: submission.settlement_chain_submission_id,
                        owner: self.worker_id,
                        expected_signed_envelope_hash: envelope_hash,
                        relayer_transaction_id,
                        observed_at,
                    })
                    .await?;
                SettlementPassOutcome::DispatchAccepted {
                    settlement_chain_submission_id: submission.settlement_chain_submission_id,
                }
            }
        };
        self.defer(
            &settlement_redeem_id,
            Some(submission.settlement_chain_submission_id),
            observed_at,
            self.config.submission_poll_secs,
        )
        .await?;
        Ok(outcome)
    }

    async fn handle_executor_failure(
        &self,
        redeem: &SettlementRedeemInfo,
        submission: Option<&SettlementChainSubmissionInfo>,
        error: SettlementExecutorError,
        observed_at: DateTime<Utc>,
    ) -> QuantResult<SettlementPassOutcome> {
        match error {
            SettlementExecutorError::Transient { detail, .. } => {
                self.schedule_retry(
                    redeem,
                    submission,
                    SettlementFailureCode::TransportUncertain,
                    detail,
                    observed_at,
                )
                .await
            }
            SettlementExecutorError::Terminal {
                failure_code,
                detail,
            } => match submission {
                Some(submission) => {
                    self.require_reconciliation(
                        redeem,
                        submission,
                        failure_code,
                        detail,
                        observed_at,
                    )
                    .await
                }
                None => {
                    self.block_submission_preflight(redeem, failure_code, detail, observed_at)
                        .await
                }
            },
            SettlementExecutorError::Invariant { detail } => match submission {
                Some(submission) => {
                    self.require_reconciliation(
                        redeem,
                        submission,
                        SettlementFailureCode::LocalInvariant,
                        detail,
                        observed_at,
                    )
                    .await
                }
                None => {
                    self.block_submission_preflight(
                        redeem,
                        SettlementFailureCode::LocalInvariant,
                        detail,
                        observed_at,
                    )
                    .await
                }
            },
        }
    }

    async fn block_submission_preflight(
        &self,
        redeem: &SettlementRedeemInfo,
        failure_code: SettlementFailureCode,
        detail: String,
        observed_at: DateTime<Utc>,
    ) -> QuantResult<SettlementPassOutcome> {
        let detail = bounded_detail(&detail);
        let next_attempt_at = retry_deadline(
            observed_at,
            redeem.retry_count,
            self.config.retry_initial_secs,
            self.config.retry_max_secs,
            &redeem.settlement_redeem_id.to_string(),
        )?;
        self.repository
            .persist_preflight(PersistSettlementPreflight {
                settlement_redeem_id: redeem.settlement_redeem_id,
                owner: self.worker_id,
                expected_inventory_digest: redeem.inventory_digest,
                readiness_status: SettlementReadinessStatus::Blocked,
                readiness_evidence: SettlementReadinessEvidence {
                    reasons: vec![SettlementReadinessReason::SubmissionPreflightFailed {
                        failure_code,
                        detail,
                    }],
                    advisories: redeem.readiness_evidence_json.advisories.clone(),
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
                next_attempt_at: Some(next_attempt_at),
                observed_at,
            })
            .await?;
        Ok(SettlementPassOutcome::RetryScheduled {
            settlement_redeem_id: redeem.settlement_redeem_id,
            settlement_chain_submission_id: None,
            failure_code,
            next_attempt_at,
        })
    }

    async fn schedule_retry(
        &self,
        redeem: &SettlementRedeemInfo,
        submission: Option<&SettlementChainSubmissionInfo>,
        failure_code: SettlementFailureCode,
        detail: String,
        observed_at: DateTime<Utc>,
    ) -> QuantResult<SettlementPassOutcome> {
        let next_attempt_at = retry_deadline(
            observed_at,
            redeem.retry_count,
            self.config.retry_initial_secs,
            self.config.retry_max_secs,
            &redeem.settlement_redeem_id.to_string(),
        )?;
        let settlement_chain_submission_id =
            submission.map(|value| value.settlement_chain_submission_id);
        self.repository
            .schedule_retry(ScheduleSettlementRetry {
                settlement_redeem_id: redeem.settlement_redeem_id,
                settlement_chain_submission_id,
                owner: self.worker_id,
                failure_code,
                detail: bounded_detail(&detail),
                next_attempt_at,
                observed_at,
            })
            .await?;
        Ok(SettlementPassOutcome::RetryScheduled {
            settlement_redeem_id: redeem.settlement_redeem_id,
            settlement_chain_submission_id,
            failure_code,
            next_attempt_at,
        })
    }

    async fn require_reconciliation(
        &self,
        redeem: &SettlementRedeemInfo,
        submission: &SettlementChainSubmissionInfo,
        failure_code: SettlementFailureCode,
        detail: String,
        observed_at: DateTime<Utc>,
    ) -> QuantResult<SettlementPassOutcome> {
        self.repository
            .require_reconciliation(RequireSettlementReconciliation {
                settlement_redeem_id: redeem.settlement_redeem_id,
                settlement_chain_submission_id: submission.settlement_chain_submission_id,
                owner: self.worker_id,
                failure_code,
                detail: bounded_detail(&detail),
                observed_at,
            })
            .await?;
        self.metrics
            .record_settlement_reconciliation_required("redeem", failure_code.as_str());
        Ok(SettlementPassOutcome::ReconciliationRequired {
            settlement_chain_submission_id: submission.settlement_chain_submission_id,
            failure_code,
        })
    }

    async fn defer(
        &self,
        redeem_id: &SettlementRedeemId,
        submission_id: Option<SettlementChainSubmissionId>,
        observed_at: DateTime<Utc>,
        seconds: u64,
    ) -> QuantResult<()> {
        let next_attempt_at = deadline(observed_at, seconds, "polymarket.settlement.next_work_at")?;
        self.repository
            .schedule_work(ScheduleSettlementWork {
                settlement_redeem_id: *redeem_id,
                settlement_chain_submission_id: submission_id,
                owner: self.worker_id,
                next_attempt_at,
                observed_at,
            })
            .await?;
        Ok(())
    }

    async fn release(&self, redeem_id: &SettlementRedeemId) -> QuantResult<()> {
        if !self
            .repository
            .release_claim(redeem_id, &self.worker_id)
            .await?
        {
            return Err(ExecutionError::SettlementRedeemInvariant {
                reason: format!("settlement case {redeem_id} lease was lost before release"),
            }
            .into());
        }
        Ok(())
    }
}

fn bounded_detail(detail: &str) -> String {
    let bounded = detail.trim().chars().take(2_048).collect::<String>();
    if bounded.is_empty() {
        "settlement failure without diagnostic detail".to_owned()
    } else {
        bounded
    }
}

/// Pure gate. Non-blocking deployment advisories are intentionally absent:
/// readiness is blocked only by its typed status/reasons.
#[must_use]
pub fn evaluate_new_submission_admission(
    redeem: &SettlementRedeemInfo,
    mode: QuantRuntimeMode,
    write_policy: SettlementWritePolicy,
    kill_switch: KillSwitchState,
    now: DateTime<Utc>,
) -> SettlementAdmissionDecision {
    if write_policy == SettlementWritePolicy::Disabled {
        return SettlementAdmissionDecision::Blocked(
            SettlementAdmissionBlockReason::SettlementWritePolicyDisabled,
        );
    }
    if mode == QuantRuntimeMode::ReportOnly {
        return SettlementAdmissionDecision::Blocked(SettlementAdmissionBlockReason::ReportOnly);
    }
    if !kill_switch.allows_settlement_recovery_submission() {
        return SettlementAdmissionDecision::Blocked(
            SettlementAdmissionBlockReason::KillSwitchHalted,
        );
    }
    if redeem.effective_policy != SettlementEffectivePolicy::AutomaticEligible {
        return SettlementAdmissionDecision::Blocked(
            SettlementAdmissionBlockReason::ManualOnlyInventory,
        );
    }
    if redeem.readiness_status != SettlementReadinessStatus::Ready {
        return SettlementAdmissionDecision::Blocked(
            SettlementAdmissionBlockReason::ReadinessNotReady,
        );
    }
    if redeem.target_adapter.is_none()
        || redeem.target_code_hash.is_none()
        || redeem.deployment_digest.is_none()
        || redeem.deployment_evidence_version.is_none()
        || redeem.verified_block_number.is_none()
        || redeem.verified_block_hash.is_none()
    {
        return SettlementAdmissionDecision::Blocked(
            SettlementAdmissionBlockReason::CurrentCapabilityMissing,
        );
    }
    match write_policy {
        SettlementWritePolicy::Disabled => SettlementAdmissionDecision::Blocked(
            SettlementAdmissionBlockReason::SettlementWritePolicyDisabled,
        ),
        SettlementWritePolicy::GovernedCanary | SettlementWritePolicy::SemiAuto
            if mode != QuantRuntimeMode::SemiAuto =>
        {
            SettlementAdmissionDecision::Blocked(
                SettlementAdmissionBlockReason::RuntimeModeWritePolicyMismatch,
            )
        }
        SettlementWritePolicy::GovernedCanary | SettlementWritePolicy::SemiAuto => {
            match redeem.authorization_state {
                SettlementAuthorizationState::NotRequired
                | SettlementAuthorizationState::Expired
                | SettlementAuthorizationState::Revoked => {
                    SettlementAdmissionDecision::StageSemiAutoAuthorization
                }
                SettlementAuthorizationState::Pending => SettlementAdmissionDecision::Blocked(
                    SettlementAdmissionBlockReason::AuthorizationPending,
                ),
                SettlementAuthorizationState::Approved
                    if redeem
                        .authorization_expires_at
                        .is_some_and(|expires_at| expires_at > now)
                        && redeem.authorization_digest.is_some() =>
                {
                    SettlementAdmissionDecision::Admit
                }
                SettlementAuthorizationState::Approved => SettlementAdmissionDecision::Blocked(
                    SettlementAdmissionBlockReason::AuthorizationExpired,
                ),
                SettlementAuthorizationState::Consumed => SettlementAdmissionDecision::Blocked(
                    SettlementAdmissionBlockReason::AuthorizationInvalid,
                ),
            }
        }
        SettlementWritePolicy::Auto if mode != QuantRuntimeMode::AutoExecution => {
            SettlementAdmissionDecision::Blocked(
                SettlementAdmissionBlockReason::RuntimeModeWritePolicyMismatch,
            )
        }
        SettlementWritePolicy::Auto => {
            if matches!(
                redeem.authorization_state,
                SettlementAuthorizationState::NotRequired
                    | SettlementAuthorizationState::Expired
                    | SettlementAuthorizationState::Revoked
            ) {
                SettlementAdmissionDecision::Admit
            } else {
                SettlementAdmissionDecision::Blocked(
                    SettlementAdmissionBlockReason::AuthorizationInvalid,
                )
            }
        }
    }
}

fn authorization_scope(
    redeem: &SettlementRedeemInfo,
    expires_at: DateTime<Utc>,
) -> QuantResult<SettlementAuthorizationScope> {
    let missing = |field: &'static str| ExecutionError::SettlementRedeemInvariant {
        reason: format!("cannot build authorization scope without {field}"),
    };
    Ok(SettlementAuthorizationScope {
        settlement_redeem_id: redeem.settlement_redeem_id,
        market_id: redeem.market_id.clone(),
        funder_address: redeem.funder_address.clone(),
        wallet_kind: redeem.wallet_kind,
        route: redeem.route,
        target_adapter: redeem
            .target_adapter
            .clone()
            .ok_or_else(|| missing("target_adapter"))?,
        target_code_hash: redeem
            .target_code_hash
            .clone()
            .ok_or_else(|| missing("target_code_hash"))?,
        deployment_digest: redeem
            .deployment_digest
            .ok_or_else(|| missing("deployment_digest"))?,
        deployment_evidence_version: redeem
            .deployment_evidence_version
            .clone()
            .ok_or_else(|| missing("deployment_evidence_version"))?,
        payout_vector: redeem.payout_vector_json.clone(),
        balance_before: redeem
            .balance_before_json
            .clone()
            .ok_or_else(|| missing("balance_before_json"))?,
        expected_payout_usd: redeem
            .expected_payout_usd
            .ok_or_else(|| missing("expected_payout_usd"))?,
        attempt_ordinal: redeem.attempt_count.checked_add(1).ok_or_else(|| {
            ExecutionError::SettlementRedeemInvariant {
                reason: "settlement attempt ordinal overflow".to_owned(),
            }
        })?,
        expires_at,
    })
}

fn invariant(reason: &str) -> QuantError {
    ExecutionError::SettlementRedeemInvariant {
        reason: reason.to_owned(),
    }
    .into()
}

#[cfg(test)]
mod tests {
    use chrono::{DateTime, TimeZone, Utc};
    use quant_pivot_models::{
        domain::quant::{
            settlement::SettlementRedeemInfo,
            settlement_readiness::{SettlementDeploymentEvidence, SettlementDeploymentSource},
        },
        enums::{
            execution::KillSwitchState,
            quant::{ExecutionWalletKind, QuantRuntimeMode},
            settlement::{
                SettlementAuthorizationState, SettlementCaseState, SettlementEffectivePolicy,
                SettlementReadinessStatus, SettlementReconciliationState, SettlementRoute,
                SettlementWritePolicy,
            },
        },
        types::{
            ContentHash, EvmAddress, EvmBlockHash, EvmCodeHash, EvmUint256, ExecutionAccountId,
            MarketId, SettlementEvidenceVersion, SettlementRedeemId, TokenId,
            settlement_payload::{SettlementPayoutVector, SettlementReadinessEvidence},
        },
    };

    use super::{
        SettlementAdmissionBlockReason, SettlementAdmissionDecision,
        evaluate_new_submission_admission,
    };
    #[test]
    fn submission_admission_mode_rejects() {
        let now = timestamp(1_000);
        let redeem = ready_redeem(now);

        assert_eq!(
            evaluate_new_submission_admission(
                &redeem,
                QuantRuntimeMode::ReportOnly,
                SettlementWritePolicy::Auto,
                KillSwitchState::Closed,
                now,
            ),
            SettlementAdmissionDecision::Blocked(SettlementAdmissionBlockReason::ReportOnly)
        );
        assert_eq!(
            evaluate_new_submission_admission(
                &redeem,
                QuantRuntimeMode::AutoExecution,
                SettlementWritePolicy::Auto,
                KillSwitchState::ExecutionHalted,
                now,
            ),
            SettlementAdmissionDecision::Blocked(SettlementAdmissionBlockReason::KillSwitchHalted)
        );
        assert_eq!(
            evaluate_new_submission_admission(
                &redeem,
                QuantRuntimeMode::AutoExecution,
                SettlementWritePolicy::Disabled,
                KillSwitchState::Closed,
                now,
            ),
            SettlementAdmissionDecision::Blocked(
                SettlementAdmissionBlockReason::SettlementWritePolicyDisabled
            )
        );
        assert_eq!(
            evaluate_new_submission_admission(
                &redeem,
                QuantRuntimeMode::AutoExecution,
                SettlementWritePolicy::Auto,
                KillSwitchState::ExitOnly,
                now,
            ),
            SettlementAdmissionDecision::Admit
        );
    }

    #[test]
    fn documentation_drift_requires_authorization() {
        let now = timestamp(2_000);
        let mut redeem = ready_redeem(now);
        redeem.readiness_evidence_json.advisories.push(
            SettlementDeploymentEvidence::RepositoryDocumentationDrift {
                route: SettlementRoute::StandardV2,
                source: SettlementDeploymentSource::CtfExchangeV2Readme,
                revision: Some("fixture".to_owned()),
            },
        );
        assert_eq!(
            evaluate_new_submission_admission(
                &redeem,
                QuantRuntimeMode::SemiAuto,
                SettlementWritePolicy::SemiAuto,
                KillSwitchState::ExitOnly,
                now,
            ),
            SettlementAdmissionDecision::StageSemiAutoAuthorization
        );

        redeem.authorization_state = SettlementAuthorizationState::Approved;
        redeem.authorization_digest = Some(ContentHash::from_bytes([0x44; 32]));
        redeem.authorization_expires_at = Some(timestamp(2_300));
        assert_eq!(
            evaluate_new_submission_admission(
                &redeem,
                QuantRuntimeMode::SemiAuto,
                SettlementWritePolicy::SemiAuto,
                KillSwitchState::ExitOnly,
                now,
            ),
            SettlementAdmissionDecision::Admit
        );

        redeem.authorization_expires_at = Some(now);
        assert_eq!(
            evaluate_new_submission_admission(
                &redeem,
                QuantRuntimeMode::SemiAuto,
                SettlementWritePolicy::SemiAuto,
                KillSwitchState::Closed,
                now,
            ),
            SettlementAdmissionDecision::Blocked(
                SettlementAdmissionBlockReason::AuthorizationExpired
            )
        );
    }

    #[test]
    fn auto_ignores_stale_attempts() {
        let now = timestamp(3_000);
        let mut redeem = ready_redeem(now);
        for terminal in [
            SettlementAuthorizationState::NotRequired,
            SettlementAuthorizationState::Expired,
            SettlementAuthorizationState::Revoked,
        ] {
            redeem.authorization_state = terminal;
            assert_eq!(
                evaluate_new_submission_admission(
                    &redeem,
                    QuantRuntimeMode::AutoExecution,
                    SettlementWritePolicy::Auto,
                    KillSwitchState::Closed,
                    now,
                ),
                SettlementAdmissionDecision::Admit
            );
        }
        for active_or_consumed in [
            SettlementAuthorizationState::Pending,
            SettlementAuthorizationState::Approved,
            SettlementAuthorizationState::Consumed,
        ] {
            redeem.authorization_state = active_or_consumed;
            assert_eq!(
                evaluate_new_submission_admission(
                    &redeem,
                    QuantRuntimeMode::AutoExecution,
                    SettlementWritePolicy::Auto,
                    KillSwitchState::Closed,
                    now,
                ),
                SettlementAdmissionDecision::Blocked(
                    SettlementAdmissionBlockReason::AuthorizationInvalid
                )
            );
        }
    }

    #[test]
    fn manual_only_blocks_submission() {
        let now = timestamp(4_000);
        let mut redeem = ready_redeem(now);
        redeem.effective_policy = SettlementEffectivePolicy::ManualOnly;
        redeem.authorization_state = SettlementAuthorizationState::Approved;
        redeem.authorization_digest = Some(ContentHash::from_bytes([0x45; 32]));
        redeem.authorization_expires_at = Some(timestamp(4_300));

        for (mode, write_policy) in [
            (
                QuantRuntimeMode::SemiAuto,
                SettlementWritePolicy::GovernedCanary,
            ),
            (QuantRuntimeMode::SemiAuto, SettlementWritePolicy::SemiAuto),
            (QuantRuntimeMode::AutoExecution, SettlementWritePolicy::Auto),
        ] {
            assert_eq!(
                evaluate_new_submission_admission(
                    &redeem,
                    mode,
                    write_policy,
                    KillSwitchState::Closed,
                    now,
                ),
                SettlementAdmissionDecision::Blocked(
                    SettlementAdmissionBlockReason::ManualOnlyInventory
                ),
                "{write_policy:?} must not sweep inventory containing a manual-policy lot"
            );
        }
    }

    fn ready_redeem(now: DateTime<Utc>) -> SettlementRedeemInfo {
        SettlementRedeemInfo {
            settlement_redeem_id: SettlementRedeemId::from_v7(),
            market_id: MarketId::new("settlement-admission-test"),
            yes_token_id: TokenId::new("101"),
            no_token_id: TokenId::new("102"),
            execution_account_id: ExecutionAccountId::from_v7(),
            resolution_content_hash: ContentHash::from_bytes([0x19; 32]),
            resolution_outcome: "Yes".to_owned(),
            resolved_at: now,
            funder_address: address(0x11),
            wallet_kind: ExecutionWalletKind::Eoa,
            route: SettlementRoute::StandardV2,
            effective_policy: SettlementEffectivePolicy::AutomaticEligible,
            inventory_digest: ContentHash::from_bytes([0x20; 32]),
            contributor_lots_digest: ContentHash::from_bytes([0x21; 32]),
            state: SettlementCaseState::Discovered,
            readiness_status: SettlementReadinessStatus::Ready,
            readiness_evidence_json: SettlementReadinessEvidence::default(),
            target_adapter: Some(current_adapter()),
            target_code_hash: Some(
                EvmCodeHash::parse(
                    "0x93b965351d01c1a128821ac79fc98a18105daefb46bda0d1e5b52306d713aa4f",
                )
                .expect("current code hash"),
            ),
            deployment_digest: Some(ContentHash::from_bytes([0x22; 32])),
            deployment_evidence_version: Some(
                SettlementEvidenceVersion::parse("polymarket-v2-2026-07-22.1")
                    .expect("evidence version"),
            ),
            verified_block_number: Some(90_685_098),
            verified_block_hash: Some(block_hash(0x23)),
            current_authorization_id: None,
            authorization_state: SettlementAuthorizationState::NotRequired,
            authorization_digest: None,
            authorization_expires_at: None,
            authorized_by: None,
            authorized_at: None,
            authorization_revoked_at: None,
            authorization_consumed_at: None,
            reconciliation_state: SettlementReconciliationState::NotRequired,
            payout_vector_json: SettlementPayoutVector {
                denominator: EvmUint256::parse("1").expect("denominator"),
                yes: EvmUint256::parse("1").expect("yes numerator"),
                no: EvmUint256::parse("0").expect("no numerator"),
            },
            balance_before_json: None,
            balance_after_json: None,
            expected_payout_usd: None,
            actual_payout_usd: None,
            gas_fee_pol: None,
            failure_code: None,
            attempt_count: 0,
            retry_count: 0,
            next_attempt_at: None,
            claim_owner: None,
            lease_expires_at: None,
            last_error: None,
            prepared_at: None,
            submitted_at: None,
            confirmed_at: None,
            failed_at: None,
            created_at: now,
            updated_at: now,
        }
    }

    fn current_adapter() -> EvmAddress {
        EvmAddress::parse("0xada100db00ca00073811820692005400218fce1f").expect("current adapter")
    }

    fn address(byte: u8) -> EvmAddress {
        let octet = format!("{byte:02x}");
        EvmAddress::parse(format!("0x{}", octet.repeat(20))).expect("address")
    }

    fn block_hash(byte: u8) -> EvmBlockHash {
        let octet = format!("{byte:02x}");
        EvmBlockHash::parse(format!("0x{}", octet.repeat(32))).expect("block hash")
    }

    fn timestamp(seconds: i64) -> DateTime<Utc> {
        Utc.timestamp_opt(seconds, 0).single().expect("timestamp")
    }
}

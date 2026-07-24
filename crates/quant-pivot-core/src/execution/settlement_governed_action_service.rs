//! Durable operator-approval and revocation orchestration.
//!
//! HTTP handlers only authorize immutable action scope. This service is the
//! sole production owner of signing, durable envelope persistence, exact replay,
//! relayer polling, finality verification, and terminal evidence transitions.

use std::sync::Arc;

use async_trait::async_trait;
use chrono::{DateTime, Utc};
use quant_pivot_error::{QuantError, QuantResult, execution::ExecutionError};
use quant_pivot_models::{
    config::SettlementDeployConfig,
    domain::quant::{
        settlement::{NewSettlementChainSubmission, SettlementChainSubmissionInfo},
        settlement_governance::{
            BeginGovernedActionDispatch, ConfirmSettlementGovernedAction,
            FailSettlementGovernedAction, PersistPreparedGovernedActionSubmission,
            RecordGovernedActionEoaBroadcast, RecordGovernedActionRelayerAcceptance,
            RecordGovernedActionRelayerChainHash, RequireGovernedActionReconciliation,
            ScheduleGovernedActionRetry, ScheduleGovernedActionWork, SettlementGovernedActionInfo,
            SettlementGovernedActionWorkClaim,
        },
    },
    enums::{
        quant::QuantRuntimeMode,
        settlement::{SettlementFailureCode, SettlementSubmissionState, SettlementWritePolicy},
    },
    types::{
        EvmTransactionHash, ExecutionAccountId, SettlementChainSubmissionId,
        SettlementGovernedActionId, WorkerId, settlement_payload::SettlementChainReceiptEvidence,
    },
};
use quant_pivot_repository::traits::quant::settlement_governance::SettlementGovernanceRepository;

use super::{
    settlement_service::{SettlementDispatchResult, SettlementExecutorError},
    settlement_timing::{deadline, elapsed_ms_since, retry_deadline},
};
use crate::{governance::RuntimeControlsHandle, observability::metrics_hub::MetricsHub};

/// Read-only recovery result for a governed action submission.
#[derive(Debug)]
pub enum SettlementGovernedActionTrackingResult {
    Pending,
    ChainHashObserved(EvmTransactionHash),
    Confirmed(Box<SettlementChainReceiptEvidence>),
    ReconciliationRequired {
        failure_code: SettlementFailureCode,
        detail: String,
    },
}

/// Money-moving executor boundary for operator approval and revocation.
#[async_trait]
pub trait SettlementGovernedActionExecutor: Send + Sync {
    async fn prepare_action(
        &self,
        action: &SettlementGovernedActionInfo,
    ) -> Result<NewSettlementChainSubmission, SettlementExecutorError>;

    async fn dispatch_action(
        &self,
        submission: &SettlementChainSubmissionInfo,
    ) -> Result<SettlementDispatchResult, SettlementExecutorError>;

    async fn track_action(
        &self,
        action: &SettlementGovernedActionInfo,
        submission: &SettlementChainSubmissionInfo,
    ) -> Result<SettlementGovernedActionTrackingResult, SettlementExecutorError>;
}

/// One bounded worker pass outcome.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SettlementGovernedActionPassOutcome {
    Idle,
    Deferred {
        settlement_governed_action_id: SettlementGovernedActionId,
    },
    DispatchAccepted {
        settlement_chain_submission_id: SettlementChainSubmissionId,
    },
    ExistingSubmissionTracked {
        settlement_chain_submission_id: SettlementChainSubmissionId,
    },
    Confirmed {
        settlement_chain_submission_id: SettlementChainSubmissionId,
    },
    ReconciliationRequired {
        settlement_chain_submission_id: SettlementChainSubmissionId,
        failure_code: SettlementFailureCode,
    },
    RetryScheduled {
        settlement_governed_action_id: SettlementGovernedActionId,
        settlement_chain_submission_id: Option<SettlementChainSubmissionId>,
        failure_code: SettlementFailureCode,
        next_attempt_at: DateTime<Utc>,
    },
    Failed {
        settlement_governed_action_id: SettlementGovernedActionId,
        failure_code: SettlementFailureCode,
    },
}

pub struct SettlementGovernedActionServiceDeps {
    pub repository: Arc<dyn SettlementGovernanceRepository>,
    pub executor: Arc<dyn SettlementGovernedActionExecutor>,
    pub runtime_controls: RuntimeControlsHandle,
    pub config: SettlementDeployConfig,
    pub execution_account_id: ExecutionAccountId,
    pub worker_id: WorkerId,
    pub metrics: Arc<MetricsHub>,
}

/// Lease-based, restart-safe governed action service.
pub struct SettlementGovernedActionService {
    repository: Arc<dyn SettlementGovernanceRepository>,
    executor: Arc<dyn SettlementGovernedActionExecutor>,
    runtime_controls: RuntimeControlsHandle,
    config: SettlementDeployConfig,
    execution_account_id: ExecutionAccountId,
    worker_id: WorkerId,
    metrics: Arc<MetricsHub>,
}

impl SettlementGovernedActionService {
    #[must_use]
    pub fn new(deps: SettlementGovernedActionServiceDeps) -> Self {
        Self {
            repository: deps.repository,
            executor: deps.executor,
            runtime_controls: deps.runtime_controls,
            config: deps.config,
            execution_account_id: deps.execution_account_id,
            worker_id: deps.worker_id,
            metrics: deps.metrics,
        }
    }

    pub async fn run_once(
        &self,
        now: DateTime<Utc>,
    ) -> QuantResult<SettlementGovernedActionPassOutcome> {
        let lease_expires_at = deadline(
            now,
            self.config.claim_lease_secs,
            "polymarket.settlement.claim_lease_secs",
        )?;
        let Some(claim) = self
            .repository
            .claim_next_action(
                &self.execution_account_id,
                &self.worker_id,
                now,
                lease_expires_at,
            )
            .await?
        else {
            return Ok(SettlementGovernedActionPassOutcome::Idle);
        };
        let action_id = claim.action.settlement_governed_action_id;
        let result = self.process_claim(claim, now).await;
        if result.is_err() {
            let released = self
                .repository
                .release_action_claim(&action_id, &self.worker_id)
                .await;
            if let Err(release_error) = released {
                tracing::error!(
                    %action_id,
                    %release_error,
                    "failed to release governed-action lease after pass error"
                );
            }
        }
        result
    }

    async fn process_claim(
        &self,
        claim: SettlementGovernedActionWorkClaim,
        now: DateTime<Utc>,
    ) -> QuantResult<SettlementGovernedActionPassOutcome> {
        match claim.submission {
            Some(submission) => self.recover_existing(claim.action, submission, now).await,
            None if !self.new_action_admitted() => {
                self.defer(&claim.action, now).await?;
                Ok(SettlementGovernedActionPassOutcome::Deferred {
                    settlement_governed_action_id: claim.action.settlement_governed_action_id,
                })
            }
            None => self.prepare_and_dispatch(claim.action, now).await,
        }
    }

    fn new_action_admitted(&self) -> bool {
        let controls = self.runtime_controls.snapshot();
        if !controls
            .kill_switch_state
            .allows_settlement_recovery_submission()
        {
            return false;
        }
        matches!(
            (
                controls.quant_runtime_mode,
                controls.settlement_write_policy
            ),
            (
                QuantRuntimeMode::SemiAuto,
                SettlementWritePolicy::GovernedCanary | SettlementWritePolicy::SemiAuto
            ) | (QuantRuntimeMode::AutoExecution, SettlementWritePolicy::Auto)
        )
    }

    async fn prepare_and_dispatch(
        &self,
        action: SettlementGovernedActionInfo,
        _now: DateTime<Utc>,
    ) -> QuantResult<SettlementGovernedActionPassOutcome> {
        let prepared = match self.executor.prepare_action(&action).await {
            Ok(prepared) => prepared,
            Err(error) => return self.handle_failure(&action, None, error, Utc::now()).await,
        };
        let persisted_at = Utc::now();
        let lease_expires_at = deadline(
            persisted_at,
            self.config.claim_lease_secs,
            "polymarket.settlement.claim_lease_secs",
        )?;
        if !self
            .repository
            .renew_action_claim(
                &action.settlement_governed_action_id,
                &self.worker_id,
                persisted_at,
                lease_expires_at,
            )
            .await?
        {
            self.metrics.record_settlement_lease_lost("governed_action");
            return Err(action_invariant(
                "governed-action lease expired while preparing signed envelope",
            ));
        }
        if !self.new_action_admitted() || action.expires_at <= persisted_at {
            self.defer(&action, persisted_at).await?;
            return Ok(SettlementGovernedActionPassOutcome::Deferred {
                settlement_governed_action_id: action.settlement_governed_action_id,
            });
        }
        let durable = self
            .repository
            .persist_prepared_action_submission(PersistPreparedGovernedActionSubmission {
                settlement_governed_action_id: action.settlement_governed_action_id,
                expected_scope_digest: action.scope_digest,
                owner: self.worker_id,
                submission: prepared,
                persisted_at,
            })
            .await?;
        let dispatching = self.begin_dispatch(&action, &durable, persisted_at).await?;
        self.dispatch_durable(&action, &dispatching).await
    }

    async fn recover_existing(
        &self,
        action: SettlementGovernedActionInfo,
        submission: SettlementChainSubmissionInfo,
        now: DateTime<Utc>,
    ) -> QuantResult<SettlementGovernedActionPassOutcome> {
        self.metrics.observe_submission_age_ms(
            "governed_action",
            elapsed_ms_since(now, submission.created_at),
        );
        if submission.state == SettlementSubmissionState::AwaitingFinality {
            self.metrics.observe_finality_lag_ms(
                "governed_action",
                elapsed_ms_since(
                    now,
                    submission.dispatched_at.unwrap_or(submission.created_at),
                ),
            );
        }
        match submission.state {
            SettlementSubmissionState::Prepared => {
                let dispatching = self.begin_dispatch(&action, &submission, now).await?;
                self.dispatch_durable(&action, &dispatching).await
            }
            SettlementSubmissionState::Dispatching => {
                self.dispatch_durable(&action, &submission).await
            }
            SettlementSubmissionState::AwaitingChainHash
            | SettlementSubmissionState::AwaitingFinality => {
                self.track_existing(&action, &submission, now).await
            }
            SettlementSubmissionState::Confirmed | SettlementSubmissionState::Failed => Err(
                action_invariant("governed-action recovery claim carried a terminal submission"),
            ),
        }
    }

    async fn begin_dispatch(
        &self,
        action: &SettlementGovernedActionInfo,
        submission: &SettlementChainSubmissionInfo,
        now: DateTime<Utc>,
    ) -> QuantResult<SettlementChainSubmissionInfo> {
        let envelope_hash = submission
            .signed_envelope_hash
            .ok_or_else(|| action_invariant("governed-action submission has no envelope hash"))?;
        self.repository
            .begin_action_dispatch(BeginGovernedActionDispatch {
                settlement_governed_action_id: action.settlement_governed_action_id,
                settlement_chain_submission_id: submission.settlement_chain_submission_id,
                expected_scope_digest: action.scope_digest,
                expected_target_adapter: submission.target_adapter.clone(),
                expected_deployment_digest: submission.deployment_digest,
                expected_calldata_hash: submission.calldata_hash.clone(),
                expected_signed_envelope_hash: envelope_hash,
                owner: self.worker_id,
                dispatching_at: now,
            })
            .await
            .map_err(Into::into)
    }

    async fn dispatch_durable(
        &self,
        action: &SettlementGovernedActionInfo,
        submission: &SettlementChainSubmissionInfo,
    ) -> QuantResult<SettlementGovernedActionPassOutcome> {
        let result = match self.executor.dispatch_action(submission).await {
            Ok(result) => result,
            Err(error) => {
                return self
                    .handle_failure(action, Some(submission), error, Utc::now())
                    .await;
            }
        };
        let envelope_hash = submission
            .signed_envelope_hash
            .ok_or_else(|| action_invariant("dispatching governed action has no envelope hash"))?;
        let observed_at = Utc::now();
        match result {
            SettlementDispatchResult::Ambiguous => {
                self.schedule_retry(
                    action,
                    Some(submission),
                    SettlementFailureCode::TransportUncertain,
                    "transport returned no trustworthy governed-action acknowledgement",
                    observed_at,
                )
                .await
            }
            SettlementDispatchResult::EoaAccepted => {
                self.repository
                    .record_action_eoa_broadcast(RecordGovernedActionEoaBroadcast {
                        settlement_governed_action_id: action.settlement_governed_action_id,
                        settlement_chain_submission_id: submission.settlement_chain_submission_id,
                        expected_signed_envelope_hash: envelope_hash,
                        owner: self.worker_id,
                        observed_at,
                    })
                    .await?;
                self.defer(action, observed_at).await?;
                Ok(SettlementGovernedActionPassOutcome::DispatchAccepted {
                    settlement_chain_submission_id: submission.settlement_chain_submission_id,
                })
            }
            SettlementDispatchResult::RelayerAccepted(relayer_transaction_id) => {
                self.repository
                    .record_action_relayer_acceptance(RecordGovernedActionRelayerAcceptance {
                        settlement_governed_action_id: action.settlement_governed_action_id,
                        settlement_chain_submission_id: submission.settlement_chain_submission_id,
                        expected_signed_envelope_hash: envelope_hash,
                        relayer_transaction_id,
                        owner: self.worker_id,
                        observed_at,
                    })
                    .await?;
                self.defer(action, observed_at).await?;
                Ok(SettlementGovernedActionPassOutcome::DispatchAccepted {
                    settlement_chain_submission_id: submission.settlement_chain_submission_id,
                })
            }
        }
    }

    async fn track_existing(
        &self,
        action: &SettlementGovernedActionInfo,
        submission: &SettlementChainSubmissionInfo,
        observed_at: DateTime<Utc>,
    ) -> QuantResult<SettlementGovernedActionPassOutcome> {
        let tracking = match self.executor.track_action(action, submission).await {
            Ok(tracking) => tracking,
            Err(error) => {
                return self
                    .handle_failure(action, Some(submission), error, observed_at)
                    .await;
            }
        };
        match tracking {
            SettlementGovernedActionTrackingResult::Pending => {
                self.defer(action, observed_at).await?;
                Ok(
                    SettlementGovernedActionPassOutcome::ExistingSubmissionTracked {
                        settlement_chain_submission_id: submission.settlement_chain_submission_id,
                    },
                )
            }
            SettlementGovernedActionTrackingResult::ChainHashObserved(transaction_hash) => {
                let transaction_id = submission
                    .relayer_transaction_id
                    .clone()
                    .ok_or_else(|| action_invariant("relayer action has no opaque identity"))?;
                self.repository
                    .record_relayer_chain_hash(RecordGovernedActionRelayerChainHash {
                        settlement_governed_action_id: action.settlement_governed_action_id,
                        settlement_chain_submission_id: submission.settlement_chain_submission_id,
                        expected_relayer_transaction_id: transaction_id,
                        transaction_hash,
                        owner: self.worker_id,
                        observed_at,
                    })
                    .await?;
                self.defer(action, observed_at).await?;
                Ok(
                    SettlementGovernedActionPassOutcome::ExistingSubmissionTracked {
                        settlement_chain_submission_id: submission.settlement_chain_submission_id,
                    },
                )
            }
            SettlementGovernedActionTrackingResult::Confirmed(receipt_evidence) => {
                self.repository
                    .confirm_action(ConfirmSettlementGovernedAction {
                        settlement_governed_action_id: action.settlement_governed_action_id,
                        settlement_chain_submission_id: submission.settlement_chain_submission_id,
                        expected_scope_digest: action.scope_digest,
                        owner: self.worker_id,
                        receipt_evidence: *receipt_evidence,
                        confirmed_at: observed_at,
                    })
                    .await?;
                Ok(SettlementGovernedActionPassOutcome::Confirmed {
                    settlement_chain_submission_id: submission.settlement_chain_submission_id,
                })
            }
            SettlementGovernedActionTrackingResult::ReconciliationRequired {
                failure_code,
                detail,
            } => {
                self.require_reconciliation(action, submission, failure_code, &detail, observed_at)
                    .await
            }
        }
    }

    async fn handle_failure(
        &self,
        action: &SettlementGovernedActionInfo,
        submission: Option<&SettlementChainSubmissionInfo>,
        error: SettlementExecutorError,
        observed_at: DateTime<Utc>,
    ) -> QuantResult<SettlementGovernedActionPassOutcome> {
        match error {
            SettlementExecutorError::Transient { detail, .. } => {
                self.schedule_retry(
                    action,
                    submission,
                    SettlementFailureCode::TransportUncertain,
                    &detail,
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
                        action,
                        submission,
                        failure_code,
                        &detail,
                        observed_at,
                    )
                    .await
                }
                None => self.fail(action, failure_code, &detail, observed_at).await,
            },
            SettlementExecutorError::Invariant { detail } => match submission {
                Some(submission) => {
                    self.require_reconciliation(
                        action,
                        submission,
                        SettlementFailureCode::LocalInvariant,
                        &detail,
                        observed_at,
                    )
                    .await
                }
                None => {
                    self.fail(
                        action,
                        SettlementFailureCode::LocalInvariant,
                        &detail,
                        observed_at,
                    )
                    .await
                }
            },
        }
    }

    async fn schedule_retry(
        &self,
        action: &SettlementGovernedActionInfo,
        submission: Option<&SettlementChainSubmissionInfo>,
        failure_code: SettlementFailureCode,
        detail: &str,
        observed_at: DateTime<Utc>,
    ) -> QuantResult<SettlementGovernedActionPassOutcome> {
        let next_attempt_at = retry_deadline(
            observed_at,
            action.retry_count,
            self.config.retry_initial_secs,
            self.config.retry_max_secs,
            &action.settlement_governed_action_id.to_string(),
        )?;
        self.repository
            .schedule_action_retry(ScheduleGovernedActionRetry {
                settlement_governed_action_id: action.settlement_governed_action_id,
                expected_scope_digest: action.scope_digest,
                owner: self.worker_id,
                failure_code,
                next_attempt_at,
                last_error: bounded_detail(detail),
                scheduled_at: observed_at,
            })
            .await?;
        Ok(SettlementGovernedActionPassOutcome::RetryScheduled {
            settlement_governed_action_id: action.settlement_governed_action_id,
            settlement_chain_submission_id: submission
                .map(|value| value.settlement_chain_submission_id),
            failure_code,
            next_attempt_at,
        })
    }

    async fn defer(
        &self,
        action: &SettlementGovernedActionInfo,
        observed_at: DateTime<Utc>,
    ) -> QuantResult<()> {
        let next_attempt_at = deadline(
            observed_at,
            self.config.submission_poll_secs,
            "polymarket.settlement.governed_action_next_work_at",
        )?;
        self.repository
            .schedule_action_work(ScheduleGovernedActionWork {
                settlement_governed_action_id: action.settlement_governed_action_id,
                expected_scope_digest: action.scope_digest,
                owner: self.worker_id,
                next_attempt_at,
                scheduled_at: observed_at,
            })
            .await?;
        Ok(())
    }

    async fn fail(
        &self,
        action: &SettlementGovernedActionInfo,
        failure_code: SettlementFailureCode,
        detail: &str,
        failed_at: DateTime<Utc>,
    ) -> QuantResult<SettlementGovernedActionPassOutcome> {
        self.repository
            .fail_action(FailSettlementGovernedAction {
                settlement_governed_action_id: action.settlement_governed_action_id,
                expected_scope_digest: action.scope_digest,
                owner: self.worker_id,
                failure_code,
                last_error: bounded_detail(detail),
                failed_at,
            })
            .await?;
        Ok(SettlementGovernedActionPassOutcome::Failed {
            settlement_governed_action_id: action.settlement_governed_action_id,
            failure_code,
        })
    }

    async fn require_reconciliation(
        &self,
        action: &SettlementGovernedActionInfo,
        submission: &SettlementChainSubmissionInfo,
        failure_code: SettlementFailureCode,
        detail: &str,
        observed_at: DateTime<Utc>,
    ) -> QuantResult<SettlementGovernedActionPassOutcome> {
        self.repository
            .require_action_reconciliation(RequireGovernedActionReconciliation {
                settlement_governed_action_id: action.settlement_governed_action_id,
                settlement_chain_submission_id: submission.settlement_chain_submission_id,
                expected_scope_digest: action.scope_digest,
                owner: self.worker_id,
                failure_code,
                last_error: bounded_detail(detail),
                observed_at,
            })
            .await?;
        self.metrics
            .record_settlement_reconciliation_required("governed_action", failure_code.as_str());
        Ok(
            SettlementGovernedActionPassOutcome::ReconciliationRequired {
                settlement_chain_submission_id: submission.settlement_chain_submission_id,
                failure_code,
            },
        )
    }
}

fn bounded_detail(detail: &str) -> String {
    let bounded = detail.trim().chars().take(2_048).collect::<String>();
    if bounded.is_empty() {
        "governed settlement action failed without diagnostic detail".to_owned()
    } else {
        bounded
    }
}

fn action_invariant(reason: &str) -> QuantError {
    ExecutionError::SettlementRedeemInvariant {
        reason: reason.to_owned(),
    }
    .into()
}

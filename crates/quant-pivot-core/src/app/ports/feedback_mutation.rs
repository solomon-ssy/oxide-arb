//! Governed feedback-cycle and promotion-permit application boundary.

use std::sync::Arc;

use async_trait::async_trait;
use chrono::{DateTime, Duration, Utc};
use quant_pivot_error::{QuantError, QuantResult, feedback::FeedbackError, storage::StorageError};
use quant_pivot_models::{
    domain::{
        api::{
            ActivateModelRouteRequest, BootstrapModelRouteRequest, CancelFeedbackCycleRequest,
            FeedbackCycleMutationView, FeedbackCycleTriggerView, FeedbackSchedulerControlRequest,
            FeedbackSchedulerMutationView, FeedbackSchedulerStateView, IssuePromotionPermitRequest,
            ModelRouteActivationReceiptView, ModelRouteBootstrapReceiptView,
            PromotionPermitListQuery, PromotionPermitMutationView, PromotionPermitView,
            RevokePromotionPermitRequest, TriggerFeedbackCycleRequest,
        },
        pagination::Paginated,
        ports::{
            FeedbackCandidateFamily, FeedbackCandidateFamilyInput, FeedbackCandidateRecipe,
            FeedbackComparisonContract, FeedbackDatasetBuildRequest, FeedbackMutationPort,
        },
        quant::{
            BootstrapModelRoute, FeedbackCohortWindow, FeedbackCycleActor, FeedbackCycleKey,
            FeedbackCycleKeyInput, FeedbackSchedulerClaim, FeedbackSchedulerControl,
            FeedbackSchedulerSuccess, FeedbackStageEventInput, GovernedFeedbackCancellation,
            GovernedFeedbackTrigger, IssuePromotionPermit, ModelGovernanceAuditDetail,
            NewFeedbackCycle, NewFeedbackSchedulerState, NewFeedbackStageEvent, PromoteModelRoute,
            PromotionPermitActor, PromotionPermitInfo, RevokePromotionPermit,
        },
    },
    enums::{
        common::MarketCategory,
        quant::{
            CalibrationMethod, DatasetPurpose, DownsideSource, FeedbackStage,
            FeedbackStageEventKind, FeedbackTriggerFamily,
        },
    },
    hashing::CanonicalDigest,
    runtime_config::BuyModelRoute,
    types::{
        ContentHash, DATASET_ARTIFACT_FORMAT_VERSION, DatasetSourceLineage,
        DecisionPolicySnapshotId, FeedbackCycleId, ModelSpecId, PromotionPermitId,
        ResearchProfileArtifact, ResearchProfileId, TrainingDatasetId, builtin_research_profiles,
    },
};
use quant_pivot_repository::traits::{
    FeedbackCycleCasOutcome, FeedbackCycleRepository, FeedbackCycleWriteOutcome,
    FeedbackSchedulerRepository, FeedbackStageWriteOutcome, FeedbackTriggerCommit,
    FeedbackTriggerWriteOutcome, ModelRouteBootstrapCommit, ModelRouteBootstrapOutcome,
    ModelRoutePromotionCommit, ModelRoutePromotionOutcome, PromotionPermitIssueOutcome,
    PromotionPermitRepository, PromotionPermitRevokeOutcome,
};
use tokio_util::sync::CancellationToken;

use super::training_dataset::{CoreTrainingDatasetPort, FeedbackSourceFreeze};
use crate::{
    governance::PromotionPermitService,
    observability::metrics_hub::MetricsHub,
    service::{
        feedback_coordinator::{FeedbackCoordinatorWake, FeedbackTimeline},
        model_route_governance::ModelRouteGovernanceService,
        model_serving_generation::ModelServingGenerationStore,
        model_serving_preimage::ModelServingPreimageService,
        promotion_preflight::{PromotionPreflightDraft, PromotionPreflightService},
    },
};

const FEEDBACK_SOURCE_PROGRAM_DOMAIN: &str = "quant-pivot/feedback-source-program";
const FEEDBACK_SOURCE_PROGRAM_VERSION: u32 = 1;
const FEEDBACK_DATASET_PLAN_DOMAIN: &str = "quant-pivot/feedback-dataset-plan";
const FEEDBACK_DATASET_PLAN_VERSION: u32 = 1;

/// Complete dependencies for the governed feedback mutation port.
pub struct CoreFeedbackMutationDeps {
    pub cycles: Arc<dyn FeedbackCycleRepository>,
    pub scheduler: Arc<dyn FeedbackSchedulerRepository>,
    pub permits: Arc<dyn PromotionPermitRepository>,
    pub permit_service: Arc<PromotionPermitService>,
    pub promotion_preflight: Arc<PromotionPreflightService>,
    pub serving_preimages: Arc<ModelServingPreimageService>,
    pub serving_generations: Arc<ModelServingGenerationStore>,
    pub route_governance: Arc<ModelRouteGovernanceService>,
    pub training_datasets: Arc<CoreTrainingDatasetPort>,
    pub feedback_wake: FeedbackCoordinatorWake,
    pub shutdown: CancellationToken,
    pub metrics: Arc<MetricsHub>,
}

/// Core owner of manual-cycle freezing and governed permit orchestration.
pub struct CoreFeedbackMutationPort {
    deps: CoreFeedbackMutationDeps,
}

/// Immutable server plan shared by feedback identity freezing and durable
/// Source Slice lookup.
#[derive(Debug, Clone)]
pub struct FeedbackCycleFreezePlan {
    label_cutoff: DateTime<Utc>,
    training: FeedbackCohortWindow,
    calibration: FeedbackCohortWindow,
    evaluation: FeedbackCohortWindow,
    source_start: DateTime<Utc>,
    research_program_hash: ContentHash,
}

impl FeedbackCycleFreezePlan {
    /// Derive the cadence bucket, purged cohorts, source window, and canonical
    /// research-program commitment from one exact serving preimage.
    pub fn derive(
        profile: &ResearchProfileArtifact,
        model_spec_id: ModelSpecId,
        model_spec_definition_hash: ContentHash,
        decision_policy_snapshot_id: DecisionPolicySnapshotId,
        runtime_config_hash: ContentHash,
        database_now: DateTime<Utc>,
    ) -> QuantResult<Self> {
        let label_cutoff = Self::cadence_cutoff(database_now, profile)?;
        Self::derive_at_cutoff(
            profile,
            model_spec_id,
            model_spec_definition_hash,
            decision_policy_snapshot_id,
            runtime_config_hash,
            label_cutoff,
        )
    }

    /// Derive one frozen cohort plan for an already persisted cadence cutoff.
    pub fn derive_at_cutoff(
        profile: &ResearchProfileArtifact,
        model_spec_id: ModelSpecId,
        model_spec_definition_hash: ContentHash,
        decision_policy_snapshot_id: DecisionPolicySnapshotId,
        runtime_config_hash: ContentHash,
        label_cutoff: DateTime<Utc>,
    ) -> QuantResult<Self> {
        if Self::cadence_cutoff(label_cutoff, profile)? != label_cutoff {
            return Err(FeedbackError::InvalidCycleIdentity {
                detail: "feedback label cutoff is not aligned to the profile cadence".to_owned(),
            }
            .into());
        }
        let evaluation_days = i64::from(profile.spec.feedback_policy.evaluation_window_days);
        let fit_days = i64::from(profile.spec.fit_span_days);
        let target_horizon = Self::seconds(
            profile.spec.target_horizon_secs,
            "target horizon exceeds chrono bounds",
        )?;
        let embargo = Self::seconds(
            profile.spec.purge_embargo_secs,
            "purge embargo exceeds chrono bounds",
        )?;
        let lookback = Self::seconds(
            profile.spec.max_feature_lookback_secs,
            "feature lookback exceeds chrono bounds",
        )?;
        let evaluation_end =
            Self::subtract(label_cutoff, target_horizon, "evaluation end underflowed")?;
        let evaluation_start = Self::subtract(
            evaluation_end,
            Duration::days(evaluation_days),
            "evaluation start underflowed",
        )?;
        let calibration_end =
            Self::subtract(evaluation_start, embargo, "calibration end underflowed")?;
        let calibration_start = Self::subtract(
            calibration_end,
            Duration::days(evaluation_days),
            "calibration start underflowed",
        )?;
        let training_end = Self::subtract(calibration_start, embargo, "training end underflowed")?;
        let training_start = Self::subtract(
            training_end,
            Duration::days(fit_days),
            "training start underflowed",
        )?;
        let source_start =
            Self::subtract(training_start, lookback, "source window start underflowed")?;
        let training = FeedbackCohortWindow::try_new(
            profile.profile_ref.clone(),
            training_start,
            training_end,
        )
        .map_err(|error| FeedbackError::InvalidCycleIdentity {
            detail: error.to_string(),
        })?;
        let calibration = FeedbackCohortWindow::try_new(
            profile.profile_ref.clone(),
            calibration_start,
            calibration_end,
        )
        .map_err(|error| FeedbackError::InvalidCycleIdentity {
            detail: error.to_string(),
        })?;
        let evaluation = FeedbackCohortWindow::try_new(
            profile.profile_ref.clone(),
            evaluation_start,
            evaluation_end,
        )
        .map_err(|error| FeedbackError::InvalidCycleIdentity {
            detail: error.to_string(),
        })?;
        let research_program_hash = CanonicalDigest::content_hash_typed(
            FEEDBACK_SOURCE_PROGRAM_DOMAIN,
            FEEDBACK_SOURCE_PROGRAM_VERSION,
            &(
                FEEDBACK_SOURCE_PROGRAM_VERSION,
                &profile.profile_ref,
                model_spec_id,
                model_spec_definition_hash,
                decision_policy_snapshot_id,
                runtime_config_hash,
                label_cutoff,
                training.window_start(),
                training.cutoff(),
                calibration.window_start(),
                calibration.cutoff(),
                evaluation.window_start(),
                evaluation.cutoff(),
                DATASET_ARTIFACT_FORMAT_VERSION,
            ),
        )?;
        Ok(Self {
            label_cutoff,
            training,
            calibration,
            evaluation,
            source_start,
            research_program_hash,
        })
    }

    #[must_use]
    pub const fn label_cutoff(&self) -> DateTime<Utc> {
        self.label_cutoff
    }

    #[must_use]
    pub const fn training(&self) -> &FeedbackCohortWindow {
        &self.training
    }

    #[must_use]
    pub const fn calibration(&self) -> &FeedbackCohortWindow {
        &self.calibration
    }

    #[must_use]
    pub const fn evaluation(&self) -> &FeedbackCohortWindow {
        &self.evaluation
    }

    #[must_use]
    pub const fn source_start(&self) -> DateTime<Utc> {
        self.source_start
    }

    #[must_use]
    pub const fn research_program_hash(&self) -> ContentHash {
        self.research_program_hash
    }

    fn cadence_cutoff(
        database_now: DateTime<Utc>,
        profile: &ResearchProfileArtifact,
    ) -> Result<DateTime<Utc>, FeedbackError> {
        let cadence =
            i64::try_from(profile.spec.feedback_policy.feedback_cadence_secs).map_err(|error| {
                FeedbackError::InvalidCycleIdentity {
                    detail: format!("feedback cadence exceeds i64 seconds: {error}"),
                }
            })?;
        if cadence <= 0 {
            return Err(FeedbackError::InvalidCycleIdentity {
                detail: "feedback cadence must be positive".to_owned(),
            });
        }
        let bucket = database_now.timestamp().div_euclid(cadence) * cadence;
        DateTime::from_timestamp(bucket, 0).ok_or_else(|| FeedbackError::InvalidCycleIdentity {
            detail: "feedback cadence bucket exceeds chrono bounds".to_owned(),
        })
    }

    fn seconds(value: u64, detail: &'static str) -> Result<Duration, FeedbackError> {
        let seconds = i64::try_from(value).map_err(|_| FeedbackError::InvalidCycleIdentity {
            detail: detail.to_owned(),
        })?;
        Ok(Duration::seconds(seconds))
    }

    fn subtract(
        value: DateTime<Utc>,
        duration: Duration,
        detail: &'static str,
    ) -> Result<DateTime<Utc>, FeedbackError> {
        value
            .checked_sub_signed(duration)
            .ok_or_else(|| FeedbackError::InvalidCycleIdentity {
                detail: detail.to_owned(),
            })
    }
}

impl CoreFeedbackMutationPort {
    #[must_use]
    pub const fn new(deps: CoreFeedbackMutationDeps) -> Self {
        Self { deps }
    }

    fn invalid(detail: impl Into<String>) -> QuantError {
        FeedbackError::InvalidCycleIdentity {
            detail: detail.into(),
        }
        .into()
    }

    fn activation_receipt(
        commit: ModelRoutePromotionCommit,
        permit: PromotionPermitInfo,
    ) -> QuantResult<ModelRouteActivationReceiptView> {
        let activated_by_user_id = commit.activation.activated_by_user_id.ok_or_else(|| {
            FeedbackError::PromotionTransactionConflict {
                detail: "model-route activation has no authenticated user identity".to_owned(),
            }
        })?;
        let activated_by_role = commit.audit.actor_role.clone().ok_or_else(|| {
            FeedbackError::PromotionTransactionConflict {
                detail: "model-route activation audit has no acting role".to_owned(),
            }
        })?;
        let model_governance_audit_id =
            commit.activation.model_governance_audit_id.ok_or_else(|| {
                FeedbackError::PromotionTransactionConflict {
                    detail: "model-route activation has no model governance audit".to_owned(),
                }
            })?;
        let promotion_permit_id = commit.activation.promotion_permit_id.ok_or_else(|| {
            FeedbackError::PromotionTransactionConflict {
                detail: "model-route activation has no promotion permit identity".to_owned(),
            }
        })?;
        Ok(ModelRouteActivationReceiptView {
            promotion_permit_id,
            feedback_cycle_id: match &commit.audit.detail {
                ModelGovernanceAuditDetail::PromoteRoute { record } => {
                    record.preflight().feedback_cycle_id()
                }
                _ => {
                    return Err(FeedbackError::PromotionTransactionConflict {
                        detail: "model-route activation lost its typed transaction record"
                            .to_owned(),
                    }
                    .into());
                }
            },
            previous_route_generation: commit.activation.expected_bundle_generation,
            activated_route_generation: commit.activation.bundle_generation,
            previous_model_version_id: permit.champion_model_version_id,
            activated_model_version_id: commit.audit.model_version_id.ok_or_else(|| {
                FeedbackError::PromotionTransactionConflict {
                    detail: "model-route activation audit has no candidate model".to_owned(),
                }
            })?,
            policy_activation_id: commit.activation.policy_activation_id,
            model_governance_audit_id,
            audit_event_id: commit.activation.audit_event_id,
            outbox_event_id: commit.activation.audit_event_id,
            transaction_hash: commit.transaction_hash,
            permit_issued_by_user_id: permit.issued_by_user_id,
            permit_issued_by_username: permit.issued_by_username,
            permit_issued_by_role: permit.issued_by_role,
            activated_by_user_id,
            activated_by_username: commit.activation.activated_by_label,
            activated_by_role,
            server_timestamp: commit.activation.activated_at,
            execution_authority_unchanged: true,
            replayed: commit.outcome == ModelRoutePromotionOutcome::ExactReplay,
        })
    }

    fn bootstrap_receipt(
        commit: ModelRouteBootstrapCommit,
    ) -> QuantResult<ModelRouteBootstrapReceiptView> {
        let activated_by_user_id = commit.activation.activated_by_user_id.ok_or_else(|| {
            FeedbackError::BootstrapTransactionConflict {
                detail: "model-route bootstrap has no authenticated user identity".to_owned(),
            }
        })?;
        let activated_by_role = commit.audit.actor_role.clone().ok_or_else(|| {
            FeedbackError::BootstrapTransactionConflict {
                detail: "model-route bootstrap audit has no acting role".to_owned(),
            }
        })?;
        let ModelGovernanceAuditDetail::BootstrapRoute { record } = &commit.audit.detail else {
            return Err(FeedbackError::BootstrapTransactionConflict {
                detail: "model-route bootstrap lost its typed transaction record".to_owned(),
            }
            .into());
        };
        Ok(ModelRouteBootstrapReceiptView {
            route: record.route().route,
            previous_route_generation: commit.activation.expected_bundle_generation,
            activated_route_generation: commit.activation.bundle_generation,
            activated_model_version_id: record.route().model_version_id,
            policy_activation_id: commit.activation.policy_activation_id,
            model_governance_audit_id: commit.audit.audit_id,
            audit_event_id: commit.activation.audit_event_id,
            outbox_event_id: commit.activation.audit_event_id,
            transaction_hash: commit.transaction_hash,
            activated_by_user_id,
            activated_by_username: commit.activation.activated_by_label,
            activated_by_role,
            server_timestamp: commit.activation.activated_at,
            execution_authority_unchanged: true,
            replayed: commit.outcome == ModelRouteBootstrapOutcome::ExactReplay,
        })
    }

    fn resolve_profile_route(
        profile_id: &ResearchProfileId,
    ) -> QuantResult<(ResearchProfileArtifact, BuyModelRoute)> {
        let profile = builtin_research_profiles()
            .map_err(Self::invalid)?
            .into_iter()
            .find(|profile| &profile.profile_ref.id == profile_id)
            .ok_or_else(|| StorageError::not_found("research_profile", profile_id))?;
        let route = match profile.spec.category {
            None => BuyModelRoute::Pooled,
            Some(MarketCategory::Crypto) => BuyModelRoute::Crypto,
            Some(MarketCategory::Weather) => BuyModelRoute::Weather,
            Some(category) => {
                return Err(Self::invalid(format!(
                    "profile category {category} has no canonical Buy route"
                )));
            }
        };
        Ok((profile, route))
    }

    fn dataset_request(
        purpose: DatasetPurpose,
        window: FeedbackCohortWindow,
        model_spec_id: ModelSpecId,
        model_spec_definition_hash: ContentHash,
        source_lineage: &DatasetSourceLineage,
    ) -> QuantResult<FeedbackDatasetBuildRequest> {
        let plan_hash = CanonicalDigest::content_hash_typed(
            FEEDBACK_DATASET_PLAN_DOMAIN,
            FEEDBACK_DATASET_PLAN_VERSION,
            &(
                FEEDBACK_DATASET_PLAN_VERSION,
                purpose,
                window.profile_ref(),
                window.window_start(),
                window.cutoff(),
                model_spec_id,
                model_spec_definition_hash,
                source_lineage,
                DATASET_ARTIFACT_FORMAT_VERSION,
            ),
        )?;
        let request = FeedbackDatasetBuildRequest {
            training_dataset_id: TrainingDatasetId::from_feedback_plan_hash(&plan_hash),
            model_spec_id,
            model_spec_definition_hash,
            source_lineage: source_lineage.clone(),
            window,
            purpose,
        };
        request.validate()?;
        Ok(request)
    }

    async fn freeze_feedback_cycle(
        &self,
        profile: &ResearchProfileArtifact,
        route: BuyModelRoute,
        label_cutoff: DateTime<Utc>,
    ) -> QuantResult<NewFeedbackCycle> {
        let serving = self
            .deps
            .serving_generations
            .current_route(route)
            .ok_or_else(|| FeedbackError::InvalidCycleState {
                detail: format!(
                    "feedback profile {} has no active serving route {route:?}",
                    profile.profile_ref.id
                ),
            })?;
        let champion = serving.active_version();
        let preimage = self.deps.serving_preimages.load(champion).await?;
        if preimage.profile() != profile
            || champion.profile_ref != profile.profile_ref
            || champion.category_scope != route.category()
            || serving.decision_policy_snapshot_id()
                != preimage.policy_snapshot().decision_policy_snapshot_id
        {
            return Err(Self::invalid(
                "current route, champion, profile, or committed policy preimage differs",
            ));
        }
        let model_spec = preimage.model_spec();
        let policy = preimage.policy_snapshot();
        let plan = FeedbackCycleFreezePlan::derive_at_cutoff(
            profile,
            model_spec.model_spec_id,
            model_spec.definition_hash,
            policy.decision_policy_snapshot_id,
            policy.snapshot_hash,
            label_cutoff,
        )?;
        let source_lineage = self
            .deps
            .training_datasets
            .freeze_feedback_source(
                FeedbackSourceFreeze {
                    profile,
                    runtime: &policy.snapshot,
                    decision_policy_snapshot_id: policy.decision_policy_snapshot_id,
                    runtime_config_hash: policy.snapshot_hash,
                    research_program_hash: plan.research_program_hash(),
                    window_start: plan.source_start(),
                    window_end: plan.label_cutoff(),
                    pit_cutoff: plan.label_cutoff(),
                },
                &self.deps.shutdown,
            )
            .await?;
        let training = Self::dataset_request(
            DatasetPurpose::Training,
            plan.training().clone(),
            model_spec.model_spec_id,
            model_spec.definition_hash,
            &source_lineage,
        )?;
        let calibration = Self::dataset_request(
            DatasetPurpose::Calibration,
            plan.calibration().clone(),
            model_spec.model_spec_id,
            model_spec.definition_hash,
            &source_lineage,
        )?;
        let evaluation = Self::dataset_request(
            DatasetPurpose::Evaluation,
            plan.evaluation().clone(),
            model_spec.model_spec_id,
            model_spec.definition_hash,
            &source_lineage,
        )?;
        let recipe = FeedbackCandidateRecipe::try_seal(
            training,
            calibration,
            CalibrationMethod::Platt,
            DownsideSource::MfeMae,
            policy.decision_policy_snapshot_id,
        )?;
        let candidate_family = FeedbackCandidateFamily::try_seal(FeedbackCandidateFamilyInput {
            shared_evaluation: evaluation,
            comparison_contract: FeedbackComparisonContract::try_from_policy(
                &profile.spec.feedback_policy,
            )?,
            candidates: vec![recipe],
        })?;
        let feedback_policy_hash =
            profile
                .spec
                .feedback_policy
                .content_hash()
                .map_err(|error| FeedbackError::InvalidCycleIdentity {
                    detail: error.to_string(),
                })?;
        NewFeedbackCycle::try_seal(FeedbackCycleKey::try_new(FeedbackCycleKeyInput {
            profile_ref: profile.profile_ref.clone(),
            feedback_policy_hash,
            label_cutoff: plan.label_cutoff(),
            capability_registry_hashes: source_lineage.capability_registry_hashes,
            champion_model_version_id: champion.model_version_id,
            champion_serving_contract_hash: champion.serving_contract_hash,
            candidate_family,
        })?)
        .map_err(Into::into)
    }

    /// Reconcile every governed built-in profile into durable scheduler state.
    pub(crate) async fn sync_scheduler_profiles(&self) -> QuantResult<()> {
        let database_now = self.deps.cycles.database_time().await?;
        for profile in builtin_research_profiles().map_err(Self::invalid)? {
            self.deps
                .scheduler
                .sync_state(NewFeedbackSchedulerState::try_new(&profile, database_now)?)
                .await?;
        }
        Ok(())
    }

    /// Freeze and persist one leased scheduled occurrence without granting any
    /// permit or mutating a serving route.
    pub(crate) async fn materialize_scheduled(
        &self,
        claim: &FeedbackSchedulerClaim,
    ) -> QuantResult<FeedbackSchedulerSuccess> {
        let (profile, route) = Self::resolve_profile_route(&claim.state.research_profile_id)?;
        let expected = NewFeedbackSchedulerState::try_new(&profile, claim.claimed_at)?;
        if !claim.state.matches_profile(&expected) {
            return Err(StorageError::state_conflict(
                "quant_feedback_scheduler_state",
                Some(&claim.state.research_profile_id),
                "claimed scheduler profile differs from the governed built-in profile",
            )
            .into());
        }
        let cycle =
            Box::pin(self.freeze_feedback_cycle(&profile, route, claim.state.next_due_at)).await?;
        let occurred_at = self.deps.cycles.database_time().await?;
        let trigger = NewFeedbackStageEvent::try_seal(FeedbackStageEventInput {
            feedback_cycle_id: cycle.feedback_cycle_id(),
            event_sequence: 1,
            stage: FeedbackStage::Trigger,
            event_kind: FeedbackStageEventKind::Triggered,
            trigger_family: Some(FeedbackTriggerFamily::Scheduled),
            research_job_id: None,
            actor: Some("feedback_scheduler".to_owned()),
            reason_code: Some("scheduled_cadence".to_owned()),
            evidence_uri: None,
            evidence_hash: None,
            occurred_at,
        })?;
        let commit = self.deps.cycles.record_trigger(cycle, trigger).await?;
        let (FeedbackTriggerCommit {
            cycle: FeedbackCycleWriteOutcome::Inserted(stored),
            stage: FeedbackStageWriteOutcome::Inserted(_),
            trigger: FeedbackTriggerWriteOutcome::Inserted(_),
        }
        | FeedbackTriggerCommit {
            cycle: FeedbackCycleWriteOutcome::AlreadyPresent(stored),
            stage: FeedbackStageWriteOutcome::AlreadyPresent(_),
            trigger:
                FeedbackTriggerWriteOutcome::Inserted(_)
                | FeedbackTriggerWriteOutcome::AlreadyPresent(_),
        }) = commit
        else {
            return Err(Self::invalid(
                "scheduled trigger returned inconsistent cycle, lifecycle, or provenance outcomes",
            ));
        };
        self.deps.feedback_wake.wake();
        Ok(FeedbackSchedulerSuccess {
            feedback_cycle_id: stored.feedback_cycle_id,
            label_cutoff: stored.label_cutoff,
        })
    }

    fn trigger_view(commit: FeedbackTriggerCommit) -> QuantResult<FeedbackCycleTriggerView> {
        match commit {
            FeedbackTriggerCommit {
                cycle: FeedbackCycleWriteOutcome::Inserted(cycle),
                stage: FeedbackStageWriteOutcome::Inserted(_),
                trigger: FeedbackTriggerWriteOutcome::Inserted(_),
            } => Ok(FeedbackCycleTriggerView::new(cycle, false, false)),
            FeedbackTriggerCommit {
                cycle: FeedbackCycleWriteOutcome::AlreadyPresent(cycle),
                stage: FeedbackStageWriteOutcome::AlreadyPresent(_),
                trigger: FeedbackTriggerWriteOutcome::Inserted(_),
            } => Ok(FeedbackCycleTriggerView::new(cycle, true, false)),
            FeedbackTriggerCommit {
                cycle: FeedbackCycleWriteOutcome::AlreadyPresent(cycle),
                stage: FeedbackStageWriteOutcome::AlreadyPresent(_),
                trigger: FeedbackTriggerWriteOutcome::AlreadyPresent(_),
            } => Ok(FeedbackCycleTriggerView::new(cycle, true, true)),
            _ => Err(Self::invalid(
                "governed trigger returned inconsistent cycle, lifecycle, or provenance outcomes",
            )),
        }
    }

    fn cancel_view(
        outcome: (FeedbackCycleCasOutcome, FeedbackStageWriteOutcome),
    ) -> QuantResult<FeedbackCycleMutationView> {
        match outcome {
            (FeedbackCycleCasOutcome::Applied(cycle), FeedbackStageWriteOutcome::Inserted(_)) => {
                Ok(FeedbackCycleMutationView::new(cycle, false))
            }
            (
                FeedbackCycleCasOutcome::AlreadyApplied(cycle),
                FeedbackStageWriteOutcome::AlreadyPresent(_),
            ) => Ok(FeedbackCycleMutationView::new(cycle, true)),
            _ => Err(Self::invalid(
                "governed cancellation returned inconsistent cycle/event idempotency outcomes",
            )),
        }
    }
}

#[async_trait]
impl FeedbackMutationPort for CoreFeedbackMutationPort {
    async fn trigger_cycle(
        &self,
        request: TriggerFeedbackCycleRequest,
        actor: FeedbackCycleActor,
    ) -> QuantResult<FeedbackCycleTriggerView> {
        let (profile, route) = Self::resolve_profile_route(&request.profile_id)?;
        let database_now = self.deps.cycles.database_time().await?;
        self.deps
            .scheduler
            .sync_state(NewFeedbackSchedulerState::try_new(&profile, database_now)?)
            .await?;
        let label_cutoff = FeedbackCycleFreezePlan::cadence_cutoff(database_now, &profile)?;
        let cycle = Box::pin(self.freeze_feedback_cycle(&profile, route, label_cutoff)).await?;
        let result = self
            .deps
            .cycles
            .record_governed_trigger(GovernedFeedbackTrigger {
                actor,
                cycle,
                reason_code: request.reason,
            })
            .await?;
        self.deps.feedback_wake.wake();
        Self::trigger_view(result)
    }

    async fn cancel_cycle(
        &self,
        cycle_id: FeedbackCycleId,
        request: CancelFeedbackCycleRequest,
        actor: FeedbackCycleActor,
    ) -> QuantResult<FeedbackCycleMutationView> {
        let cycle = self
            .deps
            .cycles
            .find_cycle(&cycle_id)
            .await?
            .ok_or_else(|| StorageError::not_found("quant_feedback_cycle", cycle_id))?;
        let events = self.deps.cycles.list_stage_events(&cycle_id).await?;
        let timeline = FeedbackTimeline::parse(&events)?;
        let result = self
            .deps
            .cycles
            .request_governed_cancel(GovernedFeedbackCancellation {
                actor,
                feedback_cycle_id: cycle_id,
                expected_generation: cycle.generation,
                expected_event_sequence: timeline.next_sequence(),
                stage: timeline.stage(),
                reason_code: request.reason,
            })
            .await?;
        self.deps.feedback_wake.wake();
        Self::cancel_view(result)
    }

    async fn control_scheduler(
        &self,
        profile_id: ResearchProfileId,
        pause: bool,
        request: FeedbackSchedulerControlRequest,
    ) -> QuantResult<FeedbackSchedulerMutationView> {
        let state = self
            .deps
            .scheduler
            .apply_control(FeedbackSchedulerControl {
                research_profile_id: profile_id,
                expected_pause_revision: request.expected_pause_revision,
                pause,
                reason_code: request.reason_code,
                note: request.note,
            })
            .await?;
        let observed_at = self.deps.cycles.database_time().await?;
        Ok(FeedbackSchedulerMutationView {
            observed_at,
            state: FeedbackSchedulerStateView::from(state),
        })
    }

    async fn list_permits(
        &self,
        query: PromotionPermitListQuery,
    ) -> QuantResult<Paginated<PromotionPermitView>> {
        let page = self.deps.permits.page_permits(query).await?;
        let mut items = Vec::with_capacity(page.permits.items.len());
        for permit in page.permits.items {
            items.push(PromotionPermitView::try_new(permit, page.observed_at)?);
        }
        Ok(Paginated::new(
            items,
            page.permits.total,
            page.permits.page,
            page.permits.size,
        ))
    }

    async fn issue_permit(
        &self,
        request: IssuePromotionPermitRequest,
        actor: FeedbackCycleActor,
    ) -> QuantResult<PromotionPermitMutationView> {
        let plan = self
            .deps
            .promotion_preflight
            .prepare_issue(PromotionPreflightDraft {
                feedback_cycle_id: request.feedback_cycle_id,
                ttl_secs: request.ttl_secs,
            })
            .await?;
        let reason = format!("{}: {}", request.reason_code, request.note);
        let outcome = self
            .deps
            .permit_service
            .issue(IssuePromotionPermit {
                actor: PromotionPermitActor {
                    user_id: actor.user_id,
                    acting_role: actor.acting_role,
                },
                idempotency_key: request.idempotency_key,
                scope: plan.preflight().scope().clone(),
                preflight_hash: plan.preflight().preflight_hash(),
                reason,
            })
            .await?;
        let observed_at = self.deps.cycles.database_time().await?;
        match outcome {
            PromotionPermitIssueOutcome::Issued(permit) => {
                PromotionPermitMutationView::try_new(permit, observed_at, false).map_err(Into::into)
            }
            PromotionPermitIssueOutcome::ExactReplay(permit) => {
                PromotionPermitMutationView::try_new(permit, observed_at, true).map_err(Into::into)
            }
        }
    }

    async fn revoke_permit(
        &self,
        permit_id: PromotionPermitId,
        request: RevokePromotionPermitRequest,
        actor: FeedbackCycleActor,
    ) -> QuantResult<PromotionPermitMutationView> {
        let outcome = self
            .deps
            .permit_service
            .revoke(RevokePromotionPermit {
                promotion_permit_id: permit_id,
                expected_revision: request.expected_revision,
                actor: PromotionPermitActor {
                    user_id: actor.user_id,
                    acting_role: actor.acting_role,
                },
                reason: format!("{}: {}", request.reason_code, request.note),
            })
            .await?;
        let observed_at = self.deps.cycles.database_time().await?;
        match outcome {
            PromotionPermitRevokeOutcome::Revoked(permit) => {
                PromotionPermitMutationView::try_new(permit, observed_at, false).map_err(Into::into)
            }
            PromotionPermitRevokeOutcome::ExactReplay(permit) => {
                PromotionPermitMutationView::try_new(permit, observed_at, true).map_err(Into::into)
            }
        }
    }

    async fn activate_route(
        &self,
        request: ActivateModelRouteRequest,
        actor: FeedbackCycleActor,
    ) -> QuantResult<ModelRouteActivationReceiptView> {
        let permit_id = request.promotion_permit_id;
        let permit = self.deps.permits.load(&permit_id).await?;
        let commit = match Box::pin(self.deps.route_governance.activate(PromoteModelRoute {
            promotion_permit_id: permit_id,
            feedback_cycle_id: request.feedback_cycle_id,
            expected_policy_generation: request.expected_policy_generation,
            expected_runtime_control_revision: request.expected_runtime_control_revision,
            idempotency_key: request.idempotency_key,
            actor: PromotionPermitActor {
                user_id: actor.user_id,
                acting_role: actor.acting_role,
            },
            reason_code: request.reason_code,
            note: request.note,
        }))
        .await
        {
            Ok(commit) => commit,
            Err(error) => {
                let layer = match &error {
                    QuantError::Feedback(FeedbackError::PromotionPermitConflict { .. }) => "permit",
                    QuantError::Feedback(FeedbackError::InvalidPromotionPreflight { .. }) => {
                        "preflight"
                    }
                    QuantError::Feedback(FeedbackError::PromotionTransactionConflict {
                        ..
                    }) => "route",
                    _ => "other",
                };
                self.deps
                    .metrics
                    .record_route_governance_conflict("promotion", layer);
                return Err(error);
            }
        };
        Self::activation_receipt(commit, permit)
    }

    async fn bootstrap_route(
        &self,
        request: BootstrapModelRouteRequest,
        actor: FeedbackCycleActor,
    ) -> QuantResult<ModelRouteBootstrapReceiptView> {
        let commit = match Box::pin(self.deps.route_governance.bootstrap(BootstrapModelRoute {
            model_version_id: request.model_version_id,
            expected_policy_generation: request.expected_policy_generation,
            expected_runtime_control_revision: request.expected_runtime_control_revision,
            idempotency_key: request.idempotency_key,
            actor: PromotionPermitActor {
                user_id: actor.user_id,
                acting_role: actor.acting_role,
            },
            reason_code: request.reason_code,
            note: request.note,
        }))
        .await
        {
            Ok(commit) => commit,
            Err(error) => {
                let layer = match &error {
                    QuantError::Feedback(FeedbackError::InvalidBootstrapPreflight { .. }) => {
                        "preflight"
                    }
                    QuantError::Feedback(FeedbackError::BootstrapTransactionConflict {
                        ..
                    }) => "route",
                    QuantError::Feedback(FeedbackError::ModelRouteConvergenceConflict {
                        ..
                    }) => "convergence",
                    _ => "other",
                };
                self.deps
                    .metrics
                    .record_route_governance_conflict("bootstrap", layer);
                return Err(error);
            }
        };
        Self::bootstrap_receipt(commit)
    }
}

#[cfg(test)]
mod tests {
    use chrono::{Duration, TimeZone, Utc};
    use quant_pivot_models::types::{
        CRYPTO_PRICE_15M_PROFILE_ID, ContentHash, DecisionPolicySnapshotId, ModelSpecId,
        builtin_research_profiles,
    };

    use super::FeedbackCycleFreezePlan;

    #[test]
    fn cadence_cutoff_is_stable() {
        let profile = builtin_research_profiles()
            .expect("valid built-in profiles")
            .into_iter()
            .find(|profile| profile.profile_ref.id.as_str() == CRYPTO_PRICE_15M_PROFILE_ID)
            .expect("crypto profile");
        let database_now = Utc
            .with_ymd_and_hms(2026, 7, 28, 13, 47, 29)
            .single()
            .expect("valid test timestamp");
        let cutoff = FeedbackCycleFreezePlan::cadence_cutoff(database_now, &profile)
            .expect("derive cadence cutoff");
        let cadence = i64::try_from(profile.spec.feedback_policy.feedback_cadence_secs)
            .expect("cadence fits signed seconds");

        assert!(cutoff <= database_now);
        assert_eq!(cutoff.timestamp().rem_euclid(cadence), 0);
        assert_eq!(
            FeedbackCycleFreezePlan::cadence_cutoff(
                cutoff + Duration::seconds(cadence - 1),
                &profile,
            )
            .expect("derive same cadence bucket"),
            cutoff
        );
    }

    #[test]
    fn cohort_windows_are_purged() {
        let profile = builtin_research_profiles()
            .expect("valid built-in profiles")
            .into_iter()
            .find(|profile| profile.profile_ref.id.as_str() == CRYPTO_PRICE_15M_PROFILE_ID)
            .expect("crypto profile");
        let label_cutoff = Utc
            .with_ymd_and_hms(2026, 7, 28, 12, 0, 0)
            .single()
            .expect("valid test timestamp");
        let policy_hash = ContentHash::from_bytes([0x31; 32]);
        let plan = FeedbackCycleFreezePlan::derive(
            &profile,
            ModelSpecId::from_v7(),
            ContentHash::from_bytes([0x32; 32]),
            DecisionPolicySnapshotId::from_content_hash(&policy_hash),
            policy_hash,
            label_cutoff,
        )
        .expect("derive feedback cohorts");
        let embargo = Duration::seconds(
            i64::try_from(profile.spec.purge_embargo_secs).expect("embargo fits signed seconds"),
        );
        let horizon = Duration::seconds(
            i64::try_from(profile.spec.target_horizon_secs).expect("horizon fits signed seconds"),
        );
        let lookback = Duration::seconds(
            i64::try_from(profile.spec.max_feature_lookback_secs)
                .expect("lookback fits signed seconds"),
        );

        assert_eq!(
            plan.training().cutoff() + embargo,
            plan.calibration().window_start()
        );
        assert_eq!(
            plan.calibration().cutoff() + embargo,
            plan.evaluation().window_start()
        );
        assert_eq!(plan.evaluation().cutoff() + horizon, label_cutoff);
        assert_eq!(
            plan.source_start() + lookback,
            plan.training().window_start()
        );
    }
}

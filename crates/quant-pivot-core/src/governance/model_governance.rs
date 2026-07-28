//! [`ModelGovernanceService`]: offline governance closure orchestration.
//!
//! Reuses the registry / backtest / shadow / dataset repositories and the
//! research [`ModelQualityGate`] to enforce the money-critical lifecycle:
//!
//! - **`publish`** — gate and publish an immutable artifact without changing
//!   any `ModelRouting` policy or another artifact's lifecycle.
//! - **`retire`** — retire only an unrouted artifact; serving rollback is a
//!   separately approved `ModelRouting` revision rollback.
//!
//! Leakage is enforced at dataset **build** time and **re-scanned** on
//! publish: `rescan_leakage` decodes the frozen Parquet and
//! runs [`scan_future_leakage`] so a tampered or stale artifact cannot skip
//! the `NoPitLeakage` hard gate. A missing dataset row fails closed.
//!
//! Actor identity is recorded for audit provenance only; hard role enforcement
//! is deferred to the web wiring.

use std::sync::Arc;

use async_trait::async_trait;
use chrono::{DateTime, Duration, Utc};
use quant_pivot_error::{
    QuantError, QuantResult, governance::GovernanceError, research::ResearchError,
};
use quant_pivot_models::{
    domain::{
        api::{
            BindCalibrationRequest, BindPublishPathSetRequest, GateOutcomeView, GatePreviewIntent,
            QualityGateReportView,
        },
        ports::{GovernanceActor, ModelGovernancePort, PublishModelCommand, RetireModelCommand},
        quant::{
            BacktestPathSetInfo, BacktestReportInfo, ModelGovernanceAuditDetail, ModelVersionInfo,
            NewModelGovernanceAudit, NewModelVersion, ShadowStabilitySummary, TrainingDatasetInfo,
        },
    },
    enums::{
        model::ModelFamily,
        quant::{CalibrationKind, ModelGovernanceAction, PublicationStatus},
    },
    hashing::CanonicalDigest,
    runtime_config::{
        DecisionPolicySnapshot, QualityGateConfig, sections::ResearchValidationGatesConfig,
    },
    types::{
        AuditEventId, BacktestReportId, CalibrationArtifactId, ContentHash, DatasetCoverage,
        FeatureParityRunId, ModelGovernanceAuditId, ModelVersionId, Probability, RoleCode,
        TrainingDatasetId, UserId,
        model_lineage::ModelVersionDerivation,
        model_metrics::{
            MODEL_VERSION_METRICS_FORMAT_VERSION, ModelArtifactTrainingLineage,
            ModelVersionMetricsDefinition,
        },
        model_quality::{
            GateId, GateIntent, GateOutcome, GateSubject, QualityGateFailure, QualityGateReport,
        },
        model_serving::{
            ModelServingCalibrationArtifactRef, ModelServingContract, ModelServingEstimatorBinding,
            ModelServingEstimatorInput,
        },
        model_training::{
            GovernedSellFitStatus, MODEL_TRAINING_OBJECTIVE_FORMAT_VERSION,
            ModelTrainingObjectiveDefinition,
        },
    },
};
use quant_pivot_repository::traits::{
    BacktestPathSetRepository, BacktestReportRepository, BindPublishPathSetCommit,
    CalibrationArtifactRepository, ModelGovernanceAuditRepository, ModelRegistryRepository,
    PublishFeatureParityPermit, PublishModelVersionCommit, ShadowComparisonRepository,
    TrainingDatasetRepository,
};
use quant_pivot_research::{
    artifact::ArtifactStore,
    backtest::BacktestReport,
    gates::{
        CpcvPathSetGateInput, ModelQualityGate, QualityGateInput, QualityGateThresholds,
        SellQualityGateThresholds, ValidationGateThresholds,
    },
    model::{
        CalibratedReturnModel, CalibrationArtifactLoader, ModelArtifact, ReturnModelSpec,
        artifact::ModelPayload,
    },
    training::{LeakageFindings, scan_future_leakage},
};
use rust_decimal::Decimal;
use serde::Serialize;

use crate::{
    governance::{
        calibration_loader::VerifiedModelScoreCalibration, resolve_return_model_calibration,
    },
    runtime_config::DecisionPolicyStore,
    service::{
        feature_integrity::FeatureParityGatePort,
        frozen_model_parity::FrozenModelParityService,
        model_serving_preimage::{ModelServingPreimageService, VerifiedModelServingPreimage},
        training_dataset,
    },
};

/// Repository + gate + config dependencies for the governance service.
pub struct ModelGovernanceDeps {
    /// Model registry (status transitions + gate-report persistence).
    pub model_registry_repo: Arc<dyn ModelRegistryRepository>,
    /// Backtest-report ledger (gate metric source).
    pub backtest_report_repo: Arc<dyn BacktestReportRepository>,
    /// CPCV path-set ledger used by the alpha-significance gate.
    pub backtest_path_set_repo: Arc<dyn BacktestPathSetRepository>,
    /// Shadow-comparison ledger (publish stability source).
    pub shadow_comparison_repo: Arc<dyn ShadowComparisonRepository>,
    /// Governance audit trail (WORM).
    pub governance_audit_repo: Arc<dyn ModelGovernanceAuditRepository>,
    /// Training-dataset ledger (promotion).
    pub dataset_repo: Arc<dyn TrainingDatasetRepository>,
    /// Shared side-effect-free verifier for every opaque serving commitment.
    pub serving_preimages: Arc<ModelServingPreimageService>,
    /// Content-addressed artifact store (hash + exact serving-contract guard).
    pub artifact_store: Arc<dyn ArtifactStore>,
    /// Unified calibration-artifact ledger (`bind_calibration` guard).
    pub calibration_repo: Arc<dyn CalibrationArtifactRepository>,
    /// Deep calibrator resolution (active + content-hash verified) — shared
    /// with the report builder / admission / intent creation via
    /// [`resolve_return_model_calibration`].
    pub calibration_loader: Arc<dyn CalibrationArtifactLoader>,
    /// The model quality gate.
    pub gate: Arc<dyn ModelQualityGate>,
    /// Active runtime config (gate thresholds + shadow window).
    pub runtime_config: Arc<DecisionPolicyStore>,
    /// Global deterministic parity latch. Publish is risk-increasing and is
    /// denied unless the governed state is explicitly clear.
    pub feature_parity_gate: Arc<dyn FeatureParityGatePort>,
    /// Subject-bound frozen dataset/model full parity verifier and publish gate.
    pub frozen_model_parity: Arc<FrozenModelParityService>,
}

/// Offline model-governance orchestration service.
pub struct ModelGovernanceService {
    deps: ModelGovernanceDeps,
}

const GOVERNANCE_AUDIT_IDENTITY_DOMAIN: &str = "quant-pivot/model-governance-audit-identity";
const GOVERNANCE_AUDIT_IDENTITY_VERSION: u32 = 1;

#[derive(Serialize)]
struct GovernanceAuditIdentity<'a> {
    model_version_id: &'a Option<ModelVersionId>,
    training_dataset_id: &'a Option<TrainingDatasetId>,
    action: ModelGovernanceAction,
    actor_user_id: &'a Option<UserId>,
    actor_username: &'a str,
    actor_role: &'a Option<RoleCode>,
    reason: &'a str,
    before_status: PublicationStatus,
    after_status: PublicationStatus,
    detail: &'a ModelGovernanceAuditDetail,
}

/// A shared gate evaluation: the report plus the shadow summary, which is
/// present only for shadow-gated intents (`Publish` / `AutoExecution`) and
/// feeds the publish audit's shadow-overlap evidence.
///
struct GateEvaluation {
    report: QualityGateReport,
    shadow_summary: Option<ShadowStabilitySummary>,
}

impl ModelGovernanceService {
    /// Assemble the service from its dependencies.
    #[must_use]
    pub const fn new(deps: ModelGovernanceDeps) -> Self {
        Self { deps }
    }

    /// Resolve a model version or fail with a governance not-found error.
    async fn find_version(&self, id: &ModelVersionId) -> QuantResult<ModelVersionInfo> {
        self.deps
            .model_registry_repo
            .find_model_version(id)
            .await?
            .ok_or_else(|| {
                GovernanceError::NotFound {
                    entity: "model_version",
                    id: id.to_string(),
                }
                .into()
            })
    }

    /// Re-scan a frozen dataset Parquet for point-in-time leakage before publish.
    async fn rescan_leakage(&self, dataset: &TrainingDatasetInfo) -> QuantResult<LeakageFindings> {
        let materialization = training_dataset::require_dataset_materialization(dataset)?;
        let bytes = self
            .deps
            .artifact_store
            .get(materialization.parquet_uri)
            .await?;
        let examples = training_dataset::verify_frozen_dataset_artifact(dataset, &bytes)?;
        scan_future_leakage(&examples)
    }

    /// Aggregate the shadow stability for a publish candidate over the required
    /// window, returning the effective stability the gate should evaluate.
    async fn shadow_stability(
        &self,
        shadow_version_id: &ModelVersionId,
        required_window_secs: u64,
    ) -> QuantResult<(Option<Probability>, ShadowStabilitySummary)> {
        let now = Utc::now();
        // Look back well past the required window so `window_start` reflects the
        // shadow's earliest observation, not just the recent slice.
        let lookback = checked_shadow_stability_lookback(required_window_secs)?;
        let since = now
            .checked_sub_signed(Duration::seconds(checked_seconds(
                lookback,
                "quality_gate.shadow_stability_window_secs",
            )?))
            .ok_or_else(|| GovernanceError::IllegalTransition {
                detail: format!(
                    "quality_gate.shadow_stability_window_secs={lookback} overflows chrono range"
                ),
            })?;
        let summary = self
            .deps
            .shadow_comparison_repo
            .summary(shadow_version_id, since)
            .await?;
        let stability = effective_stability(&summary, required_window_secs, now);
        Ok((stability, summary))
    }

    /// Persist a governance audit row (best-effort id minted in-process).
    async fn write_audit(&self, audit: NewModelGovernanceAudit) -> QuantResult<()> {
        self.deps
            .governance_audit_repo
            .create(audit)
            .await
            .map(|_| ())
            .map_err(QuantError::from)
    }

    fn seal_exact_audit(
        mut audit: NewModelGovernanceAudit,
    ) -> QuantResult<NewModelGovernanceAudit> {
        let identity = GovernanceAuditIdentity {
            model_version_id: &audit.model_version_id,
            training_dataset_id: &audit.training_dataset_id,
            action: audit.action,
            actor_user_id: &audit.actor_user_id,
            actor_username: &audit.actor_username,
            actor_role: &audit.actor_role,
            reason: &audit.reason,
            before_status: audit.before_status,
            after_status: audit.after_status,
            detail: &audit.detail,
        };
        let hash = CanonicalDigest::content_hash_typed(
            GOVERNANCE_AUDIT_IDENTITY_DOMAIN,
            GOVERNANCE_AUDIT_IDENTITY_VERSION,
            &identity,
        )?;
        audit.audit_id = ModelGovernanceAuditId::from_content_hash(&hash);
        audit.audit_event_id = AuditEventId::from_content_hash(&hash);
        Ok(audit)
    }

    async fn write_exact_audit(&self, audit: NewModelGovernanceAudit) -> QuantResult<()> {
        self.deps
            .governance_audit_repo
            .append_exact(Self::seal_exact_audit(audit)?)
            .await
            .map(|_| ())
            .map_err(QuantError::from)
    }

    async fn require_path_binding_audit(
        &self,
        version: &ModelVersionInfo,
        request: &BindPublishPathSetRequest,
        actor: &GovernanceActor,
        path_set_hash: ContentHash,
    ) -> QuantResult<()> {
        let audits = self
            .deps
            .governance_audit_repo
            .list_by_version(&version.model_version_id)
            .await?;
        let latest = audits
            .iter()
            .find(|audit| audit.action == ModelGovernanceAction::BindPublishPathSet);
        let exact = latest.is_some_and(|audit| {
            audit.model_version_id == Some(version.model_version_id)
                && audit.training_dataset_id == version.training_dataset_id
                && audit.actor_user_id == actor.user_id
                && audit.actor_username == actor.username
                && audit.actor_role == actor.role
                && audit.reason == request.reason
                && audit.before_status == version.publication_status
                && audit.after_status == version.publication_status
                && matches!(
                    audit.detail,
                    ModelGovernanceAuditDetail::BindPublishPathSet {
                        path_set_id,
                        path_set_hash: stored_hash,
                        ..
                    } if path_set_id == request.path_set_id && stored_hash == path_set_hash
                )
        });
        if !exact {
            return Err(GovernanceError::IllegalTransition {
                detail: format!(
                    "model {} is bound to path set {} without its exact latest governance audit",
                    version.model_version_id, request.path_set_id
                ),
            }
            .into());
        }
        Ok(())
    }

    fn seal_calibrated_artifact(
        artifact: &ModelArtifact,
        request: &BindCalibrationRequest,
        calibrator_hash: ContentHash,
    ) -> QuantResult<ModelArtifact> {
        let ModelPayload::WeightedFactor(weighted) = artifact.payload() else {
            return Err(GovernanceError::IllegalTransition {
                detail: "bind_calibration requires a weighted-factor buy model artifact".into(),
            }
            .into());
        };

        let new_version_id = ModelVersionId::from_calibration_binding(
            artifact
                .header()
                .serving_contract()
                .bindings()
                .model
                .model_version_id,
            request.calibrator_ref,
            request.downside_source,
        );
        let mut weighted = weighted.as_ref().clone();
        weighted.return_model = ReturnModelSpec::Calibrated(CalibratedReturnModel {
            calibrator_ref: request.calibrator_ref,
            downside_source: request.downside_source,
        });
        let payload = ModelPayload::WeightedFactor(Box::new(weighted));
        let mut bindings = artifact.header().serving_contract().bindings().clone();
        bindings.model.model_version_id = new_version_id;
        bindings.model.estimator = payload.serving_estimator_binding(&bindings.factors.plane)?;
        bindings.model.calibration = Some(ModelServingCalibrationArtifactRef {
            artifact_id: request.calibrator_ref,
            kind: CalibrationKind::ModelScore,
            content_hash: calibrator_hash,
        });
        let serving_contract = ModelServingContract::try_seal(bindings).map_err(|error| {
            GovernanceError::IllegalTransition {
                detail: format!("cannot seal calibrated model serving contract: {error}"),
            }
        })?;
        ModelArtifact::try_seal(serving_contract, payload)
    }

    /// Match persisted Sell metrics to the exact sealed serving lineage.
    fn validate_sell_lineage(
        version: &ModelVersionInfo,
        artifact_lineage: &ModelArtifactTrainingLineage,
    ) -> QuantResult<()> {
        let contract = version.verified_serving_contract().map_err(|error| {
            GovernanceError::IllegalTransition {
                detail: format!(
                    "sell model version {} has an invalid persisted serving contract: {error}",
                    version.model_version_id
                ),
            }
        })?;
        let bindings = contract.bindings();
        let ModelServingEstimatorBinding::FactorNative { ordered_inputs, .. } =
            &bindings.model.estimator
        else {
            return Err(GovernanceError::IllegalTransition {
                detail: format!(
                    "sell model version {} must bind a factor-native estimator",
                    version.model_version_id
                ),
            }
            .into());
        };
        let ModelArtifactTrainingLineage::FactorNative {
            training_dataset_hash,
            training_input_hash,
            input_contract_hash,
            input_transform_hash,
            factor_inputs,
        } = artifact_lineage
        else {
            return Err(GovernanceError::IllegalTransition {
                detail: format!(
                    "sell model version {} metrics require factor-native artifact lineage",
                    version.model_version_id
                ),
            }
            .into());
        };
        let transform = &bindings.transform;
        if *training_dataset_hash != transform.training_dataset_hash
            || *training_input_hash != transform.training_input_hash
            || *input_contract_hash != transform.input_contract_hash
            || *input_transform_hash != transform.input_transform_hash
        {
            return Err(GovernanceError::IllegalTransition {
                detail: format!(
                    "sell model version {} metrics hashes differ from its exact serving transform",
                    version.model_version_id
                ),
            }
            .into());
        }
        let mut expected_factors = Vec::new();
        for input in ordered_inputs {
            let ModelServingEstimatorInput::GovernedFactor {
                factor_definition_id,
            } = input
            else {
                continue;
            };
            let factor = bindings
                .factors
                .plane
                .definitions()
                .iter()
                .find(|revision| revision.factor_definition_id() == *factor_definition_id)
                .ok_or_else(|| GovernanceError::IllegalTransition {
                    detail: format!(
                        "sell model version {} estimator references unknown factor definition {factor_definition_id}",
                        version.model_version_id
                    ),
                })?;
            expected_factors.push(factor.factor_name().clone());
        }
        if factor_inputs != &expected_factors {
            return Err(GovernanceError::IllegalTransition {
                detail: format!(
                    "sell model version {} metrics factor lineage differs from its exact estimator order",
                    version.model_version_id
                ),
            }
            .into());
        }
        Ok(())
    }

    /// Require typed, matching Sell training evidence before governance.
    fn validate_sell_fit(version: &ModelVersionInfo) -> QuantResult<()> {
        if version.model_family != ModelFamily::HoldVsExitWeighted {
            return Ok(());
        }
        if version.training_objective.format_version != MODEL_TRAINING_OBJECTIVE_FORMAT_VERSION
            || version.metrics.format_version != MODEL_VERSION_METRICS_FORMAT_VERSION
        {
            return Err(GovernanceError::IllegalTransition {
                detail: format!(
                    "sell model version {} has unsupported training provenance formats",
                    version.model_version_id
                ),
            }
            .into());
        }
        let (
            ModelTrainingObjectiveDefinition::GovernedSellEstimator {
                fit_status: objective_status,
            },
            ModelVersionMetricsDefinition::GovernedSellEstimator {
                preparation,
                artifact_lineage,
            },
        ) = (
            &version.training_objective.definition,
            &version.metrics.definition,
        )
        else {
            return Err(GovernanceError::IllegalTransition {
                detail: format!(
                    "sell model version {} requires matching governed Sell objective and metrics",
                    version.model_version_id
                ),
            }
            .into());
        };
        if objective_status != &preparation.fit_status {
            return Err(GovernanceError::IllegalTransition {
                detail: format!(
                    "sell model version {} objective and metrics fit statuses disagree",
                    version.model_version_id
                ),
            }
            .into());
        }
        Self::validate_sell_lineage(version, artifact_lineage)?;
        match objective_status {
            GovernedSellFitStatus::OofPredictionsRequired => {
                Err(ResearchError::SellOofEstimatorRequired.into())
            }
        }
    }
}

#[async_trait]
impl ModelGovernancePort for ModelGovernanceService {
    async fn publish(
        &self,
        command: PublishModelCommand,
        actor: GovernanceActor,
    ) -> QuantResult<ModelVersionInfo> {
        let version = self.find_version(&command.model_version_id).await?;
        if !matches!(
            version.publication_status,
            PublicationStatus::Candidate | PublicationStatus::Shadow
        ) {
            return Err(GovernanceError::IllegalTransition {
                detail: format!(
                    "cannot publish version {} in status {}",
                    version.model_version_id,
                    version.publication_status.as_str()
                ),
            }
            .into());
        }
        Self::validate_sell_fit(&version)?;
        if version.training_dataset_id.is_none() {
            return Err(GovernanceError::IllegalTransition {
                detail: format!(
                    "model version {} has no linked training_dataset_id; publishing requires an immutable frozen dataset",
                    version.model_version_id
                ),
            }
            .into());
        }

        // The subject-bound proof is checked before latch resolution. On the
        // first publish it is the only evidence allowed to mint the initial
        // clear generation inside the atomic registry transaction.
        let parity_run = self
            .deps
            .frozen_model_parity
            .verify_and_record(
                &version,
                "model_publish",
                "pre-publication full frozen dataset/model parity",
            )
            .await?;
        let required_window = self
            .deps
            .runtime_config
            .current()
            .profile_artifacts
            .research_method
            .model_promotion
            .required_shadow_window_secs;
        let evaluation = Box::pin(self.evaluate_gate(&version, GateIntent::Publish, None)).await?;
        let report = evaluation.report;
        let summary = evaluation.shadow_summary.ok_or_else(|| {
            QuantError::from(GovernanceError::IllegalTransition {
                detail: "publish gate did not evaluate shadow stability".to_owned(),
            })
        })?;

        self.deps
            .model_registry_repo
            .set_quality_gate_report(&version.model_version_id, report.clone())
            .await?;

        if !report.passed {
            return Err(map_publish_gate_failure(
                &report.hard_failures,
                &version.model_version_id,
            ));
        }

        self.commit_publish(PublishCommit {
            version,
            parity_run_id: parity_run.run_id,
            report,
            summary,
            required_window,
            command,
            actor,
        })
        .await
    }

    async fn retire(
        &self,
        command: RetireModelCommand,
        actor: GovernanceActor,
    ) -> QuantResult<ModelVersionInfo> {
        let version = self.find_version(&command.model_version_id).await?;
        if version.publication_status != PublicationStatus::Published {
            return Err(GovernanceError::IllegalTransition {
                detail: format!(
                    "cannot retire version {} in status {}",
                    version.model_version_id,
                    version.publication_status.as_str()
                ),
            }
            .into());
        }
        if model_is_routed(
            &self.deps.runtime_config.current(),
            &version.model_version_id,
        ) {
            return Err(GovernanceError::IllegalTransition {
                detail: format!(
                    "cannot retire routed model {}; activate a ModelRouting revision without this artifact first",
                    version.model_version_id
                ),
            }
            .into());
        }

        let before_status = version.publication_status;
        let retired = self
            .deps
            .model_registry_repo
            .retire_model_version(&version.model_version_id)
            .await?;

        self.write_audit(NewModelGovernanceAudit {
            audit_id: ModelGovernanceAuditId::from_v7(),
            model_version_id: Some(retired.model_version_id),
            training_dataset_id: None,
            action: ModelGovernanceAction::Retire,
            actor_user_id: actor.user_id,
            actor_username: actor.username,
            actor_role: actor.role,
            reason: command.reason,
            before_status,
            after_status: retired.publication_status,
            detail: ModelGovernanceAuditDetail::Retire {
                retired_version_id: retired.model_version_id,
                artifact_hash: retired.artifact_hash,
            },
            audit_event_id: AuditEventId::from_v7(),
        })
        .await?;

        Ok(retired)
    }

    async fn preview_gate(
        &self,
        model_version_id: &ModelVersionId,
        intent: GatePreviewIntent,
        backtest_report_id: Option<&BacktestReportId>,
    ) -> QuantResult<QualityGateReportView> {
        let version = self.find_version(model_version_id).await?;
        let gate_intent = match intent {
            GatePreviewIntent::Candidate => GateIntent::Candidate,
            GatePreviewIntent::Publish => GateIntent::Publish,
            GatePreviewIntent::AutoExecution => GateIntent::AutoExecution,
        };
        let evaluation =
            Box::pin(self.evaluate_gate(&version, gate_intent, backtest_report_id)).await?;
        Ok(gate_report_view(&evaluation.report))
    }

    async fn bind_calibration(
        &self,
        model_version_id: &ModelVersionId,
        request: BindCalibrationRequest,
        actor: GovernanceActor,
    ) -> QuantResult<ModelVersionInfo> {
        let version = self.find_version(model_version_id).await?;
        if !matches!(
            version.publication_status,
            PublicationStatus::Candidate | PublicationStatus::Shadow
        ) {
            return Err(GovernanceError::IllegalTransition {
                detail: format!(
                    "cannot bind calibration on version {} in status {}",
                    version.model_version_id,
                    version.publication_status.as_str()
                ),
            }
            .into());
        }

        let source = self.deps.serving_preimages.load(&version).await?;
        let calibrator = self
            .load_score_calibrator(&request.calibrator_ref, &source)
            .await?;
        let calibrated = self
            .store_calibrated_artifact(&source, &request, calibrator.content_hash())
            .await?;
        Box::pin(self.persist_calibrated_version(&version, &request, calibrated, actor)).await
    }

    async fn bind_publish_path_set(
        &self,
        model_version_id: &ModelVersionId,
        request: BindPublishPathSetRequest,
        actor: GovernanceActor,
    ) -> QuantResult<ModelVersionInfo> {
        let version = self.find_version(model_version_id).await?;
        if !matches!(
            version.publication_status,
            PublicationStatus::Candidate | PublicationStatus::Shadow
        ) {
            return Err(GovernanceError::IllegalTransition {
                detail: format!(
                    "cannot bind publish path set on version {} in status {}",
                    version.model_version_id,
                    version.publication_status.as_str()
                ),
            }
            .into());
        }
        let path_set = self
            .deps
            .backtest_path_set_repo
            .find_by_id(&request.path_set_id)
            .await
            .map_err(QuantError::from)?
            .ok_or_else(|| GovernanceError::NotFound {
                entity: "backtest_path_set",
                id: request.path_set_id.to_string(),
            })?;
        path_set
            .verify_hash()
            .map_err(|error| GovernanceError::IllegalTransition {
                detail: format!(
                    "path set {} failed immutable evidence verification: {error}",
                    request.path_set_id
                ),
            })?;
        if path_set.model_version_id != version.model_version_id {
            return Err(GovernanceError::IllegalTransition {
                detail: format!(
                    "path set {} belongs to model version {}, not {}",
                    request.path_set_id, path_set.model_version_id, version.model_version_id
                ),
            }
            .into());
        }
        let before = version.publish_path_set_id;
        if before == Some(request.path_set_id) {
            self.require_path_binding_audit(&version, &request, &actor, path_set.path_set_hash)
                .await?;
            return Ok(version);
        }
        let audit = Self::seal_exact_audit(NewModelGovernanceAudit {
            audit_id: ModelGovernanceAuditId::from_v7(),
            model_version_id: Some(version.model_version_id),
            training_dataset_id: version.training_dataset_id,
            action: ModelGovernanceAction::BindPublishPathSet,
            actor_user_id: actor.user_id,
            actor_username: actor.username,
            actor_role: actor.role,
            reason: request.reason,
            before_status: version.publication_status,
            after_status: version.publication_status,
            detail: ModelGovernanceAuditDetail::BindPublishPathSet {
                previous_path_set_id: before,
                path_set_id: request.path_set_id,
                path_set_hash: path_set.path_set_hash,
            },
            audit_event_id: AuditEventId::from_v7(),
        })?;
        self.deps
            .model_registry_repo
            .commit_publish_path_binding(BindPublishPathSetCommit {
                model_version_id: version.model_version_id,
                expected_path_set_id: before,
                path_set_id: request.path_set_id,
                audit,
            })
            .await
            .map_err(Into::into)
    }
}

struct PublishCommit {
    version: ModelVersionInfo,
    parity_run_id: FeatureParityRunId,
    report: QualityGateReport,
    summary: ShadowStabilitySummary,
    required_window: u64,
    command: PublishModelCommand,
    actor: GovernanceActor,
}

fn model_is_routed(snapshot: &DecisionPolicySnapshot, model_version_id: &ModelVersionId) -> bool {
    let routing = &snapshot.model_routing.model;
    routing
        .active_model_version_id
        .iter()
        .chain(routing.shadow_model_version_id.iter())
        .chain(routing.active_exit_model_version_id.iter())
        .chain(routing.category_model_pointers.values())
        .any(|reference| reference.id == *model_version_id)
}

impl ModelGovernanceService {
    async fn commit_publish(&self, input: PublishCommit) -> QuantResult<ModelVersionInfo> {
        let PublishCommit {
            version,
            parity_run_id,
            report,
            summary,
            required_window,
            command,
            actor,
        } = input;
        let before_status = version.publication_status;
        let feature_parity_state_id = self
            .deps
            .feature_parity_gate
            .publish_state_id("model publish commit")
            .await?;
        let feature_parity_permit = match feature_parity_state_id {
            Some(state_id) => PublishFeatureParityPermit::ExistingGeneration(state_id),
            None => PublishFeatureParityPermit::InitializeFromProof {
                actor: actor.username.clone(),
                acting_role: actor.role.clone(),
                reason: format!(
                    "initialize parity latch from pre-publication proof {}: {}",
                    parity_run_id, command.reason
                ),
            },
        };
        let outcome = self
            .deps
            .model_registry_repo
            .publish_model_version(PublishModelVersionCommit {
                model_spec_id: &version.model_spec_id,
                model_version_id: &version.model_version_id,
                feature_parity_permit,
                feature_parity_run_id: &parity_run_id,
            })
            .await?;
        let published = outcome.published;
        let feature_parity_state_id = outcome.feature_parity_state_id;

        self.write_audit(NewModelGovernanceAudit {
            audit_id: ModelGovernanceAuditId::from_v7(),
            model_version_id: Some(published.model_version_id),
            training_dataset_id: None,
            action: ModelGovernanceAction::Publish,
            actor_user_id: actor.user_id,
            actor_username: actor.username,
            actor_role: actor.role,
            reason: command.reason,
            before_status,
            after_status: published.publication_status,
            detail: ModelGovernanceAuditDetail::Publish {
                artifact_hash: published.artifact_hash,
                gate_report_hash: report.report_hash,
                shadow_samples: summary.sample_count,
                shadow_mean_overlap: summary.mean_topn_overlap,
                feature_parity_state_id,
                required_shadow_window_secs: checked_seconds(
                    required_window,
                    "quality_gate.shadow_stability_window_secs",
                )?,
            },
            audit_event_id: AuditEventId::from_v7(),
        })
        .await?;

        Ok(published)
    }

    async fn load_score_calibrator(
        &self,
        calibrator_ref: &CalibrationArtifactId,
        source: &VerifiedModelServingPreimage,
    ) -> QuantResult<VerifiedModelScoreCalibration> {
        let info = self
            .deps
            .calibration_repo
            .find_by_id(calibrator_ref)
            .await?
            .ok_or_else(|| GovernanceError::NotFound {
                entity: "calibration_artifact",
                id: calibrator_ref.to_string(),
            })?;
        self.deps
            .serving_preimages
            .verify_calibrator(source, &info)
            .await
    }

    async fn store_calibrated_artifact(
        &self,
        source: &VerifiedModelServingPreimage,
        request: &BindCalibrationRequest,
        calibrator_hash: ContentHash,
    ) -> QuantResult<ModelArtifact> {
        let calibrated =
            Self::seal_calibrated_artifact(source.artifact(), request, calibrator_hash)?;
        let artifact_hash = calibrated.content_hash()?;
        self.deps
            .artifact_store
            .put(
                ModelArtifact::artifact_key(&artifact_hash)?,
                &calibrated.to_bytes()?,
            )
            .await?;
        Ok(calibrated)
    }

    async fn persist_calibrated_version(
        &self,
        version: &ModelVersionInfo,
        request: &BindCalibrationRequest,
        calibrated: ModelArtifact,
        actor: GovernanceActor,
    ) -> QuantResult<ModelVersionInfo> {
        let artifact_hash = calibrated.content_hash()?;
        let serving_contract = calibrated.header().serving_contract().clone();
        let bindings = serving_contract.bindings();
        let model = &bindings.model;
        let new_version_id = model.model_version_id;
        let model_spec_id = model.model_spec_id;
        let category_scope = model.category_scope;
        let profile_ref = model.profile_ref.clone();
        let training_dataset_id = Some(bindings.dataset.manifest.training_dataset_id);
        let trade_policy = bindings
            .trade_policy
            .as_ref()
            .map(|binding| (binding.artifact_id, binding.content_hash));
        let next = self
            .deps
            .model_registry_repo
            .next_version_for_spec(&model_spec_id)
            .await?;
        let candidate = NewModelVersion {
            model_version_id: new_version_id,
            model_spec_id,
            version: next,
            artifact_hash,
            serving_contract,
            category_scope,
            profile_ref,
            training_dataset_id,
            trade_policy_artifact_id: trade_policy.map(|(artifact_id, _)| artifact_id),
            trade_policy_hash: trade_policy.map(|(_, content_hash)| content_hash),
            publish_path_set_id: None,
            derivation: ModelVersionDerivation::ReturnCalibration {
                parent_model_version_id: version.model_version_id,
                calibration_artifact_id: request.calibrator_ref,
            },
            metrics: version.metrics.clone(),
            training_objective: version.training_objective.clone(),
            quality_gate_report: None,
            publication_status: PublicationStatus::Candidate,
            published_at: None,
            retired_at: None,
        };
        let created = self.create_calibrated_version(candidate).await?;

        self.deps
            .frozen_model_parity
            .verify_and_record(
                &created,
                "model_calibration_binding",
                "full frozen parity for calibrated model artifact",
            )
            .await?;

        // Activate the bound calibrator under the shared `active` governance:
        // a `Calibrated` return model's `calibrator_ref` must resolve
        // through `CoreCalibrationArtifactLoader`, which fails closed on
        // `active == false`. `model_score` has no cross-model exclusivity, so
        // this never deactivates another model version's calibrator.
        self.deps
            .calibration_repo
            .mark_active(&request.calibrator_ref)
            .await?;

        self.write_exact_audit(NewModelGovernanceAudit {
            audit_id: ModelGovernanceAuditId::from_v7(),
            model_version_id: Some(created.model_version_id),
            training_dataset_id: None,
            action: ModelGovernanceAction::BindCalibration,
            actor_user_id: actor.user_id,
            actor_username: actor.username,
            actor_role: actor.role,
            reason: request.reason.clone(),
            before_status: version.publication_status,
            after_status: created.publication_status,
            detail: ModelGovernanceAuditDetail::BindCalibration {
                source_version_id: version.model_version_id,
                source_artifact_hash: version.artifact_hash,
                calibrated_version_id: created.model_version_id,
                calibrated_artifact_hash: created.artifact_hash,
                calibrator_id: request.calibrator_ref,
            },
            audit_event_id: AuditEventId::from_v7(),
        })
        .await?;

        Ok(created)
    }

    async fn create_calibrated_version(
        &self,
        candidate: NewModelVersion,
    ) -> QuantResult<ModelVersionInfo> {
        if let Some(stored) = self
            .deps
            .model_registry_repo
            .find_model_version(&candidate.model_version_id)
            .await?
        {
            return Self::require_calibrated_retry(&candidate, stored);
        }
        match self
            .deps
            .model_registry_repo
            .create_model_version(candidate.clone())
            .await
        {
            Ok(created) => Ok(created),
            Err(error) => {
                let stored = self
                    .deps
                    .model_registry_repo
                    .find_model_version(&candidate.model_version_id)
                    .await?;
                stored.map_or_else(
                    || Err(error.into()),
                    |stored| Self::require_calibrated_retry(&candidate, stored),
                )
            }
        }
    }

    fn require_calibrated_retry(
        candidate: &NewModelVersion,
        stored: ModelVersionInfo,
    ) -> QuantResult<ModelVersionInfo> {
        let contract_hash = candidate.serving_contract_hash().map_err(|error| {
            GovernanceError::IllegalTransition {
                detail: format!("invalid calibrated candidate contract: {error}"),
            }
        })?;
        let exact = stored.model_version_id == candidate.model_version_id
            && stored.model_spec_id == candidate.model_spec_id
            && stored.artifact_hash == candidate.artifact_hash
            && stored.serving_contract == candidate.serving_contract
            && stored.serving_contract_hash == contract_hash
            && stored.category_scope == candidate.category_scope
            && stored.profile_ref == candidate.profile_ref
            && stored.training_dataset_id == candidate.training_dataset_id
            && stored.trade_policy_artifact_id == candidate.trade_policy_artifact_id
            && stored.trade_policy_hash == candidate.trade_policy_hash
            && stored.publish_path_set_id == candidate.publish_path_set_id
            && stored
                .verified_derivation()
                .is_ok_and(|derivation| derivation == candidate.derivation)
            && stored.metrics == candidate.metrics
            && stored.training_objective == candidate.training_objective
            && stored.quality_gate_report == candidate.quality_gate_report
            && stored.publication_status == candidate.publication_status
            && stored.published_at == candidate.published_at
            && stored.retired_at == candidate.retired_at;
        if !exact {
            return Err(GovernanceError::IllegalTransition {
                detail: format!(
                    "calibrated model identity {} was reused with semantic drift",
                    candidate.model_version_id
                ),
            }
            .into());
        }
        Ok(stored)
    }

    /// Evaluate the quality gate for a model version against the active
    /// thresholds. The single evaluation path shared by `publish` (which then
    /// persists + advances) and `preview_gate` (read-only dry-run). Shadow
    /// stability is fetched only when the intent gates on it.
    async fn evaluate_gate(
        &self,
        version: &ModelVersionInfo,
        intent: GateIntent,
        backtest_report_id: Option<&BacktestReportId>,
    ) -> QuantResult<GateEvaluation> {
        Self::validate_sell_fit(version)?;
        let config = self.deps.runtime_config.current();
        let required_window = config
            .profile_artifacts
            .research_method
            .model_promotion
            .required_shadow_window_secs;
        let thresholds =
            thresholds_from_config(&config.profile_artifacts.research_method.model_promotion);
        let validation_thresholds = validation_thresholds_from_config(
            &config
                .profile_artifacts
                .research_method
                .research
                .validation
                .gates,
        );
        let sell_thresholds =
            sell_thresholds_from_config(&config.profile_artifacts.research_method.model_promotion);
        let model_family = Self::model_family_for_version(version);

        let serving_preimage = self.deps.serving_preimages.load(version).await?;
        let artifact = serving_preimage.artifact();
        // Deep check: the same function
        // report/admission/intent-creation share, not the shallow enum-tag
        // read `ModelArtifact::return_model_is_calibrated` used to be — a
        // calibrator deactivated after `bind_calibration` must fail the
        // publish gate too, not just downstream consumers.
        let return_model_calibrated =
            resolve_return_model_calibration(self.deps.calibration_loader.as_ref(), artifact)
                .await?
                .is_some();

        let backtest = match backtest_report_id {
            Some(id) => self.backtest_by_id(id).await?,
            None => self.latest_backtest(&version.model_version_id).await?,
        };
        let dataset = self.dataset_coverage(version).await?;
        // Publish/promote must rescan real Parquet provenance (#9). A version
        // without `training_dataset_id` cannot be rescanned — fail closed
        // rather than inventing empty `LeakageFindings` that would pass
        // `NoPitLeakage`.
        let dataset_id = version.training_dataset_id.as_ref().ok_or_else(|| {
            GovernanceError::IllegalTransition {
                detail: format!(
                    "model version {} has no linked training_dataset_id; \
                     publish/promote requires a frozen dataset for leakage rescan",
                    version.model_version_id
                ),
            }
        })?;
        let dataset_info = self
            .deps
            .dataset_repo
            .find_by_id(dataset_id)
            .await?
            .ok_or_else(|| GovernanceError::NotFound {
                entity: "training_dataset",
                id: dataset_id.to_string(),
            })?;
        let leakage = self.rescan_leakage(&dataset_info).await?;
        // Sell (`HoldVsExitWeighted`) publish/promote requires
        // the same bound CPCV path set as Buy — `bound_path_set` is already
        // family-agnostic (it only reads `version.publish_path_set_id`), so
        // no `is_exit` branch is needed here at all.
        let path_set = if intent.requires_backtest() {
            self.bound_path_set(version).await?
        } else {
            None
        };
        let (shadow_stability, shadow_summary) = if intent.requires_shadow_stability() {
            let (stability, summary) = self
                .shadow_stability(&version.model_version_id, required_window)
                .await?;
            (stability, Some(summary))
        } else {
            (None, None)
        };

        let decision = self.deps.gate.evaluate(QualityGateInput {
            subject: GateSubject::ModelVersion(version.model_version_id),
            intent,
            backtest,
            dataset,
            leakage,
            shadow_stability,
            thresholds,
            validation_thresholds,
            path_set,
            sell_thresholds,
            model_family: Some(model_family),
            return_model_calibrated,
        })?;
        Ok(GateEvaluation {
            report: decision.report().clone(),
            shadow_summary,
        })
    }

    /// Reconstruct the most recent backtest report for a version, if any.
    async fn latest_backtest(
        &self,
        version_id: &ModelVersionId,
    ) -> QuantResult<Option<BacktestReport>> {
        let reports = self
            .deps
            .backtest_report_repo
            .list_by_model_version(version_id)
            .await?;
        match reports.into_iter().next() {
            Some(info) => Ok(Some(backtest_report_from_info(info)?)),
            None => Ok(None),
        }
    }

    /// Reconstruct a specific frozen backtest report by id, if it exists.
    async fn backtest_by_id(
        &self,
        backtest_report_id: &BacktestReportId,
    ) -> QuantResult<Option<BacktestReport>> {
        match self
            .deps
            .backtest_report_repo
            .find_by_id(backtest_report_id)
            .await?
        {
            Some(info) => Ok(Some(backtest_report_from_info(info)?)),
            None => Ok(None),
        }
    }

    /// Resolve the CPCV path set bound for publish gates.
    ///
    /// Only an explicitly bound `publish_path_set_id` is accepted — never an
    /// implicit "latest" row (a failed re-run must not silently replace a
    /// passing candidate).
    async fn bound_path_set(
        &self,
        version: &ModelVersionInfo,
    ) -> QuantResult<Option<CpcvPathSetGateInput>> {
        let Some(path_set_id) = &version.publish_path_set_id else {
            return Ok(None);
        };
        let Some(info) = self
            .deps
            .backtest_path_set_repo
            .find_by_id(path_set_id)
            .await
            .map_err(QuantError::from)?
        else {
            return Err(GovernanceError::NotFound {
                entity: "backtest_path_set",
                id: path_set_id.to_string(),
            }
            .into());
        };
        info.verify_hash()
            .map_err(|error| GovernanceError::IllegalTransition {
                detail: format!(
                    "bound publish_path_set_id {path_set_id} failed immutable evidence \
                     verification: {error}"
                ),
            })?;
        if info.model_version_id != version.model_version_id {
            return Err(GovernanceError::IllegalTransition {
                detail: format!(
                    "bound publish_path_set_id {path_set_id} belongs to model version {}, not {}",
                    info.model_version_id, version.model_version_id
                ),
            }
            .into());
        }
        Ok(Some(path_set_gate_input(&info)))
    }

    /// Resolve the dataset coverage backing a model version (the gate's
    /// coverage / label inputs). A version without a training dataset yields an
    /// empty coverage, which the gate treats as failing.
    async fn dataset_coverage(&self, version: &ModelVersionInfo) -> QuantResult<DatasetCoverage> {
        let Some(dataset_id) = &version.training_dataset_id else {
            return Ok(DatasetCoverage::default());
        };
        let Some(dataset) = self.deps.dataset_repo.find_by_id(dataset_id).await? else {
            return Ok(DatasetCoverage::default());
        };
        let materialization = training_dataset::require_dataset_materialization(&dataset)?;
        Ok(materialization.coverage.clone())
    }

    /// Resolve the governed model family for a version (JOIN-projected on the row).
    const fn model_family_for_version(version: &ModelVersionInfo) -> ModelFamily {
        version.model_family
    }
}

/// Project a research [`QualityGateReport`] onto the read-only wire view. Lives
/// in core because it is the only layer that sees both the research report and
/// the models-crate view (models must not depend on research).
fn gate_report_view(report: &QualityGateReport) -> QualityGateReportView {
    QualityGateReportView {
        intent: report.intent.wire_name().to_owned(),
        evaluated_at: report.evaluated_at,
        passed: report.passed,
        gates: report.gates.iter().map(gate_outcome_view).collect(),
        report_hash: report.report_hash.to_string(),
    }
}

/// Project one [`GateOutcome`] onto its wire row (stable `snake_case` strings).
fn gate_outcome_view(outcome: &GateOutcome) -> GateOutcomeView {
    GateOutcomeView {
        gate: outcome.gate.wire_name().to_owned(),
        class: outcome.class.wire_name().to_owned(),
        status: outcome.status.wire_name().to_owned(),
        observed: outcome.observed.clone(),
        threshold: outcome.threshold.clone(),
        detail: outcome.detail.clone(),
    }
}

/// Render a hard-failure list into a compact, audit-friendly summary.
fn render_failures(failures: &[QualityGateFailure]) -> String {
    failures
        .iter()
        .map(|failure| {
            format!(
                "{:?}(observed={}, threshold={})",
                failure.gate, failure.observed, failure.threshold
            )
        })
        .collect::<Vec<_>>()
        .join("; ")
}

/// Map a blocked publish gate to the most specific governance error variant.
fn map_publish_gate_failure(
    hard_failures: &[QualityGateFailure],
    model_version_id: &ModelVersionId,
) -> QuantError {
    if hard_failures
        .iter()
        .any(|failure| failure.gate == GateId::ShadowOverlapStability)
    {
        return GovernanceError::ShadowNotStable {
            detail: render_failures(hard_failures),
        }
        .into();
    }
    GovernanceError::QualityGateFailed {
        entity: "model_version",
        id: model_version_id.to_string(),
        failures: render_failures(hard_failures),
    }
    .into()
}

/// The effective shadow stability the publish gate evaluates: `None` (fails the
/// gate) unless the shadow has been observed for at least the required window
/// (its earliest comparison is at least that old), has samples, and shows no
/// hard divergence.
fn effective_stability(
    summary: &ShadowStabilitySummary,
    required_window_secs: u64,
    now: DateTime<Utc>,
) -> Option<Probability> {
    if summary.sample_count == 0 || summary.any_hard_divergence {
        return None;
    }
    let start = summary.window_start?;
    let Ok(observed_secs) = u64::try_from(now.signed_duration_since(start).num_seconds()) else {
        return None;
    };
    if observed_secs < required_window_secs {
        return None;
    }
    Some(summary.mean_topn_overlap)
}

const fn parse_threshold(value: &Decimal, _field: &str) -> Decimal {
    *value
}

fn checked_seconds(value: u64, field: &'static str) -> QuantResult<i64> {
    i64::try_from(value).map_err(|error| {
        GovernanceError::IllegalTransition {
            detail: format!("{field} does not fit chrono seconds: {error}"),
        }
        .into()
    })
}

fn checked_shadow_stability_lookback(required_window_secs: u64) -> QuantResult<u64> {
    required_window_secs.checked_mul(8).ok_or_else(|| {
        GovernanceError::NumericOverflow {
            field: "quality_gate.shadow_stability_lookback_secs",
            detail: format!(
                "required shadow window {required_window_secs} cannot be expanded by 8"
            ),
        }
        .into()
    })
}

/// Assemble research [`QualityGateThresholds`] from the governed config section.
const fn thresholds_from_config(config: &QualityGateConfig) -> QualityGateThresholds {
    QualityGateThresholds {
        min_sample_count: config.min_sample_count,
        min_label_coverage: parse_threshold(&config.min_label_coverage.value, "min_label_coverage"),
        min_materialization_coverage: parse_threshold(
            &config.min_materialization_coverage.value,
            "min_materialization_coverage",
        ),
        max_drawdown: parse_threshold(&config.max_drawdown.value, "max_drawdown"),
        min_liquidity_exit_feasibility: parse_threshold(
            &config.min_liquidity_exit_feasibility.value,
            "min_liquidity_exit_feasibility",
        ),
        min_shadow_overlap_stability: parse_threshold(
            &config.min_shadow_overlap_stability.value,
            "min_shadow_overlap_stability",
        ),
        max_category_concentration: parse_threshold(
            &config.max_category_concentration.value,
            "max_category_concentration",
        ),
    }
}

/// Assemble [`ValidationGateThresholds`] from `research.validation.gates`.
const fn validation_thresholds_from_config(
    config: &ResearchValidationGatesConfig,
) -> ValidationGateThresholds {
    ValidationGateThresholds {
        rank_ic_min: parse_threshold(
            &config.rank_ic_min.value,
            "research.validation.gates.rank_ic_min",
        ),
        dsr_significance: parse_threshold(
            &config.dsr_significance.value,
            "research.validation.gates.dsr_significance",
        ),
        max_pbo: parse_threshold(&config.max_pbo.value, "research.validation.gates.max_pbo"),
        max_turnover: parse_threshold(
            &config.max_turnover.value,
            "research.validation.gates.max_turnover",
        ),
        min_tail_loss_bps: parse_threshold(
            &config.min_tail_loss_bps.value,
            "research.validation.gates.min_tail_loss_bps",
        ),
    }
}

const fn path_set_gate_input(info: &BacktestPathSetInfo) -> CpcvPathSetGateInput {
    let distribution = info.sharpe_distribution;
    CpcvPathSetGateInput {
        median_rank_ic: info.median_rank_ic,
        deflated_sharpe: info.deflated_sharpe,
        pbo: info.pbo,
        min_track_record_length_secs: info.min_track_record_length_secs,
        median_max_drawdown: distribution.median_max_drawdown,
        median_tail_loss: distribution.median_tail_loss,
        baseline_uplift: distribution.baseline_uplift,
        window_start: Some(info.window_start),
        window_end: Some(info.window_end),
    }
}

/// Assemble sell-side [`SellQualityGateThresholds`] from the governed config section.
const fn sell_thresholds_from_config(config: &QualityGateConfig) -> SellQualityGateThresholds {
    SellQualityGateThresholds {
        min_sample_count: config.sell.min_sample_count,
        min_label_coverage: parse_threshold(
            &config.sell.min_label_coverage.value,
            "sell.min_label_coverage",
        ),
        rank_ic_min: parse_threshold(&config.sell.rank_ic_min.value, "sell.rank_ic_min"),
        max_pbo: parse_threshold(&config.sell.max_pbo.value, "sell.max_pbo"),
        min_l2_book_fidelity_ratio: parse_threshold(
            &config.sell.min_l2_book_fidelity_ratio.value,
            "sell.min_l2_book_fidelity_ratio",
        ),
        max_fallback_ratio: parse_threshold(
            &config.sell.max_fallback_ratio.value,
            "sell.max_fallback_ratio",
        ),
    }
}

/// Reconstruct a research [`BacktestReport`] from its persisted ledger row.
fn backtest_report_from_info(info: BacktestReportInfo) -> QuantResult<BacktestReport> {
    let expected_vs_realized = info.expected_vs_realized;
    let category_breakdown = info.category_breakdown.into_iter().collect();
    let report_pnl_simulation = info.report_pnl_simulation;
    Ok(BacktestReport {
        backtest_report_id: info.backtest_report_id,
        model_version_id: info.model_version_id,
        dataset_id: info.evaluation_dataset_id,
        decision_policy_snapshot_id: info.decision_policy_snapshot_id,
        window_start: info.window_start,
        window_end: info.window_end,
        coverage: info.coverage,
        sample_count: u64::try_from(info.sample_count).map_err(|error| {
            GovernanceError::IllegalTransition {
                detail: format!("backtest sample_count is negative or invalid: {error}"),
            }
        })?,
        missing_feature_count: u64::try_from(info.missing_feature_count).map_err(|error| {
            GovernanceError::IllegalTransition {
                detail: format!("backtest missing_feature_count is negative or invalid: {error}"),
            }
        })?,
        rank_ic: info.rank_ic,
        sharpe: info.sharpe,
        hit_rate: info.hit_rate,
        expected_vs_realized,
        max_drawdown: info.max_drawdown,
        turnover: info.turnover,
        liquidity_feasibility: info.liquidity_feasibility,
        category_breakdown,
        tail_loss: info.tail_loss,
        report_pnl_simulation,
        report_hash: info.report_hash,
    })
}

#[cfg(test)]
mod path_set_gate_input_tests {
    use chrono::Utc;
    use quant_pivot_models::{
        domain::quant::BacktestPathSetInfo,
        types::{
            BacktestPathSetId, ContentHash, DecisionPolicySnapshotId, ModelRunId, ModelVersionId,
            TrainingDatasetId,
            backtest::{
                BacktestPaths, CpcvFoldArtifact, CpcvFoldArtifacts, CpcvFoldCalibrationPolicy,
                CpcvFoldRole, CpcvMethodologyBinding, CpcvPathSetSubject, SharpeDistribution,
            },
        },
    };
    use rust_decimal_macros::dec;

    use super::{checked_shadow_stability_lookback, path_set_gate_input};

    fn hash() -> ContentHash {
        ContentHash::parse(&format!("blake3:{}", "a".repeat(64))).expect("hash")
    }

    fn info_with_distribution(sharpe_distribution: SharpeDistribution) -> BacktestPathSetInfo {
        BacktestPathSetInfo {
            path_set_id: BacktestPathSetId::from_v7(),
            model_version_id: ModelVersionId::from_v7(),
            model_run_id: ModelRunId::from_v7(),
            training_dataset_id: TrainingDatasetId::from_v7(),
            decision_policy_snapshot_id: DecisionPolicySnapshotId::from_v7(),
            window_start: Utc::now(),
            window_end: Utc::now(),
            subject: CpcvPathSetSubject::new(hash(), hash(), hash(), hash(), hash(), hash()),
            methodology: CpcvMethodologyBinding::new(
                hash(),
                hash(),
                hash(),
                CpcvFoldCalibrationPolicy::SubjectHeuristic {
                    return_model_hash: hash(),
                },
            ),
            fold_artifacts: CpcvFoldArtifacts::try_new(vec![
                CpcvFoldArtifact {
                    role: CpcvFoldRole::Validation,
                    training_groups_hash: hash(),
                    training_group_count: 1,
                    model_artifact_hash: hash(),
                    serving_contract_hash: hash(),
                    model_payload_hash: hash(),
                },
                CpcvFoldArtifact {
                    role: CpcvFoldRole::Trial { trial_id: 0 },
                    training_groups_hash: ContentHash::parse(&format!("blake3:{}", "b".repeat(64)))
                        .expect("trial hash"),
                    training_group_count: 1,
                    model_artifact_hash: hash(),
                    serving_contract_hash: hash(),
                    model_payload_hash: hash(),
                },
            ])
            .expect("fold artifacts"),
            path_count: 3,
            combination_count: 6,
            paths: BacktestPaths::default(),
            sharpe_distribution,
            median_rank_ic: dec!(0.1),
            deflated_sharpe: dec!(0.95),
            dsr_benchmark_sharpe: dec!(0.1),
            pbo: dec!(0.2),
            min_track_record_length_secs: Some(86_400),
            trial_count: 4,
            trial_grid_count: 4,
            coord_search_effective_n: 0,
            path_set_hash: hash(),
            created_at: Utc::now(),
        }
    }

    #[test]
    fn set_gate_decodes_diagnostics() {
        let info = info_with_distribution(SharpeDistribution {
            min: dec!(0),
            p25: dec!(0),
            median: dec!(0.5),
            p75: dec!(1),
            max: dec!(1),
            median_max_drawdown: Some(dec!(0.1)),
            median_tail_loss: Some(dec!(-0.005)),
            baseline_uplift: Some(dec!(0.001)),
        });
        let gate = path_set_gate_input(&info);
        assert_eq!(gate.median_max_drawdown, Some(dec!(0.1)));
        assert_eq!(gate.median_tail_loss, Some(dec!(-0.005)));
        assert_eq!(gate.baseline_uplift, Some(dec!(0.001)));
    }

    #[test]
    fn shadow_stability_rejects_overflow() {
        assert_eq!(
            checked_shadow_stability_lookback(86_400).expect("ordinary window"),
            691_200
        );
        let error = checked_shadow_stability_lookback(u64::MAX)
            .expect_err("lookback multiplication must not saturate");
        assert!(error.to_string().contains("shadow_stability_lookback_secs"));
    }
}

#[cfg(test)]
mod artifact_contract_tests {
    use quant_pivot_error::{QuantError, research::ResearchError};
    use quant_pivot_models::{
        domain::api::BindCalibrationRequest,
        enums::quant::{CalibrationKind, DownsideSource, PublicationStatus},
        types::{
            CalibrationArtifactId, ContentHash,
            model_metrics::{
                GovernedSellEstimatorMetrics, ModelArtifactTrainingLineage, ModelVersionMetrics,
            },
            model_serving::{ModelServingEstimatorBinding, ModelServingEstimatorInput},
            model_training::{GovernedSellFitStatus, ModelTrainingObjective},
        },
    };
    use quant_pivot_research::model::{ModelArtifact, ReturnModelSpec, artifact::ModelPayload};

    use super::ModelGovernanceService;
    use crate::service::model_serving_test_support::{
        model_artifact, model_version, sell_artifact,
    };

    fn factor_lineage(artifact: &ModelArtifact) -> ModelArtifactTrainingLineage {
        let bindings = artifact.header().serving_contract().bindings();
        let ModelServingEstimatorBinding::FactorNative { ordered_inputs, .. } =
            &bindings.model.estimator
        else {
            panic!("Sell fixture must use a factor-native estimator");
        };
        let factor_inputs = ordered_inputs
            .iter()
            .filter_map(|input| match input {
                ModelServingEstimatorInput::GovernedFactor {
                    factor_definition_id,
                } => Some(
                    bindings
                        .factors
                        .plane
                        .definitions()
                        .iter()
                        .find(|revision| revision.factor_definition_id() == *factor_definition_id)
                        .expect("known fixture factor")
                        .factor_name()
                        .clone(),
                ),
                ModelServingEstimatorInput::ModelIntrinsic { .. } => None,
            })
            .collect();
        let transform = &bindings.transform;
        ModelArtifactTrainingLineage::FactorNative {
            training_dataset_hash: transform.training_dataset_hash,
            training_input_hash: transform.training_input_hash,
            input_contract_hash: transform.input_contract_hash,
            input_transform_hash: transform.input_transform_hash,
            factor_inputs,
        }
    }

    #[test]
    fn calibration_reseals_contract() {
        let source = model_artifact(None);
        let source_version_id = source.header().model_version_id();
        let calibrator_ref = CalibrationArtifactId::from_v7();
        let calibrator_hash = ContentHash::from_bytes([9; 32]);
        let request = BindCalibrationRequest {
            calibrator_ref,
            downside_source: DownsideSource::MfeMae,
            reason: "bind independent calibration".to_owned(),
        };

        let calibrated =
            ModelGovernanceService::seal_calibrated_artifact(&source, &request, calibrator_hash)
                .expect("seal calibrated artifact");
        let contract = calibrated.header().serving_contract();
        let bindings = contract.bindings();
        assert_ne!(bindings.model.model_version_id, source_version_id);
        assert_eq!(
            bindings.model.calibration.as_ref().map(|binding| (
                binding.artifact_id,
                binding.kind,
                binding.content_hash
            )),
            Some((calibrator_ref, CalibrationKind::ModelScore, calibrator_hash))
        );
        assert_eq!(
            bindings.model.estimator,
            calibrated
                .payload()
                .serving_estimator_binding(&bindings.factors.plane)
                .expect("exact estimator binding")
        );
        let ModelPayload::WeightedFactor(weighted) = calibrated.payload() else {
            panic!("calibrated artifact must remain weighted-factor");
        };
        assert!(matches!(
            weighted.return_model,
            ReturnModelSpec::Calibrated(ref model) if model.calibrator_ref == calibrator_ref
        ));
        calibrated.validate().expect("valid calibrated artifact");
    }

    #[test]
    fn sell_oof_publish_rejected() {
        let artifact = sell_artifact();
        let mut version = model_version(&artifact, PublicationStatus::Candidate, None);
        let fit_status = GovernedSellFitStatus::OofPredictionsRequired;
        version.training_objective = ModelTrainingObjective::governed_sell(fit_status);
        version.metrics = ModelVersionMetrics::governed_sell(
            GovernedSellEstimatorMetrics {
                resolved_label_rows: 128,
                position_state_rows: 128,
                fit_status,
            },
            factor_lineage(&artifact),
        );

        let error = ModelGovernanceService::validate_sell_fit(&version)
            .expect_err("OOF-required Sell model must not enter governance");
        assert!(matches!(
            error,
            QuantError::Research(ResearchError::SellOofEstimatorRequired)
        ));
    }

    #[test]
    fn sell_lineage_mismatch_rejected() {
        let artifact = sell_artifact();
        let mut version = model_version(&artifact, PublicationStatus::Candidate, None);
        let fit_status = GovernedSellFitStatus::OofPredictionsRequired;
        let mut lineage = factor_lineage(&artifact);
        let ModelArtifactTrainingLineage::FactorNative {
            training_input_hash,
            ..
        } = &mut lineage
        else {
            panic!("Sell fixture must use factor-native lineage");
        };
        *training_input_hash = ContentHash::from_bytes([6; 32]);
        version.training_objective = ModelTrainingObjective::governed_sell(fit_status);
        version.metrics = ModelVersionMetrics::governed_sell(
            GovernedSellEstimatorMetrics {
                resolved_label_rows: 128,
                position_state_rows: 128,
                fit_status,
            },
            lineage,
        );

        let error = ModelGovernanceService::validate_sell_fit(&version)
            .expect_err("Sell lineage mismatch must fail closed");
        assert!(error.to_string().contains("metrics hashes differ"));
    }

    #[test]
    fn sell_format_drift_rejected() {
        let artifact = sell_artifact();
        let mut version = model_version(&artifact, PublicationStatus::Candidate, None);
        let fit_status = GovernedSellFitStatus::OofPredictionsRequired;
        version.training_objective = ModelTrainingObjective::governed_sell(fit_status);
        version.metrics = ModelVersionMetrics::governed_sell(
            GovernedSellEstimatorMetrics {
                resolved_label_rows: 128,
                position_state_rows: 128,
                fit_status,
            },
            factor_lineage(&artifact),
        );
        version.metrics.format_version += 1;

        let error = ModelGovernanceService::validate_sell_fit(&version)
            .expect_err("Sell format drift must fail closed");
        assert!(
            error
                .to_string()
                .contains("unsupported training provenance formats")
        );
    }
}

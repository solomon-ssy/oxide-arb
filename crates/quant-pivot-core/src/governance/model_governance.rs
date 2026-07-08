//! [`ModelGovernanceService`]: the offline governance closure orchestration
//! (Phase 3.7).
//!
//! Reuses the registry / backtest / shadow / dataset repositories and the
//! research [`ModelQualityGate`] to enforce the money-critical lifecycle:
//!
//! - **`publish`** — retire every other `Published` version for the spec, promote
//!   the candidate / shadow version, sync `model.active_model_version_id` (and
//!   clear the shadow slot) through a durable runtime-config activation, then
//!   audit.
//! - **`rollback`** — retire the current `Published` version, restore the
//!   recorded predecessor (`Retired → Published` when needed), sync runtime
//!   config to the restored version, and audit.
//! - **`promote_dataset_ready`** — a `Built` dataset becomes `Ready` only after
//!   a `DatasetReady` gate pass; `InsufficientLabels` can never be promoted (the
//!   repository state machine returns a `Conflict`).
//!
//! Leakage is enforced at dataset **build** time (a leaking dataset can never
//! reach `Built`), so the gate is fed a clean [`LeakageFindings`] here; its
//! leakage arm is exercised directly by the gate's own unit tests.
//!
//! Actor identity is recorded for audit provenance only; hard role enforcement
//! is deferred to the Phase 07 web wiring.

use std::sync::Arc;

use async_trait::async_trait;
use chrono::{Duration, Utc};
use quant_pivot_error::{QuantError, QuantResult, governance::GovernanceError};
use quant_pivot_models::{
    domain::{
        BacktestReportInfo, BindCalibrationRequest, GateOutcomeView, GatePreviewIntent,
        GovernanceActor, ModelGovernancePort, ModelVersionInfo, NewModelGovernanceAudit,
        NewModelVersion, PromoteDatasetRequest, PublishModelCommand, QualityGateReportView,
        RetireModelCommand, RollbackModelCommand, RuntimeConfigPort, ShadowStabilitySummary,
        TrainingDatasetInfo,
    },
    enums::{
        model::ModelFamily,
        quant::{CalibrationKind, ModelGovernanceAction, PublicationStatus, TrainingDatasetStatus},
    },
    runtime_config::QualityGateConfig,
    types::{
        AuditEventId, BacktestReportId, CalibrationArtifactId, ModelGovernanceAuditId, ModelSpecId,
        ModelVersionId, Probability,
    },
};
use quant_pivot_repository::traits::{
    BacktestReportRepository, CalibrationArtifactRepository, ModelGovernanceAuditRepository,
    ModelRegistryRepository, RuntimeConfigVersionRepository, ShadowComparisonRepository,
    TrainingDatasetRepository,
};
use quant_pivot_research::{
    artifact::ArtifactStore,
    backtest::BacktestReport,
    gates::{
        GateId, GateIntent, GateOutcome, GateSubject, ModelQualityGate, QualityGateDecision,
        QualityGateFailure, QualityGateInput, QualityGateReport, QualityGateThresholds,
        SellQualityGateThresholds,
    },
    model::{
        CalibratedReturnModel, ModelArtifact, ReturnModelSpec, load_hash_verified_artifact,
        validate_category_scope_weights,
    },
    training::{DatasetCoverage, LeakageFindings},
};
use rust_decimal::Decimal;

use crate::{
    governance::runtime_model_pointers::{
        RuntimeModelPointerSync, sync_after_model_retire, sync_production_active,
    },
    runtime_config::RuntimeConfigStore,
};

/// Repository + gate + config dependencies for the governance service.
pub struct ModelGovernanceDeps {
    /// Model registry (status transitions + gate-report persistence).
    pub model_registry_repo: Arc<dyn ModelRegistryRepository>,
    /// Backtest-report ledger (gate metric source).
    pub backtest_report_repo: Arc<dyn BacktestReportRepository>,
    /// Shadow-comparison ledger (publish stability source).
    pub shadow_comparison_repo: Arc<dyn ShadowComparisonRepository>,
    /// Governance audit trail (WORM).
    pub governance_audit_repo: Arc<dyn ModelGovernanceAuditRepository>,
    /// Training-dataset ledger (promotion).
    pub dataset_repo: Arc<dyn TrainingDatasetRepository>,
    /// Content-addressed artifact store (publish-time category-scope guard).
    pub artifact_store: Arc<dyn ArtifactStore>,
    /// Unified calibration-artifact ledger (`bind_calibration` guard).
    pub calibration_repo: Arc<dyn CalibrationArtifactRepository>,
    /// The model quality gate.
    pub gate: Arc<dyn ModelQualityGate>,
    /// Active runtime config (gate thresholds + shadow window).
    pub runtime_config: Arc<RuntimeConfigStore>,
    /// Live runtime-config apply (model pointer hot-reload).
    pub runtime_config_apply: Arc<dyn RuntimeConfigPort>,
    /// Durable runtime-config version ledger (pointer sync audit).
    pub runtime_config_repo: Arc<dyn RuntimeConfigVersionRepository>,
}

/// Offline model-governance orchestration service.
pub struct ModelGovernanceService {
    deps: ModelGovernanceDeps,
}

/// A shared gate evaluation: the report plus the shadow summary, which is
/// present only for shadow-gated intents (`Publish` / `AutoExecution`) and
/// feeds the publish audit's shadow-overlap evidence.
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
            .find_model_version_by_id(id)
            .await?
            .ok_or_else(|| {
                GovernanceError::NotFound {
                    entity: "model_version",
                    id: id.to_string(),
                }
                .into()
            })
    }

    /// Build the gate thresholds from the active `quality_gate` config.
    fn thresholds(&self) -> QuantResult<QualityGateThresholds> {
        thresholds_from_config(&self.deps.runtime_config.current().quality_gate)
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
        let lookback = required_window_secs
            .saturating_mul(8)
            .max(required_window_secs);
        let since = now - Duration::seconds(i64::try_from(lookback).unwrap_or(i64::MAX));
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

    fn pointer_sync(&self) -> RuntimeModelPointerSync {
        RuntimeModelPointerSync {
            runtime_config_apply: Arc::clone(&self.deps.runtime_config_apply),
            runtime_config_repo: Arc::clone(&self.deps.runtime_config_repo),
            model_registry_repo: Arc::clone(&self.deps.model_registry_repo),
        }
    }

    /// Optional publish guard (11.2.2): Crypto-scoped weighted artifacts must
    /// carry non-zero weight on at least one domain-crypto factor column.
    async fn validate_publish_artifact(&self, version: &ModelVersionInfo) -> QuantResult<()> {
        let artifact = load_hash_verified_artifact(&self.deps.artifact_store, version).await?;
        if let ModelArtifact::WeightedFactor(weighted) = artifact {
            validate_category_scope_weights(weighted.category_scope, &weighted.weights)?;
        }
        Ok(())
    }

    /// Retire every currently published version for a spec (single-active invariant).
    async fn retire_published_predecessors(
        &self,
        model_spec_id: &ModelSpecId,
        except: &ModelVersionId,
    ) -> QuantResult<Vec<ModelVersionId>> {
        let predecessors = self
            .deps
            .model_registry_repo
            .list_published_for_spec(model_spec_id)
            .await?;
        let mut retired = Vec::new();
        for predecessor in predecessors {
            if predecessor.model_version_id == *except {
                continue;
            }
            self.deps
                .model_registry_repo
                .retire_model_version(&predecessor.model_version_id)
                .await?;
            retired.push(predecessor.model_version_id);
        }
        Ok(retired)
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

        let required_window = self
            .deps
            .runtime_config
            .current()
            .quality_gate
            .required_shadow_window_secs;
        let evaluation = self
            .evaluate_gate(&version, GateIntent::Publish, None)
            .await?;
        let report = evaluation.report;
        let summary = evaluation.shadow_summary.ok_or_else(|| {
            QuantError::from(GovernanceError::IllegalTransition {
                detail: "publish gate did not evaluate shadow stability".to_owned(),
            })
        })?;

        self.deps
            .model_registry_repo
            .set_quality_gate_report(&version.model_version_id, gate_report_json(&report)?)
            .await?;

        if !report.passed {
            return Err(map_publish_gate_failure(
                &report.hard_failures,
                &version.model_version_id,
            ));
        }

        self.validate_publish_artifact(&version).await?;
        self.commit_publish(version, report, summary, required_window, command, actor)
            .await
    }

    async fn rollback(
        &self,
        command: RollbackModelCommand,
        actor: GovernanceActor,
    ) -> QuantResult<ModelVersionInfo> {
        let version = self.find_version(&command.model_version_id).await?;
        if version.publication_status != PublicationStatus::Published {
            return Err(GovernanceError::IllegalTransition {
                detail: format!(
                    "cannot roll back version {} in status {}",
                    version.model_version_id,
                    version.publication_status.as_str()
                ),
            }
            .into());
        }

        let target = self.resolve_rollback_target(&version).await?;
        let target_status = target.publication_status;

        let before_status = version.publication_status;
        let retired = self
            .deps
            .model_registry_repo
            .retire_model_version(&version.model_version_id)
            .await?;

        let restored = match target_status {
            PublicationStatus::Retired => {
                self.deps
                    .model_registry_repo
                    .restore_model_version(&target.model_version_id)
                    .await?
            }
            PublicationStatus::Published => target,
            status => {
                return Err(GovernanceError::IllegalTransition {
                    detail: format!(
                        "rollback target {} cannot be restored from status {}",
                        target.model_version_id,
                        status.as_str()
                    ),
                }
                .into());
            }
        };

        sync_production_active(
            &self.pointer_sync(),
            &restored.model_version_id,
            true,
            &format!(
                "rollback from {} to {}",
                retired.model_version_id, restored.model_version_id
            ),
            &actor.username,
        )
        .await?;

        self.write_audit(NewModelGovernanceAudit {
            audit_id: ModelGovernanceAuditId::from_v7(),
            model_version_id: Some(retired.model_version_id.clone()),
            training_dataset_id: None,
            action: ModelGovernanceAction::Rollback,
            actor_username: actor.username,
            actor_role: actor.role,
            reason: command.reason,
            before_status,
            after_status: retired.publication_status,
            before_hash: Some(version.artifact_hash.as_str().to_owned()),
            after_hash: Some(restored.artifact_hash.as_str().to_owned()),
            quality_gate_passed: false,
            rollback_target_version_id: Some(restored.model_version_id.clone()),
            shadow_window_secs: None,
            detail_json: serde_json::json!({
                "retired_version": retired.model_version_id.to_string(),
                "restored_version": restored.model_version_id.to_string(),
                "restored_from_status": target_status.as_str(),
            }),
            audit_event_id: Some(AuditEventId::from_v7()),
        })
        .await?;

        Ok(restored)
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

        let before_status = version.publication_status;
        let retired = self
            .deps
            .model_registry_repo
            .retire_model_version(&version.model_version_id)
            .await?;

        sync_after_model_retire(
            &self.pointer_sync(),
            &retired.model_version_id,
            &format!("retire model version {}", retired.model_version_id),
            &actor.username,
        )
        .await?;

        self.write_audit(NewModelGovernanceAudit {
            audit_id: ModelGovernanceAuditId::from_v7(),
            model_version_id: Some(retired.model_version_id.clone()),
            training_dataset_id: None,
            action: ModelGovernanceAction::Retire,
            actor_username: actor.username,
            actor_role: actor.role,
            reason: command.reason,
            before_status,
            after_status: retired.publication_status,
            before_hash: Some(version.artifact_hash.as_str().to_owned()),
            after_hash: Some(retired.artifact_hash.as_str().to_owned()),
            quality_gate_passed: false,
            rollback_target_version_id: None,
            shadow_window_secs: None,
            detail_json: serde_json::json!({
                "retired_version": retired.model_version_id.to_string(),
            }),
            audit_event_id: Some(AuditEventId::from_v7()),
        })
        .await?;

        Ok(retired)
    }

    async fn promote_dataset_ready(
        &self,
        request: PromoteDatasetRequest,
        actor: GovernanceActor,
    ) -> QuantResult<TrainingDatasetInfo> {
        let dataset = self
            .deps
            .dataset_repo
            .find_by_id(&request.training_dataset_id)
            .await?
            .ok_or_else(|| GovernanceError::NotFound {
                entity: "training_dataset",
                id: request.training_dataset_id.to_string(),
            })?;

        let coverage = dataset.coverage_json.clone();

        let model_family = if coverage.exit_decision_built > 0 {
            Some(ModelFamily::HoldVsExitWeighted)
        } else {
            None
        };
        let sell_thresholds =
            sell_thresholds_from_config(&self.deps.runtime_config.current().quality_gate)?;

        let decision = self.deps.gate.evaluate(QualityGateInput {
            subject: GateSubject::TrainingDataset(dataset.training_dataset_id.clone()),
            intent: GateIntent::DatasetReady,
            backtest: None,
            dataset: coverage,
            leakage: LeakageFindings::default(),
            shadow_stability: None,
            thresholds: self.thresholds()?,
            sell_thresholds,
            model_family,
            return_model_calibrated: false,
        })?;
        let report = decision.report().clone();

        if let QualityGateDecision::Fail { hard_failures, .. } = &decision {
            return Err(GovernanceError::QualityGateFailed {
                entity: "training_dataset",
                id: dataset.training_dataset_id.to_string(),
                failures: render_failures(hard_failures),
            }
            .into());
        }

        // The repository state machine enforces the legal transition (and
        // refuses `InsufficientLabels → Ready` with a `Conflict`).
        let before_status = dataset.status;
        let promoted = self
            .deps
            .dataset_repo
            .mark_status(&dataset.training_dataset_id, TrainingDatasetStatus::Ready)
            .await?;

        self.write_audit(NewModelGovernanceAudit {
            audit_id: ModelGovernanceAuditId::from_v7(),
            model_version_id: None,
            training_dataset_id: Some(promoted.training_dataset_id.clone()),
            action: ModelGovernanceAction::DatasetReady,
            actor_username: actor.username,
            actor_role: actor.role,
            reason: request.reason,
            before_status: PublicationStatus::default(),
            after_status: PublicationStatus::default(),
            before_hash: None,
            after_hash: Some(promoted.dataset_hash.as_str().to_owned()),
            quality_gate_passed: true,
            rollback_target_version_id: None,
            shadow_window_secs: None,
            detail_json: serde_json::json!({
                "gate_report_hash": report.report_hash.as_str(),
                "dataset_status_from": before_status.as_str(),
                "dataset_status_to": promoted.status.as_str(),
            }),
            audit_event_id: Some(AuditEventId::from_v7()),
        })
        .await?;

        Ok(promoted)
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
        let evaluation = self
            .evaluate_gate(&version, gate_intent, backtest_report_id)
            .await?;
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

        self.ensure_model_score_calibrator(&request.calibrator_ref)
            .await?;
        let calibrated = self
            .calibrated_artifact_from_version(&version, &request)
            .await?;
        self.persist_calibrated_version(&version, &request, calibrated, actor)
            .await
    }
}

impl ModelGovernanceService {
    async fn commit_publish(
        &self,
        version: ModelVersionInfo,
        report: QualityGateReport,
        summary: ShadowStabilitySummary,
        required_window: u64,
        command: PublishModelCommand,
        actor: GovernanceActor,
    ) -> QuantResult<ModelVersionInfo> {
        let rollback_target = self
            .deps
            .model_registry_repo
            .list_published_for_spec(&version.model_spec_id)
            .await?
            .into_iter()
            .next();
        let retired_predecessors = self
            .retire_published_predecessors(&version.model_spec_id, &version.model_version_id)
            .await?;
        let before_status = version.publication_status;
        let published = self
            .deps
            .model_registry_repo
            .publish_model_version(&version.model_version_id)
            .await?;

        sync_production_active(
            &self.pointer_sync(),
            &published.model_version_id,
            true,
            &format!("publish model version {}", published.model_version_id),
            &actor.username,
        )
        .await?;

        self.write_audit(NewModelGovernanceAudit {
            audit_id: ModelGovernanceAuditId::from_v7(),
            model_version_id: Some(published.model_version_id.clone()),
            training_dataset_id: None,
            action: ModelGovernanceAction::Publish,
            actor_username: actor.username,
            actor_role: actor.role,
            reason: command.reason,
            before_status,
            after_status: published.publication_status,
            before_hash: rollback_target
                .as_ref()
                .map(|predecessor| predecessor.artifact_hash.as_str().to_owned()),
            after_hash: Some(published.artifact_hash.as_str().to_owned()),
            quality_gate_passed: true,
            rollback_target_version_id: rollback_target.map(|predecessor| predecessor.model_version_id),
            shadow_window_secs: Some(i64::try_from(required_window).unwrap_or(i64::MAX)),
            detail_json: serde_json::json!({
                "gate_report_hash": report.report_hash.as_str(),
                "shadow_samples": summary.sample_count,
                "shadow_mean_overlap": summary.mean_topn_overlap.inner().to_string(),
                "retired_predecessors": retired_predecessors.iter().map(ToString::to_string).collect::<Vec<_>>(),
            }),
            audit_event_id: Some(AuditEventId::from_v7()),
        })
        .await?;

        Ok(published)
    }

    async fn ensure_model_score_calibrator(
        &self,
        calibrator_ref: &CalibrationArtifactId,
    ) -> QuantResult<()> {
        let calibrator = self
            .deps
            .calibration_repo
            .find_by_id(calibrator_ref)
            .await?
            .ok_or_else(|| GovernanceError::NotFound {
                entity: "calibration_artifact",
                id: calibrator_ref.to_string(),
            })?;
        if calibrator.kind != CalibrationKind::ModelScore {
            return Err(GovernanceError::IllegalTransition {
                detail: format!(
                    "calibrator {calibrator_ref} has kind {:?}, expected model_score",
                    calibrator.kind
                ),
            }
            .into());
        }
        Ok(())
    }

    async fn calibrated_artifact_from_version(
        &self,
        version: &ModelVersionInfo,
        request: &BindCalibrationRequest,
    ) -> QuantResult<ModelArtifact> {
        let bytes = self
            .deps
            .artifact_store
            .get_by_key(&ModelArtifact::artifact_key(&version.artifact_hash)?)
            .await?;
        let ModelArtifact::WeightedFactor(mut weighted) = ModelArtifact::from_bytes(&bytes)? else {
            return Err(GovernanceError::IllegalTransition {
                detail: "bind_calibration requires a weighted-factor buy model artifact".into(),
            }
            .into());
        };

        let new_version_id = ModelVersionId::from_v7();
        weighted.header.model_version_id = new_version_id.clone();
        weighted.return_model = ReturnModelSpec::Calibrated(CalibratedReturnModel {
            calibrator_ref: request.calibrator_ref.clone(),
            downside_source: request.downside_source,
        });
        let calibrated = ModelArtifact::WeightedFactor(weighted);
        calibrated.validate()?;
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
        let new_version_id = calibrated.header().model_version_id.clone();
        let next = self
            .deps
            .model_registry_repo
            .next_version_for_spec(&version.model_spec_id)
            .await?;
        let created = self
            .deps
            .model_registry_repo
            .create_model_version(NewModelVersion {
                model_version_id: new_version_id.clone(),
                model_spec_id: version.model_spec_id.clone(),
                version: next,
                artifact_hash,
                training_dataset_id: version.training_dataset_id.clone(),
                metrics_json: serde_json::json!({
                    "calibrated_from": version.model_version_id.to_string(),
                    "calibrator_ref": request.calibrator_ref.to_string(),
                    "downside_source": request.downside_source.as_str(),
                }),
                quality_gate_report: serde_json::json!({}),
                publication_status: PublicationStatus::Candidate,
                published_at: None,
                retired_at: None,
            })
            .await?;

        self.write_audit(NewModelGovernanceAudit {
            audit_id: ModelGovernanceAuditId::from_v7(),
            model_version_id: Some(created.model_version_id.clone()),
            training_dataset_id: None,
            action: ModelGovernanceAction::BindCalibration,
            actor_username: actor.username,
            actor_role: actor.role,
            reason: request.reason.clone(),
            before_status: version.publication_status,
            after_status: created.publication_status,
            before_hash: Some(version.artifact_hash.as_str().to_owned()),
            after_hash: Some(created.artifact_hash.as_str().to_owned()),
            quality_gate_passed: false,
            rollback_target_version_id: None,
            shadow_window_secs: None,
            detail_json: serde_json::json!({
                "source_version": version.model_version_id.to_string(),
                "calibrator_ref": request.calibrator_ref.to_string(),
            }),
            audit_event_id: Some(AuditEventId::from_v7()),
        })
        .await?;

        Ok(created)
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
        let config = self.deps.runtime_config.current();
        let required_window = config.quality_gate.required_shadow_window_secs;
        let thresholds = thresholds_from_config(&config.quality_gate)?;
        let sell_thresholds = sell_thresholds_from_config(&config.quality_gate)?;
        let model_family = self.model_family_for_version(version).await?;

        let return_model_calibrated = {
            let artifact = load_hash_verified_artifact(&self.deps.artifact_store, version).await?;
            artifact.return_model_is_calibrated()
        };

        let backtest = match backtest_report_id {
            Some(id) => self.backtest_by_id(id).await?,
            None => self.latest_backtest(&version.model_version_id).await?,
        };
        let dataset = self.dataset_coverage(version).await?;
        let (shadow_stability, shadow_summary) = if intent.requires_shadow_stability() {
            let (stability, summary) = self
                .shadow_stability(&version.model_version_id, required_window)
                .await?;
            (stability, Some(summary))
        } else {
            (None, None)
        };

        let decision = self.deps.gate.evaluate(QualityGateInput {
            subject: GateSubject::ModelVersion(version.model_version_id.clone()),
            intent,
            backtest,
            // Built ⇒ leakage-clean (enforced at dataset build); the gate's
            // leakage arm is unit-tested directly.
            leakage: LeakageFindings::default(),
            dataset,
            shadow_stability,
            thresholds,
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
        Ok(dataset.coverage_json)
    }

    /// Resolve the governed model family for a version (via its spec).
    async fn model_family_for_version(
        &self,
        version: &ModelVersionInfo,
    ) -> QuantResult<ModelFamily> {
        let spec = self
            .deps
            .model_registry_repo
            .find_model_spec_by_id(&version.model_spec_id)
            .await?
            .ok_or_else(|| GovernanceError::NotFound {
                entity: "model_spec",
                id: version.model_spec_id.to_string(),
            })?;
        Ok(spec.model_family)
    }

    /// Resolve the version to restore on rollback: the predecessor recorded at
    /// publish time, falling back to the latest other published version.
    async fn resolve_rollback_target(
        &self,
        version: &ModelVersionInfo,
    ) -> QuantResult<ModelVersionInfo> {
        let audits = self
            .deps
            .governance_audit_repo
            .list_by_version(&version.model_version_id)
            .await?;
        let recorded_target = audits
            .iter()
            .find(|audit| audit.action == ModelGovernanceAction::Publish)
            .and_then(|audit| audit.rollback_target_version_id.clone());
        if let Some(target_id) = recorded_target {
            return self.find_version(&target_id).await;
        }
        self.deps
            .model_registry_repo
            .list_published_for_spec(&version.model_spec_id)
            .await?
            .into_iter()
            .find(|candidate| candidate.model_version_id != version.model_version_id)
            .ok_or_else(|| {
                GovernanceError::IllegalTransition {
                    detail: format!(
                        "no predecessor to roll back version {} to",
                        version.model_version_id
                    ),
                }
                .into()
            })
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
        report_hash: report.report_hash.as_str().to_owned(),
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

/// Serialize a gate report into the JSON persisted on the model version.
fn gate_report_json(report: &QualityGateReport) -> QuantResult<serde_json::Value> {
    serde_json::to_value(report).map_err(|error| {
        GovernanceError::IllegalTransition {
            detail: format!("quality gate report is not serializable: {error}"),
        }
        .into()
    })
}

/// The effective shadow stability the publish gate evaluates: `None` (fails the
/// gate) unless the shadow has been observed for at least the required window
/// (its earliest comparison is at least that old), has samples, and shows no
/// hard divergence.
fn effective_stability(
    summary: &ShadowStabilitySummary,
    required_window_secs: u64,
    now: chrono::DateTime<Utc>,
) -> Option<Probability> {
    if summary.sample_count == 0 || summary.any_hard_divergence {
        return None;
    }
    let start = summary.window_start?;
    let observed_secs = u64::try_from(now.signed_duration_since(start).num_seconds()).unwrap_or(0);
    if observed_secs < required_window_secs {
        return None;
    }
    Some(summary.mean_topn_overlap)
}

/// Parse a `[0, 1]`-or-correlation governed threshold from its decimal string.
fn parse_threshold(value: &str, field: &str) -> QuantResult<Decimal> {
    value.parse::<Decimal>().map_err(|error| {
        QuantError::config(format!("invalid quality_gate.{field} `{value}`: {error}"))
    })
}

/// Assemble research [`QualityGateThresholds`] from the governed config section.
fn thresholds_from_config(config: &QualityGateConfig) -> QuantResult<QualityGateThresholds> {
    Ok(QualityGateThresholds {
        min_sample_count: config.min_sample_count,
        min_label_coverage: parse_threshold(
            &config.min_label_coverage.value,
            "min_label_coverage",
        )?,
        min_critical_feature_coverage: parse_threshold(
            &config.min_critical_feature_coverage.value,
            "min_critical_feature_coverage",
        )?,
        max_drawdown: parse_threshold(&config.max_drawdown.value, "max_drawdown")?,
        min_liquidity_exit_feasibility: parse_threshold(
            &config.min_liquidity_exit_feasibility.value,
            "min_liquidity_exit_feasibility",
        )?,
        min_shadow_overlap_stability: parse_threshold(
            &config.min_shadow_overlap_stability.value,
            "min_shadow_overlap_stability",
        )?,
        min_rank_ic: parse_threshold(&config.min_rank_ic.value, "min_rank_ic")?,
        max_category_concentration: parse_threshold(
            &config.max_category_concentration.value,
            "max_category_concentration",
        )?,
    })
}

/// Assemble sell-side [`SellQualityGateThresholds`] from the governed config section.
fn sell_thresholds_from_config(
    config: &QualityGateConfig,
) -> QuantResult<SellQualityGateThresholds> {
    Ok(SellQualityGateThresholds {
        min_sample_count: config.sell.min_sample_count,
        min_label_coverage: parse_threshold(
            &config.sell.min_label_coverage.value,
            "sell.min_label_coverage",
        )?,
        min_exit_alpha_rank_ic: parse_threshold(
            &config.sell.min_exit_alpha_rank_ic.value,
            "sell.min_exit_alpha_rank_ic",
        )?,
        min_l2_book_fidelity_ratio: parse_threshold(
            &config.sell.min_l2_book_fidelity_ratio.value,
            "sell.min_l2_book_fidelity_ratio",
        )?,
        max_fallback_ratio: parse_threshold(
            &config.sell.max_fallback_ratio.value,
            "sell.max_fallback_ratio",
        )?,
    })
}

/// Reconstruct a research [`BacktestReport`] from its persisted ledger row.
fn backtest_report_from_info(info: BacktestReportInfo) -> QuantResult<BacktestReport> {
    let expected_vs_realized =
        serde_json::from_value(info.expected_vs_realized).map_err(|error| {
            GovernanceError::IllegalTransition {
                detail: format!("backtest report `expected_vs_realized` is not decodable: {error}"),
            }
        })?;
    let category_breakdown = serde_json::from_value(info.category_breakdown).map_err(|error| {
        GovernanceError::IllegalTransition {
            detail: format!("backtest report `category_breakdown` is not decodable: {error}"),
        }
    })?;
    let report_pnl_simulation =
        serde_json::from_value(info.report_pnl_simulation).map_err(|error| {
            GovernanceError::IllegalTransition {
                detail: format!(
                    "backtest report `report_pnl_simulation` is not decodable: {error}"
                ),
            }
        })?;
    Ok(BacktestReport {
        backtest_report_id: info.backtest_report_id,
        model_version_id: info.model_version_id,
        runtime_config_version_id: info.runtime_config_version_id,
        window_start: info.window_start,
        window_end: info.window_end,
        coverage: info.coverage,
        sample_count: u64::try_from(info.sample_count).unwrap_or(0),
        missing_feature_count: u64::try_from(info.missing_feature_count).unwrap_or(0),
        rank_ic: info.rank_ic,
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

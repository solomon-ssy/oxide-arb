//! [`ModelGovernanceService`]: the offline governance closure orchestration
//! (Phase 3.7).
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
//! publish: [`Self::rescan_leakage`] decodes the frozen Parquet and
//! runs [`scan_future_leakage`] so a tampered or stale artifact cannot skip
//! the `NoPitLeakage` hard gate. A missing dataset row fails closed.
//!
//! Actor identity is recorded for audit provenance only; hard role enforcement
//! is deferred to the Phase 07 web wiring.

use std::sync::Arc;

use async_trait::async_trait;
use chrono::{Duration, Utc};
use quant_pivot_error::{QuantError, QuantResult, governance::GovernanceError};
use quant_pivot_models::{
    domain::{
        BacktestPathSetInfo, BacktestReportInfo, BindCalibrationRequest, BindPublishPathSetRequest,
        GateOutcomeView, GatePreviewIntent, GovernanceActor, ModelGovernancePort, ModelVersionInfo,
        NewModelGovernanceAudit, NewModelVersion, PublishModelCommand, QualityGateReportView,
        RetireModelCommand, ShadowStabilitySummary, TrainingDatasetInfo,
    },
    enums::{
        model::ModelFamily,
        quant::{CalibrationKind, ModelGovernanceAction, PublicationStatus},
    },
    runtime_config::{
        DecisionPolicySnapshot, QualityGateConfig, sections::ResearchValidationGatesConfig,
    },
    types::{
        AuditEventId, BacktestReportId, CalibrationArtifactId, FeatureParityRunId,
        ModelGovernanceAuditId, ModelVersionId, Probability,
    },
};
use quant_pivot_repository::traits::{
    BacktestPathSetRepository, BacktestReportRepository, CalibrationArtifactRepository,
    ModelGovernanceAuditRepository, ModelRegistryRepository, PublishFeatureParityPermit,
    PublishModelVersionCommit, ShadowComparisonRepository, TrainingDatasetRepository,
};
use quant_pivot_research::{
    artifact::ArtifactStore,
    backtest::BacktestReport,
    gates::{
        CpcvPathSetGateInput, GateId, GateIntent, GateOutcome, GateSubject, ModelQualityGate,
        QualityGateFailure, QualityGateInput, QualityGateReport, QualityGateThresholds,
        SellQualityGateThresholds, ValidationGateThresholds,
    },
    model::{
        CalibratedReturnModel, CalibrationArtifactLoader, ModelArtifact, ReturnModelSpec,
        load_hash_verified_artifact, validate_category_scope_weights,
    },
    training::{DatasetCoverage, LeakageFindings, scan_future_leakage},
    validation::SharpeDistribution,
};
use rust_decimal::Decimal;

use crate::{
    governance::resolve_return_model_calibration,
    runtime_config::DecisionPolicyStore,
    service::{
        feature_integrity::FeatureParityGatePort, frozen_model_parity::FrozenModelParityService,
        training_dataset,
    },
};

/// Repository + gate + config dependencies for the governance service.
pub struct ModelGovernanceDeps {
    /// Model registry (status transitions + gate-report persistence).
    pub model_registry_repo: Arc<dyn ModelRegistryRepository>,
    /// Backtest-report ledger (gate metric source).
    pub backtest_report_repo: Arc<dyn BacktestReportRepository>,
    /// CPCV path-set ledger (Phase 11.5 alpha-significance gate source).
    pub backtest_path_set_repo: Arc<dyn BacktestPathSetRepository>,
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
    /// Deep calibrator resolution (active + content-hash verified) — shared
    /// with the report builder / admission / intent creation via
    /// [`resolve_return_model_calibration`] (Phase 11.3 closed-loop hardening).
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

/// A shared gate evaluation: the report plus the shadow summary, which is
/// present only for shadow-gated intents (`Publish` / `AutoExecution`) and
/// feeds the publish audit's shadow-overlap evidence.
///
/// Also carries the hash-verified [`ModelArtifact`] this evaluation loaded to
/// compute `return_model_calibrated` — `publish()` reuses it for
/// [`ModelGovernanceService::validate_publish_artifact`] instead of a second,
/// redundant `load_hash_verified_artifact` round-trip (Phase 11.3 §8: "one
/// load, two checks").
struct GateEvaluation {
    report: QualityGateReport,
    shadow_summary: Option<ShadowStabilitySummary>,
    artifact: ModelArtifact,
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

    /// Re-scan a frozen dataset Parquet for point-in-time leakage (Phase 11.5 #9).
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

    /// Optional publish guard (11.2.2): Crypto-scoped weighted artifacts must
    /// carry non-zero weight on at least one domain-crypto factor column.
    ///
    /// Takes the already hash-verified `artifact` `evaluate_gate` loaded —
    /// never re-loads it (single-load publish path, Phase 11.3 §8).
    fn validate_publish_artifact(artifact: &ModelArtifact) -> QuantResult<()> {
        if let ModelArtifact::WeightedFactor(weighted) = artifact {
            validate_category_scope_weights(weighted.category_scope, &weighted.weights)?;
        }
        Ok(())
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

        Self::validate_publish_artifact(&evaluation.artifact)?;
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
            shadow_window_secs: None,
            detail_json: serde_json::json!({
                "retired_version": retired.model_version_id.to_string(),
            }),
            audit_event_id: Some(AuditEventId::from_v7()),
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
        if path_set.model_version_id != version.model_version_id {
            return Err(GovernanceError::IllegalTransition {
                detail: format!(
                    "path set {} belongs to model version {}, not {}",
                    request.path_set_id, path_set.model_version_id, version.model_version_id
                ),
            }
            .into());
        }
        let before = version.publish_path_set_id.clone();
        let updated = self
            .deps
            .model_registry_repo
            .set_publish_path_set_id(&version.model_version_id, Some(request.path_set_id.clone()))
            .await
            .map_err(QuantError::from)?;
        self.write_audit(NewModelGovernanceAudit {
            audit_id: ModelGovernanceAuditId::from_v7(),
            model_version_id: Some(version.model_version_id.clone()),
            training_dataset_id: version.training_dataset_id.clone(),
            action: ModelGovernanceAction::BindPublishPathSet,
            actor_username: actor.username,
            actor_role: actor.role,
            reason: request.reason,
            before_status: version.publication_status,
            after_status: updated.publication_status,
            before_hash: before.map(|id| id.to_string()),
            after_hash: Some(request.path_set_id.to_string()),
            quality_gate_passed: false,
            shadow_window_secs: None,
            detail_json: serde_json::json!({
                "path_set_id": request.path_set_id.to_string(),
                "path_set_hash": path_set.path_set_hash.as_str(),
            }),
            audit_event_id: Some(AuditEventId::from_v7()),
        })
        .await?;
        Ok(updated)
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
            model_version_id: Some(published.model_version_id.clone()),
            training_dataset_id: None,
            action: ModelGovernanceAction::Publish,
            actor_username: actor.username,
            actor_role: actor.role,
            reason: command.reason,
            before_status,
            after_status: published.publication_status,
            before_hash: None,
            after_hash: Some(published.artifact_hash.as_str().to_owned()),
            quality_gate_passed: true,
            shadow_window_secs: Some(checked_seconds(
                required_window,
                "quality_gate.shadow_stability_window_secs",
            )?),
            detail_json: serde_json::json!({
                "gate_report_hash": report.report_hash.as_str(),
                "shadow_samples": summary.sample_count,
                "shadow_mean_overlap": summary.mean_topn_overlap.inner().to_string(),
                "feature_parity_state_id": feature_parity_state_id.to_string(),
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
                category_scope: calibrated.category_scope(),
                profile_ref: version.profile_ref.clone(),
                training_dataset_id: version.training_dataset_id.clone(),
                trade_policy_artifact_id: calibrated.header().trade_policy_artifact_id.clone(),
                trade_policy_hash: calibrated.header().trade_policy_hash.clone(),
                publish_path_set_id: None,
                metrics_json: serde_json::json!({
                    "calibrated_from": version.model_version_id.to_string(),
                    "calibrator_ref": request.calibrator_ref.to_string(),
                    "downside_source": request.downside_source.as_str(),
                }),
                training_objective_json: version.training_objective_json.clone(),
                quality_gate_report: serde_json::json!({}),
                publication_status: PublicationStatus::Candidate,
                published_at: None,
                retired_at: None,
            })
            .await?;

        self.deps
            .frozen_model_parity
            .verify_and_record(
                &created,
                "model_calibration_binding",
                "full frozen parity for calibrated model artifact",
            )
            .await?;

        // Activate the bound calibrator (Phase 11.3 `active` governance —
        // §3.4): a `Calibrated` return model's `calibrator_ref` must resolve
        // through `CoreCalibrationArtifactLoader`, which fails closed on
        // `active == false`. `model_score` has no cross-model exclusivity, so
        // this never deactivates another model version's calibrator.
        self.deps
            .calibration_repo
            .mark_active(&request.calibrator_ref)
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

        let artifact = load_hash_verified_artifact(&self.deps.artifact_store, version).await?;
        // Deep check (Phase 11.3 closed-loop hardening): the *same* function
        // report/admission/intent-creation share, not the shallow enum-tag
        // read `ModelArtifact::return_model_is_calibrated` used to be — a
        // calibrator deactivated after `bind_calibration` must fail the
        // publish gate too, not just downstream consumers.
        let return_model_calibrated =
            resolve_return_model_calibration(self.deps.calibration_loader.as_ref(), &artifact)
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
        // Phase 11.5.1: Sell (`HoldVsExitWeighted`) publish/promote requires
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
            subject: GateSubject::ModelVersion(version.model_version_id.clone()),
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
            artifact,
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
        if info.model_version_id != version.model_version_id {
            return Err(GovernanceError::IllegalTransition {
                detail: format!(
                    "bound publish_path_set_id {path_set_id} belongs to model version {}, not {}",
                    info.model_version_id, version.model_version_id
                ),
            }
            .into());
        }
        Ok(Some(path_set_gate_input_from_info(&info)?))
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

/// Assemble Phase 11.5 [`ValidationGateThresholds`] from `research.validation.gates`.
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

fn path_set_gate_input_from_info(info: &BacktestPathSetInfo) -> QuantResult<CpcvPathSetGateInput> {
    // Fail closed on corrupt JSON — never silently drop Sell diagnostics
    // (`median_max_drawdown` / `median_tail_loss` / `baseline_uplift`) into
    // `None`, which would surface as opaque hard-gate failures without a
    // readable persistence error.
    let distribution = serde_json::from_value::<SharpeDistribution>(
        info.sharpe_distribution.clone(),
    )
    .map_err(|error| GovernanceError::IllegalTransition {
        detail: format!(
            "backtest path set {} `sharpe_distribution` is not decodable: {error}",
            info.path_set_id
        ),
    })?;
    Ok(CpcvPathSetGateInput {
        median_rank_ic: info.median_rank_ic,
        deflated_sharpe: info.deflated_sharpe,
        pbo: info.pbo,
        min_track_record_length_secs: info.min_track_record_length_secs,
        median_max_drawdown: distribution.median_max_drawdown,
        median_tail_loss: distribution.median_tail_loss,
        baseline_uplift: distribution.baseline_uplift,
        window_start: Some(info.window_start),
        window_end: Some(info.window_end),
    })
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
    use super::{checked_shadow_stability_lookback, path_set_gate_input_from_info};
    use chrono::Utc;
    use quant_pivot_models::{
        domain::BacktestPathSetInfo,
        types::{
            BacktestPathSetId, ContentHash, DecisionPolicySnapshotId, ModelRunId, ModelVersionId,
            TrainingDatasetId,
        },
    };
    use rust_decimal_macros::dec;

    fn hash() -> ContentHash {
        ContentHash::parse(format!("blake3:{}", "a".repeat(64))).expect("hash")
    }

    fn info_with_distribution(sharpe_distribution: serde_json::Value) -> BacktestPathSetInfo {
        BacktestPathSetInfo {
            path_set_id: BacktestPathSetId::from_v7(),
            model_version_id: ModelVersionId::from_v7(),
            model_run_id: ModelRunId::from_v7(),
            training_dataset_id: TrainingDatasetId::from_v7(),
            decision_policy_snapshot_id: DecisionPolicySnapshotId::from_v7(),
            window_start: Utc::now(),
            window_end: Utc::now(),
            path_count: 3,
            combination_count: 6,
            paths: serde_json::json!([]),
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
    fn path_set_gate_input_decodes_sell_diagnostics() {
        let info = info_with_distribution(serde_json::json!({
            "min": "0",
            "p25": "0",
            "median": "0.5",
            "p75": "1",
            "max": "1",
            "median_max_drawdown": "0.1",
            "median_tail_loss": "-0.005",
            "baseline_uplift": "0.001"
        }));
        let gate = path_set_gate_input_from_info(&info).expect("decode");
        assert_eq!(gate.median_max_drawdown, Some(dec!(0.1)));
        assert_eq!(gate.median_tail_loss, Some(dec!(-0.005)));
        assert_eq!(gate.baseline_uplift, Some(dec!(0.001)));
    }

    #[test]
    fn path_set_gate_input_rejects_corrupt_sharpe_distribution() {
        let info = info_with_distribution(serde_json::json!("not-an-object"));
        let err = path_set_gate_input_from_info(&info).expect_err("corrupt JSON");
        let detail = err.to_string();
        assert!(
            detail.contains("sharpe_distribution") || detail.contains("not decodable"),
            "expected explicit decode error, got: {detail}"
        );
    }

    #[test]
    fn shadow_stability_lookback_rejects_overflow() {
        assert_eq!(
            checked_shadow_stability_lookback(86_400).expect("ordinary window"),
            691_200
        );
        let error = checked_shadow_stability_lookback(u64::MAX)
            .expect_err("lookback multiplication must not saturate");
        assert!(error.to_string().contains("shadow_stability_lookback_secs"));
    }
}

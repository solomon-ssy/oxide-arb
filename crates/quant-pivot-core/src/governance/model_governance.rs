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
        BacktestReportInfo, GovernanceActor, ModelGovernancePort, ModelVersionInfo,
        NewModelGovernanceAudit, PromoteDatasetRequest, PublishModelCommand, RollbackModelCommand,
        RuntimeConfigPort, ShadowStabilitySummary, TrainingDatasetInfo,
    },
    enums::quant::{ModelGovernanceAction, ModelPublicationStatus, TrainingDatasetStatus},
    runtime_config::QualityGateConfig,
    types::{AuditEventId, ModelGovernanceAuditId, ModelSpecId, ModelVersionId, Probability},
};
use quant_pivot_repository::traits::{
    BacktestReportRepository, ModelGovernanceAuditRepository, ModelRegistryRepository,
    RuntimeConfigVersionRepository, ShadowComparisonRepository, TrainingDatasetRepository,
};
use quant_pivot_research::{
    backtest::BacktestReport,
    gates::{
        GateId, GateIntent, GateSubject, ModelQualityGate, QualityGateDecision, QualityGateFailure,
        QualityGateInput, QualityGateReport, QualityGateThresholds,
    },
    training::{DatasetCoverage, LeakageFindings},
};
use rust_decimal::Decimal;

use crate::{
    governance::runtime_model_pointers::{RuntimeModelPointerSync, sync_production_active},
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
            ModelPublicationStatus::Candidate | ModelPublicationStatus::Shadow
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

        let config = self.deps.runtime_config.current();
        let required_window = config.quality_gate.required_shadow_window_secs;
        let thresholds = thresholds_from_config(&config.quality_gate)?;

        let backtest = self.latest_backtest(&version.model_version_id).await?;
        let dataset = self.dataset_coverage(&version).await?;
        let (shadow_stability, summary) = self
            .shadow_stability(&version.model_version_id, required_window)
            .await?;

        let decision = self.deps.gate.evaluate(QualityGateInput {
            subject: GateSubject::ModelVersion(version.model_version_id.clone()),
            intent: GateIntent::Publish,
            backtest,
            dataset,
            // Built ⇒ leakage-clean (enforced at dataset build); the gate's
            // leakage arm is unit-tested directly.
            leakage: LeakageFindings::default(),
            shadow_stability,
            thresholds,
        })?;
        let report = decision.report().clone();

        // Persist the gate evaluation onto the version regardless of outcome —
        // it is the durable evidence for both a pass and a blocked attempt.
        self.deps
            .model_registry_repo
            .set_quality_gate_report(&version.model_version_id, gate_report_json(&report)?)
            .await?;

        if let QualityGateDecision::Fail { hard_failures, .. } = &decision {
            return Err(map_publish_gate_failure(
                hard_failures,
                &version.model_version_id,
            ));
        }

        // Capture the rollback target before retiring predecessors (most recent
        // published version for this spec, if any).
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
                .map(|version| version.artifact_hash.as_str().to_owned()),
            after_hash: Some(published.artifact_hash.as_str().to_owned()),
            quality_gate_passed: true,
            rollback_target_version_id: rollback_target.map(|version| version.model_version_id),
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

    async fn rollback(
        &self,
        command: RollbackModelCommand,
        actor: GovernanceActor,
    ) -> QuantResult<ModelVersionInfo> {
        let version = self.find_version(&command.model_version_id).await?;
        if version.publication_status != ModelPublicationStatus::Published {
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
            ModelPublicationStatus::Retired => {
                self.deps
                    .model_registry_repo
                    .restore_model_version(&target.model_version_id)
                    .await?
            }
            ModelPublicationStatus::Published => target,
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

        let coverage: DatasetCoverage = serde_json::from_value(dataset.coverage_json.clone())
            .map_err(|error| GovernanceError::IllegalTransition {
                detail: format!("dataset coverage is not decodable: {error}"),
            })?;

        let decision = self.deps.gate.evaluate(QualityGateInput {
            subject: GateSubject::TrainingDataset(dataset.training_dataset_id.clone()),
            intent: GateIntent::DatasetReady,
            backtest: None,
            dataset: coverage,
            leakage: LeakageFindings::default(),
            shadow_stability: None,
            thresholds: self.thresholds()?,
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
            before_status: ModelPublicationStatus::default(),
            after_status: ModelPublicationStatus::default(),
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
}

impl ModelGovernanceService {
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
        serde_json::from_value(dataset.coverage_json).map_err(|error| {
            GovernanceError::IllegalTransition {
                detail: format!("dataset coverage is not decodable: {error}"),
            }
            .into()
        })
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

/// Reconstruct a research [`BacktestReport`] from its persisted ledger row.
fn backtest_report_from_info(info: BacktestReportInfo) -> QuantResult<BacktestReport> {
    let expected_vs_realized = serde_json::from_value(info.expected_vs_realized)
        .map_err(|error| decode_error("expected_vs_realized", &error))?;
    let category_breakdown = serde_json::from_value(info.category_breakdown)
        .map_err(|error| decode_error("category_breakdown", &error))?;
    let report_pnl_simulation = serde_json::from_value(info.report_pnl_simulation)
        .map_err(|error| decode_error("report_pnl_simulation", &error))?;
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

/// Build a governance error for an undecodable persisted backtest sub-structure.
fn decode_error(field: &str, error: &serde_json::Error) -> QuantError {
    GovernanceError::IllegalTransition {
        detail: format!("backtest report `{field}` is not decodable: {error}"),
    }
    .into()
}

//! Dedicated full-stack server for the protected Phase 11.7 Playwright suite.
//!
//! This module is compiled only into the existing web integration-test binary;
//! production route assembly remains unchanged.

use std::sync::Arc;

use actix_web::{App, HttpResponse, HttpServer, middleware::from_fn, web};
use async_trait::async_trait;
use chrono::{DateTime, Duration, Utc};
use quant_pivot_core::{
    execution::{decide_entry_condition_state, evaluate_entry_condition},
    ingest::book_store::BookStore,
    observability::metrics_hub::MetricsHub,
};
use quant_pivot_error::{QuantError, QuantResult, control::ControlError, storage::StorageError};
use quant_pivot_models::{
    clickhouse::ReportMarketFunnelRow,
    domain::{
        AcknowledgeFeatureParityLatchRequest, ApplyEntryConditionEvaluation, BacktestPathSetInfo,
        BacktestPathSetListQuery, BacktestReportInfo, BacktestReportListQuery, BookLevel,
        CompleteTrainingDatasetBuild, EntryConditionArtifactInfo, EntryConditionInstanceInfo,
        FactorCollinearitySource, FactorCollinearityView, FactorDefinitionInfo,
        FactorDefinitionListQuery, FailTradePolicyValidation, FeatureIntegrityActionContext,
        FeatureIntegrityLatchView, FeatureIntegrityPort, FeatureIntegritySummaryView,
        FeatureParityEventListQuery, FeatureParityEventView, FeatureParityRunListQuery,
        FeatureParityRunView, JobProgressSink, MarketDataPort, ModelComparisonReportInfo,
        ModelPublishedCatalogQuery, ModelSpecInfo, ModelSpecListQuery, ModelTrainingPort,
        ModelVersionInfo, ModelVersionListQuery, NewTradePolicyValidationRow,
        NewTradePolicyValidationRun, NewTrainingDatasetPlan, Paginated, PublishedModelOptionView,
        RecommendationInfo, RecommendationReportInfo, ResearchCatalogPort, ResearchJobView,
        RunFullFeatureParityRequest, TradePolicyArtifactInfo, TradePolicyAuditListQuery,
        TradePolicyEvidenceDownloadView, TradePolicyEvidenceRowListQuery,
        TradePolicyEvidenceRowView, TradePolicyFitPreflightRequest, TradePolicyFitPreflightView,
        TradePolicyFitReadiness, TradePolicyGovernanceAuditInfo, TradePolicyListQuery,
        TradePolicyPort, TradePolicyPreflightBlockerDetail, TradePolicyPreflightBlockerView,
        TradePolicyPreflightCheckStatus, TradePolicySourceSliceObjectListQuery,
        TradePolicySourceSliceObjectView, TradePolicySourceSliceView,
        TradePolicyValidationListQuery, TradePolicyValidationRowInfo,
        TradePolicyValidationRowListQuery, TradePolicyValidationRunInfo, TrainModelRequest,
        TrainedModelView, TrainingDatasetInfo, TrainingDatasetListQuery, empty_catalog_page,
    },
    entities::{quant_entry_condition_instance, quant_recommendation},
    enums::quant::{
        DatasetPurpose, EntryConditionState, TradePolicyGovernanceAction, TradePolicyStatus,
        TradePolicyValidationStatus, TrainingDatasetStatus,
    },
    hashing::CanonicalDigest,
    runtime_config::{DecimalValue, DecisionPolicySnapshot},
    types::{
        ArtifactUri, ContentHash, DATASET_ARTIFACT_FORMAT_VERSION, DatasetCoverage,
        DatasetManifest, EntryConditionInputSet, EventId, ExecutablePriceInput, FactorDefinitionId,
        FeatureVectorId, MarketId, MarketSelectionId, ModelVersionId, OrderIntentId, Price,
        RecommendationId, RecommendationReportId, RecommendationTradePlan, ReportFunnelDiagnostics,
        ReportFunnelReason, ReportFunnelStage, ReportRunId, ResearchProfileId, ResearchProfileRef,
        Shares, SignalCandidateId, SourceSliceManifestRef, SourceSliceObjectKind, TokenId,
        TradePlanBlocker, TradePolicyArtifactId, TradePolicyEvidenceObjectKind,
        TradePolicyGovernanceAuditId, TradePolicyValidationRunId, TrainingExampleId,
        TrainingHorizonsSecs, TrainingSampleSources, UserId, WorkerId, default_sample_sources,
    },
};
use quant_pivot_repository::{
    postgres::{
        PgEntryConditionRepository, PgExecutionSubmissionRepository, PgModelRegistryRepository,
        PgOrderIntentRepository, PgRecommendationReportRepository, PgRecommendationRepository,
        PgReportRunRepository, PgTradePolicyRepository, PgTrainingDatasetRepository,
    },
    traits::{
        EntryConditionRepository, ExecutionSubmissionRepository, ModelRegistryRepository,
        OrderIntentRepository, RecommendationReportRepository, RecommendationRepository,
        ReportRunRepository, TradePolicyRepository, TrainingDatasetRepository,
    },
};
use quant_pivot_research::{
    artifact::ArtifactStore, hashing::ResearchHasher, training::dataset_manifest_hash,
};
use quant_pivot_test_support::ui_demo_seed::{DemoSeedRecord, seed_ui_demo_pg};
use quant_pivot_web::{middleware, routes};
use rust_decimal::Decimal;
use rust_decimal_macros::dec;
use sea_orm::{ActiveModelTrait, ActiveValue, DatabaseConnection, EntityTrait, IntoActiveModel};
use serde::{Deserialize, Serialize};
use tokio_util::sync::CancellationToken;
use tracing_subscriber::EnvFilter;
use uuid::Uuid;

use crate::harness;

const LISTEN_HOST: &str = "127.0.0.1";
const LISTEN_PORT: u16 = 8088;

struct E2eTradePolicyPort {
    policies: PgTradePolicyRepository,
}

struct E2eFeatureIntegrityPort;

#[async_trait]
impl FeatureIntegrityPort for E2eFeatureIntegrityPort {
    async fn summary(&self) -> QuantResult<FeatureIntegritySummaryView> {
        Err(QuantError::NotImplemented(
            "UI E2E feature integrity summary".into(),
        ))
    }

    async fn list_runs(
        &self,
        query: FeatureParityRunListQuery,
    ) -> QuantResult<Paginated<FeatureParityRunView>> {
        Ok(Paginated::empty_for(&query))
    }

    async fn list_events(
        &self,
        query: FeatureParityEventListQuery,
    ) -> QuantResult<Paginated<FeatureParityEventView>> {
        Ok(Paginated::empty_for(&query))
    }

    async fn request_full_run(
        &self,
        _request: RunFullFeatureParityRequest,
        _ctx: FeatureIntegrityActionContext,
    ) -> QuantResult<ResearchJobView> {
        Err(QuantError::NotImplemented(
            "UI E2E feature parity full run".into(),
        ))
    }

    async fn acknowledge_latch(
        &self,
        _request: AcknowledgeFeatureParityLatchRequest,
        _ctx: FeatureIntegrityActionContext,
    ) -> QuantResult<FeatureIntegrityLatchView> {
        Err(QuantError::NotImplemented(
            "UI E2E feature parity acknowledge".into(),
        ))
    }
}

impl E2eTradePolicyPort {
    const fn new(db: DatabaseConnection) -> Self {
        Self {
            policies: PgTradePolicyRepository::new(db),
        }
    }
}

fn e2e_artifact_uri(
    artifact_id: &TradePolicyArtifactId,
    object_name: &str,
) -> QuantResult<ArtifactUri> {
    ArtifactUri::parse(format!(
        "s3://ui-e2e/source-slices/{artifact_id}/{object_name}"
    ))
    .map_err(|error| QuantError::config(format!("invalid UI E2E artifact URI: {error}")))
}

#[async_trait]
impl TradePolicyPort for E2eTradePolicyPort {
    fn list_profiles(
        &self,
    ) -> QuantResult<Vec<quant_pivot_models::types::ResearchProfileArtifact>> {
        quant_pivot_models::types::builtin_research_profiles().map_err(|detail| {
            quant_pivot_error::research::ResearchError::ValidationMethodology { detail }.into()
        })
    }

    fn find_profile(
        &self,
        id: &ResearchProfileId,
        version: u32,
    ) -> QuantResult<Option<quant_pivot_models::types::ResearchProfileArtifact>> {
        Ok(self.list_profiles()?.into_iter().find(|profile| {
            profile.profile_ref.id == *id && profile.profile_ref.version == version
        }))
    }

    async fn preflight(
        &self,
        request: &TradePolicyFitPreflightRequest,
    ) -> QuantResult<TradePolicyFitPreflightView> {
        let profile = self
            .find_profile(
                &request.selection.profile_ref.id,
                request.selection.profile_ref.version,
            )?
            .ok_or_else(|| StorageError::NotFound {
                entity: "research_profile",
                id: format!(
                    "{}@{}",
                    request.selection.profile_ref.id, request.selection.profile_ref.version
                ),
            })?;
        let fit_window_end = request.selection.pit_cutoff
            - Duration::seconds(i64::try_from(profile.spec.target_horizon_secs).map_err(
                |error| QuantError::NotImplemented(format!("invalid E2E target horizon: {error}")),
            )?);
        let fit_window_start =
            fit_window_end - Duration::days(i64::from(profile.spec.fit_span_days));
        let candidate_set_hash = ResearchHasher::canonical(&request.candidates)?;
        let methodology_hash =
            ResearchHasher::canonical(&("ui-e2e-methodology-v1", &profile.profile_ref))?;
        let research_program_hash = ResearchHasher::canonical(&(
            &request.selection,
            request.evaluation_track,
            &request.candidates,
        ))?;
        let estimated_candidate_trials =
            u64::try_from(request.candidates.len()).map_err(|error| {
                QuantError::NotImplemented(format!("invalid E2E candidate count: {error}"))
            })?;
        let required_raw_retention_days = profile
            .spec
            .required_days()
            .map_err(QuantError::NotImplemented)?
            .checked_mul(2)
            .map(|days| days.max(180));
        let fail = TradePolicyPreflightCheckStatus::Fail;
        let pass = TradePolicyPreflightCheckStatus::Pass;
        Ok(TradePolicyFitPreflightView {
            readiness: TradePolicyFitReadiness::Blocked,
            reusable_source_dataset_id: None,
            profile: Some(profile),
            fit_window_start: Some(fit_window_start),
            fit_window_end: Some(fit_window_end),
            research_program_hash: Some(research_program_hash),
            source_slice_id: None,
            source_slice_identity_hash: Some(candidate_set_hash.clone()),
            estimated_candidate_trials,
            estimated_fold_evaluations: estimated_candidate_trials * 56,
            catalog_completeness_proven: fail,
            source_completeness_proven: fail,
            required_raw_retention_days,
            retention_runway_days: None,
            retention_runway_proven: fail,
            contract_valid: pass,
            profile_fitter_available: pass,
            source_dataset_ready: pass,
            source_dataset_policy_fit: pass,
            raw_trajectory_labels_present: pass,
            profile_lineage_valid: pass,
            source_slice_verified: fail,
            fit_window_contained: pass,
            profile_quality_gate_available: pass,
            decision_policy_snapshot_id: None,
            methodology_hash: Some(methodology_hash),
            latency_profile_present: fail,
            latency_evidence: None,
            pit_cutoff_valid: pass,
            labels_matured_by_cutoff: 11_200,
            labels_excluded_after_cutoff: 0,
            full_l2_trajectory_present: fail,
            fee_model_present: fail,
            retention_evidence: None,
            publishable_input: fail,
            canonical_candidates: Some(request.candidates.clone()),
            candidate_set_hash: Some(candidate_set_hash),
            blockers: vec![
                TradePolicyPreflightBlockerView {
                    detail: TradePolicyPreflightBlockerDetail::SourceSliceUnverified {
                        diagnostics: vec!["fixture source slice is intentionally unverified".to_owned()],
                    },
                    remediation: "Materialize and hash-verify every required Source Slice v1 object."
                        .to_owned(),
                    evidence_link: None,
                },
                TradePolicyPreflightBlockerView {
                    detail: TradePolicyPreflightBlockerDetail::FullL2TrajectoryMissing,
                    remediation: "Materialize continuous snapshot-rooted L2 sessions and reconciled trade tape."
                        .to_owned(),
                    evidence_link: None,
                },
                TradePolicyPreflightBlockerView {
                    detail: TradePolicyPreflightBlockerDetail::PitFeeFactsMissing,
                    remediation: "Backfill append-only CLOB market-info versions for every sample."
                        .to_owned(),
                    evidence_link: None,
                },
                TradePolicyPreflightBlockerView {
                    detail: TradePolicyPreflightBlockerDetail::ProductionLatencyProfileMissing {
                        observed_profile: None,
                    },
                    remediation: "Capture, sign, and bind the latest complete 24-hour production latency profile."
                        .to_owned(),
                    evidence_link: None,
                },
                TradePolicyPreflightBlockerView {
                    detail: TradePolicyPreflightBlockerDetail::RetentionRunwayUnproven {
                        actual_runway_days: None,
                        required_minimum_days: required_raw_retention_days,
                    },
                    remediation: "Bind a signed retention plan and current ClickHouse runway measurement."
                        .to_owned(),
                    evidence_link: None,
                },
            ],
        })
    }

    async fn fit(
        &self,
        _fit_job_id: &quant_pivot_models::types::ResearchJobId,
        _training_dataset_id: &quant_pivot_models::types::TrainingDatasetId,
        _request: quant_pivot_models::domain::FitTradePolicyRequest,
        _progress: Arc<dyn JobProgressSink>,
        _cancel: CancellationToken,
    ) -> QuantResult<TradePolicyArtifactInfo> {
        Err(QuantError::NotImplemented(
            "trade-policy fit is outside the UI E2E server".to_owned(),
        ))
    }

    async fn validate(
        &self,
        _validation_run_id: &quant_pivot_models::types::TradePolicyValidationRunId,
        artifact_id: &TradePolicyArtifactId,
        actor_id: UserId,
        reason: String,
        _progress: &dyn JobProgressSink,
        _cancel: &CancellationToken,
    ) -> QuantResult<TradePolicyArtifactInfo> {
        self.transition(artifact_id, TradePolicyStatus::Validated, actor_id, reason)
            .await
    }

    async fn find(
        &self,
        artifact_id: &TradePolicyArtifactId,
    ) -> QuantResult<Option<TradePolicyArtifactInfo>> {
        self.policies.find(artifact_id).await.map_err(Into::into)
    }

    async fn source_slice(
        &self,
        artifact_id: &TradePolicyArtifactId,
    ) -> QuantResult<Option<TradePolicySourceSliceView>> {
        let Some(policy) = self.find(artifact_id).await? else {
            return Ok(None);
        };
        let bundle = policy.payload_json.evidence_bundle.ok_or_else(|| {
            QuantError::config("UI E2E policy must bind a Source Slice evidence bundle")
        })?;
        Ok(Some(TradePolicySourceSliceView {
            artifact_id: artifact_id.clone(),
            profile_ref: policy.payload_json.fit_contract.profile_ref,
            source_slice: SourceSliceManifestRef {
                manifest_uri: e2e_artifact_uri(artifact_id, "manifest.json")?,
                manifest_hash: bundle.source_slice_manifest_hash,
            },
        }))
    }

    async fn page_source_slice_objects(
        &self,
        artifact_id: &TradePolicyArtifactId,
        query: TradePolicySourceSliceObjectListQuery,
    ) -> QuantResult<Option<Paginated<TradePolicySourceSliceObjectView>>> {
        let Some(policy) = self.find(artifact_id).await? else {
            return Ok(None);
        };
        let objects = [
            (SourceSliceObjectKind::ClobMarketInfo, 4_800_u64),
            (SourceSliceObjectKind::L2Event, 240_000_u64),
            (SourceSliceObjectKind::TradeTape, 18_400_u64),
        ]
        .into_iter()
        .filter(|(kind, _)| query.kind.is_none_or(|requested| requested == *kind))
        .map(|(kind, row_count)| {
            let object_hash = ResearchHasher::canonical(&(
                "ui_e2e_source_slice_object_v1",
                artifact_id,
                kind,
                row_count,
            ))?;
            let schema_hash =
                ResearchHasher::canonical(&("source_slice_parquet_envelope_v1", kind))?;
            Ok(TradePolicySourceSliceObjectView {
                kind,
                uri: e2e_artifact_uri(artifact_id, &format!("{kind:?}.parquet"))?,
                object_version: format!("ui-e2e-object-lock:{}", object_hash.as_str()),
                byte_hash: object_hash,
                schema_hash,
                row_count,
                min_event_at: Some(policy.payload_json.fit_contract.fit_window_start),
                max_event_at: Some(policy.payload_json.fit_contract.fit_window_end),
                min_available_at: Some(policy.payload_json.fit_contract.fit_window_start),
                max_available_at: Some(policy.payload_json.fit_contract.pit_cutoff),
            })
        })
        .collect::<QuantResult<Vec<_>>>()?;
        let page = query.page.normalized();
        let total = u64::try_from(objects.len())
            .map_err(|error| QuantError::config(format!("invalid E2E object count: {error}")))?;
        let offset = usize::try_from(page.offset())
            .map_err(|error| QuantError::config(format!("invalid E2E page offset: {error}")))?;
        let size = usize::try_from(page.limit())
            .map_err(|error| QuantError::config(format!("invalid E2E page size: {error}")))?;
        Ok(Some(Paginated::new(
            objects.into_iter().skip(offset).take(size).collect(),
            total,
            page.page,
            page.size,
        )))
    }

    async fn evidence_download(
        &self,
        artifact_id: &TradePolicyArtifactId,
        kind: TradePolicyEvidenceObjectKind,
    ) -> QuantResult<Option<TradePolicyEvidenceDownloadView>> {
        let Some(policy) = self.find(artifact_id).await? else {
            return Ok(None);
        };
        let bundle = policy
            .payload_json
            .evidence_bundle
            .ok_or_else(|| QuantError::config("UI E2E policy must bind an evidence bundle"))?;
        let byte_hash =
            ResearchHasher::canonical(&("ui_e2e_evidence_object_v1", artifact_id, kind))?;
        let expires_at = Utc::now() + Duration::minutes(5);
        let signature = ResearchHasher::canonical(&(
            "ui_e2e_signed_download_v1",
            artifact_id,
            kind,
            expires_at.timestamp(),
            &bundle.manifest_hash,
        ))?;
        Ok(Some(TradePolicyEvidenceDownloadView {
            artifact_id: artifact_id.clone(),
            kind,
            byte_hash,
            row_count: 128,
            expires_at,
            url: format!(
                "http://{LISTEN_HOST}:{LISTEN_PORT}/__test/evidence/{artifact_id}/{kind:?}?expires={}&signature={}",
                expires_at.timestamp(),
                signature.as_str(),
            ),
        }))
    }

    async fn page_evidence_rows(
        &self,
        artifact_id: &TradePolicyArtifactId,
        kind: TradePolicyEvidenceObjectKind,
        query: TradePolicyEvidenceRowListQuery,
    ) -> QuantResult<Option<Paginated<TradePolicyEvidenceRowView>>> {
        if self.find(artifact_id).await?.is_none() {
            return Ok(None);
        }
        let payload = serde_json::json!({
            "candidate_id": "weather-balanced",
            "diagnostic": "verified UI E2E evidence row",
            "sample_count": 128,
        });
        let row_hash = ResearchHasher::canonical(&(
            "ui_e2e_policy_evidence_row_v1",
            artifact_id,
            kind,
            &payload,
        ))?;
        let rows = vec![TradePolicyEvidenceRowView {
            kind,
            record_key: format!("{kind:?}/000001"),
            event_at: Some(Utc::now()),
            payload,
            row_hash,
        }];
        let page = query.page.normalized();
        let items = if page.page == 1 { rows } else { Vec::new() };
        Ok(Some(Paginated::new(items, 1, page.page, page.size)))
    }

    async fn page(
        &self,
        query: TradePolicyListQuery,
    ) -> QuantResult<Paginated<TradePolicyArtifactInfo>> {
        self.policies.page(query).await.map_err(Into::into)
    }

    async fn page_audits(
        &self,
        artifact_id: &TradePolicyArtifactId,
        query: TradePolicyAuditListQuery,
    ) -> QuantResult<Paginated<TradePolicyGovernanceAuditInfo>> {
        self.policies
            .page_audits(artifact_id, query)
            .await
            .map_err(Into::into)
    }

    async fn page_trials(
        &self,
        _fit_job_id: &quant_pivot_models::types::ResearchJobId,
        query: quant_pivot_models::domain::TradePolicyTrialListQuery,
    ) -> QuantResult<Paginated<quant_pivot_models::domain::TradePolicyTrialAttemptInfo>> {
        Ok(Paginated::empty(query.page.page, query.page.size))
    }

    async fn find_validation(
        &self,
        validation_run_id: &quant_pivot_models::types::TradePolicyValidationRunId,
    ) -> QuantResult<Option<TradePolicyValidationRunInfo>> {
        self.policies
            .find_validation(validation_run_id)
            .await
            .map_err(Into::into)
    }

    async fn page_validations(
        &self,
        artifact_id: &TradePolicyArtifactId,
        query: TradePolicyValidationListQuery,
    ) -> QuantResult<Paginated<TradePolicyValidationRunInfo>> {
        self.policies
            .page_validations(artifact_id, query)
            .await
            .map_err(Into::into)
    }

    async fn page_validation_rows(
        &self,
        validation_run_id: &quant_pivot_models::types::TradePolicyValidationRunId,
        query: TradePolicyValidationRowListQuery,
    ) -> QuantResult<Paginated<TradePolicyValidationRowInfo>> {
        self.policies
            .page_validation_rows(validation_run_id, query)
            .await
            .map_err(Into::into)
    }

    async fn transition(
        &self,
        artifact_id: &TradePolicyArtifactId,
        target: TradePolicyStatus,
        actor_id: UserId,
        reason: String,
    ) -> QuantResult<TradePolicyArtifactInfo> {
        let current =
            self.policies
                .find(artifact_id)
                .await?
                .ok_or_else(|| StorageError::NotFound {
                    entity: "trade_policy_artifact",
                    id: artifact_id.to_string(),
                })?;
        let action = match target {
            TradePolicyStatus::Validated => TradePolicyGovernanceAction::Validate,
            TradePolicyStatus::Published => TradePolicyGovernanceAction::Publish,
            TradePolicyStatus::Retired => TradePolicyGovernanceAction::Retire,
            TradePolicyStatus::Draft => {
                return Err(StorageError::IllegalTransition {
                    entity: "trade_policy_artifact",
                    id: Some(artifact_id.to_string()),
                    from: current.status.as_str().to_owned(),
                    to: target.as_str().to_owned(),
                }
                .into());
            }
        };
        self.policies
            .transition(
                artifact_id,
                current.status,
                target,
                quant_pivot_models::domain::NewTradePolicyGovernanceAudit {
                    audit_id: TradePolicyGovernanceAuditId::from_v7(),
                    artifact_id: artifact_id.clone(),
                    action,
                    from_status: current.status,
                    to_status: target,
                    content_hash: current.content_hash,
                    actor_id,
                    reason,
                },
            )
            .await
            .map_err(Into::into)
    }
}

struct E2eResearchCatalogPort {
    datasets: PgTrainingDatasetRepository,
    models: PgModelRegistryRepository,
}

struct E2eModelTrainingPort {
    models: PgModelRegistryRepository,
}

impl E2eModelTrainingPort {
    const fn new(db: DatabaseConnection) -> Self {
        Self {
            models: PgModelRegistryRepository::new(db),
        }
    }
}

#[async_trait]
impl ModelTrainingPort for E2eModelTrainingPort {
    async fn train(
        &self,
        _model_version_id: ModelVersionId,
        _request: TrainModelRequest,
        _progress: Arc<dyn JobProgressSink>,
        _cancel: CancellationToken,
    ) -> QuantResult<TrainedModelView> {
        Err(QuantError::NotImplemented(
            "model training is outside the UI E2E server".to_owned(),
        ))
    }

    async fn find_version(
        &self,
        model_version_id: &ModelVersionId,
    ) -> QuantResult<Option<ModelVersionInfo>> {
        self.models
            .find_model_version_by_id(model_version_id)
            .await
            .map_err(Into::into)
    }
}

impl E2eResearchCatalogPort {
    fn new(db: DatabaseConnection) -> Self {
        Self {
            datasets: PgTrainingDatasetRepository::new(db.clone()),
            models: PgModelRegistryRepository::new(db),
        }
    }
}

#[async_trait]
impl ResearchCatalogPort for E2eResearchCatalogPort {
    async fn list_training_datasets(
        &self,
        query: TrainingDatasetListQuery,
    ) -> QuantResult<Paginated<TrainingDatasetInfo>> {
        self.datasets.page(query).await.map_err(Into::into)
    }

    async fn list_models(
        &self,
        query: ModelVersionListQuery,
    ) -> QuantResult<Paginated<ModelVersionInfo>> {
        self.models.page_versions(query).await.map_err(Into::into)
    }

    async fn list_model_specs(
        &self,
        query: ModelSpecListQuery,
    ) -> QuantResult<Paginated<ModelSpecInfo>> {
        self.models.page_specs(query).await.map_err(Into::into)
    }

    async fn list_published_model_options(
        &self,
        query: ModelPublishedCatalogQuery,
    ) -> QuantResult<Vec<PublishedModelOptionView>> {
        self.models
            .list_published_catalog(query.side, query.category)
            .await
            .map_err(Into::into)
            .map(|rows| {
                rows.into_iter()
                    .map(|row| PublishedModelOptionView {
                        model_version_id: row.model_version_id,
                        model_spec_id: row.model_spec_id,
                        spec_name: row.spec_name,
                        version: row.version,
                        artifact_hash: row.artifact_hash,
                        model_family: row.model_family,
                        category_scope: row.category_scope,
                        published_at: row.published_at,
                    })
                    .collect()
            })
    }

    async fn list_backtest_reports(
        &self,
        query: BacktestReportListQuery,
    ) -> QuantResult<Paginated<BacktestReportInfo>> {
        Ok(empty_catalog_page(&query))
    }

    async fn list_backtest_path_sets(
        &self,
        query: BacktestPathSetListQuery,
    ) -> QuantResult<Paginated<BacktestPathSetInfo>> {
        Ok(empty_catalog_page(&query))
    }

    async fn list_comparison_reports(
        &self,
        query: quant_pivot_models::domain::ComparisonReportListQuery,
    ) -> QuantResult<Paginated<ModelComparisonReportInfo>> {
        Ok(empty_catalog_page(&query))
    }

    async fn list_factors(
        &self,
        query: FactorDefinitionListQuery,
    ) -> QuantResult<Paginated<FactorDefinitionInfo>> {
        Ok(empty_catalog_page(&query))
    }

    async fn find_factor(
        &self,
        _factor_definition_id: &FactorDefinitionId,
    ) -> QuantResult<Option<FactorDefinitionInfo>> {
        Ok(None)
    }

    async fn factor_collinearity(
        &self,
        lookback_secs: u64,
        threshold: Decimal,
        source: FactorCollinearitySource,
        _neutralize_by_category: bool,
    ) -> QuantResult<FactorCollinearityView> {
        Ok(FactorCollinearityView {
            factors: Vec::new(),
            matrix: Vec::new(),
            violations: Vec::new(),
            threshold,
            observation_count: 0,
            lookback_secs,
            panel_source: source,
        })
    }
}

struct E2eMarketData {
    books: Arc<BookStore>,
}

#[async_trait]
impl MarketDataPort for E2eMarketData {
    fn book(
        &self,
        yes_token: &TokenId,
        no_token: &TokenId,
    ) -> (
        Option<Arc<quant_pivot_models::domain::BookSnapshot>>,
        Option<Arc<quant_pivot_models::domain::BookSnapshot>>,
    ) {
        (self.books.load(yes_token), self.books.load(no_token))
    }

    fn subscribed_tokens(&self, token_ids: &[TokenId]) -> std::collections::HashSet<TokenId> {
        token_ids.iter().cloned().collect()
    }

    fn all_subscribed_tokens(&self) -> std::collections::HashSet<TokenId> {
        std::collections::HashSet::new()
    }

    async fn subscribe(&self, _token_ids: Vec<TokenId>) -> Result<(), ControlError> {
        Ok(())
    }

    async fn unsubscribe(&self, _token_ids: Vec<TokenId>) -> Result<(), ControlError> {
        Ok(())
    }
}

#[derive(Clone, Serialize)]
struct E2eFixtures {
    fixture_format_version: u32,
    unavailable_recommendation_id: RecommendationId,
    frozen_recommendation_id: RecommendationId,
    report_id: RecommendationReportId,
    current_report_id: RecommendationReportId,
    current_report_run_id: ReportRunId,
    pending_intent_id: OrderIntentId,
    waiting_intent_id: OrderIntentId,
    position_id: quant_pivot_models::types::PositionId,
    model_version_id: quant_pivot_models::types::ModelVersionId,
    trade_policy_artifact_id: TradePolicyArtifactId,
    trade_policy_content_hash: ContentHash,
}

#[derive(Serialize)]
struct E2eFunnelRowHashInput<'a> {
    report_id: &'a RecommendationReportId,
    market_selection_id: &'a MarketSelectionId,
    profile_ref: &'a ResearchProfileRef,
    market_id: &'a MarketId,
    event_id: &'a EventId,
    token_id: &'a TokenId,
    terminal_stage: ReportFunnelStage,
    primary_reason: ReportFunnelReason,
    secondary_diagnostics: &'a ReportFunnelDiagnostics,
    feature_vector_id: Option<&'a FeatureVectorId>,
    signal_candidate_id: Option<&'a SignalCandidateId>,
    recommendation_id: Option<&'a RecommendationId>,
}

fn e2e_published_funnel_row(
    report: &RecommendationReportInfo,
    recommendation: &RecommendationInfo,
) -> ReportMarketFunnelRow {
    assert_eq!(report.profile_ref, recommendation.profile_ref);
    assert_eq!(
        report.model_run_id.as_ref(),
        Some(&recommendation.evidence_refs.model_run_id)
    );
    let terminal_stage = ReportFunnelStage::Published;
    let primary_reason = ReportFunnelReason::Published;
    let secondary_diagnostics = ReportFunnelDiagnostics::None {};
    let secondary_diagnostics_json =
        serde_json::to_string(&secondary_diagnostics).expect("serialize E2E funnel diagnostics");
    let hash_input = E2eFunnelRowHashInput {
        report_id: &report.recommendation_report_id,
        market_selection_id: &report.market_selection_id,
        profile_ref: &report.profile_ref,
        market_id: &recommendation.market_id,
        event_id: &recommendation.event_id,
        token_id: &recommendation.token_id,
        terminal_stage,
        primary_reason,
        secondary_diagnostics: &secondary_diagnostics,
        feature_vector_id: Some(&recommendation.evidence_refs.feature_vector_id),
        signal_candidate_id: Some(&recommendation.evidence_refs.signal_candidate_id),
        recommendation_id: Some(&recommendation.recommendation_id),
    };
    let row_hash =
        CanonicalDigest::content_hash_json(&hash_input).expect("hash E2E funnel decision row");
    ReportMarketFunnelRow {
        event_time: report.decision_at.timestamp_millis(),
        recommendation_report_id: report.recommendation_report_id.clone(),
        market_selection_id: report.market_selection_id.clone(),
        profile_id: report.profile_ref.id.to_string(),
        profile_version: report.profile_ref.version,
        profile_content_hash: report.profile_ref.content_hash.to_string(),
        decision_policy_snapshot_id: report.decision_policy_snapshot_id.clone(),
        model_version_id: report.model_version_id.clone(),
        model_run_id: report.model_run_id.clone(),
        market_id: recommendation.market_id.clone(),
        event_id: recommendation.event_id.clone(),
        token_id: recommendation.token_id.clone(),
        terminal_stage: terminal_stage.as_str().to_owned(),
        primary_reason: primary_reason.as_str().to_owned(),
        secondary_diagnostics_json,
        feature_vector_id: Some(recommendation.evidence_refs.feature_vector_id.clone()),
        signal_candidate_id: Some(recommendation.evidence_refs.signal_candidate_id.clone()),
        recommendation_id: Some(recommendation.recommendation_id.clone()),
        row_hash: row_hash.to_string(),
        ingestion_time: report.decision_at.timestamp_millis(),
    }
}

struct E2eControlState {
    db: DatabaseConnection,
    books: Arc<BookStore>,
    fixtures: E2eFixtures,
}

#[derive(Deserialize)]
struct BookObservationRequest {
    best_ask: Price,
    observed_at: DateTime<Utc>,
    #[serde(default)]
    stale: bool,
}

#[derive(Serialize)]
struct BookObservationResponse {
    entry_condition_state: EntryConditionState,
    confirming_since: Option<DateTime<Utc>>,
    ready_at: Option<DateTime<Utc>>,
}

struct E2eConditionFixture {
    recommendation: RecommendationInfo,
    instance: EntryConditionInstanceInfo,
    artifact: EntryConditionArtifactInfo,
}

async fn get_fixtures(control: web::Data<E2eControlState>) -> web::Json<E2eFixtures> {
    web::Json(control.fixtures.clone())
}

async fn get_test_evidence(path: web::Path<(TradePolicyArtifactId, String)>) -> HttpResponse {
    let (artifact_id, kind) = path.into_inner();
    HttpResponse::Ok().json(serde_json::json!({
        "artifact_id": artifact_id,
        "kind": kind,
        "signed": true,
    }))
}

async fn observe_book(
    intent_id: web::Path<OrderIntentId>,
    request: web::Json<BookObservationRequest>,
    control: web::Data<E2eControlState>,
) -> HttpResponse {
    match observe_book_inner(&control, &intent_id, &request).await {
        Ok(response) => HttpResponse::Ok().json(response),
        Err(error) => HttpResponse::BadRequest().json(serde_json::json!({
            "error": error.to_string(),
        })),
    }
}

async fn observe_book_inner(
    control: &E2eControlState,
    intent_id: &OrderIntentId,
    request: &BookObservationRequest,
) -> QuantResult<BookObservationResponse> {
    let E2eConditionFixture {
        recommendation,
        mut instance,
        artifact,
    } = load_condition_fixture(control, intent_id).await?;
    let timestamp = if request.stale {
        Utc::now() - Duration::seconds(10)
    } else {
        Utc::now()
    };
    apply_book(
        &control.books,
        &recommendation.token_id,
        request.best_ask,
        timestamp,
    )?;
    let snapshot = control.books.load(&recommendation.token_id);
    let best_ask = snapshot
        .as_deref()
        .and_then(quant_pivot_models::domain::BookSnapshot::best_ask)
        .ok_or_else(|| QuantError::config("E2E condition book has no best ask"))?;
    let observed_at = if request.stale {
        request.observed_at - Duration::seconds(10)
    } else {
        request.observed_at
    };
    let evaluation = evaluate_entry_condition(
        &artifact.payload_json,
        &EntryConditionInputSet {
            binding: artifact.payload_json.binding.clone(),
            binding_revision: artifact.content_hash.clone(),
            binding_unavailable_reason: None,
            fold_state: instance.fold_state_json.clone(),
            evaluated_at: request.observed_at,
            prices: vec![ExecutablePriceInput {
                token_id: recommendation.token_id,
                price: best_ask,
                observed_at,
                available_at: observed_at,
                gap_generation: control.books.gap_generation(),
            }],
            factors: Vec::new(),
            crypto: Vec::new(),
            weather: Vec::new(),
        },
    )?;
    let decision = decide_entry_condition_state(
        instance.state,
        instance.confirmation_started_at,
        instance.continuity_hash.as_ref(),
        instance.last_evaluated_at,
        &artifact.payload_json,
        &evaluation,
        request.observed_at,
    );
    let worker_id = WorkerId::from_v7();
    instance = lease_condition_instance(
        &control.db,
        &instance,
        worker_id.clone(),
        request.observed_at,
    )
    .await?;
    let conditions = PgEntryConditionRepository::new(control.db.clone());
    let updated = conditions
        .apply_evaluation(
            &instance.condition_instance_id,
            worker_id.clone(),
            ApplyEntryConditionEvaluation {
                expected_revision: instance.revision,
                expected_lease_epoch: instance.lease_epoch,
                state: decision.state,
                truth: evaluation.truth,
                evaluation_hash: evaluation.evaluation_hash,
                input_fingerprint: evaluation.input_fingerprint,
                continuity_hash: evaluation.continuity_hash,
                fold_state: evaluation.fold_state,
                confirmation_started_at: decision.confirmation_started_at,
                evaluated_at: request.observed_at,
                next_evaluation_at: Some(request.observed_at + Duration::seconds(1)),
                evaluator_version: u32::try_from(artifact.evaluator_version).map_err(|error| {
                    QuantError::config(format!("invalid condition evaluator version: {error}"))
                })?,
                tree_json: serde_json::to_string(&evaluation.tree).map_err(|error| {
                    QuantError::config(format!("condition tree serialization failed: {error}"))
                })?,
            },
        )
        .await?;
    Ok(BookObservationResponse {
        entry_condition_state: updated.instance.state,
        confirming_since: updated.instance.confirmation_started_at,
        ready_at: (updated.instance.state == EntryConditionState::Qualified)
            .then_some(request.observed_at),
    })
}

async fn load_condition_fixture(
    control: &E2eControlState,
    intent_id: &OrderIntentId,
) -> QuantResult<E2eConditionFixture> {
    let intent = PgOrderIntentRepository::new(control.db.clone())
        .find_by_id(intent_id)
        .await?
        .ok_or_else(|| StorageError::NotFound {
            entity: "quant_order_intent",
            id: intent_id.to_string(),
        })?;
    let recommendation = PgRecommendationRepository::new(control.db.clone())
        .find_by_id(&intent.recommendation_id)
        .await?
        .ok_or_else(|| StorageError::NotFound {
            entity: "quant_recommendation",
            id: intent.recommendation_id.to_string(),
        })?;
    let conditions = PgEntryConditionRepository::new(control.db.clone());
    let instance = conditions
        .find_instance(&intent.condition_instance_id)
        .await?
        .ok_or_else(|| StorageError::NotFound {
            entity: "quant_entry_condition_instance",
            id: intent.condition_instance_id.to_string(),
        })?;
    let artifact_id = instance
        .artifact_id
        .clone()
        .ok_or_else(|| QuantError::config("E2E conditional intent instance has no artifact id"))?;
    let artifact = conditions
        .find_artifact(&artifact_id)
        .await?
        .ok_or_else(|| StorageError::NotFound {
            entity: "quant_entry_condition_artifact",
            id: artifact_id.to_string(),
        })?;
    Ok(E2eConditionFixture {
        recommendation,
        instance,
        artifact,
    })
}

async fn lease_condition_instance(
    db: &DatabaseConnection,
    instance: &EntryConditionInstanceInfo,
    worker_id: WorkerId,
    observed_at: DateTime<Utc>,
) -> QuantResult<EntryConditionInstanceInfo> {
    let row =
        quant_entry_condition_instance::Entity::find_by_id(instance.condition_instance_id.clone())
            .one(db)
            .await?
            .ok_or_else(|| StorageError::NotFound {
                entity: "quant_entry_condition_instance",
                id: instance.condition_instance_id.to_string(),
            })?;
    let mut active = row.into_active_model();
    active.lease_owner = ActiveValue::Set(Some(worker_id));
    active.lease_expires_at = ActiveValue::Set(Some(observed_at + Duration::seconds(15)));
    Ok(active.update(db).await?.into())
}

fn apply_book(
    books: &BookStore,
    token_id: &TokenId,
    best_ask: Price,
    observed_at: DateTime<Utc>,
) -> QuantResult<()> {
    let bid = Price::new((best_ask.inner() - dec!(0.01)).max(dec!(0.01)));
    let bid_level = BookLevel::from_decimal(bid, Shares::new(dec!(1000)))
        .map_err(|error| QuantError::config(format!("invalid E2E bid level: {error}")))?;
    let ask_level = BookLevel::from_decimal(best_ask, Shares::new(dec!(1000)))
        .map_err(|error| QuantError::config(format!("invalid E2E ask level: {error}")))?;
    let timestamp_ms = u64::try_from(observed_at.timestamp_millis()).unwrap_or(0);
    books.apply_snapshot(
        token_id,
        Arc::<[BookLevel]>::from([bid_level]),
        Arc::<[BookLevel]>::from([ask_level]),
        timestamp_ms,
        None,
    );
    Ok(())
}

fn record<'a>(records: &'a [DemoSeedRecord], slug: &str) -> &'a DemoSeedRecord {
    records
        .iter()
        .find(|record| record.slug == slug)
        .unwrap_or_else(|| panic!("missing UI E2E seed record `{slug}`"))
}

async fn seed_exit_reinference_observation(
    db: &DatabaseConnection,
    intent_id: &OrderIntentId,
    model_version_id: quant_pivot_models::types::ModelVersionId,
) {
    let now = Utc::now();
    PgExecutionSubmissionRepository::new(db.clone())
        .touch_exit_monitor(
            intent_id,
            now + Duration::seconds(30),
            Some(Price::new(dec!(0.74))),
            Some(now),
            Some(quant_pivot_models::types::ExitReinferenceObservation {
                observed_at: now,
                model_version_id,
                model_artifact_hash: quant_pivot_models::types::ContentHash::parse(concat!(
                    "blake3:",
                    "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
                ))
                .expect("fixture model hash"),
                factor_snapshot_hash: quant_pivot_models::types::ContentHash::parse(concat!(
                    "blake3:",
                    "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
                ))
                .expect("fixture factor snapshot hash"),
                mark: Price::new(dec!(0.69)),
                score: quant_pivot_models::types::Probability::new(dec!(0.67)),
                score_retention: dec!(0.82),
                expected_return_bps: quant_pivot_models::types::Bps::new(dec!(120)),
                execution_eligible: true,
                verdict: quant_pivot_models::types::ExitReinferenceVerdictKind::Holds,
                detail: "test-only governed reinference observation".to_owned(),
                shadow: false,
            }),
        )
        .await
        .expect("seed exit monitor observation");
}

async fn seed_policy_validation_dataset(
    db: &DatabaseConnection,
    artifact: &TradePolicyArtifactInfo,
    recommendation: &RecommendationInfo,
) {
    let datasets = PgTrainingDatasetRepository::new(db.clone());
    if datasets
        .find_by_id(&artifact.source_dataset_id)
        .await
        .expect("find policy validation dataset")
        .is_some()
    {
        return;
    }
    let models = PgModelRegistryRepository::new(db.clone());
    let version = models
        .find_model_version_by_id(&recommendation.evidence_refs.model_version_id)
        .await
        .expect("find policy validation model version")
        .expect("policy validation model version");
    let spec = models
        .find_model_spec_by_id(&version.model_spec_id)
        .await
        .expect("find policy validation model spec")
        .expect("policy validation model spec");
    let contract = &artifact.payload_json.fit_contract;
    let bundle = artifact
        .payload_json
        .evidence_bundle
        .as_ref()
        .expect("UI E2E policy evidence bundle");
    let sample_count = artifact
        .payload_json
        .pit_cutoff_evidence
        .as_ref()
        .map_or(1, |evidence| evidence.filtered_sample_count.max(1));
    let sample_interval_secs = 3_600_u64;
    let knowledge_lag_secs = 10_u64;
    let factor_schema_hash =
        ResearchHasher::canonical(&("ui_e2e_policy_fit_factor_schema_v1", &artifact.artifact_id))
            .expect("hash policy validation factor schema");
    let source_fingerprint = ResearchHasher::canonical(&(
        "ui_e2e_policy_fit_sources_v1",
        &artifact.artifact_id,
        &bundle.source_slice_manifest_hash,
    ))
    .expect("hash policy validation sources");
    let source_slice = SourceSliceManifestRef {
        manifest_uri: e2e_artifact_uri(&artifact.artifact_id, "manifest.json")
            .expect("policy validation Source Slice URI"),
        manifest_hash: bundle.source_slice_manifest_hash.clone(),
    };
    let manifest = DatasetManifest {
        format_version: DATASET_ARTIFACT_FORMAT_VERSION,
        training_dataset_id: artifact.source_dataset_id.clone(),
        profile_ref: contract.profile_ref.clone(),
        research_program_hash: contract.research_program_hash.clone(),
        source_slice,
        model_spec_id: version.model_spec_id.clone(),
        model_spec_definition_hash: spec.definition_hash.clone(),
        trade_policy_artifact_id: None,
        trade_policy_hash: None,
        decision_policy_snapshot_id: contract.decision_policy_snapshot_id.clone(),
        window_start: contract.fit_window_start,
        window_end: contract.fit_window_end,
        purpose: DatasetPurpose::PolicyFit,
        knowledge_lag_secs,
        sample_interval_secs,
        horizons_secs: vec![contract.target_horizon_secs],
        feature_schema_hash: artifact.payload_json.feature_schema_hash.clone(),
        factor_schema_hash: factor_schema_hash.clone(),
        label_schema_hash: artifact.payload_json.label_schema_hash.clone(),
        semantic_dataset_hash: artifact.payload_json.source_dataset_hash.clone(),
        source_fingerprint,
        sample_count,
    };
    let manifest_hash = dataset_manifest_hash(&manifest).expect("hash policy validation manifest");
    datasets
        .create_plan(NewTrainingDatasetPlan {
            training_dataset_id: artifact.source_dataset_id.clone(),
            model_spec_id: version.model_spec_id,
            model_spec_definition_hash: spec.definition_hash.clone(),
            window_start: contract.fit_window_start,
            window_end: contract.fit_window_end,
            purpose: DatasetPurpose::PolicyFit,
            knowledge_lag_secs: i64::try_from(knowledge_lag_secs)
                .expect("policy validation knowledge lag fits i64"),
            sample_interval_secs: i64::try_from(sample_interval_secs)
                .expect("policy validation sample interval fits i64"),
            horizons_secs: TrainingHorizonsSecs(vec![contract.target_horizon_secs]),
            feature_schema_version: Some(spec.feature_schema_version),
            sample_sources: Some(TrainingSampleSources(default_sample_sources())),
            decision_policy_snapshot_id: contract.decision_policy_snapshot_id.clone(),
        })
        .await
        .expect("create policy validation dataset");
    datasets
        .start_build(&artifact.source_dataset_id)
        .await
        .expect("start policy validation dataset");
    let sample_count_i64 =
        i64::try_from(sample_count).expect("policy validation sample count fits i64");
    datasets
        .complete_build(
            &artifact.source_dataset_id,
            CompleteTrainingDatasetBuild {
                status: TrainingDatasetStatus::Ready,
                feature_schema_hash: artifact.payload_json.feature_schema_hash.clone(),
                factor_schema_hash,
                label_schema_hash: artifact.payload_json.label_schema_hash.clone(),
                dataset_hash: artifact.payload_json.source_dataset_hash.clone(),
                manifest_hash,
                manifest_json: manifest,
                artifact_bytes_hash: ResearchHasher::canonical(&(
                    "ui_e2e_policy_fit_parquet_v1",
                    &artifact.artifact_id,
                ))
                .expect("hash policy validation parquet"),
                parquet_uri: e2e_artifact_uri(&artifact.artifact_id, "policy-fit.parquet")
                    .expect("policy validation parquet URI"),
                sample_count: sample_count_i64,
                coverage_json: DatasetCoverage {
                    planned_samples: sample_count,
                    built_examples: sample_count,
                    markets: 1,
                    labels_available: sample_count,
                    ..DatasetCoverage::default()
                },
                failure_detail: None,
            },
        )
        .await
        .expect("complete policy validation dataset");
}

async fn seed_policy_validation_diagnostic(
    db: &DatabaseConnection,
    artifact_id: &TradePolicyArtifactId,
    recommendation: &RecommendationInfo,
) {
    let policies = PgTradePolicyRepository::new(db.clone());
    let artifact = policies
        .find(artifact_id)
        .await
        .expect("load policy for validation diagnostic")
        .expect("policy validation diagnostic artifact");
    seed_policy_validation_dataset(db, &artifact, recommendation).await;
    let bundle = artifact
        .payload_json
        .evidence_bundle
        .as_ref()
        .expect("UI E2E policy evidence bundle");
    let validation_run_id = TradePolicyValidationRunId::from_v7();
    policies
        .begin_validation(NewTradePolicyValidationRun {
            validation_run_id: validation_run_id.clone(),
            artifact_id: artifact.artifact_id.clone(),
            artifact_hash: artifact.content_hash.clone(),
            source_dataset_id: artifact.source_dataset_id.clone(),
            source_dataset_hash: artifact.payload_json.source_dataset_hash.clone(),
            source_slice_manifest_hash: bundle.source_slice_manifest_hash.clone(),
            evidence_manifest_hash: bundle.manifest_hash.clone(),
            status: TradePolicyValidationStatus::Running,
            actor_id: UserId::new(Uuid::nil()),
            reason: "test-only independent row diagnostic".to_owned(),
        })
        .await
        .expect("begin policy validation diagnostic");
    let decision_at = artifact.payload_json.fit_contract.fit_window_end - Duration::seconds(1);
    let diagnostic_kind = "fee_evidence_mismatch".to_owned();
    let detail = "test-only row proves failed diagnostics remain inspectable".to_owned();
    let evidence_kind = "candidate_trials".to_owned();
    let record_key = format!("test-only:{}", recommendation.recommendation_id);
    let expected_row_hash = artifact.content_hash.clone();
    let actual_row_hash = artifact.payload_json.source_dataset_hash.clone();
    let row_hash = ResearchHasher::canonical(&(
        "ui_e2e_trade_policy_validation_row_v2",
        &validation_run_id,
        &evidence_kind,
        &record_key,
        &recommendation.market_id,
        &recommendation.token_id,
        decision_at,
        &expected_row_hash,
        &actual_row_hash,
        &diagnostic_kind,
        &detail,
    ))
    .expect("hash policy validation diagnostic row");
    policies
        .append_validation_rows(vec![NewTradePolicyValidationRow {
            validation_run_id: validation_run_id.clone(),
            row_ordinal: 0,
            evidence_kind,
            record_key,
            example_id: Some(TrainingExampleId::from_v7()),
            market_id: Some(recommendation.market_id.clone()),
            token_id: Some(recommendation.token_id.clone()),
            decision_at: Some(decision_at),
            expected_row_hash: Some(expected_row_hash),
            actual_row_hash: Some(actual_row_hash),
            passed: false,
            diagnostic_kind: Some(diagnostic_kind),
            detail: Some(detail),
            row_hash,
        }])
        .await
        .expect("append policy validation diagnostic row");
    let validation_hash = ResearchHasher::canonical(&(
        "ui_e2e_trade_policy_validation_failure_v1",
        &validation_run_id,
        &artifact.content_hash,
    ))
    .expect("hash policy validation diagnostic failure");
    policies
        .fail_validation(
            &validation_run_id,
            FailTradePolicyValidation {
                status: TradePolicyValidationStatus::Failed,
                validation_hash,
                failure_detail: "test-only validation failure retains immutable row diagnostics"
                    .to_owned(),
            },
        )
        .await
        .expect("complete failed policy validation diagnostic");
}

async fn prepare_e2e_fixtures(
    db: &DatabaseConnection,
    books: &BookStore,
    model_artifact_store: &Arc<dyn ArtifactStore>,
    quant_facts: &harness::MockQuantFactRead,
) -> E2eFixtures {
    let summary = seed_ui_demo_pg(
        db,
        "0x0000000000000000000000000000000000000001",
        model_artifact_store,
    )
    .await;
    let unavailable_recommendation_id = summary
        .actionable_recommendation_id
        .clone()
        .expect("UI seed must expose one recommendation without an intent");
    let unavailable =
        quant_recommendation::Entity::find_by_id(unavailable_recommendation_id.clone())
            .one(db)
            .await
            .expect("load unavailable recommendation fixture")
            .expect("unavailable recommendation fixture");
    let mut unavailable = unavailable.into_active_model();
    unavailable.trade_plan = ActiveValue::Set(RecommendationTradePlan::Unavailable {
        blockers: vec![TradePlanBlocker::CohortCoverageInsufficient],
    });
    unavailable
        .update(db)
        .await
        .expect("mark recommendation trade plan unavailable");

    let frozen_record = record(&summary.records, "active-conditional");
    let frozen = PgRecommendationRepository::new(db.clone())
        .find_by_id(&frozen_record.recommendation_id)
        .await
        .expect("load frozen recommendation")
        .expect("frozen recommendation");
    let (policy, _, _, _, _) = frozen
        .trade_plan
        .frozen()
        .expect("seeded recommendation must carry a Frozen trade plan");
    seed_policy_validation_diagnostic(db, &policy.artifact_id, &frozen).await;
    let pending_intent_id = frozen_record
        .intent_id
        .clone()
        .expect("pending intent fixture");
    let waiting_intent_id = pending_intent_id.clone();
    apply_book(books, &frozen.token_id, Price::new(dec!(0.60)), Utc::now())
        .expect("seed frozen recommendation book");

    let position_record = record(&summary.records, "filled-open");
    let position_id = position_record
        .position_id
        .clone()
        .expect("open position fixture");
    let position_intent_id = position_record
        .intent_id
        .clone()
        .expect("open position intent fixture");
    let position_recommendation = PgRecommendationRepository::new(db.clone())
        .find_by_id(&position_record.recommendation_id)
        .await
        .expect("load position recommendation")
        .expect("position recommendation");
    apply_book(
        books,
        &position_recommendation.token_id,
        Price::new(dec!(0.70)),
        Utc::now(),
    )
    .expect("seed open-position book");
    seed_exit_reinference_observation(
        db,
        &position_intent_id,
        position_recommendation
            .evidence_refs
            .model_version_id
            .clone(),
    )
    .await;

    let reports = PgRecommendationReportRepository::new(db.clone());
    let report = reports
        .find_by_id(&frozen_record.report_id)
        .await
        .expect("load E2E funnel report")
        .expect("E2E funnel report");
    quant_facts.replace_report_funnel(vec![e2e_published_funnel_row(&report, &frozen)]);
    verify_seeded_report_fact_deliveries(db).await;
    let current_report_id = reports
        .current(&report.profile_id, report.report_kind)
        .await
        .expect("load current E2E report authority")
        .expect("E2E report scope has current authority")
        .recommendation_report_id;
    let current_report_run_id = PgReportRunRepository::new(db.clone())
        .find_by_output_report(&current_report_id)
        .await
        .expect("load current E2E report run")
        .expect("current E2E report has a durable run")
        .report_run_id;

    E2eFixtures {
        fixture_format_version: 3,
        unavailable_recommendation_id,
        frozen_recommendation_id: frozen_record.recommendation_id.clone(),
        report_id: frozen_record.report_id.clone(),
        current_report_id,
        current_report_run_id,
        pending_intent_id,
        waiting_intent_id,
        position_id,
        model_version_id: frozen.evidence_refs.model_version_id,
        trade_policy_artifact_id: policy.artifact_id.clone(),
        trade_policy_content_hash: policy.artifact_hash.clone(),
    }
}

async fn verify_seeded_report_fact_deliveries(db: &DatabaseConnection) {
    // The E2E harness installs hash-valid in-memory funnel facts above. Move
    // seeded bundles through the repository CAS so protected routes exercise
    // the same verified-report visibility rule without a second ClickHouse
    // container in this web-layer suite.
    let reports = PgRecommendationReportRepository::new(db.clone());
    let worker_id = WorkerId::from_v7();
    loop {
        let delivery = reports
            .claim_fact_delivery(worker_id.clone(), 30)
            .await
            .expect("claim seeded report fact delivery");
        let Some(delivery) = delivery else {
            break;
        };
        reports
            .verify_and_publish_report(
                &delivery.recommendation_report_id,
                worker_id.clone(),
                Utc::now(),
            )
            .await
            .expect("verify seeded report fact delivery")
            .into_applied()
            .expect("seeded report delivery claim must remain held");
    }
}

#[actix_web::test]
#[ignore = "long-running Playwright backend; requires Docker"]
async fn serve_protected_ui_e2e() {
    let _ = tracing_subscriber::fmt()
        .with_env_filter(EnvFilter::from_default_env())
        .try_init();
    let mut env = harness::TestEnv::start_with_core_report_port().await;
    let books = Arc::new(BookStore::new(Arc::new(MetricsHub::new())));
    let fixtures = Box::pin(prepare_e2e_fixtures(
        &env.db,
        &books,
        &env.model_artifact_store,
        &env.quant_facts,
    ))
    .await;
    let mut runtime_config = DecisionPolicySnapshot::default();
    runtime_config
        .execution_authorization
        .semi_auto
        .canary
        .enabled = true;
    runtime_config
        .execution_authorization
        .semi_auto
        .canary
        .policy_artifact_id = Some(fixtures.trade_policy_artifact_id.to_string());
    runtime_config
        .execution_authorization
        .semi_auto
        .canary
        .policy_content_hash = Some(fixtures.trade_policy_content_hash.to_string());
    runtime_config
        .execution_authorization
        .semi_auto
        .canary
        .allowed_cash_budget_tiers_usd = vec![DecimalValue::new(rust_decimal_macros::dec!(25))];
    runtime_config
        .execution_authorization
        .semi_auto
        .canary
        .max_open_intents = 1;
    runtime_config
        .execution_authorization
        .semi_auto
        .canary
        .max_total_cash_per_report = DecimalValue::new(rust_decimal_macros::dec!(25));
    runtime_config
        .execution_authorization
        .semi_auto
        .canary
        .expires_at = Some((Utc::now() + Duration::hours(1)).to_rfc3339());
    env.order_intent_runtime_config.replace(runtime_config);
    env.state.market_data = Arc::new(E2eMarketData {
        books: Arc::clone(&books),
    });
    env.quant_facts.set_evaluation_outbox(env.db.clone());
    env.state.feature_integrity = Arc::new(E2eFeatureIntegrityPort);
    env.state.trade_policies = Arc::new(E2eTradePolicyPort::new(env.db.clone()));
    env.state.research_catalog = Arc::new(E2eResearchCatalogPort::new(env.db.clone()));
    env.state.model_training = Arc::new(E2eModelTrainingPort::new(env.db.clone()));

    let control = web::Data::new(E2eControlState {
        db: env.db.clone(),
        books,
        fixtures,
    });
    let state = web::Data::new(env.state.clone());
    let server = HttpServer::new(move || {
        App::new()
            .app_data(state.clone())
            .app_data(control.clone())
            .wrap(from_fn(middleware::request_id))
            .wrap(from_fn(middleware::operation_audit))
            .service(
                web::scope("/__test")
                    .route("/fixtures", web::get().to(get_fixtures))
                    .route(
                        "/evidence/{artifact_id}/{kind}",
                        web::get().to(get_test_evidence),
                    )
                    .route("/intents/{id}/book", web::post().to(observe_book)),
            )
            .configure(routes::configure)
    })
    .bind((LISTEN_HOST, LISTEN_PORT))
    .expect("bind protected UI E2E server")
    .run();

    eprintln!("protected UI E2E server ready at http://{LISTEN_HOST}:{LISTEN_PORT}");
    server.await.expect("run protected UI E2E server");
}

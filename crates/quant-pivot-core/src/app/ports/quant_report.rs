//! Core implementation of [`QuantReportPort`] for the report HTTP surface.
//!
//! This is the service boundary between the web handlers and the report plane:
//! reads go through the report / recommendation repositories, the ad-hoc run goes
//! through the durable run ledger, and revoke goes through the
//! [`ReportLifecycleService`] (transactional + post-commit event). Handlers
//! never touch a repository or a venue client directly.

use std::{
    collections::{
        BTreeMap, BTreeSet, HashMap, HashSet,
        hash_map::Entry::{Occupied, Vacant},
    },
    sync::Arc,
};

use async_trait::async_trait;
use chrono::{DateTime, Utc};
use quant_pivot_error::{QuantResult, report::ReportError, research::ResearchError};
use quant_pivot_models::{
    clickhouse::{
        QuantFeatureEventRow, QuantModelInputEventRow, QuantServingEvidenceCompletionRow,
        ReportMarketFunnelCountRow, ReportMarketFunnelRow,
    },
    domain::{
        AdHocReportCommand, DecisionBoundary, DecisionBoundaryEvidenceView,
        EnqueueReportRunOutcome, FeatureCellEvidenceView, FeatureVectorInfo,
        ModelInputEvidenceView, ModelRouteEvidenceView, OperationLogInfo, OperationLogQuery,
        PageWindow, Paginated, QuantEvidenceView, QuantRecommendationView,
        QuantReportDiagnosticsView, QuantReportFunnelView, QuantReportListQuery, QuantReportPort,
        RecommendationInfo, RecommendationReportInfo, RecommendationViewContext,
        ReportDataQualitySnapshotInfo, ReportDiagnosticsSubject, ReportDiff,
        ReportFactDeliveryInfo, ReportFunnelMarketListQuery, ReportFunnelMarketView,
        ReportFunnelStageView, ReportRunInfo, ReportRunListQuery, ReportScheduleGapInfo,
        ReportScheduleGapListQuery, ReportScheduleHealthInfo, ReportTimelineQuery,
        compute_report_diff,
    },
    enums::{
        operation_log::OperationCategory,
        quant::{EmptyReportReason, FeatureParityStage, RecommendationReportStatus, ReportKind},
        rbac::ResourceType,
    },
    types::{
        ContentHash, FeatureVectorId, ModelRunId, OrderIntentId, RecommendationId,
        RecommendationReportId, ReportFunnelReason, ReportFunnelStage, ReportRunId,
        ResearchProfileRef, TokenDataQualityRecord,
    },
};
use quant_pivot_repository::traits::{
    FeatureRepository, OperationLogRepository, OrderIntentRepository, PolicyRepository,
    QuantFactReadRepository, RecommendationReportRepository, RecommendationRepository,
    ReportRunRepository, ServingEvidenceRepository,
};

use crate::{
    observability::serving_evidence::verify_completion,
    report::{AdHocReportRequest, ReportLifecycleService, RetryAdHocReportRequest},
    service::durable_feature_parity::{persisted_capture, report_decision_boundary},
};

/// Web-facing report port assembled from the report plane.
pub struct CoreQuantReportPort {
    report_repo: Arc<dyn RecommendationReportRepository>,
    report_run_repo: Arc<dyn ReportRunRepository>,
    recommendation_repo: Arc<dyn RecommendationRepository>,
    order_intent_repo: Arc<dyn OrderIntentRepository>,
    lifecycle: Arc<ReportLifecycleService>,
    serving_evidence: Arc<dyn ServingEvidenceRepository>,
    feature_repo: Arc<dyn FeatureRepository>,
    runtime_config_repo: Arc<dyn PolicyRepository>,
    quant_fact_read: Arc<dyn QuantFactReadRepository>,
    operation_logs: Arc<dyn OperationLogRepository>,
}

/// Explicit dependency bundle for [`CoreQuantReportPort`].
///
/// Keeping the report-plane services and evidence repositories in one typed
/// bundle makes construction auditable without relying on positional wiring.
pub struct CoreQuantReportPortDeps {
    pub report_repo: Arc<dyn RecommendationReportRepository>,
    pub report_run_repo: Arc<dyn ReportRunRepository>,
    pub recommendation_repo: Arc<dyn RecommendationRepository>,
    pub order_intent_repo: Arc<dyn OrderIntentRepository>,
    pub lifecycle: Arc<ReportLifecycleService>,
    pub serving_evidence: Arc<dyn ServingEvidenceRepository>,
    pub feature_repo: Arc<dyn FeatureRepository>,
    pub runtime_config_repo: Arc<dyn PolicyRepository>,
    pub quant_fact_read: Arc<dyn QuantFactReadRepository>,
    pub operation_logs: Arc<dyn OperationLogRepository>,
}

impl CoreQuantReportPort {
    /// Assemble the port from the report-plane services and read repositories.
    #[must_use]
    pub fn new(deps: CoreQuantReportPortDeps) -> Self {
        Self {
            report_repo: deps.report_repo,
            report_run_repo: deps.report_run_repo,
            recommendation_repo: deps.recommendation_repo,
            order_intent_repo: deps.order_intent_repo,
            lifecycle: deps.lifecycle,
            serving_evidence: deps.serving_evidence,
            feature_repo: deps.feature_repo,
            runtime_config_repo: deps.runtime_config_repo,
            quant_fact_read: deps.quant_fact_read,
            operation_logs: deps.operation_logs,
        }
    }

    /// Project a recommendation into its outbound view, joining the parent
    /// report's lifecycle status and any blocking pre-submission intent.
    fn assemble_view(
        recommendation: RecommendationInfo,
        report_status: RecommendationReportStatus,
        active_order_intent_id: Option<OrderIntentId>,
    ) -> QuantRecommendationView {
        RecommendationViewContext {
            recommendation,
            report_status,
            active_order_intent_id,
        }
        .into()
    }

    async fn load_serving_evidence(
        &self,
        model_run_id: &ModelRunId,
    ) -> QuantResult<RunServingEvidence> {
        let run_ids = [model_run_id.clone()];
        let marker =
            canonical_completion(self.serving_evidence.completions_for_runs(&run_ids).await?)?;
        let model_inputs = self
            .serving_evidence
            .model_inputs_for_runs(&run_ids)
            .await?;
        let vector_ids = match marker.as_ref() {
            Some(marker) => completion_vector_ids(marker)?,
            None => unique_feature_vector_ids(&model_inputs),
        };
        let feature_cells = self
            .serving_evidence
            .feature_cells_for_vectors(&vector_ids)
            .await?;
        if let Some(marker) = marker.as_ref() {
            verify_completion(marker, &feature_cells, &model_inputs)?;
        }
        Ok(RunServingEvidence {
            complete: marker.is_some(),
            feature_cells,
            model_inputs,
        })
    }

    async fn load_report_boundary(
        &self,
        report: &RecommendationReportInfo,
    ) -> QuantResult<DecisionBoundary> {
        let version = self
            .runtime_config_repo
            .load_snapshot(&report.decision_policy_snapshot_id)
            .await?
            .ok_or_else(|| ResearchError::Determinism {
                detail: format!(
                    "report {} references missing runtime config {}",
                    report.recommendation_report_id, report.decision_policy_snapshot_id
                ),
            })?;
        let config = version.snapshot;
        let run = self
            .report_run_repo
            .find_by_output_report(&report.recommendation_report_id)
            .await?
            .ok_or_else(|| ResearchError::Determinism {
                detail: format!(
                    "report {} has no successful report-run lineage",
                    report.recommendation_report_id
                ),
            })?;
        report_decision_boundary(report, &run, &config)
    }

    async fn load_report_data_quality(
        &self,
        report: &RecommendationReportInfo,
    ) -> QuantResult<ReportDataQualitySnapshotInfo> {
        let snapshot = self
            .report_repo
            .find_data_quality_snapshot(&report.recommendation_report_id)
            .await?
            .ok_or_else(|| ResearchError::Determinism {
                detail: format!(
                    "report {} is missing data-quality snapshot {}",
                    report.recommendation_report_id, report.data_quality_snapshot_ref
                ),
            })?;
        let snapshot_matches =
            snapshot.report_data_quality_snapshot_id == report.data_quality_snapshot_ref;
        let decision_matches = snapshot.decision_at == report.decision_at;
        let config_matches =
            snapshot.decision_policy_snapshot_id == report.decision_policy_snapshot_id;
        if !snapshot_matches || !decision_matches || !config_matches {
            return Err(ResearchError::Determinism {
                detail: format!(
                    "report {} data-quality snapshot is not bound to its decision/config",
                    report.recommendation_report_id
                ),
            }
            .into());
        }
        Ok(snapshot)
    }

    async fn load_pre_inference_features(
        &self,
        report: &RecommendationReportInfo,
        snapshot: &ReportDataQualitySnapshotInfo,
        boundary: &DecisionBoundary,
    ) -> QuantResult<PreInferenceFeatureEvidence> {
        let vector_ids = pre_inference_vector_ids(report, snapshot)?;

        let infos = self.feature_repo.find_by_ids(&vector_ids).await?;
        let mut infos_by_id = infos
            .into_iter()
            .map(|info| (info.feature_vector_id.clone(), info))
            .collect::<HashMap<_, _>>();
        if infos_by_id.len() != vector_ids.len() {
            return Err(ResearchError::Determinism {
                detail: format!(
                    "report {} data-quality snapshot references missing feature vectors",
                    report.recommendation_report_id
                ),
            }
            .into());
        }

        let cells = latest_feature_cells(
            self.serving_evidence
                .feature_cells_for_vectors(&vector_ids)
                .await?,
        );
        let observed_boundary = decision_boundary(&cells)?;
        let expected_boundary = DecisionBoundaryEvidenceView::from(boundary);
        if observed_boundary
            .as_ref()
            .is_some_and(|observed| observed != &expected_boundary)
        {
            return Err(ResearchError::Determinism {
                detail: format!(
                    "report {} feature evidence does not match its frozen decision boundary",
                    report.recommendation_report_id
                ),
            }
            .into());
        }

        let mut cells_by_vector = HashMap::<FeatureVectorId, Vec<QuantFeatureEventRow>>::new();
        for row in cells.iter().cloned() {
            cells_by_vector
                .entry(row.feature_vector_id.clone())
                .or_default()
                .push(row);
        }

        let mut complete = true;
        for token in &snapshot.tokens_json.0 {
            let vector_id =
                token
                    .feature_vector_id
                    .as_ref()
                    .ok_or_else(|| ResearchError::Determinism {
                        detail: "validated data-quality row lost its feature-vector binding"
                            .to_owned(),
                    })?;
            let info = infos_by_id
                .remove(vector_id)
                .ok_or_else(|| ResearchError::Determinism {
                    detail: format!("feature vector {vector_id} disappeared during diagnostics"),
                })?;
            let rows = cells_by_vector
                .get(vector_id)
                .map_or(&[][..], Vec::as_slice);
            validate_pre_inference_vector(report, token, &info, rows, boundary)?;
            let expected_names = persisted_feature_names(&info)?;
            let actual_names = rows
                .iter()
                .map(|row| row.feature_name.clone())
                .collect::<BTreeSet<_>>();
            if !actual_names.is_subset(&expected_names) {
                return Err(ResearchError::Determinism {
                    detail: format!(
                        "feature vector {vector_id} emitted cells outside its persisted payload"
                    ),
                }
                .into());
            }
            complete &= actual_names == expected_names;
        }

        Ok(PreInferenceFeatureEvidence {
            capture_count: checked_len(vector_ids.len(), "decision capture")?,
            cells,
            complete,
            vector_count: checked_len(vector_ids.len(), "feature vector")?,
        })
    }
}

struct PreInferenceFeatureEvidence {
    capture_count: u64,
    cells: Vec<QuantFeatureEventRow>,
    complete: bool,
    vector_count: u64,
}

fn pre_inference_vector_ids(
    report: &RecommendationReportInfo,
    snapshot: &ReportDataQualitySnapshotInfo,
) -> QuantResult<Vec<FeatureVectorId>> {
    let mut vector_ids = Vec::with_capacity(snapshot.tokens_json.0.len());
    let mut unique_ids = HashSet::with_capacity(snapshot.tokens_json.0.len());
    let mut unique_markets = HashSet::with_capacity(snapshot.tokens_json.0.len());
    for token in &snapshot.tokens_json.0 {
        let vector_id =
            token
                .feature_vector_id
                .clone()
                .ok_or_else(|| ResearchError::Determinism {
                    detail: format!(
                        "report {} contains legacy-unbound data-quality evidence",
                        report.recommendation_report_id
                    ),
                })?;
        if !unique_ids.insert(vector_id.clone()) || !unique_markets.insert(token.market_id.clone())
        {
            return Err(ResearchError::Determinism {
                detail: format!(
                    "report {} data-quality evidence contains duplicate vector or market bindings",
                    report.recommendation_report_id
                ),
            }
            .into());
        }
        vector_ids.push(vector_id);
    }
    Ok(vector_ids)
}

#[async_trait]
impl QuantReportPort for CoreQuantReportPort {
    async fn list_reports(
        &self,
        query: QuantReportListQuery,
    ) -> QuantResult<Paginated<RecommendationReportInfo>> {
        Ok(self.report_repo.page(query).await?)
    }

    async fn find_report(
        &self,
        report_id: &RecommendationReportId,
    ) -> QuantResult<Option<RecommendationReportInfo>> {
        Ok(self.report_repo.find_by_id(report_id).await?)
    }

    async fn find_report_predecessor_id(
        &self,
        report_id: &RecommendationReportId,
    ) -> QuantResult<Option<RecommendationReportId>> {
        Ok(self.report_repo.find_predecessor_id(report_id).await?)
    }

    async fn find_report_fact_delivery(
        &self,
        report_id: &RecommendationReportId,
    ) -> QuantResult<Option<ReportFactDeliveryInfo>> {
        Ok(self.report_repo.find_fact_delivery(report_id).await?)
    }

    async fn find_report_run(
        &self,
        report_id: &RecommendationReportId,
    ) -> QuantResult<Option<ReportRunInfo>> {
        Ok(self
            .report_run_repo
            .find_by_output_report(report_id)
            .await?)
    }

    async fn list_report_runs(
        &self,
        query: ReportRunListQuery,
    ) -> QuantResult<Paginated<ReportRunInfo>> {
        Ok(self.report_run_repo.page(query).await?)
    }

    async fn find_report_run_by_id(
        &self,
        run_id: &ReportRunId,
    ) -> QuantResult<Option<ReportRunInfo>> {
        Ok(self.report_run_repo.find_by_id(run_id).await?)
    }

    async fn retry_report_run(
        &self,
        run_id: &ReportRunId,
        request_id: &str,
    ) -> QuantResult<EnqueueReportRunOutcome> {
        self.lifecycle
            .retry_ad_hoc(RetryAdHocReportRequest {
                source_run_id: run_id.clone(),
                request_id: request_id.to_owned(),
                requested_at: Utc::now(),
            })
            .await
    }

    async fn report_schedule_health(&self) -> QuantResult<ReportScheduleHealthInfo> {
        Ok(self.report_run_repo.schedule_health().await?)
    }

    async fn list_report_schedule_gaps(
        &self,
        query: ReportScheduleGapListQuery,
    ) -> QuantResult<Paginated<ReportScheduleGapInfo>> {
        Ok(self.report_run_repo.page_schedule_gaps(query).await?)
    }

    async fn retry_report_publication(
        &self,
        report_id: &RecommendationReportId,
    ) -> QuantResult<ReportFactDeliveryInfo> {
        self.lifecycle
            .retry_publication(report_id, Utc::now())
            .await
    }

    async fn report_timeline(
        &self,
        report_id: &RecommendationReportId,
        query: ReportTimelineQuery,
    ) -> QuantResult<Option<Paginated<OperationLogInfo>>> {
        if self.report_repo.find_by_id(report_id).await?.is_none() {
            return Ok(None);
        }
        Ok(Some(
            self.operation_logs
                .page(OperationLogQuery {
                    category: Some(OperationCategory::QuantReport),
                    resource_type: Some(ResourceType::QuantReport),
                    resource_id: Some(report_id.to_string()),
                    from: query.from,
                    to: query.to,
                    page: query.page,
                    ..Default::default()
                })
                .await?,
        ))
    }

    async fn find_report_diagnostics(
        &self,
        report_id: &RecommendationReportId,
    ) -> QuantResult<Option<QuantReportDiagnosticsView>> {
        let Some(report) = self.report_repo.find_by_id(report_id).await? else {
            return Ok(None);
        };
        let boundary = self.load_report_boundary(&report).await?;
        let data_quality = self.load_report_data_quality(&report).await?;
        let (_, stage_ceiling) = report_diagnostics_execution(
            report.model_run_id.as_ref(),
            report.summary_json.empty_reason,
        )?;
        let selection_count = u64::from(report.summary_json.market_selection_count);
        let Some(model_run_id) = report.model_run_id.as_ref() else {
            return Ok(Some(match stage_ceiling {
                FeatureParityStage::Selection => {
                    if !data_quality.tokens_json.0.is_empty() {
                        return Err(ResearchError::Determinism {
                            detail: format!(
                                "selection-only report {} unexpectedly contains feature-stage data-quality rows",
                                report.recommendation_report_id
                            ),
                        }
                        .into());
                    }
                    pre_inference_selection_diagnostics(&boundary, selection_count)
                }
                FeatureParityStage::DataQuality => {
                    if data_quality.tokens_json.0.is_empty() {
                        return Err(ResearchError::Determinism {
                            detail: format!(
                                "data-quality report {} has no bound feature vectors",
                                report.recommendation_report_id
                            ),
                        }
                        .into());
                    }
                    let evidence = self
                        .load_pre_inference_features(&report, &data_quality, &boundary)
                        .await?;
                    pre_inference_data_quality_diagnostics(&boundary, selection_count, evidence)?
                }
                other => {
                    return Err(ResearchError::Determinism {
                        detail: format!(
                            "pre-inference report {} has invalid stage ceiling {other:?}",
                            report.recommendation_report_id
                        ),
                    }
                    .into());
                }
            }));
        };
        let evidence = self.load_serving_evidence(model_run_id).await?;
        if evidence.model_inputs.iter().any(|row| {
            row.recommendation_report_id.as_ref() != Some(report_id)
                || &row.model_run_id != model_run_id
                || row.model_version_id != report.model_version_id
        }) {
            return Err(ResearchError::Determinism {
                detail: format!(
                    "serving evidence route does not match report {}",
                    report.recommendation_report_id
                ),
            }
            .into());
        }
        let inputs = evidence.model_inputs;
        Ok(Some(model_run_diagnostics(
            evidence.complete,
            &inputs,
            &evidence.feature_cells,
            &boundary,
            selection_count,
        )?))
    }

    async fn find_report_funnel(
        &self,
        report_id: &RecommendationReportId,
    ) -> QuantResult<Option<QuantReportFunnelView>> {
        if self.report_repo.find_by_id(report_id).await?.is_none() {
            return Ok(None);
        }
        let counts = self
            .quant_fact_read
            .report_market_funnel_counts(report_id)
            .await?;
        Ok(Some(funnel_summary(report_id.clone(), counts)?))
    }

    async fn page_report_funnel_markets(
        &self,
        report_id: &RecommendationReportId,
        query: ReportFunnelMarketListQuery,
    ) -> QuantResult<Option<Paginated<ReportFunnelMarketView>>> {
        let Some(report) = self.report_repo.find_by_id(report_id).await? else {
            return Ok(None);
        };
        let window = PageWindow::from_query(&query);
        let terminal_stage = query.terminal_stage.map(ReportFunnelStage::as_str);
        let primary_reason = query.primary_reason.map(ReportFunnelReason::as_str);
        let total = self
            .quant_fact_read
            .report_market_funnel_count(report_id, terminal_stage, primary_reason)
            .await?;
        let rows = self
            .quant_fact_read
            .report_market_funnel_page(
                report_id,
                terminal_stage,
                primary_reason,
                window.offset(),
                window.size(),
            )
            .await?;
        let items = rows
            .into_iter()
            .map(|row| funnel_market_view(&report, row))
            .collect::<QuantResult<Vec<_>>>()?;
        Ok(Some(Paginated::from_window(items, total, window)))
    }

    async fn current_report(
        &self,
        profile_id: &str,
        kind: ReportKind,
    ) -> QuantResult<Option<RecommendationReportInfo>> {
        Ok(self.report_repo.current(profile_id, kind).await?)
    }

    async fn find_recommendations(
        &self,
        report_id: &RecommendationReportId,
    ) -> QuantResult<Vec<QuantRecommendationView>> {
        let recommendations = self.recommendation_repo.find_by_report(report_id).await?;
        if recommendations.is_empty() {
            return Ok(Vec::new());
        }
        // FK guarantees the report exists when it has recommendations; if it is
        // somehow absent, fall back to the persisted per-recommendation status.
        let Some(report) = self.report_repo.find_by_id(report_id).await? else {
            return Ok(Vec::new());
        };
        let blocking = self
            .order_intent_repo
            .find_blocking_by_report(report_id)
            .await?
            .into_iter()
            .map(|intent| (intent.recommendation_id, intent.order_intent_id))
            .collect::<HashMap<_, _>>();
        Ok(recommendations
            .into_iter()
            .map(|rec| {
                let active = blocking.get(&rec.recommendation_id).cloned();
                Self::assemble_view(rec, report.status, active)
            })
            .collect())
    }

    async fn find_recommendation(
        &self,
        recommendation_id: &RecommendationId,
    ) -> QuantResult<Option<QuantRecommendationView>> {
        let Some(recommendation) = self
            .recommendation_repo
            .find_by_id(recommendation_id)
            .await?
        else {
            return Ok(None);
        };
        let Some(report) = self
            .report_repo
            .find_by_id(&recommendation.recommendation_report_id)
            .await?
        else {
            return Ok(None);
        };
        let active_order_intent_id = self
            .order_intent_repo
            .find_active_by_recommendation(recommendation_id)
            .await?
            .map(|intent| intent.order_intent_id);
        Ok(Some(Self::assemble_view(
            recommendation,
            report.status,
            active_order_intent_id,
        )))
    }

    async fn find_evidence(
        &self,
        recommendation_id: &RecommendationId,
    ) -> QuantResult<Option<QuantEvidenceView>> {
        let Some(recommendation) = self
            .recommendation_repo
            .find_by_id(recommendation_id)
            .await?
        else {
            return Ok(None);
        };
        let evidence_refs = recommendation.evidence_refs.clone();
        let evidence = self
            .load_serving_evidence(&evidence_refs.model_run_id)
            .await?;
        let inputs = evidence
            .model_inputs
            .into_iter()
            .filter(|row| {
                row.recommendation_report_id.as_ref()
                    == Some(&recommendation.recommendation_report_id)
                    && row.market_id == recommendation.market_id
                    && row.feature_vector_id == evidence_refs.feature_vector_id
                    && row.model_version_id == evidence_refs.model_version_id
            })
            .collect::<Vec<_>>();
        let features = evidence
            .feature_cells
            .into_iter()
            .filter(|row| {
                row.market_id == recommendation.market_id
                    && row.feature_vector_id == evidence_refs.feature_vector_id
            })
            .collect::<Vec<_>>();
        if evidence.complete && (inputs.is_empty() || features.is_empty()) {
            return Err(ResearchError::Determinism {
                detail: format!(
                    "completed serving run has no bound evidence for recommendation {}",
                    recommendation.recommendation_id
                ),
            }
            .into());
        }
        let mut view = QuantEvidenceView::from(recommendation);
        view.evidence_complete = evidence.complete;
        view.decision_boundary = decision_boundary(&features)?;
        view.feature_schema_hash = consistent_string(&features, |row| &row.feature_schema_hash)?;
        view.feature_hash = consistent_string(&features, |row| &row.feature_hash)?;
        view.feature_cells = latest_feature_cells(features)
            .into_iter()
            .map(feature_cell_view)
            .collect::<QuantResult<Vec<_>>>()?;
        view.model_inputs = latest_model_inputs(inputs)
            .into_iter()
            .map(model_input_view)
            .collect();
        Ok(Some(view))
    }

    async fn diff_reports(
        &self,
        base_report_id: &RecommendationReportId,
        compare_report_id: &RecommendationReportId,
    ) -> QuantResult<Option<ReportDiff>> {
        let (Some(base), Some(compare)) = (
            self.report_repo.find_by_id(base_report_id).await?,
            self.report_repo.find_by_id(compare_report_id).await?,
        ) else {
            return Ok(None);
        };
        if base.profile_id != compare.profile_id || base.report_kind != compare.report_kind {
            return Err(ReportError::IncomparableReports {
                detail: format!(
                    "{}:{} cannot be compared with {}:{}",
                    base.profile_id, base.report_kind, compare.profile_id, compare.report_kind,
                ),
            }
            .into());
        }
        let base_recs = self
            .recommendation_repo
            .find_by_report(base_report_id)
            .await?;
        let compare_recs = self
            .recommendation_repo
            .find_by_report(compare_report_id)
            .await?;
        Ok(Some(compute_report_diff(
            &base,
            &base_recs,
            &compare,
            &compare_recs,
        )))
    }

    async fn enqueue_ad_hoc(
        &self,
        command: AdHocReportCommand,
    ) -> QuantResult<EnqueueReportRunOutcome> {
        let trigger_time = Utc::now();
        self.lifecycle
            .run_ad_hoc(AdHocReportRequest {
                request_id: command.request_id,
                trigger_time,
                top_n: command.top_n,
                knowledge_lag_secs: command.knowledge_lag_secs,
            })
            .await
    }

    async fn revoke(
        &self,
        report_id: &RecommendationReportId,
        reason: &str,
    ) -> QuantResult<RecommendationReportInfo> {
        self.lifecycle.revoke(report_id, reason, Utc::now()).await
    }
}

fn funnel_summary(
    report_id: RecommendationReportId,
    rows: Vec<ReportMarketFunnelCountRow>,
) -> QuantResult<QuantReportFunnelView> {
    let mut counts = BTreeMap::new();
    for row in rows {
        let stage = row
            .terminal_stage
            .parse::<ReportFunnelStage>()
            .map_err(|detail| ResearchError::Determinism {
                detail: format!("report {report_id} funnel contains {detail}"),
            })?;
        if counts.insert(stage, row.row_count).is_some() {
            return Err(ResearchError::Determinism {
                detail: format!("report {report_id} funnel contains duplicate stage counts"),
            }
            .into());
        }
    }
    let checked_sum = |values: Vec<u64>| -> QuantResult<u64> {
        values.into_iter().try_fold(0_u64, |sum, value| {
            sum.checked_add(value).ok_or_else(|| {
                ResearchError::Determinism {
                    detail: format!("report {report_id} funnel count overflow"),
                }
                .into()
            })
        })
    };
    let catalog_visible_count = checked_sum(counts.values().copied().collect())?;
    let mut stages = Vec::with_capacity(ReportFunnelStage::ALL.len());
    let mut prior_output = None;
    let mut conserved = true;
    for stage in ReportFunnelStage::ALL {
        let input_count = if stage == ReportFunnelStage::Published {
            counts.get(&stage).copied().unwrap_or(0)
        } else {
            checked_sum(
                counts
                    .iter()
                    .filter_map(|(terminal, count)| (*terminal >= stage).then_some(*count))
                    .collect(),
            )?
        };
        let excluded_count = if stage == ReportFunnelStage::Published {
            0
        } else {
            counts.get(&stage).copied().unwrap_or(0)
        };
        let output_count =
            input_count
                .checked_sub(excluded_count)
                .ok_or_else(|| ResearchError::Determinism {
                    detail: format!("report {report_id} funnel stage subtraction underflow"),
                })?;
        conserved &= input_count == output_count + excluded_count;
        if let Some(prior) = prior_output {
            conserved &= prior == input_count;
        }
        prior_output = Some(output_count);
        stages.push(ReportFunnelStageView {
            stage,
            input_count,
            output_count,
            excluded_count,
        });
    }
    let published_count = counts
        .get(&ReportFunnelStage::Published)
        .copied()
        .unwrap_or(0);
    Ok(QuantReportFunnelView {
        recommendation_report_id: report_id,
        catalog_visible_count,
        published_count,
        conserved,
        stages,
    })
}

fn funnel_market_view(
    report: &RecommendationReportInfo,
    row: ReportMarketFunnelRow,
) -> QuantResult<ReportFunnelMarketView> {
    let profile_content_hash =
        row.profile_content_hash
            .parse::<ContentHash>()
            .map_err(|error| ResearchError::Determinism {
                detail: format!("invalid funnel profile hash: {error}"),
            })?;
    let profile_ref = ResearchProfileRef {
        id: row.profile_id,
        version: row.profile_version,
        content_hash: profile_content_hash,
    };
    if row.recommendation_report_id != report.recommendation_report_id
        || row.market_selection_id != report.market_selection_id
        || row.decision_policy_snapshot_id != report.decision_policy_snapshot_id
        || row.model_version_id != report.model_version_id
        || profile_ref != report.profile_ref
    {
        return Err(ResearchError::Determinism {
            detail: format!(
                "report {} funnel row lineage does not match its report header",
                report.recommendation_report_id
            ),
        }
        .into());
    }
    let terminal_stage =
        row.terminal_stage
            .parse()
            .map_err(|detail| ResearchError::Determinism {
                detail: format!("report funnel {detail}"),
            })?;
    let primary_reason =
        row.primary_reason
            .parse()
            .map_err(|detail| ResearchError::Determinism {
                detail: format!("report funnel {detail}"),
            })?;
    let secondary_diagnostics =
        serde_json::from_str(&row.secondary_diagnostics_json).map_err(|error| {
            ResearchError::Determinism {
                detail: format!("invalid report funnel secondary diagnostics: {error}"),
            }
        })?;
    let row_hash =
        row.row_hash
            .parse::<ContentHash>()
            .map_err(|error| ResearchError::Determinism {
                detail: format!("invalid report funnel row hash: {error}"),
            })?;
    Ok(ReportFunnelMarketView {
        recommendation_report_id: row.recommendation_report_id,
        market_selection_id: row.market_selection_id,
        profile_ref,
        decision_policy_snapshot_id: row.decision_policy_snapshot_id,
        model_version_id: row.model_version_id,
        model_run_id: row.model_run_id,
        market_id: row.market_id,
        event_id: row.event_id,
        token_id: row.token_id,
        terminal_stage,
        primary_reason,
        secondary_diagnostics,
        feature_vector_id: row.feature_vector_id,
        signal_candidate_id: row.signal_candidate_id,
        recommendation_id: row.recommendation_id,
        row_hash,
    })
}

struct RunServingEvidence {
    complete: bool,
    feature_cells: Vec<QuantFeatureEventRow>,
    model_inputs: Vec<QuantModelInputEventRow>,
}

fn canonical_completion(
    rows: Vec<QuantServingEvidenceCompletionRow>,
) -> QuantResult<Option<QuantServingEvidenceCompletionRow>> {
    let mut selected: Option<QuantServingEvidenceCompletionRow> = None;
    for row in rows {
        if let Some(current) = selected.as_ref()
            && current.completion_hash != row.completion_hash
        {
            return Err(ResearchError::Determinism {
                detail: format!(
                    "conflicting serving evidence completion markers for run {}",
                    row.model_run_id
                ),
            }
            .into());
        }
        if selected
            .as_ref()
            .is_none_or(|current| row.ingestion_time >= current.ingestion_time)
        {
            selected = Some(row);
        }
    }
    Ok(selected)
}

fn completion_vector_ids(
    marker: &QuantServingEvidenceCompletionRow,
) -> QuantResult<Vec<FeatureVectorId>> {
    serde_json::from_str(&marker.feature_vector_ids_json).map_err(|error| {
        ResearchError::Serialization {
            detail: format!(
                "invalid serving evidence vector ids for run {}: {error}",
                marker.model_run_id
            ),
        }
        .into()
    })
}

fn report_diagnostics_execution(
    model_run_id: Option<&ModelRunId>,
    empty_reason: Option<EmptyReportReason>,
) -> QuantResult<(ReportDiagnosticsSubject, FeatureParityStage)> {
    if model_run_id.is_some() {
        return Ok((
            ReportDiagnosticsSubject::ModelRun,
            FeatureParityStage::Prediction,
        ));
    }
    match empty_reason {
        Some(EmptyReportReason::EmptySelection | EmptyReportReason::SystemDegraded) => Ok((
            ReportDiagnosticsSubject::PreInferenceReport,
            FeatureParityStage::Selection,
        )),
        Some(EmptyReportReason::InsufficientDataQuality) => Ok((
            ReportDiagnosticsSubject::PreInferenceReport,
            FeatureParityStage::DataQuality,
        )),
        Some(reason) => Err(ResearchError::Determinism {
            detail: format!(
                "report without model_run_id cannot have post-inference empty reason {reason:?}"
            ),
        }
        .into()),
        None => Err(ResearchError::Determinism {
            detail: "report without model_run_id must declare a pre-inference empty reason"
                .to_owned(),
        }
        .into()),
    }
}

fn pre_inference_selection_diagnostics(
    boundary: &DecisionBoundary,
    selection_count: u64,
) -> QuantReportDiagnosticsView {
    QuantReportDiagnosticsView {
        subject: ReportDiagnosticsSubject::PreInferenceReport,
        stage_ceiling: FeatureParityStage::Selection,
        evidence_complete: true,
        decision_boundary: Some(DecisionBoundaryEvidenceView::from(boundary)),
        model_route: None,
        selection_count,
        decision_capture_count: None,
        feature_vector_count: None,
        feature_state_counts: None,
        feature_cell_count: None,
        model_input_state_counts: None,
        model_input_count: None,
    }
}

fn pre_inference_data_quality_diagnostics(
    boundary: &DecisionBoundary,
    selection_count: u64,
    evidence: PreInferenceFeatureEvidence,
) -> QuantResult<QuantReportDiagnosticsView> {
    let feature_rows = evidence.cells;
    let feature_state_counts = (!feature_rows.is_empty())
        .then(|| counts_by(feature_rows.iter().map(|row| row.cell_state.as_wire())));
    let feature_cell_count = non_empty_count(&feature_rows, "feature cell")?;
    Ok(QuantReportDiagnosticsView {
        subject: ReportDiagnosticsSubject::PreInferenceReport,
        stage_ceiling: FeatureParityStage::DataQuality,
        evidence_complete: evidence.complete,
        decision_boundary: Some(DecisionBoundaryEvidenceView::from(boundary)),
        model_route: None,
        selection_count,
        decision_capture_count: Some(evidence.capture_count),
        feature_vector_count: Some(evidence.vector_count),
        feature_state_counts,
        feature_cell_count,
        model_input_state_counts: None,
        model_input_count: None,
    })
}

fn model_run_diagnostics(
    evidence_complete: bool,
    inputs: &[QuantModelInputEventRow],
    features: &[QuantFeatureEventRow],
    boundary: &DecisionBoundary,
    selection_count: u64,
) -> QuantResult<QuantReportDiagnosticsView> {
    let feature_rows = latest_feature_cells(features.to_vec());
    let input_rows = latest_model_inputs(inputs.to_vec());
    let observed_boundary = decision_boundary(&feature_rows)?;
    let expected_boundary = DecisionBoundaryEvidenceView::from(boundary);
    if observed_boundary
        .as_ref()
        .is_some_and(|observed| observed != &expected_boundary)
    {
        return Err(ResearchError::Determinism {
            detail: "serving evidence does not match the report's frozen decision boundary"
                .to_owned(),
        }
        .into());
    }
    let feature_state_counts = (!feature_rows.is_empty())
        .then(|| counts_by(feature_rows.iter().map(|row| row.cell_state.as_wire())));
    let model_input_state_counts = (!input_rows.is_empty())
        .then(|| counts_by(input_rows.iter().map(|row| row.raw_state.as_str())));
    let model_route = consistent_model_route(&input_rows)?;
    let (feature_vector_count, decision_capture_count) =
        serving_feature_evidence_counts(&feature_rows)?;
    Ok(QuantReportDiagnosticsView {
        subject: ReportDiagnosticsSubject::ModelRun,
        stage_ceiling: FeatureParityStage::Prediction,
        evidence_complete,
        decision_boundary: Some(expected_boundary),
        model_route,
        selection_count,
        decision_capture_count,
        feature_vector_count,
        feature_state_counts,
        model_input_state_counts,
        feature_cell_count: non_empty_count(&feature_rows, "feature cell")?,
        model_input_count: non_empty_count(&input_rows, "model input")?,
    })
}

fn unique_feature_vector_ids(inputs: &[QuantModelInputEventRow]) -> Vec<FeatureVectorId> {
    let mut ids = Vec::new();
    for row in inputs {
        if !ids.contains(&row.feature_vector_id) {
            ids.push(row.feature_vector_id.clone());
        }
    }
    ids
}

fn latest_feature_cells(rows: Vec<QuantFeatureEventRow>) -> Vec<QuantFeatureEventRow> {
    let mut latest = BTreeMap::new();
    for row in rows {
        let key = (row.feature_vector_id.to_string(), row.feature_name.clone());
        let replace = latest
            .get(&key)
            .is_none_or(|current: &QuantFeatureEventRow| {
                row.ingestion_time >= current.ingestion_time
            });
        if replace {
            latest.insert(key, row);
        }
    }
    latest.into_values().collect()
}

fn validate_pre_inference_vector(
    report: &RecommendationReportInfo,
    token: &TokenDataQualityRecord,
    info: &FeatureVectorInfo,
    rows: &[QuantFeatureEventRow],
    boundary: &DecisionBoundary,
) -> QuantResult<()> {
    let identity_mismatch = info.market_id != token.market_id
        || info.token_id.as_ref() != Some(&token.token_id)
        || info.decision_at != report.decision_at
        || info.data_quality != token.status
        || info.decision_boundary.as_ref() != Some(boundary);
    if identity_mismatch {
        return Err(ResearchError::Determinism {
            detail: format!(
                "feature vector {} is not aligned with report {} data-quality evidence",
                info.feature_vector_id, report.recommendation_report_id
            ),
        }
        .into());
    }
    if rows.iter().any(|row| {
        row.feature_vector_id != info.feature_vector_id
            || row.decision_policy_snapshot_id != report.decision_policy_snapshot_id
            || row.market_id != info.market_id
            || row.token_id != info.token_id
            || row.decision_at != report.decision_at.timestamp_millis()
            || row.knowledge_cutoff != boundary.knowledge_cutoff().timestamp_millis()
            || row.data_quality != info.data_quality.as_str()
    }) {
        return Err(ResearchError::Determinism {
            detail: format!(
                "feature-cell evidence for vector {} is not aligned with its report/vector",
                info.feature_vector_id
            ),
        }
        .into());
    }
    persisted_capture(info, rows, boundary)?;
    Ok(())
}

fn persisted_feature_names(info: &FeatureVectorInfo) -> QuantResult<BTreeSet<String>> {
    let object = info
        .payload
        .as_object()
        .ok_or_else(|| ResearchError::Serialization {
            detail: format!(
                "feature vector {} payload is not an object",
                info.feature_vector_id
            ),
        })?;
    let generic = object
        .get("generic")
        .and_then(serde_json::Value::as_object)
        .ok_or_else(|| ResearchError::Serialization {
            detail: format!(
                "feature vector {} payload has no generic feature map",
                info.feature_vector_id
            ),
        })?;
    let mut names = generic.keys().cloned().collect::<BTreeSet<_>>();
    match object.get("domain") {
        None | Some(serde_json::Value::Null) => {}
        Some(domain) => {
            let values = domain
                .as_object()
                .and_then(|value| value.get("values"))
                .and_then(serde_json::Value::as_object)
                .ok_or_else(|| ResearchError::Serialization {
                    detail: format!(
                        "feature vector {} domain payload has no feature map",
                        info.feature_vector_id
                    ),
                })?;
            for name in values.keys() {
                if !names.insert(name.clone()) {
                    return Err(ResearchError::Determinism {
                        detail: format!(
                            "feature vector {} repeats feature name `{name}` across planes",
                            info.feature_vector_id
                        ),
                    }
                    .into());
                }
            }
        }
    }
    if names.is_empty() {
        return Err(ResearchError::Determinism {
            detail: format!(
                "feature vector {} has an empty persisted feature payload",
                info.feature_vector_id
            ),
        }
        .into());
    }
    Ok(names)
}

fn serving_feature_evidence_counts(
    rows: &[QuantFeatureEventRow],
) -> QuantResult<(Option<u64>, Option<u64>)> {
    if rows.is_empty() {
        return Ok((None, None));
    }
    let mut captures = HashMap::<FeatureVectorId, &str>::new();
    for row in rows {
        if row.decision_capture_hash.is_empty() {
            return Err(ResearchError::Determinism {
                detail: format!(
                    "feature vector {} has an empty decision-capture hash",
                    row.feature_vector_id
                ),
            }
            .into());
        }
        match captures.entry(row.feature_vector_id.clone()) {
            Vacant(entry) => {
                entry.insert(row.decision_capture_hash.as_str());
            }
            Occupied(entry) if *entry.get() != row.decision_capture_hash => {
                return Err(ResearchError::Determinism {
                    detail: format!(
                        "feature vector {} carries multiple decision-capture hashes",
                        row.feature_vector_id
                    ),
                }
                .into());
            }
            Occupied(_) => {}
        }
    }
    let count = checked_len(captures.len(), "feature vector")?;
    Ok((Some(count), Some(count)))
}

fn latest_model_inputs(rows: Vec<QuantModelInputEventRow>) -> Vec<QuantModelInputEventRow> {
    let mut latest = BTreeMap::new();
    for row in rows {
        let key = (
            row.market_id.to_string(),
            row.raw_input_name.clone(),
            row.encoded_column.clone(),
        );
        let replace = latest
            .get(&key)
            .is_none_or(|current: &QuantModelInputEventRow| {
                row.ingestion_time >= current.ingestion_time
            });
        if replace {
            latest.insert(key, row);
        }
    }
    latest.into_values().collect()
}

fn decision_boundary(
    rows: &[QuantFeatureEventRow],
) -> QuantResult<Option<DecisionBoundaryEvidenceView>> {
    let Some(first) = rows.first() else {
        return Ok(None);
    };
    let expected_cutoffs = parse_source_cutoffs(&first.per_source_cutoffs_json)?;
    for row in &rows[1..] {
        if row.decision_at != first.decision_at
            || row.knowledge_cutoff != first.knowledge_cutoff
            || parse_source_cutoffs(&row.per_source_cutoffs_json)? != expected_cutoffs
        {
            return Err(ResearchError::Determinism {
                detail: "serving feature evidence carries inconsistent decision boundaries"
                    .to_owned(),
            }
            .into());
        }
    }
    Ok(Some(DecisionBoundaryEvidenceView {
        decision_at: evidence_time(first.decision_at, "decision_at")?,
        knowledge_cutoff: evidence_time(first.knowledge_cutoff, "knowledge_cutoff")?,
        per_source_cutoffs: expected_cutoffs,
    }))
}

fn parse_source_cutoffs(value: &str) -> QuantResult<BTreeMap<String, DateTime<Utc>>> {
    serde_json::from_str(value).map_err(|error| {
        ResearchError::Serialization {
            detail: format!("invalid serving per-source cutoffs: {error}"),
        }
        .into()
    })
}

fn consistent_model_route(
    rows: &[QuantModelInputEventRow],
) -> QuantResult<Option<ModelRouteEvidenceView>> {
    let Some(first) = rows.first() else {
        return Ok(None);
    };
    if rows.iter().skip(1).any(|row| {
        row.model_run_id != first.model_run_id
            || row.model_version_id != first.model_version_id
            || row.model_family != first.model_family
            || row.input_contract_hash != first.input_contract_hash
            || row.transform_hash != first.transform_hash
            || row.training_input_hash != first.training_input_hash
    }) {
        return Err(ResearchError::Determinism {
            detail: "serving model-input evidence carries inconsistent route or transform hashes"
                .to_owned(),
        }
        .into());
    }
    Ok(Some(ModelRouteEvidenceView {
        model_run_id: first.model_run_id.to_string(),
        model_version_id: first.model_version_id.to_string(),
        model_family: first.model_family.clone(),
        input_contract_hash: first.input_contract_hash.clone(),
        transform_hash: first.transform_hash.clone(),
        training_input_hash: first.training_input_hash.clone(),
    }))
}

fn feature_cell_view(row: QuantFeatureEventRow) -> QuantResult<FeatureCellEvidenceView> {
    Ok(FeatureCellEvidenceView {
        feature_name: row.feature_name,
        state: row.cell_state.as_wire().to_owned(),
        raw_value: row.raw_value,
        value_kind: row.value_kind.as_wire().to_owned(),
        source_kind: row.source_kind.as_wire().to_owned(),
        evidence_source_kind: row
            .evidence_source_kind
            .map(|source| source.as_wire().to_owned()),
        evidence_reference: row.evidence_reference,
        evidence_effective_at: optional_evidence_time(
            row.evidence_effective_at,
            "evidence_effective_at",
        )?,
        evidence_available_at: optional_evidence_time(
            row.evidence_available_at,
            "evidence_available_at",
        )?,
        reason: row.reason,
        staleness_ms: row.staleness_ms,
        data_quality: row.data_quality,
        audit_fingerprint: row.audit_fingerprint,
    })
}

fn model_input_view(row: QuantModelInputEventRow) -> ModelInputEvidenceView {
    ModelInputEvidenceView {
        raw_input_name: row.raw_input_name,
        raw_state: row.raw_state,
        raw_value: row.raw_value,
        encoded_column: row.encoded_column,
        encoded_value_bits: row.encoded_value_bits.map(|bits| bits.to_string()),
        input_contract_hash: row.input_contract_hash,
        transform_hash: row.transform_hash,
        training_input_hash: row.training_input_hash,
        audit_fingerprint: row.audit_fingerprint,
    }
}

fn consistent_string<T>(rows: &[T], value: impl Fn(&T) -> &String) -> QuantResult<Option<String>> {
    let Some(first) = rows.first().map(&value) else {
        return Ok(None);
    };
    if rows.iter().skip(1).any(|row| value(row) != first) {
        return Err(ResearchError::Determinism {
            detail: "serving evidence carries inconsistent content hashes".to_owned(),
        }
        .into());
    }
    Ok(Some(first.clone()))
}

fn counts_by<'a>(values: impl Iterator<Item = &'a str>) -> BTreeMap<String, u64> {
    let mut counts = BTreeMap::new();
    for value in values {
        *counts.entry(value.to_owned()).or_default() += 1;
    }
    counts
}

fn evidence_time(value: i64, field: &'static str) -> QuantResult<DateTime<Utc>> {
    DateTime::from_timestamp_millis(value).ok_or_else(|| {
        ResearchError::Serialization {
            detail: format!("serving evidence {field} is outside chrono range: {value}"),
        }
        .into()
    })
}

fn optional_evidence_time(
    value: Option<i64>,
    field: &'static str,
) -> QuantResult<Option<DateTime<Utc>>> {
    value
        .map(|timestamp| evidence_time(timestamp, field))
        .transpose()
}

fn checked_len(value: usize, entity: &'static str) -> QuantResult<u64> {
    u64::try_from(value).map_err(|error| {
        ResearchError::Serialization {
            detail: format!("{entity} evidence count overflow: {error}"),
        }
        .into()
    })
}

fn non_empty_count<T>(rows: &[T], entity: &'static str) -> QuantResult<Option<u64>> {
    if rows.is_empty() {
        Ok(None)
    } else {
        checked_len(rows.len(), entity).map(Some)
    }
}

#[cfg(test)]
mod diagnostics_tests {
    use chrono::{TimeZone, Utc};
    use quant_pivot_models::{
        domain::{DecisionBoundary, DecisionClock},
        enums::quant::{EmptyReportReason, FeatureParityStage},
        types::ModelRunId,
    };

    use super::{
        model_run_diagnostics, pre_inference_selection_diagnostics, report_diagnostics_execution,
    };

    fn boundary() -> DecisionBoundary {
        DecisionClock::new(30)
            .boundary(
                Utc.with_ymd_and_hms(2026, 7, 12, 12, 0, 0)
                    .single()
                    .expect("test time"),
            )
            .expect("test boundary")
    }

    #[test]
    fn pre_inference_selection_does_not_fabricate_later_stage_zeros() {
        let view = pre_inference_selection_diagnostics(&boundary(), 0);

        assert!(view.evidence_complete);
        assert_eq!(view.stage_ceiling, FeatureParityStage::Selection);
        assert!(view.decision_boundary.is_some());
        assert_eq!(view.selection_count, 0);
        assert_eq!(view.decision_capture_count, None);
        assert_eq!(view.feature_vector_count, None);
        assert_eq!(view.feature_cell_count, None);
        assert_eq!(view.model_input_count, None);
    }

    #[test]
    fn unavailable_model_run_evidence_is_none_not_zero() {
        let view =
            model_run_diagnostics(false, &[], &[], &boundary(), 2).expect("diagnostics projection");

        assert!(!view.evidence_complete);
        assert_eq!(view.stage_ceiling, FeatureParityStage::Prediction);
        assert_eq!(view.feature_state_counts, None);
        assert_eq!(view.feature_cell_count, None);
        assert_eq!(view.model_input_state_counts, None);
        assert_eq!(view.model_input_count, None);
    }

    #[test]
    fn report_subject_rejects_impossible_pre_inference_reason() {
        let model_run_id = ModelRunId::from_v7();
        assert_eq!(
            report_diagnostics_execution(Some(&model_run_id), None)
                .expect("model-run subject")
                .1,
            FeatureParityStage::Prediction
        );
        assert_eq!(
            report_diagnostics_execution(None, Some(EmptyReportReason::InsufficientDataQuality))
                .expect("data-quality subject")
                .1,
            FeatureParityStage::DataQuality
        );
        assert!(
            report_diagnostics_execution(None, Some(EmptyReportReason::NoPositiveSignal)).is_err()
        );
        assert!(report_diagnostics_execution(None, None).is_err());
    }
}

//! Exact recovery of a report's immutable decision boundary.

use std::collections::{HashMap, HashSet};

use quant_pivot_error::{QuantResult, research::ResearchError};
use quant_pivot_models::{
    domain::{
        data_plane::{
            DecisionBoundary, DecisionClock, DecisionSource, ExchangeHistoryFrontier,
            HistoryServingHeadSeal,
        },
        governance::DecisionPolicySnapshotInfo,
        quant::{
            FeatureVectorInfo, RecommendationReportInfo, ReportDataQualitySnapshotInfo,
            ReportRouteRunInfo, ReportRunInfo, RepresentedRouteSet, RouteHistoryLineage,
        },
    },
    enums::quant::ReportRunStatus,
    types::{ContentHash, FeatureVectorId, FinalizedExecutionEvidence, TokenDataQualityRecord},
};
use quant_pivot_repository::traits::{ExchangeHistoryRepository, QuantFactReadRepository};
use quant_pivot_research::hashing::ResearchHasher;

use crate::governance::policy_snapshot::VerifiedPolicySnapshotBinding;

/// Validated report lineage and the exact Route source owning its clock.
pub struct ReportBoundaryEvidence<'a> {
    report: &'a RecommendationReportInfo,
    configured: DecisionBoundary,
    history: &'a RouteHistoryLineage,
    universe_plan_hash: ContentHash,
    tokens: &'a [TokenDataQualityRecord],
    feature_ids: Vec<FeatureVectorId>,
}

impl<'a> ReportBoundaryEvidence<'a> {
    pub(crate) fn try_new(
        report: &'a RecommendationReportInfo,
        run: &ReportRunInfo,
        policy: &DecisionPolicySnapshotInfo,
        quality: &'a ReportDataQualitySnapshotInfo,
        route_runs: &'a [ReportRouteRunInfo],
    ) -> QuantResult<Self> {
        VerifiedPolicySnapshotBinding::try_from(policy)?;
        let run_matches = run.report_run_id == report.report_run_id
            && run.output_report_id == Some(report.recommendation_report_id)
            && run.status == ReportRunStatus::Succeeded
            && run.decision_at == Some(report.decision_at)
            && run.decision_policy_snapshot_id == Some(report.decision_policy_snapshot_id);
        let quality_binding = (
            quality.report_data_quality_snapshot_id,
            quality.decision_at,
            quality.decision_policy_snapshot_id,
        );
        let report_binding = (
            report.data_quality_snapshot_ref,
            report.decision_at,
            report.decision_policy_snapshot_id,
        );
        let quality_matches = quality_binding == report_binding;
        if !run_matches
            || !quality_matches
            || policy.decision_policy_snapshot_id != report.decision_policy_snapshot_id
        {
            return Err(ResearchError::Determinism {
                detail: format!("report {} boundary lineage differs from its exact successful run, policy, or data-quality snapshot", report.recommendation_report_id),
            }.into());
        }
        let lag = run
            .knowledge_lag_secs
            .and_then(|lag| u64::try_from(lag).ok())
            .ok_or_else(|| ResearchError::Determinism {
                detail: format!(
                    "report run {} has no valid frozen knowledge lag",
                    run.report_run_id
                ),
            })?;
        let domain = &policy.snapshot.profile_artifacts.domain.definition;
        let configured = DecisionClock::new(lag).serving_boundary(
            report.decision_at,
            domain.crypto.availability_lag_secs,
            domain.weather.availability_lag_secs,
        )?;
        let (history, universe_plan_hash) = Self::route_history(report, route_runs)?;
        let mut feature_ids = Vec::with_capacity(quality.tokens_json.0.len());
        let mut ids = HashSet::new();
        let mut markets = HashSet::new();
        let mut tokens = HashSet::new();
        for record in &quality.tokens_json.0 {
            if !ids.insert(record.feature_vector_id)
                || !markets.insert(&record.market_id)
                || !tokens.insert(&record.token_id)
            {
                return Err(ResearchError::Determinism {
                    detail: format!("report {} data-quality evidence contains duplicate vector, market, or token bindings", report.recommendation_report_id),
                }.into());
            }
            feature_ids.push(record.feature_vector_id);
        }
        Ok(Self {
            report,
            configured,
            history,
            universe_plan_hash,
            tokens: &quality.tokens_json.0,
            feature_ids,
        })
    }

    pub(crate) fn feature_ids(&self) -> &[FeatureVectorId] {
        &self.feature_ids
    }

    pub(crate) const fn history(&self) -> &RouteHistoryLineage {
        self.history
    }

    pub(crate) const fn universe_plan_hash(&self) -> ContentHash {
        self.universe_plan_hash
    }

    /// Resolve the exact frozen Route source once, then verify every capture.
    pub(crate) async fn restore(
        &self,
        vectors: &[FeatureVectorInfo],
        history: &dyn ExchangeHistoryRepository,
        facts: &dyn QuantFactReadRepository,
    ) -> QuantResult<DecisionBoundary> {
        let head = match self.history {
            RouteHistoryLineage::Runtime {
                serving_head_seal_id,
                serving_head_seal_hash,
            } => Some(
                history
                    .validate_serving_head(*serving_head_seal_id, *serving_head_seal_hash)
                    .await?,
            ),
            RouteHistoryLineage::Materialized { chunks, .. } => {
                facts
                    .validate_execution_history_chunks(chunks.clone())
                    .await?;
                None
            }
        };
        self.validate_vectors(vectors, head.as_ref())
    }

    fn validate_vectors(
        &self,
        vectors: &[FeatureVectorInfo],
        head: Option<&HistoryServingHeadSeal>,
    ) -> QuantResult<DecisionBoundary> {
        let expected = self.history_boundary(head)?;
        let mut by_id = HashMap::with_capacity(vectors.len());
        for vector in vectors {
            if by_id.insert(vector.feature_vector_id, vector).is_some() {
                return Err(ResearchError::Determinism {
                    detail: format!(
                        "report {} boundary evidence repeats a feature vector",
                        self.report.recommendation_report_id
                    ),
                }
                .into());
            }
        }
        if by_id.len() != self.feature_ids.len() {
            return Err(ResearchError::Determinism {
                detail: format!(
                    "report {} boundary evidence has an incomplete vector population",
                    self.report.recommendation_report_id
                ),
            }
            .into());
        }
        for token in self.tokens {
            let vector =
                by_id
                    .get(&token.feature_vector_id)
                    .ok_or_else(|| ResearchError::Determinism {
                        detail: format!(
                            "report {} boundary evidence omitted vector {}",
                            self.report.recommendation_report_id, token.feature_vector_id
                        ),
                    })?;
            self.validate_vector(token, vector, &expected, head)?;
        }
        Ok(expected)
    }

    fn validate_vector(
        &self,
        token: &TokenDataQualityRecord,
        vector: &FeatureVectorInfo,
        expected: &DecisionBoundary,
        head: Option<&HistoryServingHeadSeal>,
    ) -> QuantResult<()> {
        let capture = &vector.decision_capture;
        let snapshot = &capture.snapshot;
        let identity_matches = vector.market_id == token.market_id
            && vector.token_id.as_ref() == Some(&token.token_id)
            && vector.decision_at == self.report.decision_at
            && vector.data_quality == token.status
            && snapshot.market_id == token.market_id
            && snapshot.token_id == token.token_id
            && snapshot.selection.market_id == token.market_id
            && snapshot.selection.primary_token_id == token.token_id
            && snapshot.selection.event_id == snapshot.event_id
            && snapshot.book_snapshot_ref.token_id == token.token_id
            && capture.identity.category == snapshot.selection.category
            && capture.data_quality == token.status;
        if !identity_matches || ResearchHasher::canonical(capture)? != vector.decision_capture_hash
        {
            return Err(ResearchError::Determinism {
                detail: format!(
                    "feature vector {} capture hash or report/token identity differs",
                    vector.feature_vector_id
                ),
            }
            .into());
        }
        vector.decision_boundary.validate()?;
        snapshot.boundary.validate()?;
        self.validate_capture(&capture.finalized_execution_evidence, head)?;
        if &vector.decision_boundary != expected || &snapshot.boundary != expected {
            return Err(ResearchError::Determinism {
                detail: format!("feature vector {} frozen boundary differs from its configured clock and exact Route history; unproven source cutoffs are not recoverable", vector.feature_vector_id),
            }.into());
        }
        Ok(())
    }

    fn validate_capture(
        &self,
        evidence: &FinalizedExecutionEvidence,
        head: Option<&HistoryServingHeadSeal>,
    ) -> QuantResult<()> {
        let matches = match (evidence, self.history) {
            (FinalizedExecutionEvidence::NotRequired, _)
            | (
                FinalizedExecutionEvidence::Runtime {
                    history_enabled: false,
                    accepted_through_block: None,
                    accepted_through_at: None,
                },
                RouteHistoryLineage::Runtime { .. },
            ) => true,
            (
                FinalizedExecutionEvidence::Materialized { available_by },
                RouteHistoryLineage::Materialized {
                    available_by: bound,
                    ..
                },
            ) => available_by == bound,
            (
                FinalizedExecutionEvidence::Runtime {
                    history_enabled: true,
                    accepted_through_block: Some(block),
                    accepted_through_at: Some(at),
                },
                RouteHistoryLineage::Runtime { .. },
            ) => head.is_some_and(|head| {
                i64::try_from(*block).ok() == Some(head.seal.accepted_through_block)
                    && *at == head.seal.effective_through_at
            }),
            _ => false,
        };
        if !matches {
            return Err(ResearchError::Determinism {
                detail: format!("report {} capture execution source differs from its exact frozen Route history", self.report.recommendation_report_id),
            }.into());
        }
        Ok(())
    }

    fn history_boundary(
        &self,
        head: Option<&HistoryServingHeadSeal>,
    ) -> QuantResult<DecisionBoundary> {
        if let Some(head) = head {
            let actual_hash = head
                .derive_hash()
                .map_err(|error| ResearchError::Determinism {
                    detail: format!("report serving-head preimage hash failed: {error}"),
                })?;
            if actual_hash != head.seal.seal_hash {
                return Err(ResearchError::Determinism {
                    detail: format!(
                        "report {} serving-head preimage differs from its frozen hash",
                        self.report.recommendation_report_id
                    ),
                }
                .into());
            }
        }
        match (self.history, head) {
            (RouteHistoryLineage::Runtime { serving_head_seal_id, serving_head_seal_hash }, Some(head))
                if head.seal.serving_head_seal_id == *serving_head_seal_id
                    && head.seal.seal_hash == *serving_head_seal_hash
                    && head.seal.frontier == ExchangeHistoryFrontier::Activation
                    && head.seal.created_at <= self.report.decision_at
                    && head.seal.window_from_block >= 0
                    && head.seal.accepted_through_block >= head.seal.window_from_block => {
                self.configured.clone().with_source_watermark(DecisionSource::FinalizedExecution, head.seal.effective_through_at)
            }
            (RouteHistoryLineage::Materialized { .. }, None) => Ok(self.configured.clone()),
            _ => Err(ResearchError::Determinism {
                detail: format!("report {} exact serving head is missing, unavailable, or has a different identity/frontier", self.report.recommendation_report_id),
            }.into()),
        }
    }

    fn route_history(
        report: &RecommendationReportInfo,
        route_runs: &'a [ReportRouteRunInfo],
    ) -> QuantResult<(&'a RouteHistoryLineage, ContentHash)> {
        let represented = RepresentedRouteSet::from_routes(route_runs.iter().map(|run| run.route))
            .map_err(|error| ResearchError::Determinism {
                detail: format!("report Route set hash failed: {error}"),
            })?;
        let mut ids = HashSet::new();
        let mut history = None;
        let mut universe_hash = None;
        if route_runs.is_empty()
            || represented != report.represented_routes_json
            || represented.routes.len() != route_runs.len()
        {
            return Err(ResearchError::Determinism {
                detail: format!(
                    "report {} has no exact represented Route population",
                    report.recommendation_report_id
                ),
            }
            .into());
        }
        for route in route_runs {
            let lineage =
                route
                    .lineage_json
                    .as_ref()
                    .ok_or_else(|| ResearchError::Determinism {
                        detail: format!(
                            "report Route {} has no frozen source lineage",
                            route.report_route_run_id
                        ),
                    })?;
            let identity_matches = route.report_run_id == report.report_run_id
                && ids.insert(route.report_route_run_id)
                && route.model_version_id == Some(lineage.model_version_id)
                && route.model_run_id == lineage.model_run_id
                && route.calibration_artifact_id == Some(lineage.calibration_artifact_id)
                && route.trade_policy_artifact_id == lineage.trade_policy_artifact_id
                && route.research_profile_artifact_id.as_ref()
                    == Some(&lineage.research_profile_artifact_id)
                && lineage.research_profile_artifact_id
                    == lineage.research_profile_ref.artifact_id();
            if !identity_matches
                || history.is_some_and(|expected| expected != &lineage.history)
                || universe_hash
                    .is_some_and(|expected| expected != lineage.report_universe_plan_hash)
            {
                return Err(ResearchError::Determinism {
                    detail: format!(
                        "report Route {} changed its parent, identity, universe, or exact history",
                        route.report_route_run_id
                    ),
                }
                .into());
            }
            history.get_or_insert(&lineage.history);
            universe_hash.get_or_insert(lineage.report_universe_plan_hash);
        }
        let history = history.ok_or_else(|| ResearchError::Determinism {
            detail: "report has no frozen Route history".to_owned(),
        })?;
        if let RouteHistoryLineage::Materialized {
            available_by,
            chunks,
        } = history
        {
            let mut chunk_ids = HashSet::new();
            if *available_by < report.decision_at
                || chunks.is_empty()
                || chunks.iter().any(|chunk| {
                    chunk.from_block < 0
                        || chunk.to_block < chunk.from_block
                        || chunk.state_revision <= 0
                        || !chunk_ids.insert(chunk.chunk_id)
                })
                || chunks
                    .windows(2)
                    .any(|pair| pair[0].to_block.checked_add(1) != Some(pair[1].from_block))
            {
                return Err(ResearchError::Determinism {
                    detail:
                        "report materialized history has invalid availability or chunk coverage"
                            .to_owned(),
                }
                .into());
            }
        }
        let universe_plan_hash = universe_hash.ok_or_else(|| ResearchError::Determinism {
            detail: "report has no frozen Route universe hash".to_owned(),
        })?;
        Ok((history, universe_plan_hash))
    }
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use chrono::{DateTime, Duration, TimeZone, Utc};
    use quant_pivot_error::QuantResult;
    use quant_pivot_models::{
        domain::{
            data_plane::{
                DecisionBoundary, DecisionClock, DecisionSource, ExchangeHistoryFrontier,
                HistorySealChunkRef, HistoryServingHeadSeal, HistoryServingHeadSealInfo,
            },
            governance::DecisionPolicySnapshotInfo,
            quant::{
                FeatureVectorInfo, RecommendationReportInfo, ReportDataQualitySnapshotInfo,
                ReportRouteRunInfo, ReportRunInfo, RepresentedRouteSet, RouteCandidateFunnel,
                RouteHistoryLineage, RouteModelLineage, RouteRunOutcome,
            },
        },
        enums::{
            catalog::CatalogTimestampQuality,
            quant::{
                DataQualityStatus, OutcomeSide, RecommendationReportStatus, ReportKind,
                ReportRunStatus, ReportTriggerKind,
            },
            runtime_config::{DecisionPolicySnapshotSource, PolicyActorKind},
        },
        runtime_config::{BuyModelRoute, DecisionPolicySnapshot, PolicyRevisionBundle},
        types::{
            CalibrationArtifactId, CatalogDecisionRef, CatalogEventChangeId, CatalogMarketChangeId,
            CatalogSyncBatchId, ContentHash, CorrelationId, DecisionCaptureEvidence,
            DecisionPolicySnapshotId, DecisionSnapshotEvidence, FeatureSourceRefs, FeatureVectorId,
            FeatureVectorPayload, FinalizedExecutionEvidence, HistoryServingHeadSealId,
            ModelVersionId, PolicyRevisionId, Probability, RecommendationId,
            RecommendationReportId, ReportDataQualityTokens, ReportRouteRunId, ReportRunId,
            ReportTriggerKey, SchemaVersion, SelectionMemberEvidence, ServingAuthority,
            TokenDataQualityRecord, TokenId, Usd,
        },
    };
    use quant_pivot_research::hashing::ResearchHasher;
    use uuid::Uuid;

    use super::ReportBoundaryEvidence;
    use crate::test_fixtures::{execution_pg_seed, report_fixtures};

    struct BoundaryFixture {
        report: RecommendationReportInfo,
        run: ReportRunInfo,
        policy: DecisionPolicySnapshotInfo,
        quality: ReportDataQualitySnapshotInfo,
        boundary: DecisionBoundary,
        watermark: DateTime<Utc>,
        vectors: Vec<FeatureVectorInfo>,
        routes: Vec<ReportRouteRunInfo>,
        head: HistoryServingHeadSeal,
    }

    impl BoundaryFixture {
        fn new() -> Self {
            let decision_at = Utc
                .timestamp_opt(1_700_000_000, 123_456_000)
                .single()
                .expect("decision time");
            let revision = PolicyRevisionId::from_v7();
            let snapshot = DecisionPolicySnapshot {
                revisions: PolicyRevisionBundle {
                    recommendation_policy: Some(revision),
                    execution_risk_policy: Some(revision),
                    model_routing: Some(revision),
                    report_schedule: Some(revision),
                    operations_policy: Some(revision),
                    execution_authorization_policy: Some(revision),
                },
                ..DecisionPolicySnapshot::default()
            };
            let snapshot_hash = snapshot.persistence_hash().expect("policy hash");
            let policy = DecisionPolicySnapshotInfo {
                decision_policy_snapshot_id: DecisionPolicySnapshotId::from_content_hash(
                    &snapshot_hash,
                ),
                snapshot_hash,
                snapshot,
                recommendation_policy_revision_id: revision,
                execution_risk_policy_revision_id: revision,
                model_routing_revision_id: revision,
                report_schedule_revision_id: revision,
                operations_policy_revision_id: revision,
                execution_authorization_policy_revision_id: revision,
                source: DecisionPolicySnapshotSource::Bootstrap,
                created_by_kind: PolicyActorKind::System,
                created_by_user_id: None,
                created_by_label: "boundary-test".to_owned(),
                reason: "exact report boundary".to_owned(),
                created_at: decision_at - Duration::hours(1),
            };
            let mut report = report_fixtures::report(
                RecommendationReportId::from_v7(),
                ReportKind::TopN,
                RecommendationReportStatus::Published,
            );
            report.decision_at = decision_at;
            report.decision_policy_snapshot_id = policy.decision_policy_snapshot_id;
            let run = ReportRunInfo {
                report_run_id: report.report_run_id,
                trigger_kind: ReportTriggerKind::AdHoc,
                trigger_key: ReportTriggerKey::parse(format!(
                    "ad_hoc:{}",
                    CorrelationId::new("boundary-fixture")
                ))
                .expect("trigger key"),
                schedule_id: None,
                request_id: None,
                retry_of_run_id: None,
                scheduled_for: None,
                requested_at: decision_at,
                status: ReportRunStatus::Succeeded,
                started_at: Some(decision_at),
                decision_at: Some(decision_at),
                heartbeat_at: None,
                lease_expires_at: None,
                finished_at: Some(decision_at),
                lease_owner: None,
                decision_policy_snapshot_id: Some(policy.decision_policy_snapshot_id),
                top_n: Some(report.top_n),
                knowledge_lag_secs: Some(2),
                output_report_id: Some(report.recommendation_report_id),
                terminal_reason: None,
                error_code: None,
                error_summary: None,
            };
            let quality = ReportDataQualitySnapshotInfo {
                report_data_quality_snapshot_id: report.data_quality_snapshot_ref,
                decision_at,
                decision_policy_snapshot_id: policy.decision_policy_snapshot_id,
                tokens_json: ReportDataQualityTokens(Vec::new()),
                created_at: decision_at,
            };
            let domain = &policy.snapshot.profile_artifacts.domain.definition;
            let watermark = decision_at - Duration::seconds(30) - Duration::microseconds(17);
            let boundary = DecisionClock::new(2)
                .serving_boundary(
                    decision_at,
                    domain.crypto.availability_lag_secs,
                    domain.weather.availability_lag_secs,
                )
                .expect("configured boundary")
                .with_source_watermark(DecisionSource::FinalizedExecution, watermark)
                .expect("runtime watermark");
            let mut fixture = Self {
                report,
                run,
                policy,
                quality,
                boundary,
                watermark,
                vectors: Vec::new(),
                routes: Vec::new(),
                head: HistoryServingHeadSeal {
                    seal: HistoryServingHeadSealInfo {
                        serving_head_seal_id: HistoryServingHeadSealId::new(Uuid::from_u128(42)),
                        seal_hash: ContentHash::from_bytes([42; 32]),
                        plan_id: Uuid::from_u128(41),
                        frontier: ExchangeHistoryFrontier::Activation,
                        previous_seal_id: None,
                        window_from_block: 1,
                        accepted_through_block: 42,
                        effective_through_at: watermark,
                        policy_hash: ContentHash::from_bytes([41; 32]),
                        created_at: decision_at - Duration::seconds(1),
                    },
                    chunks: vec![HistorySealChunkRef {
                        chunk_id: Uuid::from_u128(43),
                        frontier: ExchangeHistoryFrontier::Activation,
                        state_revision: 1,
                        from_block: 1,
                        to_block: 42,
                    }],
                },
            };
            fixture.head.seal.seal_hash =
                fixture.head.derive_hash().expect("canonical serving head");
            fixture.routes.push(fixture.route());
            fixture.add_vectors();
            fixture
        }

        fn add_vectors(&mut self) {
            for index in 0..2 {
                let vector = self.vector(index);
                self.quality.tokens_json.0.push(TokenDataQualityRecord {
                    feature_vector_id: vector.feature_vector_id,
                    token_id: vector.token_id.clone().expect("fixture token"),
                    market_id: vector.market_id.clone(),
                    status: vector.data_quality,
                    book_age_ms: 0,
                    crossed: false,
                    empty: false,
                    fact_lag_ms: None,
                    missing_required: Vec::new(),
                });
                self.vectors.push(vector);
            }
        }

        fn route(&self) -> ReportRouteRunInfo {
            let profile = execution_pg_seed::fixture_profile_ref();
            let hash = ContentHash::from_bytes([1; 32]);
            let lineage = RouteModelLineage {
                model_version_id: ModelVersionId::from_v7(),
                model_run_id: None,
                calibration_artifact_id: CalibrationArtifactId::from_v7(),
                trade_policy_artifact_id: None,
                research_profile_artifact_id: profile.artifact_id(),
                research_profile_ref: profile,
                prediction_horizon_secs: 3_600,
                feature_contract_digest: hash,
                pit_lineage_digest: hash,
                serving_contract_digest: hash,
                recommendation_contract_hash: hash,
                report_universe_plan_hash: hash,
                history: RouteHistoryLineage::Runtime {
                    serving_head_seal_id: self.head.seal.serving_head_seal_id,
                    serving_head_seal_hash: self.head.seal.seal_hash,
                },
                serving_authority: ServingAuthority::ExecutionEligible,
            };
            ReportRouteRunInfo {
                report_route_run_id: ReportRouteRunId::from_v7(),
                report_run_id: self.report.report_run_id,
                route: BuyModelRoute::Pooled,
                outcome: RouteRunOutcome::ZeroCandidates,
                model_version_id: Some(lineage.model_version_id),
                model_run_id: lineage.model_run_id,
                calibration_artifact_id: Some(lineage.calibration_artifact_id),
                trade_policy_artifact_id: lineage.trade_policy_artifact_id,
                research_profile_artifact_id: Some(lineage.research_profile_artifact_id.clone()),
                lineage_json: Some(lineage),
                funnel_json: RouteCandidateFunnel {
                    eligible_markets: 2,
                    feature_complete_markets: 2,
                    calibrated_candidates: 0,
                    admitted_economic_tiers: 0,
                    selected_recommendations: 0,
                },
                diagnostic_code: None,
                finished_at: self.report.decision_at,
                created_at: self.report.decision_at,
            }
        }

        fn vector(&self, index: usize) -> FeatureVectorInfo {
            let recommendation = report_fixtures::recommendation(
                self.report.recommendation_report_id,
                RecommendationId::from_v7(),
                1,
                &format!("market-{index}"),
                OutcomeSide::Yes,
                Usd::ZERO,
            );
            let decision_at = self.report.decision_at;
            let content_hash = ContentHash::from_bytes([1; 32]);
            let mut book_snapshot_ref = recommendation.evidence_refs.book_snapshot_ref;
            book_snapshot_ref.token_id = recommendation.token_id.clone();
            let capture = DecisionCaptureEvidence {
                snapshot: DecisionSnapshotEvidence {
                    boundary: self.boundary.clone(),
                    market_id: recommendation.market_id.clone(),
                    event_id: recommendation.event_id.clone(),
                    token_id: recommendation.token_id.clone(),
                    catalog: CatalogDecisionRef {
                        catalog_sync_batch_id: CatalogSyncBatchId::from_v7(),
                        market_change_id: CatalogMarketChangeId::from_v7(),
                        event_change_id: CatalogEventChangeId::from_v7(),
                        market_content_hash: content_hash,
                        event_content_hash: content_hash,
                        membership_hash: content_hash,
                        market_effective_at: decision_at,
                        market_available_at: decision_at,
                        event_effective_at: decision_at,
                        event_available_at: decision_at,
                        market_timestamp_quality: CatalogTimestampQuality::Source,
                        event_timestamp_quality: CatalogTimestampQuality::Source,
                    },
                    book_snapshot_ref,
                    book_effective_at: decision_at,
                    book_available_at: decision_at,
                    selection: SelectionMemberEvidence {
                        market_id: recommendation.market_id.clone(),
                        event_id: recommendation.event_id,
                        category: recommendation.identity.category,
                        primary_token_id: recommendation.token_id.clone(),
                        secondary_token_id: None,
                        liquidity_usd: None,
                        volume_24h_usd: None,
                        source_refs: Vec::new(),
                    },
                },
                finalized_execution_evidence: FinalizedExecutionEvidence::runtime(
                    true,
                    Some(42),
                    Some(self.watermark),
                ),
                identity: recommendation.identity,
                market_context: recommendation.market_context,
                data_quality: DataQualityStatus::Fresh,
                liquidity_score: Probability::ZERO,
            };
            let capture_hash = ResearchHasher::canonical(&capture).expect("capture hash");
            FeatureVectorInfo {
                feature_vector_id: FeatureVectorId::from_v7(),
                market_id: recommendation.market_id,
                token_id: Some(recommendation.token_id),
                decision_at,
                decision_boundary: self.boundary.clone(),
                feature_schema_version: SchemaVersion::FIRST,
                feature_hash: content_hash,
                data_quality: DataQualityStatus::Fresh,
                staleness_ms: 0,
                payload: FeatureVectorPayload {
                    generic: BTreeMap::new(),
                    domain: None,
                },
                source_refs: FeatureSourceRefs::default(),
                decision_capture: capture,
                decision_capture_hash: capture_hash,
                created_at: decision_at,
            }
        }

        fn restore(&self) -> QuantResult<DecisionBoundary> {
            let head = matches!(
                self.routes
                    .first()
                    .and_then(|route| route.lineage_json.as_ref())
                    .map(|lineage| &lineage.history),
                Some(RouteHistoryLineage::Runtime { .. })
            )
            .then_some(&self.head);
            ReportBoundaryEvidence::try_new(
                &self.report,
                &self.run,
                &self.policy,
                &self.quality,
                &self.routes,
            )?
            .validate_vectors(&self.vectors, head)
        }

        fn reseal(&mut self) {
            for vector in &mut self.vectors {
                vector.decision_capture_hash =
                    ResearchHasher::canonical(&vector.decision_capture).expect("resealed capture");
            }
        }
    }

    #[test]
    fn restores_watermark_exactly() {
        let mut fixture = BoundaryFixture::new();
        fixture.vectors.reverse();
        let boundary = fixture.restore().expect("exact report boundary");
        assert_eq!(boundary, fixture.boundary);
        assert_eq!(
            boundary.cutoff_for(DecisionSource::FinalizedExecution),
            fixture.watermark
        );
        assert_ne!(
            boundary.cutoff_for(DecisionSource::FinalizedExecution),
            boundary.knowledge_cutoff()
        );
        assert_ne!(fixture.watermark.timestamp_subsec_micros() % 1_000, 0);
    }

    #[test]
    fn rejects_clock_and_cutoff() {
        let mut fixture = BoundaryFixture::new();
        fixture.vectors[0].decision_at += Duration::milliseconds(1);
        assert!(fixture.restore().is_err());
        fixture.vectors[0].decision_at = fixture.report.decision_at;
        fixture.vectors[0].decision_boundary = fixture
            .boundary
            .clone()
            .with_source_watermark(
                DecisionSource::FinalizedExecution,
                fixture.watermark - Duration::microseconds(1),
            )
            .expect("earlier cutoff");
        fixture.vectors[0].decision_capture.snapshot.boundary =
            fixture.vectors[0].decision_boundary.clone();
        fixture.reseal();
        assert!(
            fixture.restore().is_err(),
            "a correctly rehashed but unproven earlier cutoff is invalid"
        );
    }

    #[test]
    fn rejects_hash_and_identity() {
        let mut fixture = BoundaryFixture::new();
        fixture.vectors[0].decision_capture_hash = ContentHash::from_bytes([9; 32]);
        assert!(fixture.restore().is_err());
        fixture.reseal();
        fixture.vectors[0]
            .decision_capture
            .snapshot
            .selection
            .primary_token_id = TokenId::new("different-token");
        fixture.reseal();
        assert!(fixture.restore().is_err());
    }

    #[test]
    fn rejects_population_changes() {
        let mut fixture = BoundaryFixture::new();
        let original = fixture.vectors.clone();
        fixture.vectors.pop();
        assert!(fixture.restore().is_err());
        fixture.vectors = original.clone();
        fixture.vectors[1] = fixture.vectors[0].clone();
        assert!(fixture.restore().is_err());
        fixture.vectors = original.clone();
        fixture.vectors[1].feature_vector_id = FeatureVectorId::from_v7();
        assert!(fixture.restore().is_err());
        fixture.vectors = original;
        fixture.quality.tokens_json.0[1].token_id =
            fixture.quality.tokens_json.0[0].token_id.clone();
        assert!(fixture.restore().is_err());
        fixture.quality.tokens_json.0[1].token_id =
            fixture.vectors[1].token_id.clone().expect("second token");
        fixture.quality.tokens_json.0[1].market_id =
            fixture.quality.tokens_json.0[0].market_id.clone();
        assert!(fixture.restore().is_err());
    }

    #[test]
    fn rejects_lineage_and_policy() {
        let mut fixture = BoundaryFixture::new();
        fixture.run.report_run_id = ReportRunId::from_v7();
        assert!(fixture.restore().is_err());
        fixture.run.report_run_id = fixture.report.report_run_id;
        fixture.run.output_report_id = Some(RecommendationReportId::from_v7());
        assert!(fixture.restore().is_err());
        fixture.run.output_report_id = Some(fixture.report.recommendation_report_id);
        fixture.quality.decision_at += Duration::milliseconds(1);
        assert!(fixture.restore().is_err());
        fixture.quality.decision_at = fixture.report.decision_at;
        fixture.policy.snapshot_hash = ContentHash::from_bytes([9; 32]);
        assert!(fixture.restore().is_err());
    }

    #[test]
    fn unused_source_keeps_boundary() {
        let mut fixture = BoundaryFixture::new();
        fixture.vectors[0]
            .decision_capture
            .finalized_execution_evidence = FinalizedExecutionEvidence::NotRequired;
        fixture.reseal();
        assert_eq!(
            fixture
                .restore()
                .expect("mixed schemas share the report boundary"),
            fixture.boundary
        );
        for vector in &mut fixture.vectors {
            vector.decision_capture.finalized_execution_evidence =
                FinalizedExecutionEvidence::NotRequired;
        }
        fixture.reseal();
        assert_eq!(
            fixture
                .restore()
                .expect("Route source owns the report watermark"),
            fixture.boundary
        );
        for vector in &mut fixture.vectors {
            vector.decision_capture.finalized_execution_evidence =
                FinalizedExecutionEvidence::runtime(true, None, Some(fixture.watermark));
        }
        fixture.reseal();
        assert!(fixture.restore().is_err());
    }

    #[test]
    fn empty_selection_keeps_watermark() {
        let mut fixture = BoundaryFixture::new();
        fixture.vectors.clear();
        fixture.quality.tokens_json.0.clear();
        let restored = fixture.restore().expect("selection-only report");
        assert_eq!(
            restored.cutoff_for(DecisionSource::FinalizedExecution),
            fixture.watermark
        );
        fixture.vectors.push(fixture.vector(0));
        assert!(
            fixture.restore().is_err(),
            "unrequested vectors must not be ignored"
        );
    }

    #[test]
    fn rejects_missing_route_authority() {
        let mut fixture = BoundaryFixture::new();
        let routes = fixture.routes.clone();
        fixture.routes.push(fixture.routes[0].clone());
        assert!(fixture.restore().is_err());
        fixture.routes.clear();
        assert!(fixture.restore().is_err());
        fixture.routes = routes.clone();
        fixture.routes[0].lineage_json = None;
        assert!(fixture.restore().is_err());
        fixture.routes = routes.clone();
        fixture.routes[0].report_run_id = ReportRunId::from_v7();
        assert!(fixture.restore().is_err());
        fixture.routes = routes.clone();
        fixture.routes[0].model_version_id = Some(ModelVersionId::from_v7());
        assert!(fixture.restore().is_err());
        fixture.routes = routes;
        let evidence = ReportBoundaryEvidence::try_new(
            &fixture.report,
            &fixture.run,
            &fixture.policy,
            &fixture.quality,
            &fixture.routes,
        )
        .expect("valid Route lineage");
        assert!(evidence.validate_vectors(&fixture.vectors, None).is_err());
        fixture.head.seal.created_at = fixture.report.decision_at + Duration::microseconds(1);
        assert!(fixture.restore().is_err());
        fixture.head.seal.created_at = fixture.report.decision_at;
        fixture.head.seal.frontier = ExchangeHistoryFrontier::Retention;
        assert!(fixture.restore().is_err());
        fixture.head.seal.frontier = ExchangeHistoryFrontier::Activation;
        fixture.head.seal.seal_hash = ContentHash::from_bytes([99; 32]);
        assert!(fixture.restore().is_err());
    }

    #[test]
    fn routes_require_one_universe() {
        let mut fixture = BoundaryFixture::new();
        let mut second = fixture.route();
        second.route = BuyModelRoute::Crypto;
        fixture.routes.push(second);
        fixture.report.represented_routes_json =
            RepresentedRouteSet::from_routes(fixture.routes.iter().map(|route| route.route))
                .expect("represented Routes");
        assert_eq!(
            fixture.restore().expect("one shared source"),
            fixture.boundary
        );
        let lineage = fixture.routes[1]
            .lineage_json
            .as_mut()
            .expect("second lineage");
        lineage.report_universe_plan_hash = ContentHash::from_bytes([11; 32]);
        assert!(fixture.restore().is_err());
        fixture.routes[1] = fixture.route();
        fixture.routes[1].route = BuyModelRoute::Crypto;
        fixture.routes[1]
            .lineage_json
            .as_mut()
            .expect("second lineage")
            .history = RouteHistoryLineage::Runtime {
            serving_head_seal_id: HistoryServingHeadSealId::new(Uuid::from_u128(99)),
            serving_head_seal_hash: fixture.head.seal.seal_hash,
        };
        assert!(fixture.restore().is_err());
    }

    #[test]
    fn head_metadata_binds_hash() {
        let mut fixture = BoundaryFixture::new();
        fixture.vectors.clear();
        fixture.quality.tokens_json.0.clear();
        fixture.head.seal.effective_through_at -= Duration::microseconds(1);
        assert!(
            fixture.restore().is_err(),
            "a copied hash cannot certify an altered head clock even without feature vectors"
        );
    }

    #[test]
    fn watermark_never_widens_cutoff() {
        let mut fixture = BoundaryFixture::new();
        fixture.head.seal.effective_through_at = fixture.report.decision_at - Duration::seconds(1);
        fixture.head.seal.seal_hash = fixture.head.derive_hash().expect("later canonical head");
        fixture.routes[0]
            .lineage_json
            .as_mut()
            .expect("lineage")
            .history = RouteHistoryLineage::Runtime {
            serving_head_seal_id: fixture.head.seal.serving_head_seal_id,
            serving_head_seal_hash: fixture.head.seal.seal_hash,
        };
        let domain = &fixture.policy.snapshot.profile_artifacts.domain.definition;
        let boundary = DecisionClock::new(2)
            .serving_boundary(
                fixture.report.decision_at,
                domain.crypto.availability_lag_secs,
                domain.weather.availability_lag_secs,
            )
            .expect("configured boundary");
        for vector in &mut fixture.vectors {
            vector.decision_boundary = boundary.clone();
            vector.decision_capture.snapshot.boundary = boundary.clone();
            vector.decision_capture.finalized_execution_evidence =
                FinalizedExecutionEvidence::runtime(
                    true,
                    Some(42),
                    Some(fixture.head.seal.effective_through_at),
                );
        }
        fixture.reseal();
        assert_eq!(
            fixture.restore().expect("head cannot widen global cutoff"),
            boundary
        );
    }

    #[test]
    fn materialized_history_is_explicit() {
        let mut fixture = BoundaryFixture::new();
        let available_by = fixture.report.decision_at + Duration::milliseconds(1);
        let mut chunks = fixture.head.chunks.clone();
        chunks[0].frontier = ExchangeHistoryFrontier::Retention;
        fixture.routes[0]
            .lineage_json
            .as_mut()
            .expect("lineage")
            .history = RouteHistoryLineage::Materialized {
            available_by,
            chunks,
        };
        let domain = &fixture.policy.snapshot.profile_artifacts.domain.definition;
        let boundary = DecisionClock::new(2)
            .serving_boundary(
                fixture.report.decision_at,
                domain.crypto.availability_lag_secs,
                domain.weather.availability_lag_secs,
            )
            .expect("materialized boundary");
        for vector in &mut fixture.vectors {
            vector.decision_boundary = boundary.clone();
            vector.decision_capture.snapshot.boundary = boundary.clone();
            vector.decision_capture.finalized_execution_evidence =
                FinalizedExecutionEvidence::materialized(available_by);
        }
        fixture.reseal();
        assert_eq!(
            fixture.restore().expect("explicit sealed materialization"),
            boundary
        );
        fixture.vectors[0]
            .decision_capture
            .finalized_execution_evidence =
            FinalizedExecutionEvidence::materialized(available_by + Duration::microseconds(1));
        fixture.reseal();
        assert!(fixture.restore().is_err());
        fixture.vectors[0]
            .decision_capture
            .finalized_execution_evidence = FinalizedExecutionEvidence::materialized(available_by);
        fixture.reseal();
        if let RouteHistoryLineage::Materialized { chunks, .. } = &mut fixture.routes[0]
            .lineage_json
            .as_mut()
            .expect("lineage")
            .history
        {
            chunks.clear();
        }
        assert!(fixture.restore().is_err());
    }
}

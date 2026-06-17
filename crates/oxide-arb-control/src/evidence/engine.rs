use crate::{
    evidence::{
        book::{
            self, BookReconstructionArtifact, BookReconstructionInput, BookReconstructionReport,
            DecisionBookViewPurpose, DecisionBookViewRequest,
        },
        detector::{self, DetectorEvidenceArtifact},
        execution::{self, ExecutionEvidenceArtifact},
        gate::EvidenceStageGate,
        metric_gate::{
            execution_production_required_missing_count,
            portfolio_production_required_missing_count,
        },
        portfolio::{self, PortfolioRiskEvidenceArtifact},
        settlement::{self, SettlementReconciliationEvidenceArtifact},
        training::{self, TrainingExampleArtifact},
    },
    materialization::{
        ArtifactHasher, MaterializationError, MaterializationResult, StageReportBuilder,
    },
};
use chrono::Utc;
use oxide_arb_models::{
    clickhouse::OpportunityAuditRow,
    domain::{
        MarketFilter, TimeWindow,
        control_factor::{
            EvidenceSourceBundle, InputResolutionReport, MaterializationRunManifest,
            QueryFingerprint, StageArtifactRef, StageCoverageReport, StageOutput, StageReportBody,
            StageWarning,
        },
        evidence::{EvidenceMetric, EvidenceQueryResult},
    },
    enums::{
        clickhouse::ChMarketCategory,
        control_factor::{EvidenceStageStatus, MaterializationStageName},
    },
};
use oxide_arb_repository::traits::EvidenceTimeseriesRepository;
use std::sync::Arc;

pub struct EvidenceEngine {
    timeseries: Arc<dyn EvidenceTimeseriesRepository>,
}

impl EvidenceEngine {
    #[must_use]
    pub fn new(timeseries: Arc<dyn EvidenceTimeseriesRepository>) -> Self {
        Self { timeseries }
    }

    pub async fn book_reconstruction(
        &self,
        manifest: &MaterializationRunManifest,
        input_report: InputResolutionReport,
    ) -> MaterializationResult<StageOutput<BookReconstructionArtifact>> {
        let started_at = Utc::now();
        let tokens = book::expected_tokens(&input_report);
        let snapshots = self
            .timeseries
            .book_snapshots_before(
                &tokens,
                manifest.window.from,
                manifest.simulation_config.snapshot_limit_per_token,
            )
            .await?;
        let l2_events = self
            .timeseries
            .book_l2_replay(
                &tokens,
                TimeWindow::new(manifest.window.from, manifest.window.to),
            )
            .await?;
        let query_fingerprints = vec![snapshots.fingerprint.clone(), l2_events.fingerprint.clone()];
        let artifact = book::reconstruct(&BookReconstructionInput {
            input_report,
            snapshots: snapshots.rows,
            l2_events: l2_events.rows,
            max_replay_gap_ms: manifest.simulation_config.max_replay_gap_ms,
            stale_book_after_ms: manifest.simulation_config.stale_book_after_ms,
            query_fingerprints,
        })?;
        let report = artifact.report.clone();
        let metrics = serde_json::to_value(&report)
            .map_err(|error| MaterializationError::Codec(error.to_string()))?;
        let stage_report = stage_report(
            manifest,
            StageReportSpec {
                stage_name: MaterializationStageName::BookReconstruction,
                started_at,
                status: status_from_book_report(&report),
                coverage: coverage(
                    report.token_count_expected,
                    report.token_count_reconstructed,
                    report.insufficient_reasons.clone(),
                ),
                metrics,
                records_read: report
                    .l2_event_count
                    .saturating_add(report.snapshot_bootstrap_count),
                fingerprints: report.query_fingerprints,
                warnings: Vec::new(),
                input_artifacts: Vec::new(),
            },
            &artifact,
        )?;
        Ok(StageOutput {
            stage_report,
            artifact: Some(artifact),
        })
    }

    pub async fn detector_evidence(
        &self,
        manifest: &MaterializationRunManifest,
        book: &BookReconstructionArtifact,
    ) -> MaterializationResult<StageOutput<DetectorEvidenceArtifact>> {
        let started_at = Utc::now();
        let detections = self
            .timeseries
            .detections(
                MarketFilter {
                    market_ids: manifest.markets.market_ids.clone(),
                    event_ids: manifest.markets.event_ids.clone(),
                    token_ids: manifest.markets.token_ids.clone(),
                    categories: manifest
                        .markets
                        .categories
                        .iter()
                        .copied()
                        .map(ChMarketCategory::from)
                        .collect(),
                },
                TimeWindow::new(manifest.window.from, manifest.window.to),
            )
            .await?;
        let mut book_with_views = book.clone();
        book_with_views.materialize_decision_views(
            detections
                .rows
                .iter()
                .filter_map(|row| {
                    chrono::DateTime::from_timestamp_millis(row.detected_at).map(|decision_time| {
                        DecisionBookViewRequest {
                            market_id: row.market_id.clone(),
                            decision_time,
                            purpose: DecisionBookViewPurpose::Detection,
                        }
                    })
                })
                .collect(),
            manifest.simulation_config.stale_book_after_ms,
        );
        let artifact = detector::build(
            &book_with_views,
            &detections.rows,
            vec![detections.fingerprint],
        );
        let report = artifact.report.clone();
        let metrics = serde_json::to_value(&report)
            .map_err(|error| MaterializationError::Codec(error.to_string()))?;
        let coverage_report = if report.replay_complete() {
            StageCoverageReport::complete(report.live_detection_count)
        } else {
            coverage(
                report.live_detection_count,
                0,
                vec!["detector replay metrics are unavailable".to_owned()],
            )
        };
        let status = EvidenceStageGate {
            output_policy: manifest.output_policy,
            coverage: coverage_report.clone(),
            blocking_issue_count: 0,
            warning_count: 0,
            required_metric_missing_count: detector_missing_metric_count(&report),
        }
        .decide();
        let stage_report = stage_report(
            manifest,
            StageReportSpec {
                stage_name: MaterializationStageName::DetectorEvidence,
                started_at,
                status,
                coverage: coverage_report,
                metrics,
                records_read: report.live_detection_count,
                fingerprints: report.query_fingerprints,
                warnings: Vec::new(),
                input_artifacts: vec![artifact_ref(
                    MaterializationStageName::BookReconstruction,
                    book,
                )?],
            },
            &artifact,
        )?;
        Ok(StageOutput {
            stage_report,
            artifact: Some(artifact),
        })
    }

    pub async fn execution_evidence(
        &self,
        manifest: &MaterializationRunManifest,
        book: &BookReconstructionArtifact,
        detector: &DetectorEvidenceArtifact,
    ) -> MaterializationResult<StageOutput<ExecutionEvidenceArtifact>> {
        let started_at = Utc::now();
        let opportunity_ids = detector
            .detections
            .iter()
            .map(|detection| detection.opportunity_id.clone())
            .collect::<Vec<_>>();
        let audits = self.timeseries.terminal_audits(&opportunity_ids).await?;
        let mut book_with_views = book.clone();
        book_with_views.materialize_decision_views(
            audits
                .rows
                .iter()
                .filter_map(|row| {
                    chrono::DateTime::from_timestamp_millis(row.stage_at).map(|decision_time| {
                        DecisionBookViewRequest {
                            market_id: row.market_id.clone(),
                            decision_time,
                            purpose: DecisionBookViewPurpose::TerminalExecution,
                        }
                    })
                })
                .collect(),
            manifest.simulation_config.stale_book_after_ms,
        );
        let artifact = execution::build(
            &book_with_views,
            &audits.rows,
            vec![audits.fingerprint],
            &manifest.simulation_config,
        );
        let report = artifact.report.clone();
        let warnings = if report.missing_attribution_count > 0 {
            vec![StageWarning {
                code: "audit.attribution_missing".to_owned(),
                message: format!(
                    "{} audit rows are missing scored snapshot attribution",
                    report.missing_attribution_count
                ),
            }]
        } else {
            Vec::new()
        };
        let metrics = serde_json::to_value(&report)
            .map_err(|error| MaterializationError::Codec(error.to_string()))?;
        let coverage_report = StageCoverageReport::complete(
            u64::try_from(artifact.examples.len()).unwrap_or(u64::MAX),
        );
        let status = EvidenceStageGate {
            output_policy: manifest.output_policy,
            coverage: coverage_report.clone(),
            blocking_issue_count: 0,
            warning_count: warnings.len(),
            required_metric_missing_count: execution_production_required_missing_count(&report),
        }
        .decide();
        let stage_report = stage_report(
            manifest,
            StageReportSpec {
                stage_name: MaterializationStageName::ExecutionEvidence,
                started_at,
                status,
                coverage: coverage_report,
                metrics,
                records_read: u64::try_from(artifact.examples.len()).unwrap_or(u64::MAX),
                fingerprints: report.query_fingerprints,
                warnings,
                input_artifacts: vec![
                    artifact_ref(MaterializationStageName::BookReconstruction, book)?,
                    artifact_ref(MaterializationStageName::DetectorEvidence, detector)?,
                ],
            },
            &artifact,
        )?;
        Ok(StageOutput {
            stage_report,
            artifact: Some(artifact),
        })
    }

    pub async fn audit_funnel(
        &self,
        manifest: &MaterializationRunManifest,
    ) -> MaterializationResult<EvidenceQueryResult<OpportunityAuditRow>> {
        self.timeseries
            .audit_funnel(
                MarketFilter {
                    market_ids: manifest.markets.market_ids.clone(),
                    event_ids: manifest.markets.event_ids.clone(),
                    token_ids: manifest.markets.token_ids.clone(),
                    categories: manifest
                        .markets
                        .categories
                        .iter()
                        .copied()
                        .map(ChMarketCategory::from)
                        .collect(),
                },
                TimeWindow::new(manifest.window.from, manifest.window.to),
            )
            .await
            .map_err(Into::into)
    }

    pub fn portfolio_evidence(
        &self,
        manifest: &MaterializationRunManifest,
        audits: &[OpportunityAuditRow],
        query_fingerprints: Vec<QueryFingerprint>,
        source_bundle: &EvidenceSourceBundle,
        execution: &ExecutionEvidenceArtifact,
    ) -> MaterializationResult<StageOutput<PortfolioRiskEvidenceArtifact>> {
        let started_at = Utc::now();
        let artifact = portfolio::build(audits, query_fingerprints, source_bundle);
        let report = artifact.report.clone();
        let metrics = serde_json::to_value(&report)
            .map_err(|error| MaterializationError::Codec(error.to_string()))?;
        let coverage_report = if artifact.sequence_complete {
            StageCoverageReport::complete(u64::try_from(audits.len()).unwrap_or(u64::MAX))
        } else {
            coverage(1, 0, report.insufficient_reasons.clone())
        };
        let status = EvidenceStageGate {
            output_policy: manifest.output_policy,
            coverage: coverage_report.clone(),
            blocking_issue_count: usize::from(!artifact.sequence_complete),
            warning_count: 0,
            required_metric_missing_count: portfolio_production_required_missing_count(&report),
        }
        .decide();
        let stage_report = stage_report(
            manifest,
            StageReportSpec {
                stage_name: MaterializationStageName::PortfolioRiskEvidence,
                started_at,
                status,
                coverage: coverage_report,
                metrics,
                records_read: u64::try_from(audits.len()).unwrap_or(u64::MAX),
                fingerprints: report.query_fingerprints,
                warnings: Vec::new(),
                input_artifacts: vec![artifact_ref(
                    MaterializationStageName::ExecutionEvidence,
                    execution,
                )?],
            },
            &artifact,
        )?;
        Ok(StageOutput {
            stage_report,
            artifact: Some(artifact),
        })
    }

    pub fn settlement_evidence(
        &self,
        manifest: &MaterializationRunManifest,
        audits: &[OpportunityAuditRow],
        query_fingerprints: Vec<QueryFingerprint>,
        source_bundle: &EvidenceSourceBundle,
        execution: &ExecutionEvidenceArtifact,
    ) -> MaterializationResult<StageOutput<SettlementReconciliationEvidenceArtifact>> {
        let started_at = Utc::now();
        let artifact = settlement::build(
            audits,
            query_fingerprints,
            source_bundle,
            manifest.window.to,
        );
        let report = artifact.report.clone();
        let warnings = artifact
            .missing_joins
            .iter()
            .map(|missing| StageWarning {
                code: "settlement.join_missing".to_owned(),
                message: format!(
                    "opportunity {} missing settlement join field {}: {}",
                    missing.opportunity_id, missing.field, missing.reason
                ),
            })
            .collect::<Vec<_>>();
        let metrics = serde_json::to_value(&report)
            .map_err(|error| MaterializationError::Codec(error.to_string()))?;
        let coverage_report = coverage(
            u64::try_from(audits.len()).unwrap_or(u64::MAX),
            u64::try_from(audits.len().saturating_sub(artifact.missing_joins.len()))
                .unwrap_or(u64::MAX),
            artifact
                .missing_joins
                .iter()
                .map(|missing| format!("missing {} for {}", missing.field, missing.opportunity_id))
                .collect(),
        );
        let status = EvidenceStageGate {
            output_policy: manifest.output_policy,
            coverage: coverage_report.clone(),
            blocking_issue_count: artifact.missing_joins.len(),
            warning_count: warnings.len(),
            required_metric_missing_count: settlement_missing_metric_count(&report),
        }
        .decide();
        let stage_report = stage_report(
            manifest,
            StageReportSpec {
                stage_name: MaterializationStageName::SettlementReconciliationEvidence,
                started_at,
                status,
                coverage: coverage_report,
                metrics,
                records_read: u64::try_from(audits.len()).unwrap_or(u64::MAX),
                fingerprints: report.query_fingerprints,
                warnings,
                input_artifacts: vec![artifact_ref(
                    MaterializationStageName::ExecutionEvidence,
                    execution,
                )?],
            },
            &artifact,
        )?;
        Ok(StageOutput {
            stage_report,
            artifact: Some(artifact),
        })
    }

    pub fn training_examples(
        &self,
        manifest: &MaterializationRunManifest,
        detector: &DetectorEvidenceArtifact,
        execution: &ExecutionEvidenceArtifact,
        settlement: &SettlementReconciliationEvidenceArtifact,
    ) -> MaterializationResult<StageOutput<TrainingExampleArtifact>> {
        let started_at = Utc::now();
        let artifact = training::build(
            manifest.requested_factor_types.clone(),
            detector,
            execution,
            settlement,
        )?;
        let report = artifact.report.clone();
        let metrics = serde_json::to_value(&report)
            .map_err(|error| MaterializationError::Codec(error.to_string()))?;
        let coverage_report = StageCoverageReport::complete(report.example_count);
        let status = EvidenceStageGate {
            output_policy: manifest.output_policy,
            coverage: coverage_report.clone(),
            blocking_issue_count: 0,
            warning_count: 0,
            required_metric_missing_count: training_missing_metric_count(&report),
        }
        .decide();
        let stage_report = stage_report(
            manifest,
            StageReportSpec {
                stage_name: MaterializationStageName::TrainingExampleBuild,
                started_at,
                status,
                coverage: coverage_report,
                metrics,
                records_read: report.example_count,
                fingerprints: report.query_fingerprints,
                warnings: Vec::new(),
                input_artifacts: vec![
                    artifact_ref(MaterializationStageName::DetectorEvidence, detector)?,
                    artifact_ref(MaterializationStageName::ExecutionEvidence, execution)?,
                    artifact_ref(
                        MaterializationStageName::SettlementReconciliationEvidence,
                        settlement,
                    )?,
                ],
            },
            &artifact,
        )?;
        Ok(StageOutput {
            stage_report,
            artifact: Some(artifact),
        })
    }
}

struct StageReportSpec {
    stage_name: MaterializationStageName,
    started_at: chrono::DateTime<Utc>,
    status: EvidenceStageStatus,
    coverage: StageCoverageReport,
    metrics: serde_json::Value,
    records_read: u64,
    fingerprints: Vec<QueryFingerprint>,
    warnings: Vec<StageWarning>,
    input_artifacts: Vec<StageArtifactRef>,
}

fn artifact_ref<T: serde::Serialize>(
    stage_name: MaterializationStageName,
    artifact: &T,
) -> MaterializationResult<StageArtifactRef> {
    Ok(StageArtifactRef {
        stage_name,
        artifact_hash: ArtifactHasher::compute(artifact)?,
    })
}

fn stage_report<T: serde::Serialize>(
    manifest: &MaterializationRunManifest,
    spec: StageReportSpec,
    artifact: &T,
) -> MaterializationResult<StageReportBody> {
    let mut builder =
        StageReportBuilder::new(manifest.run_id.clone(), spec.stage_name, spec.started_at)
            .status(spec.status)
            .finished_at(Utc::now())
            .coverage(spec.coverage)
            .metrics(spec.metrics)
            .records_read(spec.records_read)
            .output_artifact(artifact)?;
    for input_artifact in spec.input_artifacts {
        builder = builder.input_artifact(input_artifact);
    }
    for fingerprint in spec.fingerprints {
        builder = builder.query_fingerprint(fingerprint);
    }
    for warning in spec.warnings {
        builder = builder.warning(warning);
    }
    Ok(builder.build())
}

fn status_from_book_report(report: &BookReconstructionReport) -> EvidenceStageStatus {
    if report.production_eligible() {
        EvidenceStageStatus::Completed
    } else {
        EvidenceStageStatus::InsufficientCoverage
    }
}

fn detector_missing_metric_count(report: &detector::DetectorEvidenceReport) -> usize {
    [
        metric_missing(&report.materialized_detection_count),
        metric_missing(&report.matched_opportunity_count),
        metric_missing(&report.missed_live_signal_count),
        metric_missing(&report.extra_materialized_signal_count),
        metric_missing(&report.score_delta_p50),
        metric_missing(&report.score_delta_p95),
        metric_missing(&report.bucket_mismatch_count),
        metric_missing(&report.calibration_snapshot_mismatch_count),
    ]
    .into_iter()
    .sum()
}

fn settlement_missing_metric_count(
    report: &settlement::SettlementReconciliationEvidenceReport,
) -> usize {
    [
        metric_missing(&report.unsettled_trade_count),
        metric_missing(&report.payout_usd_sum),
        metric_missing(&report.realized_pnl_usd_sum),
        metric_missing(&report.settlement_delay_p50_ms),
        metric_missing(&report.settlement_delay_p95_ms),
        metric_missing(&report.cash_drift_usd),
        metric_missing(&report.critical_drift_count),
        metric_missing(&report.metrics_stale_secs),
    ]
    .into_iter()
    .sum()
}

fn training_missing_metric_count(report: &training::TrainingExampleReport) -> usize {
    usize::from(report.example_count > 0 && report.label_count == 0)
        + usize::from(report.query_fingerprints.is_empty())
}

const fn metric_missing<T>(metric: &EvidenceMetric<T>) -> usize {
    if metric.is_available() { 0 } else { 1 }
}

fn coverage(
    expected: u64,
    observed: u64,
    insufficient_reasons: Vec<String>,
) -> StageCoverageReport {
    let missing_rows = expected.saturating_sub(observed);
    let coverage_ratio = if expected == 0 {
        rust_decimal::Decimal::ONE
    } else {
        rust_decimal::Decimal::from(observed) / rust_decimal::Decimal::from(expected)
    };
    StageCoverageReport {
        expected_rows: expected,
        observed_rows: observed,
        missing_rows,
        coverage_ratio,
        insufficient_reasons,
    }
}

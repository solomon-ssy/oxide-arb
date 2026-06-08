use oxide_arb_error::control::MaterializationError;
use oxide_arb_models::{
    domain::{
        control_factor::{
            AccountScope, AnomalyType, AssetScope, BookAgeBucket, BucketRiskDimensions,
            CountBucket, DepthBucket, DrawdownBucket, ExecutionQualityDimensions, FactorDimensions,
            LatencyBucket, MarketAnomalyDimensions, MetricsFreshnessBucket, PortfolioRegime,
            PortfolioRiskDimensions, QueryFingerprint, ReconciliationHealthDimensions,
            RedeemStatusBucket, SpreadBucket, StageArtifactRef, TimeToSettlementBucket,
            UsdExposureBucket,
        },
        evidence::{
            EvidenceSourceRef, EvidenceSourceRefs, FactorFeature, FactorFeatureValue,
            FactorFeatureVector, FactorLabel, FactorLabelRef, FactorTrainingExample,
        },
    },
    enums::{
        common::StalenessLevel,
        control_factor::{ControlFactorType, FactorSeverity, MaterializationStageName},
    },
};
use serde::{Deserialize, Serialize};

use crate::{
    evidence::{
        detector::{DetectorDetectionRef, DetectorEvidenceArtifact},
        execution::ExecutionEvidenceArtifact,
        settlement::SettlementReconciliationEvidenceArtifact,
    },
    materialization::{ArtifactHasher, MaterializationResult},
};

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TrainingExampleArtifact {
    pub report: TrainingExampleReport,
    pub examples: Vec<FactorTrainingExample>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TrainingExampleReport {
    pub dataset_hash: String,
    pub feature_schema_hash: String,
    pub label_schema_hash: String,
    pub entity_count: u64,
    pub example_count: u64,
    pub label_count: u64,
    pub factor_types: Vec<ControlFactorType>,
    pub query_fingerprints: Vec<QueryFingerprint>,
}

pub fn build(
    requested_factor_types: Vec<ControlFactorType>,
    detector: &DetectorEvidenceArtifact,
    execution: &ExecutionEvidenceArtifact,
    settlement: &SettlementReconciliationEvidenceArtifact,
) -> MaterializationResult<TrainingExampleArtifact> {
    let mut examples = Vec::new();
    let query_refs = source_refs(detector, execution, settlement);
    let artifact_refs = artifact_refs(detector, execution, settlement)?;
    for detection in &detector.detections {
        for factor_type in &requested_factor_types {
            let settlement_label = settlement
                .settled_opportunities
                .iter()
                .find(|settled| settled.opportunity_id == detection.opportunity_id)
                .filter(|_| {
                    settlement
                        .missing_joins
                        .iter()
                        .all(|missing| missing.opportunity_id != detection.opportunity_id)
                });
            let label = if let Some(settled) = settlement_label {
                Some(FactorLabel {
                    schema_version: 1,
                    entries: vec![FactorLabelRef {
                        name: "settlement_outcome".to_owned(),
                        source_ref: required_source_ref(&query_refs, "settlement")?,
                        available_at: settled.settled_at,
                    }],
                })
            } else {
                None
            };
            let outcome_available_at = label
                .as_ref()
                .and_then(|label| label.entries.iter().map(|entry| entry.available_at).max());
            examples.push(FactorTrainingExample {
                opportunity_id: detection.opportunity_id.clone(),
                market_id: detection.market_id.clone(),
                factor_type: *factor_type,
                entity_key: entity_key(*factor_type, detection),
                event_time: detection.detected_at,
                features: FactorFeatureVector {
                    schema_version: 1,
                    entries: feature_refs(detection, execution, &query_refs)?,
                },
                label,
                outcome_available_at,
                source_refs: EvidenceSourceRefs {
                    query_refs: query_refs.clone(),
                    artifact_refs: artifact_refs.clone(),
                },
            });
        }
    }
    let dataset_hash = ArtifactHasher::compute(&examples)?.0;
    let feature_schema_hash = ArtifactHasher::compute(&feature_schema_names(&examples))?.0;
    let label_schema_hash = ArtifactHasher::compute(&label_schema_names(&examples))?.0;
    let label_count = examples
        .iter()
        .filter(|example| example.label.is_some())
        .count()
        .try_into()
        .unwrap_or(u64::MAX);
    Ok(TrainingExampleArtifact {
        report: TrainingExampleReport {
            dataset_hash,
            feature_schema_hash,
            label_schema_hash,
            entity_count: u64::try_from(detector.detections.len()).unwrap_or(u64::MAX),
            example_count: u64::try_from(examples.len()).unwrap_or(u64::MAX),
            label_count,
            factor_types: requested_factor_types,
            query_fingerprints: query_refs
                .iter()
                .map(|source| source.query_fingerprint.clone())
                .collect(),
        },
        examples,
    })
}

fn artifact_refs(
    detector: &DetectorEvidenceArtifact,
    execution: &ExecutionEvidenceArtifact,
    settlement: &SettlementReconciliationEvidenceArtifact,
) -> MaterializationResult<Vec<StageArtifactRef>> {
    Ok(vec![
        StageArtifactRef {
            stage_name: MaterializationStageName::DetectorEvidence,
            artifact_hash: ArtifactHasher::compute(detector)?,
        },
        StageArtifactRef {
            stage_name: MaterializationStageName::ExecutionEvidence,
            artifact_hash: ArtifactHasher::compute(execution)?,
        },
        StageArtifactRef {
            stage_name: MaterializationStageName::SettlementReconciliationEvidence,
            artifact_hash: ArtifactHasher::compute(settlement)?,
        },
    ])
}

fn feature_schema_names(examples: &[FactorTrainingExample]) -> Vec<String> {
    let mut names = examples
        .iter()
        .flat_map(|example| {
            example
                .features
                .entries
                .iter()
                .map(|entry| entry.name.clone())
        })
        .collect::<Vec<_>>();
    names.sort();
    names.dedup();
    names
}

fn label_schema_names(examples: &[FactorTrainingExample]) -> Vec<String> {
    let mut names = examples
        .iter()
        .filter_map(|example| example.label.as_ref())
        .flat_map(|label| label.entries.iter().map(|entry| entry.name.clone()))
        .collect::<Vec<_>>();
    names.sort();
    names.dedup();
    names
}

fn entity_key(
    factor_type: ControlFactorType,
    detection: &DetectorDetectionRef,
) -> FactorDimensions {
    match factor_type {
        ControlFactorType::BucketRisk => FactorDimensions::BucketRisk(BucketRiskDimensions {
            category: detection.bucket.category,
            price_zone: detection.bucket.price_zone,
            duration_bucket: detection.bucket.duration_bucket,
            hours_to_settlement_bucket: Some(TimeToSettlementBucket::UnderOneHour),
            neg_risk: None,
            fee_profile: None,
        }),
        ControlFactorType::ExecutionQuality => {
            FactorDimensions::ExecutionQuality(ExecutionQualityDimensions {
                category: detection.bucket.category,
                price_zone: detection.bucket.price_zone,
                spread_bucket: SpreadBucket::Tight,
                depth_bucket: DepthBucket::Deep,
                book_age_bucket: BookAgeBucket::Fresh,
                latency_bucket: LatencyBucket::Unknown,
                staleness_level: StalenessLevel::Fresh,
            })
        }
        ControlFactorType::PortfolioRisk => {
            FactorDimensions::PortfolioRisk(PortfolioRiskDimensions {
                portfolio_regime: PortfolioRegime::Normal,
                category: Some(detection.bucket.category),
                open_position_bucket: CountBucket::One,
                potential_loss_bucket: UsdExposureBucket::Low,
                drawdown_bucket: DrawdownBucket::None,
                settlement_backlog_bucket: CountBucket::Zero,
            })
        }
        ControlFactorType::ReconciliationHealth => {
            FactorDimensions::ReconciliationHealth(ReconciliationHealthDimensions {
                account_scope: AccountScope::Global,
                asset_scope: AssetScope::Market(detection.market_id.clone()),
                drift_severity: FactorSeverity::Warning,
                metrics_freshness_bucket: MetricsFreshnessBucket::Fresh,
                redeem_status_bucket: RedeemStatusBucket::NotRedeemable,
            })
        }
        ControlFactorType::MarketAnomaly => {
            FactorDimensions::MarketAnomaly(MarketAnomalyDimensions {
                market_id: Some(detection.market_id.clone()),
                event_id: None,
                category: Some(detection.bucket.category),
                anomaly_type: AnomalyType::AbnormalBook,
                severity: FactorSeverity::Warning,
            })
        }
    }
}

fn source_refs(
    detector: &DetectorEvidenceArtifact,
    execution: &ExecutionEvidenceArtifact,
    settlement: &SettlementReconciliationEvidenceArtifact,
) -> Vec<EvidenceSourceRef> {
    detector
        .report
        .query_fingerprints
        .iter()
        .map(|fingerprint| source_ref("detector", "EvidenceTimeseriesRepository", fingerprint))
        .chain(
            execution
                .report
                .query_fingerprints
                .iter()
                .map(|fingerprint| {
                    source_ref("execution", "EvidenceTimeseriesRepository", fingerprint)
                }),
        )
        .chain(
            settlement
                .report
                .query_fingerprints
                .iter()
                .map(|fingerprint| {
                    source_ref("settlement", "EvidenceTimeseriesRepository", fingerprint)
                }),
        )
        .collect()
}

fn source_ref(
    source_domain: &str,
    source_repository: &str,
    fingerprint: &QueryFingerprint,
) -> EvidenceSourceRef {
    EvidenceSourceRef {
        source_domain: source_domain.to_owned(),
        source_repository: source_repository.to_owned(),
        source_table: None,
        query_fingerprint: fingerprint.clone(),
        row_ref: None,
        artifact_hash: None,
    }
}

fn feature_refs(
    detection: &DetectorDetectionRef,
    execution: &ExecutionEvidenceArtifact,
    query_refs: &[EvidenceSourceRef],
) -> MaterializationResult<Vec<FactorFeature>> {
    let market_context = query_refs
        .iter()
        .find(|source| source.source_domain == "detector")
        .cloned()
        .ok_or_else(|| missing_source_ref("detector"))?;
    let execution_ref = query_refs
        .iter()
        .find(|source| source.source_domain == "execution")
        .cloned()
        .ok_or_else(|| missing_source_ref("execution"))?;
    let execution_example = execution
        .examples
        .iter()
        .find(|example| example.opportunity_id == detection.opportunity_id);
    Ok(vec![
        FactorFeature {
            name: "detector.materialized_detected".to_owned(),
            value: FactorFeatureValue::Bool(detection.replay.materialized_detected),
            source_ref: market_context,
            observed_at: detection.detected_at,
            point_in_time_visible: true,
        },
        FactorFeature {
            name: "detector.score".to_owned(),
            value: FactorFeatureValue::I64(detection.score.unwrap_or_default()),
            source_ref: execution_ref.clone(),
            observed_at: detection.detected_at,
            point_in_time_visible: true,
        },
        FactorFeature {
            name: "execution.strict_fok_fill".to_owned(),
            value: FactorFeatureValue::Bool(
                execution_example.is_some_and(|example| example.simulated_strict_fok_fill),
            ),
            source_ref: execution_ref,
            observed_at: detection.detected_at,
            point_in_time_visible: true,
        },
    ])
}

fn required_source_ref(
    query_refs: &[EvidenceSourceRef],
    source_domain: &str,
) -> MaterializationResult<EvidenceSourceRef> {
    query_refs
        .iter()
        .find(|source| source.source_domain == source_domain)
        .cloned()
        .ok_or_else(|| missing_source_ref(source_domain))
}

fn missing_source_ref(source_domain: &str) -> MaterializationError {
    MaterializationError::stable(
        "evidence.source_ref_missing",
        format!("training dataset is missing required {source_domain} source ref"),
    )
}

#[cfg(test)]
mod tests {
    use chrono::{TimeZone, Utc};
    use oxide_arb_models::{
        domain::{control_factor::QueryFingerprint, evidence::EvidenceMetric},
        enums::control_factor::ControlFactorType,
        types::{MarketId, OpportunityId},
    };

    use oxide_arb_models::enums::{
        calibration::{DurationBucket, PriceZone},
        common::MarketCategory,
    };

    use crate::evidence::{
        detector::{
            DetectorBucketRef, DetectorDetectionRef, DetectorEvidenceArtifact,
            DetectorEvidenceReport, DetectorMismatchRef, DetectorReplayRef,
        },
        execution::{ExecutionEvidenceArtifact, ExecutionEvidenceReport},
        settlement::{
            SettledOpportunityRef, SettlementReconciliationEvidenceArtifact,
            SettlementReconciliationEvidenceReport,
        },
        training::build,
    };

    #[test]
    fn dataset_hash_is_reproducible_for_same_examples() {
        let first = build(
            vec![ControlFactorType::BucketRisk],
            &detector(),
            &execution(),
            &settlement(),
        )
        .expect("first dataset");
        let second = build(
            vec![ControlFactorType::BucketRisk],
            &detector(),
            &execution(),
            &settlement(),
        )
        .expect("second dataset");

        assert_eq!(first.report.dataset_hash, second.report.dataset_hash);
        assert_eq!(first.report.example_count, 1);
        assert_eq!(first.report.label_count, 0);
    }

    #[test]
    fn delayed_settlement_label_sets_outcome_available_at() {
        let artifact = build(
            vec![ControlFactorType::BucketRisk],
            &detector(),
            &execution(),
            &settlement_with_label(),
        )
        .expect("dataset");

        let example = artifact.examples.first().expect("example");
        assert_eq!(artifact.report.label_count, 1);
        assert!(example.label.is_some());
        assert_eq!(
            example.outcome_available_at,
            Some(Utc.timestamp_millis_opt(2_000).single().expect("settled"))
        );
        assert!(
            example
                .features
                .entries
                .iter()
                .all(|entry| entry.point_in_time_visible)
        );
    }

    fn detector() -> DetectorEvidenceArtifact {
        DetectorEvidenceArtifact {
            report: DetectorEvidenceReport {
                live_detection_count: 1,
                reconstructed_book_context_count: 1,
                materialized_detection_count: EvidenceMetric::Unavailable {
                    code: "test.materialized_detection_missing".to_owned(),
                    reason: "materialized_detection unavailable in fixture".to_owned(),
                },
                matched_opportunity_count: EvidenceMetric::Unavailable {
                    code: "test.matched_missing".to_owned(),
                    reason: "matched unavailable in fixture".to_owned(),
                },
                missed_live_signal_count: EvidenceMetric::Unavailable {
                    code: "test.missed_missing".to_owned(),
                    reason: "missed unavailable in fixture".to_owned(),
                },
                extra_materialized_signal_count: EvidenceMetric::Unavailable {
                    code: "test.extra_missing".to_owned(),
                    reason: "extra unavailable in fixture".to_owned(),
                },
                score_delta_p50: EvidenceMetric::Unavailable {
                    code: "test.score_delta_p50_missing".to_owned(),
                    reason: "score_delta_p50 unavailable in fixture".to_owned(),
                },
                score_delta_p95: EvidenceMetric::Unavailable {
                    code: "test.score_delta_p95_missing".to_owned(),
                    reason: "score_delta_p95 unavailable in fixture".to_owned(),
                },
                bucket_mismatch_count: EvidenceMetric::Unavailable {
                    code: "test.bucket_mismatch_missing".to_owned(),
                    reason: "bucket_mismatch unavailable in fixture".to_owned(),
                },
                calibration_snapshot_mismatch_count: EvidenceMetric::Unavailable {
                    code: "test.calibration_mismatch_missing".to_owned(),
                    reason: "calibration_mismatch unavailable in fixture".to_owned(),
                },
                query_fingerprints: vec![QueryFingerprint("detector".to_owned())],
            },
            detections: vec![DetectorDetectionRef {
                opportunity_id: OpportunityId::new(oxide_arb_test_support::seeded_uuid("opp")),
                market_id: MarketId::new("market"),
                detected_at: Utc
                    .timestamp_millis_opt(1_000)
                    .single()
                    .expect("fixed detection time"),
                bucket: DetectorBucketRef {
                    category: MarketCategory::Politics,
                    price_zone: PriceZone::Z95,
                    duration_bucket: DurationBucket::Short,
                },
                replay: DetectorReplayRef {
                    has_reconstructed_book: true,
                    materialized_detected: true,
                },
                mismatches: DetectorMismatchRef {
                    bucket: false,
                    calibration_snapshot: false,
                },
                score_delta: Some(0),
                score: Some(1),
                calibration_snapshot_hash: Some("calibration".to_owned()),
            }],
        }
    }

    fn execution() -> ExecutionEvidenceArtifact {
        ExecutionEvidenceArtifact {
            report: ExecutionEvidenceReport {
                strict_fok_fill_rate_bps: 0,
                live_fill_rate_bps: 0,
                true_fill_count: 0,
                true_miss_count: 0,
                false_fill_count: 0,
                false_miss_count: 0,
                simulated_vwap_p50_bps: EvidenceMetric::Unavailable {
                    code: "test.vwap_p50_missing".to_owned(),
                    reason: "vwap_p50 unavailable in fixture".to_owned(),
                },
                simulated_vwap_p95_bps: EvidenceMetric::Unavailable {
                    code: "test.vwap_p95_missing".to_owned(),
                    reason: "vwap_p95 unavailable in fixture".to_owned(),
                },
                realized_slippage_p50_bps: EvidenceMetric::Unavailable {
                    code: "test.slippage_p50_missing".to_owned(),
                    reason: "slippage_p50 unavailable in fixture".to_owned(),
                },
                realized_slippage_p95_bps: EvidenceMetric::Unavailable {
                    code: "test.slippage_p95_missing".to_owned(),
                    reason: "slippage_p95 unavailable in fixture".to_owned(),
                },
                depth_consumed_pct_p50_bps: EvidenceMetric::Unavailable {
                    code: "test.depth_p50_missing".to_owned(),
                    reason: "depth_p50 unavailable in fixture".to_owned(),
                },
                depth_consumed_pct_p95_bps: EvidenceMetric::Unavailable {
                    code: "test.depth_p95_missing".to_owned(),
                    reason: "depth_p95 unavailable in fixture".to_owned(),
                },
                latency_shifted_miss_rate_bps: EvidenceMetric::Unavailable {
                    code: "test.latency_missing".to_owned(),
                    reason: "latency unavailable in fixture".to_owned(),
                },
                adverse_selection_loss_p95_bps: EvidenceMetric::Unavailable {
                    code: "test.adverse_selection_missing".to_owned(),
                    reason: "adverse_selection unavailable in fixture".to_owned(),
                },
                book_age_fill_correlation_bps: EvidenceMetric::Unavailable {
                    code: "test.book_age_correlation_missing".to_owned(),
                    reason: "book_age_correlation unavailable in fixture".to_owned(),
                },
                missing_attribution_count: 0,
                query_fingerprints: vec![QueryFingerprint("execution".to_owned())],
            },
            examples: Vec::new(),
            audits: Vec::new(),
        }
    }

    fn settlement() -> SettlementReconciliationEvidenceArtifact {
        SettlementReconciliationEvidenceArtifact {
            report: SettlementReconciliationEvidenceReport {
                settled_trade_count: 0,
                unsettled_trade_count: EvidenceMetric::Unavailable {
                    code: "test.unsettled_missing".to_owned(),
                    reason: "unsettled unavailable in fixture".to_owned(),
                },
                won_count: 0,
                lost_count: 0,
                payout_usd_sum: EvidenceMetric::Unavailable {
                    code: "test.payout_missing".to_owned(),
                    reason: "payout unavailable in fixture".to_owned(),
                },
                realized_pnl_usd_sum: EvidenceMetric::Unavailable {
                    code: "test.pnl_missing".to_owned(),
                    reason: "pnl unavailable in fixture".to_owned(),
                },
                settlement_delay_p50_ms: EvidenceMetric::Unavailable {
                    code: "test.delay_p50_missing".to_owned(),
                    reason: "delay_p50 unavailable in fixture".to_owned(),
                },
                settlement_delay_p95_ms: EvidenceMetric::Unavailable {
                    code: "test.delay_p95_missing".to_owned(),
                    reason: "delay_p95 unavailable in fixture".to_owned(),
                },
                redeem_pending_count: 0,
                redeem_failed_count: 0,
                cash_drift_usd: EvidenceMetric::Unavailable {
                    code: "test.cash_drift_missing".to_owned(),
                    reason: "cash_drift unavailable in fixture".to_owned(),
                },
                critical_drift_count: EvidenceMetric::Unavailable {
                    code: "test.critical_drift_missing".to_owned(),
                    reason: "critical_drift unavailable in fixture".to_owned(),
                },
                metrics_stale_secs: EvidenceMetric::Unavailable {
                    code: "test.metrics_stale_missing".to_owned(),
                    reason: "metrics_stale unavailable in fixture".to_owned(),
                },
                query_fingerprints: vec![QueryFingerprint("settlement".to_owned())],
            },
            missing_joins: Vec::new(),
            settled_opportunity_ids: Vec::new(),
            settled_opportunities: Vec::new(),
        }
    }

    fn settlement_with_label() -> SettlementReconciliationEvidenceArtifact {
        let mut artifact = settlement();
        artifact.report.settled_trade_count = 1;
        artifact.settled_opportunities.push(SettledOpportunityRef {
            opportunity_id: OpportunityId::new(oxide_arb_test_support::seeded_uuid("opp")),
            settled_at: Utc.timestamp_millis_opt(2_000).single().expect("settled"),
        });
        artifact.settled_opportunity_ids.push(OpportunityId::new(
            oxide_arb_test_support::seeded_uuid("opp"),
        ));
        artifact
    }
}

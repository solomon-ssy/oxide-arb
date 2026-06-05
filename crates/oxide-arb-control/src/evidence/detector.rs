use std::sync::Arc;

use chrono::{DateTime, Duration, TimeZone, Utc};
use oxide_arb_algorithm::{
    calibration::{CalibrationEntry, ResolutionCalibrator},
    endgame::{EndgameDetectInput, EndgameDetector},
    fee::FeeEstimator,
    scorer::EndgameScorer,
};
use oxide_arb_models::{
    clickhouse::{CalibrationSnapshotRow, OpportunityDetectionRow},
    config::CalibrationConfig,
    domain::{
        RuntimeConfigDocument,
        book::{BookSnapshot, EndgameBookPair},
        calibration::BucketKey,
        control_factor::QueryFingerprint,
        evidence::EvidenceMetric,
    },
    enums::{
        calibration::{DurationBucket, PriceZone},
        clickhouse::{ChDurationBucket, ChPriceZone},
        common::{MarketCategory, StalenessLevel},
    },
    types::{MarketId, OpportunityId, Price, Shares, TokenId, Usd},
};
use serde::{Deserialize, Serialize};

use crate::evidence::book::{
    BookReconstructionArtifact, DecisionBookView, DecisionBookViewPurpose, ReconstructedTokenBook,
};

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DetectorEvidenceArtifact {
    pub report: DetectorEvidenceReport,
    pub detections: Vec<DetectorDetectionRef>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DetectorEvidenceReport {
    pub live_detection_count: u64,
    pub reconstructed_book_context_count: u64,
    pub materialized_detection_count: EvidenceMetric<u64>,
    pub matched_opportunity_count: EvidenceMetric<u64>,
    pub missed_live_signal_count: EvidenceMetric<u64>,
    pub extra_materialized_signal_count: EvidenceMetric<u64>,
    pub score_delta_p50: EvidenceMetric<i64>,
    pub score_delta_p95: EvidenceMetric<i64>,
    pub bucket_mismatch_count: EvidenceMetric<u64>,
    pub calibration_snapshot_mismatch_count: EvidenceMetric<u64>,
    pub query_fingerprints: Vec<QueryFingerprint>,
}

impl DetectorEvidenceReport {
    #[must_use]
    pub const fn replay_complete(&self) -> bool {
        self.materialized_detection_count.is_available()
            && self.matched_opportunity_count.is_available()
            && self.missed_live_signal_count.is_available()
            && self.extra_materialized_signal_count.is_available()
            && self.score_delta_p50.is_available()
            && self.score_delta_p95.is_available()
            && self.bucket_mismatch_count.is_available()
            && self.calibration_snapshot_mismatch_count.is_available()
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DetectorBucketRef {
    pub category: MarketCategory,
    pub price_zone: PriceZone,
    pub duration_bucket: DurationBucket,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DetectorDetectionRef {
    pub opportunity_id: OpportunityId,
    pub market_id: MarketId,
    pub detected_at: DateTime<Utc>,
    pub bucket: DetectorBucketRef,
    pub replay: DetectorReplayRef,
    pub mismatches: DetectorMismatchRef,
    pub score_delta: Option<i64>,
    pub score: Option<i64>,
    pub calibration_snapshot_hash: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DetectorReplayRef {
    pub has_reconstructed_book: bool,
    pub materialized_detected: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DetectorMismatchRef {
    pub bucket: bool,
    pub calibration_snapshot: bool,
}

#[must_use]
pub fn build(
    book: &BookReconstructionArtifact,
    detections: &[OpportunityDetectionRow],
    query_fingerprints: Vec<QueryFingerprint>,
) -> DetectorEvidenceArtifact {
    let refs = detections
        .iter()
        .map(|row| {
            let detected_at = Utc
                .timestamp_millis_opt(row.detected_at)
                .single()
                .unwrap_or_else(Utc::now);
            let decision_view = decision_view_for(book, row, detected_at);
            let has_reconstructed_book = decision_view.is_some_and(|view| {
                view.production_eligible && view.yes_book.is_some() && view.no_book.is_some()
            });
            let replay = replay_detection(book, row, decision_view, detected_at);
            let materialized_detected = replay.as_ref().is_some_and(|replay| replay.detected);
            let calibration_snapshot_mismatch = replay
                .as_ref()
                .is_none_or(|replay| replay.calibration_snapshot_mismatch);
            let bucket_mismatch = replay.as_ref().is_none_or(|replay| replay.bucket_mismatch);
            DetectorDetectionRef {
                opportunity_id: row.opportunity_id.clone(),
                market_id: row.market_id.clone(),
                detected_at,
                bucket: DetectorBucketRef {
                    category: MarketCategory::from(row.category),
                    price_zone: PriceZone::from(row.price_zone),
                    duration_bucket: DurationBucket::from(row.duration_bucket),
                },
                replay: DetectorReplayRef {
                    has_reconstructed_book,
                    materialized_detected,
                },
                mismatches: DetectorMismatchRef {
                    bucket: bucket_mismatch,
                    calibration_snapshot: calibration_snapshot_mismatch,
                },
                score_delta: replay.as_ref().and_then(|replay| replay.score_delta),
                score: row.score,
                calibration_snapshot_hash: row.calibration_snapshot_hash.clone(),
            }
        })
        .collect::<Vec<_>>();
    let live_detection_count = u64::try_from(detections.len()).unwrap_or(u64::MAX);
    DetectorEvidenceArtifact {
        report: build_report(book, &refs, live_detection_count, query_fingerprints),
        detections: refs,
    }
}

fn build_report(
    book: &BookReconstructionArtifact,
    refs: &[DetectorDetectionRef],
    live_detection_count: u64,
    query_fingerprints: Vec<QueryFingerprint>,
) -> DetectorEvidenceReport {
    let reconstructed_book_context_count = refs
        .iter()
        .filter(|detected| detected.replay.has_reconstructed_book)
        .count()
        .try_into()
        .unwrap_or(u64::MAX);
    let replay_inputs_complete = detector_replay_inputs_complete(book);
    let materialized_detection_count = refs
        .iter()
        .filter(|detected| detected.replay.materialized_detected)
        .count()
        .try_into()
        .unwrap_or(u64::MAX);
    let matched_opportunity_count = refs
        .iter()
        .filter(|detected| detected.replay.materialized_detected)
        .count()
        .try_into()
        .unwrap_or(u64::MAX);
    let score_deltas = refs
        .iter()
        .filter_map(|detected| detected.score_delta)
        .collect::<Vec<_>>();
    let bucket_mismatch_count = refs
        .iter()
        .filter(|detected| detected.mismatches.bucket)
        .count()
        .try_into()
        .unwrap_or(u64::MAX);
    let calibration_snapshot_mismatch_count = refs
        .iter()
        .filter(|detected| detected.mismatches.calibration_snapshot)
        .count()
        .try_into()
        .unwrap_or(u64::MAX);
    DetectorEvidenceReport {
        live_detection_count,
        reconstructed_book_context_count,
        materialized_detection_count: replay_metric(
            replay_inputs_complete,
            materialized_detection_count,
            "detector.materialized_outputs_missing",
            "manifest-pinned detector replay requires runtime config and calibration snapshots",
        ),
        matched_opportunity_count: replay_metric(
            replay_inputs_complete,
            matched_opportunity_count,
            "detector.matching_missing",
            "matched opportunity count requires complete replay inputs",
        ),
        missed_live_signal_count: replay_metric(
            replay_inputs_complete,
            live_detection_count.saturating_sub(matched_opportunity_count),
            "detector.matching_missing",
            "missed live signal count requires complete replay inputs",
        ),
        extra_materialized_signal_count: replay_metric(
            replay_inputs_complete,
            0,
            "detector.matching_missing",
            "extra materialized signal count requires complete replay inputs",
        ),
        score_delta_p50: replay_metric(
            replay_inputs_complete,
            percentile_i64(&score_deltas, 50).unwrap_or(0),
            "detector.replay_delta_missing",
            "score delta requires complete replay inputs",
        ),
        score_delta_p95: replay_metric(
            replay_inputs_complete,
            percentile_i64(&score_deltas, 95).unwrap_or(0),
            "detector.replay_delta_missing",
            "score delta requires complete replay inputs",
        ),
        bucket_mismatch_count: replay_metric(
            replay_inputs_complete,
            bucket_mismatch_count,
            "detector.bucket_replay_missing",
            "bucket mismatch requires complete replay inputs",
        ),
        calibration_snapshot_mismatch_count: replay_metric(
            replay_inputs_complete,
            calibration_snapshot_mismatch_count,
            "detector.calibration_replay_missing",
            "calibration mismatch requires complete replay inputs",
        ),
        query_fingerprints,
    }
}

fn replay_metric<T>(available: bool, value: T, code: &str, reason: &str) -> EvidenceMetric<T> {
    if available {
        EvidenceMetric::Available { value }
    } else {
        EvidenceMetric::Unavailable {
            code: code.to_owned(),
            reason: reason.to_owned(),
        }
    }
}

#[derive(Debug, Clone, Copy)]
struct DetectorReplayOutcome {
    detected: bool,
    score_delta: Option<i64>,
    bucket_mismatch: bool,
    calibration_snapshot_mismatch: bool,
}

fn replay_detection(
    book: &BookReconstructionArtifact,
    row: &OpportunityDetectionRow,
    decision_view: Option<&DecisionBookView>,
    detected_at: DateTime<Utc>,
) -> Option<DetectorReplayOutcome> {
    let runtime_config = runtime_config(book)?;
    let endgame_config = runtime_config.detection.endgame.as_ref()?;
    let calibration_config = runtime_config.detection.calibration.as_ref()?;
    let decision_view = decision_view?;
    if !decision_view.production_eligible {
        return None;
    }
    let pair = endgame_pair(decision_view)?;
    let deadline = market_deadline(book, &row.market_id)?;
    let category = MarketCategory::from(row.category);
    let token_yes = row.token_yes.as_ref()?;
    let token_no = row.token_no.as_ref()?;
    let calibrator = Arc::new(calibrator_from_snapshots(
        &book.source_bundle.calibration_snapshots,
        calibration_config,
        detected_at,
    ));
    let detector = EndgameDetector::new(
        endgame_config,
        calibration_config,
        calibrator,
        ReplayFeeEstimator {
            fee: row.total_fees_usd.to_usd(),
        },
    );
    let scorer = EndgameScorer::new(
        endgame_config.scorer.clone(),
        &endgame_config.fill_probability,
        endgame_config.settlement_window_hours,
    );
    let direction = detector.detect_direction(pair.view())?;
    let detect_input = EndgameDetectInput {
        market_id: &row.market_id,
        event_id: &row.event_id,
        token_yes,
        token_no,
        book: &pair,
        direction,
        category,
        staleness: StalenessLevel::Fresh,
        settlement_deadline: Some(deadline),
    };
    warm_convergence(&detector, &detect_input, row, detected_at);
    let Some(opportunity) = detector.detect_with_direction(&detect_input, detected_at) else {
        return Some(DetectorReplayOutcome {
            detected: false,
            score_delta: None,
            bucket_mismatch: true,
            calibration_snapshot_mismatch: true,
        });
    };
    if opportunity.expected_net_profit.inner() < runtime_config.detection.min_profit_threshold_usd {
        return Some(DetectorReplayOutcome {
            detected: false,
            score_delta: None,
            bucket_mismatch: true,
            calibration_snapshot_mismatch: true,
        });
    }
    if opportunity.depth_used_pct > endgame_config.scorer.max_depth_usage_pct.to_pct_decimal() {
        return Some(DetectorReplayOutcome {
            detected: false,
            score_delta: None,
            bucket_mismatch: true,
            calibration_snapshot_mismatch: true,
        });
    }
    let draft = scorer.score(&opportunity, detected_at, None);
    let detected = draft.score >= endgame_config.scorer.min_score;
    let score_delta = row.score.map(|score| draft.score.micro() - score);
    let replay_bucket = opportunity.meta.duration_bucket;
    let replay_zone = opportunity.meta.price_zone;
    let replay_snapshot_hash = calibration_snapshot_hash(
        &book.source_bundle.calibration_snapshots,
        category,
        replay_zone,
        replay_bucket,
        detected_at,
    );
    Some(DetectorReplayOutcome {
        detected,
        score_delta,
        bucket_mismatch: ChPriceZone::from(replay_zone) != row.price_zone
            || ChDurationBucket::from(replay_bucket) != row.duration_bucket,
        calibration_snapshot_mismatch: row.calibration_snapshot_hash != replay_snapshot_hash,
    })
}

fn decision_view_for<'a>(
    book: &'a BookReconstructionArtifact,
    row: &OpportunityDetectionRow,
    detected_at: DateTime<Utc>,
) -> Option<&'a DecisionBookView> {
    book.decision_views.iter().find(|view| {
        view.market_id == row.market_id
            && view.decision_time == detected_at
            && view.purpose == DecisionBookViewPurpose::Detection
    })
}

fn runtime_config(book: &BookReconstructionArtifact) -> Option<RuntimeConfigDocument> {
    book.source_bundle
        .runtime_config
        .as_ref()
        .and_then(|version| serde_json::from_value(version.config_json.clone()).ok())
}

fn detector_replay_inputs_complete(book: &BookReconstructionArtifact) -> bool {
    book.source_bundle.runtime_config.as_ref().is_some()
        && runtime_config(book).is_some_and(|config| {
            config.detection.endgame.is_some() && config.detection.calibration.is_some()
        })
        && !book.source_bundle.calibration_snapshots.is_empty()
        && book
            .market_books
            .iter()
            .all(|market| market.settlement_deadline.is_some())
}

#[derive(Debug, Clone, Copy)]
struct ReplayFeeEstimator {
    fee: Usd,
}

impl FeeEstimator for ReplayFeeEstimator {
    fn estimate_fee(
        &self,
        _shares: Shares,
        _price: Price,
        _category: MarketCategory,
        _token_id: &TokenId,
    ) -> Usd {
        self.fee
    }
}

fn endgame_pair(view: &DecisionBookView) -> Option<EndgameBookPair> {
    let yes = view.yes_book.as_ref()?;
    let no = view.no_book.as_ref()?;
    Some(EndgameBookPair {
        yes: Arc::new(book_snapshot(&yes.book)),
        no: Arc::new(book_snapshot(&no.book)),
    })
}

fn book_snapshot(book: &ReconstructedTokenBook) -> BookSnapshot {
    BookSnapshot::new(
        Arc::from(book.bids.clone().into_boxed_slice()),
        Arc::from(book.asks.clone().into_boxed_slice()),
        u64::try_from(book.event_time.timestamp_millis().max(0)).unwrap_or(u64::MAX),
        book.book_version,
    )
}

fn market_deadline(
    book: &BookReconstructionArtifact,
    market_id: &MarketId,
) -> Option<DateTime<Utc>> {
    book.market_books
        .iter()
        .find(|market| &market.market_id == market_id)?
        .settlement_deadline
}

fn warm_convergence<F: FeeEstimator>(
    detector: &EndgameDetector<F>,
    input: &EndgameDetectInput<'_>,
    row: &OpportunityDetectionRow,
    detected_at: DateTime<Utc>,
) {
    let warm_at = detected_at - Duration::seconds(i64::from(row.convergence_secs.max(1)));
    let _ = detector.detect_with_direction(input, warm_at);
}

fn calibrator_from_snapshots(
    snapshots: &[CalibrationSnapshotRow],
    config: &CalibrationConfig,
    detected_at: DateTime<Utc>,
) -> ResolutionCalibrator {
    let entries = snapshots
        .iter()
        .filter(|snapshot| snapshot.event_time <= detected_at.timestamp_millis())
        .map(calibration_entry)
        .collect::<Vec<_>>();
    ResolutionCalibrator::from_entries(entries, config.clone())
}

fn calibration_entry(row: &CalibrationSnapshotRow) -> CalibrationEntry {
    CalibrationEntry {
        bucket_key: BucketKey {
            category: MarketCategory::from(row.category),
            price_zone: PriceZone::from(row.price_zone),
            duration_bucket: DurationBucket::from(row.duration_bucket),
        },
        total_count: row.total_count,
        correct_count: row.correct_count,
        alpha_prior: row.alpha_prior.to_decimal(),
        beta_prior: row.beta_prior.to_decimal(),
        fallback_tier: row.fallback_tier,
    }
}

fn calibration_snapshot_hash(
    snapshots: &[CalibrationSnapshotRow],
    category: MarketCategory,
    target_price_zone: PriceZone,
    target_duration_bucket: DurationBucket,
    detected_at: DateTime<Utc>,
) -> Option<String> {
    snapshots
        .iter()
        .filter(|snapshot| {
            MarketCategory::from(snapshot.category) == category
                && PriceZone::from(snapshot.price_zone) == target_price_zone
                && DurationBucket::from(snapshot.duration_bucket) == target_duration_bucket
                && snapshot.event_time <= detected_at.timestamp_millis()
        })
        .max_by(|left, right| {
            left.event_time
                .cmp(&right.event_time)
                .then(left.ingestion_time.cmp(&right.ingestion_time))
                .then(left.sequence.cmp(&right.sequence))
        })
        .map(|snapshot| snapshot.snapshot_hash.clone())
}

fn percentile_i64(values: &[i64], pct: usize) -> Option<i64> {
    if values.is_empty() {
        return None;
    }
    let mut sorted = values.to_vec();
    sorted.sort_unstable();
    let idx = sorted
        .len()
        .saturating_sub(1)
        .saturating_mul(pct)
        .saturating_div(100);
    Some(sorted[idx])
}

#[cfg(test)]
mod tests {
    use chrono::{TimeZone, Utc};
    use oxide_arb_models::{
        clickhouse::{
            CalibrationSnapshotRow, ChBps, ChDecimal64, ChFactor, ChPrice, ChProbability,
            ChSchemaVersion, ChShares, ChUsd, OpportunityDetectionRow,
        },
        config::{CalibrationConfig, EndgameDetectionConfig},
        domain::{
            BookLevel, DetectionRuntimeConfig, EvidenceSourceBundle, ExecutionRuntimeConfig,
            OperatorRuntimeConfig, RiskLimitRuntimeConfig, RuntimeConfigDocument,
            RuntimeConfigVersionInfo, SizingRuntimeConfig, control_factor::QueryFingerprint,
        },
        enums::{
            clickhouse::{ChDurationBucket, ChFactSource, ChMarketCategory, ChPriceZone, ChSide},
            runtime_config::RuntimeConfigVersionSource,
        },
        types::{
            EventId, MarketId, OpportunityId, Price, RuntimeConfigVersionId, Shares, TokenId, Usd,
        },
    };
    use rust_decimal_macros::dec;

    use crate::evidence::{
        book::{
            BookReconstructionArtifact, BookReconstructionReport, DecisionBookView,
            DecisionBookViewPurpose, DecisionTokenBookView, MarketBookReconstruction,
            ReconstructedTokenBook, ReconstructedTokenBookTimeline,
        },
        detector::build,
    };

    #[test]
    fn detector_replay_produces_available_metrics() {
        let artifact = build(
            &book(),
            &[detection_row()],
            vec![QueryFingerprint("det".to_owned())],
        );

        assert!(artifact.report.materialized_detection_count.is_available());
        assert!(artifact.report.score_delta_p50.is_available());
        assert!(artifact.detections[0].replay.materialized_detected);
    }

    #[test]
    fn detector_replay_without_full_config_is_not_production_complete() {
        let mut book = book();
        let version = book
            .source_bundle
            .runtime_config
            .as_mut()
            .expect("runtime config");
        let mut config: RuntimeConfigDocument =
            serde_json::from_value(version.config_json.clone()).expect("config");
        config.detection.endgame = None;
        version.config_json = serde_json::to_value(config).expect("config json");
        let artifact = build(
            &book,
            &[detection_row()],
            vec![QueryFingerprint("det".to_owned())],
        );

        assert!(!artifact.report.replay_complete());
        assert!(!artifact.report.materialized_detection_count.is_available());
    }

    fn book() -> BookReconstructionArtifact {
        let event_time = Utc.timestamp_millis_opt(1_000).single().expect("time");
        let deadline = Utc.timestamp_millis_opt(60_000).single().expect("deadline");
        let yes = TokenId::new("yes");
        let no = TokenId::new("no");
        let yes_book = token_book(
            yes.clone(),
            vec![],
            vec![level(dec!(0.94), dec!(2_000))],
            event_time,
        );
        let no_book = token_book(
            no.clone(),
            vec![],
            vec![level(dec!(0.05), dec!(2_000))],
            event_time,
        );
        let mut source_bundle = EvidenceSourceBundle::empty();
        source_bundle.runtime_config = Some(runtime_config());
        source_bundle.calibration_snapshots = vec![calibration_snapshot()];
        BookReconstructionArtifact {
            report: BookReconstructionReport {
                token_count_expected: 2,
                token_count_reconstructed: 2,
                l2_event_count: 0,
                snapshot_bootstrap_count: 2,
                gap_count: 0,
                max_gap_ms: 0,
                median_book_age_ms: 0,
                p95_book_age_ms: 0,
                crossed_book_count: 0,
                invalid_level_count: 0,
                stale_interval_ms: 0,
                insufficient_reasons: Vec::new(),
                query_fingerprints: Vec::new(),
            },
            market_books: vec![MarketBookReconstruction {
                market_id: MarketId::new("market"),
                yes_token_id: yes.clone(),
                no_token_id: no.clone(),
                settlement_deadline: Some(deadline),
                yes_book: Some(yes_book.clone()),
                no_book: Some(no_book.clone()),
            }],
            token_timelines: vec![
                ReconstructedTokenBookTimeline {
                    token_id: yes,
                    books: vec![yes_book.clone()],
                },
                ReconstructedTokenBookTimeline {
                    token_id: no,
                    books: vec![no_book.clone()],
                },
            ],
            decision_views: vec![DecisionBookView {
                market_id: MarketId::new("market"),
                decision_time: event_time,
                purpose: DecisionBookViewPurpose::Detection,
                yes_book: Some(DecisionTokenBookView {
                    book: yes_book,
                    book_age_ms: 0,
                    max_gap_ms: 0,
                    stale: false,
                    crossed: false,
                    invalid_level_count: 0,
                }),
                no_book: Some(DecisionTokenBookView {
                    book: no_book,
                    book_age_ms: 0,
                    max_gap_ms: 0,
                    stale: false,
                    crossed: false,
                    invalid_level_count: 0,
                }),
                production_eligible: true,
                insufficient_reasons: Vec::new(),
                query_fingerprints: Vec::new(),
            }],
            source_bundle,
        }
    }

    fn token_book(
        token_id: TokenId,
        bids: Vec<BookLevel>,
        asks: Vec<BookLevel>,
        event_time: chrono::DateTime<Utc>,
    ) -> ReconstructedTokenBook {
        ReconstructedTokenBook {
            token_id,
            bids,
            asks,
            event_time,
            book_version: 1,
            source_event_count: 1,
            invalid_level_count: 0,
            crossed: false,
            max_gap_ms: 0,
            stale_interval_ms: 0,
        }
    }

    fn detection_row() -> OpportunityDetectionRow {
        OpportunityDetectionRow {
            opportunity_id: OpportunityId::new("opp"),
            market_id: MarketId::new("market"),
            event_id: EventId::new("event"),
            token_id: TokenId::new("yes"),
            token_yes: Some(TokenId::new("yes")),
            token_no: Some(TokenId::new("no")),
            side: ChSide::Buy,
            entry_price: ChPrice::from(Price::new(dec!(0.94))),
            edge_bps: ChBps::from(dec!(100)),
            expected_net_profit_usd: ChUsd::from(Usd::new(dec!(1))),
            net_profit_if_correct_usd: ChUsd::from(Usd::new(dec!(1))),
            shares: ChShares::from(Shares::new(dec!(100))),
            total_cost_usd: ChUsd::from(Usd::new(dec!(95))),
            total_fees_usd: ChUsd::from(Usd::ZERO),
            resolution_prob: ChProbability::from(dec!(0.95)),
            confidence: ChProbability::from(dec!(0.95)),
            fill_probability: Some(ChProbability::from(dec!(0.95))),
            score: Some(0),
            urgency_factor: Some(ChFactor::from(dec!(1))),
            category_weight: Some(ChFactor::from(dec!(1))),
            staleness_discount: Some(ChFactor::from(dec!(1))),
            depth_used_pct: ChFactor::from(dec!(0.25)),
            convergence_secs: 2,
            category: ChMarketCategory::Politics,
            price_zone: ChPriceZone::Z95,
            duration_bucket: ChDurationBucket::Short,
            calibration_sample_size: 1_000,
            calibration_fallback_tier: 1,
            calibration_alpha: ChDecimal64::from(dec!(2)),
            calibration_beta: ChDecimal64::from(dec!(0.001)),
            calibration_posterior_mean: ChProbability::from(dec!(0.995)),
            calibration_snapshot_hash: Some("calibration".to_owned()),
            book_age_ms: Some(0),
            yes_book_version: Some(1),
            no_book_version: Some(1),
            control_publication_id: None,
            score_components_json: "{}".to_owned(),
            calibration_snapshot_json: "{}".to_owned(),
            book_context_json: None,
            applied_factors_json: None,
            applied_factor_ids_json: None,
            latency_trace_json: None,
            missing_fields_json: None,
            detected_at: 1_000,
            ingestion_time: 1_000,
            sequence: 1,
            schema_version: ChSchemaVersion(2),
        }
    }

    fn runtime_config() -> RuntimeConfigVersionInfo {
        let endgame = EndgameDetectionConfig {
            high_threshold: dec!(0.94),
            min_convergence_duration_secs: 1,
            ..Default::default()
        };
        RuntimeConfigVersionInfo {
            runtime_config_version_id: RuntimeConfigVersionId::new("rcv"),
            config_hash: "cfg".to_owned(),
            schema_version: 1,
            config_json: serde_json::to_value(RuntimeConfigDocument {
                schema_version: 1,
                operator: OperatorRuntimeConfig {
                    maintenance_mode: false,
                    dry_run_mode: true,
                },
                detection: DetectionRuntimeConfig {
                    min_profit_threshold_usd: dec!(0),
                    endgame_hours_before_close: 24,
                    convergence_threshold: dec!(0.94),
                    endgame: Some(endgame),
                    calibration: Some(CalibrationConfig::default()),
                },
                execution: ExecutionRuntimeConfig {
                    max_slippage_bps: 0,
                    order_timeout_secs: 1,
                    cooldown_after_trade_secs: 0,
                },
                sizing: SizingRuntimeConfig {
                    kelly_fraction: dec!(1),
                    max_position_fraction_of_book: dec!(1),
                },
                risk_limits: RiskLimitRuntimeConfig {
                    max_portfolio_exposure_usd: Usd::new(dec!(1_000)),
                    max_single_position_usd: Usd::new(dec!(1_000)),
                    max_daily_loss_usd: Usd::new(dec!(1_000)),
                    circuit_breaker_threshold: 10,
                },
            })
            .expect("runtime config"),
            source: RuntimeConfigVersionSource::Operator,
            created_by: "test".to_owned(),
            reason: "test".to_owned(),
            created_at: Utc.timestamp_millis_opt(0).single().expect("created"),
        }
    }

    fn calibration_snapshot() -> CalibrationSnapshotRow {
        CalibrationSnapshotRow {
            category: ChMarketCategory::Politics,
            price_zone: ChPriceZone::Z95,
            duration_bucket: ChDurationBucket::Short,
            total_count: 1_000,
            correct_count: 1_000,
            alpha_prior: ChDecimal64::from(dec!(2)),
            beta_prior: ChDecimal64::from(dec!(0.001)),
            posterior_mean: Some(ChProbability::from(dec!(0.995))),
            fallback_tier: 1,
            config_hash: "cfg".to_owned(),
            snapshot_hash: "calibration".to_owned(),
            event_time: 0,
            ingestion_time: 0,
            sequence: 1,
            source: ChFactSource::CalibrationUpdater,
            schema_version: ChSchemaVersion(1),
        }
    }

    fn level(price: rust_decimal::Decimal, size: rust_decimal::Decimal) -> BookLevel {
        BookLevel::try_from_decimal(Price::new(price), Shares::new(size)).expect("valid level")
    }
}

//! Factor-aware opportunity pipeline: market-anomaly pre-gate blocks before
//! detection, and bucket-risk haircut tightens the recomputed expected profit.

use chrono::{Duration, Utc};
use oxide_arb_algorithm::{
    calibration::ResolutionCalibrator,
    cooldown::InMemoryEmissionCooldown,
    endgame::EndgameDetector,
    fee::FeeEstimator,
    pipeline::{MarketScanInputRef, OpportunityPipeline},
    scorer::EndgameScorer,
};
use oxide_arb_models::{
    domain::{
        book::{BookLevel, BookSnapshot, EndgameBookPair},
        control_factor::{
            AnomalyType, BucketRiskDimensions, BucketRiskPayload, ConfidenceInterval,
            ControlFactorProvider, ControlFactorPublication, ControlFactorSnapshot,
            ControlFactorValue, DataCoverageReport, FactorDimensions, FactorEvidence,
            FactorPayload, MarketAnomalyDimensions, MarketAnomalyPayload, PointInTimeInputManifest,
            TailRiskEvidence,
        },
        latency::LatencyTrace,
    },
    enums::{
        common::{MarketCategory, StalenessLevel},
        control_factor::{
            FactorMaturity, FactorSeverity, FactorStatus, PublicationMode, PublicationStatus,
        },
    },
    runtime_config::{
        CalibrationConfig, DetectionConfig, EmissionCooldownConfig, EndgameDetectionConfig,
        FillProbabilityConfig, ScorerConfig,
    },
    types::{
        ControlFactorId, EventId, FactorPublicationId, MarketId, MaterializationRunId, Price,
        Shares, StageReportId, TokenId, Usd,
    },
};
use rust_decimal::Decimal;
use rust_decimal_macros::dec;
use std::sync::Arc;

struct ZeroFeeEstimator;

impl FeeEstimator for ZeroFeeEstimator {
    fn estimate_fee(
        &self,
        _shares: Shares,
        _price: Price,
        _category: MarketCategory,
        _token_id: &TokenId,
    ) -> Usd {
        Usd::ZERO
    }
}

/// Static snapshot provider for tests.
struct StubProvider(Arc<ControlFactorSnapshot>);

impl ControlFactorProvider for StubProvider {
    fn snapshot(&self) -> Arc<ControlFactorSnapshot> {
        Arc::clone(&self.0)
    }
}

fn level(price: Decimal, size: Decimal) -> BookLevel {
    BookLevel::from_decimal_unchecked(Price::new(price), Shares::new(size))
}

fn book() -> EndgameBookPair {
    EndgameBookPair {
        yes: Arc::new(BookSnapshot::new(
            Arc::from([]),
            Arc::from([level(dec!(0.97), dec!(1000))]),
            0,
            0,
        )),
        no: Arc::new(BookSnapshot::new(
            Arc::from([level(dec!(0.02), dec!(1000))]),
            Arc::from([level(dec!(0.03), dec!(1000))]),
            0,
            0,
        )),
    }
}

fn pipeline(factors: Arc<dyn ControlFactorProvider>) -> OpportunityPipeline<ZeroFeeEstimator> {
    let detection_config = DetectionConfig {
        min_profit_threshold_usd: Decimal::ZERO,
        endgame: EndgameDetectionConfig {
            min_convergence_duration_secs: 0,
            scorer: ScorerConfig {
                min_score: Decimal::ZERO,
                max_depth_usage_pct: dec!(100),
                ..Default::default()
            },
            ..Default::default()
        },
        calibration: CalibrationConfig::default(),
    };
    let calibrator = Arc::new(ResolutionCalibrator::empty(
        detection_config.calibration.clone(),
    ));
    let detector = EndgameDetector::new(
        &detection_config.endgame,
        &detection_config.calibration,
        calibrator,
        ZeroFeeEstimator,
    );
    let scorer = EndgameScorer::new(
        &detection_config.endgame.scorer,
        &FillProbabilityConfig::default(),
        48,
    );
    let cooldown = InMemoryEmissionCooldown::new(&EmissionCooldownConfig::default());
    OpportunityPipeline::new(detector, scorer, cooldown, factors, &detection_config)
}

fn scan_input<'a>(
    market_id: &'a MarketId,
    event_id: &'a EventId,
    token_yes: &'a TokenId,
    token_no: &'a TokenId,
    book: &'a EndgameBookPair,
    deadline: chrono::DateTime<Utc>,
) -> MarketScanInputRef<'a> {
    MarketScanInputRef {
        market_id,
        event_id,
        token_yes,
        token_no,
        book,
        category: MarketCategory::Geopolitics,
        staleness: StalenessLevel::Fresh,
        settlement_deadline: Some(deadline),
        latency: Arc::new(LatencyTrace::default()),
    }
}

fn evidence() -> FactorEvidence {
    let now = Utc::now();
    FactorEvidence {
        materialization_run_id: MaterializationRunId::from_v7(),
        stage_report_ids: vec![StageReportId::from_v7()],
        window_from: now - Duration::hours(1),
        window_to: now,
        source_delay_secs: 60,
        market_count: 1,
        event_count: 1,
        opportunity_count: 1,
        settlement_count: 1,
        sample_count: 1,
        data_coverage: DataCoverageReport {
            expected_rows: 1,
            observed_rows: 1,
            missing_rows: 0,
            coverage_ratio: Decimal::ONE,
            insufficient_reasons: Vec::new(),
        },
        point_in_time_inputs: PointInTimeInputManifest {
            inputs: Vec::new(),
            production_eligible: true,
            missing_inputs: Vec::new(),
            fatal_errors: Vec::new(),
            warnings: Vec::new(),
            manifest_hash: "pit".into(),
        },
        baseline_config_hash: "cfg".into(),
        code_git_sha: "sha".into(),
        dataset_hash: "ds".into(),
        feature_schema_hash: "fs".into(),
        label_schema_hash: "ls".into(),
        query_fingerprint: "fp".into(),
        confidence_interval: ConfidenceInterval {
            lower: dec!(0),
            point_estimate: dec!(0),
            upper: dec!(0),
            confidence_level: dec!(0.95),
        },
        tail_risk: TailRiskEvidence {
            p95_loss: dec!(0),
            p99_loss: dec!(0),
            max_loss: dec!(0),
            expected_shortfall: dec!(0),
        },
        maturity: FactorMaturity::StatisticallyMaterialized,
        source_refs: Vec::new(),
        warnings: Vec::new(),
    }
}

fn factor(dimensions: FactorDimensions, payload: FactorPayload) -> ControlFactorValue {
    ControlFactorValue {
        factor_id: ControlFactorId::from_v7(),
        factor_type: payload.factor_type(),
        dimensions,
        payload,
        evidence: evidence(),
        status: FactorStatus::Published,
        generated_at: Utc::now() - Duration::hours(2),
        expires_at: Utc::now() + Duration::days(1),
        owner: "test".into(),
        schema_version: 1,
    }
}

fn snapshot(factors: &[ControlFactorValue]) -> Arc<ControlFactorSnapshot> {
    let publication = ControlFactorPublication {
        publication_id: FactorPublicationId::from_v7(),
        mode: PublicationMode::Published,
        factor_ids: factors.iter().map(|f| f.factor_id.clone()).collect(),
        previous_publication_id: None,
        status: PublicationStatus::Active,
        effective_from: Utc::now() - Duration::hours(1),
        expires_at: Utc::now() + Duration::days(1),
        approved_by: Some("op".into()),
        approval_reason: "test".into(),
        publication_hash: "v1".into(),
    };
    Arc::new(
        ControlFactorSnapshot::compile(&publication, factors, Utc::now(), false)
            .expect("compile snapshot"),
    )
}

fn neutral_provider() -> Arc<dyn ControlFactorProvider> {
    Arc::new(StubProvider(Arc::new(ControlFactorSnapshot::neutral(
        Utc::now(),
    ))))
}

#[test]
fn neutral_pipeline_emits_opportunity() {
    let market = MarketId::new("m1");
    let event = EventId::new("e1");
    let yes = TokenId::new("yes-1");
    let no = TokenId::new("no-1");
    let book = book();
    let now = Utc::now();
    let pipe = pipeline(neutral_provider());
    let outcome = pipe.process_ref(
        &scan_input(&market, &event, &yes, &no, &book, now + Duration::hours(12)),
        now,
    );
    assert!(outcome.is_emitted(), "neutral pipeline should emit");
    assert!(outcome.opportunity.expect("emit").applied_factors.is_empty());
}

#[test]
fn reload_tightened_min_profit_suppresses_next_scan() {
    let event = EventId::new("e1");
    let book = book();
    let now = Utc::now();
    let pipe = pipeline(neutral_provider());

    let m1 = MarketId::new("m1");
    let (yes1, no1) = (TokenId::new("yes-1"), TokenId::new("no-1"));
    assert!(
        pipe.process_ref(
            &scan_input(&m1, &event, &yes1, &no1, &book, now + Duration::hours(12)),
            now,
        )
        .is_emitted(),
        "baseline config emits"
    );

    // Hot-reload with an unreachable profit floor: the very next scan (on a
    // fresh market, so no cooldown interference) must be suppressed.
    let mut tightened = DetectionConfig {
        min_profit_threshold_usd: dec!(1000000),
        ..DetectionConfig::default()
    };
    tightened.endgame.min_convergence_duration_secs = 0;
    tightened.endgame.scorer.min_score = Decimal::ZERO;
    tightened.endgame.scorer.max_depth_usage_pct = dec!(100);
    pipe.reload(&tightened);

    let m2 = MarketId::new("m2");
    let (yes2, no2) = (TokenId::new("yes-2"), TokenId::new("no-2"));
    let tightened_outcome = pipe.process_ref(
        &scan_input(&m2, &event, &yes2, &no2, &book, now + Duration::hours(12)),
        now,
    );
    assert!(
        !tightened_outcome.is_emitted(),
        "tightened min profit must suppress emission immediately after reload"
    );
    assert_eq!(
        tightened_outcome.reject,
        Some(oxide_arb_algorithm::DetectionRejectReason::MinProfitThreshold)
    );

    // Reloading the permissive config restores emission (third fresh market).
    let mut permissive = DetectionConfig {
        min_profit_threshold_usd: Decimal::ZERO,
        ..DetectionConfig::default()
    };
    permissive.endgame.min_convergence_duration_secs = 0;
    permissive.endgame.scorer.min_score = Decimal::ZERO;
    permissive.endgame.scorer.max_depth_usage_pct = dec!(100);
    pipe.reload(&permissive);

    let m3 = MarketId::new("m3");
    let (yes3, no3) = (TokenId::new("yes-3"), TokenId::new("no-3"));
    assert!(
        pipe.process_ref(
            &scan_input(&m3, &event, &yes3, &no3, &book, now + Duration::hours(12)),
            now,
        )
        .is_emitted(),
        "permissive reload restores emission"
    );
}

#[test]
fn market_anomaly_block_skips_before_detection() {
    let market = MarketId::new("m1");
    let event = EventId::new("e1");
    let yes = TokenId::new("yes-1");
    let no = TokenId::new("no-1");
    let book = book();
    let now = Utc::now();

    let anomaly = factor(
        FactorDimensions::MarketAnomaly(MarketAnomalyDimensions {
            market_id: Some(market.clone()),
            event_id: None,
            category: None,
            anomaly_type: AnomalyType::OracleMismatch,
            severity: FactorSeverity::Critical,
        }),
        FactorPayload::MarketAnomaly(MarketAnomalyPayload {
            severity: FactorSeverity::Critical,
            block_market: true,
            block_event: false,
            category_cooldown_secs: None,
            reason_code: "oracle".into(),
            manual_ack_required: true,
        }),
    );
    let provider: Arc<dyn ControlFactorProvider> = Arc::new(StubProvider(snapshot(&[anomaly])));
    let pipe = pipeline(provider);
    let outcome = pipe.process_ref(
        &scan_input(&market, &event, &yes, &no, &book, now + Duration::hours(12)),
        now,
    );
    assert!(!outcome.is_emitted(), "blocked market must not emit");
    assert_eq!(
        outcome.reject,
        Some(oxide_arb_algorithm::DetectionRejectReason::MarketAnomaly)
    );
}

/// Detect the bucket dimensions of the baseline (neutral) opportunity.
fn baseline_bucket_dims(
    market: &MarketId,
    event: &EventId,
    yes: &TokenId,
    no: &TokenId,
    book: &EndgameBookPair,
    now: chrono::DateTime<Utc>,
    deadline: chrono::DateTime<Utc>,
) -> BucketRiskDimensions {
    let baseline = pipeline(neutral_provider())
        .process_ref(&scan_input(market, event, yes, no, book, deadline), now)
        .opportunity
        .expect("baseline emit");
    BucketRiskDimensions::coarse(
        baseline.opportunity.category,
        baseline.opportunity.meta.price_zone,
        baseline.opportunity.meta.duration_bucket,
    )
}

#[test]
fn bucket_haircut_re_gates_min_profit() {
    let market = MarketId::new("m1");
    let event = EventId::new("e1");
    let yes = TokenId::new("yes-1");
    let no = TokenId::new("no-1");
    let book = book();
    let now = Utc::now();
    let deadline = now + Duration::hours(12);

    let dims = baseline_bucket_dims(&market, &event, &yes, &no, &book, now, deadline);

    // A hard haircut pushes the thin endgame margin below min profit: rejected.
    let bucket = factor(
        FactorDimensions::BucketRisk(dims),
        FactorPayload::BucketRisk(BucketRiskPayload {
            resolution_haircut_factor: dec!(0.5),
            size_multiplier: dec!(1),
            min_edge_bps_addon: dec!(0),
            block_new_entries: false,
        }),
    );
    let provider: Arc<dyn ControlFactorProvider> = Arc::new(StubProvider(snapshot(&[bucket])));
    let outcome = pipeline(provider).process_ref(
        &scan_input(&market, &event, &yes, &no, &book, deadline),
        now,
    );
    assert!(
        !outcome.is_emitted(),
        "bucket haircut must re-gate min profit and reject"
    );
}

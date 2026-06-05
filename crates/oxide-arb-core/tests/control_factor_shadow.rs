//! Shadow evaluator: the Shadow publication's would-reject delta is recorded
//! relative to the live published baseline, without affecting any real order.

use chrono::{Duration, Utc};
use oxide_arb_core::control::factor_shadow::ShadowEvaluator;
use oxide_arb_models::{
    domain::control_factor::{
        BookAgeBucket, BucketRiskDimensions, BucketRiskPayload, ConfidenceInterval,
        ControlFactorPublication, ControlFactorSnapshot, ControlFactorValue, DataCoverageReport,
        DepthBucket, ExecutionQualityDimensions, FactorDimensions, FactorEvidence, FactorPayload,
        LatencyBucket, PointInTimeInputManifest, SpreadBucket, TailRiskEvidence,
    },
    enums::{
        calibration::PriceZone,
        common::{MarketCategory, StalenessLevel},
        control_factor::{
            ControlFactorType, FactorMaturity, FactorStatus, PublicationMode, PublicationStatus,
        },
        fact::ShadowDecisionType,
    },
    types::{ControlFactorId, FactorPublicationId, MaterializationRunId, StageReportId, Usd},
};
use oxide_arb_test_support::fixtures::sample_opportunity;
use rust_decimal::Decimal;
use rust_decimal_macros::dec;

fn evidence() -> FactorEvidence {
    let now = Utc::now();
    FactorEvidence {
        materialization_run_id: MaterializationRunId::new_v7(),
        stage_report_ids: vec![StageReportId::new_v7()],
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

fn bucket_block_factor(dims: BucketRiskDimensions) -> ControlFactorValue {
    ControlFactorValue {
        factor_id: ControlFactorId::new_v7(),
        factor_type: ControlFactorType::BucketRisk,
        dimensions: FactorDimensions::BucketRisk(dims),
        payload: FactorPayload::BucketRisk(BucketRiskPayload {
            resolution_haircut_factor: dec!(1),
            size_multiplier: dec!(1),
            min_edge_bps_addon: dec!(0),
            block_new_entries: true,
        }),
        evidence: evidence(),
        status: FactorStatus::Shadow,
        generated_at: Utc::now() - Duration::hours(2),
        expires_at: Utc::now() + Duration::days(1),
        owner: "test".into(),
        schema_version: 1,
    }
}

fn shadow_publication(factors: &[ControlFactorValue]) -> ControlFactorPublication {
    ControlFactorPublication {
        publication_id: FactorPublicationId::new_v7(),
        mode: PublicationMode::Shadow,
        factor_ids: factors.iter().map(|f| f.factor_id.clone()).collect(),
        previous_publication_id: None,
        status: PublicationStatus::Active,
        effective_from: Utc::now() - Duration::hours(1),
        expires_at: Utc::now() + Duration::days(1),
        approved_by: Some("op".into()),
        approval_reason: "shadow".into(),
        publication_hash: "shadow-v1".into(),
    }
}

const fn eq_dims() -> ExecutionQualityDimensions {
    ExecutionQualityDimensions {
        category: MarketCategory::Politics,
        price_zone: PriceZone::Z97,
        spread_bucket: SpreadBucket::Tight,
        depth_bucket: DepthBucket::Deep,
        book_age_bucket: BookAgeBucket::Fresh,
        latency_bucket: LatencyBucket::Unknown,
        staleness_level: StalenessLevel::Fresh,
    }
}

#[test]
fn shadow_records_would_reject_delta_without_touching_orders() {
    let opp = sample_opportunity();
    let bucket_dims =
        BucketRiskDimensions::coarse(opp.category, opp.meta.price_zone, opp.meta.duration_bucket);

    // Published baseline is neutral; shadow blocks this bucket.
    let published = ControlFactorSnapshot::neutral(Utc::now());
    let shadow_factor = bucket_block_factor(bucket_dims.clone());
    let factors = vec![shadow_factor];
    let publication = shadow_publication(&factors);
    let shadow = ControlFactorSnapshot::compile(&publication, &factors, Utc::now(), false)
        .expect("compile shadow");

    let decision = ShadowEvaluator::evaluate(
        &published,
        &shadow,
        &opp,
        &bucket_dims,
        &eq_dims(),
        Usd::new(dec!(100)),
    )
    .expect("shadow decision");

    assert_eq!(decision.decision_type, ShadowDecisionType::WouldReject);
    assert_eq!(decision.publication_id, publication.publication_id);
}

#[test]
fn shadow_evaluate_is_none_without_active_shadow_publication() {
    let opp = sample_opportunity();
    let bucket_dims =
        BucketRiskDimensions::coarse(opp.category, opp.meta.price_zone, opp.meta.duration_bucket);
    let published = ControlFactorSnapshot::neutral(Utc::now());
    let shadow = ControlFactorSnapshot::neutral(Utc::now());
    assert!(
        ShadowEvaluator::evaluate(
            &published,
            &shadow,
            &opp,
            &bucket_dims,
            &eq_dims(),
            Usd::new(dec!(100)),
        )
        .is_none()
    );
}

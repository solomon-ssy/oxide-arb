//! Control-factor seed helpers for web governance integration tests.

use chrono::Utc;
use oxide_arb_models::{
    domain::{
        EnqueueMaterializationRunOptions, EnqueueMaterializationRunOutcome,
        NewControlFactorMaterializationRun,
        control_factor::{
            BucketRiskDimensions, BucketRiskPayload, ConfidenceInterval, ControlFactorValue,
            DataCoverageReport, FactorDimensions, FactorEvidence, FactorPayload,
            NewControlFactorAuditEvent, NewControlFactorValue, PointInTimeInputManifest,
            TailRiskEvidence,
        },
    },
    enums::{
        calibration::{DurationBucket, PriceZone},
        common::MarketCategory,
        control_factor::{
            AuditResourceType, ControlAuditEventType, ControlFactorType, FactorMaturity,
            FactorStatus, MaterializationOutputPolicy, MaterializationRunKind,
            MaterializationRunStatus, RunTriggerType,
        },
    },
    types::{ControlFactorId, MaterializationRunId, StageReportId},
};
use oxide_arb_repository::{postgres::PgControlFactorRepository, traits::ControlFactorRepository};
use rust_decimal::Decimal;
use rust_decimal_macros::dec;

async fn seed_materialization_run(repo: &PgControlFactorRepository) -> MaterializationRunId {
    let now = Utc::now();
    let run = NewControlFactorMaterializationRun {
        materialization_run_id: MaterializationRunId::from_v7(),
        run_dedupe_key: None,
        run_kind: MaterializationRunKind::Scheduled,
        trigger_type: RunTriggerType::Scheduled,
        trigger_ref: Some("web-test".into()),
        status: MaterializationRunStatus::Queued,
        window_from: now - chrono::Duration::hours(1),
        window_to: now,
        source_delay_secs: 900,
        market_filter: serde_json::json!({ "market_ids": [] }),
        requested_factor_types: serde_json::json!(["bucket_risk"]),
        data_requirements: serde_json::json!({ "required_inputs": ["runtime_config"] }),
        runtime_config_ref: serde_json::json!({ "mode": "active_at", "at": now }),
        simulation_config_hash: "blake3:sim".into(),
        quality_gate_policy_hash: "blake3:gate".into(),
        output_policy: MaterializationOutputPolicy::NoFactorOutput,
        manifest: serde_json::json!({ "run": "web-test" }),
        manifest_hash: "blake3:manifest".into(),
        report: serde_json::json!({}),
        code_git_sha: "abc".into(),
        created_by: "web-test".into(),
        started_at: None,
        finished_at: None,
        failure_code: None,
        failure_detail: None,
        report_uri: None,
    };
    match repo
        .enqueue_materialization_run(
            run,
            EnqueueMaterializationRunOptions {
                force_new_run: false,
                reason: None,
            },
        )
        .await
        .expect("enqueue materialization run")
    {
        EnqueueMaterializationRunOutcome::Created(created) => created.materialization_run_id,
        other => panic!("expected created run, got {other:?}"),
    }
}

fn candidate_factor(run_id: &MaterializationRunId) -> ControlFactorValue {
    let now = Utc::now();
    ControlFactorValue {
        factor_id: ControlFactorId::from_v7(),
        factor_type: ControlFactorType::BucketRisk,
        dimensions: FactorDimensions::BucketRisk(BucketRiskDimensions {
            category: MarketCategory::Politics,
            price_zone: PriceZone::Z99,
            duration_bucket: DurationBucket::Short,
            hours_to_settlement_bucket: None,
            neg_risk: Some(false),
            fee_profile: None,
        }),
        payload: FactorPayload::BucketRisk(BucketRiskPayload {
            resolution_haircut_factor: dec!(0.9),
            size_multiplier: dec!(0.5),
            min_edge_bps_addon: dec!(0),
            block_new_entries: false,
        }),
        evidence: FactorEvidence {
            materialization_run_id: run_id.clone(),
            stage_report_ids: vec![StageReportId::from_v7()],
            window_from: now - chrono::Duration::hours(1),
            window_to: now,
            source_delay_secs: 30,
            market_count: 1,
            event_count: 1,
            opportunity_count: 1,
            settlement_count: 0,
            sample_count: 10,
            data_coverage: DataCoverageReport {
                expected_rows: 10,
                observed_rows: 10,
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
                manifest_hash: "blake3:pit".into(),
            },
            baseline_config_hash: "blake3:cfg".into(),
            code_git_sha: "abc".into(),
            dataset_hash: "blake3:dataset".into(),
            feature_schema_hash: "blake3:features".into(),
            label_schema_hash: "blake3:labels".into(),
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
        },
        status: FactorStatus::Candidate,
        generated_at: now,
        expires_at: now + chrono::Duration::days(1),
        owner: "web-test".into(),
        schema_version: 1,
    }
}

fn factor_created_audit(factor_id: &ControlFactorId) -> NewControlFactorAuditEvent {
    NewControlFactorAuditEvent {
        event_type: ControlAuditEventType::FactorCreated,
        actor: "web-test".into(),
        actor_role: "operator".into(),
        resource_type: AuditResourceType::Factor,
        resource_id: factor_id.to_string(),
        request_id: "seed-factor".into(),
        reason: "web governance integration test".into(),
        before_hash: None,
        after_hash: None,
        diff: serde_json::json!({}),
    }
}

/// Insert a `Candidate` control factor and return its id.
pub async fn seed_candidate_factor(repo: &PgControlFactorRepository) -> ControlFactorId {
    let run_id = seed_materialization_run(repo).await;
    let factor = candidate_factor(&run_id);
    let factor_id = factor.factor_id.clone();
    repo.create_factor(
        NewControlFactorValue::from_typed(&factor, None).expect("typed factor"),
        factor_created_audit(&factor_id),
    )
    .await
    .expect("create candidate factor");
    factor_id
}

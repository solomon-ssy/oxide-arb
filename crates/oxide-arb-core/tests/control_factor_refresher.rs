//! Live control-factor refresher: hash verification, version no-op, and stale
//! snapshot retention when refresh fails.

use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use chrono::{Duration, Utc};
use oxide_arb_control::governance::PublicationHasher;
use oxide_arb_core::{
    control::{
        factor_refresher::{FactorRefreshConfig, FactorRefresher},
        factor_snapshot::FactorSnapshotStore,
    },
    observability::metrics_hub::MetricsHub,
};
use oxide_arb_error::{OxideError, control::SnapshotBuildError, storage::StorageError};
use oxide_arb_models::{
    domain::control_factor::{
        AcquireMaterializationRunOutcome, AuditActor, AuditedOutcome, BucketRiskDimensions,
        BucketRiskPayload, CancelMaterializationRunOutcome, ConfidenceInterval,
        ControlFactorAuditEventInfo, ControlFactorMaterializationRunInfo, ControlFactorPublication,
        ControlFactorPublicationInfo, ControlFactorStageReportInfo, ControlFactorValue,
        ControlFactorValueInfo, DataCoverageReport, EnqueueMaterializationRunOptions,
        EnqueueMaterializationRunOutcome, ExpireFactorsOutcome, FactorDimensions, FactorEvidence,
        FactorPayload, LIVE_SNAPSHOT_SCHEMA_VERSION, MaterializationRunStatusPatch,
        NewControlFactorAuditEvent, NewControlFactorMaterializationRun,
        NewControlFactorPublication, NewControlFactorStageReport, NewControlFactorValue,
        PointInTimeInputManifest, PublishPublicationOutcome, RunTransitionOutcome,
        TailRiskEvidence,
    },
    enums::{
        calibration::{DurationBucket, PriceZone},
        common::MarketCategory,
        control_factor::{
            ControlFactorType, FactorMaturity, FactorStatus, MaterializationRunStatus,
            MaterializationStageName, PublicationMode, PublicationStatus,
        },
    },
    types::{ControlFactorId, FactorPublicationId, MaterializationRunId, StageReportId},
};
use oxide_arb_repository::traits::ControlFactorRepository;
use rust_decimal::Decimal;
use rust_decimal_macros::dec;

struct MockRefresherRepo {
    published: Mutex<Option<ControlFactorPublicationInfo>>,
    factors: Mutex<Vec<ControlFactorValueInfo>>,
    refresh_calls: Mutex<u32>,
}

impl MockRefresherRepo {
    const fn with_publication(
        publication: ControlFactorPublicationInfo,
        factors: Vec<ControlFactorValueInfo>,
    ) -> Self {
        Self {
            published: Mutex::new(Some(publication)),
            factors: Mutex::new(factors),
            refresh_calls: Mutex::new(0),
        }
    }

    fn refresh_calls(&self) -> u32 {
        *self.refresh_calls.lock().unwrap()
    }
}

#[async_trait]
impl ControlFactorRepository for MockRefresherRepo {
    async fn enqueue_materialization_run(
        &self,
        _run: NewControlFactorMaterializationRun,
        _options: EnqueueMaterializationRunOptions,
    ) -> Result<EnqueueMaterializationRunOutcome, StorageError> {
        Err(unexpected("enqueue_materialization_run"))
    }

    async fn load_materialization_run(
        &self,
        _run_id: &MaterializationRunId,
    ) -> Result<Option<ControlFactorMaterializationRunInfo>, StorageError> {
        Ok(None)
    }

    async fn find_materialization_run_by_dedupe_key(
        &self,
        _dedupe_key: &str,
    ) -> Result<Option<ControlFactorMaterializationRunInfo>, StorageError> {
        Ok(None)
    }

    async fn latest_run_for_schedule(
        &self,
        _schedule_id: &str,
        _statuses: &[MaterializationRunStatus],
    ) -> Result<Option<ControlFactorMaterializationRunInfo>, StorageError> {
        Ok(None)
    }

    async fn try_acquire_materialization_run(
        &self,
        _run_id: &MaterializationRunId,
        _started_at: chrono::DateTime<Utc>,
    ) -> Result<AcquireMaterializationRunOutcome, StorageError> {
        Err(unexpected("try_acquire_materialization_run"))
    }

    async fn retry_materialization_run(
        &self,
        _run_id: &MaterializationRunId,
    ) -> Result<RunTransitionOutcome, StorageError> {
        Err(unexpected("retry_materialization_run"))
    }

    async fn transition_materialization_run(
        &self,
        _run_id: &MaterializationRunId,
        _expected_from: MaterializationRunStatus,
        _target: MaterializationRunStatus,
        _patch: MaterializationRunStatusPatch,
    ) -> Result<RunTransitionOutcome, StorageError> {
        Err(unexpected("transition_materialization_run"))
    }

    async fn cancel_materialization_run(
        &self,
        _run_id: &MaterializationRunId,
        _reason: &str,
        _cancelled_at: chrono::DateTime<Utc>,
    ) -> Result<CancelMaterializationRunOutcome, StorageError> {
        Err(unexpected("cancel_materialization_run"))
    }

    async fn upsert_stage_report(
        &self,
        _report: NewControlFactorStageReport,
    ) -> Result<ControlFactorStageReportInfo, StorageError> {
        Err(unexpected("upsert_stage_report"))
    }

    async fn load_stage_report(
        &self,
        _run_id: &MaterializationRunId,
        _stage_name: MaterializationStageName,
    ) -> Result<Option<ControlFactorStageReportInfo>, StorageError> {
        Ok(None)
    }

    async fn list_stage_reports(
        &self,
        _run_id: &MaterializationRunId,
    ) -> Result<Vec<ControlFactorStageReportInfo>, StorageError> {
        Ok(Vec::new())
    }

    async fn create_factor(
        &self,
        _factor: NewControlFactorValue,
        _audit: NewControlFactorAuditEvent,
    ) -> Result<ControlFactorValueInfo, StorageError> {
        Err(unexpected("create_factor"))
    }

    async fn load_factor(
        &self,
        _factor_id: &ControlFactorId,
    ) -> Result<Option<ControlFactorValueInfo>, StorageError> {
        Ok(None)
    }

    async fn load_factors_by_ids(
        &self,
        factor_ids: &[ControlFactorId],
    ) -> Result<Vec<ControlFactorValueInfo>, StorageError> {
        *self.refresh_calls.lock().unwrap() += 1;
        let factors = self.factors.lock().unwrap();
        Ok(factors
            .iter()
            .filter(|factor| factor_ids.iter().any(|id| id == &factor.factor_id))
            .cloned()
            .collect())
    }

    async fn list_factors_by_run(
        &self,
        _run_id: &MaterializationRunId,
    ) -> Result<Vec<ControlFactorValueInfo>, StorageError> {
        Ok(Vec::new())
    }

    async fn list_factors_by_status(
        &self,
        _status: FactorStatus,
        _factor_type: Option<ControlFactorType>,
    ) -> Result<Vec<ControlFactorValueInfo>, StorageError> {
        Ok(Vec::new())
    }

    async fn reject_factor(
        &self,
        _factor_id: &ControlFactorId,
        _status_reason: &str,
        _audit: NewControlFactorAuditEvent,
    ) -> Result<Option<AuditedOutcome<ControlFactorValueInfo>>, StorageError> {
        Ok(None)
    }

    async fn expire_factors(
        &self,
        _now: chrono::DateTime<Utc>,
        _actor: AuditActor,
    ) -> Result<ExpireFactorsOutcome, StorageError> {
        Ok(ExpireFactorsOutcome::default())
    }

    async fn publish_publication(
        &self,
        _publication: NewControlFactorPublication,
        _audit: NewControlFactorAuditEvent,
    ) -> Result<PublishPublicationOutcome, StorageError> {
        Err(unexpected("publish_publication"))
    }

    async fn load_publication(
        &self,
        _publication_id: &FactorPublicationId,
    ) -> Result<Option<ControlFactorPublicationInfo>, StorageError> {
        Ok(None)
    }

    async fn load_active_publication(
        &self,
        mode: PublicationMode,
    ) -> Result<Option<ControlFactorPublicationInfo>, StorageError> {
        if mode == PublicationMode::Published {
            Ok(self.published.lock().unwrap().clone())
        } else {
            Ok(None)
        }
    }

    async fn list_publications(
        &self,
        _mode: PublicationMode,
        _status: Option<PublicationStatus>,
        _limit: u64,
    ) -> Result<Vec<ControlFactorPublicationInfo>, StorageError> {
        Ok(Vec::new())
    }

    async fn rollback_publication(
        &self,
        _active_publication_id: &FactorPublicationId,
        _target_publication_id: &FactorPublicationId,
        _audit: NewControlFactorAuditEvent,
    ) -> Result<AuditedOutcome<ControlFactorPublicationInfo>, StorageError> {
        Err(unexpected("rollback_publication"))
    }

    async fn append_audit_event(
        &self,
        _event: NewControlFactorAuditEvent,
    ) -> Result<ControlFactorAuditEventInfo, StorageError> {
        Err(unexpected("append_audit_event"))
    }

    async fn load_audit_chain(
        &self,
        _from_sequence: i64,
        _limit: u64,
    ) -> Result<Vec<ControlFactorAuditEventInfo>, StorageError> {
        Ok(Vec::new())
    }
}

fn unexpected(method: &str) -> StorageError {
    StorageError::Codec(format!("refresher mock must not call {method}"))
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

fn bucket_factor() -> ControlFactorValue {
    ControlFactorValue {
        factor_id: ControlFactorId::from_v7(),
        factor_type: ControlFactorType::BucketRisk,
        dimensions: FactorDimensions::BucketRisk(BucketRiskDimensions::coarse(
            MarketCategory::Politics,
            PriceZone::Z97,
            DurationBucket::Short,
        )),
        payload: FactorPayload::BucketRisk(BucketRiskPayload {
            resolution_haircut_factor: dec!(0.8),
            size_multiplier: dec!(0.5),
            min_edge_bps_addon: dec!(10),
            block_new_entries: false,
        }),
        evidence: evidence(),
        status: FactorStatus::Published,
        generated_at: Utc::now() - Duration::hours(2),
        expires_at: Utc::now() + Duration::days(1),
        owner: "test".into(),
        schema_version: LIVE_SNAPSHOT_SCHEMA_VERSION,
    }
}

fn factor_info(value: &ControlFactorValue) -> ControlFactorValueInfo {
    let row = NewControlFactorValue::from_typed(value, None).expect("factor row");
    let now = Utc::now();
    ControlFactorValueInfo {
        factor_id: row.factor_id,
        run_id: row.run_id,
        factor_type: row.factor_type,
        dimensions: row.dimensions,
        dimensions_hash: row.dimensions_hash,
        payload: row.payload,
        payload_hash: row.payload_hash,
        evidence: row.evidence,
        status: row.status,
        status_reason: row.status_reason,
        generated_at: row.generated_at,
        expires_at: row.expires_at,
        owner: row.owner,
        schema_version: row.schema_version,
        created_at: now,
        updated_at: now,
    }
}

fn sealed_publication(
    factors: &[ControlFactorValue],
    publication_hash: Option<&str>,
) -> (ControlFactorPublicationInfo, Vec<ControlFactorValueInfo>) {
    let factor_infos: Vec<ControlFactorValueInfo> = factors.iter().map(factor_info).collect();
    let mut publication = ControlFactorPublication {
        publication_id: FactorPublicationId::from_v7(),
        mode: PublicationMode::Published,
        factor_ids: factors.iter().map(|f| f.factor_id.clone()).collect(),
        previous_publication_id: None,
        status: PublicationStatus::Active,
        effective_from: Utc::now() - Duration::hours(1),
        expires_at: Utc::now() + Duration::days(1),
        approved_by: Some("op".into()),
        approval_reason: "test".into(),
        publication_hash: publication_hash.unwrap_or("").into(),
    };
    if publication_hash.is_none() {
        PublicationHasher::seal(&mut publication).expect("seal publication");
    }
    let now = Utc::now();
    let info = ControlFactorPublicationInfo {
        publication_id: publication.publication_id.clone(),
        mode: publication.mode,
        factor_ids: publication.factor_ids.clone(),
        previous_publication_id: publication.previous_publication_id.clone(),
        status: publication.status,
        effective_from: publication.effective_from,
        expires_at: publication.expires_at,
        approved_by: publication.approved_by.clone(),
        approval_reason: publication.approval_reason.clone(),
        publication_hash: publication.publication_hash.clone(),
        created_at: now,
        updated_at: now,
    };
    (info, factor_infos)
}

fn refresher(
    repo: Arc<MockRefresherRepo>,
    store: Arc<FactorSnapshotStore>,
    fail_closed: bool,
) -> FactorRefresher {
    FactorRefresher::new(
        repo,
        store,
        Arc::new(MetricsHub::new()),
        FactorRefreshConfig::for_live(fail_closed),
    )
}

#[tokio::test]
async fn startup_loads_valid_publication_with_hash_verification() {
    let factor = bucket_factor();
    let (publication, factor_infos) = sealed_publication(&[factor], None);
    let repo = Arc::new(MockRefresherRepo::with_publication(
        publication.clone(),
        factor_infos,
    ));
    let store = Arc::new(FactorSnapshotStore::new(Utc::now()));
    let refresher = refresher(Arc::clone(&repo), Arc::clone(&store), true);

    refresher.startup().await.expect("startup");

    let snapshot = store.published();
    assert_eq!(snapshot.publication_id, Some(publication.publication_id));
    assert_eq!(snapshot.bucket_risk.len(), 1);
}

#[tokio::test]
async fn startup_rejects_publication_hash_mismatch_when_fail_closed() {
    let factor = bucket_factor();
    let (publication, factor_infos) = sealed_publication(&[factor], Some("blake3:tampered"));
    let repo = Arc::new(MockRefresherRepo::with_publication(
        publication,
        factor_infos,
    ));
    let store = Arc::new(FactorSnapshotStore::new(Utc::now()));
    let refresher = refresher(Arc::clone(&repo), Arc::clone(&store), true);

    let error = refresher.startup().await.expect_err("must fail closed");
    assert!(matches!(
        error,
        OxideError::SnapshotBuild(SnapshotBuildError::PublicationHashMismatch { .. })
    ));
}

#[tokio::test]
async fn startup_rejects_payload_hash_mismatch_when_fail_closed() {
    let factor = bucket_factor();
    let (publication, mut factor_infos) = sealed_publication(&[factor], None);
    factor_infos[0].payload_hash = "blake3:tampered".into();
    let repo = Arc::new(MockRefresherRepo::with_publication(
        publication,
        factor_infos,
    ));
    let store = Arc::new(FactorSnapshotStore::new(Utc::now()));
    let refresher = refresher(Arc::clone(&repo), Arc::clone(&store), true);

    let error = refresher.startup().await.expect_err("must fail closed");
    assert!(matches!(
        error,
        OxideError::SnapshotBuild(SnapshotBuildError::PayloadHashMismatch { .. })
    ));
}

#[tokio::test]
async fn refresh_skips_unchanged_version_without_reloading_factors() {
    let factor = bucket_factor();
    let (publication, factor_infos) = sealed_publication(&[factor], None);
    let repo = Arc::new(MockRefresherRepo::with_publication(
        publication,
        factor_infos,
    ));
    let store = Arc::new(FactorSnapshotStore::new(Utc::now()));
    let refresher = refresher(Arc::clone(&repo), Arc::clone(&store), true);
    refresher.startup().await.expect("startup");
    let calls_after_startup = repo.refresh_calls();

    refresher.refresh_tick().await;

    assert_eq!(repo.refresh_calls(), calls_after_startup);
}

#[tokio::test]
async fn refresh_failed_keeps_prior_snapshot() {
    let factor = bucket_factor();
    let (publication, factor_infos) = sealed_publication(&[factor], None);
    let repo = Arc::new(MockRefresherRepo::with_publication(
        publication.clone(),
        factor_infos,
    ));
    let store = Arc::new(FactorSnapshotStore::new(Utc::now()));
    let refresher = refresher(Arc::clone(&repo), Arc::clone(&store), true);
    refresher.startup().await.expect("startup");
    let prior = store.published();

    repo.published
        .lock()
        .unwrap()
        .as_mut()
        .expect("active publication")
        .publication_hash = "blake3:broken-version".into();

    refresher.refresh_tick().await;

    let current = store.published();
    assert_eq!(current.publication_id, prior.publication_id);
    assert_eq!(current.bucket_risk.len(), prior.bucket_risk.len());
}

#[tokio::test]
async fn refresh_swaps_snapshot_on_version_change() {
    let first_factor = bucket_factor();
    let (first_publication, first_infos) = sealed_publication(&[first_factor], None);
    let repo = Arc::new(MockRefresherRepo::with_publication(
        first_publication.clone(),
        first_infos,
    ));
    let store = Arc::new(FactorSnapshotStore::new(Utc::now()));
    let refresher = refresher(Arc::clone(&repo), Arc::clone(&store), true);
    refresher.startup().await.expect("startup");

    let mut second_factor = bucket_factor();
    second_factor.payload = FactorPayload::BucketRisk(BucketRiskPayload {
        resolution_haircut_factor: dec!(0.5),
        size_multiplier: dec!(0.25),
        min_edge_bps_addon: dec!(20),
        block_new_entries: false,
    });
    let (second_publication, second_infos) = sealed_publication(&[second_factor], None);
    *repo.published.lock().unwrap() = Some(second_publication.clone());
    *repo.factors.lock().unwrap() = second_infos;

    refresher.refresh_tick().await;

    let snapshot = store.published();
    assert_eq!(
        snapshot.publication_id,
        Some(second_publication.publication_id)
    );
    assert_eq!(snapshot.source_version, second_publication.publication_hash);
}

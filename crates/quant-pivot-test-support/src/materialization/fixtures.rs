//! Shared materialization test fakes and fixtures.

use std::{collections::HashSet, sync::Arc};

use crate::{mocks::EXECUTION_QUALITY_HOURLY_SCHEDULE_ID, seeded_uuid};
use async_trait::async_trait;
use chrono::{DateTime, Duration, TimeZone, Utc};
use oxide_arb_error::storage::StorageError;
use oxide_arb_models::{
    domain::{
        MarketInfo, MarketPageQuery, MarketPitSnapshotInfo, NewRuntimeConfigActivation,
        NewRuntimeConfigVersion, Paginated, RuntimeConfigActivationInfo, RuntimeConfigVersionInfo,
        UpsertMarket,
        control_factor::{
            AuditedOutcome, ControlFactorMaterializationRunInfo, DataRequirements,
            MarketFilterSpec, MaterializationRunManifest, NewControlFactorAuditEvent,
            QualityGatePolicy, RequiredInputDomain, RunTrigger, RuntimeConfigRef, SimulationConfig,
            TimeWindowSpec,
        },
    },
    enums::{
        common::{MarketCategory, TickSize},
        control_factor::{
            ControlFactorType, MaterializationOutputPolicy, MaterializationRunKind,
            MaterializationRunStatus, RunTriggerType,
        },
        market::MarketStatus,
        runtime_config::RuntimeConfigVersionSource,
    },
    types::{
        EventId, MarketId, MarketPitSnapshotId, MaterializationRunId, RuntimeConfigVersionId,
        TokenId,
    },
};
use oxide_arb_repository::traits::{MarketRepository, RuntimeConfigVersionRepository};

#[derive(Clone, Default)]
pub struct FakeMarketRepository {
    pub snapshots: Vec<MarketPitSnapshotInfo>,
    pub current: Vec<MarketInfo>,
}

#[derive(Default)]
pub struct FakeRuntimeConfigRepository;

pub fn materialization_manifest(
    as_of: DateTime<Utc>,
    market_ids: Vec<MarketId>,
) -> MaterializationRunManifest {
    MaterializationRunManifest {
        run_id: MaterializationRunId::from_v7(),
        run_kind: MaterializationRunKind::Scheduled,
        trigger: RunTrigger::Scheduled {
            schedule_id: "test".to_owned(),
        },
        window: TimeWindowSpec::new(as_of - Duration::hours(1), as_of),
        source_delay_secs: 0,
        markets: MarketFilterSpec {
            market_ids,
            event_ids: Vec::new(),
            token_ids: Vec::new(),
            categories: Vec::new(),
        },
        replay_account_scope: None,
        requested_factor_types: vec![ControlFactorType::BucketRisk],
        data_requirements: DataRequirements {
            required_inputs: vec![RequiredInputDomain::TokenMapping],
            production_required_inputs: vec![RequiredInputDomain::TokenMapping],
            min_l2_coverage_ratio: None,
            require_settlement_truth: false,
        },
        runtime_config_ref: RuntimeConfigRef::Version {
            version_id: RuntimeConfigVersionId::new(seeded_uuid("rcv_test")),
            config_hash: "blake3:cfg".to_owned(),
        },
        simulation_config: SimulationConfig::production_default(),
        quality_gate_policy: QualityGatePolicy::default(),
        output_policy: MaterializationOutputPolicy::NoFactorOutput,
        code_git_sha: "abc".to_owned(),
        created_by: "test".to_owned(),
        created_at: as_of,
    }
}

pub fn market_snapshot(
    market_id: &MarketId,
    yes_token_id: &str,
    no_token_id: &str,
    observed_at: DateTime<Utc>,
) -> MarketPitSnapshotInfo {
    MarketPitSnapshotInfo {
        market_pit_snapshot_id: MarketPitSnapshotId::from_v7(),
        market_id: market_id.clone(),
        event_id: EventId::new("evt"),
        question: "question".to_owned(),
        slug: "slug".to_owned(),
        categories: vec![MarketCategory::Politics],
        status: MarketStatus::Active,
        outcome: None,
        yes_token_id: TokenId::new(yes_token_id),
        no_token_id: TokenId::new(no_token_id),
        tick_size: TickSize::Hundredth,
        neg_risk: false,
        end_date: None,
        resolved_at: None,
        fees_enabled: true,
        fee_rate: None,
        fee_exponent: None,
        fee_taker_only: None,
        fee_rebate_rate: None,
        fee_source: None,
        fee_observed_at: None,
        payload_hash: format!("hash:{yes_token_id}:{no_token_id}"),
        observed_at,
        created_at: observed_at,
    }
}

pub fn market_info(
    market_id: &MarketId,
    yes_token_id: &str,
    no_token_id: &str,
    updated_at: DateTime<Utc>,
) -> MarketInfo {
    MarketInfo {
        market_id: market_id.clone(),
        event_id: EventId::new("evt"),
        question: "question".to_owned(),
        slug: "slug".to_owned(),
        categories: vec![MarketCategory::Politics],
        status: MarketStatus::Active,
        outcome: None,
        yes_token_id: TokenId::new(yes_token_id),
        no_token_id: TokenId::new(no_token_id),
        tick_size: TickSize::Hundredth,
        neg_risk: false,
        end_date: None,
        resolved_at: None,
        fees_enabled: true,
        fee_rate: None,
        fee_exponent: None,
        fee_taker_only: None,
        fee_rebate_rate: None,
        fee_source: None,
        fee_observed_at: None,
        created_at: updated_at,
        updated_at,
    }
}

pub fn fixed_time(hour: u32) -> DateTime<Utc> {
    Utc.with_ymd_and_hms(2026, 6, 3, hour, 0, 0)
        .single()
        .expect("fixed timestamp")
}

/// Fixed wall clock for materialization scheduler tests (2026-06-05 12:00 UTC).
#[must_use]
pub fn scheduler_fixed_now() -> DateTime<Utc> {
    Utc.with_ymd_and_hms(2026, 6, 5, 12, 0, 0)
        .single()
        .expect("fixed timestamp")
}

/// Builds a scheduled materialization run row for scheduler mock seeding.
#[must_use]
pub fn scheduled_materialization_run_info(
    schedule_id: &str,
    status: MaterializationRunStatus,
    created_at: DateTime<Utc>,
    finished_at: Option<DateTime<Utc>>,
) -> ControlFactorMaterializationRunInfo {
    ControlFactorMaterializationRunInfo {
        materialization_run_id: MaterializationRunId::from_v7(),
        run_dedupe_key: None,
        run_kind: MaterializationRunKind::Scheduled,
        trigger_type: RunTriggerType::Scheduled,
        trigger_ref: Some(schedule_id.to_owned()),
        status,
        window_from: created_at - Duration::hours(1),
        window_to: created_at,
        source_delay_secs: 900,
        market_filter: serde_json::json!({}),
        requested_factor_types: serde_json::json!([]),
        data_requirements: serde_json::json!({}),
        runtime_config_ref: serde_json::json!({}),
        simulation_config_hash: "blake3:sim".into(),
        quality_gate_policy_hash: "blake3:gate".into(),
        output_policy: MaterializationOutputPolicy::EmitDraftCandidates,
        manifest: serde_json::json!({}),
        manifest_hash: "blake3:manifest".into(),
        report: serde_json::json!({}),
        code_git_sha: "abc".into(),
        created_by: "scheduler".into(),
        started_at: None,
        finished_at,
        failure_code: None,
        failure_detail: None,
        report_uri: None,
        created_at,
        updated_at: created_at,
    }
}

/// Scheduled run fixture using [`EXECUTION_QUALITY_HOURLY_SCHEDULE_ID`].
#[must_use]
pub fn execution_quality_hourly_run(
    status: MaterializationRunStatus,
    created_at: DateTime<Utc>,
    finished_at: Option<DateTime<Utc>>,
) -> ControlFactorMaterializationRunInfo {
    scheduled_materialization_run_info(
        EXECUTION_QUALITY_HOURLY_SCHEDULE_ID,
        status,
        created_at,
        finished_at,
    )
}

pub fn runtime_config_repo() -> Arc<dyn RuntimeConfigVersionRepository> {
    Arc::new(FakeRuntimeConfigRepository)
}

#[async_trait]
impl MarketRepository for FakeMarketRepository {
    async fn page(&self, query: MarketPageQuery) -> Result<Paginated<MarketInfo>, StorageError> {
        let window = query.page.normalized();
        let items: Vec<MarketInfo> = self
            .current
            .iter()
            .filter(|m| query.status.is_none_or(|s| m.status == s))
            .filter(|m| query.category.is_none_or(|c| m.categories.contains(&c)))
            .cloned()
            .collect();
        let total = items.len() as u64;
        let page = items
            .into_iter()
            .skip(usize::try_from(window.offset()).unwrap_or(usize::MAX))
            .take(usize::try_from(window.limit()).unwrap_or(usize::MAX))
            .collect();
        Ok(Paginated::from_request(page, total, &window))
    }

    async fn find_by_id(&self, id: &MarketId) -> Result<Option<Arc<MarketInfo>>, StorageError> {
        Ok(self
            .current
            .iter()
            .find(|market| &market.market_id == id)
            .cloned()
            .map(Arc::new))
    }

    async fn find_by_ids(&self, ids: &[MarketId]) -> Result<Vec<Arc<MarketInfo>>, StorageError> {
        Ok(self
            .current
            .iter()
            .filter(|market| ids.contains(&market.market_id))
            .cloned()
            .map(Arc::new)
            .collect())
    }

    async fn latest_pit_snapshots_before(
        &self,
        ids: &[MarketId],
        as_of: DateTime<Utc>,
    ) -> Result<Vec<MarketPitSnapshotInfo>, StorageError> {
        let mut rows = self
            .snapshots
            .iter()
            .filter(|snapshot| ids.contains(&snapshot.market_id) && snapshot.observed_at <= as_of)
            .cloned()
            .collect::<Vec<_>>();
        rows.sort_by(|left, right| {
            left.market_id
                .as_str()
                .cmp(right.market_id.as_str())
                .then_with(|| right.observed_at.cmp(&left.observed_at))
        });
        let mut seen = HashSet::new();
        Ok(rows
            .into_iter()
            .filter(|snapshot| seen.insert(snapshot.market_id.clone()))
            .collect())
    }

    async fn find_active(&self) -> Result<Arc<[MarketInfo]>, StorageError> {
        Ok(Arc::from([]))
    }

    async fn find_by_event(&self, _event_id: &str) -> Result<Vec<Arc<MarketInfo>>, StorageError> {
        Ok(Vec::new())
    }

    async fn find_endgame_candidates(
        &self,
        _before_deadline: DateTime<Utc>,
    ) -> Result<Vec<Arc<MarketInfo>>, StorageError> {
        Ok(Vec::new())
    }

    async fn find_existing_ids(&self, _ids: &[MarketId]) -> Result<HashSet<String>, StorageError> {
        Ok(HashSet::new())
    }

    async fn upsert(&self, _market: UpsertMarket) -> Result<Arc<MarketInfo>, StorageError> {
        Err(StorageError::Codec(
            "FakeMarketRepository::upsert is not implemented".to_owned(),
        ))
    }

    async fn upsert_batch(&self, _markets: Vec<UpsertMarket>) -> Result<u64, StorageError> {
        Ok(0)
    }

    async fn update_status(
        &self,
        _id: &MarketId,
        _status: &str,
        _outcome: Option<&str>,
    ) -> Result<(), StorageError> {
        Ok(())
    }
}

#[async_trait]
impl RuntimeConfigVersionRepository for FakeRuntimeConfigRepository {
    async fn create_version(
        &self,
        _version: NewRuntimeConfigVersion,
    ) -> Result<RuntimeConfigVersionInfo, StorageError> {
        Err(StorageError::Codec(
            "FakeRuntimeConfigRepository::create_version is not implemented".to_owned(),
        ))
    }

    async fn activate_version(
        &self,
        _activation: NewRuntimeConfigActivation,
    ) -> Result<RuntimeConfigActivationInfo, StorageError> {
        Err(StorageError::Codec(
            "FakeRuntimeConfigRepository::activate_version is not implemented".to_owned(),
        ))
    }

    async fn create_version_governed(
        &self,
        _version: NewRuntimeConfigVersion,
        _audit: NewControlFactorAuditEvent,
    ) -> Result<AuditedOutcome<RuntimeConfigVersionInfo>, StorageError> {
        Err(StorageError::Codec(
            "FakeRuntimeConfigRepository::create_version_governed is not implemented".to_owned(),
        ))
    }

    async fn activate_version_governed(
        &self,
        _activation: NewRuntimeConfigActivation,
        _audit: NewControlFactorAuditEvent,
    ) -> Result<AuditedOutcome<RuntimeConfigActivationInfo>, StorageError> {
        Err(StorageError::Codec(
            "FakeRuntimeConfigRepository::activate_version_governed is not implemented".to_owned(),
        ))
    }

    async fn load_current_activation(
        &self,
    ) -> Result<Option<RuntimeConfigActivationInfo>, StorageError> {
        Ok(None)
    }

    async fn load_version(
        &self,
        version_id: &RuntimeConfigVersionId,
    ) -> Result<Option<RuntimeConfigVersionInfo>, StorageError> {
        Ok(Some(runtime_config_version(version_id.clone())))
    }

    async fn load_by_hash(
        &self,
        _config_hash: &str,
    ) -> Result<Option<RuntimeConfigVersionInfo>, StorageError> {
        Ok(Some(runtime_config_version(RuntimeConfigVersionId::new(
            seeded_uuid("rcv_hash"),
        ))))
    }

    async fn load_current(&self) -> Result<Option<RuntimeConfigVersionInfo>, StorageError> {
        Ok(Some(runtime_config_version(RuntimeConfigVersionId::new(
            seeded_uuid("rcv_current"),
        ))))
    }

    async fn load_active_at(
        &self,
        _at: DateTime<Utc>,
    ) -> Result<Option<RuntimeConfigVersionInfo>, StorageError> {
        Ok(Some(runtime_config_version(RuntimeConfigVersionId::new(
            seeded_uuid("rcv_active"),
        ))))
    }

    async fn list_versions(
        &self,
        _limit: u64,
    ) -> Result<Vec<RuntimeConfigVersionInfo>, StorageError> {
        Ok(vec![runtime_config_version(RuntimeConfigVersionId::new(
            seeded_uuid("rcv_current"),
        ))])
    }

    async fn list_activations(
        &self,
        _limit: u64,
    ) -> Result<Vec<RuntimeConfigActivationInfo>, StorageError> {
        Ok(Vec::new())
    }
}

fn runtime_config_version(version_id: RuntimeConfigVersionId) -> RuntimeConfigVersionInfo {
    RuntimeConfigVersionInfo {
        runtime_config_version_id: version_id,
        config_hash: "blake3:cfg".to_owned(),
        schema_version: 1,
        config_json: serde_json::json!({}),
        source: RuntimeConfigVersionSource::Operator,
        created_by: "test".to_owned(),
        reason: "test".to_owned(),
        created_at: fixed_time(1),
    }
}

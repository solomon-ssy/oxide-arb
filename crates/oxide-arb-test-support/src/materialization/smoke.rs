//! End-to-end materialization smoke fixtures (synthetic CH/PG facts, no live infra).

use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use chrono::{DateTime, Duration, Utc};
use oxide_arb_error::storage::StorageError;
use oxide_arb_models::{
    clickhouse::{
        BookSnapshotRow, CalibrationSnapshotRow, ChBps, ChDecimal64, ChFactor, ChPrice,
        ChProbability, ChSchemaVersion, ChShares, ChUsd, OpportunityAuditRow,
        OpportunityDetectionRow, TickEventL2Row,
    },
    config::{CalibrationConfig, EndgameDetectionConfig},
    domain::{
        BalanceSnapshotInfo, ControlFactorStageReportInfo, DetectionRuntimeConfig,
        ExecutionRuntimeConfig, MarketPitSnapshotInfo, NewBalanceSnapshot, NewPotentialLoss,
        NewReconciliationReport, NewRiskAuditEvent, NewRuntimeConfigActivation,
        NewRuntimeConfigVersion, OperatorRuntimeConfig, PositionInfo, PotentialLossInfo,
        ReconciliationReportInfo, ResolvePotentialLoss, RiskAuditEventInfo, RiskLimitRuntimeConfig,
        RuntimeConfigActivationInfo, RuntimeConfigDocument, RuntimeConfigVersionInfo,
        SizingRuntimeConfig, TradeInfo,
        control_factor::{
            AcquireMaterializationRunOutcome, AuditActor, AuditEventContent,
            CancelMaterializationRunOutcome, ControlFactorAuditEventInfo,
            ControlFactorMaterializationRunInfo, ControlFactorPublicationInfo,
            ControlFactorValueInfo, DataRequirements, EnqueueMaterializationRunOptions,
            EnqueueMaterializationRunOutcome, ExpireFactorsOutcome, MarketFilterSpec,
            MaterializationRunManifest, MaterializationRunStatusPatch, NewControlFactorAuditEvent,
            NewControlFactorMaterializationRun, NewControlFactorPublication,
            NewControlFactorStageReport, NewControlFactorValue, PublishPublicationOutcome,
            QualityGatePolicy, ReplayAccountScope, RequiredInputDomain, RunTransitionOutcome,
            RunTrigger, RuntimeConfigRef, SimulationConfig, TimeWindowSpec,
        },
        settlement::{NewResolutionEvent, ResolutionEventInfo},
    },
    enums::{
        clickhouse::{
            ChAuditOutcome, ChBookEventType, ChDurationBucket, ChFactSource, ChMarketCategory,
            ChOpportunityAuditStage, ChPriceZone, ChSettlementAccountingStatus,
            ChSettlementOutcome, ChSettlementTrigger, ChSide, ChSnapshotReason,
        },
        common::{
            ExecutionMode, LedgerStatus, MarketCategory, PositionStatus, RedeemStatus,
            SettlementAccountingStatus, SettlementTrigger, Side, TradeState,
        },
        control_factor::{
            AuditResourceType, ControlAuditEventType, ControlFactorType, FactorStatus,
            MaterializationOutputPolicy, MaterializationRunKind, MaterializationRunStatus,
            MaterializationStageName, PublicationMode, PublicationStatus,
        },
        fact::BalanceSnapshotSource,
        risk::{ReconciliationStatus, RiskAuditEventType},
        runtime_config::RuntimeConfigVersionSource,
    },
    types::{
        AuditEventId, BalanceSnapshotId, ControlFactorId, EventId, ExecutionId,
        FactorPublicationId, LedgerId, MarketId, MaterializationRunId, OpportunityId, PositionId,
        Price, ReservationId, RuntimeConfigVersionId, Shares, TokenId, TradeId, Usd,
    },
};
use oxide_arb_repository::traits::{
    BalanceSnapshotRepository, ControlFactorRepository, PotentialLossRepository,
    ReconciliationRepository, ResolutionEventRepository, RiskAuditRepository,
    RuntimeConfigVersionRepository,
};
use rust_decimal_macros::dec;

use super::fixtures::{FakeMarketRepository, fixed_time, market_snapshot};
use crate::mocks::{MockPositionRepository, MockTimeseriesRepository, MockTradeRepository};

pub const SMOKE_MARKET_ID: &str = "0xsmoke_acceptance";
pub const SMOKE_YES_TOKEN: &str = "smoke_yes";
pub const SMOKE_NO_TOKEN: &str = "smoke_no";
pub const SMOKE_OPPORTUNITY_ID: &str = "opp_smoke_acceptance";
pub const SMOKE_HOLDER: &str = "0xholder_smoke";

#[must_use]
pub fn smoke_simulation_config() -> SimulationConfig {
    let mut config = SimulationConfig::production_default();
    config.max_replay_gap_ms = 120_000;
    config.stale_book_after_ms = 120_000;
    config
}

/// Wall-clock window for the acceptance scenario.
#[must_use]
pub fn smoke_window() -> TimeWindowSpec {
    let from = fixed_time(10);
    let to = fixed_time(12);
    TimeWindowSpec::new(from, to)
}

/// Manifest with production PIT inputs for all evidence stages.
#[must_use]
pub fn smoke_manifest() -> MaterializationRunManifest {
    let as_of = smoke_window().to;
    MaterializationRunManifest {
        run_id: MaterializationRunId::new_v7(),
        run_kind: MaterializationRunKind::Scheduled,
        trigger: RunTrigger::Scheduled {
            schedule_id: "phase5.3-smoke".to_owned(),
        },
        window: smoke_window(),
        source_delay_secs: 0,
        markets: MarketFilterSpec {
            market_ids: vec![MarketId::new(SMOKE_MARKET_ID)],
            event_ids: vec![EventId::new("evt_smoke")],
            token_ids: Vec::new(),
            categories: vec![MarketCategory::Politics],
        },
        replay_account_scope: Some(ReplayAccountScope {
            holder_address: SMOKE_HOLDER.to_owned(),
        }),
        requested_factor_types: vec![
            ControlFactorType::BucketRisk,
            ControlFactorType::ExecutionQuality,
            ControlFactorType::PortfolioRisk,
            ControlFactorType::ReconciliationHealth,
        ],
        data_requirements: DataRequirements {
            required_inputs: vec![
                RequiredInputDomain::TokenMapping,
                RequiredInputDomain::RuntimeConfig,
                RequiredInputDomain::CalibrationSnapshots,
                RequiredInputDomain::FeeSchedule,
                RequiredInputDomain::Trades,
                RequiredInputDomain::Positions,
                RequiredInputDomain::RiskState,
                RequiredInputDomain::BalanceSnapshot,
                RequiredInputDomain::SettlementTruth,
                RequiredInputDomain::ReconciliationStatus,
            ],
            production_required_inputs: vec![
                RequiredInputDomain::TokenMapping,
                RequiredInputDomain::RuntimeConfig,
                RequiredInputDomain::CalibrationSnapshots,
                RequiredInputDomain::FeeSchedule,
                RequiredInputDomain::Trades,
                RequiredInputDomain::Positions,
                RequiredInputDomain::BalanceSnapshot,
                RequiredInputDomain::SettlementTruth,
                RequiredInputDomain::ReconciliationStatus,
            ],
            min_l2_coverage_ratio: None,
            require_settlement_truth: true,
        },
        runtime_config_ref: RuntimeConfigRef::Version {
            version_id: RuntimeConfigVersionId::new("rcv_smoke"),
            config_hash: "blake3:smoke_cfg".to_owned(),
        },
        simulation_config: smoke_simulation_config(),
        quality_gate_policy: QualityGatePolicy::smoke_acceptance(),
        output_policy: MaterializationOutputPolicy::NoFactorOutput,
        code_git_sha: "smoke-local".to_owned(),
        created_by: "phase5.3-smoke".to_owned(),
        created_at: as_of,
    }
}

/// In-memory repositories populated with a single endgame acceptance scenario.
pub struct SmokeRepositories {
    pub timeseries: Arc<MockTimeseriesRepository>,
    pub trades: Arc<MockTradeRepository>,
    pub positions: Arc<MockPositionRepository>,
    pub runtime_config: Arc<SmokeRuntimeConfigRepository>,
    pub balances: Arc<SmokeBalanceRepository>,
    pub potential_loss: Arc<SmokePotentialLossRepository>,
    pub risk_audit: Arc<SmokeRiskAuditRepository>,
    pub reconciliation: Arc<SmokeReconciliationRepository>,
    pub resolution_events: Arc<SmokeResolutionEventRepository>,
    pub control_factors: Arc<SmokeControlFactorRepository>,
    pub markets: FakeMarketRepository,
}

struct SmokeScenarioIds {
    market_id: MarketId,
    yes: TokenId,
    no: TokenId,
    opportunity_id: OpportunityId,
    trade_id: TradeId,
    decision_at: DateTime<Utc>,
    decision_ms: i64,
    from_ms: i64,
    l2_ms: i64,
}

fn smoke_scenario_ids(window: &TimeWindowSpec) -> SmokeScenarioIds {
    let decision_at = window.from + Duration::milliseconds(800);
    let from_ms = window.from.timestamp_millis();
    SmokeScenarioIds {
        market_id: MarketId::new(SMOKE_MARKET_ID),
        yes: TokenId::new(SMOKE_YES_TOKEN),
        no: TokenId::new(SMOKE_NO_TOKEN),
        opportunity_id: OpportunityId::new(SMOKE_OPPORTUNITY_ID),
        trade_id: TradeId::new("trade_smoke"),
        decision_at,
        decision_ms: decision_at.timestamp_millis(),
        from_ms,
        l2_ms: from_ms + 500,
    }
}

fn build_smoke_timeseries(ids: &SmokeScenarioIds) -> Arc<MockTimeseriesRepository> {
    let timeseries = Arc::new(MockTimeseriesRepository::default());
    timeseries.set_book_snapshots(vec![
        book_snapshot(
            SMOKE_YES_TOKEN,
            ids.from_ms,
            dec!(0.94),
            dec!(10),
            dec!(0.95),
            dec!(10),
        ),
        book_snapshot(
            SMOKE_NO_TOKEN,
            ids.from_ms,
            dec!(0.05),
            dec!(10),
            dec!(0.06),
            dec!(10),
        ),
    ]);
    // Ask size must cover filled_audit buy budget (94 USD @ 0.95) or StrictFok yields false_miss.
    timeseries.set_l2_events(vec![
        l2_delta(
            SMOKE_YES_TOKEN,
            ids.l2_ms,
            dec!(0.94),
            dec!(20),
            dec!(0.95),
            dec!(120),
        ),
        l2_delta(
            SMOKE_NO_TOKEN,
            ids.l2_ms,
            dec!(0.05),
            dec!(20),
            dec!(0.06),
            dec!(120),
        ),
    ]);
    timeseries.set_detections(vec![detection_row(
        &ids.opportunity_id,
        &ids.market_id,
        &ids.yes,
        &ids.no,
        ids.decision_ms,
    )]);
    timeseries.set_calibration_snapshots(vec![calibration_snapshot(ids.decision_ms)]);
    timeseries.set_audits(vec![
        filled_audit(
            &ids.opportunity_id,
            &ids.market_id,
            &ids.yes,
            &ids.trade_id,
            ids.decision_ms,
            dec!(94),
        ),
        settled_audit(
            &ids.opportunity_id,
            &ids.market_id,
            &ids.yes,
            &ids.trade_id,
            ids.decision_ms + 3_600_000,
        ),
    ]);
    timeseries
}

impl SmokeRepositories {
    #[must_use]
    pub fn build() -> Self {
        let window = smoke_window();
        let ids = smoke_scenario_ids(&window);
        let timeseries = build_smoke_timeseries(&ids);

        let trades = Arc::new(MockTradeRepository::default());
        trades.insert(smoke_trade(
            &ids.trade_id,
            &ids.opportunity_id,
            &ids.market_id,
            &ids.yes,
            ids.decision_at,
        ));

        let positions = Arc::new(MockPositionRepository::default());
        let position_closed_at = window.to + Duration::minutes(5);
        positions.insert(smoke_position(
            &ids.trade_id,
            &ids.market_id,
            &ids.yes,
            ids.decision_at,
            position_closed_at,
        ));

        Self {
            timeseries,
            trades,
            positions,
            runtime_config: Arc::new(SmokeRuntimeConfigRepository),
            balances: Arc::new(SmokeBalanceRepository::new(ids.decision_at, window.to)),
            potential_loss: Arc::new(SmokePotentialLossRepository::new(
                &ids.market_id,
                &ids.yes,
                window.from,
            )),
            risk_audit: Arc::new(SmokeRiskAuditRepository::new(
                ids.decision_at,
                &ids.opportunity_id,
                &ids.trade_id,
            )),
            reconciliation: Arc::new(SmokeReconciliationRepository::new(ids.decision_at)),
            resolution_events: Arc::new(SmokeResolutionEventRepository::new(
                &ids.market_id,
                window.to - Duration::minutes(1),
            )),
            control_factors: Arc::new(SmokeControlFactorRepository::default()),
            markets: FakeMarketRepository {
                snapshots: vec![smoke_market_snapshot(
                    &ids.market_id,
                    window.from + Duration::minutes(5),
                    window.to + Duration::hours(24),
                )],
                current: Vec::new(),
            },
        }
    }
}

#[must_use]
pub fn smoke_runtime_config_version() -> RuntimeConfigVersionInfo {
    let endgame = EndgameDetectionConfig {
        high_threshold: dec!(0.94),
        min_convergence_duration_secs: 1,
        ..Default::default()
    };
    RuntimeConfigVersionInfo {
        runtime_config_version_id: RuntimeConfigVersionId::new("rcv_smoke"),
        config_hash: "blake3:smoke_cfg".to_owned(),
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
                max_portfolio_exposure_usd: Usd::new(dec!(10_000)),
                max_single_position_usd: Usd::new(dec!(1_000)),
                max_daily_loss_usd: Usd::new(dec!(1_000)),
                circuit_breaker_threshold: 10,
            },
        })
        .expect("smoke runtime config json"),
        source: RuntimeConfigVersionSource::Operator,
        created_by: "phase5.3-smoke".to_owned(),
        reason: "acceptance scenario".to_owned(),
        created_at: smoke_window().from,
    }
}

pub struct SmokeRuntimeConfigRepository;

#[async_trait]
impl RuntimeConfigVersionRepository for SmokeRuntimeConfigRepository {
    async fn create_version(
        &self,
        _version: NewRuntimeConfigVersion,
    ) -> Result<RuntimeConfigVersionInfo, StorageError> {
        Err(StorageError::Codec("not implemented".into()))
    }

    async fn activate_version(
        &self,
        _activation: NewRuntimeConfigActivation,
    ) -> Result<RuntimeConfigActivationInfo, StorageError> {
        Err(StorageError::Codec("not implemented".into()))
    }

    async fn create_version_governed(
        &self,
        _version: NewRuntimeConfigVersion,
        _audit: NewControlFactorAuditEvent,
    ) -> Result<RuntimeConfigVersionInfo, StorageError> {
        Err(StorageError::Codec("not implemented".into()))
    }

    async fn activate_version_governed(
        &self,
        _activation: NewRuntimeConfigActivation,
        _audit: NewControlFactorAuditEvent,
    ) -> Result<RuntimeConfigActivationInfo, StorageError> {
        Err(StorageError::Codec("not implemented".into()))
    }

    async fn load_version(
        &self,
        _version_id: &RuntimeConfigVersionId,
    ) -> Result<Option<RuntimeConfigVersionInfo>, StorageError> {
        Ok(Some(smoke_runtime_config_version()))
    }

    async fn load_by_hash(
        &self,
        _config_hash: &str,
    ) -> Result<Option<RuntimeConfigVersionInfo>, StorageError> {
        Ok(Some(smoke_runtime_config_version()))
    }

    async fn load_current(&self) -> Result<Option<RuntimeConfigVersionInfo>, StorageError> {
        Ok(Some(smoke_runtime_config_version()))
    }

    async fn load_active_at(
        &self,
        _at: DateTime<Utc>,
    ) -> Result<Option<RuntimeConfigVersionInfo>, StorageError> {
        Ok(Some(smoke_runtime_config_version()))
    }

    async fn list_activations(
        &self,
        _limit: u64,
    ) -> Result<Vec<RuntimeConfigActivationInfo>, StorageError> {
        Ok(Vec::new())
    }
}

pub struct SmokeBalanceRepository {
    balance: BalanceSnapshotInfo,
}

impl SmokeBalanceRepository {
    fn new(observed_at: DateTime<Utc>, window_to: DateTime<Utc>) -> Self {
        Self {
            balance: BalanceSnapshotInfo {
                balance_snapshot_id: BalanceSnapshotId::new_v7(),
                holder_address: SMOKE_HOLDER.to_owned(),
                internal_available_usd: Usd::new(dec!(5_000)),
                internal_reserved_usd: Usd::new(dec!(95)),
                external_available_usd: Usd::new(dec!(5_000)),
                external_locked_usd: Usd::ZERO,
                drift_usd: Usd::ZERO,
                source: BalanceSnapshotSource::InternalLedger,
                block_number: None,
                reconciliation_report_id: Some(1),
                observed_at: window_to - Duration::minutes(1),
                created_at: observed_at,
            },
        }
    }
}

#[async_trait]
impl BalanceSnapshotRepository for SmokeBalanceRepository {
    async fn create_balance_snapshot(
        &self,
        _snapshot: NewBalanceSnapshot,
    ) -> Result<BalanceSnapshotInfo, StorageError> {
        Err(StorageError::Codec("not implemented".into()))
    }

    async fn latest_balance_before(
        &self,
        holder_address: &str,
        before: DateTime<Utc>,
    ) -> Result<Option<BalanceSnapshotInfo>, StorageError> {
        if holder_address == SMOKE_HOLDER && self.balance.observed_at < before {
            Ok(Some(self.balance.clone()))
        } else {
            Ok(None)
        }
    }
}

pub struct SmokePotentialLossRepository {
    baseline: Vec<PotentialLossInfo>,
}

impl SmokePotentialLossRepository {
    fn new(market_id: &MarketId, token_id: &TokenId, at: DateTime<Utc>) -> Self {
        Self {
            baseline: vec![PotentialLossInfo {
                ledger_id: LedgerId::new("pl_smoke"),
                market_id: market_id.clone(),
                token_id: token_id.clone(),
                shares: Shares::new(dec!(100)),
                entry_price: Price::new(dec!(0.94)),
                max_loss_usd: Usd::new(dec!(6)),
                status: LedgerStatus::Active,
                created_at: at,
                resolved_at: None,
            }],
        }
    }
}

#[async_trait]
impl PotentialLossRepository for SmokePotentialLossRepository {
    async fn create(&self, _entry: NewPotentialLoss) -> Result<PotentialLossInfo, StorageError> {
        Err(StorageError::Codec("not implemented".into()))
    }

    async fn find_active(&self) -> Result<Vec<PotentialLossInfo>, StorageError> {
        Ok(self.baseline.clone())
    }

    async fn find_active_as_of(
        &self,
        at: DateTime<Utc>,
    ) -> Result<Vec<PotentialLossInfo>, StorageError> {
        Ok(self
            .baseline
            .iter()
            .filter(|row| row.created_at <= at)
            .cloned()
            .collect())
    }

    async fn find_changed_between(
        &self,
        _from: DateTime<Utc>,
        _to: DateTime<Utc>,
    ) -> Result<Vec<PotentialLossInfo>, StorageError> {
        Ok(Vec::new())
    }

    async fn find_by_market(
        &self,
        market_id: &MarketId,
    ) -> Result<Vec<PotentialLossInfo>, StorageError> {
        Ok(self
            .baseline
            .iter()
            .filter(|row| &row.market_id == market_id)
            .cloned()
            .collect())
    }

    async fn resolve(
        &self,
        _ledger_id: &LedgerId,
        _command: ResolvePotentialLoss,
    ) -> Result<PotentialLossInfo, StorageError> {
        Err(StorageError::Codec("not implemented".into()))
    }

    async fn total_active_loss(&self) -> Result<Usd, StorageError> {
        Ok(self
            .baseline
            .iter()
            .map(|row| row.max_loss_usd)
            .fold(Usd::ZERO, |acc, value| acc + value))
    }
}

pub struct SmokeRiskAuditRepository {
    events: Vec<RiskAuditEventInfo>,
}

impl SmokeRiskAuditRepository {
    fn new(at: DateTime<Utc>, opportunity_id: &OpportunityId, trade_id: &TradeId) -> Self {
        Self {
            events: vec![RiskAuditEventInfo {
                id: 1,
                event_type: RiskAuditEventType::TradeAllowed,
                opportunity_id: Some(opportunity_id.clone()),
                trade_id: Some(trade_id.clone()),
                payload: serde_json::json!({}),
                created_at: at,
            }],
        }
    }
}

#[async_trait]
impl RiskAuditRepository for SmokeRiskAuditRepository {
    async fn create(&self, _event: NewRiskAuditEvent) -> Result<(), StorageError> {
        Ok(())
    }

    async fn create_batch(&self, _events: Vec<NewRiskAuditEvent>) -> Result<(), StorageError> {
        Ok(())
    }

    async fn find_between(
        &self,
        from: DateTime<Utc>,
        to: DateTime<Utc>,
    ) -> Result<Vec<RiskAuditEventInfo>, StorageError> {
        Ok(self
            .events
            .iter()
            .filter(|row| row.created_at >= from && row.created_at < to)
            .cloned()
            .collect())
    }
}

pub struct SmokeReconciliationRepository {
    reports: Vec<ReconciliationReportInfo>,
}

impl SmokeReconciliationRepository {
    fn new(at: DateTime<Utc>) -> Self {
        Self {
            reports: vec![ReconciliationReportInfo {
                id: 1,
                status: ReconciliationStatus::Ok,
                mismatches: serde_json::json!([]),
                internal_balance: Usd::new(dec!(5_000)),
                external_balance: Usd::new(dec!(5_000)),
                internal_exposure: Usd::new(dec!(95)),
                external_exposure: Usd::new(dec!(95)),
                reserved: Usd::new(dec!(95)),
                tolerance: Usd::new(dec!(1)),
                checked_at: at,
                duration_ms: 10,
            }],
        }
    }
}

#[async_trait]
impl ReconciliationRepository for SmokeReconciliationRepository {
    async fn create(&self, _report: NewReconciliationReport) -> Result<(), StorageError> {
        Ok(())
    }

    async fn latest_before(
        &self,
        before: DateTime<Utc>,
    ) -> Result<Option<ReconciliationReportInfo>, StorageError> {
        Ok(self
            .reports
            .iter()
            .filter(|row| row.checked_at < before)
            .max_by_key(|row| row.checked_at)
            .cloned())
    }

    async fn find_between(
        &self,
        from: DateTime<Utc>,
        to: DateTime<Utc>,
    ) -> Result<Vec<ReconciliationReportInfo>, StorageError> {
        Ok(self
            .reports
            .iter()
            .filter(|row| row.checked_at >= from && row.checked_at < to)
            .cloned()
            .collect())
    }
}

pub struct SmokeResolutionEventRepository {
    events: Vec<ResolutionEventInfo>,
}

impl SmokeResolutionEventRepository {
    fn new(market_id: &MarketId, resolved_at: DateTime<Utc>) -> Self {
        Self {
            events: vec![ResolutionEventInfo {
                resolution_id: "res_smoke".to_owned(),
                market_id: market_id.clone(),
                outcome: "yes".to_owned(),
                source: "gamma".to_owned(),
                gamma_agrees: Some(true),
                ctf_agrees: Some(true),
                evidence: None,
                resolved_at,
                created_at: resolved_at,
            }],
        }
    }
}

#[async_trait]
impl ResolutionEventRepository for SmokeResolutionEventRepository {
    async fn append(&self, _event: NewResolutionEvent) -> Result<(), StorageError> {
        Ok(())
    }

    async fn latest_for_market(
        &self,
        market_id: &MarketId,
    ) -> Result<Option<ResolutionEventInfo>, StorageError> {
        Ok(self
            .events
            .iter()
            .find(|row| &row.market_id == market_id)
            .cloned())
    }

    async fn latest_before(
        &self,
        market_id: &MarketId,
        before: DateTime<Utc>,
    ) -> Result<Option<ResolutionEventInfo>, StorageError> {
        Ok(self
            .events
            .iter()
            .filter(|row| &row.market_id == market_id && row.resolved_at < before)
            .max_by_key(|row| row.resolved_at)
            .cloned())
    }

    async fn latest_by_source(
        &self,
        _market_id: &MarketId,
        _source: &str,
    ) -> Result<Option<ResolutionEventInfo>, StorageError> {
        Ok(None)
    }
}

#[derive(Default)]
pub struct SmokeControlFactorRepository {
    stage_reports: Mutex<Vec<NewControlFactorStageReport>>,
    factors: Mutex<Vec<ControlFactorValueInfo>>,
    audit_events: Mutex<Vec<ControlFactorAuditEventInfo>>,
}

impl SmokeControlFactorRepository {
    /// In-memory analogue of the global audit chain (sequence + prev/event hash).
    fn append_chained(
        &self,
        event: NewControlFactorAuditEvent,
    ) -> Result<ControlFactorAuditEventInfo, StorageError> {
        let mut events = self.audit_events.lock().unwrap();
        let sequence = i64::try_from(events.len()).unwrap_or(i64::MAX) + 1;
        let prev_event_hash = events.last().map(|event| event.event_hash.clone());
        let now = Utc::now();
        let event_hash = AuditEventContent {
            sequence,
            event_type: event.event_type,
            actor: event.actor.as_str(),
            actor_role: event.actor_role.as_str(),
            resource_type: event.resource_type,
            resource_id: event.resource_id.as_str(),
            request_id: event.request_id.as_str(),
            reason: event.reason.as_str(),
            before_hash: event.before_hash.as_deref(),
            after_hash: event.after_hash.as_deref(),
            diff: &event.diff,
            prev_event_hash: prev_event_hash.as_deref(),
            created_at: now,
        }
        .event_hash()
        .map_err(|error| StorageError::Codec(error.to_string()))?;
        let info = ControlFactorAuditEventInfo {
            event_id: AuditEventId::new_v7(),
            sequence,
            event_type: event.event_type,
            actor: event.actor,
            actor_role: event.actor_role,
            resource_type: event.resource_type,
            resource_id: event.resource_id,
            request_id: event.request_id,
            reason: event.reason,
            before_hash: event.before_hash,
            after_hash: event.after_hash,
            diff: event.diff,
            prev_event_hash,
            event_hash,
            created_at: now,
        };
        events.push(info.clone());
        drop(events);
        Ok(info)
    }
}

#[async_trait]
impl ControlFactorRepository for SmokeControlFactorRepository {
    async fn enqueue_materialization_run(
        &self,
        _run: NewControlFactorMaterializationRun,
        _options: EnqueueMaterializationRunOptions,
    ) -> Result<EnqueueMaterializationRunOutcome, StorageError> {
        Err(StorageError::Codec("not implemented".into()))
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
        _started_at: DateTime<Utc>,
    ) -> Result<AcquireMaterializationRunOutcome, StorageError> {
        Err(StorageError::Codec("not implemented".into()))
    }

    async fn retry_materialization_run(
        &self,
        _run_id: &MaterializationRunId,
    ) -> Result<RunTransitionOutcome, StorageError> {
        Err(StorageError::Codec("not implemented".into()))
    }

    async fn transition_materialization_run(
        &self,
        _run_id: &MaterializationRunId,
        _expected_from: MaterializationRunStatus,
        _target: MaterializationRunStatus,
        _patch: MaterializationRunStatusPatch,
    ) -> Result<RunTransitionOutcome, StorageError> {
        Err(StorageError::Codec("not implemented".into()))
    }

    async fn cancel_materialization_run(
        &self,
        _run_id: &MaterializationRunId,
        _reason: &str,
        _cancelled_at: DateTime<Utc>,
    ) -> Result<CancelMaterializationRunOutcome, StorageError> {
        Err(StorageError::Codec("not implemented".into()))
    }

    async fn upsert_stage_report(
        &self,
        report: NewControlFactorStageReport,
    ) -> Result<ControlFactorStageReportInfo, StorageError> {
        let info = ControlFactorStageReportInfo {
            stage_report_id: report.stage_report_id.clone(),
            materialization_run_id: report.materialization_run_id.clone(),
            stage_name: report.stage_name,
            status: report.status,
            started_at: report.started_at,
            finished_at: report.finished_at,
            input_artifact_hashes: report.input_artifact_hashes.clone(),
            output_artifact_hash: report.output_artifact_hash.clone(),
            coverage: report.coverage.clone(),
            metrics: report.metrics.clone(),
            records_read: report.records_read,
            records_written: report.records_written,
            warnings: report.warnings.clone(),
            errors: report.errors.clone(),
            query_fingerprints: report.query_fingerprints.clone(),
            created_at: Utc::now(),
        };
        self.stage_reports.lock().unwrap().push(report);
        Ok(info)
    }

    async fn load_stage_report(
        &self,
        run_id: &MaterializationRunId,
        stage_name: MaterializationStageName,
    ) -> Result<Option<ControlFactorStageReportInfo>, StorageError> {
        Ok(self
            .list_stage_reports(run_id)
            .await?
            .into_iter()
            .find(|report| report.stage_name == stage_name))
    }

    async fn list_stage_reports(
        &self,
        run_id: &MaterializationRunId,
    ) -> Result<Vec<ControlFactorStageReportInfo>, StorageError> {
        Ok(self
            .stage_reports
            .lock()
            .unwrap()
            .iter()
            .filter(|report| &report.materialization_run_id == run_id)
            .map(stage_report_info_from_new)
            .collect())
    }

    async fn create_factor(
        &self,
        factor: NewControlFactorValue,
        audit: NewControlFactorAuditEvent,
    ) -> Result<ControlFactorValueInfo, StorageError> {
        let info = factor_info_from_new(factor);
        self.factors.lock().unwrap().push(info.clone());
        self.append_chained(audit)?;
        Ok(info)
    }

    async fn load_factor(
        &self,
        factor_id: &ControlFactorId,
    ) -> Result<Option<ControlFactorValueInfo>, StorageError> {
        Ok(self
            .factors
            .lock()
            .unwrap()
            .iter()
            .find(|factor| &factor.factor_id == factor_id)
            .cloned())
    }

    async fn load_factors_by_ids(
        &self,
        factor_ids: &[ControlFactorId],
    ) -> Result<Vec<ControlFactorValueInfo>, StorageError> {
        Ok(self
            .factors
            .lock()
            .unwrap()
            .iter()
            .filter(|factor| factor_ids.contains(&factor.factor_id))
            .cloned()
            .collect())
    }

    async fn list_factors_by_run(
        &self,
        run_id: &MaterializationRunId,
    ) -> Result<Vec<ControlFactorValueInfo>, StorageError> {
        Ok(self
            .factors
            .lock()
            .unwrap()
            .iter()
            .filter(|factor| {
                factor
                    .to_typed()
                    .is_ok_and(|typed| &typed.evidence.materialization_run_id == run_id)
            })
            .cloned()
            .collect())
    }

    async fn list_factors_by_status(
        &self,
        status: FactorStatus,
        factor_type: Option<ControlFactorType>,
    ) -> Result<Vec<ControlFactorValueInfo>, StorageError> {
        Ok(self
            .factors
            .lock()
            .unwrap()
            .iter()
            .filter(|factor| {
                factor.status == status
                    && factor_type.is_none_or(|wanted| factor.factor_type == wanted)
            })
            .cloned()
            .collect())
    }

    async fn reject_factor(
        &self,
        factor_id: &ControlFactorId,
        status_reason: &str,
        audit: NewControlFactorAuditEvent,
    ) -> Result<Option<ControlFactorValueInfo>, StorageError> {
        let mut factors = self.factors.lock().unwrap();
        let Some(factor) = factors
            .iter_mut()
            .find(|factor| &factor.factor_id == factor_id)
        else {
            return Ok(None);
        };
        factor.status = FactorStatus::Rejected;
        factor.status_reason = Some(status_reason.to_owned());
        let updated = factor.clone();
        drop(factors);
        self.append_chained(audit)?;
        Ok(Some(updated))
    }

    async fn expire_factors(
        &self,
        now: DateTime<Utc>,
        actor: AuditActor,
    ) -> Result<ExpireFactorsOutcome, StorageError> {
        let mut factors = self.factors.lock().unwrap();
        let mut due: Vec<ControlFactorValueInfo> = Vec::new();
        for factor in factors.iter_mut() {
            let eligible = matches!(
                factor.status,
                FactorStatus::Candidate
                    | FactorStatus::Shadow
                    | FactorStatus::Published
                    | FactorStatus::ReportOnly
            );
            if eligible && factor.expires_at <= now {
                factor.status = FactorStatus::Expired;
                due.push(factor.clone());
            }
        }
        drop(factors);
        let mut expired = Vec::with_capacity(due.len());
        for factor in due {
            self.append_chained(NewControlFactorAuditEvent {
                event_type: ControlAuditEventType::FactorExpired,
                actor: actor.actor.clone(),
                actor_role: actor.actor_role.clone(),
                resource_type: AuditResourceType::Factor,
                resource_id: factor.factor_id.as_str().to_owned(),
                request_id: actor.request_id.clone(),
                reason: actor.reason.clone(),
                before_hash: None,
                after_hash: None,
                diff: serde_json::json!({ "to_status": FactorStatus::Expired }),
            })?;
            expired.push(factor.factor_id);
        }
        Ok(ExpireFactorsOutcome { expired })
    }

    async fn publish_publication(
        &self,
        _publication: NewControlFactorPublication,
        _audit: NewControlFactorAuditEvent,
    ) -> Result<PublishPublicationOutcome, StorageError> {
        Err(StorageError::Codec(
            "publication lifecycle is covered by Postgres integration tests".into(),
        ))
    }

    async fn load_publication(
        &self,
        _publication_id: &FactorPublicationId,
    ) -> Result<Option<ControlFactorPublicationInfo>, StorageError> {
        Ok(None)
    }

    async fn load_active_publication(
        &self,
        _mode: PublicationMode,
    ) -> Result<Option<ControlFactorPublicationInfo>, StorageError> {
        Ok(None)
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
    ) -> Result<ControlFactorPublicationInfo, StorageError> {
        Err(StorageError::Codec(
            "publication lifecycle is covered by Postgres integration tests".into(),
        ))
    }

    async fn append_audit_event(
        &self,
        event: NewControlFactorAuditEvent,
    ) -> Result<ControlFactorAuditEventInfo, StorageError> {
        self.append_chained(event)
    }

    async fn load_audit_chain(
        &self,
        from_sequence: i64,
        limit: u64,
    ) -> Result<Vec<ControlFactorAuditEventInfo>, StorageError> {
        Ok(self
            .audit_events
            .lock()
            .unwrap()
            .iter()
            .filter(|event| event.sequence >= from_sequence)
            .take(usize::try_from(limit).unwrap_or(usize::MAX))
            .cloned()
            .collect())
    }
}

fn stage_report_info_from_new(
    report: &NewControlFactorStageReport,
) -> ControlFactorStageReportInfo {
    ControlFactorStageReportInfo {
        stage_report_id: report.stage_report_id.clone(),
        materialization_run_id: report.materialization_run_id.clone(),
        stage_name: report.stage_name,
        status: report.status,
        started_at: report.started_at,
        finished_at: report.finished_at,
        input_artifact_hashes: report.input_artifact_hashes.clone(),
        output_artifact_hash: report.output_artifact_hash.clone(),
        coverage: report.coverage.clone(),
        metrics: report.metrics.clone(),
        records_read: report.records_read,
        records_written: report.records_written,
        warnings: report.warnings.clone(),
        errors: report.errors.clone(),
        query_fingerprints: report.query_fingerprints.clone(),
        created_at: Utc::now(),
    }
}

fn factor_info_from_new(factor: NewControlFactorValue) -> ControlFactorValueInfo {
    ControlFactorValueInfo {
        factor_id: factor.factor_id,
        run_id: factor.run_id,
        factor_type: factor.factor_type,
        dimensions: factor.dimensions,
        dimensions_hash: factor.dimensions_hash,
        payload: factor.payload,
        payload_hash: factor.payload_hash,
        evidence: factor.evidence,
        status: factor.status,
        status_reason: factor.status_reason,
        generated_at: factor.generated_at,
        expires_at: factor.expires_at,
        owner: factor.owner,
        schema_version: factor.schema_version,
        created_at: Utc::now(),
        updated_at: Utc::now(),
    }
}

fn smoke_market_snapshot(
    market_id: &MarketId,
    observed_at: DateTime<Utc>,
    settlement_deadline: DateTime<Utc>,
) -> MarketPitSnapshotInfo {
    let mut snapshot = market_snapshot(market_id, SMOKE_YES_TOKEN, SMOKE_NO_TOKEN, observed_at);
    snapshot.end_date = Some(settlement_deadline);
    snapshot
}

fn book_snapshot(
    token_id: &str,
    event_time: i64,
    bid: rust_decimal::Decimal,
    bid_size: rust_decimal::Decimal,
    ask: rust_decimal::Decimal,
    ask_size: rust_decimal::Decimal,
) -> BookSnapshotRow {
    BookSnapshotRow {
        token_id: TokenId::new(token_id),
        market_id: Some(MarketId::new(SMOKE_MARKET_ID)),
        snapshot_reason: ChSnapshotReason::Periodic,
        top_n: 2,
        bids_json: format!(r#"[["{bid}","{bid_size}"]]"#),
        asks_json: format!(r#"[["{ask}","{ask_size}"]]"#),
        bid_depth_usd: None,
        ask_depth_usd: None,
        mid_price: None,
        spread_bps: None,
        book_version: 1,
        levels_count: 2,
        event_time,
        ingestion_time: event_time,
        sequence: 1,
        source: ChFactSource::WsSnapshot,
        schema_version: ChSchemaVersion(1),
    }
}

fn l2_delta(
    token_id: &str,
    event_time: i64,
    bid_price: rust_decimal::Decimal,
    bid_size: rust_decimal::Decimal,
    ask_price: rust_decimal::Decimal,
    ask_size: rust_decimal::Decimal,
) -> TickEventL2Row {
    TickEventL2Row {
        token_id: TokenId::new(token_id),
        market_id: Some(MarketId::new(SMOKE_MARKET_ID)),
        event_type: ChBookEventType::Delta,
        bid_prices: vec![ChPrice::from(Price::new(bid_price))],
        bid_sizes: vec![ChShares::from(Shares::new(bid_size))],
        ask_prices: vec![ChPrice::from(Price::new(ask_price))],
        ask_sizes: vec![ChShares::from(Shares::new(ask_size))],
        changed_levels_json: None,
        book_version: 2,
        levels_count: 2,
        is_full_snapshot: false,
        event_time,
        ingestion_time: event_time,
        sequence: 2,
        source: ChFactSource::WsDelta,
        schema_version: ChSchemaVersion(1),
    }
}

fn detection_row(
    opportunity_id: &OpportunityId,
    market_id: &MarketId,
    yes: &TokenId,
    no: &TokenId,
    detected_at: i64,
) -> OpportunityDetectionRow {
    OpportunityDetectionRow {
        opportunity_id: opportunity_id.clone(),
        market_id: market_id.clone(),
        event_id: EventId::new("evt_smoke"),
        token_id: yes.clone(),
        token_yes: Some(yes.clone()),
        token_no: Some(no.clone()),
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
        score: Some(1),
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
        calibration_snapshot_hash: Some("calibration_smoke".to_owned()),
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
        detected_at,
        ingestion_time: detected_at,
        sequence: 1,
        schema_version: ChSchemaVersion(2),
    }
}

fn calibration_snapshot(event_time: i64) -> CalibrationSnapshotRow {
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
        config_hash: "blake3:smoke_cfg".to_owned(),
        snapshot_hash: "calibration_smoke".to_owned(),
        event_time,
        ingestion_time: event_time,
        sequence: 1,
        source: ChFactSource::CalibrationUpdater,
        schema_version: ChSchemaVersion(1),
    }
}

struct SmokeAuditRowSpec<'a> {
    opportunity_id: &'a OpportunityId,
    market_id: &'a MarketId,
    token_id: &'a TokenId,
    trade_id: &'a TradeId,
    stage: ChOpportunityAuditStage,
    stage_order: u8,
    stage_at: i64,
    outcome: Option<ChAuditOutcome>,
    settlement_status: Option<ChSettlementOutcome>,
    accounting_status: Option<ChSettlementAccountingStatus>,
}

fn filled_audit(
    opportunity_id: &OpportunityId,
    market_id: &MarketId,
    token_id: &TokenId,
    trade_id: &TradeId,
    stage_at: i64,
    total_cost: rust_decimal::Decimal,
) -> OpportunityAuditRow {
    let spec = SmokeAuditRowSpec {
        opportunity_id,
        market_id,
        token_id,
        trade_id,
        stage: ChOpportunityAuditStage::Filled,
        stage_order: 70_u8,
        stage_at,
        outcome: Some(ChAuditOutcome::Success),
        settlement_status: None,
        accounting_status: None,
    };
    let mut row = audit_row(&spec);
    row.total_cost_usd = Some(ChUsd::from(Usd::new(total_cost)));
    row
}

fn settled_audit(
    opportunity_id: &OpportunityId,
    market_id: &MarketId,
    token_id: &TokenId,
    trade_id: &TradeId,
    stage_at: i64,
) -> OpportunityAuditRow {
    let spec = SmokeAuditRowSpec {
        opportunity_id,
        market_id,
        token_id,
        trade_id,
        stage: ChOpportunityAuditStage::Settled,
        stage_order: 100_u8,
        stage_at,
        outcome: None,
        settlement_status: Some(ChSettlementOutcome::Won),
        accounting_status: Some(ChSettlementAccountingStatus::Redeemed),
    };
    let mut row = audit_row(&spec);
    row.payout_usd = Some(ChUsd::from(Usd::new(dec!(100))));
    row.realized_pnl_usd = Some(ChUsd::from(Usd::new(dec!(4))));
    row.winning_token_id = Some(token_id.clone());
    row
}

fn audit_row(spec: &SmokeAuditRowSpec<'_>) -> OpportunityAuditRow {
    OpportunityAuditRow {
        opportunity_id: spec.opportunity_id.clone(),
        execution_id: ExecutionId::generate(),
        trade_id: Some(spec.trade_id.clone()),
        market_id: spec.market_id.clone(),
        event_id: EventId::new("evt_smoke"),
        token_id: spec.token_id.clone(),
        side: ChSide::Buy,
        entry_price: Some(ChPrice::from(Price::new(dec!(0.95)))),
        fill_price: Some(ChPrice::from(Price::new(dec!(0.94)))),
        requested_shares: Some(ChShares::from(Shares::new(dec!(100)))),
        filled_shares: Some(ChShares::from(Shares::new(dec!(100)))),
        total_cost_usd: Some(ChUsd::from(Usd::new(dec!(94)))),
        fees_usd: Some(ChUsd::from(Usd::ZERO)),
        net_profit_usd: None,
        expected_profit_usd: None,
        edge_bps: None,
        resolution_prob: None,
        confidence: None,
        fill_probability: None,
        convergence_secs: None,
        price_zone: None,
        duration_bucket: None,
        depth_used_pct: None,
        staleness: None,
        category: None,
        stage: spec.stage,
        stage_order: spec.stage_order,
        stage_at: spec.stage_at,
        payout_usd: None,
        realized_pnl_usd: None,
        settlement_status: spec.settlement_status,
        settlement_trigger: Some(ChSettlementTrigger::from(SettlementTrigger::Ws)),
        winning_token_id: None,
        accounting_status: spec.accounting_status,
        fee_source: None,
        outcome: spec.outcome,
        rejection_stage: None,
        rejection_reason: None,
        scored_snapshot_json: Some("{}".to_owned()),
        book_context_json: None,
        applied_factor_ids_json: None,
        missing_fields_json: None,
        detected_at: spec.stage_at.saturating_sub(1_800_000),
        ingestion_time: spec.stage_at,
        sequence: 1,
        schema_version: ChSchemaVersion(2),
        updated_at: spec.stage_at,
    }
}

fn smoke_trade(
    trade_id: &TradeId,
    opportunity_id: &OpportunityId,
    market_id: &MarketId,
    token_id: &TokenId,
    created_at: DateTime<Utc>,
) -> TradeInfo {
    TradeInfo {
        trade_id: trade_id.clone(),
        execution_id: ExecutionId::generate(),
        reservation_id: ReservationId::new("res_smoke"),
        opportunity_id: opportunity_id.clone(),
        market_id: market_id.clone(),
        event_id: EventId::new("evt_smoke"),
        token_id: token_id.clone(),
        side: Side::Buy,
        shares: Shares::new(dec!(100)),
        price: Price::new(dec!(0.94)),
        cost_usd: Usd::new(dec!(94)),
        fee_usd: Usd::ZERO,
        detected_edge_bps: None,
        detected_profit_usd: None,
        net_profit_usd: None,
        order_id: None,
        tx_hash: None,
        state: TradeState::Settled,
        business_outcome: None,
        scored_snapshot: serde_json::json!({}),
        category: MarketCategory::Politics,
        needs_reconcile: false,
        post_trade_claim_owner: None,
        post_trade_claimed_at: None,
        post_trade_attempts: 0,
        execution_mode: ExecutionMode::DryRun,
        latency_ms: None,
        error_message: None,
        submitted_at: Some(created_at),
        confirmed_at: Some(created_at),
        created_at,
        updated_at: created_at,
    }
}

fn smoke_position(
    trade_id: &TradeId,
    market_id: &MarketId,
    token_id: &TokenId,
    opened_at: DateTime<Utc>,
    settled_at: DateTime<Utc>,
) -> PositionInfo {
    PositionInfo {
        position_id: PositionId::new("pos_smoke"),
        trade_id: trade_id.clone(),
        market_id: market_id.clone(),
        token_id: token_id.clone(),
        side: Side::Buy,
        shares: Shares::new(dec!(100)),
        avg_entry_price: Price::new(dec!(0.94)),
        total_cost_usd: Usd::new(dec!(94)),
        total_fees_usd: Usd::ZERO,
        unrealized_pnl: Usd::ZERO,
        realized_pnl: Usd::new(dec!(4)),
        status: PositionStatus::Settled,
        opened_at,
        closed_at: Some(settled_at),
        settled_at: Some(settled_at),
        winning_token_id: Some(token_id.clone()),
        settlement_payout_usd: Some(Usd::new(dec!(100))),
        redeem_tx_hash: None,
        redeem_status: RedeemStatus::Completed,
        redeem_attempts: 0,
        oracle_verdict: None,
        settlement_trigger: Some(SettlementTrigger::Ws),
        settlement_accounting_status: SettlementAccountingStatus::Redeemed,
        settlement_accounting_error: None,
        settlement_accounted_at: Some(settled_at),
        redeem_terminal_reason: None,
    }
}

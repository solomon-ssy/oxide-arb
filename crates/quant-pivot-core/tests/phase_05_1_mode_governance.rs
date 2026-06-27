//! Phase 05.1 — mode-transition gate, preflight engine, kill-switch control.

use async_trait::async_trait;
use chrono::{DateTime, Utc};
use quant_pivot_core::{
    governance::{
        DefaultModePreflight, DefaultModeTransitionGate, KillSwitchControl, KillSwitchHandle,
        ModePreflight, ModePreflightDeps, ModeTransitionGate, SystemStatusPublisher,
    },
    observability::metrics_hub::MetricsHub,
    runtime_config::RuntimeConfigStore,
};
use quant_pivot_error::storage::StorageError;
use quant_pivot_models::{
    config::DeployConfig,
    domain::{
        AppendReconciliationEvidence, CapitalAllocationInfo, CoreEventPublisher, DataQualityPort,
        DataQualitySnapshot, KillSwitchPort, KillSwitchStateInfo, KillSwitchView, ModelSpecInfo,
        ModelVersionInfo, NewModelSpec, NewModelVersion, NewReconciliation, NewShadowComparison,
        ReconciliationInfo, ReconciliationPatch, SetKillSwitchCommand, ShadowComparisonInfo,
        ShadowStabilitySummary, UpsertKillSwitchState,
    },
    enums::{
        execution::KillSwitchState,
        quant::{PublicationStatus, QuantRuntimeMode},
    },
    runtime_config::{ModelVersionRef, RuntimeConfig},
    types::{
        ContentHash, ExecutionOrderId, ModelSpecId, ModelVersionId, OrderIntentId, Probability,
        ReconciliationId, Usd,
    },
};
use quant_pivot_repository::traits::{
    CapitalAllocationRepository, KillSwitchStateRepository, ModelRegistryRepository,
    ReconciliationRepository, ShadowComparisonRepository,
};
use std::sync::{Arc, Mutex};

// ── Mocks ───────────────────────────────────────────────────────────────────

struct MockModelRegistry {
    version: Option<ModelVersionInfo>,
}

#[async_trait]
impl ModelRegistryRepository for MockModelRegistry {
    async fn create_model_spec(&self, _spec: NewModelSpec) -> Result<ModelSpecInfo, StorageError> {
        unimplemented!()
    }
    async fn create_model_version(
        &self,
        _version: NewModelVersion,
    ) -> Result<ModelVersionInfo, StorageError> {
        unimplemented!()
    }
    async fn next_version_for_spec(
        &self,
        _model_spec_id: &ModelSpecId,
    ) -> Result<i32, StorageError> {
        unimplemented!()
    }
    async fn find_model_version_by_id(
        &self,
        _model_version_id: &ModelVersionId,
    ) -> Result<Option<ModelVersionInfo>, StorageError> {
        Ok(self.version.clone())
    }
    async fn list_published_for_spec(
        &self,
        _model_spec_id: &ModelSpecId,
    ) -> Result<Vec<ModelVersionInfo>, StorageError> {
        unimplemented!()
    }
    async fn publish_model_version(
        &self,
        _model_version_id: &ModelVersionId,
    ) -> Result<ModelVersionInfo, StorageError> {
        unimplemented!()
    }
    async fn retire_model_version(
        &self,
        _model_version_id: &ModelVersionId,
    ) -> Result<ModelVersionInfo, StorageError> {
        unimplemented!()
    }
    async fn promote_model_to_shadow(
        &self,
        _model_version_id: &ModelVersionId,
    ) -> Result<ModelVersionInfo, StorageError> {
        unimplemented!()
    }
    async fn restore_model_version(
        &self,
        _model_version_id: &ModelVersionId,
    ) -> Result<ModelVersionInfo, StorageError> {
        unimplemented!()
    }
    async fn set_quality_gate_report(
        &self,
        _model_version_id: &ModelVersionId,
        _quality_gate_report: serde_json::Value,
    ) -> Result<ModelVersionInfo, StorageError> {
        unimplemented!()
    }
}

struct MockShadow {
    summary: ShadowStabilitySummary,
}

#[async_trait]
impl ShadowComparisonRepository for MockShadow {
    async fn create(
        &self,
        _comparison: NewShadowComparison,
    ) -> Result<ShadowComparisonInfo, StorageError> {
        unimplemented!()
    }
    async fn summary(
        &self,
        _shadow_model_version_id: &ModelVersionId,
        _since: DateTime<Utc>,
    ) -> Result<ShadowStabilitySummary, StorageError> {
        Ok(self.summary.clone())
    }
}

struct MockRecon {
    unresolvable: bool,
}

#[async_trait]
impl ReconciliationRepository for MockRecon {
    async fn create(
        &self,
        _reconciliation: NewReconciliation,
    ) -> Result<ReconciliationInfo, StorageError> {
        unimplemented!()
    }
    async fn append_evidence(
        &self,
        _reconciliation_id: &ReconciliationId,
        _evidence: AppendReconciliationEvidence,
    ) -> Result<ReconciliationInfo, StorageError> {
        unimplemented!()
    }
    async fn resolve(
        &self,
        _reconciliation_id: &ReconciliationId,
        _patch: ReconciliationPatch,
    ) -> Result<ReconciliationInfo, StorageError> {
        unimplemented!()
    }
    async fn find_by_execution_order(
        &self,
        _execution_order_id: &ExecutionOrderId,
    ) -> Result<Option<ReconciliationInfo>, StorageError> {
        unimplemented!()
    }
    async fn find_unresolved(&self) -> Result<Vec<ReconciliationInfo>, StorageError> {
        unimplemented!()
    }
    async fn has_unresolvable(&self) -> Result<bool, StorageError> {
        Ok(self.unresolvable)
    }
}

struct MockCapital {
    impaired: bool,
}

#[async_trait]
impl CapitalAllocationRepository for MockCapital {
    async fn find_by_intent(
        &self,
        _order_intent_id: &OrderIntentId,
    ) -> Result<Option<CapitalAllocationInfo>, StorageError> {
        unimplemented!()
    }
    async fn sum_reserved_usd(&self) -> Result<Usd, StorageError> {
        unimplemented!()
    }
    async fn has_impaired(&self) -> Result<bool, StorageError> {
        Ok(self.impaired)
    }
}

struct MockDataQuality {
    snapshot: DataQualitySnapshot,
}

impl DataQualityPort for MockDataQuality {
    fn snapshot(&self) -> DataQualitySnapshot {
        self.snapshot.clone()
    }
}

struct MockKillSwitchRepo {
    row: Mutex<Option<KillSwitchStateInfo>>,
}

#[async_trait]
impl KillSwitchStateRepository for MockKillSwitchRepo {
    async fn load(&self) -> Result<Option<KillSwitchStateInfo>, StorageError> {
        Ok(self.row.lock().unwrap().clone())
    }
    async fn upsert(
        &self,
        state: UpsertKillSwitchState,
    ) -> Result<KillSwitchStateInfo, StorageError> {
        let now = Utc::now();
        let info = KillSwitchStateInfo {
            id: state.id,
            state: state.state,
            changed_by: state.changed_by,
            reason: state.reason,
            requires_operator_ack: state.requires_operator_ack,
            changed_at: state.changed_at,
            updated_at: now,
        };
        *self.row.lock().unwrap() = Some(info.clone());
        Ok(info)
    }
}

// ── Fixtures ─────────────────────────────────────────────────────────────────

fn green_snapshot() -> DataQualitySnapshot {
    DataQualitySnapshot {
        as_of: Utc::now(),
        total_tokens: 10,
        fresh: 10,
        acceptable: 0,
        degraded: 0,
        stale: 0,
        insufficient: 0,
        max_book_age_ms: 0,
        max_fact_lag_ms: 0,
        worst_fact_lag_ms: 0,
        fact_lag_exceeded: false,
    }
}

fn published_version(id: &ModelVersionId) -> ModelVersionInfo {
    ModelVersionInfo {
        model_version_id: id.clone(),
        model_spec_id: ModelSpecId::from_v7(),
        version: 1,
        artifact_hash: ContentHash::parse(format!("blake3:{}", "0".repeat(64))).expect("hash"),
        training_dataset_id: None,
        metrics_json: serde_json::json!({}),
        quality_gate_report: serde_json::json!({ "passed": true }),
        publication_status: PublicationStatus::Published,
        published_at: Some(Utc::now()),
        retired_at: None,
        created_at: Utc::now(),
    }
}

fn ready_deploy() -> DeployConfig {
    let mut deploy = DeployConfig::default();
    deploy.keys.private_key = Some("0xabc".to_owned());
    deploy.quant.account.funder = Some("0xfunder".to_owned());
    "a-sufficiently-strong-production-secret-value".clone_into(&mut deploy.web.jwt.secret);
    deploy
}

struct PreflightFixture {
    deploy: DeployConfig,
    config: RuntimeConfig,
    model_version: Option<ModelVersionInfo>,
    shadow: ShadowStabilitySummary,
    unresolvable: bool,
    impaired: bool,
    snapshot: DataQualitySnapshot,
    kill_switch: KillSwitchState,
}

impl PreflightFixture {
    fn ready_for_auto() -> Self {
        let model_id = ModelVersionId::from_v7();
        let mut config = RuntimeConfig::default();
        config.model.active_model_version_id = Some(ModelVersionRef {
            id: model_id.to_string(),
        });
        config.portfolio.budget.total_budget_usd =
            quant_pivot_models::runtime_config::DecimalString::new("1000");
        Self {
            deploy: ready_deploy(),
            config,
            model_version: Some(published_version(&model_id)),
            shadow: ShadowStabilitySummary {
                shadow_model_version_id: ModelVersionId::from_v7(),
                sample_count: 0,
                window_start: None,
                window_end: None,
                mean_topn_overlap: Probability::default(),
                any_hard_divergence: false,
            },
            unresolvable: false,
            impaired: false,
            snapshot: green_snapshot(),
            kill_switch: KillSwitchState::Closed,
        }
    }

    fn build(self) -> DefaultModePreflight {
        DefaultModePreflight::new(ModePreflightDeps {
            deploy: Arc::new(self.deploy),
            config_store: Arc::new(RuntimeConfigStore::new(self.config)),
            data_quality: Arc::new(MockDataQuality {
                snapshot: self.snapshot,
            }),
            model_registry: Arc::new(MockModelRegistry {
                version: self.model_version,
            }),
            shadow_comparison: Arc::new(MockShadow {
                summary: self.shadow,
            }),
            reconciliation: Arc::new(MockRecon {
                unresolvable: self.unresolvable,
            }),
            capital: Arc::new(MockCapital {
                impaired: self.impaired,
            }),
            kill_switch: KillSwitchHandle::new(self.kill_switch),
        })
    }
}

fn failed(report: &quant_pivot_models::domain::PreflightReport, name: &str) -> bool {
    report
        .checks
        .iter()
        .any(|check| check.name == name && check.hard && !check.passed)
}

// ── Transition matrix ────────────────────────────────────────────────────────

#[test]
fn mode_transition_matrix_allows_only_spec_edges() {
    let gate = DefaultModeTransitionGate::new();
    assert!(
        gate.check(QuantRuntimeMode::ReportOnly, QuantRuntimeMode::SemiAuto)
            .is_ok()
    );
    assert!(
        gate.check(QuantRuntimeMode::SemiAuto, QuantRuntimeMode::AutoExecution)
            .is_ok()
    );
    assert!(
        gate.check(
            QuantRuntimeMode::AutoExecution,
            QuantRuntimeMode::ReportOnly
        )
        .is_ok()
    );
    assert!(
        gate.check(
            QuantRuntimeMode::ReportOnly,
            QuantRuntimeMode::AutoExecution
        )
        .is_err(),
        "report_only -> auto_execution must be forbidden"
    );
}

// ── Preflight ────────────────────────────────────────────────────────────────

#[tokio::test]
async fn semi_auto_all_green_passes() {
    let report = PreflightFixture::ready_for_auto()
        .build()
        .run(QuantRuntimeMode::SemiAuto)
        .await
        .expect("preflight runs");
    assert!(report.passed, "summary: {}", report.summary());
}

#[tokio::test]
async fn semi_auto_requires_credentials_and_data_quality() {
    let mut fixture = PreflightFixture::ready_for_auto();
    fixture.deploy.keys.private_key = None;
    fixture.snapshot.fact_lag_exceeded = true;
    let report = fixture
        .build()
        .run(QuantRuntimeMode::SemiAuto)
        .await
        .expect("preflight runs");
    assert!(!report.passed);
    assert!(failed(&report, "credentials_loaded"));
    assert!(failed(&report, "data_quality_green"));
}

#[tokio::test]
async fn semi_auto_requires_a_model() {
    let mut fixture = PreflightFixture::ready_for_auto();
    fixture.config.model.active_model_version_id = None;
    fixture.model_version = None;
    let report = fixture
        .build()
        .run(QuantRuntimeMode::SemiAuto)
        .await
        .expect("preflight runs");
    assert!(failed(&report, "model_available"));
}

#[tokio::test]
async fn auto_execution_requires_kill_switch_closed() {
    let mut fixture = PreflightFixture::ready_for_auto();
    fixture.kill_switch = KillSwitchState::ExecutionHalted;
    let report = fixture
        .build()
        .run(QuantRuntimeMode::AutoExecution)
        .await
        .expect("preflight runs");
    assert!(!report.passed);
    assert!(failed(&report, "kill_switch_closed"));
}

#[tokio::test]
async fn auto_execution_requires_published_model() {
    let mut fixture = PreflightFixture::ready_for_auto();
    if let Some(version) = fixture.model_version.as_mut() {
        version.publication_status = PublicationStatus::Candidate;
    }
    let report = fixture
        .build()
        .run(QuantRuntimeMode::AutoExecution)
        .await
        .expect("preflight runs");
    assert!(!report.passed);
    assert!(failed(&report, "published_model"));
}

#[tokio::test]
async fn auto_execution_all_green_passes() {
    let report = PreflightFixture::ready_for_auto()
        .build()
        .run(QuantRuntimeMode::AutoExecution)
        .await
        .expect("preflight runs");
    assert!(report.passed, "summary: {}", report.summary());
}

// ── Kill switch control ──────────────────────────────────────────────────────

fn closed_view() -> KillSwitchView {
    KillSwitchView {
        state: KillSwitchState::Closed,
        requires_operator_ack: false,
        last_reason: "bootstrap".to_owned(),
        changed_by: "system".to_owned(),
        changed_at: Utc::now(),
    }
}

fn kill_switch_control(initial: KillSwitchState) -> (Arc<KillSwitchControl>, Arc<MetricsHub>) {
    let metrics = Arc::new(MetricsHub::new());
    let (events, _rx) = CoreEventPublisher::bounded(8);
    let status_publisher = SystemStatusPublisher::new(events);
    let control = Arc::new(KillSwitchControl::new(
        KillSwitchHandle::new(initial),
        closed_view(),
        Arc::new(MockKillSwitchRepo {
            row: Mutex::new(None),
        }),
        Arc::clone(&metrics),
        status_publisher,
    ));
    (control, metrics)
}

#[tokio::test]
async fn kill_switch_set_persists_and_meters() {
    let (control, metrics) = kill_switch_control(KillSwitchState::Closed);
    assert_eq!(metrics.auto_execution_halted.get(), 0);

    let view = control
        .set(SetKillSwitchCommand {
            target: KillSwitchState::ExecutionHalted,
            actor: "operator".to_owned(),
            reason: "manual halt".to_owned(),
            ack: false,
            latch: false,
        })
        .await
        .expect("set succeeds");
    assert_eq!(view.state, KillSwitchState::ExecutionHalted);
    assert_eq!(control.current(), KillSwitchState::ExecutionHalted);
    assert_eq!(metrics.auto_execution_halted.get(), 1);
}

#[tokio::test]
async fn kill_switch_emergency_requires_ack_to_clear() {
    let (control, _metrics) = kill_switch_control(KillSwitchState::EmergencyHalted);

    let denied = control
        .set(SetKillSwitchCommand {
            target: KillSwitchState::Closed,
            actor: "operator".to_owned(),
            reason: "clear".to_owned(),
            ack: false,
            latch: false,
        })
        .await;
    assert!(denied.is_err(), "clearing emergency without ack must fail");
    assert_eq!(control.current(), KillSwitchState::EmergencyHalted);

    let cleared = control
        .set(SetKillSwitchCommand {
            target: KillSwitchState::Closed,
            actor: "operator".to_owned(),
            reason: "clear".to_owned(),
            ack: true,
            latch: false,
        })
        .await
        .expect("ack clears emergency");
    assert_eq!(cleared.state, KillSwitchState::Closed);
}

// ── Postgres integration (requires Docker) ─────────────────────────────────

#[cfg(test)]
mod pg {
    use async_trait::async_trait;
    use quant_pivot_core::{
        app::RuntimeSnapshot,
        governance::{
            DefaultModeTransitionGate, KillSwitchControl, KillSwitchHandle, ModePreflight,
            QuantRuntimeControl, RuntimeModeHandle, SystemStatusPublisher,
            runtime_control::QuantRuntimeControlDeps,
        },
        observability::metrics_hub::MetricsHub,
    };
    use quant_pivot_error::QuantResult;
    use quant_pivot_models::{
        config::DeployConfig,
        domain::governance::lifecycle::OperationalPhase,
        domain::{
            CoreEvent, CoreEventPublisher, KillSwitchPort, KillSwitchView, PreflightCheck,
            PreflightReport, RuntimeControlPort, SetKillSwitchCommand,
        },
        entities::system_kill_switch,
        enums::{execution::KillSwitchState, quant::QuantRuntimeMode},
    };
    use quant_pivot_repository::{
        postgres::{
            PgKillSwitchStateRepository, PgSystemRuntimeStateRepository, SYSTEM_KILL_SWITCH_ID,
        },
        traits::{KillSwitchStateRepository, SystemRuntimeStateRepository},
    };
    use quant_pivot_storage::postgres::PostgresPool;
    use quant_pivot_test_support::{governance::operational_health_checker, pg::setup_pg};
    use sea_orm::{DatabaseConnection, EntityTrait};
    use std::sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    };

    struct CountingPreflight {
        runs: AtomicUsize,
        pass: bool,
    }

    #[async_trait]
    impl ModePreflight for CountingPreflight {
        async fn run(&self, target: QuantRuntimeMode) -> QuantResult<PreflightReport> {
            self.runs.fetch_add(1, Ordering::SeqCst);
            Ok(PreflightReport::new(
                target,
                vec![PreflightCheck::hard(
                    "mock",
                    self.pass,
                    if self.pass { "ok" } else { "denied" },
                )],
            ))
        }
    }

    struct MockKillSwitchPort {
        handle: KillSwitchHandle,
    }

    #[async_trait]
    impl KillSwitchPort for MockKillSwitchPort {
        fn current(&self) -> KillSwitchState {
            self.handle.current()
        }

        fn view(&self) -> KillSwitchView {
            KillSwitchView {
                state: self.handle.current(),
                requires_operator_ack: false,
                last_reason: "test".to_owned(),
                changed_by: "test".to_owned(),
                changed_at: chrono::Utc::now(),
            }
        }

        async fn set(&self, command: SetKillSwitchCommand) -> QuantResult<KillSwitchView> {
            self.handle.store(command.target);
            Ok(self.view())
        }
    }

    async fn build_mode_control(
        db: &DatabaseConnection,
        pg: Arc<PostgresPool>,
        initial_mode: QuantRuntimeMode,
        preflight_pass: bool,
    ) -> (
        Arc<QuantRuntimeControl>,
        PgSystemRuntimeStateRepository,
        Arc<CountingPreflight>,
        flume::Receiver<CoreEvent>,
    ) {
        let deploy = DeployConfig::default();
        let mode_repo = PgSystemRuntimeStateRepository::new(db.clone());
        mode_repo
            .upsert_quant_runtime_mode(initial_mode, "test", "seed mode")
            .await
            .expect("seed runtime mode");
        let runtime_mode = RuntimeModeHandle::new(initial_mode);

        let (events, events_rx) = CoreEventPublisher::bounded(8);
        let status_publisher = SystemStatusPublisher::new(events);
        let preflight = Arc::new(CountingPreflight {
            runs: AtomicUsize::new(0),
            pass: preflight_pass,
        });

        let control = Arc::new(QuantRuntimeControl::new(QuantRuntimeControlDeps {
            runtime_mode: runtime_mode.clone(),
            health_checker: operational_health_checker(Arc::clone(&pg), runtime_mode, &deploy),
            runtime_state_repo: PgSystemRuntimeStateRepository::new(db.clone()),
            transition_gate: Arc::new(DefaultModeTransitionGate::new()),
            preflight: Arc::clone(&preflight) as Arc<dyn ModePreflight>,
            kill_switch: Arc::new(MockKillSwitchPort {
                handle: KillSwitchHandle::default(),
            }),
            status_publisher: Arc::clone(&status_publisher),
        }));
        status_publisher.register(Arc::clone(&control));

        (control, mode_repo, preflight, events_rx)
    }

    #[tokio::test]
    #[ignore = "requires Docker"]
    async fn kill_switch_boot_reseeds_closed_when_missing() {
        let (pool, _container) = setup_pg().await;
        let db = pool.connection().clone();
        let repo = PgKillSwitchStateRepository::new(db.clone());
        system_kill_switch::Entity::delete_many()
            .exec(&db)
            .await
            .expect("clear kill-switch row");

        let snapshot = RuntimeSnapshot::bootstrap(&pool)
            .await
            .expect("bootstrap governance snapshot");
        assert_eq!(
            snapshot.kill_switch_handle.current(),
            KillSwitchState::Closed
        );

        let loaded = repo
            .load()
            .await
            .expect("load kill switch")
            .expect("row exists");
        assert_eq!(loaded.state, KillSwitchState::Closed);
        assert_eq!(loaded.id, SYSTEM_KILL_SWITCH_ID);
    }

    #[tokio::test]
    #[ignore = "requires Docker"]
    async fn switch_quant_mode_fail_closed_does_not_persist_on_preflight_fail() {
        let (pool, _container) = setup_pg().await;
        let db = pool.connection().clone();
        let pg = Arc::new(pool);
        let (control, mode_repo, preflight, events_rx) =
            build_mode_control(&db, pg, QuantRuntimeMode::ReportOnly, false).await;

        let err = control
            .switch_quant_mode(QuantRuntimeMode::SemiAuto, "operator", "upgrade")
            .await;
        assert!(err.is_err(), "preflight denial must fail closed");
        assert_eq!(preflight.runs.load(Ordering::SeqCst), 1);

        let persisted = mode_repo
            .load()
            .await
            .expect("load mode")
            .expect("singleton");
        assert_eq!(persisted.quant_runtime_mode, QuantRuntimeMode::ReportOnly);
        assert_eq!(control.quant_runtime_mode(), QuantRuntimeMode::ReportOnly);
        assert!(events_rx.try_recv().is_err(), "no WS event on deny");
    }

    #[tokio::test]
    #[ignore = "requires Docker"]
    async fn mode_downgrade_skips_business_preflight() {
        let (pool, _container) = setup_pg().await;
        let db = pool.connection().clone();
        let pg = Arc::new(pool);
        let (control, mode_repo, preflight, events_rx) =
            build_mode_control(&db, pg, QuantRuntimeMode::AutoExecution, false).await;

        let report = control
            .switch_quant_mode(QuantRuntimeMode::ReportOnly, "operator", "tighten")
            .await
            .expect("downgrade must succeed without preflight");
        assert!(report.preflight.is_none());
        assert_eq!(preflight.runs.load(Ordering::SeqCst), 0);
        assert_eq!(control.quant_runtime_mode(), QuantRuntimeMode::ReportOnly);
        assert_eq!(
            mode_repo
                .load()
                .await
                .expect("load")
                .expect("singleton")
                .quant_runtime_mode,
            QuantRuntimeMode::ReportOnly
        );
        let event = events_rx.recv_async().await.expect("status event");
        assert!(matches!(event, CoreEvent::SystemStatusChanged(_)));
    }

    #[tokio::test]
    #[ignore = "requires Docker"]
    async fn mode_switch_success_publishes_system_status_event() {
        let (pool, _container) = setup_pg().await;
        let db = pool.connection().clone();
        let pg = Arc::new(pool);
        let (control, _, _, events_rx) =
            build_mode_control(&db, pg, QuantRuntimeMode::ReportOnly, true).await;

        control
            .switch_quant_mode(QuantRuntimeMode::SemiAuto, "operator", "upgrade")
            .await
            .expect("upgrade succeeds");

        let event = events_rx.recv_async().await.expect("status event");
        match event {
            CoreEvent::SystemStatusChanged(status) => {
                assert_eq!(status.quant_runtime_mode, QuantRuntimeMode::SemiAuto);
            }
            other => panic!("expected SystemStatusChanged, got {other:?}"),
        }
    }

    #[tokio::test]
    #[ignore = "requires Docker"]
    async fn kill_switch_set_publishes_system_status_event() {
        let (pool, _container) = setup_pg().await;
        let db = pool.connection().clone();
        let pg = Arc::new(pool);
        let deploy = DeployConfig::default();
        let snapshot = RuntimeSnapshot::bootstrap(&pg).await.expect("bootstrap");
        let metrics = Arc::new(MetricsHub::new());
        let (events, events_rx) = CoreEventPublisher::bounded(8);
        let status_publisher = SystemStatusPublisher::new(events);
        let runtime_mode = snapshot.mode;
        let kill_switch_control = Arc::new(KillSwitchControl::new(
            snapshot.kill_switch_handle.clone(),
            snapshot.kill_switch_view,
            Arc::new(PgKillSwitchStateRepository::new(db.clone())),
            metrics,
            Arc::clone(&status_publisher),
        ));
        let kill_switch: Arc<dyn KillSwitchPort> =
            Arc::clone(&kill_switch_control) as Arc<dyn KillSwitchPort>;
        let control = Arc::new(QuantRuntimeControl::new(QuantRuntimeControlDeps {
            runtime_mode: runtime_mode.clone(),
            health_checker: operational_health_checker(pg, runtime_mode, &deploy),
            runtime_state_repo: PgSystemRuntimeStateRepository::new(db),
            transition_gate: Arc::new(DefaultModeTransitionGate::new()),
            preflight: Arc::new(CountingPreflight {
                runs: AtomicUsize::new(0),
                pass: true,
            }) as Arc<dyn ModePreflight>,
            kill_switch,
            status_publisher: Arc::clone(&status_publisher),
        }));
        status_publisher.register(Arc::clone(&control));

        kill_switch_control
            .set(SetKillSwitchCommand {
                target: KillSwitchState::ExecutionHalted,
                actor: "operator".to_owned(),
                reason: "halt".to_owned(),
                ack: false,
                latch: false,
            })
            .await
            .expect("set kill switch");

        let event = events_rx.recv_async().await.expect("status event");
        match event {
            CoreEvent::SystemStatusChanged(status) => {
                assert_eq!(status.kill_switch.state, KillSwitchState::ExecutionHalted);
                assert_eq!(status.operational_phase, OperationalPhase::Halted);
            }
            other => panic!("expected SystemStatusChanged, got {other:?}"),
        }
    }

    #[tokio::test]
    #[ignore = "requires Docker"]
    async fn report_only_forced_kill_switch_projects_degraded_not_halted() {
        let (pool, _container) = setup_pg().await;
        let db = pool.connection().clone();
        let pg = Arc::new(pool);
        let deploy = DeployConfig::default();
        let runtime_mode = RuntimeModeHandle::new(QuantRuntimeMode::ReportOnly);
        let (events, _rx) = CoreEventPublisher::bounded(1);
        let status_publisher = SystemStatusPublisher::new(events);
        let kill_switch_control = Arc::new(KillSwitchControl::new(
            KillSwitchHandle::new(KillSwitchState::ReportOnlyForced),
            KillSwitchView {
                state: KillSwitchState::ReportOnlyForced,
                requires_operator_ack: false,
                last_reason: "forced report-only".to_owned(),
                changed_by: "operator".to_owned(),
                changed_at: chrono::Utc::now(),
            },
            Arc::new(PgKillSwitchStateRepository::new(db.clone())),
            Arc::new(MetricsHub::new()),
            Arc::clone(&status_publisher),
        ));
        let control = Arc::new(QuantRuntimeControl::new(QuantRuntimeControlDeps {
            runtime_mode: runtime_mode.clone(),
            health_checker: operational_health_checker(pg, runtime_mode, &deploy),
            runtime_state_repo: PgSystemRuntimeStateRepository::new(db),
            transition_gate: Arc::new(DefaultModeTransitionGate::new()),
            preflight: Arc::new(CountingPreflight {
                runs: AtomicUsize::new(0),
                pass: true,
            }) as Arc<dyn ModePreflight>,
            kill_switch: kill_switch_control as Arc<dyn KillSwitchPort>,
            status_publisher,
        }));

        let status = control.system_status();
        assert!(
            matches!(status.operational_phase, OperationalPhase::Degraded { .. }),
            "report_only_forced must not map to Halted (reports still run)"
        );
        assert!(status.operational_phase.allows_report_generation());
        assert!(!status.operational_phase.allows_order_submission());
        assert_eq!(status.kill_switch.state, KillSwitchState::ReportOnlyForced);
    }
}

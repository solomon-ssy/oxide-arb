//! Durable cold-start lifecycle and capability derivation.

use std::sync::{
    Arc,
    atomic::{AtomicBool, Ordering},
};

use arc_swap::ArcSwap;
use async_trait::async_trait;
use chrono::{Duration, Utc};
use quant_pivot_error::{QuantError, QuantResult, control::ControlError, storage::StorageError};
use quant_pivot_models::{
    domain::{
        ActivateBootstrapRequest, ActivateBootstrapState, BootstrapPort, BootstrapView,
        CapabilityView, CoreEvent, CoreEventPublisher, PolicySnapshotPort, SystemCapabilities,
        SystemRuntimeStateInfo, SystemStatus,
    },
    enums::{
        execution::KillSwitchState,
        quant::QuantRuntimeMode,
        system::{BootstrapPhase, CapabilityReason},
    },
    runtime_config::validate_runtime_config,
};
use quant_pivot_repository::traits::{
    ModelRunRepository, PolicyRepository, RecommendationReportRepository,
    SystemRuntimeStateRepository,
};

pub struct BootstrapService {
    state: ArcSwap<BootstrapView>,
    state_repo: Arc<dyn SystemRuntimeStateRepository>,
    runtime_configs: Arc<dyn PolicyRepository>,
    runtime_config_apply: Arc<dyn PolicySnapshotPort>,
    model_runs: Arc<dyn ModelRunRepository>,
    reports: Arc<dyn RecommendationReportRepository>,
    events: CoreEventPublisher,
    phase_tx: tokio::sync::watch::Sender<BootstrapView>,
    capability_tx: tokio::sync::watch::Sender<SystemCapabilities>,
    last_status: ArcSwap<SystemStatus>,
    serving_evidence_present: AtomicBool,
}

impl BootstrapService {
    pub async fn initialize(
        state_repo: Arc<dyn SystemRuntimeStateRepository>,
        runtime_configs: Arc<dyn PolicyRepository>,
        runtime_config_apply: Arc<dyn PolicySnapshotPort>,
        model_runs: Arc<dyn ModelRunRepository>,
        reports: Arc<dyn RecommendationReportRepository>,
        events: CoreEventPublisher,
    ) -> QuantResult<Self> {
        let state = state_repo.begin_baseline_collection().await?;
        let initial_view = bootstrap_view(&state);
        let (phase_tx, _phase_rx) = tokio::sync::watch::channel(initial_view.clone());
        let initial_capabilities =
            SystemCapabilities::fail_closed(CapabilityReason::BootstrapInitializing);
        let (capability_tx, _capability_rx) = tokio::sync::watch::channel(initial_capabilities);
        Ok(Self {
            state: ArcSwap::from_pointee(initial_view),
            state_repo,
            runtime_configs,
            runtime_config_apply,
            model_runs,
            reports,
            events,
            phase_tx,
            capability_tx,
            last_status: ArcSwap::from_pointee(SystemStatus::bootstrap(
                QuantRuntimeMode::ReportOnly,
            )),
            serving_evidence_present: AtomicBool::new(false),
        })
    }

    fn store(&self, state: &SystemRuntimeStateInfo) -> BootstrapView {
        let view = bootstrap_view(state);
        self.state.store(Arc::new(view.clone()));
        self.phase_tx.send_replace(view.clone());
        let status = self.last_status.load_full();
        let serving_evidence = self.serving_evidence_present.load(Ordering::Acquire);
        self.publish_capabilities(self.derive_capabilities(&status, serving_evidence));
        view
    }

    fn derive_capabilities(
        &self,
        status: &SystemStatus,
        serving_subject: bool,
    ) -> SystemCapabilities {
        let bootstrap = self.view();
        let control_ready = bootstrap.phase != BootstrapPhase::Initializing;
        let catalog_ready = status.catalog.is_ready();
        let active = bootstrap.phase == BootstrapPhase::Active;

        let mut control_reasons = Vec::new();
        require(
            control_ready,
            CapabilityReason::BootstrapInitializing,
            &mut control_reasons,
        );

        let mut catalog_reasons = Vec::new();
        require(
            control_ready,
            CapabilityReason::ControlPlaneNotReady,
            &mut catalog_reasons,
        );
        require(
            catalog_ready,
            CapabilityReason::CatalogBaselineMissing,
            &mut catalog_reasons,
        );

        let mut research_reasons = catalog_reasons.clone();
        require(
            matches!(
                bootstrap.phase,
                BootstrapPhase::CollectingBaseline
                    | BootstrapPhase::AwaitingActivation
                    | BootstrapPhase::Active
            ),
            CapabilityReason::BootstrapNotCollecting,
            &mut research_reasons,
        );

        let mut report_reasons = Vec::new();
        require(
            active,
            CapabilityReason::BootstrapNotActive,
            &mut report_reasons,
        );
        require(
            catalog_ready,
            CapabilityReason::CatalogBaselineMissing,
            &mut report_reasons,
        );
        require(
            status.operational_phase.allows_report_generation(),
            CapabilityReason::OperationalPhaseBlocksReports,
            &mut report_reasons,
        );
        let runtime_config = self.runtime_config_apply.current();
        let has_active_model_pointer = runtime_config
            .model_routing
            .model
            .active_model_version_id
            .is_some()
            || !runtime_config
                .model_routing
                .model
                .category_model_pointers
                .is_empty();
        require(
            has_active_model_pointer,
            CapabilityReason::NoServingEvidence,
            &mut report_reasons,
        );

        let mut entry_reasons = report_reasons.clone();
        require(
            status.quant_runtime_mode.allows_order_submission(),
            CapabilityReason::RuntimeModeReportOnly,
            &mut entry_reasons,
        );
        require(
            status.kill_switch.state == KillSwitchState::Closed,
            CapabilityReason::KillSwitchBlocksEntries,
            &mut entry_reasons,
        );

        let mut order_reasons = entry_reasons.clone();
        require(
            status.operational_phase.allows_order_submission(),
            CapabilityReason::OperationalPhaseBlocksSubmission,
            &mut order_reasons,
        );

        let mut parity_reasons = Vec::new();
        require(
            active,
            CapabilityReason::BootstrapNotActive,
            &mut parity_reasons,
        );
        require(
            serving_subject,
            CapabilityReason::NoServingEvidence,
            &mut parity_reasons,
        );

        SystemCapabilities {
            revision: 0,
            control_plane_ready: capability(control_reasons),
            catalog_baseline_ready: capability(catalog_reasons),
            research_capture_enabled: capability(research_reasons),
            report_generation_eligible: capability(report_reasons),
            entry_admission_eligible: capability(entry_reasons),
            order_submission_eligible: capability(order_reasons),
            automatic_parity_eligible: capability(parity_reasons),
        }
    }

    fn publish_capabilities(&self, mut next: SystemCapabilities) -> SystemCapabilities {
        self.capability_tx.send_if_modified(|current| {
            if current.decisions_equal(&next) {
                return false;
            }
            next.revision = current.revision.saturating_add(1);
            *current = next;
            true
        });
        self.capability_tx.borrow().clone()
    }
}

const fn bootstrap_view(state: &SystemRuntimeStateInfo) -> BootstrapView {
    BootstrapView {
        phase: state.bootstrap_phase,
        bootstrap_contract_version: state.bootstrap_contract_version,
        state_revision: state.state_revision,
    }
}

const fn capability(reasons: Vec<CapabilityReason>) -> CapabilityView {
    CapabilityView {
        enabled: reasons.is_empty(),
        reasons,
    }
}

fn require(condition: bool, reason: CapabilityReason, reasons: &mut Vec<CapabilityReason>) {
    if !condition {
        reasons.push(reason);
    }
}

#[async_trait]
impl BootstrapPort for BootstrapService {
    fn view(&self) -> BootstrapView {
        self.state.load_full().as_ref().clone()
    }

    fn subscribe(&self) -> tokio::sync::watch::Receiver<BootstrapView> {
        self.phase_tx.subscribe()
    }

    fn capability_snapshot(&self) -> SystemCapabilities {
        self.capability_tx.borrow().clone()
    }

    fn subscribe_capabilities(&self) -> tokio::sync::watch::Receiver<SystemCapabilities> {
        self.capability_tx.subscribe()
    }

    fn refresh_operational_capabilities(&self, status: &SystemStatus) -> SystemCapabilities {
        self.last_status.store(Arc::new(status.clone()));
        self.publish_capabilities(self.derive_capabilities(
            status,
            self.serving_evidence_present.load(Ordering::Acquire),
        ))
    }

    async fn capabilities(&self, status: &SystemStatus) -> QuantResult<SystemCapabilities> {
        let now = Utc::now();
        let window_start = now - Duration::hours(24);
        let serving_subject = !self
            .reports
            .list_committed_between(window_start, now)
            .await?
            .is_empty()
            || !self
                .model_runs
                .list_succeeded_live_between(window_start, now)
                .await?
                .is_empty();
        self.serving_evidence_present
            .store(serving_subject, Ordering::Release);
        self.last_status.store(Arc::new(status.clone()));
        Ok(self.publish_capabilities(self.derive_capabilities(status, serving_subject)))
    }

    async fn mark_catalog_ready(&self) -> QuantResult<BootstrapView> {
        let state = self.state_repo.mark_catalog_baseline_ready().await?;
        Ok(self.store(&state))
    }

    async fn activate(
        &self,
        request: ActivateBootstrapRequest,
        actor: &str,
        acting_role: &str,
    ) -> QuantResult<BootstrapView> {
        if !request.report_only_forced_ack {
            return Err(QuantError::Control(ControlError::Precondition(
                "ReportOnlyForced acknowledgement is required".to_owned(),
            )));
        }
        let version = self.runtime_configs.load_current().await?.ok_or_else(|| {
            StorageError::not_found("decision_policy_snapshot", "active policy bundle")
        })?;
        let candidate = version.snapshot.clone();
        let validation = validate_runtime_config(&candidate);
        if validation.has_errors() {
            return Err(QuantError::config(validation.to_string()));
        }
        let prepared = self.runtime_config_apply.prepare(candidate).await?;
        let activated = self
            .state_repo
            .activate_bootstrap(ActivateBootstrapState {
                bootstrap_contract_version: request.bootstrap_contract_version,
                expected_state_revision: request.expected_state_revision,
                actor: actor.to_owned(),
                acting_role: acting_role.to_owned(),
                reason: request.reason,
                report_only_forced_ack: true,
            })
            .await?;
        prepared.publish()?;
        self.events.publish(CoreEvent::ConfigActivated {
            version_id: version.decision_policy_snapshot_id.to_string(),
        });
        Ok(self.store(&activated.state))
    }
}

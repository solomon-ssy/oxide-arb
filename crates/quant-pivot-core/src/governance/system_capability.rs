//! Live capability derivation from assembled services and operational facts.

use std::sync::{
    Arc,
    atomic::{AtomicBool, Ordering},
};

use arc_swap::ArcSwap;
use async_trait::async_trait;
use chrono::{Duration, Utc};
use quant_pivot_error::{QuantError, QuantResult};
use quant_pivot_models::{
    domain::{
        api::{CapabilityView, SystemCapabilities},
        governance::SystemStatus,
        ports::{PolicySnapshotPort, SystemCapabilityPort},
    },
    enums::{execution::KillSwitchState, quant::QuantRuntimeMode, system::CapabilityReason},
    runtime_config::BuyModelRoute,
};
use quant_pivot_repository::traits::{ModelRunRepository, RecommendationReportRepository};
use tokio::sync::watch::{self, Receiver, Sender};

pub struct SystemCapabilityService {
    runtime_config: Arc<dyn PolicySnapshotPort>,
    model_runs: Arc<dyn ModelRunRepository>,
    reports: Arc<dyn RecommendationReportRepository>,
    capability_tx: Sender<SystemCapabilities>,
    last_status: ArcSwap<SystemStatus>,
    serving_evidence_present: AtomicBool,
}

impl SystemCapabilityService {
    #[must_use]
    pub fn new(
        runtime_config: Arc<dyn PolicySnapshotPort>,
        model_runs: Arc<dyn ModelRunRepository>,
        reports: Arc<dyn RecommendationReportRepository>,
    ) -> Self {
        let (capability_tx, _rx) = watch::channel(SystemCapabilities::fail_closed(
            CapabilityReason::ControlPlaneNotReady,
        ));
        Self {
            runtime_config,
            model_runs,
            reports,
            capability_tx,
            last_status: ArcSwap::from_pointee(SystemStatus::bootstrap(
                QuantRuntimeMode::ReportOnly,
            )),
            serving_evidence_present: AtomicBool::new(false),
        }
    }

    fn derive(&self, status: &SystemStatus, serving_subject: bool) -> SystemCapabilities {
        let catalog_ready = status.catalog.is_ready();
        let control_reasons = Vec::new();

        let mut catalog_reasons = Vec::new();
        require(
            catalog_ready,
            CapabilityReason::CatalogBaselineMissing,
            &mut catalog_reasons,
        );
        let research_reasons = catalog_reasons.clone();

        let mut report_reasons = Vec::new();
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
        let runtime_config = self.runtime_config.current();
        let has_active_model_pointer =
            BuyModelRoute::try_from(&runtime_config.recommendation.selection)
                .ok()
                .and_then(|route| {
                    runtime_config
                        .model_routing
                        .model
                        .active_pointer(route)
                        .ok()
                })
                .is_some();
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

    fn publish(&self, mut next: SystemCapabilities) -> SystemCapabilities {
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
impl SystemCapabilityPort for SystemCapabilityService {
    fn capability_snapshot(&self) -> SystemCapabilities {
        self.capability_tx.borrow().clone()
    }

    fn subscribe_capabilities(&self) -> Receiver<SystemCapabilities> {
        self.capability_tx.subscribe()
    }

    fn refresh_operational_capabilities(&self, status: &SystemStatus) -> SystemCapabilities {
        self.last_status.store(Arc::new(status.clone()));
        self.publish(self.derive(
            status,
            self.serving_evidence_present.load(Ordering::Acquire),
        ))
    }

    async fn capabilities(&self, status: &SystemStatus) -> QuantResult<SystemCapabilities> {
        let now = Utc::now();
        let window_start = now
            .checked_sub_signed(Duration::hours(24))
            .ok_or_else(|| QuantError::config("capability evidence window underflow"))?;
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
        Ok(self.publish(self.derive(status, serving_subject)))
    }
}

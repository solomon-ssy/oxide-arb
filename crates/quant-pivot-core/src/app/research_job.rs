//! Durable research-job engine.
//!
//! The engine is the shared spine between the HTTP enqueue path
//! ([`CoreResearchJobPort`](super::ports::research_job::CoreResearchJobPort)) and the
//! [`ResearchJobWorker`](super::research_job_worker): both hold a clone of
//! [`ResearchJobEngine`] so cancellation tokens, the ledger repository, the event bus,
//! and the boot-epoch instance id are shared.

use std::sync::Arc;

use dashmap::DashMap;
use tokio_util::sync::CancellationToken;
use uuid::Uuid;

use quant_pivot_models::{
    domain::{CoreEvent, CoreEventPublisher, MaterializationRunEvent, ResearchJobInfo},
    enums::quant::{ResearchJobKind, ResearchJobStatus},
    types::{ResearchJobId, WorkerId},
};
use quant_pivot_repository::traits::ResearchJobRepository;

/// Shared, cheaply-cloneable handle wiring the job ledger, event bus, live
/// cancellation-token registry, and this process's boot epoch id.
#[derive(Clone)]
pub struct ResearchJobEngine {
    repo: Arc<dyn ResearchJobRepository>,
    events: CoreEventPublisher,
    cancels: Arc<DashMap<ResearchJobId, CancellationToken>>,
    instance_id: WorkerId,
}

impl ResearchJobEngine {
    /// Wire a fresh engine for this process (mints a boot-epoch instance id).
    #[must_use]
    pub fn new(repo: Arc<dyn ResearchJobRepository>, events: CoreEventPublisher) -> Self {
        Self {
            repo,
            events,
            cancels: Arc::new(DashMap::new()),
            instance_id: WorkerId::from_v7(),
        }
    }

    /// The shared ledger repository.
    #[must_use]
    pub fn repo(&self) -> &Arc<dyn ResearchJobRepository> {
        &self.repo
    }

    /// This process's lease-owner id (boot epoch).
    #[must_use]
    pub const fn instance_id(&self) -> &WorkerId {
        &self.instance_id
    }

    /// Register a live cancellation token for a running job.
    pub fn register_cancel(&self, job_id: &ResearchJobId, token: CancellationToken) {
        self.cancels.insert(job_id.clone(), token);
    }

    /// Drop a job's cancellation token once it has left the running state.
    pub fn clear_cancel(&self, job_id: &ResearchJobId) {
        self.cancels.remove(job_id);
    }

    /// Signal an in-flight running job to stop cooperatively.
    pub(crate) fn signal_cancel(&self, job_id: &ResearchJobId) -> bool {
        self.cancels.get(job_id).is_some_and(|token| {
            token.cancel();
            true
        })
    }

    /// Publish a job-scoped progress event without a full ledger projection.
    pub fn publish_progress(
        &self,
        job_id: &ResearchJobId,
        kind: ResearchJobKind,
        result_ref: Option<Uuid>,
        status: ResearchJobStatus,
        phase: Option<String>,
        pct: Option<f64>,
    ) {
        let run_id = result_ref.map_or_else(|| job_id.to_string(), |uuid| uuid.to_string());
        self.events
            .publish(CoreEvent::MaterializationRun(MaterializationRunEvent::job(
                job_id.to_string(),
                run_id,
                kind.into(),
                status.into(),
                phase,
                pct,
            )));
    }

    /// Publish a `materialization.run_update` lifecycle/progress event.
    pub fn publish(&self, info: &ResearchJobInfo, phase: Option<String>, pct: Option<f64>) {
        let run_id = info
            .result()
            .map_or_else(|| info.job_id.to_string(), |result| result.id.to_string());
        self.events
            .publish(CoreEvent::MaterializationRun(MaterializationRunEvent::job(
                info.job_id.to_string(),
                run_id,
                info.kind.into(),
                info.status.into(),
                phase,
                pct,
            )));
    }
}

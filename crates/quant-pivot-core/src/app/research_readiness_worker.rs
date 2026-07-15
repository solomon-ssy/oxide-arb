//! Periodic signed operational-readiness evidence producer.

use std::{sync::Arc, time::Duration};

use quant_pivot_error::{QuantResult, research::ResearchError};
use quant_pivot_models::types::minimum_raw_retention_days;
use quant_pivot_repository::traits::ResearchReadinessEvidenceRepository;

use crate::service::research_readiness::{EvidenceAttestor, ResearchReadinessEvidenceProducer};

use super::{AppContext, task_id::TaskId, task_registry::AppRunner};

const CAPTURE_INTERVAL: Duration = Duration::from_mins(5);

impl AppContext {
    pub fn register_research_readiness_evidence_worker(
        &self,
        runner: &mut AppRunner,
    ) -> QuantResult<()> {
        let attestor = EvidenceAttestor::from_config(&self.config.research.evidence_attestation)?;
        if attestor.is_none() {
            tracing::warn!(
                "research readiness evidence worker is disabled; fit preflight remains blocked"
            );
            return Ok(());
        }
        let required_days = minimum_raw_retention_days()
            .map_err(|detail| ResearchError::ValidationMethodology { detail })?;
        let producer = Arc::new(ResearchReadinessEvidenceProducer::new(
            Arc::clone(&self.infra.repos.research_readiness)
                as Arc<dyn ResearchReadinessEvidenceRepository>,
            Arc::clone(&self.research.artifact_store),
            Arc::clone(&self.infra.ch),
            attestor,
        ));
        runner.spawn(TaskId::ResearchReadinessEvidenceWorker, move |token| async move {
            loop {
                if token.is_cancelled() {
                    return;
                }
                match producer.capture(required_days).await {
                    Ok(true) => tracing::info!(
                        required_days,
                        "signed research readiness evidence captured"
                    ),
                    Ok(false) => {}
                    Err(error) => tracing::error!(
                        %error,
                        "research readiness evidence capture failed; previous evidence is not extended"
                    ),
                }
                tokio::select! {
                    () = token.cancelled() => return,
                    () = tokio::time::sleep(CAPTURE_INTERVAL) => {}
                }
            }
        });
        Ok(())
    }
}

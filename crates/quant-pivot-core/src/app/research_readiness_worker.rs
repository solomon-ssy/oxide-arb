//! Periodic signed operational-readiness evidence producer.

use std::{sync::Arc, time::Duration};

use quant_pivot_error::{QuantResult, research::ResearchError};
use quant_pivot_models::{enums::system::CapabilityId, types::minimum_raw_retention_days};
use quant_pivot_repository::traits::{
    CatalogLedgerRepository, ClobMarketInfoRepository, ResearchReadinessEvidenceRepository,
};

use super::{
    AppContext, capability_gate::wait_for_capability, task_id::TaskId, task_registry::AppRunner,
};
use crate::service::research_readiness::{
    EvidenceAttestor, EvidenceScopeIdentity, ResearchReadinessCaptureOutcome,
    ResearchReadinessEvidenceProducer,
};

const CAPTURE_INTERVAL: Duration = Duration::from_mins(5);

impl AppContext {
    pub fn register_readiness_worker(&self, runner: &mut AppRunner) -> QuantResult<()> {
        let attestor = EvidenceAttestor::from_config(&self.config.research.evidence_attestation)?;
        if attestor.is_none() {
            tracing::warn!(
                "research readiness evidence worker is disabled; fit preflight remains blocked"
            );
            return Ok(());
        }
        let required_days = minimum_raw_retention_days()
            .map_err(|detail| ResearchError::ValidationMethodology { detail })?;
        let scope = EvidenceScopeIdentity::from_config(
            &self.config.db.clickhouse,
            &self.config.research.artifact_store,
        )?;
        let producer = Arc::new(ResearchReadinessEvidenceProducer::new(
            Arc::clone(&self.infra.repos.research_readiness)
                as Arc<dyn ResearchReadinessEvidenceRepository>,
            Arc::clone(&self.research.artifact_store),
            Arc::clone(&self.infra.ch),
            Arc::clone(&self.data.catalog_ledger_repo) as Arc<dyn CatalogLedgerRepository>,
            Arc::clone(&self.infra.repos.clob_market_info) as Arc<dyn ClobMarketInfoRepository>,
            attestor,
            scope,
        )?);
        let capabilities = Arc::clone(&self.governance.capabilities);
        runner.spawn(TaskId::ResearchReadinessEvidenceWorker, move |token| async move {
            loop {
                if !wait_for_capability(
                    Arc::clone(&capabilities),
                    CapabilityId::ResearchCaptureEnabled,
                    &token,
                )
                .await
                {
                    return;
                }
                match producer.capture(required_days).await {
                    Ok(ResearchReadinessCaptureOutcome::Captured(capture))
                        if capture.retention_proven =>
                    {
                        tracing::info!(
                            required_days,
                            measured_history_days = ?capture.measured_history_days,
                            missing_binding_count = capture.missing_binding_count,
                            unready_binding_count = capture.unready_binding_count,
                            retention_evidence_id = %capture.retention.evidence_id,
                            retention_scope_hash = %capture.retention.scope_hash,
                            retention_observed_at = %capture.retention.observed_at,
                            retention_expires_at = %capture.retention.expires_at,
                            latency_evidence_id = %capture.latency.evidence_id,
                            latency_scope_hash = %capture.latency.scope_hash,
                            latency_observed_at = %capture.latency.observed_at,
                            latency_expires_at = %capture.latency.expires_at,
                            "proven signed research readiness evidence captured"
                        );
                    }
                    Ok(ResearchReadinessCaptureOutcome::Captured(capture)) => tracing::info!(
                        required_days,
                        retention_proven = capture.retention_proven,
                        measured_history_days = ?capture.measured_history_days,
                        missing_binding_count = capture.missing_binding_count,
                        unready_binding_count = capture.unready_binding_count,
                        retention_evidence_id = %capture.retention.evidence_id,
                        retention_scope_hash = %capture.retention.scope_hash,
                        retention_observed_at = %capture.retention.observed_at,
                        retention_expires_at = %capture.retention.expires_at,
                        latency_evidence_id = %capture.latency.evidence_id,
                        latency_scope_hash = %capture.latency.scope_hash,
                        latency_observed_at = %capture.latency.observed_at,
                        latency_expires_at = %capture.latency.expires_at,
                        "signed research readiness evidence captured without a proven retention runway"
                    ),
                    Ok(ResearchReadinessCaptureOutcome::Disabled) => tracing::warn!(
                        "research readiness evidence producer is disabled; fit preflight remains blocked"
                    ),
                    Err(failure) => tracing::error!(
                        error_code = failure.source.code(),
                        phase = %failure.phase,
                        kind = %failure.kind,
                        source_error = %failure.source,
                        "research readiness evidence capture failed; previous evidence is not extended"
                    ),
                }
                let mut capabilities = capabilities.subscribe_capabilities();
                tokio::select! {
                    () = token.cancelled() => return,
                    changed = capabilities.changed() => {
                        if changed.is_err() {
                            return;
                        }
                    }
                    () = tokio::time::sleep(CAPTURE_INTERVAL) => {}
                }
            }
        });
        Ok(())
    }
}

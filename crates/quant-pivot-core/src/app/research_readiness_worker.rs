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
    EvidenceAttestor, EvidenceScopeIdentity, ResearchReadinessEvidenceProducer,
};

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

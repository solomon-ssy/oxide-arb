//! Execution admission-engine wiring.
//!
//! Admission is purely internal: it has no web port and no worker. The 05.4
//! entry-execution dispatcher builds an [`AdmissionInput`](crate::execution::AdmissionInput)
//! via the [`AdmissionInputBuilder`] and evaluates it through the
//! [`ExecutionAdmissionEngine`] before submitting an order. These factory
//! methods assemble both from the shared infra / data / account / governance
//! planes.

use std::sync::Arc;

use quant_pivot_models::domain::DataQualityPort;
use quant_pivot_repository::{
    postgres::{
        PgCapitalAllocationRepository, PgModelRegistryRepository, PgRecommendationReportRepository,
        PgRecommendationRepository, PgReconciliationRepository, PgRuntimeConfigVersionRepository,
    },
    traits::{
        CapitalAllocationRepository, ModelRegistryRepository, RecommendationReportRepository,
        RecommendationRepository, ReconciliationRepository, RuntimeConfigVersionRepository,
    },
};

use super::AppContext;
use crate::execution::{
    AdmissionInputBuilder, AdmissionInputBuilderDeps, DefaultAdmissionEngine,
    ExecutionAdmissionEngine,
};

impl AppContext {
    /// Build the read-only admission input builder over the data / account /
    /// governance planes (consumed by the 05.4 dispatcher).
    #[must_use]
    pub fn build_admission_input_builder(&self) -> AdmissionInputBuilder {
        let pg = self.infra.pg.connection();
        AdmissionInputBuilder::new(AdmissionInputBuilderDeps {
            recommendations: Arc::new(PgRecommendationRepository::new(pg.clone()))
                as Arc<dyn RecommendationRepository>,
            reports: Arc::new(PgRecommendationReportRepository::new(pg.clone()))
                as Arc<dyn RecommendationReportRepository>,
            model_registry: Arc::new(PgModelRegistryRepository::new(pg.clone()))
                as Arc<dyn ModelRegistryRepository>,
            reconciliation: Arc::new(PgReconciliationRepository::new(pg.clone()))
                as Arc<dyn ReconciliationRepository>,
            capital: Arc::new(PgCapitalAllocationRepository::new(pg.clone()))
                as Arc<dyn CapitalAllocationRepository>,
            config_versions: Arc::new(PgRuntimeConfigVersionRepository::new(pg.clone()))
                as Arc<dyn RuntimeConfigVersionRepository>,
            account_factory: Arc::clone(&self.account.provider_factory),
            book_store: Arc::clone(&self.data.book_store),
            data_quality: Arc::clone(&self.data.data_quality) as Arc<dyn DataQualityPort>,
            config: self.runtime_config(),
            runtime_mode: self.runtime_mode(),
            kill_switch: self.kill_switch_handle(),
        })
    }

    /// Build the deterministic, fixed-order admission engine.
    #[must_use]
    pub fn build_admission_engine(&self) -> Arc<dyn ExecutionAdmissionEngine> {
        Arc::new(DefaultAdmissionEngine::new(Arc::clone(&self.infra.metrics)))
            as Arc<dyn ExecutionAdmissionEngine>
    }
}

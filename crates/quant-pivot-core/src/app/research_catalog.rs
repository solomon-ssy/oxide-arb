//! Core implementation of [`ResearchCatalogPort`] — the read-only research
//! catalog surface backing the operator workbench.
//!
//! Thin delegation over the research repositories already assembled in
//! [`ResearchBundle`]; every method pages a ledger or loads a by-id row and never
//! mutates state.

use std::{
    collections::{BTreeMap, HashMap},
    sync::Arc,
};

use async_trait::async_trait;
use chrono::{Duration, Utc};
use quant_pivot_error::{QuantError, QuantResult};
use quant_pivot_models::{
    domain::{
        BacktestReportInfo, BacktestReportListQuery, CollinearPairView, ComparisonReportListQuery,
        FactorCollinearityView, FactorDefinitionInfo, FactorDefinitionListQuery,
        ModelComparisonReportInfo, ModelSpecInfo, ModelSpecListQuery, ModelVersionInfo,
        ModelVersionListQuery, Paginated, ResearchCatalogPort, TrainingDatasetInfo,
        TrainingDatasetListQuery, pagination::PageRequest,
    },
    enums::quant::PublicationStatus,
    types::FactorDefinitionId,
};
use quant_pivot_repository::traits::{
    BacktestReportRepository, FactorRepository, ModelComparisonReportRepository,
    ModelRegistryRepository, TrainingDatasetRepository,
};
use rust_decimal::Decimal;

use crate::app::bundles::ResearchBundle;
use quant_pivot_research::factors::{
    FactorCollinearityAnalyzer, FactorName, FactorObservationMatrix,
};

/// Ceiling on how many published factor definitions the collinearity analysis
/// pulls in one page (the generic factor set is a dozen; this is generous).
const COLLINEARITY_DEFINITION_LIMIT: u64 = 500;

/// Read-only research catalog port wired from the research bundle repositories.
pub struct CoreResearchCatalogPort {
    datasets: Arc<dyn TrainingDatasetRepository>,
    models: Arc<dyn ModelRegistryRepository>,
    backtests: Arc<dyn BacktestReportRepository>,
    comparisons: Arc<dyn ModelComparisonReportRepository>,
    factors: Arc<dyn FactorRepository>,
}

impl CoreResearchCatalogPort {
    /// Assemble the port from an already-wired research bundle.
    #[must_use]
    pub fn from_research(research: &ResearchBundle) -> Self {
        Self {
            datasets: Arc::clone(&research.training_dataset_repo),
            models: Arc::clone(&research.model_registry_repo),
            backtests: Arc::clone(&research.backtest_report_repo),
            comparisons: Arc::clone(&research.comparison_report_repo),
            factors: Arc::clone(&research.factor_repo),
        }
    }
}

#[async_trait]
impl ResearchCatalogPort for CoreResearchCatalogPort {
    async fn list_training_datasets(
        &self,
        query: TrainingDatasetListQuery,
    ) -> QuantResult<Paginated<TrainingDatasetInfo>> {
        self.datasets.page(query).await.map_err(QuantError::from)
    }

    async fn list_models(
        &self,
        query: ModelVersionListQuery,
    ) -> QuantResult<Paginated<ModelVersionInfo>> {
        self.models
            .page_versions(query)
            .await
            .map_err(QuantError::from)
    }

    async fn list_model_specs(
        &self,
        query: ModelSpecListQuery,
    ) -> QuantResult<Paginated<ModelSpecInfo>> {
        self.models
            .page_specs(query)
            .await
            .map_err(QuantError::from)
    }

    async fn list_backtest_reports(
        &self,
        query: BacktestReportListQuery,
    ) -> QuantResult<Paginated<BacktestReportInfo>> {
        self.backtests.page(query).await.map_err(QuantError::from)
    }

    async fn list_comparison_reports(
        &self,
        query: ComparisonReportListQuery,
    ) -> QuantResult<Paginated<ModelComparisonReportInfo>> {
        self.comparisons.page(query).await.map_err(QuantError::from)
    }

    async fn list_factors(
        &self,
        query: FactorDefinitionListQuery,
    ) -> QuantResult<Paginated<FactorDefinitionInfo>> {
        self.factors
            .page_definitions(query)
            .await
            .map_err(QuantError::from)
    }

    async fn find_factor(
        &self,
        factor_definition_id: &FactorDefinitionId,
    ) -> QuantResult<Option<FactorDefinitionInfo>> {
        self.factors
            .find_definition(factor_definition_id)
            .await
            .map_err(QuantError::from)
    }

    async fn factor_collinearity(
        &self,
        lookback_secs: u64,
        threshold: Decimal,
    ) -> QuantResult<FactorCollinearityView> {
        // The published factor definitions define the columns of the panel.
        let definitions = self
            .factors
            .page_definitions(FactorDefinitionListQuery {
                factor_family: None,
                scope: None,
                status: Some(PublicationStatus::Published),
                page: PageRequest {
                    page: 1,
                    size: COLLINEARITY_DEFINITION_LIMIT,
                },
            })
            .await
            .map_err(QuantError::from)?;

        // Stable column order by factor name for a deterministic matrix.
        let mut definitions = definitions.items;
        definitions.sort_by(|left, right| left.name.cmp(&right.name));
        let column_index: HashMap<FactorDefinitionId, usize> = definitions
            .iter()
            .enumerate()
            .map(|(index, definition)| (definition.factor_definition_id.clone(), index))
            .collect();
        let factor_names: Vec<FactorName> = definitions
            .iter()
            .map(|definition| FactorName::new(definition.name.clone()))
            .collect();

        let until = Utc::now();
        let from = until - Duration::seconds(i64::try_from(lookback_secs).unwrap_or(i64::MAX));
        let ids: Vec<FactorDefinitionId> = definitions
            .iter()
            .map(|definition| definition.factor_definition_id.clone())
            .collect();
        let values = self
            .factors
            .recent_values(&ids, from, until)
            .await
            .map_err(QuantError::from)?;

        // Pivot into one observation row per (as_of, market): each column is the
        // factor's normalized score (only scored values participate).
        let mut observations: BTreeMap<(i64, String), Vec<Option<Decimal>>> = BTreeMap::new();
        for value in values {
            let Some(column) = column_index.get(&value.factor_definition_id) else {
                continue;
            };
            let Some(score) = value.normalized_score else {
                continue;
            };
            let key = (value.as_of.timestamp_millis(), value.market_id.to_string());
            let row = observations
                .entry(key)
                .or_insert_with(|| vec![None; factor_names.len()]);
            row[*column] = Some(score.inner());
        }
        let observation_count = observations.len();
        let panel = FactorObservationMatrix {
            factors: factor_names,
            rows: observations.into_values().collect(),
        };
        let report = FactorCollinearityAnalyzer::analyze(&panel, threshold);

        Ok(FactorCollinearityView {
            factors: panel
                .factors
                .iter()
                .map(|name| name.as_str().to_owned())
                .collect(),
            matrix: report.matrix,
            violations: report
                .violations
                .into_iter()
                .map(|pair| CollinearPairView {
                    left: pair.left.as_str().to_owned(),
                    right: pair.right.as_str().to_owned(),
                    correlation: pair.correlation,
                })
                .collect(),
            threshold: report.threshold,
            observation_count,
            lookback_secs,
        })
    }
}

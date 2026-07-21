//! Core implementation of [`ResearchCatalogPort`] — the read-only research
//! catalog surface backing the operator workbench.
//!
//! Thin delegation over the research repositories already assembled in
//! [`ResearchBundle`]; every method pages a ledger or loads a by-id row and never
//! mutates state.

use std::{
    collections::{BTreeMap, HashMap, HashSet},
    sync::Arc,
};

use async_trait::async_trait;
use chrono::{Duration, Utc};
use quant_pivot_error::{QuantError, QuantResult};
use quant_pivot_models::{
    domain::{
        api::{
            BacktestPathSetListQuery, BacktestReportListQuery, CollinearPairView,
            ComparisonReportListQuery, FactorCollinearitySource, FactorCollinearityView,
            FactorDefinitionListQuery, ModelPublishedCatalogQuery, ModelSpecListQuery,
            ModelVersionListQuery, PublishedModelOptionView, TrainingDatasetListQuery,
        },
        pagination::{PageRequest, Paginated},
        ports::ResearchCatalogPort,
        quant::{
            BacktestPathSetInfo, BacktestReportInfo, FactorDefinitionInfo,
            ModelComparisonReportInfo, ModelSpecInfo, ModelVersionInfo, TrainingDatasetInfo,
        },
    },
    enums::{common::MarketCategory, quant::PublicationStatus},
    types::{FactorDefinitionId, MarketId, Probability, stable_name::FactorName},
};
use quant_pivot_repository::traits::{
    BacktestPathSetRepository, BacktestReportRepository, FactorRepository, MarketRepository,
    ModelComparisonReportRepository, ModelRegistryRepository, TrainingDatasetRepository,
};
use quant_pivot_research::factors::{
    FactorCollinearityAnalyzer, FactorObservationMatrix, neutralize_by_group,
};
use rust_decimal::Decimal;

use crate::app::bundles::ResearchBundle;

/// Ceiling on how many published factor definitions the collinearity analysis
/// pulls in one page (the generic factor set is a dozen; this is generous).
const COLLINEARITY_DEFINITION_LIMIT: u64 = 500;

/// Read-only research catalog port wired from the research bundle repositories.
pub struct CoreResearchCatalogPort {
    datasets: Arc<dyn TrainingDatasetRepository>,
    models: Arc<dyn ModelRegistryRepository>,
    backtests: Arc<dyn BacktestReportRepository>,
    path_sets: Arc<dyn BacktestPathSetRepository>,
    comparisons: Arc<dyn ModelComparisonReportRepository>,
    factors: Arc<dyn FactorRepository>,
    markets: Arc<dyn MarketRepository>,
}

impl CoreResearchCatalogPort {
    /// Assemble the port from an already-wired research bundle.
    #[must_use]
    pub fn from_research(research: &ResearchBundle) -> Self {
        Self {
            datasets: Arc::clone(&research.training_dataset_repo),
            models: Arc::clone(&research.model_registry_repo),
            backtests: Arc::clone(&research.backtest_report_repo),
            path_sets: Arc::clone(&research.backtest_path_set_repo),
            comparisons: Arc::clone(&research.comparison_report_repo),
            factors: Arc::clone(&research.factor_repo),
            markets: Arc::clone(&research.market_repo),
        }
    }

    /// Resolve a per-row category group key for the collinearity panel: each
    /// distinct market category is assigned a stable integer id; a market absent
    /// from the registry (or with no category) is `None` (its own bucket, never
    /// borrowing another category's mean).
    async fn category_groups(&self, row_markets: &[String]) -> QuantResult<Vec<Option<i64>>> {
        let ids: Vec<MarketId> = {
            let mut seen: HashSet<&str> = HashSet::new();
            row_markets
                .iter()
                .filter(|market| seen.insert(market.as_str()))
                .map(|market| MarketId::new(market.as_str()))
                .collect()
        };
        let infos = self
            .markets
            .find_by_ids(&ids)
            .await
            .map_err(QuantError::from)?;
        let mut category_ids: HashMap<MarketCategory, i64> = HashMap::new();
        let mut market_category: HashMap<String, i64> = HashMap::new();
        for info in infos {
            let Some(category) = info.categories.first().copied() else {
                continue;
            };
            let next = i64::try_from(category_ids.len()).unwrap_or(i64::MAX);
            let id = *category_ids.entry(category).or_insert(next);
            market_category.insert(info.market_id.to_string(), id);
        }
        Ok(row_markets
            .iter()
            .map(|market| market_category.get(market).copied())
            .collect())
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

    async fn list_published_model_options(
        &self,
        query: ModelPublishedCatalogQuery,
    ) -> QuantResult<Vec<PublishedModelOptionView>> {
        self.models
            .list_published_catalog(query.side, query.category)
            .await
            .map_err(QuantError::from)
            .map(|rows| {
                rows.into_iter()
                    .map(|row| PublishedModelOptionView {
                        model_version_id: row.model_version_id,
                        model_spec_id: row.model_spec_id,
                        spec_name: row.spec_name,
                        version: row.version,
                        artifact_hash: row.artifact_hash,
                        model_family: row.model_family,
                        category_scope: row.category_scope,
                        published_at: row.published_at,
                    })
                    .collect()
            })
    }

    async fn list_backtest_reports(
        &self,
        query: BacktestReportListQuery,
    ) -> QuantResult<Paginated<BacktestReportInfo>> {
        self.backtests.page(query).await.map_err(QuantError::from)
    }

    async fn list_backtest_path_sets(
        &self,
        query: BacktestPathSetListQuery,
    ) -> QuantResult<Paginated<BacktestPathSetInfo>> {
        self.path_sets.page(query).await.map_err(QuantError::from)
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
        source: FactorCollinearitySource,
        neutralize_by_category: bool,
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

        // Pivot into one observation row per (as_of, market). The panel source
        // selects the plane: `Raw` uses the pre-normalization value (the correct
        // plane for detecting same-signal factors, unbiased by mixing Rank vs
        // z-score); `Normalized` uses the scored `[0, 1]` value.
        let mut observations: BTreeMap<(i64, String), Vec<Option<Decimal>>> = BTreeMap::new();
        for value in values {
            let Some(column) = column_index.get(&value.factor_definition_id) else {
                continue;
            };
            let cell = match source {
                FactorCollinearitySource::Raw => value.raw_value,
                FactorCollinearitySource::Normalized => {
                    value.normalized_score.map(Probability::inner)
                }
            };
            let Some(cell) = cell else {
                continue;
            };
            let key = (
                value.decision_at.timestamp_millis(),
                value.market_id.to_string(),
            );
            let row = observations
                .entry(key)
                .or_insert_with(|| vec![None; factor_names.len()]);
            row[*column] = Some(cell);
        }
        let observation_count = observations.len();
        // Keep each row's market alongside it so the category-neutralize operator
        // can residualize against market category.
        let mut rows = Vec::with_capacity(observation_count);
        let mut row_markets = Vec::with_capacity(observation_count);
        for ((_, market), row) in observations {
            rows.push(row);
            row_markets.push(market);
        }
        let panel = FactorObservationMatrix {
            factors: factor_names,
            rows,
        };
        let panel = if neutralize_by_category {
            let groups = self.category_groups(&row_markets).await?;
            neutralize_by_group(&panel, &groups)
        } else {
            panel
        };
        let report = FactorCollinearityAnalyzer::analyze(&panel, threshold)?;

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
            panel_source: source,
        })
    }
}

//! Immutable factor-definition helpers for integration tests.

use quant_pivot_error::{QuantError, QuantResult};
use quant_pivot_models::{
    domain::quant::NewFactorDefinition,
    enums::common::MarketCategory,
    runtime_config::{DomainConfig, FactorsConfig, FeaturesConfig},
    types::ResearchFeatureContract,
};
use quant_pivot_repository::traits::FactorRepository;
use quant_pivot_research::factors::FactorEngine;

/// Register every enabled immutable factor-definition revision.
///
/// # Errors
///
/// Propagates definition projection or persistence failures.
pub async fn register_all_factor_definitions(
    factor_repo: &dyn FactorRepository,
    factors: &FactorsConfig,
    features: &FeaturesConfig,
    domain: &DomainConfig,
    feature_contract: ResearchFeatureContract,
    category_scope: Option<MarketCategory>,
) -> QuantResult<()> {
    let engine = FactorEngine::for_model_scope(
        factors,
        features,
        domain,
        feature_contract,
        category_scope,
        None,
    );
    let definitions = engine
        .serving_plane()?
        .definitions()
        .iter()
        .cloned()
        .map(NewFactorDefinition::from)
        .collect();
    factor_repo
        .register_definitions(definitions)
        .await
        .map_err(QuantError::from)?;
    Ok(())
}

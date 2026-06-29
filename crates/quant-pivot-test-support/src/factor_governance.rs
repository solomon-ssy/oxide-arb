//! Factor-definition governance helpers for integration tests.

use quant_pivot_error::QuantResult;
use quant_pivot_models::{
    enums::quant::PublicationStatus,
    runtime_config::{FactorsConfig, FeaturesConfig},
};
use quant_pivot_repository::traits::FactorRepository;
use quant_pivot_research::factors::FactorEngine;

/// Upsert every enabled factor definition and publish it so the factor plane gate passes.
///
/// Test / local bootstrap helper — production requires explicit operator publish.
///
/// # Errors
///
/// Propagates definition projection, persistence, or publish failures.
pub async fn publish_all_factor_definitions(
    factor_repo: &dyn FactorRepository,
    factors: &FactorsConfig,
    features: &FeaturesConfig,
) -> QuantResult<()> {
    let engine = FactorEngine::new(factors, features);
    for spec in &engine.factor_set().definitions {
        let definition = spec.try_to_new(features.feature_schema_version)?;
        let row = factor_repo
            .create_definition(definition)
            .await
            .map_err(quant_pivot_error::QuantError::from)?;
        if row.status != PublicationStatus::Published {
            factor_repo
                .publish_definition(&row.factor_definition_id)
                .await
                .map_err(quant_pivot_error::QuantError::from)?;
        }
    }
    Ok(())
}

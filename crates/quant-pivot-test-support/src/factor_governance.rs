//! Factor-definition governance helpers for integration tests.

use quant_pivot_error::{QuantError, QuantResult};
use quant_pivot_models::{
    enums::quant::PublicationStatus,
    runtime_config::{DomainConfig, FactorsConfig, FeaturesConfig},
};
use quant_pivot_repository::traits::FactorRepository;
use quant_pivot_research::factors::FactorEngine;

/// Upsert every enabled factor definition as `Draft` **without** publishing.
///
/// Mirrors the production `POST /research/factors/register` bootstrap step so
/// tests can exercise the "registered but not yet Published" gate state.
///
/// # Errors
///
/// Propagates definition projection or persistence failures.
pub async fn register_all_factor_definitions(
    factor_repo: &dyn FactorRepository,
    factors: &FactorsConfig,
    features: &FeaturesConfig,
    domain: &DomainConfig,
) -> QuantResult<()> {
    let engine = FactorEngine::new(factors, features, domain, None);
    for spec in &engine.factor_set().definitions {
        let definition = spec.try_to_new(features.feature_schema_version)?;
        factor_repo
            .create_definition(definition)
            .await
            .map_err(QuantError::from)?;
    }
    Ok(())
}

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
    domain: &DomainConfig,
) -> QuantResult<()> {
    let engine = FactorEngine::new(factors, features, domain, None);
    for spec in &engine.factor_set().definitions {
        let definition = spec.try_to_new(features.feature_schema_version)?;
        let row = factor_repo
            .create_definition(definition)
            .await
            .map_err(QuantError::from)?;
        if row.status != PublicationStatus::Published {
            factor_repo
                .publish_definition(&row.factor_definition_id)
                .await
                .map_err(QuantError::from)?;
        }
    }
    Ok(())
}

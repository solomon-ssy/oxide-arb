//! Category-scoped factor-native model validation.
//!
//! Scope is validated from the sealed serving contract's category, exact factor
//! plane, and revision-bound head IDs. Human-readable factor names are never an
//! authorization or routing input.

use std::collections::HashSet;

use quant_pivot_error::{QuantError, QuantResult, research::ResearchError};
use quant_pivot_models::{
    enums::{common::MarketCategory, factor::FactorFamily},
    types::{
        FactorDefinitionId,
        factor::{FactorDefinitionRef, FactorServingPlane},
        model_serving::ModelServingContract,
    },
};
use rust_decimal::Decimal;

use crate::model::factor_heads::FactorHeadSpec;

/// Validate category routing against the exact plane and revision-bound head.
pub fn validate_category_scope(
    contract: &ModelServingContract,
    head: &FactorHeadSpec,
) -> QuantResult<()> {
    contract
        .validate()
        .map_err(|error| invalid_scope(format!("invalid serving contract: {error}")))?;
    let plane = &contract.bindings().factors.plane;
    head.validate(plane)?;
    let category = contract.bindings().model.category_scope;
    let crypto_ids = domain_factor_ids(plane, FactorFamily::DomainCrypto);
    let weather_ids = domain_factor_ids(plane, FactorFamily::DomainWeather);
    match category {
        Some(MarketCategory::Crypto) => {
            reject_domain_mix(
                MarketCategory::Crypto,
                &weather_ids,
                FactorFamily::DomainWeather,
            )?;
            require_active_domain(MarketCategory::Crypto, &crypto_ids, head)
        }
        Some(MarketCategory::Weather) => {
            reject_domain_mix(
                MarketCategory::Weather,
                &crypto_ids,
                FactorFamily::DomainCrypto,
            )?;
            require_active_domain(MarketCategory::Weather, &weather_ids, head)
        }
        Some(category) => {
            reject_domain_mix(category, &crypto_ids, FactorFamily::DomainCrypto)?;
            reject_domain_mix(category, &weather_ids, FactorFamily::DomainWeather)
        }
        None => Ok(()),
    }
}

fn domain_factor_ids(
    plane: &FactorServingPlane,
    family: FactorFamily,
) -> HashSet<FactorDefinitionId> {
    plane
        .definitions()
        .iter()
        .filter(|revision| revision.definition().family == family)
        .map(FactorDefinitionRef::factor_definition_id)
        .collect()
}

fn require_active_domain(
    category: MarketCategory,
    domain_ids: &HashSet<FactorDefinitionId>,
    head: &FactorHeadSpec,
) -> QuantResult<()> {
    if domain_ids.is_empty() {
        return Err(invalid_scope(format!(
            "{category:?} serving contract has no matching domain factor revision"
        )));
    }
    let active = head.alpha_weights.iter().any(|weight| {
        domain_ids.contains(&weight.factor_definition_id) && weight.weight > Decimal::ZERO
    }) || head.context_weights.iter().any(|weight| {
        domain_ids.contains(&weight.factor_definition_id)
            && (weight.coverage_weight > Decimal::ZERO || weight.penalty_strength > Decimal::ZERO)
    });
    if active {
        Ok(())
    } else {
        Err(invalid_scope(format!(
            "{category:?} serving contract has no active matching domain head revision"
        )))
    }
}

fn reject_domain_mix(
    category: MarketCategory,
    domain_ids: &HashSet<FactorDefinitionId>,
    family: FactorFamily,
) -> QuantResult<()> {
    if domain_ids.is_empty() {
        Ok(())
    } else {
        Err(invalid_scope(format!(
            "{category:?} serving contract cannot carry {family:?} factor revisions"
        )))
    }
}

fn invalid_scope(detail: String) -> QuantError {
    ResearchError::InvalidModelArtifact { detail }.into()
}

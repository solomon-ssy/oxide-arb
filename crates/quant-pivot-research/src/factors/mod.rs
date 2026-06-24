//! Factor plane: the [`FactorComputer`] contract, the [`FactorEngine`], the nine
//! generic factors, normalization, and the domain skeleton.
//!
//! A [`FactorComputer`] turns a [`FeatureVector`](crate::features::FeatureVector)
//! into a per-market [`RawFactor`] (pure, no normalization, no cross-section).
//! The [`FactorEngine`] owns the governed [`FactorRegistry`], applies the
//! (possibly cross-sectional) [`NormalizationSpec`], and resolves the runtime
//! confidence floor / `missing_factor_policy` into a [`MarketFactorOutcome`].
//!
//! A factor score is **not** a recommendation score — it is an explainable model
//! input. Cross-sectional normalization (`ZScore` / `Rank`) is only valid through
//! [`FactorEngine::compute_all_batch`]; the single-market path refuses to
//! fabricate a pseudo cross-section.

mod computer;
mod domain;
mod generic;
mod normalize;
mod persistence;
mod registry;
mod value;
mod writer;

#[cfg(test)]
mod acceptance;

pub use computer::{FactorComputer, FactorEngine};
pub use domain::{DomainFactorComputer, DomainFactorRegistry};
pub use generic::{factor_definition_id, generic_factors};
pub use normalize::{Normalized, normalize_column, to_probability_clamped};
pub use persistence::FactorValueInsertContext;
pub use registry::FactorRegistry;
pub use value::{
    FactorDefinitionSpec, FactorDriver, FactorEligibility, FactorExplanation, FactorFamily,
    FactorName, FactorOutputKind, FactorQualityGate, FactorSet, FactorValue, MarketFactorOutcome,
    NormalizationClampAudit, NormalizationSpec, RawFactor, ScoredFactor,
};
pub use writer::factor_events;

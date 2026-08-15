//! Factor plane: the [`FactorComputer`] contract, the [`FactorEngine`], the
//! generic factors, the platform-internal structural factors, cross-sectional
//! normalization, and collinearity analysis.
//!
//! A [`FactorComputer`] turns a [`FeatureVector`](crate::features::FeatureVector)
//! into a per-market [`RawFactor`] (pure, no normalization, no cross-section).
//! The [`FactorEngine`] owns the governed [`FactorRegistry`], runs the
//! cross-section normalization stage (or the small-cross-section policy), and
//! resolves the runtime confidence floor plus each frozen factor definition's
//! requiredness into a [`MarketFactorOutcome`].
//!
//! A factor score is **not** a recommendation score — it is an explainable model
//! input. There is no silent neutral: a too-small or degenerate cross-section
//! yields [`NormalizedFactor::Indeterminate`] with a recorded reason.

mod collinearity;
mod computer;
mod domain;
mod generic;
pub mod names;
mod normalize;
pub mod persistence;
mod reference;
mod registry;
mod semantics;
mod structural;
pub mod value;
mod writer;

pub(crate) use quant_pivot_models::types::stable_name::FactorName;

#[cfg(test)]
mod acceptance;

pub use collinearity::{
    CollinearPair, FactorCollinearityAnalyzer, FactorCollinearityReport, FactorObservationMatrix,
    neutralize_by_group,
};
pub use computer::{FactorComputer, FactorEngine};
pub use domain::{DomainFactorRegistry, crypto_domain_factors, weather_domain_factors};
pub use generic::{bootstrap_trade_factors, generic_factors};
pub use normalize::{
    CrossSectionalNormalizer, MinMaxNormalizer, NormalizationClampAudit, NormalizationStats,
    NormalizedFactor, RankNormalizer, RawFactorColumn, WinsorizedZScoreNormalizer,
    resolve_normalizer,
};
pub use persistence::FactorValueInsertContext;
pub use reference::{FrozenReferenceCdf, FrozenReferenceQuantiles};
pub use registry::FactorRegistry;
pub use structural::structural_factors;
pub use value::{
    FactorEligibility, FactorValue, MarketFactorOutcome, RawFactor, RawFactorEligibility,
    ScoredFactor,
};
pub use writer::factor_events;

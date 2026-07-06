//! Per-market feature-availability oracle.
//!
//! Translates a model's required-feature set into per-market eligibility using
//! only the frozen [`MarketCandidate`] facts and each feature's
//! [`SourceRequirement`] — a cheap structural check that never builds features,
//! so the selector stays a pure function. This replaces selection's earlier
//! fail-closed placeholder (which excluded every market once any feature was
//! required) with a real, evidence-grounded judgment.

use quant_pivot_models::domain::MarketCandidate;

use crate::features::{
    schema::{FeatureSchema, SourceRequirement},
    value::FeatureName,
};

/// Decides whether a market can supply the features a model requires.
pub struct FeatureAvailabilityOracle<'a> {
    schema: &'a FeatureSchema,
}

impl<'a> FeatureAvailabilityOracle<'a> {
    /// Build an oracle over a governed schema.
    #[must_use]
    pub const fn new(schema: &'a FeatureSchema) -> Self {
        Self { schema }
    }

    /// The subset of `required` features this market cannot supply.
    ///
    /// An unknown required name (not in the schema) is treated as unavailable —
    /// the system never claims to provide a feature it does not define.
    #[must_use]
    pub fn missing_required(
        &self,
        candidate: &MarketCandidate,
        required: &[FeatureName],
    ) -> Vec<FeatureName> {
        required
            .iter()
            .filter(|name| !self.is_available(candidate, name))
            .cloned()
            .collect()
    }

    /// Whether a single required feature is computable for this market.
    fn is_available(&self, candidate: &MarketCandidate, name: &FeatureName) -> bool {
        self.schema
            .by_name(name)
            .is_some_and(|spec| source_available(candidate, spec.source_requirement))
    }
}

/// Whether a candidate carries the evidence a source requirement needs.
const fn source_available(candidate: &MarketCandidate, requirement: SourceRequirement) -> bool {
    match requirement {
        // Gamma metadata is always present for a catalog-backed candidate.
        SourceRequirement::GammaMetadata => true,
        // Book-derived and windowed fact features need a live, two-sided book;
        // facts only flow while a book is published. Neg-risk sibling-leg
        // aggregates are platform-computable from the same book plane (a binary
        // market simply yields a `NotApplicable` value, not an ineligible market).
        SourceRequirement::PublishedL2Book
        | SourceRequirement::MicrostructureWindow
        | SourceRequirement::NegRiskSiblingLegs => has_two_sided_book(candidate),
    }
}

/// Whether the candidate has a fresh, two-sided published book.
const fn has_two_sided_book(candidate: &MarketCandidate) -> bool {
    candidate.book_age_ms.is_some()
        && candidate.best_bid.is_some()
        && candidate.best_ask.is_some()
        && !candidate.empty
}

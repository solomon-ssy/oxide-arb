//! Vertical (domain) factor skeleton.
//!
//! The [`DomainFactorComputer`] trait is the contract real vertical factors will
//! implement once 03.2's domain features carry external data. It refines
//! [`FactorComputer`] (the same `compute_raw` contract) with a [`DomainFamily`]
//! tag so factors route by category. 3.3 ships only the contract + an additive
//! by-category registry: with no domain factors registered, missing vertical
//! data never blocks the generic report (see `03.x` §4).

use std::collections::BTreeMap;
use std::sync::Arc;

use quant_pivot_models::enums::common::MarketCategory;

use crate::{factors::computer::FactorComputer, vertical::DomainFamily};

/// A vertical-specific factor computer.
///
/// Domain factors read `domain.{vertical}.*` features from the feature vector;
/// a missing domain feature yields a low-confidence raw factor (never a silent
/// zero), so an additive generic report degrades gracefully.
pub trait DomainFactorComputer: FactorComputer {
    /// The vertical this factor serves.
    fn family(&self) -> DomainFamily;
}

/// By-category registry of domain factors (additive capability).
///
/// Empty by default in 3.3. [`Self::for_category`] returns only the factors of
/// the vertical a category routes to, so generic markets are unaffected and a
/// category with no registered domain factors simply gets none.
#[derive(Default)]
pub struct DomainFactorRegistry {
    by_family: BTreeMap<DomainFamily, Vec<Arc<dyn DomainFactorComputer>>>,
}

impl DomainFactorRegistry {
    /// An empty registry.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Register a domain factor under its vertical.
    pub fn register(&mut self, computer: Arc<dyn DomainFactorComputer>) {
        self.by_family
            .entry(computer.family())
            .or_default()
            .push(computer);
    }

    /// The domain factors enabled for a market category (empty when the category
    /// has no vertical or no registered factors).
    #[must_use]
    pub fn for_category(&self, category: MarketCategory) -> &[Arc<dyn DomainFactorComputer>] {
        match DomainFamily::for_category(category) {
            Some(family) => self.by_family.get(&family).map_or(&[], Vec::as_slice),
            None => &[],
        }
    }
}

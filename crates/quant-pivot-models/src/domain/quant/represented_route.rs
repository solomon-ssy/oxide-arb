//! Canonical Route-set identity for one global report decision boundary.

use std::collections::BTreeSet;

use quant_pivot_error::hashing::CanonicalDigestError;
use schemars::JsonSchema;
use sea_orm::FromJsonQueryResult;
use serde::{Deserialize, Serialize};
use thiserror::Error;

use crate::{
    enums::common::MarketCategory, hashing::CanonicalDigest, runtime_config::BuyModelRoute,
    types::ContentHash,
};

const ROUTE_SET_DIGEST_DOMAIN: &str = "quant-pivot/represented-route-set";
const ROUTE_SET_SCHEMA_VERSION: u32 = 1;

/// Ordered, deduplicated model Routes represented by immutable market eligibility.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema, FromJsonQueryResult)]
#[serde(deny_unknown_fields)]
pub struct RepresentedRouteSet {
    pub routes: Vec<BuyModelRoute>,
    pub digest: ContentHash,
}

/// One Route-owned immutable contract hash in canonical Route-set order.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct RouteContractHash {
    pub route: BuyModelRoute,
    pub content_hash: ContentHash,
}

/// Aggregate compatibility digests bound into one promoted joint-scenario artifact.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct RouteCompatibilityDigests {
    pub serving_contract_digest: ContentHash,
    pub calibration_contract_digest: ContentHash,
    pub trade_policy_contract_digest: ContentHash,
}

/// Invalid Route-owned contract set.
#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub enum RouteCompatibilityError {
    #[error("{contract} contract Routes differ from the represented Route set")]
    RouteSetMismatch { contract: &'static str },
    #[error("canonical {contract} contract digest failed: {detail}")]
    Digest {
        contract: &'static str,
        detail: String,
    },
}

impl RouteCompatibilityDigests {
    /// Aggregate all three Route-owned contract families with domain separation.
    pub fn try_new(
        routes: &RepresentedRouteSet,
        serving: &[RouteContractHash],
        calibration: &[RouteContractHash],
        trade_policy: &[RouteContractHash],
    ) -> Result<Self, RouteCompatibilityError> {
        Ok(Self {
            serving_contract_digest: contract_digest(routes, "serving", serving)?,
            calibration_contract_digest: contract_digest(routes, "calibration", calibration)?,
            trade_policy_contract_digest: contract_digest(routes, "trade-policy", trade_policy)?,
        })
    }
}

fn contract_digest(
    routes: &RepresentedRouteSet,
    contract: &'static str,
    hashes: &[RouteContractHash],
) -> Result<ContentHash, RouteCompatibilityError> {
    if hashes.len() != routes.routes.len()
        || hashes
            .iter()
            .zip(&routes.routes)
            .any(|(binding, route)| binding.route != *route)
    {
        return Err(RouteCompatibilityError::RouteSetMismatch { contract });
    }
    CanonicalDigest::content_hash_typed(
        &format!("quant-pivot/route-{contract}-contracts"),
        ROUTE_SET_SCHEMA_VERSION,
        hashes,
    )
    .map_err(|error| RouteCompatibilityError::Digest {
        contract,
        detail: error.to_string(),
    })
}

impl RepresentedRouteSet {
    /// Resolve the configured Route universe. An empty category list is the
    /// canonical policy spelling for every supported category.
    pub fn from_enabled_categories(
        enabled_categories: &[MarketCategory],
    ) -> Result<Self, CanonicalDigestError> {
        if enabled_categories.is_empty() {
            Self::from_categories(MarketCategory::ALL_VARIANTS)
        } else {
            Self::from_categories(enabled_categories.iter().copied())
        }
    }

    /// Build the canonical Route set from eligible market categories.
    pub fn from_categories(
        categories: impl IntoIterator<Item = MarketCategory>,
    ) -> Result<Self, CanonicalDigestError> {
        Self::from_routes(categories.into_iter().map(BuyModelRoute::from))
    }

    /// Build a sorted, unique Route set and its domain-separated digest.
    pub fn from_routes(
        routes: impl IntoIterator<Item = BuyModelRoute>,
    ) -> Result<Self, CanonicalDigestError> {
        let routes = routes
            .into_iter()
            .collect::<BTreeSet<_>>()
            .into_iter()
            .collect::<Vec<_>>();
        let digest = CanonicalDigest::content_hash_typed(
            ROUTE_SET_DIGEST_DOMAIN,
            ROUTE_SET_SCHEMA_VERSION,
            &routes,
        )?;
        Ok(Self { routes, digest })
    }

    #[must_use]
    pub fn contains(&self, route: BuyModelRoute) -> bool {
        self.routes.binary_search(&route).is_ok()
    }

    #[must_use]
    pub const fn is_empty(&self) -> bool {
        self.routes.is_empty()
    }
}

impl From<MarketCategory> for BuyModelRoute {
    fn from(category: MarketCategory) -> Self {
        match category {
            MarketCategory::Crypto => Self::Crypto,
            MarketCategory::Weather => Self::Weather,
            MarketCategory::Geopolitics
            | MarketCategory::Sports
            | MarketCategory::Politics
            | MarketCategory::Finance
            | MarketCategory::Tech
            | MarketCategory::Culture
            | MarketCategory::Economics
            | MarketCategory::Other => Self::Pooled,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::RepresentedRouteSet;
    use crate::{enums::common::MarketCategory, runtime_config::BuyModelRoute};

    #[test]
    fn category_order_preserves_routes() {
        let left = RepresentedRouteSet::from_categories([
            MarketCategory::Weather,
            MarketCategory::Sports,
            MarketCategory::Crypto,
        ])
        .expect("route set");
        let right = RepresentedRouteSet::from_categories([
            MarketCategory::Crypto,
            MarketCategory::Weather,
            MarketCategory::Politics,
            MarketCategory::Sports,
        ])
        .expect("route set");
        assert_eq!(left, right);
        assert_eq!(
            left.routes,
            vec![
                BuyModelRoute::Pooled,
                BuyModelRoute::Crypto,
                BuyModelRoute::Weather,
            ]
        );
    }

    #[test]
    fn empty_categories_cover_routes() {
        let routes = RepresentedRouteSet::from_enabled_categories(&[]).expect("route set");
        assert_eq!(
            routes.routes,
            vec![
                BuyModelRoute::Pooled,
                BuyModelRoute::Crypto,
                BuyModelRoute::Weather,
            ]
        );
    }
}

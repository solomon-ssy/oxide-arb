//! Feature plane: the [`FeatureBuilder`] contract, the governed schema, the
//! null-policy engine, and the concrete builders.
//!
//! One feature *definition* serves two execution backends behind a single
//! [`PitView`]: live (current `BookStore` via
//! [`PointInTimeDataSource`](quant_pivot_models::domain::PointInTimeDataSource))
//! and historical (3.5's [`PitQueryEngine`](crate::pit::PitQueryEngine)). Both
//! resolve into source-agnostic [`ResolvedBook`] / [`ResolvedMarketContext`] and
//! a pre-fetched [`MarketWindowSnapshot`], so the same builder produces an
//! identical [`FeatureVector`] online and offline.

mod availability;
mod book;
mod builder;
mod domain;
mod market;
mod microstructure;
mod null_policy;
mod persistence;
mod resolved;
mod schema;
mod stats;
mod timeseries;
mod value;
mod writer;

#[cfg(test)]
mod acceptance;

pub use availability::FeatureAvailabilityOracle;
pub use builder::{
    ConfiguredFeatureBuilder, FeatureComputeCtx, FeatureGroupBuilder, RawFeature, ResolvedInputs,
};
pub use domain::{DomainFeatureBuilder, DomainFeatureSkeleton};
pub use null_policy::{NullDecision, NullPolicyEngine};
pub use resolved::{
    MarketWindowSnapshot, MicrostructureBucket, ResolvedBook, ResolvedMarketContext,
};
pub use schema::{
    FeatureSchema, FeatureSpec, FeatureUnit, NullPolicy, PitRule, SourceRequirement, StalenessRule,
};
pub use value::{
    EvidenceSourceKind, EvidenceSourceRef, FeatureName, FeatureValue, FeatureValueKind,
    FeatureVector, NullReason, SubstitutionAudit,
};
pub use writer::feature_events;

use std::time::Duration;

use async_trait::async_trait;
use chrono::{DateTime, Utc};
use quant_pivot_error::QuantResult;
use quant_pivot_models::{
    domain::PointInTimeDataSource,
    runtime_config::{DataQualityConfig, FeaturesConfig},
    types::{MarketId, SchemaVersion, TokenId},
};

use crate::{pit::PitQueryEngine, selection::SelectedMarket};

/// Builds a point-in-time [`FeatureVector`] for a selected market.
///
/// Implementations must be point-in-time correct: never read a fact published
/// after `as_of - source_delay`, and never silently substitute zero for a
/// missing input (the null policy decides).
#[async_trait]
pub trait FeatureBuilder: Send + Sync {
    /// Schema version this builder produces.
    fn schema_version(&self) -> SchemaVersion;

    /// Build the feature vector for one market under the given PIT view.
    async fn build(&self, input: FeatureBuildInput<'_>) -> QuantResult<FeatureVector>;
}

/// Inputs to a single feature build, all borrowed from frozen snapshots.
pub struct FeatureBuildInput<'a> {
    /// The selected market to build features for.
    pub market: &'a SelectedMarket,
    /// Decision time to compute features as of.
    pub as_of: DateTime<Utc>,
    /// Visibility delay applied to source facts (no look-ahead).
    pub source_delay: Duration,
    /// Features the active model requires (drives critical-missing rejection).
    pub required_features: &'a [FeatureName],
    /// Point-in-time data view (live or historical).
    pub pit: PitView<'a>,
    /// Pre-fetched windowed microstructure history for the primary token.
    pub window: &'a MarketWindowSnapshot,
    /// Frozen feature configuration snapshot.
    pub config: &'a FeaturesConfig,
    /// Frozen data-quality configuration snapshot.
    pub data_quality: &'a DataQualityConfig,
}

/// A unified point-in-time view that hides the live vs. historical source split
/// from feature builders, so one definition runs identically online and offline.
#[derive(Clone, Copy)]
pub enum PitView<'a> {
    /// Live source: current `BookStore` / `MarketRegistry` state (Phase 2).
    Live(&'a dyn PointInTimeDataSource),
    /// Historical source: ClickHouse-backed PIT resolution (3.5).
    Historical(&'a dyn PitQueryEngine),
}

impl PitView<'_> {
    /// Resolve the book for `token_id` visible at `as_of`, normalized.
    ///
    /// # Errors
    ///
    /// Propagates the historical engine's query errors. The live arm is
    /// infallible (in-memory read) and never errors.
    pub async fn resolve_book(
        &self,
        token_id: &TokenId,
        as_of: DateTime<Utc>,
    ) -> QuantResult<Option<ResolvedBook>> {
        match self {
            Self::Live(source) => Ok(source
                .book_snapshot(token_id, as_of)
                .map(|snapshot| ResolvedBook::from_live(token_id, &snapshot, as_of))),
            Self::Historical(engine) => Ok(engine
                .book_at(token_id, as_of)
                .await?
                .map(ResolvedBook::from)),
        }
    }

    /// Resolve the market context for `market_id` visible at `as_of`, normalized.
    ///
    /// # Errors
    ///
    /// Propagates the historical engine's query errors.
    pub async fn resolve_market(
        &self,
        market_id: &MarketId,
        as_of: DateTime<Utc>,
    ) -> QuantResult<Option<ResolvedMarketContext>> {
        match self {
            Self::Live(source) => Ok(source
                .market_context(market_id, as_of)
                .map(|info| ResolvedMarketContext::from_live(&info))),
            Self::Historical(engine) => Ok(engine
                .market_at(market_id, as_of)
                .await?
                .map(ResolvedMarketContext::from)),
        }
    }
}

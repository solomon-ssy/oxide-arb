//! Feature plane: the [`FeatureBuilder`] contract and its compute-domain value
//! types.
//!
//! One feature *definition* serves two execution backends behind a single
//! [`PitView`]: live (current `BookStore` via
//! [`PointInTimeDataSource`](quant_pivot_models::domain::PointInTimeDataSource))
//! and historical (3.5's [`PitQueryEngine`](crate::pit::PitQueryEngine)). The
//! builders, schema registry, and null-policy engine land in 3.2; 3.0 fixes the
//! trait + value contract.

mod schema;
mod value;

pub use schema::FeatureSchema;
pub use value::{
    EvidenceSourceKind, EvidenceSourceRef, FeatureName, FeatureValue, FeatureVector, NullReason,
};

use std::time::Duration;

use async_trait::async_trait;
use chrono::{DateTime, Utc};
use quant_pivot_error::QuantResult;
use quant_pivot_models::{
    domain::PointInTimeDataSource,
    runtime_config::{DataQualityConfig, FeaturesConfig},
    types::SchemaVersion,
};

use crate::{pit::PitQueryEngine, selection::SelectedMarket};

/// Builds a point-in-time [`FeatureVector`] for a selected market.
///
/// Implementations must be point-in-time correct: never read a fact published
/// after `as_of - source_delay`, and never silently substitute zero for a
/// missing input (the null policy decides — see 3.2).
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
    /// Frozen feature configuration snapshot.
    pub config: &'a FeaturesConfig,
    /// Frozen data-quality configuration snapshot.
    pub data_quality: &'a DataQualityConfig,
}

/// A unified point-in-time view that hides the live vs. historical source split
/// from feature builders, so one definition runs identically online and offline.
pub enum PitView<'a> {
    /// Live source: current `BookStore` / `MarketRegistry` state (Phase 2).
    Live(&'a dyn PointInTimeDataSource),
    /// Historical source: ClickHouse-backed PIT resolution (3.5).
    Historical(&'a dyn PitQueryEngine),
}

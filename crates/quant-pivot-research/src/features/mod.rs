//! Feature plane: the [`FeatureBuilder`] contract, the governed schema, the
//! null-policy engine, and the concrete builders.
//!
//! Every feature build resolves through the single durable
//! [`PointInTimeSnapshotSource`] contract.
//! Serving and research therefore project from the same catalog/book snapshots;
//! the live `BookStore` is not a decision input.

mod availability;
mod builder;
mod decision_capture;
pub mod domain;
pub mod generic;
pub mod names;
mod null_policy;
mod persistence;
mod resolved;
mod scalar;
mod schema;
mod value;
mod writer;

#[cfg(test)]
mod acceptance;

use async_trait::async_trait;
pub use availability::FeatureAvailabilityOracle;
pub use builder::{
    ConfiguredFeatureBuilder, FeatureComputeCtx, FeatureGroupBuilder, FeatureSourceWindows,
    RawFeature, ResolvedInputs,
};
pub use decision_capture::{
    MarketDecisionCapture, MarketDecisionCaptureInput, RejectedMarketDraft, ResolvedMarketBundle,
    capture_market_decision, draft_data_quality_snapshot,
};
pub use domain::{
    CryptoDomainFeatureBuilder, DomainComputeCtx, DomainFeatureBuilder, DomainSliceData,
    DomainSliceDataRef, DomainSliceInputs, WeatherDomainFeatureBuilder,
};
pub use null_policy::{NullDecision, NullPolicyEngine};
use quant_pivot_error::QuantResult;
#[cfg(test)]
pub(crate) use quant_pivot_models::types::{DecisionSnapshotEvidence, DomainFeatureSlice};
pub(crate) use quant_pivot_models::{
    domain::data_plane::DecisionBoundary,
    enums::feature::{EvidenceSourceKind, FeatureValueKind},
    runtime_config::{DataQualityConfig, FeaturesConfig},
    types::{
        CatalogDecisionRef, DecisionCaptureEvidence, EvidenceSourceRef, FeatureCell,
        FeatureCellState, FeatureStaleness, FeatureValue, NullReason, SchemaVersion,
        stable_name::FeatureName,
    },
};
pub use resolved::{
    FinalizedExecutionWindowSnapshot, MarketWindowSnapshot, MicrostructureBucket, ResolvedBook,
    ResolvedMarketContext,
};
pub use scalar::{feature_scalar, finite_f64};
pub use schema::{
    AuthoringFeatureCatalog, ExecutableFeatureSchema, FeatureSpec, FeatureUnit, NullPolicy,
    PitRule, SourceRequirement, StalenessRule,
};
pub use value::FeatureVector;
pub use writer::feature_events;

use crate::{pit::PointInTimeSnapshotSource, selection::SelectedMarket};

/// Builds a point-in-time [`FeatureVector`] for a selected market.
///
/// Implementations must be point-in-time correct: never read a fact beyond the
/// already-derived source cutoff in `DecisionBoundary`, and never silently
/// substitute zero for a missing input (the null policy decides).
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
    /// Sole decision-time and per-source visibility contract.
    pub boundary: &'a DecisionBoundary,
    /// Features the active model requires (drives fail-closed rejection).
    pub required_features: &'a [FeatureName],
    /// Durable point-in-time snapshot source.
    pub pit: &'a dyn PointInTimeSnapshotSource,
    /// Pre-fetched windowed microstructure history for the primary token.
    pub window: &'a MarketWindowSnapshot,
    /// Pre-fetched finalized execution-participant history for the primary token.
    pub execution_history: &'a FinalizedExecutionWindowSnapshot,
    /// Pre-fetched domain-slice inputs (present only for markets whose
    /// category maps to an enabled vertical with a resolved linkage).
    pub domain: Option<&'a DomainSliceInputs>,
    /// Frozen feature configuration snapshot.
    pub config: &'a FeaturesConfig,
    /// Frozen data-quality configuration snapshot.
    pub data_quality: &'a DataQualityConfig,
}

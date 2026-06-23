//! Market selection plane: the [`MarketSelector`] contract and its
//! compute-domain snapshot types.
//!
//! Online closure entry (3.1): a deterministic, replayable, hashable
//! [`MarketSelectionSnapshot`] of which markets enter a research/report round.
//! The 7-filter pipeline and the Postgres repository land in 3.1; 3.0 fixes the
//! trait + snapshot contract.

use async_trait::async_trait;
use chrono::{DateTime, Utc};
use quant_pivot_error::QuantResult;
use quant_pivot_models::{
    enums::common::MarketCategory,
    runtime_config::{DataQualityConfig, SelectionConfig},
    types::{
        ContentHash, EventId, MarketId, MarketSelectionId, RuntimeConfigVersionId,
        SelectionExclusionSummary, TokenId, Usd,
    },
};
use serde::{Deserialize, Serialize};

use crate::features::{EvidenceSourceRef, FeatureName};

/// Builds a deterministic market selection snapshot from frozen config and
/// data-quality inputs.
#[async_trait]
pub trait MarketSelector: Send + Sync {
    /// Build the snapshot for one research/report round.
    async fn build_snapshot(
        &self,
        request: MarketSelectionBuildRequest,
    ) -> QuantResult<MarketSelectionSnapshot>;
}

/// Frozen inputs to a selection build.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct MarketSelectionBuildRequest {
    /// Decision time.
    pub as_of: DateTime<Utc>,
    /// Config version governing this selection.
    pub runtime_config_version_id: RuntimeConfigVersionId,
    /// Frozen selection-policy snapshot.
    pub selection: SelectionConfig,
    /// Frozen data-quality snapshot.
    pub data_quality: DataQualityConfig,
    /// Feature availability the active model requires.
    pub model_requirements: ModelFeatureRequirements,
    /// Source visibility delay, in seconds.
    pub source_delay_secs: u64,
}

/// Feature availability requirements imposed by the active model on selection.
///
/// Hash via [`crate::hashing::ResearchHasher::model_feature_requirements`] so
/// `required_features` order does not affect the selector digest.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
pub struct ModelFeatureRequirements {
    /// Features that must be available for a market to be eligible.
    pub required_features: Vec<FeatureName>,
}

/// A deterministic, hashable snapshot of selected and excluded markets.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct MarketSelectionSnapshot {
    /// Snapshot id.
    pub market_selection_id: MarketSelectionId,
    /// Decision time.
    pub as_of: DateTime<Utc>,
    /// Governing config version.
    pub runtime_config_version_id: RuntimeConfigVersionId,
    /// Canonical selector hash (same inputs → same hash).
    pub selector_hash: ContentHash,
    /// Included markets.
    pub included: Vec<SelectedMarket>,
    /// Excluded markets with reasons.
    pub excluded: Vec<ExcludedMarket>,
    /// Aggregate exclusion summary.
    pub exclusion_summary: SelectionExclusionSummary,
}

/// A market that passed every filter.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SelectedMarket {
    /// Market id.
    pub market_id: MarketId,
    /// Owning event, when known.
    pub event_id: Option<EventId>,
    /// Market category.
    pub category: MarketCategory,
    /// Primary outcome token.
    pub primary_token_id: TokenId,
    /// Secondary outcome token (binary markets).
    pub secondary_token_id: Option<TokenId>,
    /// Reported liquidity, when known.
    pub liquidity_usd: Option<Usd>,
    /// Reported 24h volume, when known.
    pub volume_24h_usd: Option<Usd>,
    /// Provenance of the selection evidence.
    pub source_refs: Vec<EvidenceSourceRef>,
}

/// A market that was filtered out, with the deciding reason.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ExcludedMarket {
    /// Market id.
    pub market_id: MarketId,
    /// Why it was excluded.
    pub reason: ExclusionReason,
}

/// Why a market was excluded from the selection.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ExclusionReason {
    /// Market is not open/active.
    NotOpen,
    /// Category disabled by config.
    CategoryDisabled,
    /// Below the liquidity / volume thresholds.
    InsufficientLiquidity,
    /// Spread wider than allowed.
    SpreadTooWide,
    /// Book staler than allowed.
    StaleBook,
    /// Fact lag exceeded the threshold.
    FactLagExceeded,
    /// Resolution is near/ambiguous.
    ResolutionAmbiguous,
    /// Manually blocked by an operator.
    ManuallyBlocked,
    /// The model requires features unavailable for this market.
    ModelFeatureUnavailable {
        /// The missing required features.
        missing: Vec<FeatureName>,
    },
}

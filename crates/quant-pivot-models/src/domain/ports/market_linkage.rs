//! Admin port for market-linkage resolve / override (Phase 11.2.2).

use async_trait::async_trait;
use quant_pivot_error::QuantResult;

use crate::{
    domain::{LinkageResolveSummaryView, MarketLinkageInfo, OverrideLinkageRequest},
    types::MarketId,
};

/// Offline linkage resolver governance boundary, implemented in `quant-pivot-core`.
#[async_trait]
pub trait MarketLinkageGovernancePort: Send + Sync {
    /// Re-resolve markets whose metadata or ruleset drifted.
    async fn resolve_changed_markets(
        &self,
        market_ids: &[MarketId],
    ) -> QuantResult<LinkageResolveSummaryView>;

    /// Append an audited operator override binding.
    ///
    /// `actor` is the authenticated caller's identity, sourced from the web
    /// layer's session — never operator-supplied JSON, to prevent spoofing.
    async fn apply_override(
        &self,
        market_id: &MarketId,
        request: OverrideLinkageRequest,
        actor: String,
    ) -> QuantResult<MarketLinkageInfo>;
}

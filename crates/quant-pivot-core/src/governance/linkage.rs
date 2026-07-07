//! Offline market-linkage resolver orchestration (Phase 11.2.2).
//!
//! Loads frozen Gamma metadata, runs the deterministic layered resolver from
//! `quant-pivot-research`, and appends outcomes to the bitemporal linkage ledger.
//! The online / PIT hot path never calls this module.

use std::{collections::HashMap, sync::Arc};

use chrono::Utc;
use quant_pivot_error::{QuantError, QuantResult, storage::StorageError};
use quant_pivot_models::{
    domain::{
        GroundingProof, LinkageOutcome, LinkageResolveSummaryView, LinkageSourceMetadata,
        MarketInfo, MarketLinkage, MarketLinkageInfo, MarketSubject, OverrideLinkageRequest,
        ResolvedBinding,
    },
    enums::{
        common::MarketCategory,
        domain::{DomainFamily, LinkageStatus, ResolverTier},
    },
    types::{
        ContentHash, DomainInstrumentKey, EventId, MarketId, MarketLinkageId, Probability,
        ResolverVersion,
    },
};
use quant_pivot_repository::traits::{EventRepository, MarketLinkageRepository, MarketRepository};
use quant_pivot_research::linkage::{LayeredResolver, ResolutionResult};

/// Dependencies for the offline linkage resolver.
pub struct LinkageResolverDeps {
    /// Append-only linkage ledger.
    pub linkage_repo: Arc<dyn MarketLinkageRepository>,
    /// Market catalog (metadata source).
    pub market_repo: Arc<dyn MarketRepository>,
    /// Event catalog (`series_slug` for Tier-0 linkage).
    pub event_repo: Arc<dyn EventRepository>,
}

/// Offline orchestration around the deterministic layered resolver.
pub struct LinkageResolverService {
    deps: LinkageResolverDeps,
    resolver: LayeredResolver,
}

impl LinkageResolverService {
    /// Wire the service from boot-time dependencies.
    #[must_use]
    pub fn new(deps: LinkageResolverDeps) -> Self {
        Self {
            deps,
            resolver: LayeredResolver::deterministic(),
        }
    }

    /// Resolve one market's metadata and append the outcome to the ledger.
    ///
    /// Idempotent on `content_hash`: re-running over unchanged inputs is a no-op.
    ///
    /// # Errors
    ///
    /// Propagates resolver, hashing, serialization, and persistence failures.
    pub async fn resolve_market(
        &self,
        metadata: LinkageSourceMetadata,
    ) -> QuantResult<MarketLinkageInfo> {
        let metadata_hash = metadata.metadata_hash()?;
        let result = self.resolver.resolve(&metadata)?;
        let linkage = resolution_to_linkage(&metadata, metadata_hash, result, Utc::now())?;
        self.deps
            .linkage_repo
            .append(
                linkage
                    .to_new()
                    .map_err(|error| QuantError::config(error.to_string()))?,
            )
            .await
            .map_err(QuantError::from)
    }

    /// Re-resolve crypto markets whose metadata or ruleset drifted since the
    /// latest ledger row.
    ///
    /// When `market_ids` is empty, every active crypto-category market is examined.
    ///
    /// # Errors
    ///
    /// Propagates catalog load, resolver, and persistence failures.
    pub async fn resolve_changed_markets(
        &self,
        market_ids: &[MarketId],
    ) -> QuantResult<LinkageResolveSummaryView> {
        let markets = self.load_crypto_markets(market_ids).await?;
        let examined = u64::try_from(markets.len()).unwrap_or(u64::MAX);
        if markets.is_empty() {
            return Ok(LinkageResolveSummaryView {
                examined: 0,
                appended: 0,
                unchanged: 0,
                resolved: 0,
                unresolved: 0,
            });
        }

        let series_by_event = self.load_series_slugs(&markets).await?;
        let ids: Vec<MarketId> = markets.iter().map(|m| m.market_id.clone()).collect();
        let latest = self
            .deps
            .linkage_repo
            .latest_for_markets(&ids)
            .await
            .map_err(QuantError::from)?;
        let latest_by_market: HashMap<MarketId, MarketLinkageInfo> = latest
            .into_iter()
            .map(|row| (row.market_id.clone(), row))
            .collect();

        let resolver_version = self.resolver.resolver_version();
        let mut appended = 0_u64;
        let mut unchanged = 0_u64;
        let mut resolved = 0_u64;
        let mut unresolved = 0_u64;

        for market in &markets {
            let metadata = metadata_from_market(
                market,
                series_by_event.get(&market.event_id).cloned().flatten(),
            );
            let metadata_hash = metadata.metadata_hash()?;
            if is_current(
                latest_by_market.get(&market.market_id),
                &metadata_hash,
                resolver_version,
            ) {
                unchanged += 1;
                continue;
            }
            let row = self.resolve_market(metadata).await?;
            appended += 1;
            match row.status {
                LinkageStatus::Resolved | LinkageStatus::Overridden => resolved += 1,
                LinkageStatus::Unresolved => unresolved += 1,
            }
        }

        Ok(LinkageResolveSummaryView {
            examined,
            appended,
            unchanged,
            resolved,
            unresolved,
        })
    }

    /// Operator override stub: append an audited binding with
    /// `resolver_tier = Override`.
    ///
    /// Full governance audit wiring lands with the Phase 07 HTTP surface; this
    /// method freezes the override into the ledger using the same append path as
    /// deterministic resolution.
    ///
    /// # Errors
    ///
    /// Propagates catalog load, deserialization, hashing, and persistence failures.
    pub async fn apply_override(
        &self,
        market_id: &MarketId,
        request: OverrideLinkageRequest,
    ) -> QuantResult<MarketLinkageInfo> {
        let market = self
            .deps
            .market_repo
            .find_by_id(market_id)
            .await
            .map_err(QuantError::from)?
            .ok_or_else(|| StorageError::NotFound {
                entity: "market",
                id: market_id.to_string(),
            })?;
        let series_slug = self
            .deps
            .event_repo
            .find_by_id(&market.event_id)
            .await
            .map_err(QuantError::from)?
            .and_then(|event| event.series_slug);
        let metadata = metadata_from_market(&market, series_slug);
        let metadata_hash = metadata.metadata_hash()?;
        let subject: MarketSubject = serde_json::from_value(request.subject)
            .map_err(|error| QuantError::config(error.to_string()))?;
        let instrument_key = DomainInstrumentKey::new(&request.instrument_key);
        let domain_family = subject.family();
        let outcome = LinkageOutcome::Resolved(ResolvedBinding {
            subject,
            instrument_key,
            grounding: GroundingProof { spans: Vec::new() },
        });
        let resolver_version = self.resolver.resolver_version();
        let content_hash = MarketLinkage::compute_content_hash(
            market_id,
            domain_family,
            &outcome,
            ResolverTier::Override,
            resolver_version,
            &metadata_hash,
        )?;
        let linkage = MarketLinkage {
            linkage_id: MarketLinkageId::from_v7(),
            market_id: market_id.clone(),
            domain_family,
            outcome,
            confidence: Probability::ONE,
            resolver_tier: ResolverTier::Override,
            resolver_version,
            metadata_hash,
            content_hash,
            derived_at: Utc::now(),
        };
        self.deps
            .linkage_repo
            .append(
                linkage
                    .to_new()
                    .map_err(|error| QuantError::config(error.to_string()))?,
            )
            .await
            .map_err(QuantError::from)
    }

    async fn load_crypto_markets(&self, market_ids: &[MarketId]) -> QuantResult<Vec<MarketInfo>> {
        let markets: Vec<MarketInfo> = if market_ids.is_empty() {
            self.deps
                .market_repo
                .find_active()
                .await
                .map_err(QuantError::from)?
                .iter()
                .cloned()
                .collect()
        } else {
            self.deps
                .market_repo
                .find_by_ids(market_ids)
                .await
                .map_err(QuantError::from)?
                .into_iter()
                .map(|market| (*market).clone())
                .collect()
        };
        Ok(markets
            .into_iter()
            .filter(|market| market.fee_category() == MarketCategory::Crypto)
            .collect())
    }

    async fn load_series_slugs(
        &self,
        markets: &[MarketInfo],
    ) -> QuantResult<HashMap<EventId, Option<String>>> {
        let mut event_ids: Vec<_> = markets.iter().map(|m| m.event_id.clone()).collect();
        event_ids.sort_by(|a, b| a.as_str().cmp(b.as_str()));
        event_ids.dedup_by(|a, b| a.as_str() == b.as_str());
        let events = self
            .deps
            .event_repo
            .find_by_ids(&event_ids)
            .await
            .map_err(QuantError::from)?;
        Ok(events
            .into_iter()
            .map(|event| (event.event_id.clone(), event.series_slug))
            .collect())
    }
}

fn metadata_from_market(market: &MarketInfo, series_slug: Option<String>) -> LinkageSourceMetadata {
    LinkageSourceMetadata {
        market_id: market.market_id.clone(),
        slug: market.slug.clone(),
        question: market.question.clone(),
        description: market.description.clone(),
        series_slug,
        end_date: market.end_date,
    }
}

fn is_current(
    latest: Option<&MarketLinkageInfo>,
    metadata_hash: &ContentHash,
    resolver_version: ResolverVersion,
) -> bool {
    latest.is_some_and(|row| {
        row.metadata_hash == *metadata_hash && row.resolver_version == resolver_version
    })
}

fn resolution_to_linkage(
    metadata: &LinkageSourceMetadata,
    metadata_hash: ContentHash,
    result: ResolutionResult,
    derived_at: chrono::DateTime<Utc>,
) -> QuantResult<MarketLinkage> {
    let domain_family = domain_family_for(&result);
    let content_hash = MarketLinkage::compute_content_hash(
        &metadata.market_id,
        domain_family,
        &result.outcome,
        result.resolver_tier,
        result.resolver_version,
        &metadata_hash,
    )?;
    Ok(MarketLinkage {
        linkage_id: MarketLinkageId::from_v7(),
        market_id: metadata.market_id.clone(),
        domain_family,
        outcome: result.outcome,
        confidence: result.confidence,
        resolver_tier: result.resolver_tier,
        resolver_version: result.resolver_version,
        metadata_hash,
        content_hash,
        derived_at,
    })
}

const fn domain_family_for(result: &ResolutionResult) -> DomainFamily {
    match &result.outcome {
        LinkageOutcome::Resolved(binding) => binding.subject.family(),
        LinkageOutcome::Unresolved { .. } => DomainFamily::Crypto,
    }
}

#[async_trait::async_trait]
impl quant_pivot_models::domain::MarketLinkageGovernancePort for LinkageResolverService {
    async fn resolve_changed_markets(
        &self,
        market_ids: &[MarketId],
    ) -> QuantResult<LinkageResolveSummaryView> {
        self.resolve_changed_markets(market_ids).await
    }

    async fn apply_override(
        &self,
        market_id: &MarketId,
        request: OverrideLinkageRequest,
    ) -> QuantResult<MarketLinkageInfo> {
        self.apply_override(market_id, request).await
    }
}

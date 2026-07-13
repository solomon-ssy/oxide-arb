//! Offline market-linkage resolver orchestration (Phase 11.2.2).
//!
//! Loads frozen Gamma metadata, runs the deterministic layered resolver from
//! `quant-pivot-research`, and appends outcomes to the bitemporal linkage ledger.
//! The online / PIT hot path never calls this module.

use std::{collections::HashMap, sync::Arc};

use chrono::Utc;
use quant_pivot_error::{
    QuantError, QuantResult, governance::GovernanceError, storage::StorageError,
};
use quant_pivot_models::{
    domain::{
        LinkageOutcome, LinkageResolveSummaryView, LinkageSourceMetadata, MarketInfo,
        MarketLinkageDerivation, MarketLinkageGovernancePort, MarketLinkageInfo, MarketSubject,
        NewMarketLinkage, OverrideContext, OverrideLinkageRequest, ResolvedBinding,
    },
    enums::{
        common::MarketCategory,
        domain::{LinkageStatus, ResolverTier},
    },
    types::{ContentHash, DomainInstrumentKey, EventId, MarketId, Probability, ResolverVersion},
};
use quant_pivot_repository::traits::{EventRepository, MarketLinkageRepository, MarketRepository};
use quant_pivot_research::linkage::{LayeredResolver, ResolutionResult, validate_manual_override};

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
        let linkage = resolution_to_new_linkage(&metadata, metadata_hash, result, Utc::now())?;
        self.deps
            .linkage_repo
            .append(linkage)
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
        let examined =
            u64::try_from(markets.len()).map_err(|error| GovernanceError::NumericOverflow {
                field: "linkage resolver market count",
                detail: error.to_string(),
            })?;
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

    /// Audited operator override: append a binding with `resolver_tier =
    /// Override` after it passes the same structural-consistency checks
    /// every automated candidate must clear (ruleset instrument binding +
    /// oracle↔asset consistency), **and** after every load-bearing identity
    /// field (`asset` / `resolution_oracle` / `strike` when present) is
    /// grounded to a byte-exact literal citation from the market's real
    /// metadata via [`validate_manual_override`] — an override is a human
    /// decision, never text-extracted, but it is never accepted as an
    /// unanchored assertion either (11.2.2 remediation R4).
    ///
    /// The real audit trail is [`ResolvedBinding::override_context`]
    /// (`reason` + `actor`), persisted both inside `outcome` and projected
    /// into the ledger's first-class `override_reason` / `override_actor`
    /// columns (via [`NewMarketLinkage::from_derivation`]) — never discarded.
    ///
    /// # Errors
    ///
    /// Propagates catalog load, deserialization, hashing, and persistence
    /// failures, and returns [`QuantError::config`] when the proposed subject
    /// fails structural consistency or grounding (wrong instrument for the
    /// asset, an oracle/asset mismatch, a missing citation for a load-bearing
    /// field, or a citation that does not literally occur in the cited
    /// source field).
    pub async fn apply_override(
        &self,
        market_id: &MarketId,
        request: OverrideLinkageRequest,
        actor: String,
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
        let MarketSubject::Crypto(crypto_subject) = &subject;
        let grounding = validate_manual_override(
            crypto_subject,
            &instrument_key,
            &metadata,
            &request.evidence,
        )
        .map_err(QuantError::config)?;
        let outcome = LinkageOutcome::Resolved(ResolvedBinding {
            subject,
            instrument_key,
            grounding,
            override_context: Some(OverrideContext {
                reason: request.reason,
                actor,
            }),
        });
        let resolver_version = self.resolver.resolver_version();
        let effective_at = Utc::now();
        let linkage = NewMarketLinkage::from_derivation(MarketLinkageDerivation {
            market_id: market_id.clone(),
            outcome,
            confidence: Probability::ONE,
            resolver_tier: ResolverTier::Override,
            resolver_version,
            metadata_hash,
            effective_at,
        })?;
        self.deps
            .linkage_repo
            .append(linkage)
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

fn resolution_to_new_linkage(
    metadata: &LinkageSourceMetadata,
    metadata_hash: ContentHash,
    result: ResolutionResult,
    effective_at: chrono::DateTime<Utc>,
) -> QuantResult<NewMarketLinkage> {
    NewMarketLinkage::from_derivation(MarketLinkageDerivation {
        market_id: metadata.market_id.clone(),
        outcome: result.outcome,
        confidence: result.confidence,
        resolver_tier: result.resolver_tier,
        resolver_version: result.resolver_version,
        metadata_hash,
        effective_at,
    })
}

#[async_trait::async_trait]
impl MarketLinkageGovernancePort for LinkageResolverService {
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
        actor: String,
    ) -> QuantResult<MarketLinkageInfo> {
        self.apply_override(market_id, request, actor).await
    }
}

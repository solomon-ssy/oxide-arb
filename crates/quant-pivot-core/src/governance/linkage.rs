//! Continuous market-linkage reconciliation for the Gamma catalog.
//!
//! Every pass classifies the complete affected Crypto/Weather catalog before
//! running the deterministic layered resolver and appending changed outcomes
//! to the bitemporal linkage ledger. The online / PIT hot path never performs
//! catalog classification or resolution.

use std::{
    collections::{BTreeMap, HashMap, HashSet},
    sync::Arc,
};

use chrono::{DateTime, Utc};
use quant_pivot_error::{
    QuantError, QuantResult, governance::GovernanceError, storage::StorageError,
};
use quant_pivot_models::{
    config::{WeatherStationProfileConfig, WeatherVerticalBindingsConfig},
    domain::{
        api::{LinkageResolveSummaryView, OverrideLinkageRequest},
        market::{EventInfo, MarketInfo},
        ports::MarketLinkageGovernancePort,
        quant::{
            LinkageOutcome, LinkageSourceMetadata, MarketLinkageDerivation, MarketLinkageInfo,
            MarketSubject, NewMarketLinkage, OverrideContext, ResolvedBinding,
        },
    },
    enums::{
        common::MarketCategory,
        domain::{DomainFamily, LinkageSourceRole, LinkageStatus, ResolverTier},
    },
    types::{
        ContentHash, EventId, MarketId, Probability, ResolverVersion,
        domain_classification::{
            DomainCatalogClassificationArtifact, DomainMarketClassificationOutcome,
        },
    },
};
use quant_pivot_repository::traits::{EventRepository, MarketLinkageRepository, MarketRepository};
use quant_pivot_research::linkage::{
    LayeredResolver, ResolutionResult, WeatherDecisionGroupMember, WeatherStationRegistry,
    capability_registry::domain_capability_registry,
    catalog_classification::DomainCatalogClassifier, source_bindings_for_subject,
    validate_manual_override, validate_weather_decision_group,
};

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
    classifier: DomainCatalogClassifier,
    capability_registry_hash: ContentHash,
}

impl LinkageResolverService {
    /// Wire the service from boot-time dependencies.
    pub fn new(
        deps: LinkageResolverDeps,
        weather_stations: HashMap<String, WeatherStationProfileConfig>,
        weather_vertical_bindings: &WeatherVerticalBindingsConfig,
    ) -> QuantResult<Self> {
        let weather_station_registry =
            WeatherStationRegistry::try_new(weather_stations.into_iter().collect())?;
        let capability_registry_hash = domain_capability_registry(
            &weather_station_registry.registry_hash()?,
            weather_vertical_bindings,
        )?
        .registry_hash;
        let classifier = DomainCatalogClassifier::new(
            weather_station_registry.clone(),
            weather_vertical_bindings,
        )?;
        Ok(Self {
            deps,
            resolver: LayeredResolver::deterministic(weather_station_registry),
            classifier,
            capability_registry_hash,
        })
    }

    /// Re-resolve Crypto/Weather markets whose metadata, decision-group
    /// membership, capability registry, or resolver rules drifted since the
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
        let initial_markets = self.load_supported_markets(market_ids).await?;
        let events_by_id = self.load_events(&initial_markets).await?;
        let markets = self
            .expand_weather_decision_groups(initial_markets, &events_by_id)
            .await?;
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

        let classification = self.classifier.classify_catalog(&markets, &events_by_id)?;
        log_catalog_classification(&classification);
        let family_by_market: HashMap<_, _> = classification
            .classifications
            .iter()
            .map(|row| (row.market_id.clone(), row.family))
            .collect();

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
        let mut unchanged = 0_u64;
        let mut resolved = 0_u64;
        let mut unresolved = 0_u64;
        let cycle_at = Utc::now();
        let mut outcomes_by_market = HashMap::with_capacity(markets.len());
        let mut pending = Vec::new();

        for market in &markets {
            let domain_family = family_by_market
                .get(&market.market_id)
                .copied()
                .ok_or_else(|| GovernanceError::QualityGateFailed {
                    entity: "domain_catalog_classification",
                    id: market.market_id.to_string(),
                    failures: "active domain market has no classification".to_owned(),
                })?;
            let event =
                events_by_id
                    .get(&market.event_id)
                    .ok_or_else(|| StorageError::NotFound {
                        entity: "event",
                        id: market.event_id.to_string(),
                    })?;
            let metadata = metadata_from_market(market, event);
            let metadata_hash = metadata.metadata_hash()?;
            if is_current(
                latest_by_market.get(&market.market_id),
                domain_family,
                &metadata_hash,
                resolver_version,
                &self.capability_registry_hash,
            ) {
                unchanged += 1;
                let row = latest_by_market.get(&market.market_id).ok_or_else(|| {
                    StorageError::NotFound {
                        entity: "quant_market_linkage",
                        id: market.market_id.to_string(),
                    }
                })?;
                outcomes_by_market.insert(market.market_id.clone(), row.outcome.clone());
                continue;
            }
            let result = self.resolver.resolve(&metadata, cycle_at)?;
            outcomes_by_market.insert(market.market_id.clone(), result.outcome.clone());
            pending.push(resolution_to_new_linkage(
                domain_family,
                &metadata,
                metadata_hash,
                self.capability_registry_hash,
                result,
                cycle_at,
            )?);
        }

        validate_weather_groups(&markets, &events_by_id, &outcomes_by_market)?;
        let rows = if pending.is_empty() {
            Vec::new()
        } else {
            self.deps
                .linkage_repo
                .append_batch(pending)
                .await
                .map_err(QuantError::from)?
        };
        let appended =
            u64::try_from(rows.len()).map_err(|error| GovernanceError::NumericOverflow {
                field: "linkage resolver append count",
                detail: error.to_string(),
            })?;
        for row in rows {
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
    /// unanchored assertion either.
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
        let event = self
            .deps
            .event_repo
            .find_by_id(&market.event_id)
            .await
            .map_err(QuantError::from)?
            .ok_or_else(|| StorageError::NotFound {
                entity: "event",
                id: market.event_id.to_string(),
            })?;
        let metadata = metadata_from_market(&market, &event);
        let metadata_hash = metadata.metadata_hash()?;
        let subject = request.subject;
        let MarketSubject::Crypto(crypto_subject) = &subject else {
            return Err(QuantError::config(
                "manual Weather linkage overrides are not supported by the crypto validator",
            ));
        };
        let effective_at = Utc::now();
        let source_bindings = source_bindings_for_subject(&subject, effective_at)?;
        let feature_binding = source_bindings
            .iter()
            .find(|binding| binding.role == LinkageSourceRole::Feature)
            .ok_or_else(|| QuantError::config("subject has no canonical feature source binding"))?;
        let grounding = validate_manual_override(
            crypto_subject,
            &feature_binding.instrument_key,
            &metadata,
            &request.evidence,
        )
        .map_err(QuantError::config)?;
        let mut requested_source_bindings = request.source_bindings;
        requested_source_bindings.sort_by(|left, right| {
            (left.role, &left.source_id, &left.instrument_key).cmp(&(
                right.role,
                &right.source_id,
                &right.instrument_key,
            ))
        });
        let requested_identities = requested_source_bindings
            .iter()
            .map(|binding| (binding.role, &binding.source_id, &binding.instrument_key))
            .collect::<Vec<_>>();
        let expected_identities = source_bindings
            .iter()
            .map(|binding| (binding.role, &binding.source_id, &binding.instrument_key))
            .collect::<Vec<_>>();
        if requested_identities != expected_identities {
            return Err(QuantError::config(
                "override source bindings do not match the subject's frozen source rules",
            ));
        }
        let domain_family = subject.family();
        let outcome = LinkageOutcome::Resolved(Box::new(ResolvedBinding {
            subject,
            source_bindings,
            grounding,
            override_context: Some(OverrideContext {
                reason: request.reason,
                actor,
            }),
        }));
        let resolver_version = self.resolver.resolver_version();
        let linkage = NewMarketLinkage::from_derivation(MarketLinkageDerivation {
            market_id: market_id.clone(),
            domain_family,
            outcome,
            confidence: Probability::ONE,
            resolver_tier: ResolverTier::Override,
            resolver_version,
            metadata_hash,
            capability_registry_hash: self.capability_registry_hash,
            effective_at,
        })?;
        self.deps
            .linkage_repo
            .append(linkage)
            .await
            .map_err(QuantError::from)
    }

    async fn load_supported_markets(
        &self,
        market_ids: &[MarketId],
    ) -> QuantResult<Vec<MarketInfo>> {
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
            .filter(|market| {
                matches!(
                    market.primary_category(),
                    MarketCategory::Crypto | MarketCategory::Weather
                )
            })
            .collect())
    }

    async fn load_events(
        &self,
        markets: &[MarketInfo],
    ) -> QuantResult<BTreeMap<EventId, EventInfo>> {
        let mut event_ids: Vec<_> = markets.iter().map(|m| m.event_id.clone()).collect();
        event_ids.sort_by(|a, b| a.as_str().cmp(b.as_str()));
        event_ids.dedup_by(|a, b| a.as_str() == b.as_str());
        let events = self
            .deps
            .event_repo
            .find_by_ids(&event_ids)
            .await
            .map_err(QuantError::from)?;
        let events_by_id: BTreeMap<_, _> = events
            .into_iter()
            .map(|event| (event.event_id.clone(), event))
            .collect();
        for event_id in event_ids {
            if !events_by_id.contains_key(&event_id) {
                return Err(StorageError::NotFound {
                    entity: "event",
                    id: event_id.to_string(),
                }
                .into());
            }
        }
        Ok(events_by_id)
    }

    async fn expand_weather_decision_groups(
        &self,
        markets: Vec<MarketInfo>,
        events_by_id: &BTreeMap<EventId, EventInfo>,
    ) -> QuantResult<Vec<MarketInfo>> {
        let mut ids: HashSet<MarketId> = markets
            .iter()
            .map(|market| market.market_id.clone())
            .collect();
        for market in &markets {
            if market.primary_category() != MarketCategory::Weather {
                continue;
            }
            let event =
                events_by_id
                    .get(&market.event_id)
                    .ok_or_else(|| StorageError::NotFound {
                        entity: "event",
                        id: market.event_id.to_string(),
                    })?;
            ids.extend(event.catalog_market_ids.iter().cloned());
        }
        let mut ids: Vec<_> = ids.into_iter().collect();
        ids.sort_by(|left, right| left.as_str().cmp(right.as_str()));
        let mut expanded: Vec<_> = self
            .deps
            .market_repo
            .find_by_ids(&ids)
            .await
            .map_err(QuantError::from)?
            .into_iter()
            .map(|market| (*market).clone())
            .filter(|market| {
                matches!(
                    market.primary_category(),
                    MarketCategory::Crypto | MarketCategory::Weather
                )
            })
            .collect();
        expanded.sort_by(|left, right| left.market_id.as_str().cmp(right.market_id.as_str()));
        Ok(expanded)
    }
}

fn metadata_from_market(market: &MarketInfo, event: &EventInfo) -> LinkageSourceMetadata {
    LinkageSourceMetadata {
        market_id: market.market_id.clone(),
        slug: market.slug.clone(),
        question: market.question.clone(),
        description: market.description.clone(),
        series_slug: event.series_slug.clone(),
        decision_group_market_ids: if market.primary_category() == MarketCategory::Weather {
            event.catalog_market_ids.iter().cloned().collect()
        } else {
            Vec::new()
        },
        end_date: market.end_date,
    }
}

fn validate_weather_groups(
    markets: &[MarketInfo],
    events_by_id: &BTreeMap<EventId, EventInfo>,
    outcomes_by_market: &HashMap<MarketId, LinkageOutcome>,
) -> QuantResult<()> {
    let markets_by_id: HashMap<_, _> = markets
        .iter()
        .map(|market| (market.market_id.clone(), market))
        .collect();
    let mut event_ids: Vec<_> = markets
        .iter()
        .filter(|market| {
            matches!(
                outcomes_by_market.get(&market.market_id),
                Some(LinkageOutcome::Resolved(binding))
                    if matches!(binding.subject, MarketSubject::Weather(_))
            )
        })
        .map(|market| market.event_id.clone())
        .collect();
    event_ids.sort_by(|left, right| left.as_str().cmp(right.as_str()));
    event_ids.dedup_by(|left, right| left.as_str() == right.as_str());

    for event_id in event_ids {
        let event = events_by_id
            .get(&event_id)
            .ok_or_else(|| StorageError::NotFound {
                entity: "event",
                id: event_id.to_string(),
            })?;
        if !event.neg_risk {
            return Err(weather_group_failure(
                &event_id,
                "event is not declared as a mutually exclusive neg-risk group",
            ));
        }
        if event.catalog_market_ids.is_empty() {
            return Err(weather_group_failure(
                &event_id,
                "Gamma catalog decision-group membership is empty",
            ));
        }

        let mut members = Vec::with_capacity(event.catalog_market_ids.len());
        for market_id in event.catalog_market_ids.iter() {
            let market = markets_by_id.get(market_id).ok_or_else(|| {
                weather_group_failure(
                    &event_id,
                    format!("catalog sibling `{market_id}` is not materialized as Weather"),
                )
            })?;
            if market.event_id != event_id
                || market.primary_category() != MarketCategory::Weather
                || !market.neg_risk
            {
                return Err(weather_group_failure(
                    &event_id,
                    format!(
                        "catalog sibling `{market_id}` does not belong to the same Weather neg-risk group"
                    ),
                ));
            }
            let outcome = outcomes_by_market.get(market_id).ok_or_else(|| {
                weather_group_failure(
                    &event_id,
                    format!("catalog sibling `{market_id}` has no resolver outcome"),
                )
            })?;
            let subject = match outcome {
                LinkageOutcome::Resolved(binding) => match &binding.subject {
                    MarketSubject::Weather(subject) => subject.clone(),
                    MarketSubject::Crypto(_) => {
                        return Err(weather_group_failure(
                            &event_id,
                            format!("catalog sibling `{market_id}` resolved to Crypto"),
                        ));
                    }
                },
                LinkageOutcome::Unresolved { reason } => {
                    return Err(weather_group_failure(
                        &event_id,
                        format!(
                            "catalog sibling `{market_id}` failed deterministic resolution: {reason:?}"
                        ),
                    ));
                }
            };
            members.push(WeatherDecisionGroupMember {
                market_id: market_id.clone(),
                subject,
                yes_won: weather_yes_won(market).map_err(|detail| {
                    weather_group_failure(&event_id, format!("sibling `{market_id}`: {detail}"))
                })?,
            });
        }
        validate_weather_decision_group(&members, true)
            .map_err(|detail| weather_group_failure(&event_id, detail))?;
    }
    Ok(())
}

fn log_catalog_classification(classification: &DomainCatalogClassificationArtifact) {
    let mut supported = 0_u64;
    let mut credential_blocked = 0_u64;
    let mut insufficient_evidence = 0_u64;
    let mut excluded = 0_u64;
    let mut unsupported_template = 0_u64;
    for row in &classification.classifications {
        match row.outcome {
            DomainMarketClassificationOutcome::Supported => supported += 1,
            DomainMarketClassificationOutcome::CredentialBlocked { .. } => {
                credential_blocked += 1;
            }
            DomainMarketClassificationOutcome::InsufficientEvidence { .. } => {
                insufficient_evidence += 1;
            }
            DomainMarketClassificationOutcome::Excluded { .. } => excluded += 1,
            DomainMarketClassificationOutcome::UnsupportedTemplate { .. } => {
                unsupported_template += 1;
            }
        }
    }
    tracing::info!(
        artifact_hash = %classification.artifact_hash,
        catalog_hash = %classification.catalog_hash,
        capability_registry_hash = %classification.capability_registry_hash,
        supported,
        credential_blocked,
        insufficient_evidence,
        excluded,
        unsupported_template,
        "domain catalog classification complete before linkage reconciliation"
    );
}

fn weather_yes_won(market: &MarketInfo) -> Result<Option<bool>, String> {
    match (market.resolved_at, market.outcome.as_deref()) {
        (None, None) => Ok(None),
        (Some(_), Some(outcome)) if outcome.eq_ignore_ascii_case("yes") => Ok(Some(true)),
        (Some(_), Some(outcome)) if outcome.eq_ignore_ascii_case("no") => Ok(Some(false)),
        (Some(_), Some(outcome)) => Err(format!(
            "resolved binary Weather market has unsupported winning outcome `{outcome}`"
        )),
        (Some(_), None) => Err("resolved market is missing its winning outcome".to_owned()),
        (None, Some(outcome)) => Err(format!(
            "unresolved market unexpectedly carries winning outcome `{outcome}`"
        )),
    }
}

fn weather_group_failure(event_id: &EventId, detail: impl Into<String>) -> QuantError {
    GovernanceError::QualityGateFailed {
        entity: "weather_decision_group",
        id: event_id.to_string(),
        failures: detail.into(),
    }
    .into()
}

fn is_current(
    latest: Option<&MarketLinkageInfo>,
    domain_family: DomainFamily,
    metadata_hash: &ContentHash,
    resolver_version: ResolverVersion,
    capability_registry_hash: &ContentHash,
) -> bool {
    let expected = LinkageCurrentness {
        domain_family,
        metadata_hash,
        resolver_version,
        capability_registry_hash: Some(capability_registry_hash),
    };
    latest.is_some_and(|row| currentness_matches(LinkageCurrentness::from(row), expected))
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct LinkageCurrentness<'a> {
    domain_family: DomainFamily,
    metadata_hash: &'a ContentHash,
    resolver_version: ResolverVersion,
    capability_registry_hash: Option<&'a ContentHash>,
}

impl<'a> From<&'a MarketLinkageInfo> for LinkageCurrentness<'a> {
    fn from(linkage: &'a MarketLinkageInfo) -> Self {
        Self {
            domain_family: linkage.domain_family,
            metadata_hash: &linkage.metadata_hash,
            resolver_version: linkage.resolver_version,
            capability_registry_hash: linkage.capability_registry_hash.as_ref(),
        }
    }
}

fn currentness_matches(latest: LinkageCurrentness<'_>, expected: LinkageCurrentness<'_>) -> bool {
    latest == expected
}

fn resolution_to_new_linkage(
    domain_family: DomainFamily,
    metadata: &LinkageSourceMetadata,
    metadata_hash: ContentHash,
    capability_registry_hash: ContentHash,
    result: ResolutionResult,
    effective_at: DateTime<Utc>,
) -> QuantResult<NewMarketLinkage> {
    NewMarketLinkage::from_derivation(MarketLinkageDerivation {
        market_id: metadata.market_id.clone(),
        domain_family,
        outcome: result.outcome,
        confidence: result.confidence,
        resolver_tier: result.resolver_tier,
        resolver_version: result.resolver_version,
        metadata_hash,
        capability_registry_hash,
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

#[cfg(test)]
mod tests {
    use quant_pivot_models::{
        enums::domain::DomainFamily,
        types::{ContentHash, ResolverVersion},
    };

    use super::{LinkageCurrentness, currentness_matches};

    fn hash(fill: char) -> ContentHash {
        ContentHash::parse(&format!("blake3:{}", fill.to_string().repeat(64))).expect("hash")
    }

    fn currentness<'a>(
        domain_family: DomainFamily,
        metadata_hash: &'a ContentHash,
        resolver_version: ResolverVersion,
        capability_registry_hash: Option<&'a ContentHash>,
    ) -> LinkageCurrentness<'a> {
        LinkageCurrentness {
            domain_family,
            metadata_hash,
            resolver_version,
            capability_registry_hash,
        }
    }

    #[test]
    fn linkage_currentness_binds_registry() {
        let metadata = hash('a');
        let registry = hash('b');
        let resolver = ResolverVersion::new(3);

        let expected = currentness(DomainFamily::Crypto, &metadata, resolver, Some(&registry));

        assert!(currentness_matches(
            currentness(DomainFamily::Crypto, &metadata, resolver, Some(&registry),),
            expected,
        ));
        assert!(!currentness_matches(
            currentness(DomainFamily::Crypto, &metadata, resolver, None),
            expected,
        ));
        assert!(!currentness_matches(
            currentness(DomainFamily::Crypto, &metadata, resolver, Some(&hash('c')),),
            expected,
        ));
        assert!(!currentness_matches(
            currentness(DomainFamily::Crypto, &hash('c'), resolver, Some(&registry),),
            expected,
        ));
        assert!(!currentness_matches(
            currentness(
                DomainFamily::Crypto,
                &metadata,
                ResolverVersion::new(2),
                Some(&registry),
            ),
            expected,
        ));
        assert!(!currentness_matches(
            currentness(DomainFamily::Crypto, &metadata, resolver, Some(&registry),),
            currentness(DomainFamily::Weather, &metadata, resolver, Some(&registry),),
        ));
    }
}

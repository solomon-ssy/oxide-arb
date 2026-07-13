//! Domain-slice PIT input assembly (Phase 11.2.2).
//!
//! [`build_domain_slice_inputs`] is the **single** shared function that decides
//! whether a market carries a domain slice and, if so, assembles its PIT
//! observation windows. The online feature pipeline and the offline replay
//! (dataset build / backtest) both call it with the same frozen linkage and
//! the same prefetched observations, so the domain slice is byte-identical
//! across planes by construction — there is no second implementation to drift.
//!
//! Fail-closed ladder (each rung returns `None` → `domain: None` on the
//! vector, structurally absent, never a fabricated zero row):
//!
//! 1. the market's category maps to no vertical;
//! 2. the vertical is disabled in `domain.enabled_by_family`;
//! 3. no linkage record is PIT-valid at the decision boundary;
//! 4. the PIT-valid record is `Unresolved` (no binding).

use std::{collections::HashMap, hash::BuildHasher};

use chrono::{DateTime, Duration as ChronoDuration, Utc};
use quant_pivot_error::{QuantError, QuantResult};
use quant_pivot_models::{
    domain::{
        DecisionBoundary, DecisionSource, DomainAvailability, DomainObservation, MarketLinkage,
        MarketSubject, ResolutionOracle, ResolvedBinding,
    },
    enums::{
        common::MarketCategory,
        domain::{DomainFamily, DomainMetric},
    },
    runtime_config::DomainConfig,
    types::DomainInstrumentKey,
};

use crate::{
    domain::DomainObservationWindow,
    features::{DomainSliceInputs, EvidenceSourceKind, EvidenceSourceRef},
};

/// The trailing observation lookback (seconds) the crypto domain slice needs:
/// the widest of the momentum / volatility feature windows.
#[must_use]
pub fn crypto_lookback_secs(domain: &DomainConfig) -> u64 {
    domain
        .crypto
        .momentum_window_secs
        .max(domain.crypto.volatility_window_secs)
}

/// The latest linkage visible at `boundary` on both bitemporal axes.
///
/// `linkages` may arrive in any order; ties on `effective_at` break on
/// `available_at` then `linkage_id` — byte-identical to the Postgres
/// repository's `ORDER BY derived_at DESC, created_at DESC, linkage_id DESC`,
/// so the online (`valid_at`) and offline (`ledger_for_markets` + this
/// function) planes can never pick a different revision for the same
/// market decision on a tie.
#[must_use]
pub fn linkage_valid_at<'a>(
    linkages: &'a [MarketLinkage],
    boundary: &DecisionBoundary,
) -> Option<&'a MarketLinkage> {
    let source_cutoff = boundary.cutoff_for(DecisionSource::Linkage);
    linkages
        .iter()
        .filter(|linkage| {
            linkage.effective_at <= source_cutoff && linkage.available_at <= boundary.decision_at()
        })
        .max_by(|a, b| {
            (a.effective_at, a.available_at, a.linkage_id.as_uuid()).cmp(&(
                b.effective_at,
                b.available_at,
                b.linkage_id.as_uuid(),
            ))
        })
}

fn linkage_evidence(linkage: &MarketLinkage) -> EvidenceSourceRef {
    EvidenceSourceRef {
        source_kind: EvidenceSourceKind::Linkage,
        reference: format!("linkage:{}@{}", linkage.linkage_id, linkage.content_hash),
        effective_at: linkage.effective_at,
        available_at: Some(linkage.available_at),
    }
}

/// Frozen domain-plane availability for one category and decision boundary (Phase
/// 11.2.2 §3.8), computed purely from a market's PIT-bounded linkage history
/// and a prefetched observation series.
///
/// This is the **zero-I/O, offline-replay counterpart** to the live batched
/// projector (`resolve_domain_availability` in `quant-pivot-core`'s
/// `prefetch::domain_availability`): both apply byte-identical rules —
/// mapped ∧ family-enabled ∧ a PIT-valid `Resolved` linkage at the boundary ∧ a
/// visible `Close` observation at the domain source cutoff — so a
/// training-dataset build can never see a different verdict than the live
/// report pipeline would have for the same evidence. [`linkage_valid_at`]
/// supplies the shared bitemporal tie-break.
///
/// # Honest approximation
///
/// `observations` is whatever window the caller prefetched (bounded below by
/// the build's lookback horizon), not an unbounded "ever observed before
/// cutoff" scan like the live `domain_observation_at` query. A source that
/// stopped publishing long before the prefetch window and never resumed
/// would read `SourceEmpty` here but `Available` online — a fail-safe
/// direction (never a false `Available`), acceptable for a continuously
/// live source like Binance/Chainlink.
#[must_use]
pub fn domain_availability_at<S: BuildHasher>(
    category: MarketCategory,
    linkages: &[MarketLinkage],
    boundary: &DecisionBoundary,
    domain: &DomainConfig,
    observations: &HashMap<DomainInstrumentKey, Vec<DomainObservation>, S>,
) -> DomainAvailability {
    let Some(family) = DomainFamily::for_category(category) else {
        return DomainAvailability::NotMapped;
    };
    if !domain.family_enabled(family) {
        return DomainAvailability::NotMapped;
    }
    let Some(binding) = linkage_valid_at(linkages, boundary).and_then(MarketLinkage::binding)
    else {
        return DomainAvailability::Unresolved;
    };

    let cutoff = boundary.cutoff_for(DecisionSource::DomainCrypto);
    let has_close_observation = observations
        .get(&binding.instrument_key)
        .is_some_and(|series| {
            series.iter().any(|observation| {
                observation.metric == DomainMetric::Close
                    && observation.observed_at <= cutoff
                    && observation.publish_time <= cutoff
                    && observation
                        .available_at
                        .is_some_and(|available_at| available_at <= boundary.decision_at())
            })
        });
    if has_close_observation {
        DomainAvailability::Available
    } else {
        DomainAvailability::SourceEmpty
    }
}

/// Assemble the optional domain-slice inputs for one market decision.
///
/// `observations` is keyed by instrument, each series ascending by
/// `observed_at` and already PIT-safe to slice (the caller prefetched at least
/// `[source_cutoff - lookback, decision_at)`).
pub fn build_domain_slice_inputs<S: BuildHasher>(
    category: MarketCategory,
    linkages: &[MarketLinkage],
    boundary: &DecisionBoundary,
    domain: &DomainConfig,
    observations: &HashMap<DomainInstrumentKey, Vec<DomainObservation>, S>,
) -> QuantResult<Option<DomainSliceInputs>> {
    let Some(family) = DomainFamily::for_category(category) else {
        return Ok(None);
    };
    if !domain.family_enabled(family) {
        return Ok(None);
    }
    let Some(linkage) = linkage_valid_at(linkages, boundary) else {
        return Ok(None);
    };
    if linkage.domain_family != family {
        return Ok(None);
    }
    let Some(binding) = linkage.binding().cloned() else {
        return Ok(None);
    };

    let lookback_secs = i64::try_from(crypto_lookback_secs(domain)).map_err(|error| {
        QuantError::config(format!(
            "domain lookback does not fit chrono seconds: {error}"
        ))
    })?;
    let lookback = ChronoDuration::seconds(lookback_secs);
    let cutoff = boundary.cutoff_for(DecisionSource::DomainCrypto);
    let from = cutoff - lookback;

    let primary = observation_window(
        observations,
        &binding.instrument_key,
        from,
        cutoff,
        boundary.decision_at(),
    );
    let oracle = oracle_instrument(&binding)
        .map(|key| observation_window(observations, &key, from, cutoff, boundary.decision_at()));

    Ok(Some(DomainSliceInputs {
        family,
        binding,
        linkage_evidence: linkage_evidence(linkage),
        primary,
        oracle,
    }))
}

/// The settlement-oracle instrument to cross-check against.
///
/// Chainlink feeds only; a Binance-settled market needs no second source, and an
/// unrecognized oracle stays fail-closed.
#[must_use]
pub fn oracle_instrument(binding: &ResolvedBinding) -> Option<DomainInstrumentKey> {
    let MarketSubject::Crypto(subject) = &binding.subject;
    match &subject.resolution_oracle {
        ResolutionOracle::ChainlinkDataStreams { feed } => {
            Some(DomainInstrumentKey::chainlink_feed(feed))
        }
        ResolutionOracle::BinanceKline { .. } | ResolutionOracle::Other { .. } => None,
    }
}

/// Slice one instrument's series into a PIT window `[from, cutoff]`.
fn observation_window<S: BuildHasher>(
    observations: &HashMap<DomainInstrumentKey, Vec<DomainObservation>, S>,
    instrument_key: &DomainInstrumentKey,
    from: DateTime<Utc>,
    cutoff: DateTime<Utc>,
    decision_at: DateTime<Utc>,
) -> DomainObservationWindow {
    let slice = observations
        .get(instrument_key)
        .map(|series| {
            series
                .iter()
                .filter(|observation| {
                    observation.observed_at >= from
                        && observation.observed_at <= cutoff
                        && observation.publish_time <= cutoff
                        && observation
                            .available_at
                            .is_some_and(|available_at| available_at <= decision_at)
                })
                .cloned()
                .collect()
        })
        .unwrap_or_default();
    DomainObservationWindow {
        cutoff,
        observations: slice,
    }
}

#[cfg(test)]
mod tests {
    use super::{build_domain_slice_inputs, domain_availability_at, linkage_valid_at};
    use chrono::{DateTime, Duration, TimeZone, Utc};
    use quant_pivot_models::{
        domain::{
            CryptoSubject, DecisionBoundary, DecisionClock, DecisionSource, DomainAvailability,
            DomainObservation, GroundingProof, LinkageOutcome, MarketLinkage, MarketSubject,
            PriceComparator, ResolutionOracle, ResolvedBinding,
        },
        enums::{
            common::MarketCategory,
            domain::{DomainFamily, DomainMetric, KlineInterval, ResolverTier},
        },
        runtime_config::DomainConfig,
        types::{
            BinanceSymbol, ChainlinkFeedKey, ContentHash, CryptoAsset, CryptoQuote,
            DomainInstrumentKey, DomainSourceId, MarketId, MarketLinkageId, Probability,
            ResolverVersion,
        },
    };
    use rust_decimal_macros::dec;
    use std::collections::HashMap;

    fn instrument() -> DomainInstrumentKey {
        DomainInstrumentKey::binance_kline(
            &BinanceSymbol::parse("BTCUSDT").expect("symbol"),
            KlineInterval::OneMinute,
        )
    }

    fn binding() -> ResolvedBinding {
        let now = Utc.with_ymd_and_hms(2026, 7, 1, 12, 0, 0).unwrap();
        ResolvedBinding {
            subject: MarketSubject::Crypto(CryptoSubject {
                asset: CryptoAsset::parse("BTC").expect("asset"),
                quote: CryptoQuote::parse("USD").expect("quote"),
                comparator: PriceComparator::UpVsReference,
                strike: None,
                reference_at: Some(now - Duration::minutes(5)),
                observation_at: now,
                resolution_oracle: ResolutionOracle::ChainlinkDataStreams {
                    feed: ChainlinkFeedKey::parse("BTC-USD").expect("feed"),
                },
            }),
            instrument_key: instrument(),
            grounding: GroundingProof { spans: Vec::new() },
            override_context: None,
        }
    }

    fn linkage(outcome: LinkageOutcome, derived_minute: u32) -> MarketLinkage {
        let market_id = MarketId::new("0xmarket");
        let metadata_hash = ContentHash::parse(format!("blake3:{}", "0".repeat(64))).expect("hash");
        let content_hash = MarketLinkage::compute_content_hash(
            &market_id,
            DomainFamily::Crypto,
            &outcome,
            ResolverTier::Tier0Slug,
            ResolverVersion::FIRST,
            &metadata_hash,
        )
        .expect("hash");
        let effective_at = Utc
            .with_ymd_and_hms(2026, 7, 1, 11, derived_minute, 0)
            .unwrap();
        MarketLinkage {
            linkage_id: MarketLinkageId::from_v7(),
            market_id,
            domain_family: DomainFamily::Crypto,
            outcome,
            confidence: Probability::ONE,
            resolver_tier: ResolverTier::Tier0Slug,
            resolver_version: ResolverVersion::FIRST,
            metadata_hash,
            content_hash,
            effective_at,
            available_at: effective_at,
        }
    }

    fn boundary(as_of: DateTime<Utc>, domain: &DomainConfig) -> DecisionBoundary {
        DecisionClock::new(0)
            .boundary(as_of)
            .expect("boundary")
            .with_source_cutoff(
                DecisionSource::DomainCrypto,
                domain.crypto.availability_lag_secs,
            )
            .expect("domain cutoff")
    }

    #[test]
    fn pit_valid_linkage_never_reads_a_future_revision() {
        let early = linkage(LinkageOutcome::Resolved(binding()), 0);
        let late = linkage(
            LinkageOutcome::Unresolved {
                reason: "metadata revised".to_owned(),
            },
            30,
        );
        let linkages = vec![late.clone(), early.clone()];
        let domain = DomainConfig::default();

        let mid = Utc.with_ymd_and_hms(2026, 7, 1, 11, 15, 0).unwrap();
        assert_eq!(
            linkage_valid_at(&linkages, &boundary(mid, &domain))
                .expect("valid")
                .linkage_id,
            early.linkage_id,
            "a decision before the revision must see the earlier record"
        );

        let after = Utc.with_ymd_and_hms(2026, 7, 1, 11, 45, 0).unwrap();
        assert_eq!(
            linkage_valid_at(&linkages, &boundary(after, &domain))
                .expect("valid")
                .linkage_id,
            late.linkage_id,
            "a decision after the revision must see the latest record"
        );

        let before = Utc.with_ymd_and_hms(2026, 7, 1, 10, 0, 0).unwrap();
        assert!(
            linkage_valid_at(&linkages, &boundary(before, &domain)).is_none(),
            "no record was effective by this decision boundary"
        );
    }

    #[test]
    fn backdated_linkage_is_invisible_until_its_availability_time() {
        let early = linkage(LinkageOutcome::Resolved(binding()), 0);
        let mut backdated = linkage(
            LinkageOutcome::Unresolved {
                reason: "late correction".to_owned(),
            },
            10,
        );
        backdated.available_at = Utc.with_ymd_and_hms(2026, 7, 1, 11, 30, 0).unwrap();
        let rows = [early.clone(), backdated.clone()];

        let before_available = DecisionClock::new(9 * 60)
            .boundary(Utc.with_ymd_and_hms(2026, 7, 1, 11, 20, 0).unwrap())
            .expect("boundary");
        assert_eq!(
            linkage_valid_at(&rows, &before_available)
                .expect("early row")
                .linkage_id,
            early.linkage_id
        );

        let after_available = DecisionClock::new(20 * 60)
            .boundary(Utc.with_ymd_and_hms(2026, 7, 1, 11, 31, 0).unwrap())
            .expect("boundary");
        assert!(
            backdated.available_at > after_available.cutoff_for(DecisionSource::Linkage),
            "availability intentionally falls after the source cutoff"
        );
        assert_eq!(
            linkage_valid_at(&rows, &after_available)
                .expect("correction row")
                .linkage_id,
            backdated.linkage_id,
            "availability is bounded by decision_at, not by source_cutoff"
        );
    }

    #[test]
    fn linkage_ties_use_the_stable_id_order() {
        let domain = DomainConfig::default();
        let mut lower_id = linkage(LinkageOutcome::Resolved(binding()), 0);
        lower_id.linkage_id = MarketLinkageId::new(uuid::Uuid::from_u128(1));
        let mut higher_id = linkage(
            LinkageOutcome::Unresolved {
                reason: "same-clock correction".to_owned(),
            },
            0,
        );
        higher_id.linkage_id = MarketLinkageId::new(uuid::Uuid::from_u128(2));

        let at = boundary(Utc.with_ymd_and_hms(2026, 7, 1, 11, 1, 0).unwrap(), &domain);
        assert_eq!(
            linkage_valid_at(&[higher_id.clone(), lower_id], &at)
                .expect("tie resolved")
                .linkage_id,
            higher_id.linkage_id,
            "stable UUID ordering must match the repository's final ORDER BY key"
        );
    }

    #[test]
    fn slice_inputs_fail_closed_per_rung() {
        let as_of = Utc.with_ymd_and_hms(2026, 7, 1, 12, 0, 0).unwrap();
        let domain = DomainConfig::default();
        let boundary = boundary(as_of, &domain);
        let observations = HashMap::new();
        let resolved = vec![linkage(LinkageOutcome::Resolved(binding()), 0)];

        assert!(
            build_domain_slice_inputs(
                MarketCategory::Sports,
                &resolved,
                &boundary,
                &domain,
                &observations
            )
            .expect("slice build")
            .is_none()
        );

        assert!(
            build_domain_slice_inputs(
                MarketCategory::Crypto,
                &[],
                &boundary,
                &domain,
                &observations,
            )
            .expect("slice build")
            .is_none()
        );

        let unresolved = vec![linkage(
            LinkageOutcome::Unresolved {
                reason: "no template".to_owned(),
            },
            0,
        )];
        assert!(
            build_domain_slice_inputs(
                MarketCategory::Crypto,
                &unresolved,
                &boundary,
                &domain,
                &observations
            )
            .expect("slice build")
            .is_none()
        );

        let inputs = build_domain_slice_inputs(
            MarketCategory::Crypto,
            &resolved,
            &boundary,
            &domain,
            &observations,
        )
        .expect("slice build")
        .expect("slice applies");
        assert_eq!(inputs.family, DomainFamily::Crypto);
        assert!(
            inputs.oracle.is_some(),
            "chainlink-settled subject carries an oracle window"
        );
    }

    #[test]
    fn observation_windows_respect_the_visibility_cutoff() {
        let as_of = Utc.with_ymd_and_hms(2026, 7, 1, 12, 0, 0).unwrap();
        let domain = DomainConfig::default();
        let boundary = boundary(as_of, &domain);
        let visible = as_of - Duration::minutes(1);
        let too_fresh = as_of - Duration::seconds(1);
        let make = |at| DomainObservation {
            family: DomainFamily::Crypto,
            source_id: DomainSourceId::binance(),
            instrument_key: instrument(),
            metric: DomainMetric::Close,
            value: dec!(100000),
            observed_at: at,
            publish_time: at,
            available_at: Some(at),
        };
        let observations = HashMap::from([(instrument(), vec![make(visible), make(too_fresh)])]);
        let resolved = vec![linkage(LinkageOutcome::Resolved(binding()), 0)];
        let inputs = build_domain_slice_inputs(
            MarketCategory::Crypto,
            &resolved,
            &boundary,
            &domain,
            &observations,
        )
        .expect("slice build")
        .expect("slice applies");
        assert_eq!(
            inputs.primary.observations.len(),
            1,
            "only observations at or before the frozen source cutoff are visible"
        );
        assert_eq!(inputs.primary.observations[0].observed_at, visible);
    }

    fn close_observation(at: DateTime<Utc>) -> DomainObservation {
        DomainObservation {
            family: DomainFamily::Crypto,
            source_id: DomainSourceId::binance(),
            instrument_key: instrument(),
            metric: DomainMetric::Close,
            value: dec!(100000),
            observed_at: at,
            publish_time: at,
            available_at: Some(at),
        }
    }

    #[test]
    fn availability_is_not_mapped_for_an_unrouted_category_or_disabled_family() {
        let as_of = Utc.with_ymd_and_hms(2026, 7, 1, 12, 0, 0).unwrap();
        let domain = DomainConfig::default();
        let boundary = boundary(as_of, &domain);
        let resolved = vec![linkage(LinkageOutcome::Resolved(binding()), 0)];

        assert_eq!(
            domain_availability_at(
                MarketCategory::Sports,
                &resolved,
                &boundary,
                &domain,
                &HashMap::new()
            ),
            DomainAvailability::NotMapped,
            "a category with no domain family must never gate on domain evidence"
        );

        let mut disabled_domain = DomainConfig::default();
        disabled_domain
            .enabled_by_family
            .insert(DomainFamily::Crypto, false);
        assert_eq!(
            domain_availability_at(
                MarketCategory::Crypto,
                &resolved,
                &boundary,
                &disabled_domain,
                &HashMap::new()
            ),
            DomainAvailability::NotMapped,
            "a disabled vertical must behave exactly like an unmapped category"
        );
    }

    #[test]
    fn availability_is_unresolved_without_a_pit_valid_resolved_linkage() {
        let as_of = Utc.with_ymd_and_hms(2026, 7, 1, 12, 0, 0).unwrap();
        let domain = DomainConfig::default();
        let boundary = boundary(as_of, &domain);

        assert_eq!(
            domain_availability_at(
                MarketCategory::Crypto,
                &[],
                &boundary,
                &domain,
                &HashMap::new(),
            ),
            DomainAvailability::Unresolved,
            "no ledger row at all must fail closed to Unresolved"
        );

        let unresolved = vec![linkage(
            LinkageOutcome::Unresolved {
                reason: "no template matched".to_owned(),
            },
            0,
        )];
        assert_eq!(
            domain_availability_at(
                MarketCategory::Crypto,
                &unresolved,
                &boundary,
                &domain,
                &HashMap::new()
            ),
            DomainAvailability::Unresolved,
            "an Unresolved outcome must never be treated as mapped-but-missing-data"
        );
    }

    #[test]
    fn availability_distinguishes_source_empty_from_available_at_the_cutoff() {
        let as_of = Utc.with_ymd_and_hms(2026, 7, 1, 12, 0, 0).unwrap();
        let domain = DomainConfig::default();
        let boundary = boundary(as_of, &domain);
        let resolved = vec![linkage(LinkageOutcome::Resolved(binding()), 0)];

        assert_eq!(
            domain_availability_at(
                MarketCategory::Crypto,
                &resolved,
                &boundary,
                &domain,
                &HashMap::new()
            ),
            DomainAvailability::SourceEmpty,
            "resolved linkage with no observation series must be SourceEmpty, never fabricated"
        );

        // Chainlink knowledge_lag_secs default is 5s; an observation exactly at
        // the cutoff (as_of - 5s) is visible, one strictly inside the delay
        // window is not.
        let visible_at = as_of - Duration::seconds(5);
        let too_fresh_at = as_of - Duration::seconds(1);
        let visible_only = HashMap::from([(instrument(), vec![close_observation(visible_at)])]);
        assert_eq!(
            domain_availability_at(
                MarketCategory::Crypto,
                &resolved,
                &boundary,
                &domain,
                &visible_only
            ),
            DomainAvailability::Available,
            "an observation at or before the source-delayed cutoff must be Available"
        );

        let too_fresh_only = HashMap::from([(instrument(), vec![close_observation(too_fresh_at)])]);
        assert_eq!(
            domain_availability_at(
                MarketCategory::Crypto,
                &resolved,
                &boundary,
                &domain,
                &too_fresh_only
            ),
            DomainAvailability::SourceEmpty,
            "an observation still inside the source-delay window must not count as visible"
        );
    }
}

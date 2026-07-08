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
//! 3. no linkage record is PIT-valid at `as_of`;
//! 4. the PIT-valid record is `Unresolved` (no binding).

use std::collections::HashMap;
use std::hash::BuildHasher;

use chrono::{DateTime, Duration as ChronoDuration, Utc};
use quant_pivot_models::{
    domain::{DomainObservation, MarketLinkage, MarketSubject, ResolutionOracle, ResolvedBinding},
    enums::{common::MarketCategory, domain::DomainFamily},
    runtime_config::DomainConfig,
    types::DomainInstrumentKey,
};

use crate::{domain::DomainObservationWindow, features::DomainSliceInputs};

/// The trailing observation lookback (seconds) the crypto domain slice needs:
/// the widest of the momentum / volatility feature windows.
#[must_use]
pub fn crypto_lookback_secs(domain: &DomainConfig) -> u64 {
    domain
        .crypto
        .momentum_window_secs
        .max(domain.crypto.volatility_window_secs)
}

/// The PIT-valid linkage at `as_of`: the latest record derived at or before
/// `as_of` (bitemporal knowledge axis — never a future revision).
///
/// `linkages` may arrive in any order; ties on `derived_at` break on
/// `created_at` then `linkage_id` — byte-identical to the Postgres
/// repository's `ORDER BY derived_at DESC, created_at DESC, linkage_id DESC`,
/// so the online (`valid_at`) and offline (`ledger_for_markets` + this
/// function) planes can never pick a different revision for the same
/// `(market, as_of)` on a tie.
#[must_use]
pub fn linkage_valid_at(
    linkages: &[MarketLinkage],
    as_of: DateTime<Utc>,
) -> Option<&MarketLinkage> {
    linkages
        .iter()
        .filter(|linkage| linkage.derived_at <= as_of)
        .max_by(|a, b| {
            (a.derived_at, a.created_at, &a.linkage_id.to_string()).cmp(&(
                b.derived_at,
                b.created_at,
                &b.linkage_id.to_string(),
            ))
        })
}

/// Assemble the optional domain-slice inputs for one `(market, as_of)`.
///
/// `observations` is keyed by instrument, each series ascending by
/// `observed_at` and already PIT-safe to slice (the caller prefetched at least
/// `[as_of - source_delay - lookback, as_of)`).
#[must_use]
pub fn build_domain_slice_inputs<S: BuildHasher>(
    category: MarketCategory,
    linkages: &[MarketLinkage],
    as_of: DateTime<Utc>,
    domain: &DomainConfig,
    observations: &HashMap<DomainInstrumentKey, Vec<DomainObservation>, S>,
) -> Option<DomainSliceInputs> {
    let family = DomainFamily::for_category(category)?;
    if !domain.family_enabled(family) {
        return None;
    }
    let linkage = linkage_valid_at(linkages, as_of)?;
    if linkage.domain_family != family {
        return None;
    }
    let binding = linkage.binding()?.clone();

    let source_delay =
        ChronoDuration::seconds(i64::try_from(domain.crypto.source_delay_secs).unwrap_or(0));
    let lookback =
        ChronoDuration::seconds(i64::try_from(crypto_lookback_secs(domain)).unwrap_or(0));
    let cutoff = as_of - source_delay;
    let from = cutoff - lookback;

    let primary = observation_window(observations, &binding.instrument_key, from, cutoff);
    let oracle =
        oracle_instrument(&binding).map(|key| observation_window(observations, &key, from, cutoff));

    Some(DomainSliceInputs {
        family,
        binding,
        primary,
        oracle,
    })
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
) -> DomainObservationWindow {
    let slice = observations
        .get(instrument_key)
        .map(|series| {
            series
                .iter()
                .filter(|observation| {
                    observation.observed_at >= from && observation.observed_at <= cutoff
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
    use super::{build_domain_slice_inputs, linkage_valid_at};
    use chrono::{Duration, TimeZone, Utc};
    use quant_pivot_models::{
        domain::{
            CryptoSubject, DomainObservation, GroundingProof, LinkageOutcome, MarketLinkage,
            MarketSubject, PriceComparator, ResolutionOracle, ResolvedBinding,
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
        let derived_at = Utc
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
            derived_at,
            created_at: derived_at,
        }
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

        let mid = Utc.with_ymd_and_hms(2026, 7, 1, 11, 15, 0).unwrap();
        assert_eq!(
            linkage_valid_at(&linkages, mid).expect("valid").linkage_id,
            early.linkage_id,
            "as_of before the revision must see the earlier record"
        );

        let after = Utc.with_ymd_and_hms(2026, 7, 1, 11, 45, 0).unwrap();
        assert_eq!(
            linkage_valid_at(&linkages, after)
                .expect("valid")
                .linkage_id,
            late.linkage_id,
            "as_of after the revision must see the latest record"
        );

        let before = Utc.with_ymd_and_hms(2026, 7, 1, 10, 0, 0).unwrap();
        assert!(
            linkage_valid_at(&linkages, before).is_none(),
            "no record was derived yet at this as_of"
        );
    }

    #[test]
    fn slice_inputs_fail_closed_per_rung() {
        let as_of = Utc.with_ymd_and_hms(2026, 7, 1, 12, 0, 0).unwrap();
        let domain = DomainConfig::default();
        let observations = HashMap::new();
        let resolved = vec![linkage(LinkageOutcome::Resolved(binding()), 0)];

        assert!(
            build_domain_slice_inputs(
                MarketCategory::Sports,
                &resolved,
                as_of,
                &domain,
                &observations
            )
            .is_none()
        );

        assert!(
            build_domain_slice_inputs(MarketCategory::Crypto, &[], as_of, &domain, &observations)
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
                as_of,
                &domain,
                &observations
            )
            .is_none()
        );

        let inputs = build_domain_slice_inputs(
            MarketCategory::Crypto,
            &resolved,
            as_of,
            &domain,
            &observations,
        )
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
        };
        let observations = HashMap::from([(instrument(), vec![make(visible), make(too_fresh)])]);
        let resolved = vec![linkage(LinkageOutcome::Resolved(binding()), 0)];
        let inputs = build_domain_slice_inputs(
            MarketCategory::Crypto,
            &resolved,
            as_of,
            &domain,
            &observations,
        )
        .expect("slice applies");
        assert_eq!(
            inputs.primary.observations.len(),
            1,
            "only the observation at or before as_of - source_delay is visible"
        );
        assert_eq!(inputs.primary.observations[0].observed_at, visible);
    }
}

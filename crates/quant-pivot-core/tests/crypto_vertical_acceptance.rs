//! Phase 11.2.2 §9 acceptance tests (core pipeline plane).

use std::collections::HashMap;

use chrono::{Duration, TimeZone, Utc};
use quant_pivot_core::pipeline::domain_pit::{build_domain_slice_inputs, linkage_valid_at};
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
        derived_at: Utc
            .with_ymd_and_hms(2026, 7, 1, 11, derived_minute, 0)
            .unwrap(),
    }
}

#[test]
fn linkage_pit_uses_metadata_version_not_future_revision() {
    let early = linkage(LinkageOutcome::Resolved(binding()), 0);
    let late = linkage(
        LinkageOutcome::Unresolved {
            reason: "metadata revised".to_owned(),
        },
        30,
    );
    let linkages = vec![late, early.clone()];
    let mid = Utc.with_ymd_and_hms(2026, 7, 1, 11, 15, 0).unwrap();
    assert_eq!(
        linkage_valid_at(&linkages, mid).expect("valid").linkage_id,
        early.linkage_id
    );
}

#[test]
fn domain_observation_pit_excludes_after_as_of_minus_delay() {
    let as_of = Utc.with_ymd_and_hms(2026, 7, 1, 12, 0, 0).unwrap();
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
        &DomainConfig::default(),
        &observations,
    )
    .expect("slice");
    assert_eq!(inputs.primary.observations.len(), 1);
    assert_eq!(inputs.primary.observations[0].observed_at, visible);
}

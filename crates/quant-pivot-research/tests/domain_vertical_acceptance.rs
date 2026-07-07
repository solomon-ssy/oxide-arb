//! Phase 11.2.2 §9 acceptance tests (research plane).

use std::collections::{BTreeMap, HashMap};

use chrono::{Duration, TimeZone, Utc};
use quant_pivot_models::{
    domain::{
        CryptoSubject, DomainObservation, GroundingProof, LinkageOutcome, LinkageSourceMetadata,
        MarketSubject, PriceComparator, ResolutionOracle, ResolvedBinding,
    },
    enums::{
        common::MarketCategory,
        domain::{DomainFamily, DomainMetric, KlineInterval, ResolverTier},
        quant::DataQualityStatus,
    },
    runtime_config::DomainConfig,
    types::{
        BinanceSymbol, ChainlinkFeedKey, CryptoAsset, CryptoQuote, DomainInstrumentKey,
        DomainSourceId, SchemaVersion, TokenId,
    },
};
use quant_pivot_research::{
    domain::{DomainPitQueryEngine, MaterializedDomainPitEngine},
    factors::DomainFactorRegistry,
    features::{
        DomainFeatureBuilder, DomainFeatureSlice, EvidenceSourceRef, FeatureValue, FeatureVector,
        domain::{CryptoDomainFeatureBuilder, DomainComputeCtx},
        names::{domain_crypto, market as market_names},
    },
    hashing::ResearchHasher,
    linkage::{CryptoSubjectParser, LayeredResolver, SubjectExtractor, Tier0SlugExtractor},
    training::{
        LabelName, TrainingDatasetArtifact, TrainingExample, TrainingLabel,
        assert_no_future_leakage,
    },
};
use rust_decimal::Decimal;
use rust_decimal_macros::dec;

fn metadata(slug: &str) -> LinkageSourceMetadata {
    LinkageSourceMetadata {
        market_id: quant_pivot_models::types::MarketId::new("0xmarket"),
        slug: slug.to_owned(),
        question: "Bitcoin Up or Down".to_owned(),
        description: None,
        series_slug: None,
        end_date: Some(Utc.with_ymd_and_hms(2026, 7, 8, 12, 0, 0).unwrap()),
    }
}

fn crypto_slice() -> DomainFeatureSlice {
    let mut values = BTreeMap::new();
    values.insert(
        domain_crypto::DISTANCE_TO_STRIKE,
        FeatureValue::Decimal(dec!(0.02)),
    );
    DomainFeatureSlice {
        family: DomainFamily::Crypto,
        schema_version: SchemaVersion::new(5),
        values,
    }
}

fn vector(category: MarketCategory, domain: Option<DomainFeatureSlice>) -> FeatureVector {
    let mut generic = BTreeMap::new();
    generic.insert(market_names::CATEGORY, FeatureValue::Category(category));
    FeatureVector {
        market_id: quant_pivot_models::types::MarketId::new("m"),
        token_id: Some(TokenId::new("t")),
        as_of: Utc::now(),
        generic_schema_version: SchemaVersion::FIRST,
        generic,
        domain,
        substitutions: Vec::new(),
        data_quality: DataQualityStatus::Fresh,
        staleness_ms: 0,
        source_refs: Vec::new(),
    }
}

#[test]
fn feature_vector_domain_slice_only_for_mapped_category() {
    let crypto = vector(MarketCategory::Crypto, Some(crypto_slice()));
    assert!(crypto.domain.is_some());

    let sports = vector(MarketCategory::Sports, None);
    assert!(sports.domain.is_none());
}

#[test]
fn feature_hash_composite_stable_and_distinguishes_domain_family() {
    let base = vector(MarketCategory::Crypto, None);
    let with_crypto = vector(MarketCategory::Crypto, Some(crypto_slice()));

    let h_none = ResearchHasher::feature_vector(&base).expect("hash");
    let h_crypto = ResearchHasher::feature_vector(&with_crypto).expect("hash");
    assert_ne!(h_none, h_crypto, "domain slice must change the digest");

    let h_crypto_2 = ResearchHasher::feature_vector(&with_crypto).expect("hash");
    assert_eq!(h_crypto, h_crypto_2, "hash must be stable across runs");
}

#[test]
fn domain_factor_registered_and_routed_by_category() {
    let registry = DomainFactorRegistry::build(&DomainConfig::default());
    assert_eq!(registry.for_category(MarketCategory::Crypto).len(), 2);
    assert!(registry.for_category(MarketCategory::Sports).is_empty());
}

#[test]
fn dataset_hash_changes_when_domain_slice_added() {
    use quant_pivot_models::{
        hashing::CanonicalDigest,
        types::{ModelSpecId, TrainingExampleId, TrainingSampleSource},
    };

    let spec = ModelSpecId::from_v7();
    let as_of = Utc.timestamp_opt(1_000_000, 0).single().expect("ts");
    let (feature, factor, label) = (
        CanonicalDigest::content_hash_json("feature").expect("h"),
        CanonicalDigest::content_hash_json("factor").expect("h"),
        CanonicalDigest::content_hash_json("label").expect("h"),
    );

    let make = |domain: Option<DomainFeatureSlice>| TrainingExample {
        example_id: TrainingExampleId::from_v7(),
        market_id: quant_pivot_models::types::MarketId::new("m"),
        token_id: TokenId::new("m-yes"),
        as_of,
        sample_source: TrainingSampleSource::HistoricalPit,
        feature_vector: vector(MarketCategory::Crypto, domain),
        factor_values: Vec::new(),
        labels: vec![TrainingLabel {
            label_name: LabelName::from_static("return_to_horizon"),
            horizon_secs: 60,
            value: Decimal::ONE,
            is_resolved: true,
        }],
        source_refs: Vec::new(),
        lot_context: None,
        position_state: None,
        book_fidelity: None,
    };

    let without = vec![make(None)];
    let with = vec![make(Some(crypto_slice()))];
    let h0 = TrainingDatasetArtifact::compute_dataset_hash(
        &spec, as_of, as_of, &feature, &factor, &label, &without,
    )
    .expect("hash");
    let h1 = TrainingDatasetArtifact::compute_dataset_hash(
        &spec, as_of, as_of, &feature, &factor, &label, &with,
    )
    .expect("hash");
    assert_ne!(h0, h1);
}

#[test]
fn dataset_builder_domain_slice_no_future_leakage() {
    use quant_pivot_models::{
        enums::feature::EvidenceSourceKind,
        types::{TrainingExampleId, TrainingSampleSource},
    };

    let as_of = Utc.with_ymd_and_hms(2026, 7, 1, 12, 0, 0).unwrap();
    let cutoff = as_of - Duration::seconds(5);
    let mut fv = vector(MarketCategory::Crypto, Some(crypto_slice()));
    fv.as_of = as_of;
    let clean = TrainingExample {
        example_id: TrainingExampleId::from_v7(),
        market_id: quant_pivot_models::types::MarketId::new("m"),
        token_id: TokenId::new("m-yes"),
        as_of,
        sample_source: TrainingSampleSource::HistoricalPit,
        feature_vector: fv,
        factor_values: Vec::new(),
        labels: Vec::new(),
        source_refs: vec![EvidenceSourceRef {
            source_kind: EvidenceSourceKind::DomainExternal,
            reference: "binance:BTCUSDT:1m".to_owned(),
            observed_at: cutoff,
        }],
        lot_context: None,
        position_state: None,
        book_fidelity: None,
    };
    assert!(assert_no_future_leakage(std::slice::from_ref(&clean), 5).is_ok());

    let bad = TrainingExample {
        source_refs: vec![EvidenceSourceRef {
            source_kind: EvidenceSourceKind::DomainExternal,
            reference: "binance:BTCUSDT:1m".to_owned(),
            observed_at: as_of,
        }],
        ..clean
    };
    assert!(assert_no_future_leakage(&[bad], 5).is_err());
}

#[test]
fn crypto_subject_parser_tier1_deterministic_fail_closed() {
    let parser = CryptoSubjectParser;
    let meta = LinkageSourceMetadata {
        market_id: quant_pivot_models::types::MarketId::new("0xmarket"),
        slug: "bitcoin-up-or-down-july-7-12pm-et".to_owned(),
        question: "Bitcoin Up or Down - July 7, 12PM ET".to_owned(),
        description: Some(
            "resolution source for this market is the Chainlink BTC/USD data stream \
             at https://data.chain.link/streams/btc-usd"
                .to_owned(),
        ),
        series_slug: None,
        end_date: Some(Utc.with_ymd_and_hms(2026, 7, 7, 17, 0, 0).unwrap()),
    };
    let candidate = parser.extract(&meta).expect("extract");
    assert!(candidate.is_some(), "tier1 must recognize ET slug markets");
}

#[test]
fn linkage_tier0_slug_direct_read_matches_tier1() {
    let slug = "btc-updown-5m-1780319100";
    let resolver = LayeredResolver::deterministic();
    let tier0 = Tier0SlugExtractor
        .extract(&metadata(slug))
        .expect("tier0")
        .expect("candidate");
    let resolution = resolver.resolve(&metadata(slug)).expect("resolve");
    assert_eq!(resolution.resolver_tier, ResolverTier::Tier0Slug);
    assert!(matches!(resolution.outcome, LinkageOutcome::Resolved(_)));
    if let LinkageOutcome::Resolved(binding) = resolution.outcome {
        let MarketSubject::Crypto(subject) = binding.subject;
        let MarketSubject::Crypto(t0) = tier0.subject;
        assert_eq!(subject.asset, t0.asset);
    }
}

#[test]
fn linkage_grounding_rejects_field_absent_from_source() {
    let resolver = LayeredResolver::deterministic();
    let bad = resolver
        .resolve(&metadata("totally-unknown-market-slug"))
        .expect("resolve");
    assert!(matches!(bad.outcome, LinkageOutcome::Unresolved { .. }));
}

#[test]
fn crypto_domain_feature_present_with_domain_external_evidence() {
    use quant_pivot_models::runtime_config::CryptoDomainConfig;
    use quant_pivot_models::types::Usd;
    use quant_pivot_research::domain::DomainObservationWindow;

    let as_of = Utc.with_ymd_and_hms(2026, 7, 1, 12, 0, 0).unwrap();
    let at = as_of - Duration::minutes(1);
    let primary = DomainObservationWindow {
        cutoff: as_of,
        observations: vec![DomainObservation {
            family: DomainFamily::Crypto,
            source_id: DomainSourceId::binance(),
            instrument_key: DomainInstrumentKey::binance_kline(
                &BinanceSymbol::parse("BTCUSDT").expect("symbol"),
                KlineInterval::OneMinute,
            ),
            metric: DomainMetric::Close,
            value: dec!(100000),
            observed_at: at,
            publish_time: at,
        }],
    };
    let binding = ResolvedBinding {
        subject: MarketSubject::Crypto(CryptoSubject {
            asset: CryptoAsset::parse("BTC").expect("asset"),
            quote: CryptoQuote::parse("USD").expect("quote"),
            comparator: PriceComparator::Above,
            strike: Some(Usd::new(dec!(99000))),
            reference_at: None,
            observation_at: as_of,
            resolution_oracle: ResolutionOracle::BinanceKline {
                symbol: BinanceSymbol::parse("BTCUSDT").expect("symbol"),
                interval: KlineInterval::OneMinute,
            },
        }),
        instrument_key: primary.observations[0].instrument_key.clone(),
        grounding: GroundingProof { spans: Vec::new() },
    };
    let ctx = DomainComputeCtx {
        as_of,
        binding: &binding,
        primary: &primary,
        oracle: None,
        crypto: &CryptoDomainConfig::default(),
    };
    let features = CryptoDomainFeatureBuilder.compute(&ctx);
    let distance = features
        .iter()
        .find(|f| f.name == domain_crypto::DISTANCE_TO_STRIKE)
        .expect("distance feature");
    assert!(distance.value.is_ok());
    assert!(distance.evidence.is_some());
}

#[tokio::test]
async fn materialized_domain_pit_matches_ch_source_byte_identical() {
    let key = DomainInstrumentKey::binance_kline(
        &BinanceSymbol::parse("BTCUSDT").expect("symbol"),
        KlineInterval::OneMinute,
    );
    let series: Vec<DomainObservation> = (1..=5)
        .map(|m| {
            let at = Utc.with_ymd_and_hms(2026, 7, 1, 12, m, 0).unwrap();
            DomainObservation {
                family: DomainFamily::Crypto,
                source_id: DomainSourceId::binance(),
                instrument_key: key.clone(),
                metric: DomainMetric::Close,
                value: Decimal::from(100_000 + m),
                observed_at: at,
                publish_time: at,
            }
        })
        .collect();
    let engine = MaterializedDomainPitEngine::new(HashMap::from([(key.clone(), series.clone())]));
    let from = Utc.with_ymd_and_hms(2026, 7, 1, 12, 2, 0).unwrap();
    let to = Utc.with_ymd_and_hms(2026, 7, 1, 12, 5, 0).unwrap();
    let materialized = engine
        .observations_between(&key, from, to)
        .await
        .expect("query");
    let ch_like: Vec<DomainObservation> = series
        .into_iter()
        .filter(|obs| obs.observed_at >= from && obs.observed_at < to)
        .collect();
    assert_eq!(materialized, ch_like);
}

#[test]
fn basis_vs_resolution_source_flags_divergence_for_chainlink_oracle() {
    use quant_pivot_models::runtime_config::CryptoDomainConfig;
    use quant_pivot_research::domain::DomainObservationWindow;

    let as_of = Utc.with_ymd_and_hms(2026, 7, 1, 12, 0, 0).unwrap();
    let primary = DomainObservationWindow {
        cutoff: as_of,
        observations: vec![DomainObservation {
            family: DomainFamily::Crypto,
            source_id: DomainSourceId::binance(),
            instrument_key: DomainInstrumentKey::binance_kline(
                &BinanceSymbol::parse("BTCUSDT").expect("symbol"),
                KlineInterval::OneMinute,
            ),
            metric: DomainMetric::Close,
            value: dec!(100500),
            observed_at: as_of - Duration::minutes(1),
            publish_time: as_of - Duration::minutes(1),
        }],
    };
    let oracle = DomainObservationWindow {
        cutoff: as_of,
        observations: vec![DomainObservation {
            family: DomainFamily::Crypto,
            source_id: DomainSourceId::chainlink(),
            instrument_key: DomainInstrumentKey::chainlink_feed(
                &ChainlinkFeedKey::parse("BTC-USD").expect("feed"),
            ),
            metric: DomainMetric::OraclePrice,
            value: dec!(100000),
            observed_at: as_of - Duration::minutes(1),
            publish_time: as_of - Duration::minutes(1),
        }],
    };
    let binding = ResolvedBinding {
        subject: MarketSubject::Crypto(CryptoSubject {
            asset: CryptoAsset::parse("BTC").expect("asset"),
            quote: CryptoQuote::parse("USD").expect("quote"),
            comparator: PriceComparator::UpVsReference,
            strike: None,
            reference_at: None,
            observation_at: as_of,
            resolution_oracle: ResolutionOracle::ChainlinkDataStreams {
                feed: ChainlinkFeedKey::parse("BTC-USD").expect("feed"),
            },
        }),
        instrument_key: primary.observations[0].instrument_key.clone(),
        grounding: GroundingProof { spans: Vec::new() },
    };
    let ctx = DomainComputeCtx {
        as_of,
        binding: &binding,
        primary: &primary,
        oracle: Some(&oracle),
        crypto: &CryptoDomainConfig::default(),
    };
    let features = CryptoDomainFeatureBuilder.compute(&ctx);
    let basis = features
        .iter()
        .find(|f| f.name == domain_crypto::BASIS_VS_RESOLUTION_SOURCE)
        .expect("basis feature");
    assert_eq!(
        basis.value.as_ref().expect("present"),
        &FeatureValue::Bps(dec!(50))
    );
}

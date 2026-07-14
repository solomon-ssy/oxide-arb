//! Phase 11.2.2 §9 acceptance tests (research plane).

use std::collections::BTreeMap;
use std::sync::Arc;

use chrono::{DateTime, Duration, TimeZone, Utc};
use quant_pivot_error::{QuantResult, research::ResearchError};
use quant_pivot_models::{
    domain::{
        CryptoPriceReport, CryptoSubject, DecisionClock, DecisionSource, DomainObservation,
        GroundingProof, LinkageOutcome, LinkageSourceMetadata, MarketLinkage, MarketSubject,
        PriceComparator, ResolutionOracle, ResolvedBinding, ResolvedSourceBinding,
        market::{
            book::BookLevel,
            registry::{MarketRegistryInfo, TokenInfo},
        },
    },
    enums::{
        common::{CategorySet, MarketCategory, TickSize},
        domain::{DomainFamily, DomainMetric, KlineInterval, LinkageSourceRole, ResolverTier},
        feature::EvidenceSourceKind,
        market::MarketStatus,
        quant::{DataQualityStatus, DatasetPurpose},
    },
    runtime_config::{DataQualityConfig, DomainConfig, FactorsConfig, FeaturesConfig},
    types::{
        BinanceSymbol, CatalogSyncBatchId, ChainlinkFeedKey, ContentHash, CryptoAsset, CryptoQuote,
        DomainInstrumentKey, DomainSourceId, EventCatalogVersionId, EventId,
        MarketCatalogVersionId, MarketId, MarketLinkageId, Price, Probability, ResolverVersion,
        SchemaVersion, Shares, TokenId, Usd,
    },
};
use quant_pivot_research::{
    domain::{
        CryptoPriceReportWindow, DomainFactWindows, DomainObservationWindow,
        build_domain_slice_inputs, linkage_valid_at,
    },
    factors::{
        DomainFactorRegistry, FactorEngine, FactorName, MarketFactorOutcome, ScoredFactor,
        names::{DOMAIN_CRYPTO_BETA_REGIME, DOMAIN_CRYPTO_STRIKE_PRESSURE},
    },
    features::{
        CatalogDecisionRef, ConfiguredFeatureBuilder, CryptoDomainFeatureBuilder, DomainComputeCtx,
        DomainFeatureBuilder, DomainFeatureSlice, DomainSliceData, DomainSliceDataRef,
        DomainSliceInputs, EvidenceSourceRef, FeatureCell, FeatureStaleness, FeatureValue,
        FeatureVector, MarketDecisionCaptureInput, MarketWindowSnapshot, ResolvedBook,
        ResolvedInputs, ResolvedMarketBundle, ResolvedMarketContext, TradeTapeWindowSnapshot,
        market_decision_capture_from_resolved,
        names::{
            domain_crypto::{self, BASIS_VS_RESOLUTION_SOURCE},
            market as market_names,
        },
    },
    hashing::ResearchHasher,
    linkage::{
        CryptoSubjectParser, DefaultSubjectValidator, LayeredResolver, SubjectExtractor,
        SubjectValidator, Tier0SlugExtractor, ValidationOutcome, WeatherStationCatalog,
    },
    pit::CanonicalBookEventRef,
    selection::SelectedMarket,
    training::{
        DatasetHashContract, LabelName, TrainingDatasetArtifact, TrainingExample, TrainingLabel,
        assert_no_future_leakage,
    },
};
use rust_decimal::Decimal;
use rust_decimal_macros::dec;

fn content_hash(seed: char) -> ContentHash {
    ContentHash::parse(format!("blake3:{}", seed.to_string().repeat(64))).expect("test hash")
}

fn source_binding(
    role: LinkageSourceRole,
    source_id: DomainSourceId,
    instrument_key: DomainInstrumentKey,
    available_at: chrono::DateTime<Utc>,
) -> ResolvedSourceBinding {
    ResolvedSourceBinding {
        role,
        source_id,
        instrument_key,
        available_at,
        binding_hash: content_hash('d'),
    }
}

fn metadata(slug: &str) -> LinkageSourceMetadata {
    LinkageSourceMetadata {
        market_id: MarketId::new("0xmarket"),
        slug: slug.to_owned(),
        question: "Bitcoin Up or Down".to_owned(),
        // Every observed short-cycle up/down market carries this literal
        // Chainlink Data Streams anchor; Tier 0/1 now ground the oracle to it
        // instead of assuming a ruleset default (11.2.2 remediation R4).
        description: Some(
            "The resolution source for this market is the Chainlink BTC/USD data stream, \
             available at https://data.chain.link/streams/btc-usd."
                .to_owned(),
        ),
        series_slug: None,
        end_date: Some(Utc.with_ymd_and_hms(2026, 7, 8, 12, 0, 0).unwrap()),
    }
}

fn crypto_slice() -> DomainFeatureSlice {
    let mut values = BTreeMap::new();
    values.insert(
        domain_crypto::DISTANCE_TO_STRIKE,
        FeatureCell::observed(
            FeatureValue::Decimal(dec!(0.02)),
            None,
            FeatureStaleness::Unknown,
        ),
    );
    DomainFeatureSlice {
        family: DomainFamily::Crypto,
        schema_version: SchemaVersion::new(5),
        values,
    }
}

/// Locate one named factor in a computed batch outcome (panics if the
/// governed registry does not contain it — every registered factor is
/// present in every market's output; see `FactorEngine::compute_all_batch_inner`).
fn find_factor<'a>(outcome: &'a MarketFactorOutcome, name: &FactorName) -> &'a ScoredFactor {
    outcome
        .factors
        .iter()
        .find(|scored| &scored.value.name == name)
        .unwrap_or_else(|| panic!("factor {name} must be present in the batch output"))
}

fn vector(category: MarketCategory, domain: Option<DomainFeatureSlice>) -> FeatureVector {
    let mut generic = BTreeMap::new();
    generic.insert(
        market_names::CATEGORY,
        FeatureCell::observed(
            FeatureValue::Category(category),
            None,
            FeatureStaleness::Unknown,
        ),
    );
    FeatureVector {
        market_id: MarketId::new("m"),
        token_id: Some(TokenId::new("t")),
        decision_at: Utc::now(),
        generic_schema_version: SchemaVersion::FIRST,
        generic,
        domain,
        data_quality: DataQualityStatus::Fresh,
    }
}

fn domain_test_market(category: MarketCategory) -> SelectedMarket {
    SelectedMarket {
        market_id: MarketId::new("m"),
        event_id: EventId::new("e"),
        category,
        primary_token_id: TokenId::new("yes"),
        secondary_token_id: Some(TokenId::new("no")),
        liquidity_usd: None,
        volume_24h_usd: None,
        source_refs: Vec::new(),
    }
}

fn domain_test_book(market: &SelectedMarket, cutoff: chrono::DateTime<Utc>) -> ResolvedBook {
    ResolvedBook {
        token_id: market.primary_token_id.clone(),
        bids: Arc::from([BookLevel::from_decimal_unchecked(
            Price::new(dec!(0.49)),
            Shares::new(dec!(100)),
        )]),
        asks: Arc::from([BookLevel::from_decimal_unchecked(
            Price::new(dec!(0.51)),
            Shares::new(dec!(100)),
        )]),
        timestamp_ms: u64::try_from(cutoff.timestamp_millis()).expect("positive timestamp"),
        version: 1,
        sequence: 1,
        source_event: Some(CanonicalBookEventRef {
            stream_session_id: uuid::Uuid::from_u128(1),
            token_sequence: 1,
            source_event_hash: ContentHash::parse(format!("blake3:{}", "d".repeat(64)))
                .expect("canonical event hash"),
        }),
        effective_at: cutoff,
        available_at: cutoff,
    }
}

fn domain_test_registry(
    market: &SelectedMarket,
    context: &ResolvedMarketContext,
    cutoff: chrono::DateTime<Utc>,
) -> MarketRegistryInfo {
    MarketRegistryInfo {
        market_id: market.market_id.clone(),
        event_id: market.event_id.clone(),
        token_yes: market.primary_token_id.clone(),
        token_no: market.secondary_token_id.clone().expect("secondary token"),
        question: "domain acceptance".to_owned(),
        slug: "domain-acceptance".to_owned(),
        description: None,
        categories: CategorySet::from(market.category),
        status: MarketStatus::Active,
        outcome: None,
        neg_risk: false,
        tick_size: TickSize::Hundredth,
        tokens: vec![TokenInfo {
            token_id: market.primary_token_id.clone(),
            outcome: "Yes".to_owned(),
            neg_risk: false,
        }],
        best_bid: Some(dec!(0.49)),
        best_ask: Some(dec!(0.51)),
        depth_usd: Some(Usd::new(dec!(100))),
        min_order_size: dec!(1),
        liquidity_usd: Some(Usd::new(dec!(100))),
        volume_24h: None,
        fee_schedule: None,
        start_date: context.start_date,
        end_date: context.end_date,
        resolved_at: None,
        created_at: context.created_at,
        updated_at: cutoff,
    }
}

fn build_domain_test_vector(
    builder: &ConfiguredFeatureBuilder,
    features: &FeaturesConfig,
    as_of: chrono::DateTime<Utc>,
    cutoff: chrono::DateTime<Utc>,
    category: MarketCategory,
    domain: Option<&DomainSliceInputs>,
) -> QuantResult<FeatureVector> {
    let market = domain_test_market(category);
    let book = domain_test_book(&market, cutoff);
    let market_ctx = ResolvedMarketContext {
        market_id: market.market_id.clone(),
        effective_at: cutoff,
        available_at: as_of,
        status: MarketStatus::Active,
        neg_risk: false,
        start_date: Some(as_of - Duration::hours(1)),
        end_date: Some(as_of + Duration::days(1)),
        created_at: Some(as_of - Duration::days(1)),
        fee_schedule: None,
    };
    let registry = domain_test_registry(&market, &market_ctx, cutoff);
    let lag_secs = u64::try_from((as_of - cutoff).num_seconds()).map_err(|error| {
        ResearchError::DatasetBuild {
            detail: format!("test boundary lag conversion failed: {error}"),
        }
    })?;
    let boundary = DecisionClock::new(lag_secs).boundary(as_of)?;
    let hash = |seed: char| {
        ContentHash::parse(format!("blake3:{}", seed.to_string().repeat(64))).expect("test hash")
    };
    let capture = market_decision_capture_from_resolved(MarketDecisionCaptureInput {
        boundary: &boundary,
        selected: &market,
        book: book.clone(),
        market: market_ctx.clone(),
        registry: Some(&registry),
        catalog: CatalogDecisionRef {
            catalog_sync_batch_id: CatalogSyncBatchId::from_v7(),
            market_catalog_version_id: MarketCatalogVersionId::from_v7(),
            event_catalog_version_id: EventCatalogVersionId::from_v7(),
            market_content_hash: hash('a'),
            event_content_hash: hash('b'),
            membership_hash: hash('c'),
            market_effective_at: cutoff,
            market_available_at: as_of,
            event_effective_at: cutoff,
            event_available_at: as_of,
            market_timestamp_quality: "source".to_owned(),
            event_timestamp_quality: "source".to_owned(),
        },
        domain,
        liquidity_cap_usd: Usd::ZERO,
    })?;
    let window = MarketWindowSnapshot::empty(market.primary_token_id.clone(), as_of, cutoff);
    let trade_tape = TradeTapeWindowSnapshot::empty(market.market_id.clone(), as_of, cutoff);
    let bundle = ResolvedMarketBundle {
        inputs: ResolvedInputs {
            market: &market,
            decision_at: as_of,
            book: Some(book),
            secondary_book: None,
            secondary_book_snapshot_ref: None,
            market_ctx: Some(market_ctx),
            window: &window,
            trade_tape: &trade_tape,
            domain,
            sibling_legs: Vec::new(),
            sibling_leg_total: 0,
        },
        capture,
    };
    builder.compute_vector(&bundle, &[], features, &DataQualityConfig::default())
}

fn crypto_domain_inputs(as_of: chrono::DateTime<Utc>) -> DomainSliceInputs {
    let observed_at = as_of - Duration::minutes(1);
    let instrument_key = DomainInstrumentKey::binance_kline(
        &BinanceSymbol::parse("BTCUSDT").expect("symbol"),
        KlineInterval::OneMinute,
    );
    DomainSliceInputs {
        family: DomainFamily::Crypto,
        linkage_id: MarketLinkageId::from_v7(),
        linkage_hash: content_hash('e'),
        binding: ResolvedBinding {
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
            source_bindings: vec![source_binding(
                LinkageSourceRole::Feature,
                DomainSourceId::binance(),
                instrument_key.clone(),
                observed_at,
            )],
            grounding: GroundingProof { spans: Vec::new() },
            override_context: None,
        },
        linkage_evidence: linkage_evidence(as_of),
        data: DomainSliceData::Crypto {
            primary: DomainObservationWindow {
                cutoff: as_of,
                observations: vec![DomainObservation {
                    family: DomainFamily::Crypto,
                    source_id: DomainSourceId::binance(),
                    instrument_key,
                    metric: DomainMetric::Close,
                    value: dec!(100000),
                    observed_at,
                    publish_time: observed_at,
                    available_at: Some(observed_at),
                }],
            },
            oracle: None,
        },
    }
}

fn linkage_evidence(as_of: chrono::DateTime<Utc>) -> EvidenceSourceRef {
    EvidenceSourceRef {
        source_kind: EvidenceSourceKind::Linkage,
        reference: "linkage:test@blake3:test".to_owned(),
        effective_at: as_of - Duration::minutes(1),
        available_at: Some(as_of - Duration::minutes(1)),
    }
}

#[test]
fn feature_vector_domain_slice_only_for_mapped_category() {
    // Drives through the real `ConfiguredFeatureBuilder::compute_vector` — the
    // production domain-slice assembly path — rather than a hand-built
    // `FeatureVector` literal (11.2.2 remediation R11).
    let features = FeaturesConfig::default();
    let domain_config = DomainConfig::default();
    let builder =
        ConfiguredFeatureBuilder::new(&features, &domain_config).expect("feature builder");
    let as_of = Utc.with_ymd_and_hms(2026, 7, 1, 12, 0, 0).unwrap();
    let knowledge_cutoff = as_of - Duration::seconds(5);

    // Sports never prefetches domain inputs at all (no vertical maps to it) —
    // the pipeline passes `domain: None` upstream of the builder.
    let sports = build_domain_test_vector(
        &builder,
        &features,
        as_of,
        knowledge_cutoff,
        MarketCategory::Sports,
        None,
    )
    .expect("sports vector");
    assert!(
        sports.domain.is_none(),
        "a category with no domain vertical must never carry a domain slice"
    );

    // Crypto with a resolved linkage + PIT window DOES carry a domain slice.
    let domain_inputs = crypto_domain_inputs(as_of);
    let crypto = build_domain_test_vector(
        &builder,
        &features,
        as_of,
        knowledge_cutoff,
        MarketCategory::Crypto,
        Some(&domain_inputs),
    )
    .expect("crypto vector");
    assert!(
        crypto.domain.is_some(),
        "a category-mapped market with a resolved linkage must carry a domain slice"
    );
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

    // `DomainFamily` ships exactly one variant today (`Crypto`), so a second
    // real family discriminant does not exist to vary against. The composite
    // hash's *domain-specific* dimension is proven sensitive instead via the
    // other field the algorithm packs alongside `domain_family` —
    // `domain_schema_version` — which is genuinely constructible with two
    // distinct values and must independently perturb the digest exactly as
    // a second family value would (§3.3 composite: `generic_hash ⊕
    // domain_family ⊕ domain_schema_version ⊕ domain_hash`).
    let mut slice_v6 = crypto_slice();
    slice_v6.schema_version = SchemaVersion::new(6);
    let with_v6 = vector(MarketCategory::Crypto, Some(slice_v6));
    let h_v6 = ResearchHasher::feature_vector(&with_v6).expect("hash");
    assert_ne!(
        h_crypto, h_v6,
        "domain_schema_version must independently perturb the digest"
    );
}

#[test]
fn domain_factor_registered_and_routed_by_category() {
    let registry = DomainFactorRegistry::build(&DomainConfig::default());
    assert_eq!(registry.for_category(MarketCategory::Crypto).len(), 2);
    assert!(registry.for_category(MarketCategory::Sports).is_empty());

    // Assert on the real `FactorEngine` batch output, not just registry
    // cardinality: a Crypto-category vector must have the domain factors
    // routed into its computed set; a Sports-category vector (same engine,
    // same call) must never see them (11.2.2 remediation R11).
    let engine = FactorEngine::new(
        &FactorsConfig::default(),
        &FeaturesConfig::default(),
        &DomainConfig::default(),
        None,
    );
    let crypto_vector = vector(MarketCategory::Crypto, Some(crypto_slice()));
    let sports_vector = vector(MarketCategory::Sports, None);

    let crypto_outcome = engine
        .compute_all(&crypto_vector, &FactorsConfig::default())
        .expect("crypto factors");
    let sports_outcome = engine
        .compute_all(&sports_vector, &FactorsConfig::default())
        .expect("sports factors");

    // Every registered factor appears in every market's batch output (a fixed
    // governed set — see `FactorEngine::compute_all_batch_inner`); category
    // routing is expressed through each domain factor's own eligibility, not
    // through absence from the list. A crypto market with a real domain slice
    // must score the domain factors; a Sports market must see them come back
    // structurally `NotApplicable` (`routes_to_crypto` gate), never scored.
    let crypto_strike = find_factor(&crypto_outcome, &DOMAIN_CRYPTO_STRIKE_PRESSURE);
    let crypto_beta = find_factor(&crypto_outcome, &DOMAIN_CRYPTO_BETA_REGIME);
    assert!(
        !crypto_strike.value.is_not_applicable(),
        "crypto batch output must route (not structurally skip) the strike-pressure factor"
    );
    assert!(
        !crypto_beta.value.is_not_applicable(),
        "crypto batch output must route (not structurally skip) the beta-regime factor"
    );

    let sports_strike = find_factor(&sports_outcome, &DOMAIN_CRYPTO_STRIKE_PRESSURE);
    let sports_beta = find_factor(&sports_outcome, &DOMAIN_CRYPTO_BETA_REGIME);
    assert!(
        sports_strike.value.is_not_applicable(),
        "a crypto-only domain factor must be structurally not-applicable for a Sports market"
    );
    assert!(
        sports_beta.value.is_not_applicable(),
        "a crypto-only domain factor must be structurally not-applicable for a Sports market"
    );
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

    let make = |domain: Option<DomainFeatureSlice>| {
        let mut feature_vector = vector(MarketCategory::Crypto, domain);
        feature_vector.decision_at = as_of;
        TrainingExample {
            example_id: TrainingExampleId::from_v7(),
            market_id: MarketId::new("m"),
            token_id: TokenId::new("m-yes"),
            selected_market: SelectedMarket {
                market_id: MarketId::new("m"),
                event_id: EventId::new("event:m"),
                category: MarketCategory::Crypto,
                primary_token_id: TokenId::new("m-yes"),
                secondary_token_id: None,
                liquidity_usd: None,
                volume_24h_usd: None,
                source_refs: Vec::new(),
            },
            decision_boundary: DecisionClock::new(0).boundary(as_of).expect("boundary"),
            sample_source: TrainingSampleSource::HistoricalPit,
            feature_vector,
            factor_values: Vec::new(),
            labels: vec![TrainingLabel {
                label_name: LabelName::from_static("return_to_horizon"),
                horizon_secs: 60,
                value: Decimal::ONE,
                is_resolved: true,
                matured_at: as_of + Duration::seconds(60),
            }],
            source_refs: Vec::new(),
            decision_capture: None,
            lot_context: None,
            position_state: None,
            book_fidelity: None,
        }
    };

    let without = vec![make(None)];
    let with = vec![make(Some(crypto_slice()))];
    let h0 = TrainingDatasetArtifact::compute_dataset_hash(
        DatasetHashContract {
            model_spec_id: &spec,
            window_start: as_of,
            window_end: as_of,
            purpose: DatasetPurpose::Training,
            feature_schema_hash: &feature,
            factor_schema_hash: &factor,
            label_schema_hash: &label,
        },
        &without,
    )
    .expect("hash");
    let h1 = TrainingDatasetArtifact::compute_dataset_hash(
        DatasetHashContract {
            model_spec_id: &spec,
            window_start: as_of,
            window_end: as_of,
            purpose: DatasetPurpose::Training,
            feature_schema_hash: &feature,
            factor_schema_hash: &factor,
            label_schema_hash: &label,
        },
        &with,
    )
    .expect("hash");
    assert_ne!(h0, h1);
}

#[test]
fn dataset_builder_domain_slice_no_future_leakage() {
    use quant_pivot_models::types::{TrainingExampleId, TrainingSampleSource};

    let as_of = Utc.with_ymd_and_hms(2026, 7, 1, 12, 0, 0).unwrap();
    let cutoff = as_of - Duration::seconds(5);
    let mut fv = vector(MarketCategory::Crypto, Some(crypto_slice()));
    fv.decision_at = as_of;
    let clean = TrainingExample {
        example_id: TrainingExampleId::from_v7(),
        market_id: MarketId::new("m"),
        token_id: TokenId::new("m-yes"),
        selected_market: SelectedMarket {
            market_id: MarketId::new("m"),
            event_id: EventId::new("event:m"),
            category: MarketCategory::Crypto,
            primary_token_id: TokenId::new("m-yes"),
            secondary_token_id: None,
            liquidity_usd: None,
            volume_24h_usd: None,
            source_refs: Vec::new(),
        },
        decision_boundary: DecisionClock::new(5).boundary(as_of).expect("boundary"),
        sample_source: TrainingSampleSource::HistoricalPit,
        feature_vector: fv,
        factor_values: Vec::new(),
        labels: Vec::new(),
        source_refs: vec![EvidenceSourceRef {
            source_kind: EvidenceSourceKind::DomainExternal,
            reference: "binance:BTCUSDT:1m".to_owned(),
            effective_at: cutoff,
            available_at: Some(cutoff),
        }],
        decision_capture: None,
        lot_context: None,
        position_state: None,
        book_fidelity: None,
    };
    assert!(assert_no_future_leakage(std::slice::from_ref(&clean)).is_ok());

    let bad = TrainingExample {
        source_refs: vec![EvidenceSourceRef {
            source_kind: EvidenceSourceKind::DomainExternal,
            reference: "binance:BTCUSDT:1m".to_owned(),
            effective_at: as_of,
            available_at: Some(as_of),
        }],
        ..clean
    };
    assert!(assert_no_future_leakage(&[bad]).is_err());
}

#[test]
fn crypto_subject_parser_tier1_deterministic_fail_closed() {
    let parser = CryptoSubjectParser;
    let meta = LinkageSourceMetadata {
        market_id: MarketId::new("0xmarket"),
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

    // Determinism: repeated extraction over the identical metadata is
    // byte-for-byte the same candidate (research is deterministic/replayable).
    let candidate_again = parser.extract(&meta).expect("extract");
    assert_eq!(
        candidate, candidate_again,
        "tier1 extraction must be deterministic across repeated runs"
    );

    // Genuine fail-closed case (11.2.2 remediation R4): the slug names
    // January 7 with no explicit year, but `end_date` is six months away in
    // July — no candidate year (anchor ± 1) reconstructs an instant within
    // `MAX_PLAUSIBLE_YEAR_DRIFT` of `end_date`, so `infer_year` must return
    // `None` and the whole template must decline rather than silently guess
    // the nearest year (the pre-R4 fail-open bug).
    let implausible = LinkageSourceMetadata {
        slug: "bitcoin-up-or-down-january-7-12pm-et".to_owned(),
        question: "Bitcoin Up or Down - January 7, 12PM ET".to_owned(),
        end_date: Some(Utc.with_ymd_and_hms(2026, 7, 7, 17, 0, 0).unwrap()),
        ..meta
    };
    let rejected = parser.extract(&implausible).expect("extract");
    assert!(
        rejected.is_none(),
        "an implausible year drift must fail closed, never guess the nearest year"
    );
}

#[test]
fn linkage_tier0_slug_direct_read_matches_tier1() {
    // Invoke both deterministic tiers **independently** (not through
    // `LayeredResolver`, which would short-circuit at the first hit) on the
    // same metadata, and assert they agree: Tier 0 owns the short-cycle
    // epoch-templated slug (`btc-updown-5m-{epoch}`) and must resolve it;
    // Tier 1 owns a disjoint, human-readable ET slug grammar
    // (`{alias}-up-or-down-{month}-{day}-{hour}{am|pm}-et`) and must
    // correctly abstain (`None`) rather than mis-parse or silently disagree
    // with Tier 0 over the same board (11.2.2 remediation R11: tier
    // boundaries never overlap in a way that could produce two conflicting
    // subjects for one market).
    let slug = "btc-updown-5m-1780319100";
    let meta = metadata(slug);

    let tier0 = Tier0SlugExtractor
        .extract(&meta)
        .expect("tier0 extract")
        .expect("tier0 candidate");
    let tier1 = CryptoSubjectParser.extract(&meta).expect("tier1 extract");
    assert!(
        tier1.is_none(),
        "tier1's ET slug grammar must not also match tier0's epoch slug \
         (no silent disagreement between tiers over the same board)"
    );

    // Cross-check against the full layered resolver: it must reach the exact
    // same subject Tier 0 produced directly.
    let resolver = LayeredResolver::deterministic(WeatherStationCatalog::default());
    let resolution = resolver.resolve(&meta, Utc::now()).expect("resolve");
    assert_eq!(resolution.resolver_tier, ResolverTier::Tier0Slug);
    let LinkageOutcome::Resolved(binding) = resolution.outcome else {
        panic!("expected a resolved binding");
    };
    let MarketSubject::Crypto(resolved_subject) = binding.subject else {
        panic!("expected crypto subject");
    };
    let MarketSubject::Crypto(tier0_subject) = tier0.subject else {
        panic!("expected crypto subject");
    };
    assert_eq!(resolved_subject.asset, tier0_subject.asset);
    assert_eq!(resolved_subject.comparator, tier0_subject.comparator);
}

#[test]
fn linkage_grounding_rejects_field_absent_from_source() {
    let resolver = LayeredResolver::deterministic(WeatherStationCatalog::default());
    let bad = resolver
        .resolve(&metadata("totally-unknown-market-slug"), Utc::now())
        .expect("resolve");
    assert!(matches!(bad.outcome, LinkageOutcome::Unresolved { .. }));

    // The real anti-hallucination scenario (11.2.2 remediation R11): take a
    // genuinely valid, grounded candidate and corrupt one span's literal
    // text so it no longer matches the source substring at its own
    // recorded offsets. The gate must reject it outright — it must never
    // fall back to "close enough" or silently drop the corrupted field.
    let slug = "btc-updown-5m-1780319100";
    let meta = metadata(slug);
    let mut candidate = Tier0SlugExtractor
        .extract(&meta)
        .expect("extract")
        .expect("candidate");
    assert!(
        !candidate.grounding.spans.is_empty(),
        "fixture must carry at least one grounding span to corrupt"
    );
    candidate.grounding.spans[0].text = "totally-fabricated-text".to_owned();

    let outcome = DefaultSubjectValidator.validate(&candidate, &meta);
    assert!(
        matches!(outcome, ValidationOutcome::Rejected { .. }),
        "a grounding span whose literal text disagrees with the source must be rejected"
    );

    // A corrupted byte offset (out of bounds) must fail closed the same way.
    let mut offset_corrupt = Tier0SlugExtractor
        .extract(&meta)
        .expect("extract")
        .expect("candidate");
    offset_corrupt.grounding.spans[0].start = 10_000;
    offset_corrupt.grounding.spans[0].end = 10_010;
    let outcome = DefaultSubjectValidator.validate(&offset_corrupt, &meta);
    assert!(
        matches!(outcome, ValidationOutcome::Rejected { .. }),
        "an out-of-bounds grounding span must be rejected, never panic or pass silently"
    );
}

#[test]
fn crypto_domain_feature_present_with_domain_external_evidence() {
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
            available_at: Some(at),
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
        source_bindings: vec![source_binding(
            LinkageSourceRole::Feature,
            DomainSourceId::binance(),
            primary.observations[0].instrument_key.clone(),
            at,
        )],
        grounding: GroundingProof { spans: Vec::new() },
        override_context: None,
    };
    let linkage_evidence = linkage_evidence(as_of);
    let domain = DomainConfig::default();
    let ctx = DomainComputeCtx {
        decision_at: as_of,
        binding: &binding,
        linkage_evidence: &linkage_evidence,
        data: DomainSliceDataRef::Crypto {
            primary: &primary,
            oracle: None,
        },
        domain: &domain,
    };
    let features = CryptoDomainFeatureBuilder.compute(&ctx);
    let distance = features
        .iter()
        .find(|f| f.name == domain_crypto::DISTANCE_TO_STRIKE)
        .expect("distance feature");
    assert!(distance.value.is_ok());
    assert!(distance.evidence.is_some());
}

fn crypto_pit_linkage(
    as_of: DateTime<Utc>,
    instrument_key: &DomainInstrumentKey,
    visible_at: DateTime<Utc>,
) -> MarketLinkage {
    let binding = ResolvedBinding {
        subject: MarketSubject::Crypto(CryptoSubject {
            asset: CryptoAsset::parse("BTC").expect("asset"),
            quote: CryptoQuote::parse("USD").expect("quote"),
            comparator: PriceComparator::UpVsReference,
            strike: None,
            reference_at: None,
            observation_at: as_of,
            resolution_oracle: ResolutionOracle::BinanceKline {
                symbol: BinanceSymbol::parse("BTCUSDT").expect("symbol"),
                interval: KlineInterval::OneMinute,
            },
        }),
        source_bindings: vec![source_binding(
            LinkageSourceRole::Feature,
            DomainSourceId::binance(),
            instrument_key.clone(),
            visible_at,
        )],
        grounding: GroundingProof { spans: Vec::new() },
        override_context: None,
    };
    let content_hash = ContentHash::parse(format!("blake3:{}", "0".repeat(64))).expect("hash");
    MarketLinkage {
        linkage_id: MarketLinkageId::from_v7(),
        market_id: MarketId::new("m"),
        domain_family: DomainFamily::Crypto,
        outcome: LinkageOutcome::Resolved(Box::new(binding)),
        confidence: Probability::ONE,
        resolver_tier: ResolverTier::Tier0Slug,
        resolver_version: ResolverVersion::FIRST,
        metadata_hash: content_hash.clone(),
        content_hash,
        effective_at: as_of - Duration::days(1),
        available_at: as_of - Duration::days(1),
    }
}

#[test]
fn domain_observation_pit_excludes_after_as_of_minus_delay() {
    // Exercises the **actual** production PIT assembly (`build_domain_slice_inputs`
    // over a prefetched `HashMap`) — the single path both the online and
    // offline planes call — rather than a deleted, never-wired dual-engine
    // mirror (11.2.2 remediation R9).
    let as_of = Utc.with_ymd_and_hms(2026, 7, 1, 12, 0, 0).unwrap();
    let knowledge_lag_secs = 5_u64;
    let cutoff = as_of - Duration::seconds(i64::try_from(knowledge_lag_secs).expect("small delay"));

    let instrument_key = DomainInstrumentKey::binance_kline(
        &BinanceSymbol::parse("BTCUSDT").expect("symbol"),
        KlineInterval::OneMinute,
    );
    let visible_at = cutoff - Duration::seconds(1);
    let invisible_at = as_of - Duration::seconds(1); // after cutoff, before as_of
    let make_observation = |observed_at| DomainObservation {
        family: DomainFamily::Crypto,
        source_id: DomainSourceId::binance(),
        instrument_key: instrument_key.clone(),
        metric: DomainMetric::Close,
        value: dec!(100000),
        observed_at,
        publish_time: observed_at,
        available_at: Some(observed_at),
    };
    let observations = std::collections::HashMap::from([(
        instrument_key.clone(),
        vec![make_observation(visible_at), make_observation(invisible_at)],
    )]);

    let mut domain_config = DomainConfig::default();
    domain_config.crypto.availability_lag_secs = knowledge_lag_secs;
    let linkage = crypto_pit_linkage(as_of, &instrument_key, visible_at);

    let boundary = DecisionClock::new(0)
        .boundary(as_of)
        .expect("boundary")
        .with_source_cutoff(
            DecisionSource::DomainCrypto,
            domain_config.crypto.availability_lag_secs,
        )
        .expect("domain cutoff");
    let inputs = build_domain_slice_inputs(
        MarketCategory::Crypto,
        std::slice::from_ref(&linkage),
        &boundary,
        &domain_config,
        DomainFactWindows {
            observations: &observations,
            crypto_reports: &std::collections::HashMap::new(),
            weather_observations: &std::collections::HashMap::new(),
            weather_forecasts: &std::collections::HashMap::new(),
            weather_calibrations: &[],
        },
    )
    .expect("domain slice build")
    .expect("domain slice inputs");

    let DomainSliceData::Crypto { primary, .. } = inputs.data else {
        panic!("crypto data expected");
    };
    let observed_times: Vec<_> = primary
        .observations
        .iter()
        .map(|observation| observation.observed_at)
        .collect();
    assert!(
        observed_times.contains(&visible_at),
        "an observation at or before the frozen source cutoff must be visible"
    );
    assert!(
        !observed_times.contains(&invisible_at),
        "an observation after the frozen source cutoff must be excluded, even though \
         it is still before `decision_at` itself"
    );
}

#[test]
fn linkage_pit_uses_metadata_version_not_future_revision() {
    // The bitemporal axis (11.2.2 remediation R5): a market's linkage can be
    // re-derived after a metadata revision, appending a **new** ledger row
    // rather than mutating the old one. A PIT read at an `as_of` between two
    // revisions must see the version that was current at that instant —
    // never a revision `derived_at` later than `as_of` ("future" relative to
    // the read).
    let market_id = MarketId::new("m");
    let hash = |seed: &str| ContentHash::parse(format!("blake3:{seed:0<64}")).expect("hash");

    let binding_for = |asset: &str| ResolvedBinding {
        subject: MarketSubject::Crypto(CryptoSubject {
            asset: CryptoAsset::parse(asset).expect("asset"),
            quote: CryptoQuote::parse("USD").expect("quote"),
            comparator: PriceComparator::UpVsReference,
            strike: None,
            reference_at: None,
            observation_at: Utc.with_ymd_and_hms(2026, 7, 8, 12, 0, 0).unwrap(),
            resolution_oracle: ResolutionOracle::BinanceKline {
                symbol: BinanceSymbol::parse(format!("{asset}USDT")).expect("symbol"),
                interval: KlineInterval::OneMinute,
            },
        }),
        source_bindings: vec![source_binding(
            LinkageSourceRole::Feature,
            DomainSourceId::binance(),
            DomainInstrumentKey::binance_kline(
                &BinanceSymbol::parse(format!("{asset}USDT")).expect("symbol"),
                KlineInterval::OneMinute,
            ),
            Utc.with_ymd_and_hms(2026, 7, 1, 0, 0, 0).unwrap(),
        )],
        grounding: GroundingProof { spans: Vec::new() },
        override_context: None,
    };

    let t1 = Utc.with_ymd_and_hms(2026, 7, 1, 0, 0, 0).unwrap();
    let t2 = Utc.with_ymd_and_hms(2026, 7, 5, 0, 0, 0).unwrap();
    let v1 = MarketLinkage {
        linkage_id: MarketLinkageId::from_v7(),
        market_id: market_id.clone(),
        domain_family: DomainFamily::Crypto,
        outcome: LinkageOutcome::Resolved(Box::new(binding_for("BTC"))),
        confidence: Probability::ONE,
        resolver_tier: ResolverTier::Tier0Slug,
        resolver_version: ResolverVersion::FIRST,
        metadata_hash: hash("aa"),
        content_hash: hash("11"),
        effective_at: t1,
        available_at: t1,
    };
    // A metadata revision at `t2` changed the resolved asset (simulating a
    // corrected/edited market description that re-derivation picked up).
    let v2 = MarketLinkage {
        linkage_id: MarketLinkageId::from_v7(),
        market_id,
        domain_family: DomainFamily::Crypto,
        outcome: LinkageOutcome::Resolved(Box::new(binding_for("ETH"))),
        confidence: Probability::ONE,
        resolver_tier: ResolverTier::Tier0Slug,
        resolver_version: ResolverVersion::FIRST,
        metadata_hash: hash("bb"),
        content_hash: hash("22"),
        effective_at: t2,
        available_at: t2,
    };
    let ledger = [v1, v2];

    // Read strictly between the two revisions: must see v1 (BTC), never v2.
    let between = t1 + Duration::days(1);
    let between_boundary = DecisionClock::new(0).boundary(between).expect("boundary");
    let resolved = linkage_valid_at(&ledger, &between_boundary).expect("a valid record exists");
    let LinkageOutcome::Resolved(binding) = &resolved.outcome else {
        panic!("expected resolved outcome");
    };
    let MarketSubject::Crypto(subject) = &binding.subject else {
        panic!("expected crypto subject");
    };
    assert_eq!(
        subject.asset.as_str(),
        "BTC",
        "PIT read between revisions must see the version current at that instant, \
         never a later (future-relative-to-as_of) revision"
    );

    // Read at/after the second revision: must see v2 (ETH).
    let after = t2 + Duration::hours(1);
    let after_boundary = DecisionClock::new(0).boundary(after).expect("boundary");
    let resolved_after = linkage_valid_at(&ledger, &after_boundary).expect("a valid record exists");
    let LinkageOutcome::Resolved(binding_after) = &resolved_after.outcome else {
        panic!("expected resolved outcome");
    };
    let MarketSubject::Crypto(subject_after) = &binding_after.subject else {
        panic!("expected crypto subject");
    };
    assert_eq!(subject_after.asset.as_str(), "ETH");

    // Read before either revision exists: no valid record.
    let before = t1 - Duration::days(1);
    let before_boundary = DecisionClock::new(0).boundary(before).expect("boundary");
    assert!(linkage_valid_at(&ledger, &before_boundary).is_none());
}

/// Focused, pure feature-computation test: `basis_vs_resolution_source` is
/// the Binance-close-vs-Chainlink-oracle bps divergence itself. Whether that
/// divergence is *actionable* (crosses `max_basis_bps`, gets persisted, and
/// enters the operator review queue) is a distinct concern owned by
/// `detect_basis_alerts` — see
/// `crates/quant-pivot-core/src/service/basis_alert.rs`'s own unit tests and
/// `crates/quant-pivot-repository/tests/pg_basis_alert.rs`'s
/// `acknowledge_marks_the_alert_and_is_idempotent` for that closed loop
/// (11.2.2 remediation R6 — this test was previously misnamed as asserting
/// "flagging", which it never did).
#[test]
fn basis_vs_resolution_source_computes_bps_from_close_and_oracle() {
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
            available_at: Some(as_of - Duration::minutes(1)),
        }],
    };
    let oracle_instrument = DomainInstrumentKey::chainlink_data_streams(
        &ChainlinkFeedKey::parse("BTC-USD").expect("feed"),
    );
    let oracle = CryptoPriceReportWindow {
        cutoff: as_of,
        reports: vec![CryptoPriceReport {
            source_id: DomainSourceId::chainlink_data_streams(),
            instrument_key: oracle_instrument.clone(),
            source_sequence: 1,
            price: Usd::new(dec!(100000)),
            quantity: None,
            event_time: as_of - Duration::minutes(1),
            published_at: as_of - Duration::minutes(1),
            available_at: as_of - Duration::minutes(1),
            valid_from: Some(as_of - Duration::minutes(1)),
            observations_timestamp: Some(as_of - Duration::minutes(1)),
            expires_at: Some(as_of + Duration::minutes(1)),
            report_hash: content_hash('f'),
            raw_report: "fixture".to_owned(),
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
        source_bindings: vec![
            source_binding(
                LinkageSourceRole::Feature,
                DomainSourceId::binance(),
                primary.observations[0].instrument_key.clone(),
                as_of - Duration::minutes(1),
            ),
            source_binding(
                LinkageSourceRole::Resolution,
                DomainSourceId::chainlink_data_streams(),
                oracle_instrument,
                as_of - Duration::minutes(1),
            ),
        ],
        grounding: GroundingProof { spans: Vec::new() },
        override_context: None,
    };
    let linkage_evidence = linkage_evidence(as_of);
    let domain = DomainConfig::default();
    let ctx = DomainComputeCtx {
        decision_at: as_of,
        binding: &binding,
        linkage_evidence: &linkage_evidence,
        data: DomainSliceDataRef::Crypto {
            primary: &primary,
            oracle: Some(&oracle),
        },
        domain: &domain,
    };
    let features = CryptoDomainFeatureBuilder.compute(&ctx);
    let basis = features
        .iter()
        .find(|f| f.name == BASIS_VS_RESOLUTION_SOURCE)
        .expect("basis feature");
    assert_eq!(
        basis.value.as_ref().expect("present"),
        &FeatureValue::Bps(dec!(50))
    );
}

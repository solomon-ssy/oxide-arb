//! Phase 03.2 §8 acceptance tests for the feature plane.

use std::collections::HashMap;
use std::sync::atomic::AtomicUsize;
use std::sync::{Arc, atomic::Ordering};

use chrono::{DateTime, Duration as ChronoDuration, TimeZone, Utc};
use quant_pivot_error::QuantResult;
use quant_pivot_models::runtime_config::SelectionConfig;
use quant_pivot_models::{
    domain::{
        DecisionBoundary, DecisionClock, DecisionSource, DomainAvailability, FeatureVectorInfo,
        MarketCandidate, MarketDataHealth, MarketRegistryInfo, TokenInfo,
        market::{
            book::BookLevel,
            registry::{EventRegistryInfo, NegRiskLeg, NegRiskLegSet},
        },
    },
    enums::{
        clickhouse::{ChFeatureCellState, ChFeatureSourceKind, ChFeatureValueKind},
        common::{CategorySet, MarketCategory, TickSize},
        market::{EventStatus, MarketStatus},
        quant::DataQualityStatus,
    },
    runtime_config::{
        DataQualityConfig, DomainConfig, FeatureFamily, FeatureStalenessPolicy, FeaturesConfig,
    },
    types::{
        Bps, CatalogSyncBatchId, ContentHash, EventCatalogVersionId, EventId,
        MarketCatalogVersionId, MarketId, Price, Probability, RuntimeConfigVersionId,
        SchemaVersion, Shares, TokenId, Usd,
    },
};
use rust_decimal::Decimal;
use rust_decimal_macros::dec;

use crate::features::names::structural::{NEGRISK_LEG_ASK_SUM, NEGRISK_LEG_COUNT};
use crate::{
    features::{
        FeatureBuildInput, FeatureBuilder, MarketWindowSnapshot, MicrostructureBucket,
        ResolvedBook, TradeTapeWindowSnapshot,
        availability::FeatureAvailabilityOracle,
        builder::ConfiguredFeatureBuilder,
        names::{book, domain_crypto, market},
        null_policy::{NullDecision, NullPolicyEngine},
        schema::FeatureSchema,
        value::{
            EvidenceSourceKind, EvidenceSourceRef, FeatureCell, FeatureCellState, FeatureName,
            FeatureStaleness, FeatureValue, FeatureVector, NullReason,
        },
        writer::feature_events,
    },
    hashing::ResearchHasher,
    pit::{BookSnapshotAt, MarketContextAt, PointInTimeSnapshotSource, ResolvedMarketSnapshot},
    selection::{
        FilterChain, FilterOutcome, MarketCandidateCtx, ModelFeatureRequirements, SelectedMarket,
        SelectionThresholds,
    },
};

fn test_context(
    market_id: &MarketId,
    boundary: &DecisionBoundary,
    neg_risk: bool,
) -> MarketContextAt {
    let observed_at = boundary.cutoff_for(DecisionSource::Catalog);
    MarketContextAt {
        market_id: market_id.clone(),
        effective_at: observed_at,
        available_at: boundary.decision_at(),
        status: MarketStatus::Active,
        neg_risk,
        end_date: Some(boundary.decision_at() + ChronoDuration::days(7)),
        created_at: Some(boundary.decision_at() - ChronoDuration::days(30)),
    }
}

fn test_catalog_snapshot(
    boundary: &DecisionBoundary,
    market_id: &MarketId,
    event_id: EventId,
    category: MarketCategory,
    primary_token: TokenId,
    context: MarketContextAt,
    neg_risk_leg_set: NegRiskLegSet,
) -> ResolvedMarketSnapshot {
    let effective_at = context.effective_at;
    let available_at = context.available_at;
    let mut event_market_ids = vec![market_id.clone()];
    for leg in &neg_risk_leg_set.legs {
        if !event_market_ids.contains(&leg.market_id) {
            event_market_ids.push(leg.market_id.clone());
        }
    }
    let event = EventRegistryInfo {
        event_id,
        title: "test event".to_owned(),
        slug: "test-event".to_owned(),
        series_slug: None,
        status: EventStatus::Active,
        market_ids: event_market_ids,
        categories: CategorySet::from(category),
        tags: Vec::new(),
        neg_risk: context.neg_risk,
        end_date: context.end_date,
        created_at: context.created_at.unwrap_or(context.effective_at),
        updated_at: context.effective_at,
    };
    let market = MarketRegistryInfo {
        market_id: market_id.clone(),
        event_id: event.event_id.clone(),
        token_yes: primary_token.clone(),
        token_no: TokenId::new(format!("{}-other", primary_token.as_str())),
        question: "test market?".to_owned(),
        slug: "test-market".to_owned(),
        description: None,
        categories: CategorySet::from(category),
        status: context.status,
        outcome: Some("Yes".to_owned()),
        neg_risk: context.neg_risk,
        tick_size: TickSize::Hundredth,
        tokens: vec![TokenInfo {
            token_id: primary_token,
            outcome: "Yes".to_owned(),
            neg_risk: context.neg_risk,
        }],
        best_bid: None,
        best_ask: None,
        depth_usd: None,
        min_order_size: Decimal::ONE,
        liquidity_usd: Some(Usd::new(Decimal::from(5_000))),
        volume_24h: None,
        fee_schedule: None,
        end_date: context.end_date,
        resolved_at: None,
        created_at: context.created_at,
        updated_at: context.effective_at,
    };
    ResolvedMarketSnapshot {
        boundary: boundary.clone(),
        market: Arc::new(market),
        event: Arc::new(event),
        context,
        neg_risk_leg_set,
        catalog_sync_batch_id: CatalogSyncBatchId::from_v7(),
        market_catalog_version_id: MarketCatalogVersionId::from_v7(),
        event_catalog_version_id: EventCatalogVersionId::from_v7(),
        market_content_hash: ContentHash::parse(format!("blake3:{}", "a".repeat(64)))
            .expect("valid market hash"),
        event_content_hash: ContentHash::parse(format!("blake3:{}", "b".repeat(64)))
            .expect("valid event hash"),
        membership_hash: ContentHash::parse(format!("blake3:{}", "c".repeat(64)))
            .expect("valid membership hash"),
        market_timestamp_quality: "source".to_owned(),
        event_timestamp_quality: "source".to_owned(),
        market_effective_at: effective_at,
        market_available_at: available_at,
        event_effective_at: effective_at,
        event_available_at: available_at,
    }
}

fn generic_event_id(market_id: &MarketId) -> EventId {
    match market_id.as_str() {
        "m-window" => EventId::new("e-window"),
        _ => EventId::new("e1"),
    }
}

fn healthy_book(token_id: &TokenId, as_of: DateTime<Utc>) -> BookSnapshotAt {
    BookSnapshotAt {
        token_id: token_id.clone(),
        source_cutoff: as_of,
        decision_at: as_of,
        bids: Arc::from([BookLevel::from_decimal_unchecked(
            Price::new(Decimal::new(49, 2)),
            Shares::new(Decimal::from(100)),
        )]),
        asks: Arc::from([BookLevel::from_decimal_unchecked(
            Price::new(Decimal::new(51, 2)),
            Shares::new(Decimal::from(100)),
        )]),
        timestamp_ms: u64::try_from(as_of.timestamp_millis()).expect("positive test time"),
        version: 1,
        sequence: 1,
        available_at: as_of,
    }
}

fn book_at_times(
    token_id: &TokenId,
    bid: Decimal,
    ask: Option<Decimal>,
    effective_at: DateTime<Utc>,
    source_cutoff: DateTime<Utc>,
    decision_at: DateTime<Utc>,
    version: u64,
) -> BookSnapshotAt {
    BookSnapshotAt {
        token_id: token_id.clone(),
        source_cutoff,
        decision_at,
        bids: Arc::from([BookLevel::from_decimal_unchecked(
            Price::new(bid),
            Shares::new(Decimal::from(100)),
        )]),
        asks: ask.map_or_else(
            || Arc::<[BookLevel]>::from([]),
            |ask| {
                Arc::from([BookLevel::from_decimal_unchecked(
                    Price::new(ask),
                    Shares::new(Decimal::from(100)),
                )])
            },
        ),
        timestamp_ms: u64::try_from(effective_at.timestamp_millis()).expect("positive test time"),
        version,
        sequence: version,
        available_at: decision_at - ChronoDuration::milliseconds(100),
    }
}

// ── Null policy ────────────────────────────────────────────────────────────

#[test]
fn feature_null_policy_rejects_required_missing() {
    let spec = FeatureSchema::build(&FeaturesConfig::default())
        .expect("schema")
        .by_name(&book::BEST_BID)
        .expect("spec")
        .clone();
    let data_quality = DataQualityConfig::default();

    let decision =
        NullPolicyEngine::decide(&spec, NullReason::SourceUnavailable, &data_quality, true);
    assert!(matches!(
        decision,
        NullDecision::Reject(NullReason::SourceUnavailable)
    ));
}

#[test]
fn feature_null_policy_no_silent_zero() {
    let spec = FeatureSchema::build(&FeaturesConfig::default())
        .expect("schema")
        .by_name(&book::DEPTH_IMBALANCE)
        .expect("spec")
        .clone();
    let data_quality = DataQualityConfig::default();

    let decision =
        NullPolicyEngine::decide(&spec, NullReason::SourceUnavailable, &data_quality, false);
    match decision {
        NullDecision::KeepMissing { reason, .. } => {
            assert_eq!(reason, NullReason::SourceUnavailable);
        }
        NullDecision::Substitute { .. } => {
            panic!("penalize path must preserve missingness, not substitute")
        }
        NullDecision::Reject(_) => {}
    }

    let missing_value = FeatureCell::missing(
        NullReason::SourceUnavailable,
        None,
        FeatureStaleness::Unknown,
    );
    assert!(missing_value.value().is_none());
    assert_ne!(
        missing_value,
        FeatureCell::observed(
            FeatureValue::Decimal(Decimal::ZERO),
            None,
            FeatureStaleness::Unknown,
        ),
        "missing must never equal zero"
    );
}

// ── PIT visibility ─────────────────────────────────────────────────────────

struct MemoryPitEngine {
    books: Vec<BookSnapshotAt>,
}

#[async_trait::async_trait]
impl PointInTimeSnapshotSource for MemoryPitEngine {
    async fn book_at_boundary(
        &self,
        token_id: &TokenId,
        boundary: &DecisionBoundary,
    ) -> QuantResult<Option<BookSnapshotAt>> {
        let source_cutoff = boundary.cutoff_for(DecisionSource::Book);
        let cutoff_ms = source_cutoff.timestamp_millis();
        Ok(self
            .books
            .iter()
            .filter(|book| {
                &book.token_id == token_id
                    && i64::try_from(book.timestamp_ms).is_ok_and(|ms| ms <= cutoff_ms)
                    && book.available_at <= boundary.decision_at()
            })
            .max_by_key(|book| (book.timestamp_ms, book.available_at, book.sequence))
            .map(|book| BookSnapshotAt {
                source_cutoff,
                decision_at: boundary.decision_at(),
                ..book.clone()
            }))
    }

    async fn market_snapshot_at(
        &self,
        market_id: &MarketId,
        boundary: &DecisionBoundary,
    ) -> QuantResult<Option<ResolvedMarketSnapshot>> {
        let token = self
            .books
            .first()
            .map_or_else(|| TokenId::new("test-token"), |book| book.token_id.clone());
        let context = test_context(market_id, boundary, false);
        Ok(Some(test_catalog_snapshot(
            boundary,
            market_id,
            generic_event_id(market_id),
            MarketCategory::Sports,
            token,
            context,
            NegRiskLegSet::empty(),
        )))
    }
}

#[tokio::test]
async fn feature_pit_visibility_excludes_future_book() {
    let token = TokenId::new("tok-pit");
    let as_of = Utc.with_ymd_and_hms(2025, 6, 1, 12, 0, 0).unwrap();
    let past = as_of - ChronoDuration::minutes(5);
    let future = as_of + ChronoDuration::minutes(5);

    let level = |price: Decimal| {
        BookLevel::from_decimal_unchecked(Price::new(price), Shares::new(Decimal::from(10)))
    };

    let engine = MemoryPitEngine {
        books: vec![
            BookSnapshotAt {
                token_id: token.clone(),
                source_cutoff: as_of,
                decision_at: as_of,
                bids: Arc::from([level(Decimal::new(45, 2))]),
                asks: Arc::from([level(Decimal::new(55, 2))]),
                timestamp_ms: u64::try_from(past.timestamp_millis()).unwrap_or(0),
                version: 1,
                sequence: 1,
                available_at: as_of,
            },
            BookSnapshotAt {
                token_id: token.clone(),
                source_cutoff: as_of,
                decision_at: as_of,
                bids: Arc::from([level(Decimal::new(99, 2))]),
                asks: Arc::from([level(Decimal::new(99, 2))]),
                timestamp_ms: u64::try_from(future.timestamp_millis()).unwrap_or(0),
                version: 2,
                sequence: 2,
                available_at: future,
            },
        ],
    };

    let boundary = DecisionClock::new(0).boundary(as_of).expect("boundary");
    let resolved = engine
        .book_at_boundary(&token, &boundary)
        .await
        .expect("resolve")
        .map(ResolvedBook::try_from)
        .transpose()
        .expect("valid book timestamp")
        .expect("book");
    assert_eq!(resolved.version, 1);
    assert_eq!(
        resolved.best_bid().expect("bid").inner(),
        Decimal::new(45, 2)
    );
}

#[tokio::test]
async fn primary_and_secondary_executable_asks_share_one_nonzero_lag_boundary() {
    let decision_at = Utc.with_ymd_and_hms(2025, 6, 1, 12, 0, 0).unwrap();
    let boundary = DecisionClock::new(2)
        .boundary(decision_at)
        .expect("boundary");
    let cutoff = boundary.cutoff_for(DecisionSource::Book);
    let primary = TokenId::new("tok-executable");
    let secondary = TokenId::new("tok-executable-other");
    let engine = MemoryPitEngine {
        books: vec![
            book_at_times(
                &primary,
                dec!(0.59),
                Some(dec!(0.61)),
                cutoff,
                cutoff,
                decision_at,
                1,
            ),
            book_at_times(
                &secondary,
                dec!(0.42),
                Some(dec!(0.44)),
                cutoff - ChronoDuration::seconds(1),
                cutoff,
                decision_at,
                2,
            ),
            // A later effective revision is known before decision time but is
            // still outside the source cutoff and must remain invisible.
            book_at_times(
                &secondary,
                dec!(0.98),
                Some(dec!(0.99)),
                cutoff + ChronoDuration::seconds(1),
                cutoff,
                decision_at,
                3,
            ),
        ],
    };
    let selected = SelectedMarket {
        market_id: MarketId::new("m-executable"),
        event_id: EventId::new("e1"),
        category: MarketCategory::Sports,
        primary_token_id: primary.clone(),
        secondary_token_id: Some(secondary.clone()),
        liquidity_usd: None,
        volume_24h_usd: None,
        source_refs: Vec::new(),
    };
    let config = FeaturesConfig {
        enabled_feature_families: vec![FeatureFamily::PriceBook],
        ..FeaturesConfig::default()
    };
    let builder =
        ConfiguredFeatureBuilder::new(&config, &DomainConfig::default()).expect("builder");
    let window = MarketWindowSnapshot::empty(primary.clone(), decision_at, cutoff);
    let trade_tape =
        TradeTapeWindowSnapshot::empty(selected.market_id.clone(), decision_at, cutoff);
    let vector = builder
        .build(FeatureBuildInput {
            market: &selected,
            boundary: &boundary,
            required_features: &[],
            pit: &engine,
            window: &window,
            trade_tape: &trade_tape,
            domain: None,
            config: &config,
            data_quality: &DataQualityConfig::default(),
        })
        .await
        .expect("feature build");

    let yes = vector.cell(&book::BEST_ASK).expect("YES ask cell");
    let no = vector.cell(&book::SECONDARY_BEST_ASK).expect("NO ask cell");
    assert_eq!(
        yes.value(),
        Some(&FeatureValue::Probability(Probability::new(dec!(0.61))))
    );
    assert_eq!(
        no.value(),
        Some(&FeatureValue::Probability(Probability::new(dec!(0.44))))
    );
    assert_eq!(yes.staleness, FeatureStaleness::Known { age_ms: 2_000 });
    assert_eq!(no.staleness, FeatureStaleness::Known { age_ms: 3_000 });
    assert!(
        no.evidence
            .as_ref()
            .is_some_and(|evidence| evidence.reference.contains(secondary.as_str())),
        "NO ask evidence must bind the secondary token snapshot"
    );
}

#[tokio::test]
async fn unquoted_secondary_ask_preserves_snapshot_evidence_without_a_value() {
    let decision_at = Utc.with_ymd_and_hms(2025, 6, 1, 12, 0, 0).unwrap();
    let boundary = DecisionClock::new(0)
        .boundary(decision_at)
        .expect("boundary");
    let primary = TokenId::new("tok-unquoted");
    let secondary = TokenId::new("tok-unquoted-other");
    let engine = MemoryPitEngine {
        books: vec![
            book_at_times(
                &primary,
                dec!(0.49),
                Some(dec!(0.51)),
                decision_at,
                decision_at,
                decision_at,
                1,
            ),
            book_at_times(
                &secondary,
                dec!(0.47),
                None,
                decision_at - ChronoDuration::seconds(1),
                decision_at,
                decision_at,
                2,
            ),
        ],
    };
    let selected = SelectedMarket {
        market_id: MarketId::new("m-unquoted"),
        event_id: EventId::new("e1"),
        category: MarketCategory::Sports,
        primary_token_id: primary.clone(),
        secondary_token_id: Some(secondary.clone()),
        liquidity_usd: None,
        volume_24h_usd: None,
        source_refs: Vec::new(),
    };
    let config = FeaturesConfig {
        enabled_feature_families: vec![FeatureFamily::PriceBook],
        ..FeaturesConfig::default()
    };
    let builder =
        ConfiguredFeatureBuilder::new(&config, &DomainConfig::default()).expect("builder");
    let window = MarketWindowSnapshot::empty(primary.clone(), decision_at, decision_at);
    let trade_tape =
        TradeTapeWindowSnapshot::empty(selected.market_id.clone(), decision_at, decision_at);
    let vector = builder
        .build(FeatureBuildInput {
            market: &selected,
            boundary: &boundary,
            required_features: &[],
            pit: &engine,
            window: &window,
            trade_tape: &trade_tape,
            domain: None,
            config: &config,
            data_quality: &DataQualityConfig::default(),
        })
        .await
        .expect("feature build");
    let no = vector.cell(&book::SECONDARY_BEST_ASK).expect("NO ask cell");

    assert_eq!(no.state, FeatureCellState::Missing);
    assert_eq!(no.reason, Some(NullReason::SourceUnavailable));
    assert!(no.value().is_none());
    assert_eq!(no.staleness, FeatureStaleness::Known { age_ms: 1_000 });
    assert!(
        no.evidence
            .as_ref()
            .is_some_and(|evidence| evidence.reference.contains(secondary.as_str()))
    );
}

// ── Hash stability ─────────────────────────────────────────────────────────

fn sample_vector() -> FeatureVector {
    let as_of = Utc.with_ymd_and_hms(2025, 1, 1, 0, 0, 0).unwrap();
    let mut values = std::collections::BTreeMap::new();
    values.insert(
        book::MID,
        FeatureCell::observed(
            FeatureValue::Probability(Probability::new(Decimal::new(50, 2))),
            Some(EvidenceSourceRef {
                source_kind: EvidenceSourceKind::Book,
                reference: "book:m1:t1:v1".to_owned(),
                effective_at: as_of - ChronoDuration::seconds(60),
                available_at: Some(as_of - ChronoDuration::seconds(59)),
            }),
            FeatureStaleness::Known { age_ms: 60_000 },
        ),
    );
    FeatureVector {
        market_id: MarketId::new("m1"),
        token_id: Some(TokenId::new("t1")),
        decision_at: as_of,
        generic_schema_version: SchemaVersion::FIRST,
        generic: values,
        domain: None,
        data_quality: DataQualityStatus::Fresh,
    }
}

fn persisted_feature(vector: &FeatureVector) -> FeatureVectorInfo {
    let boundary = DecisionClock::new(0)
        .boundary(vector.decision_at)
        .expect("decision boundary");
    persisted_feature_at(vector, &boundary)
}

fn persisted_feature_at(vector: &FeatureVector, boundary: &DecisionBoundary) -> FeatureVectorInfo {
    let row = vector
        .try_to_new(boundary)
        .expect("feature persistence projection");
    FeatureVectorInfo {
        feature_vector_id: row.feature_vector_id,
        market_id: row.market_id,
        token_id: row.token_id,
        decision_at: row.decision_at,
        decision_boundary: row.decision_boundary,
        feature_schema_version: row.feature_schema_version,
        feature_hash: row.feature_hash,
        data_quality: row.data_quality,
        staleness_ms: row.staleness_ms,
        payload: row.payload,
        source_refs: row.source_refs,
        decision_capture: Some(serde_json::json!({"test": true})),
        decision_capture_hash: Some(
            ContentHash::parse(format!("blake3:{}", "d".repeat(64))).expect("capture hash"),
        ),
        created_at: vector.decision_at,
    }
}

#[test]
fn feature_hash_stable_for_same_input() {
    let left = sample_vector();
    let right = sample_vector();
    assert_eq!(
        ResearchHasher::feature_vector(&left).expect("hash"),
        ResearchHasher::feature_vector(&right).expect("hash"),
    );
}

#[test]
fn feature_schema_version_change_changes_hash() {
    let config_v1 = FeaturesConfig {
        feature_schema_version: SchemaVersion::FIRST,
        ..FeaturesConfig::default()
    };
    let config_v2 = FeaturesConfig {
        feature_schema_version: SchemaVersion::new(2),
        ..FeaturesConfig::default()
    };

    let schema_v1 = FeatureSchema::build(&config_v1).expect("schema v1");
    let schema_v2 = FeatureSchema::build(&config_v2).expect("schema v2");
    assert_ne!(
        ResearchHasher::feature_schema(&schema_v1).expect("hash"),
        ResearchHasher::feature_schema(&schema_v2).expect("hash"),
    );
}

// ── Domain missing ─────────────────────────────────────────────────────────

struct HealthyPit;

#[async_trait::async_trait]
impl PointInTimeSnapshotSource for HealthyPit {
    async fn book_at_boundary(
        &self,
        token_id: &TokenId,
        boundary: &DecisionBoundary,
    ) -> QuantResult<Option<BookSnapshotAt>> {
        let source_cutoff = boundary.cutoff_for(DecisionSource::Book);
        Ok(Some(BookSnapshotAt {
            decision_at: boundary.decision_at(),
            ..healthy_book(token_id, source_cutoff)
        }))
    }

    async fn market_snapshot_at(
        &self,
        market_id: &MarketId,
        boundary: &DecisionBoundary,
    ) -> QuantResult<Option<ResolvedMarketSnapshot>> {
        let context = test_context(market_id, boundary, false);
        Ok(Some(test_catalog_snapshot(
            boundary,
            market_id,
            generic_event_id(market_id),
            MarketCategory::Sports,
            TokenId::new("test-token"),
            context,
            NegRiskLegSet::empty(),
        )))
    }
}

#[tokio::test]
async fn binary_market_negrisk_feature_is_not_applicable() {
    // 11.2.1: a binary (non-neg-risk) market's neg-risk full-leg aggregate is
    // `NotApplicable` — structurally absent, never a fabricated zero and never a
    // data-missing reason.
    let config = FeaturesConfig {
        enabled_feature_families: vec![FeatureFamily::Structural],
        ..FeaturesConfig::default()
    };
    let builder =
        ConfiguredFeatureBuilder::new(&config, &DomainConfig::default()).expect("feature builder");
    let market = SelectedMarket {
        market_id: MarketId::new("m-binary"),
        event_id: EventId::new("e1"),
        category: MarketCategory::Sports,
        primary_token_id: TokenId::new("t1"),
        secondary_token_id: None,
        liquidity_usd: None,
        volume_24h_usd: None,
        source_refs: Vec::new(),
    };
    let pit = HealthyPit;
    let as_of = Utc::now();
    let boundary = DecisionClock::new(0).boundary(as_of).expect("boundary");
    let window = MarketWindowSnapshot::empty(market.primary_token_id.clone(), as_of, as_of);
    let trade_tape = TradeTapeWindowSnapshot::empty(market.market_id.clone(), as_of, as_of);
    let vector = builder
        .build(FeatureBuildInput {
            market: &market,
            boundary: &boundary,
            required_features: &[],
            pit: &pit,
            window: &window,
            trade_tape: &trade_tape,
            domain: None,
            config: &config,
            data_quality: &DataQualityConfig::default(),
        })
        .await
        .expect("build");

    let value = vector
        .generic
        .get(&NEGRISK_LEG_ASK_SUM)
        .expect("structural neg-risk feature present in schema");
    assert_eq!(
        value.reason,
        Some(NullReason::NotApplicable),
        "a binary market's neg-risk aggregate must be NotApplicable",
    );
}

#[tokio::test]
async fn unresolved_mapped_domain_is_missing_not_not_applicable() {
    let config = FeaturesConfig {
        enabled_feature_families: vec![FeatureFamily::Domain],
        ..FeaturesConfig::default()
    };
    let builder =
        ConfiguredFeatureBuilder::new(&config, &DomainConfig::default()).expect("feature builder");
    let market = SelectedMarket {
        market_id: MarketId::new("m-crypto-unresolved"),
        event_id: EventId::new("e1"),
        category: MarketCategory::Crypto,
        primary_token_id: TokenId::new("t-crypto"),
        secondary_token_id: None,
        liquidity_usd: None,
        volume_24h_usd: None,
        source_refs: Vec::new(),
    };
    let as_of = Utc::now();
    let boundary = DecisionClock::new(0).boundary(as_of).expect("boundary");
    let window = MarketWindowSnapshot::empty(market.primary_token_id.clone(), as_of, as_of);
    let trade_tape = TradeTapeWindowSnapshot::empty(market.market_id.clone(), as_of, as_of);
    let vector = builder
        .build(FeatureBuildInput {
            market: &market,
            boundary: &boundary,
            required_features: &[],
            pit: &HealthyPit,
            window: &window,
            trade_tape: &trade_tape,
            domain: None,
            config: &config,
            data_quality: &DataQualityConfig::default(),
        })
        .await
        .expect("build unresolved crypto vector");

    let domain = vector
        .domain
        .expect("mapped category must carry domain cells");
    let distance = domain
        .values
        .get(&domain_crypto::DISTANCE_TO_STRIKE)
        .expect("distance-to-strike cell");
    assert_eq!(distance.state, FeatureCellState::Missing);
    assert_eq!(distance.reason, Some(NullReason::LinkageUnresolved));
    assert_eq!(distance.staleness, FeatureStaleness::Unknown);
}

// ── Family gating (feature inputs vs decision capture) ─────────────────────

/// A PIT source that counts how many book / market lookups the builder issues.
struct CountingPit {
    book_calls: AtomicUsize,
    market_calls: AtomicUsize,
}

#[async_trait::async_trait]
impl PointInTimeSnapshotSource for CountingPit {
    async fn book_at_boundary(
        &self,
        token_id: &TokenId,
        boundary: &DecisionBoundary,
    ) -> QuantResult<Option<BookSnapshotAt>> {
        self.book_calls.fetch_add(1, Ordering::Relaxed);
        let source_cutoff = boundary.cutoff_for(DecisionSource::Book);
        Ok(Some(BookSnapshotAt {
            decision_at: boundary.decision_at(),
            ..healthy_book(token_id, source_cutoff)
        }))
    }

    async fn market_snapshot_at(
        &self,
        market_id: &MarketId,
        boundary: &DecisionBoundary,
    ) -> QuantResult<Option<ResolvedMarketSnapshot>> {
        self.market_calls.fetch_add(1, Ordering::Relaxed);
        let context = test_context(market_id, boundary, false);
        Ok(Some(test_catalog_snapshot(
            boundary,
            market_id,
            generic_event_id(market_id),
            MarketCategory::Sports,
            TokenId::new("test-token"),
            context,
            NegRiskLegSet::empty(),
        )))
    }
}

#[tokio::test]
async fn builder_resolves_capture_inputs_even_when_feature_families_skip_book() {
    // Only time-series + microstructure: no book- or metadata-sourced features, but
    // decision capture still resolves book + market context for evidence refs.
    let config = FeaturesConfig {
        enabled_feature_families: vec![FeatureFamily::TimeSeries, FeatureFamily::Microstructure],
        ..FeaturesConfig::default()
    };
    let builder =
        ConfiguredFeatureBuilder::new(&config, &DomainConfig::default()).expect("feature builder");
    let market = SelectedMarket {
        market_id: MarketId::new("m-gate"),
        event_id: EventId::new("e1"),
        category: MarketCategory::Sports,
        primary_token_id: TokenId::new("t1"),
        secondary_token_id: None,
        liquidity_usd: None,
        volume_24h_usd: None,
        source_refs: Vec::new(),
    };
    let source = CountingPit {
        book_calls: AtomicUsize::new(0),
        market_calls: AtomicUsize::new(0),
    };
    let as_of = Utc::now();
    let boundary = DecisionClock::new(0).boundary(as_of).expect("boundary");
    let window = MarketWindowSnapshot::empty(market.primary_token_id.clone(), as_of, as_of);
    let trade_tape = TradeTapeWindowSnapshot::empty(market.market_id.clone(), as_of, as_of);
    builder
        .build(FeatureBuildInput {
            market: &market,
            boundary: &boundary,
            required_features: &[],
            pit: &source,
            window: &window,
            trade_tape: &trade_tape,
            domain: None,
            config: &config,
            data_quality: &DataQualityConfig::default(),
        })
        .await
        .expect("build");

    assert_eq!(
        source.book_calls.load(Ordering::Relaxed),
        1,
        "decision capture resolves book once even when feature compute skips book"
    );
    assert_eq!(
        source.market_calls.load(Ordering::Relaxed),
        1,
        "decision capture resolves one immutable catalog snapshot"
    );
}

// ── CH writer (pure projection) ────────────────────────────────────────────

#[test]
fn feature_event_writer_emits_every_cell_state_with_full_audit_context() {
    let schema = FeatureSchema::build(&FeaturesConfig::default()).expect("schema");
    let mut vector = sample_vector();
    vector.generic_schema_version = schema.version();
    vector.generic.insert(
        book::CROSSED,
        FeatureCell::substituted(
            FeatureValue::Bool(false),
            NullReason::SourceUnavailable,
            None,
            FeatureStaleness::Unknown,
        ),
    );
    vector.generic.insert(
        book::SPREAD_BPS,
        FeatureCell::missing(
            NullReason::SourceUnavailable,
            None,
            FeatureStaleness::Unknown,
        ),
    );
    vector.generic.insert(
        NEGRISK_LEG_COUNT,
        FeatureCell::not_applicable(NullReason::NotApplicable),
    );
    let boundary = DecisionClock::new(30)
        .boundary(vector.decision_at)
        .expect("boundary")
        .with_source_cutoff(DecisionSource::Book, 60)
        .expect("book cutoff");
    let persisted = persisted_feature_at(&vector, &boundary);
    let runtime_config_version_id = RuntimeConfigVersionId::from_v7();

    let rows = feature_events(
        &vector,
        &persisted,
        &boundary,
        &runtime_config_version_id,
        &schema,
        1_000,
    )
    .expect("feature rows");
    assert_eq!(rows.len(), vector.value_count());

    let observed = rows
        .iter()
        .find(|row| row.feature_name == "book.mid")
        .expect("observed row");
    assert_eq!(observed.cell_state, ChFeatureCellState::Observed);
    assert_eq!(observed.raw_value.as_deref(), Some("0.50"));
    assert_eq!(observed.value_kind, ChFeatureValueKind::Probability);
    assert_eq!(observed.source_kind, ChFeatureSourceKind::Book);
    assert_eq!(
        observed.evidence_source_kind,
        Some(ChFeatureSourceKind::Book)
    );
    assert_eq!(
        observed.evidence_effective_at,
        Some(vector.decision_at.timestamp_millis() - 60_000)
    );
    assert_eq!(
        observed.evidence_available_at,
        Some(vector.decision_at.timestamp_millis() - 59_000)
    );
    assert_eq!(observed.staleness_ms, Some(60_000));
    assert_eq!(observed.feature_vector_id, persisted.feature_vector_id);
    assert_eq!(
        observed.runtime_config_version_id,
        runtime_config_version_id
    );
    assert_eq!(observed.decision_at, vector.decision_at.timestamp_millis());
    assert_eq!(
        observed.knowledge_cutoff,
        (vector.decision_at - ChronoDuration::seconds(30)).timestamp_millis()
    );
    assert!(observed.per_source_cutoffs_json.contains("book"));
    assert!(!observed.feature_schema_hash.is_empty());
    assert_eq!(observed.feature_hash, persisted.feature_hash.as_str());
    assert_eq!(observed.data_quality, "fresh");
    assert!(observed.audit_fingerprint.starts_with("blake3:"));
    assert_eq!(observed.ingestion_time, 1_000);

    let missing = rows
        .iter()
        .find(|row| row.feature_name == "book.spread_bps")
        .expect("missing row");
    assert_eq!(missing.cell_state, ChFeatureCellState::Missing);
    assert_eq!(missing.raw_value, None);
    assert_eq!(missing.reason.as_deref(), Some("source_unavailable"));
    assert_eq!(missing.staleness_ms, None);

    let substituted = rows
        .iter()
        .find(|row| row.feature_name == "book.crossed")
        .expect("substituted row");
    assert_eq!(substituted.cell_state, ChFeatureCellState::Substituted);
    assert_eq!(substituted.raw_value.as_deref(), Some("false"));

    let not_applicable = rows
        .iter()
        .find(|row| row.feature_name == "struct.negrisk_leg_count")
        .expect("not-applicable row");
    assert_eq!(not_applicable.cell_state, ChFeatureCellState::NotApplicable);
    assert_eq!(not_applicable.reason.as_deref(), Some("not_applicable"));
}

#[test]
fn feature_event_writer_rejects_persistence_binding_mismatch() {
    let schema = FeatureSchema::build(&FeaturesConfig::default()).expect("schema");
    let mut vector = sample_vector();
    vector.generic_schema_version = schema.version();
    let mut persisted = persisted_feature(&vector);
    persisted.market_id = MarketId::new("wrong-market");
    let boundary = DecisionClock::new(0)
        .boundary(vector.decision_at)
        .expect("boundary");
    let error = feature_events(
        &vector,
        &persisted,
        &boundary,
        &RuntimeConfigVersionId::from_v7(),
        &schema,
        1_000,
    )
    .expect_err("binding mismatch must fail");
    assert!(error.to_string().contains("market_id"));
}

// ── Availability oracle ────────────────────────────────────────────────────

fn candidate_with_book() -> MarketCandidate {
    MarketCandidate {
        market_id: MarketId::new("m1"),
        event_id: EventId::new("e1"),
        category: MarketCategory::Sports,
        status: MarketStatus::Active,
        primary_token_id: TokenId::new("t1"),
        secondary_token_id: None,
        end_date: Some(Utc::now() + ChronoDuration::days(30)),
        liquidity_usd: Some(Usd::new(Decimal::from(1_000))),
        volume_24h_usd: Some(Usd::new(Decimal::from(500))),
        best_bid: Some(Price::new(Decimal::new(48, 2))),
        best_ask: Some(Price::new(Decimal::new(52, 2))),
        depth_usd: Some(Usd::new(Decimal::from(200))),
        book_age_ms: Some(500),
        market_data_health: MarketDataHealth::Healthy,
        ingest_lag_ms: Some(0),
        domain_availability: DomainAvailability::NotMapped,
        crossed: Some(false),
        empty: Some(false),
        decision_at: Utc::now(),
    }
}

#[test]
fn model_eligibility_uses_real_availability_oracle() {
    let schema = FeatureSchema::build(&FeaturesConfig::default()).expect("schema");
    let oracle = FeatureAvailabilityOracle::new(&schema);
    let candidate = candidate_with_book();

    let book_required = vec![book::BEST_BID];
    assert!(
        oracle
            .missing_required(&candidate, &book_required)
            .is_empty()
    );

    // A feature the schema does not define is never claimed as available.
    let unknown_required = vec![FeatureName::from_static("nonexistent.feature")];
    let missing = oracle.missing_required(&candidate, &unknown_required);
    assert_eq!(missing.len(), 1);

    let thresholds = SelectionThresholds::resolve(
        &SelectionConfig {
            enabled_categories: vec![MarketCategory::Sports],
            ..SelectionConfig::default()
        },
        &DataQualityConfig::default(),
    )
    .expect("thresholds");
    let ctx = MarketCandidateCtx {
        candidate: &candidate,
        thresholds: &thresholds,
        decision_at: Utc::now(),
        model_requirements: &ModelFeatureRequirements::generic_only(book_required),
        feature_schema: &schema,
    };
    assert!(matches!(
        FilterChain::standard().evaluate(&ctx),
        FilterOutcome::Keep
    ));

    let ctx = MarketCandidateCtx {
        model_requirements: &ModelFeatureRequirements::generic_only(unknown_required),
        ..ctx
    };
    assert!(matches!(
        FilterChain::standard().evaluate(&ctx),
        FilterOutcome::Exclude(_)
    ));
}

// ── Online / offline parity ─────────────────────────────────────────────────

#[derive(Clone)]
struct ParityFixture {
    token: TokenId,
    market_id: MarketId,
    as_of: DateTime<Utc>,
    book: BookSnapshotAt,
    market: MarketContextAt,
}

struct ParityPitEngine {
    fixture: ParityFixture,
}

fn parity_snapshot(fixture: &ParityFixture, boundary: &DecisionBoundary) -> ResolvedMarketSnapshot {
    let event_id = if fixture.market_id.as_str() == "m-parity-win" {
        EventId::new("evt-parity-win")
    } else {
        EventId::new("evt-parity")
    };
    test_catalog_snapshot(
        boundary,
        &fixture.market_id,
        event_id,
        MarketCategory::Sports,
        fixture.token.clone(),
        fixture.market.clone(),
        NegRiskLegSet::empty(),
    )
}

#[async_trait::async_trait]
impl PointInTimeSnapshotSource for ParityPitEngine {
    async fn book_at_boundary(
        &self,
        token_id: &TokenId,
        boundary: &DecisionBoundary,
    ) -> QuantResult<Option<BookSnapshotAt>> {
        if token_id == &self.fixture.token
            && boundary.cutoff_for(DecisionSource::Book) == self.fixture.book.source_cutoff
            && boundary.decision_at() == self.fixture.as_of
        {
            Ok(Some(self.fixture.book.clone()))
        } else {
            Ok(None)
        }
    }

    async fn market_snapshot_at(
        &self,
        market_id: &MarketId,
        boundary: &DecisionBoundary,
    ) -> QuantResult<Option<ResolvedMarketSnapshot>> {
        if market_id == &self.fixture.market_id && boundary.decision_at() == self.fixture.as_of {
            Ok(Some(parity_snapshot(&self.fixture, boundary)))
        } else {
            Ok(None)
        }
    }
}

struct ParityLiveSource {
    fixture: ParityFixture,
}

#[async_trait::async_trait]
impl PointInTimeSnapshotSource for ParityLiveSource {
    async fn book_at_boundary(
        &self,
        token_id: &TokenId,
        boundary: &DecisionBoundary,
    ) -> QuantResult<Option<BookSnapshotAt>> {
        if token_id == &self.fixture.token
            && boundary.cutoff_for(DecisionSource::Book) == self.fixture.book.source_cutoff
            && boundary.decision_at() == self.fixture.as_of
        {
            Ok(Some(self.fixture.book.clone()))
        } else {
            Ok(None)
        }
    }

    async fn market_snapshot_at(
        &self,
        market_id: &MarketId,
        boundary: &DecisionBoundary,
    ) -> QuantResult<Option<ResolvedMarketSnapshot>> {
        if market_id == &self.fixture.market_id && boundary.decision_at() == self.fixture.as_of {
            Ok(Some(parity_snapshot(&self.fixture, boundary)))
        } else {
            Ok(None)
        }
    }
}

#[tokio::test]
async fn online_offline_feature_parity() {
    let as_of = Utc.with_ymd_and_hms(2025, 6, 1, 12, 0, 0).unwrap();
    let token = TokenId::new("tok-parity");
    let market_id = MarketId::new("m-parity");
    let level = BookLevel::from_decimal_unchecked(
        Price::new(Decimal::new(49, 2)),
        Shares::new(Decimal::from(50)),
    );

    let fixture = ParityFixture {
        token: token.clone(),
        market_id: market_id.clone(),
        as_of,
        book: BookSnapshotAt {
            token_id: token.clone(),
            source_cutoff: as_of,
            decision_at: as_of,
            bids: Arc::from([level]),
            asks: Arc::from([BookLevel::from_decimal_unchecked(
                Price::new(Decimal::new(51, 2)),
                Shares::new(Decimal::from(50)),
            )]),
            timestamp_ms: u64::try_from(as_of.timestamp_millis()).unwrap_or(0),
            version: 7,
            sequence: 7,
            available_at: as_of,
        },
        market: MarketContextAt {
            market_id: market_id.clone(),
            effective_at: as_of,
            available_at: as_of,
            status: MarketStatus::Active,
            neg_risk: false,
            end_date: Some(as_of + ChronoDuration::days(3)),
            created_at: Some(as_of - ChronoDuration::days(10)),
        },
    };

    let config = FeaturesConfig {
        enabled_feature_families: vec![FeatureFamily::MarketMetadata, FeatureFamily::PriceBook],
        ..FeaturesConfig::default()
    };
    let builder =
        ConfiguredFeatureBuilder::new(&config, &DomainConfig::default()).expect("feature builder");
    let boundary = DecisionClock::new(0).boundary(as_of).expect("boundary");
    let selected = SelectedMarket {
        market_id: market_id.clone(),
        event_id: EventId::new("evt-parity"),
        category: MarketCategory::Sports,
        primary_token_id: token.clone(),
        secondary_token_id: None,
        liquidity_usd: Some(Usd::new(Decimal::from(5_000))),
        volume_24h_usd: None,
        source_refs: Vec::new(),
    };
    let window = MarketWindowSnapshot::empty(token.clone(), as_of, as_of);
    let trade_tape = TradeTapeWindowSnapshot::empty(selected.market_id.clone(), as_of, as_of);
    let data_quality = DataQualityConfig {
        feature_staleness_policy: FeatureStalenessPolicy::AllowDegraded,
        ..DataQualityConfig::default()
    };

    let live_source = ParityLiveSource {
        fixture: fixture.clone(),
    };
    let live = builder
        .build(FeatureBuildInput {
            market: &selected,
            boundary: &boundary,
            required_features: &[],
            pit: &live_source,
            window: &window,
            trade_tape: &trade_tape,
            domain: None,
            config: &config,
            data_quality: &data_quality,
        })
        .await
        .expect("live build");

    let hist_engine = ParityPitEngine {
        fixture: fixture.clone(),
    };
    let historical = builder
        .build(FeatureBuildInput {
            market: &selected,
            boundary: &boundary,
            required_features: &[],
            pit: &hist_engine,
            window: &window,
            trade_tape: &trade_tape,
            domain: None,
            config: &config,
            data_quality: &data_quality,
        })
        .await
        .expect("historical build");

    assert_eq!(live.generic, historical.generic);
    assert_eq!(
        ResearchHasher::feature_vector(&live).expect("hash"),
        ResearchHasher::feature_vector(&historical).expect("hash"),
    );
}

// ── Window-backed time-series, staleness, range, and category ────────────────

/// One microstructure bucket with sensible defaults for the window-backed tests.
fn micro_bucket(
    time: DateTime<Utc>,
    mid: Decimal,
    spread: Decimal,
    depth: Decimal,
) -> MicrostructureBucket {
    MicrostructureBucket {
        bucket_time: time,
        available_at: time,
        mid_close: Some(Price::new(mid)),
        spread_bps_avg: Some(Bps::new(spread)),
        top1_depth_usd_avg: Some(Usd::new(depth)),
        top5_depth_usd_avg: Some(Usd::new(depth)),
        imbalance_avg: Some(Decimal::ZERO),
        update_count: 10,
        snapshot_count: 1,
        delta_count: 9,
        crossed_count: 0,
        gap_count: 0,
        max_book_age_ms: 100,
    }
}

/// A minimal selected market for the window-backed tests.
fn windowed_market(token: &TokenId) -> SelectedMarket {
    SelectedMarket {
        market_id: MarketId::new("m-window"),
        event_id: EventId::new("e-window"),
        category: MarketCategory::Sports,
        primary_token_id: token.clone(),
        secondary_token_id: None,
        liquidity_usd: None,
        volume_24h_usd: None,
        source_refs: Vec::new(),
    }
}

/// A single bid/ask level at the given price.
fn lvl(price: Decimal) -> BookLevel {
    BookLevel::from_decimal_unchecked(Price::new(price), Shares::new(Decimal::from(10)))
}

#[tokio::test]
async fn feature_window_nonempty_yields_timeseries_and_is_not_stale() {
    let as_of = Utc.with_ymd_and_hms(2025, 6, 1, 12, 0, 0).unwrap();
    let token = TokenId::new("tok-window");
    // Five 1s buckets within the trailing minute; the freshest is 1s old.
    let buckets: Vec<MicrostructureBucket> = (1_i64..=5)
        .rev()
        .map(|secs| {
            micro_bucket(
                as_of - ChronoDuration::seconds(secs),
                Decimal::new(50, 2) + Decimal::new(5 - secs, 3),
                Decimal::from(20),
                Decimal::from(1_000),
            )
        })
        .collect();
    let window = MarketWindowSnapshot {
        token_id: token.clone(),
        decision_at: as_of,
        knowledge_cutoff: as_of,
        buckets,
    };
    let market = windowed_market(&token);
    let trade_tape = TradeTapeWindowSnapshot::empty(market.market_id.clone(), as_of, as_of);
    let config = FeaturesConfig {
        enabled_feature_families: vec![FeatureFamily::TimeSeries],
        ..FeaturesConfig::default()
    };
    let builder =
        ConfiguredFeatureBuilder::new(&config, &DomainConfig::default()).expect("feature builder");
    let boundary = DecisionClock::new(0).boundary(as_of).expect("boundary");
    let vector = builder
        .build(FeatureBuildInput {
            market: &market,
            boundary: &boundary,
            required_features: &[],
            pit: &HealthyPit,
            window: &window,
            trade_tape: &trade_tape,
            domain: None,
            config: &config,
            data_quality: &DataQualityConfig::default(),
        })
        .await
        .expect("build");

    let ret = vector
        .generic
        .get(&FeatureName::ts_return(60))
        .expect("return present");
    assert!(
        ret.value().is_some(),
        "a non-empty window must yield a real return"
    );
    assert_ne!(
        vector.data_quality,
        DataQualityStatus::Stale,
        "a 1s-fresh window must not be Stale (regression: fact lag was judged by the book-age bound)"
    );
}

#[tokio::test]
async fn feature_stale_book_rejects_market() {
    let as_of = Utc.with_ymd_and_hms(2025, 6, 1, 12, 0, 0).unwrap();
    let token = TokenId::new("tok-stale");
    // 10s old > the 5s default `max_book_age_ms`.
    let stale_ts = as_of - ChronoDuration::seconds(10);
    let engine = MemoryPitEngine {
        books: vec![BookSnapshotAt {
            token_id: token.clone(),
            source_cutoff: as_of,
            decision_at: as_of,
            bids: Arc::from([lvl(Decimal::new(48, 2))]),
            asks: Arc::from([lvl(Decimal::new(52, 2))]),
            timestamp_ms: u64::try_from(stale_ts.timestamp_millis()).unwrap_or(0),
            version: 1,
            sequence: 1,
            available_at: stale_ts,
        }],
    };
    let config = FeaturesConfig {
        enabled_feature_families: vec![FeatureFamily::PriceBook],
        ..FeaturesConfig::default()
    };
    let builder =
        ConfiguredFeatureBuilder::new(&config, &DomainConfig::default()).expect("feature builder");
    let market = windowed_market(&token);
    let boundary = DecisionClock::new(0).boundary(as_of).expect("boundary");
    let window = MarketWindowSnapshot::empty(token.clone(), as_of, as_of);
    let trade_tape = TradeTapeWindowSnapshot::empty(market.market_id.clone(), as_of, as_of);
    let vector = builder
        .build(FeatureBuildInput {
            market: &market,
            boundary: &boundary,
            required_features: &[],
            pit: &engine,
            window: &window,
            trade_tape: &trade_tape,
            domain: None,
            config: &config,
            data_quality: &DataQualityConfig::default(),
        })
        .await
        .expect("build");

    assert_eq!(
        vector.data_quality,
        DataQualityStatus::Insufficient,
        "a stale book must reject the market"
    );
    assert_eq!(
        vector
            .generic
            .get(&book::BEST_BID)
            .and_then(|cell| cell.reason),
        Some(NullReason::StaleBeyondPolicy),
    );
}

#[tokio::test]
async fn allow_degraded_keeps_stale_required_fact_feature() {
    let as_of = Utc.with_ymd_and_hms(2025, 6, 1, 12, 0, 0).unwrap();
    let token = TokenId::new("tok-degraded");
    // Buckets older than the 30s `max_feature_bucket_age_secs` but within the 60s window,
    // with >= 2 mids so the return is *present* (and so subject to staleness).
    let buckets = vec![
        micro_bucket(
            as_of - ChronoDuration::seconds(45),
            Decimal::new(50, 2),
            Decimal::from(20),
            Decimal::from(1_000),
        ),
        micro_bucket(
            as_of - ChronoDuration::seconds(40),
            Decimal::new(51, 2),
            Decimal::from(20),
            Decimal::from(1_000),
        ),
        micro_bucket(
            as_of - ChronoDuration::seconds(35),
            Decimal::new(50, 2),
            Decimal::from(20),
            Decimal::from(1_000),
        ),
    ];
    let window = MarketWindowSnapshot {
        token_id: token.clone(),
        decision_at: as_of,
        knowledge_cutoff: as_of,
        buckets,
    };
    let market = windowed_market(&token);
    let trade_tape = TradeTapeWindowSnapshot::empty(market.market_id.clone(), as_of, as_of);
    let config = FeaturesConfig {
        enabled_feature_families: vec![FeatureFamily::TimeSeries],
        ..FeaturesConfig::default()
    };
    let builder =
        ConfiguredFeatureBuilder::new(&config, &DomainConfig::default()).expect("feature builder");
    let boundary = DecisionClock::new(0).boundary(as_of).expect("boundary");
    let required = vec![FeatureName::ts_return(60)];

    // Default policy rejects a stale *required* feature ⇒ Insufficient.
    let strict = builder
        .build(FeatureBuildInput {
            market: &market,
            boundary: &boundary,
            required_features: &required,
            pit: &HealthyPit,
            window: &window,
            trade_tape: &trade_tape,
            domain: None,
            config: &config,
            data_quality: &DataQualityConfig::default(),
        })
        .await
        .expect("strict build");
    assert_eq!(strict.data_quality, DataQualityStatus::Insufficient);

    // `AllowDegraded` keeps it missing instead of rejecting (aggregate is Stale
    // because the whole window is older than the fact-lag bound, but the market
    // is *not* dropped).
    let lenient = DataQualityConfig {
        feature_staleness_policy: FeatureStalenessPolicy::AllowDegraded,
        ..DataQualityConfig::default()
    };
    let degraded = builder
        .build(FeatureBuildInput {
            market: &market,
            boundary: &boundary,
            required_features: &required,
            pit: &HealthyPit,
            window: &window,
            trade_tape: &trade_tape,
            domain: None,
            config: &config,
            data_quality: &lenient,
        })
        .await
        .expect("lenient build");
    assert_ne!(degraded.data_quality, DataQualityStatus::Insufficient);
    assert_eq!(
        degraded
            .generic
            .get(&FeatureName::ts_return(60))
            .and_then(|cell| cell.reason),
        Some(NullReason::StaleBeyondPolicy),
    );
}

#[tokio::test]
async fn feature_out_of_valid_range_rejects() {
    let as_of = Utc.with_ymd_and_hms(2025, 6, 1, 12, 0, 0).unwrap();
    let token = TokenId::new("tok-oor");
    // best_bid = 1.50 is outside the `[0, 1]` probability range.
    let engine = MemoryPitEngine {
        books: vec![BookSnapshotAt {
            token_id: token.clone(),
            source_cutoff: as_of,
            decision_at: as_of,
            bids: Arc::from([lvl(Decimal::new(150, 2))]),
            asks: Arc::from([lvl(Decimal::new(160, 2))]),
            timestamp_ms: u64::try_from(as_of.timestamp_millis()).unwrap_or(0),
            version: 1,
            sequence: 1,
            available_at: as_of,
        }],
    };
    let config = FeaturesConfig {
        enabled_feature_families: vec![FeatureFamily::PriceBook],
        ..FeaturesConfig::default()
    };
    let builder =
        ConfiguredFeatureBuilder::new(&config, &DomainConfig::default()).expect("feature builder");
    let boundary = DecisionClock::new(0).boundary(as_of).expect("boundary");
    let market = windowed_market(&token);
    let window = MarketWindowSnapshot::empty(token.clone(), as_of, as_of);
    let trade_tape = TradeTapeWindowSnapshot::empty(market.market_id.clone(), as_of, as_of);
    let vector = builder
        .build(FeatureBuildInput {
            market: &market,
            boundary: &boundary,
            required_features: &[],
            pit: &engine,
            window: &window,
            trade_tape: &trade_tape,
            domain: None,
            config: &config,
            data_quality: &DataQualityConfig::default(),
        })
        .await
        .expect("build");

    assert_eq!(
        vector
            .generic
            .get(&book::BEST_BID)
            .and_then(|cell| cell.reason),
        Some(NullReason::OutOfValidRange),
        "a value outside its valid range must not be clamped to a silent value"
    );
    assert_eq!(vector.data_quality, DataQualityStatus::Insufficient);
}

#[test]
fn category_feature_projects_table_index() {
    let schema = FeatureSchema::build(&FeaturesConfig::default()).expect("schema");
    let as_of = Utc.with_ymd_and_hms(2025, 1, 1, 0, 0, 0).unwrap();
    let mut values = std::collections::BTreeMap::new();
    values.insert(
        market::CATEGORY,
        FeatureCell::observed(
            FeatureValue::Category(MarketCategory::Sports),
            Some(EvidenceSourceRef {
                source_kind: EvidenceSourceKind::GammaMetadata,
                reference: "catalog:m1:v1".to_owned(),
                effective_at: as_of,
                available_at: Some(as_of),
            }),
            FeatureStaleness::Known { age_ms: 0 },
        ),
    );
    let vector = FeatureVector {
        market_id: MarketId::new("m1"),
        token_id: Some(TokenId::new("t1")),
        decision_at: as_of,
        generic_schema_version: schema.version(),
        generic: values,
        domain: None,
        data_quality: DataQualityStatus::Fresh,
    };

    let persisted = persisted_feature(&vector);
    let rows = feature_events(
        &vector,
        &persisted,
        &DecisionClock::new(0).boundary(as_of).expect("boundary"),
        &RuntimeConfigVersionId::from_v7(),
        &schema,
        10,
    )
    .expect("feature rows");
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].feature_name, "market.category");
    assert_eq!(
        rows[0].value_kind,
        ChFeatureValueKind::Category,
        "category value_kind code"
    );
    assert_eq!(rows[0].source_kind, ChFeatureSourceKind::GammaMetadata);
    assert_eq!(rows[0].raw_value.as_deref(), Some("sports"));
}

fn parity_selected_market(market_id: &MarketId, token: &TokenId) -> SelectedMarket {
    SelectedMarket {
        market_id: market_id.clone(),
        event_id: EventId::new("evt-parity-win"),
        category: MarketCategory::Sports,
        primary_token_id: token.clone(),
        secondary_token_id: None,
        liquidity_usd: Some(Usd::new(Decimal::from(5_000))),
        volume_24h_usd: None,
        source_refs: Vec::new(),
    }
}

#[tokio::test]
async fn online_offline_parity_full_families_with_window() {
    let as_of = Utc.with_ymd_and_hms(2025, 6, 1, 12, 0, 0).unwrap();
    let token = TokenId::new("tok-parity-win");
    let market_id = MarketId::new("m-parity-win");

    let fixture = ParityFixture {
        token: token.clone(),
        market_id: market_id.clone(),
        as_of,
        book: BookSnapshotAt {
            token_id: token.clone(),
            source_cutoff: as_of,
            decision_at: as_of,
            bids: Arc::from([lvl(Decimal::new(49, 2))]),
            asks: Arc::from([lvl(Decimal::new(51, 2))]),
            timestamp_ms: u64::try_from(as_of.timestamp_millis()).unwrap_or(0),
            version: 9,
            sequence: 9,
            available_at: as_of,
        },
        market: MarketContextAt {
            market_id: market_id.clone(),
            effective_at: as_of,
            available_at: as_of,
            status: MarketStatus::Active,
            neg_risk: false,
            end_date: Some(as_of + ChronoDuration::days(3)),
            created_at: Some(as_of - ChronoDuration::days(10)),
        },
    };
    let buckets: Vec<MicrostructureBucket> = (1_i64..=5)
        .rev()
        .map(|secs| {
            micro_bucket(
                as_of - ChronoDuration::seconds(secs),
                Decimal::new(50, 2) + Decimal::new(5 - secs, 3),
                Decimal::from(20),
                Decimal::from(1_000),
            )
        })
        .collect();
    let window = MarketWindowSnapshot {
        token_id: token.clone(),
        decision_at: as_of,
        knowledge_cutoff: as_of,
        buckets,
    };
    let trade_tape = TradeTapeWindowSnapshot::empty(market_id.clone(), as_of, as_of);
    let config = FeaturesConfig::default();
    let builder =
        ConfiguredFeatureBuilder::new(&config, &DomainConfig::default()).expect("feature builder");
    let boundary = DecisionClock::new(0).boundary(as_of).expect("boundary");
    let selected = parity_selected_market(&market_id, &token);
    let dq = DataQualityConfig::default();

    let live_source = ParityLiveSource {
        fixture: fixture.clone(),
    };
    let live = builder
        .build(FeatureBuildInput {
            market: &selected,
            boundary: &boundary,
            required_features: &[],
            pit: &live_source,
            window: &window,
            trade_tape: &trade_tape,
            domain: None,
            config: &config,
            data_quality: &dq,
        })
        .await
        .expect("live build");

    let hist_engine = ParityPitEngine {
        fixture: fixture.clone(),
    };
    let historical = builder
        .build(FeatureBuildInput {
            market: &selected,
            boundary: &boundary,
            required_features: &[],
            pit: &hist_engine,
            window: &window,
            trade_tape: &trade_tape,
            domain: None,
            config: &config,
            data_quality: &dq,
        })
        .await
        .expect("historical build");

    assert_eq!(live.generic, historical.generic);
    assert_eq!(
        ResearchHasher::feature_vector(&live).expect("hash"),
        ResearchHasher::feature_vector(&historical).expect("hash"),
    );
    assert!(
        live.generic.contains_key(&FeatureName::ts_return(60)),
        "the window-backed time-series family must participate in the parity proof"
    );
}

// ── Phase 11.2.1 neg-risk sibling-leg parity ─────────────────────────────────

/// Multi-token book fixture for neg-risk full-leg parity proofs.
struct SiblingLegParityFixture {
    as_of: DateTime<Utc>,
    market_id: MarketId,
    primary_token: TokenId,
    books: HashMap<String, BookSnapshotAt>,
    market: MarketContextAt,
}

fn sibling_book(token_id: &TokenId, as_of: DateTime<Utc>, ask: Decimal) -> BookSnapshotAt {
    let bid = (ask - Decimal::new(2, 2)).max(Decimal::ZERO);
    BookSnapshotAt {
        token_id: token_id.clone(),
        source_cutoff: as_of,
        decision_at: as_of,
        bids: Arc::from([BookLevel::from_decimal_unchecked(
            Price::new(bid),
            Shares::new(Decimal::from(100)),
        )]),
        asks: Arc::from([BookLevel::from_decimal_unchecked(
            Price::new(ask),
            Shares::new(Decimal::from(100)),
        )]),
        timestamp_ms: u64::try_from(as_of.timestamp_millis()).unwrap_or(0),
        version: 1,
        sequence: 1,
        available_at: as_of,
    }
}

struct SiblingLegLiveSource<'a> {
    fixture: &'a SiblingLegParityFixture,
    sibling: &'a NegRiskLegSet,
}

#[async_trait::async_trait]
impl PointInTimeSnapshotSource for SiblingLegLiveSource<'_> {
    async fn book_at_boundary(
        &self,
        token_id: &TokenId,
        boundary: &DecisionBoundary,
    ) -> QuantResult<Option<BookSnapshotAt>> {
        let source_cutoff = boundary.cutoff_for(DecisionSource::Book);
        Ok(self
            .fixture
            .books
            .get(token_id.as_str())
            .filter(|book| {
                book.source_cutoff == source_cutoff && book.available_at <= boundary.decision_at()
            })
            .cloned())
    }

    async fn market_snapshot_at(
        &self,
        market_id: &MarketId,
        boundary: &DecisionBoundary,
    ) -> QuantResult<Option<ResolvedMarketSnapshot>> {
        if market_id != &self.fixture.market_id || boundary.decision_at() != self.fixture.as_of {
            return Ok(None);
        }
        Ok(Some(test_catalog_snapshot(
            boundary,
            market_id,
            EventId::new("evt-negrisk-parity"),
            MarketCategory::Crypto,
            self.fixture.primary_token.clone(),
            self.fixture.market.clone(),
            self.sibling.clone(),
        )))
    }
}

struct SiblingLegPitEngine<'a> {
    fixture: &'a SiblingLegParityFixture,
    sibling: &'a NegRiskLegSet,
}

#[async_trait::async_trait]
impl PointInTimeSnapshotSource for SiblingLegPitEngine<'_> {
    async fn book_at_boundary(
        &self,
        token_id: &TokenId,
        boundary: &DecisionBoundary,
    ) -> QuantResult<Option<BookSnapshotAt>> {
        let source_cutoff = boundary.cutoff_for(DecisionSource::Book);
        Ok(self
            .fixture
            .books
            .get(token_id.as_str())
            .filter(|book| {
                book.source_cutoff == source_cutoff && book.available_at <= boundary.decision_at()
            })
            .cloned())
    }

    async fn market_snapshot_at(
        &self,
        market_id: &MarketId,
        boundary: &DecisionBoundary,
    ) -> QuantResult<Option<ResolvedMarketSnapshot>> {
        if market_id != &self.fixture.market_id || boundary.decision_at() != self.fixture.as_of {
            return Ok(None);
        }
        Ok(Some(test_catalog_snapshot(
            boundary,
            market_id,
            EventId::new("evt-negrisk-parity"),
            MarketCategory::Crypto,
            self.fixture.primary_token.clone(),
            self.fixture.market.clone(),
            self.sibling.clone(),
        )))
    }
}

fn sibling_leg_parity_fixture() -> (SiblingLegParityFixture, [NegRiskLeg; 3], SelectedMarket) {
    let as_of = Utc.with_ymd_and_hms(2025, 6, 1, 12, 0, 0).unwrap();
    let primary = TokenId::new("tok-leg0");
    let leg1 = TokenId::new("tok-leg1");
    let leg2 = TokenId::new("tok-leg2");
    let market_id = MarketId::new("m-negrisk-parity");

    let mut books = HashMap::new();
    books.insert(
        primary.as_str().to_owned(),
        sibling_book(&primary, as_of, Decimal::new(35, 2)),
    );
    books.insert(
        leg1.as_str().to_owned(),
        sibling_book(&leg1, as_of, Decimal::new(33, 2)),
    );
    books.insert(
        leg2.as_str().to_owned(),
        sibling_book(&leg2, as_of, Decimal::new(34, 2)),
    );

    let fixture = SiblingLegParityFixture {
        as_of,
        market_id: market_id.clone(),
        primary_token: primary.clone(),
        books,
        market: MarketContextAt {
            market_id: market_id.clone(),
            effective_at: as_of,
            available_at: as_of,
            status: MarketStatus::Active,
            neg_risk: true,
            end_date: Some(as_of + ChronoDuration::days(7)),
            created_at: Some(as_of - ChronoDuration::days(30)),
        },
    };
    let sibling_legs = [
        NegRiskLeg {
            market_id: market_id.clone(),
            yes_token_id: primary.clone(),
        },
        NegRiskLeg {
            market_id: MarketId::new("m-leg1"),
            yes_token_id: leg1,
        },
        NegRiskLeg {
            market_id: MarketId::new("m-leg2"),
            yes_token_id: leg2,
        },
    ];
    let selected = SelectedMarket {
        market_id,
        event_id: EventId::new("evt-negrisk-parity"),
        category: MarketCategory::Crypto,
        primary_token_id: primary,
        secondary_token_id: None,
        liquidity_usd: None,
        volume_24h_usd: None,
        source_refs: Vec::new(),
    };
    (fixture, sibling_legs, selected)
}

async fn build_sibling_parity_vectors(
    fixture: &SiblingLegParityFixture,
    sibling: &NegRiskLegSet,
    selected: &SelectedMarket,
) -> (FeatureVector, FeatureVector) {
    let config = FeaturesConfig {
        enabled_feature_families: vec![
            FeatureFamily::MarketMetadata,
            FeatureFamily::PriceBook,
            FeatureFamily::Structural,
        ],
        ..FeaturesConfig::default()
    };
    let builder =
        ConfiguredFeatureBuilder::new(&config, &DomainConfig::default()).expect("feature builder");
    let boundary = DecisionClock::new(0)
        .boundary(fixture.as_of)
        .expect("boundary");
    let window = MarketWindowSnapshot::empty(
        selected.primary_token_id.clone(),
        fixture.as_of,
        fixture.as_of,
    );
    let trade_tape =
        TradeTapeWindowSnapshot::empty(selected.market_id.clone(), fixture.as_of, fixture.as_of);
    let data_quality = DataQualityConfig {
        feature_staleness_policy: FeatureStalenessPolicy::AllowDegraded,
        ..DataQualityConfig::default()
    };
    let live_source = SiblingLegLiveSource { fixture, sibling };
    let live = builder
        .build(FeatureBuildInput {
            market: selected,
            boundary: &boundary,
            required_features: &[],
            pit: &live_source,
            window: &window,
            trade_tape: &trade_tape,
            domain: None,
            config: &config,
            data_quality: &data_quality,
        })
        .await
        .expect("live build");
    let hist_engine = SiblingLegPitEngine { fixture, sibling };
    let historical = builder
        .build(FeatureBuildInput {
            market: selected,
            boundary: &boundary,
            required_features: &[],
            pit: &hist_engine,
            window: &window,
            trade_tape: &trade_tape,
            domain: None,
            config: &config,
            data_quality: &data_quality,
        })
        .await
        .expect("historical build");
    (live, historical)
}

#[tokio::test]
async fn negrisk_sibling_legs_online_offline_parity() {
    let (fixture, sibling_legs, selected) = sibling_leg_parity_fixture();
    let sibling = NegRiskLegSet {
        expected_legs: sibling_legs.len(),
        legs: sibling_legs.to_vec(),
    };
    let (live, historical) = build_sibling_parity_vectors(&fixture, &sibling, &selected).await;

    assert_eq!(live.generic, historical.generic);
    assert_eq!(
        ResearchHasher::feature_vector(&live).expect("hash"),
        ResearchHasher::feature_vector(&historical).expect("hash"),
    );
    let ask_sum = live
        .generic
        .get(&NEGRISK_LEG_ASK_SUM)
        .expect("leg ask sum present");
    assert_eq!(
        ask_sum
            .value()
            .expect("observed ask sum")
            .to_fact_decimal()
            .expect("decimal projection"),
        Decimal::new(102, 2),
        "three-leg ask sum must be 1.02 (0.35 + 0.33 + 0.34)"
    );
}

#[tokio::test]
async fn negrisk_missing_catalog_leg_fails_closed() {
    let (fixture, sibling_legs, selected) = sibling_leg_parity_fixture();
    // Catalog expects one more neg-risk leg than we enumerate — structural features must
    // fail closed with LegBookMissing, never a partial aggregate.
    let sibling = NegRiskLegSet {
        expected_legs: sibling_legs.len() + 1,
        legs: sibling_legs.to_vec(),
    };
    let (live, _) = build_sibling_parity_vectors(&fixture, &sibling, &selected).await;
    let value = live
        .generic
        .get(&NEGRISK_LEG_ASK_SUM)
        .expect("neg-risk leg ask sum in schema");
    assert_eq!(
        value.reason,
        Some(NullReason::LegBookMissing),
        "missing catalog leg must surface LegBookMissing",
    );
}

#[test]
fn negrisk_from_catalog_excludes_non_neg_risk_members() {
    use std::sync::Arc;

    use chrono::Utc;
    use quant_pivot_models::{
        domain::market::{
            MarketInfo,
            registry::{CatalogMarketLeg, NegRiskLegSet},
        },
        enums::{
            common::{MarketCategory, TickSize},
            market::MarketStatus,
        },
        types::{EventId, MarketId, TokenId},
    };

    let now = Utc::now();
    let catalog = |id: &str, neg_risk: bool| {
        Arc::new(MarketInfo {
            market_id: MarketId::new(id),
            event_id: EventId::new("evt-catalog"),
            question: "Q?".into(),
            slug: id.into(),
            description: None,
            categories: vec![MarketCategory::Crypto],
            status: MarketStatus::Active,
            outcome: None,
            yes_token_id: TokenId::new(format!("{id}-yes")),
            no_token_id: TokenId::new(format!("{id}-no")),
            tick_size: TickSize::Hundredth,
            neg_risk,
            end_date: None,
            resolved_at: None,
            fees_enabled: false,
            fee_rate: None,
            fee_exponent: None,
            fee_taker_only: None,
            fee_rebate_rate: None,
            fee_source: None,
            fee_observed_at: None,
            created_at: now,
            updated_at: now,
        })
    };

    let leg_a = catalog("leg-a", true);
    let leg_b = catalog("leg-b", true);
    let binary = catalog("binary", false);
    let by_id = [
        (leg_a.market_id.clone(), leg_a),
        (leg_b.market_id.clone(), leg_b),
        (binary.market_id.clone(), binary),
    ]
    .into_iter()
    .collect::<std::collections::HashMap<_, _>>();
    let market_ids = [
        MarketId::new("leg-a"),
        MarketId::new("leg-b"),
        MarketId::new("binary"),
    ];
    let set = NegRiskLegSet::from_catalog(&market_ids, |market_id| {
        by_id.get(market_id).map(|info| {
            if info.neg_risk {
                CatalogMarketLeg::NegRisk {
                    yes_token_id: info.yes_token_id.clone(),
                }
            } else {
                CatalogMarketLeg::NonNegRisk
            }
        })
    });
    assert_eq!(
        set.expected_legs, 2,
        "non-neg-risk catalog members must not inflate expected_legs"
    );
    assert_eq!(set.legs.len(), 2);
}

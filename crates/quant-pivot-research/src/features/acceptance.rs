//! Phase 03.2 §8 acceptance tests for the feature plane.

use std::collections::HashMap;
use std::sync::LazyLock;
use std::sync::atomic::AtomicUsize;
use std::sync::{Arc, atomic::Ordering};
use std::time::Duration;

use chrono::{DateTime, Duration as ChronoDuration, TimeZone, Utc};
use quant_pivot_error::QuantResult;
use quant_pivot_models::runtime_config::SelectionConfig;
use quant_pivot_models::{
    domain::{
        BookSnapshot, MarketCandidate, MarketRegistryInfo, PointInTimeDataSource, TokenInfo,
        market::{
            book::BookLevel,
            registry::{NegRiskLeg, NegRiskLegSet},
        },
    },
    enums::{
        clickhouse::{ChFeatureSourceKind, ChFeatureValueKind},
        common::{CategorySet, MarketCategory, TickSize},
        market::MarketStatus,
        quant::DataQualityStatus,
    },
    runtime_config::{DataQualityConfig, FeatureFamily, FeatureStalenessPolicy, FeaturesConfig},
    types::{Bps, EventId, MarketId, Price, Probability, SchemaVersion, Shares, TokenId, Usd},
};
use rust_decimal::Decimal;

use crate::{
    features::{
        FeatureBuildInput, FeatureBuilder, MarketWindowSnapshot, MicrostructureBucket, PitView,
        availability::FeatureAvailabilityOracle,
        builder::ConfiguredFeatureBuilder,
        names::{book, market},
        null_policy::{NullDecision, NullPolicyEngine},
        schema::FeatureSchema,
        value::{FeatureName, FeatureValue, FeatureVector, NullReason},
        writer::feature_events,
    },
    hashing::ResearchHasher,
    pit::{BookSnapshotAt, MarketContextAt, PitQueryEngine},
    selection::{
        FilterChain, FilterOutcome, MarketCandidateCtx, ModelFeatureRequirements, SelectedMarket,
        SelectionThresholds,
    },
};

static EMPTY_NEGRISK_LEGS: LazyLock<NegRiskLegSet> = LazyLock::new(NegRiskLegSet::empty);

// ── Null policy ────────────────────────────────────────────────────────────

#[test]
fn feature_null_policy_rejects_required_missing() {
    let spec = FeatureSchema::build(&FeaturesConfig::default())
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
        NullDecision::Substitute { value } => {
            assert!(
                value.is_missing(),
                "penalize path must not substitute silent zero"
            );
        }
        NullDecision::Reject(_) => {}
    }

    let missing_value = FeatureValue::Missing(NullReason::SourceUnavailable);
    assert!(missing_value.to_fact_decimal().is_none());
    assert_ne!(
        missing_value,
        FeatureValue::Decimal(Decimal::ZERO),
        "missing must never equal zero"
    );
}

// ── PIT visibility ─────────────────────────────────────────────────────────

struct MemoryPitEngine {
    books: Vec<BookSnapshotAt>,
}

#[async_trait::async_trait]
impl PitQueryEngine for MemoryPitEngine {
    async fn book_at(
        &self,
        token_id: &TokenId,
        as_of: DateTime<Utc>,
    ) -> QuantResult<Option<BookSnapshotAt>> {
        // PIT cutoff is `timestamp_ms` (the single observed-time source): never
        // return a book published after `as_of`.
        let cutoff = as_of.timestamp_millis();
        Ok(self
            .books
            .iter()
            .filter(|book| {
                &book.token_id == token_id
                    && i64::try_from(book.timestamp_ms).unwrap_or(i64::MAX) <= cutoff
            })
            .max_by_key(|book| book.timestamp_ms)
            .cloned())
    }

    async fn market_at(
        &self,
        _market_id: &MarketId,
        _as_of: DateTime<Utc>,
    ) -> QuantResult<Option<MarketContextAt>> {
        Ok(None)
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
                as_of,
                bids: Arc::from([level(Decimal::new(45, 2))]),
                asks: Arc::from([level(Decimal::new(55, 2))]),
                timestamp_ms: u64::try_from(past.timestamp_millis()).unwrap_or(0),
                version: 1,
            },
            BookSnapshotAt {
                token_id: token.clone(),
                as_of,
                bids: Arc::from([level(Decimal::new(99, 2))]),
                asks: Arc::from([level(Decimal::new(99, 2))]),
                timestamp_ms: u64::try_from(future.timestamp_millis()).unwrap_or(0),
                version: 2,
            },
        ],
    };

    let pit = PitView::Historical(&engine);
    let resolved = pit
        .resolve_book(&token, as_of)
        .await
        .expect("resolve")
        .expect("book");
    assert_eq!(resolved.version, 1);
    assert_eq!(
        resolved.best_bid().expect("bid").inner(),
        Decimal::new(45, 2)
    );
}

// ── Hash stability ─────────────────────────────────────────────────────────

fn sample_vector() -> FeatureVector {
    let mut values = std::collections::BTreeMap::new();
    values.insert(
        book::MID,
        FeatureValue::Probability(Probability::new(Decimal::new(50, 2))),
    );
    FeatureVector {
        market_id: MarketId::new("m1"),
        token_id: Some(TokenId::new("t1")),
        as_of: Utc.with_ymd_and_hms(2025, 1, 1, 0, 0, 0).unwrap(),
        schema_version: SchemaVersion::FIRST,
        values,
        substitutions: Vec::new(),
        data_quality: DataQualityStatus::Fresh,
        staleness_ms: 100,
        source_refs: Vec::new(),
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

    let schema_v1 = FeatureSchema::build(&config_v1);
    let schema_v2 = FeatureSchema::build(&config_v2);
    assert_ne!(
        ResearchHasher::feature_schema(&schema_v1).expect("hash"),
        ResearchHasher::feature_schema(&schema_v2).expect("hash"),
    );
}

// ── Domain missing ─────────────────────────────────────────────────────────

struct EmptyPit;

impl PointInTimeDataSource for EmptyPit {
    fn book_snapshot(
        &self,
        _token_id: &TokenId,
        _as_of: DateTime<Utc>,
    ) -> Option<Arc<BookSnapshot>> {
        None
    }

    fn market_context(
        &self,
        _market_id: &MarketId,
        _as_of: DateTime<Utc>,
    ) -> Option<Arc<MarketRegistryInfo>> {
        None
    }

    fn neg_risk_leg_set(&self, _event_id: &EventId) -> NegRiskLegSet {
        NegRiskLegSet::empty()
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
    let builder = ConfiguredFeatureBuilder::new(&config);
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
    let pit = PitView::Live(&EmptyPit);
    let window =
        MarketWindowSnapshot::empty(market.primary_token_id.clone(), Utc::now(), Duration::ZERO);
    let vector = builder
        .build(FeatureBuildInput {
            market: &market,
            as_of: Utc::now(),
            source_delay: Duration::ZERO,
            required_features: &[],
            pit,
            window: &window,
            sibling: &EMPTY_NEGRISK_LEGS,
            config: &config,
            data_quality: &DataQualityConfig::default(),
        })
        .await
        .expect("build");

    let value = vector
        .values
        .get(&crate::features::names::structural::NEGRISK_LEG_ASK_SUM)
        .expect("structural neg-risk feature present in schema");
    assert_eq!(
        value.null_reason(),
        Some(NullReason::NotApplicable),
        "a binary market's neg-risk aggregate must be NotApplicable",
    );
}

// ── Family gating (feature inputs vs decision capture) ─────────────────────

/// A PIT source that counts how many book / market lookups the builder issues.
struct CountingPit {
    book_calls: AtomicUsize,
    market_calls: AtomicUsize,
}

impl PointInTimeDataSource for CountingPit {
    fn book_snapshot(
        &self,
        _token_id: &TokenId,
        _as_of: DateTime<Utc>,
    ) -> Option<Arc<BookSnapshot>> {
        self.book_calls.fetch_add(1, Ordering::Relaxed);
        None
    }

    fn market_context(
        &self,
        _market_id: &MarketId,
        _as_of: DateTime<Utc>,
    ) -> Option<Arc<MarketRegistryInfo>> {
        self.market_calls.fetch_add(1, Ordering::Relaxed);
        None
    }

    fn neg_risk_leg_set(&self, _event_id: &EventId) -> NegRiskLegSet {
        NegRiskLegSet::empty()
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
    let builder = ConfiguredFeatureBuilder::new(&config);
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
    let window =
        MarketWindowSnapshot::empty(market.primary_token_id.clone(), Utc::now(), Duration::ZERO);
    builder
        .build(FeatureBuildInput {
            market: &market,
            as_of: Utc::now(),
            source_delay: Duration::ZERO,
            required_features: &[],
            pit: PitView::Live(&source),
            window: &window,
            sibling: &EMPTY_NEGRISK_LEGS,
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
        2,
        "decision capture resolves market context plus registry metadata"
    );
}

// ── CH writer (pure projection) ────────────────────────────────────────────

#[test]
fn feature_event_writer_batches_present_only() {
    let schema = FeatureSchema::build(&FeaturesConfig::default());
    let vector = sample_vector();
    let rows = feature_events(&vector, &schema, 1_000);
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].feature_name, "book.mid");
    assert_eq!(rows[0].value_kind, ChFeatureValueKind::Probability);
    assert_eq!(rows[0].source_kind, ChFeatureSourceKind::Book);
    assert_eq!(rows[0].ingestion_time, 1_000);

    let mut missing_only = sample_vector();
    missing_only.values.insert(
        book::SPREAD_BPS,
        FeatureValue::Missing(NullReason::SourceUnavailable),
    );
    let rows = feature_events(&missing_only, &schema, 1_000);
    assert_eq!(rows.len(), 1, "missing values must not emit fact rows");
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
        connection_healthy: true,
        ingest_lag_ms: 0,
        crossed: false,
        empty: false,
        observed_at: Utc::now(),
    }
}

#[test]
fn model_eligibility_uses_real_availability_oracle() {
    let schema = FeatureSchema::build(&FeaturesConfig::default());
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
        as_of: Utc::now(),
        model_requirements: &ModelFeatureRequirements {
            required_features: book_required,
        },
        feature_schema: &schema,
    };
    assert!(matches!(
        FilterChain::standard().evaluate(&ctx),
        FilterOutcome::Keep
    ));

    let ctx = MarketCandidateCtx {
        model_requirements: &ModelFeatureRequirements {
            required_features: unknown_required,
        },
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

#[async_trait::async_trait]
impl PitQueryEngine for ParityPitEngine {
    async fn book_at(
        &self,
        token_id: &TokenId,
        as_of: DateTime<Utc>,
    ) -> QuantResult<Option<BookSnapshotAt>> {
        if token_id == &self.fixture.token && as_of == self.fixture.as_of {
            Ok(Some(self.fixture.book.clone()))
        } else {
            Ok(None)
        }
    }

    async fn market_at(
        &self,
        market_id: &MarketId,
        as_of: DateTime<Utc>,
    ) -> QuantResult<Option<MarketContextAt>> {
        if market_id == &self.fixture.market_id && as_of == self.fixture.as_of {
            Ok(Some(self.fixture.market.clone()))
        } else {
            Ok(None)
        }
    }
}

struct ParityLiveSource {
    fixture: ParityFixture,
}

impl PointInTimeDataSource for ParityLiveSource {
    fn book_snapshot(
        &self,
        token_id: &TokenId,
        _as_of: DateTime<Utc>,
    ) -> Option<Arc<BookSnapshot>> {
        if token_id != &self.fixture.token {
            return None;
        }
        Some(Arc::new(BookSnapshot::new(
            Arc::clone(&self.fixture.book.bids),
            Arc::clone(&self.fixture.book.asks),
            self.fixture.book.timestamp_ms,
            self.fixture.book.version,
        )))
    }

    fn market_context(
        &self,
        market_id: &MarketId,
        _as_of: DateTime<Utc>,
    ) -> Option<Arc<MarketRegistryInfo>> {
        if market_id != &self.fixture.market_id {
            return None;
        }
        Some(Arc::new(MarketRegistryInfo {
            market_id: self.fixture.market_id.clone(),
            event_id: EventId::new("evt-parity"),
            token_yes: self.fixture.token.clone(),
            token_no: TokenId::new("no"),
            question: "parity?".into(),
            slug: "parity".into(),
            categories: CategorySet::from(MarketCategory::Sports),
            status: self.fixture.market.status,
            outcome: None,
            neg_risk: self.fixture.market.neg_risk,
            tick_size: TickSize::Hundredth,
            tokens: vec![
                TokenInfo {
                    token_id: self.fixture.token.clone(),
                    outcome: "Yes".into(),
                    neg_risk: false,
                },
                TokenInfo {
                    token_id: TokenId::new("no"),
                    outcome: "No".into(),
                    neg_risk: false,
                },
            ],
            best_bid: None,
            best_ask: None,
            depth_usd: None,
            min_order_size: Decimal::ONE,
            liquidity_usd: Some(Usd::new(Decimal::from(5_000))),
            volume_24h: None,
            fee_schedule: None,
            end_date: self.fixture.market.end_date,
            resolved_at: None,
            created_at: self.fixture.market.created_at,
            updated_at: self.fixture.market.observed_at,
        }))
    }

    fn neg_risk_leg_set(&self, _event_id: &EventId) -> NegRiskLegSet {
        NegRiskLegSet::empty()
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
            as_of,
            bids: Arc::from([level]),
            asks: Arc::from([BookLevel::from_decimal_unchecked(
                Price::new(Decimal::new(51, 2)),
                Shares::new(Decimal::from(50)),
            )]),
            timestamp_ms: u64::try_from(as_of.timestamp_millis()).unwrap_or(0),
            version: 7,
        },
        market: MarketContextAt {
            market_id: market_id.clone(),
            as_of,
            observed_at: as_of,
            status: MarketStatus::Active,
            neg_risk: false,
            end_date: Some(as_of + ChronoDuration::days(3)),
            created_at: as_of - ChronoDuration::days(10),
            outcome_count: 2,
        },
    };

    let config = FeaturesConfig {
        enabled_feature_families: vec![FeatureFamily::MarketMetadata, FeatureFamily::PriceBook],
        ..FeaturesConfig::default()
    };
    let builder = ConfiguredFeatureBuilder::new(&config);
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
    let window = MarketWindowSnapshot::empty(token.clone(), as_of, Duration::ZERO);
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
            as_of,
            source_delay: Duration::ZERO,
            required_features: &[],
            pit: PitView::Live(&live_source),
            window: &window,
            sibling: &EMPTY_NEGRISK_LEGS,
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
            as_of,
            source_delay: Duration::ZERO,
            required_features: &[],
            pit: PitView::Historical(&hist_engine),
            window: &window,
            sibling: &EMPTY_NEGRISK_LEGS,
            config: &config,
            data_quality: &data_quality,
        })
        .await
        .expect("historical build");

    assert_eq!(live.values, historical.values);
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
        as_of,
        source_delay: Duration::ZERO,
        buckets,
    };
    let config = FeaturesConfig {
        enabled_feature_families: vec![FeatureFamily::TimeSeries],
        ..FeaturesConfig::default()
    };
    let builder = ConfiguredFeatureBuilder::new(&config);
    let market = windowed_market(&token);
    let vector = builder
        .build(FeatureBuildInput {
            market: &market,
            as_of,
            source_delay: Duration::ZERO,
            required_features: &[],
            pit: PitView::Live(&EmptyPit),
            window: &window,
            sibling: &EMPTY_NEGRISK_LEGS,
            config: &config,
            data_quality: &DataQualityConfig::default(),
        })
        .await
        .expect("build");

    let ret = vector
        .values
        .get(&FeatureName::ts_return(60))
        .expect("return present");
    assert!(
        !ret.is_missing(),
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
            as_of,
            bids: Arc::from([lvl(Decimal::new(48, 2))]),
            asks: Arc::from([lvl(Decimal::new(52, 2))]),
            timestamp_ms: u64::try_from(stale_ts.timestamp_millis()).unwrap_or(0),
            version: 1,
        }],
    };
    let config = FeaturesConfig {
        enabled_feature_families: vec![FeatureFamily::PriceBook],
        ..FeaturesConfig::default()
    };
    let builder = ConfiguredFeatureBuilder::new(&config);
    let market = windowed_market(&token);
    let window = MarketWindowSnapshot::empty(token.clone(), as_of, Duration::ZERO);
    let vector = builder
        .build(FeatureBuildInput {
            market: &market,
            as_of,
            source_delay: Duration::ZERO,
            required_features: &[],
            pit: PitView::Historical(&engine),
            window: &window,
            sibling: &EMPTY_NEGRISK_LEGS,
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
            .values
            .get(&book::BEST_BID)
            .and_then(FeatureValue::null_reason),
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
        as_of,
        source_delay: Duration::ZERO,
        buckets,
    };
    let config = FeaturesConfig {
        enabled_feature_families: vec![FeatureFamily::TimeSeries],
        ..FeaturesConfig::default()
    };
    let builder = ConfiguredFeatureBuilder::new(&config);
    let market = windowed_market(&token);
    let required = vec![FeatureName::ts_return(60)];

    // Default policy rejects a stale *required* feature ⇒ Insufficient.
    let strict = builder
        .build(FeatureBuildInput {
            market: &market,
            as_of,
            source_delay: Duration::ZERO,
            required_features: &required,
            pit: PitView::Live(&EmptyPit),
            window: &window,
            sibling: &EMPTY_NEGRISK_LEGS,
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
            as_of,
            source_delay: Duration::ZERO,
            required_features: &required,
            pit: PitView::Live(&EmptyPit),
            window: &window,
            sibling: &EMPTY_NEGRISK_LEGS,
            config: &config,
            data_quality: &lenient,
        })
        .await
        .expect("lenient build");
    assert_ne!(degraded.data_quality, DataQualityStatus::Insufficient);
    assert_eq!(
        degraded
            .values
            .get(&FeatureName::ts_return(60))
            .and_then(FeatureValue::null_reason),
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
            as_of,
            bids: Arc::from([lvl(Decimal::new(150, 2))]),
            asks: Arc::from([lvl(Decimal::new(160, 2))]),
            timestamp_ms: u64::try_from(as_of.timestamp_millis()).unwrap_or(0),
            version: 1,
        }],
    };
    let config = FeaturesConfig {
        enabled_feature_families: vec![FeatureFamily::PriceBook],
        ..FeaturesConfig::default()
    };
    let builder = ConfiguredFeatureBuilder::new(&config);
    let market = windowed_market(&token);
    let window = MarketWindowSnapshot::empty(token.clone(), as_of, Duration::ZERO);
    let vector = builder
        .build(FeatureBuildInput {
            market: &market,
            as_of,
            source_delay: Duration::ZERO,
            required_features: &[],
            pit: PitView::Historical(&engine),
            window: &window,
            sibling: &EMPTY_NEGRISK_LEGS,
            config: &config,
            data_quality: &DataQualityConfig::default(),
        })
        .await
        .expect("build");

    assert_eq!(
        vector
            .values
            .get(&book::BEST_BID)
            .and_then(FeatureValue::null_reason),
        Some(NullReason::OutOfValidRange),
        "a value outside its valid range must not be clamped to a silent value"
    );
    assert_eq!(vector.data_quality, DataQualityStatus::Insufficient);
}

#[test]
fn category_feature_projects_table_index() {
    let schema = FeatureSchema::build(&FeaturesConfig::default());
    let mut values = std::collections::BTreeMap::new();
    values.insert(
        market::CATEGORY,
        FeatureValue::Category(MarketCategory::Sports),
    );
    let vector = FeatureVector {
        market_id: MarketId::new("m1"),
        token_id: Some(TokenId::new("t1")),
        as_of: Utc.with_ymd_and_hms(2025, 1, 1, 0, 0, 0).unwrap(),
        schema_version: SchemaVersion::FIRST,
        values,
        substitutions: Vec::new(),
        data_quality: DataQualityStatus::Fresh,
        staleness_ms: 0,
        source_refs: Vec::new(),
    };

    let rows = feature_events(&vector, &schema, 10);
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].feature_name, "market.category");
    assert_eq!(
        rows[0].value_kind,
        ChFeatureValueKind::Category,
        "category value_kind code"
    );
    assert_eq!(rows[0].source_kind, ChFeatureSourceKind::GammaMetadata);
    // Sports' stable `table_index` is 1.
    assert_eq!(
        rows[0].feature_value.to_decimal(),
        Decimal::ONE,
        "category projects its stable table_index"
    );
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
            as_of,
            bids: Arc::from([lvl(Decimal::new(49, 2))]),
            asks: Arc::from([lvl(Decimal::new(51, 2))]),
            timestamp_ms: u64::try_from(as_of.timestamp_millis()).unwrap_or(0),
            version: 9,
        },
        market: MarketContextAt {
            market_id: market_id.clone(),
            as_of,
            observed_at: as_of,
            status: MarketStatus::Active,
            neg_risk: false,
            end_date: Some(as_of + ChronoDuration::days(3)),
            created_at: as_of - ChronoDuration::days(10),
            outcome_count: 2,
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
        as_of,
        source_delay: Duration::ZERO,
        buckets,
    };
    let config = FeaturesConfig::default();
    let builder = ConfiguredFeatureBuilder::new(&config);
    let selected = SelectedMarket {
        market_id: market_id.clone(),
        event_id: EventId::new("evt-parity-win"),
        category: MarketCategory::Sports,
        primary_token_id: token.clone(),
        secondary_token_id: None,
        liquidity_usd: Some(Usd::new(Decimal::from(5_000))),
        volume_24h_usd: None,
        source_refs: Vec::new(),
    };
    let dq = DataQualityConfig::default();

    let live_source = ParityLiveSource {
        fixture: fixture.clone(),
    };
    let live = builder
        .build(FeatureBuildInput {
            market: &selected,
            as_of,
            source_delay: Duration::ZERO,
            required_features: &[],
            pit: PitView::Live(&live_source),
            window: &window,
            sibling: &EMPTY_NEGRISK_LEGS,
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
            as_of,
            source_delay: Duration::ZERO,
            required_features: &[],
            pit: PitView::Historical(&hist_engine),
            window: &window,
            sibling: &EMPTY_NEGRISK_LEGS,
            config: &config,
            data_quality: &dq,
        })
        .await
        .expect("historical build");

    assert_eq!(live.values, historical.values);
    assert_eq!(
        ResearchHasher::feature_vector(&live).expect("hash"),
        ResearchHasher::feature_vector(&historical).expect("hash"),
    );
    assert!(
        live.values.contains_key(&FeatureName::ts_return(60)),
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
        as_of,
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
    }
}

struct SiblingLegLiveSource<'a> {
    fixture: &'a SiblingLegParityFixture,
}

impl PointInTimeDataSource for SiblingLegLiveSource<'_> {
    fn book_snapshot(
        &self,
        token_id: &TokenId,
        _as_of: DateTime<Utc>,
    ) -> Option<Arc<BookSnapshot>> {
        let book = self.fixture.books.get(token_id.as_str())?;
        Some(Arc::new(BookSnapshot::new(
            Arc::clone(&book.bids),
            Arc::clone(&book.asks),
            book.timestamp_ms,
            book.version,
        )))
    }

    fn market_context(
        &self,
        market_id: &MarketId,
        _as_of: DateTime<Utc>,
    ) -> Option<Arc<MarketRegistryInfo>> {
        if market_id != &self.fixture.market_id {
            return None;
        }
        Some(Arc::new(MarketRegistryInfo {
            market_id: self.fixture.market_id.clone(),
            event_id: EventId::new("evt-negrisk-parity"),
            token_yes: self.fixture.primary_token.clone(),
            token_no: TokenId::new("no-negrisk"),
            question: "negrisk parity?".into(),
            slug: "negrisk-parity".into(),
            categories: CategorySet::from(MarketCategory::Crypto),
            status: self.fixture.market.status,
            outcome: None,
            neg_risk: true,
            tick_size: TickSize::Hundredth,
            tokens: vec![
                TokenInfo {
                    token_id: self.fixture.primary_token.clone(),
                    outcome: "Yes A".into(),
                    neg_risk: true,
                },
                TokenInfo {
                    token_id: TokenId::new("tok-leg1"),
                    outcome: "Yes B".into(),
                    neg_risk: true,
                },
                TokenInfo {
                    token_id: TokenId::new("tok-leg2"),
                    outcome: "Yes C".into(),
                    neg_risk: true,
                },
            ],
            best_bid: None,
            best_ask: None,
            depth_usd: None,
            min_order_size: Decimal::ONE,
            liquidity_usd: None,
            volume_24h: None,
            fee_schedule: None,
            end_date: self.fixture.market.end_date,
            resolved_at: None,
            created_at: self.fixture.market.created_at,
            updated_at: self.fixture.market.observed_at,
        }))
    }

    fn neg_risk_leg_set(&self, _event_id: &EventId) -> NegRiskLegSet {
        NegRiskLegSet::empty()
    }
}

struct SiblingLegPitEngine<'a> {
    fixture: &'a SiblingLegParityFixture,
}

#[async_trait::async_trait]
impl PitQueryEngine for SiblingLegPitEngine<'_> {
    async fn book_at(
        &self,
        token_id: &TokenId,
        as_of: DateTime<Utc>,
    ) -> QuantResult<Option<BookSnapshotAt>> {
        Ok(self
            .fixture
            .books
            .get(token_id.as_str())
            .filter(|book| book.as_of == as_of)
            .cloned())
    }

    async fn market_at(
        &self,
        market_id: &MarketId,
        as_of: DateTime<Utc>,
    ) -> QuantResult<Option<MarketContextAt>> {
        if market_id == &self.fixture.market_id && as_of == self.fixture.as_of {
            Ok(Some(self.fixture.market.clone()))
        } else {
            Ok(None)
        }
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
            as_of,
            observed_at: as_of,
            status: MarketStatus::Active,
            neg_risk: true,
            end_date: Some(as_of + ChronoDuration::days(7)),
            created_at: as_of - ChronoDuration::days(30),
            outcome_count: 3,
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
    let builder = ConfiguredFeatureBuilder::new(&config);
    let window = MarketWindowSnapshot::empty(
        selected.primary_token_id.clone(),
        fixture.as_of,
        Duration::ZERO,
    );
    let data_quality = DataQualityConfig {
        feature_staleness_policy: FeatureStalenessPolicy::AllowDegraded,
        ..DataQualityConfig::default()
    };
    let live_source = SiblingLegLiveSource { fixture };
    let live = builder
        .build(FeatureBuildInput {
            market: selected,
            as_of: fixture.as_of,
            source_delay: Duration::ZERO,
            required_features: &[],
            pit: PitView::Live(&live_source),
            window: &window,
            sibling,
            config: &config,
            data_quality: &data_quality,
        })
        .await
        .expect("live build");
    let hist_engine = SiblingLegPitEngine { fixture };
    let historical = builder
        .build(FeatureBuildInput {
            market: selected,
            as_of: fixture.as_of,
            source_delay: Duration::ZERO,
            required_features: &[],
            pit: PitView::Historical(&hist_engine),
            window: &window,
            sibling,
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

    assert_eq!(live.values, historical.values);
    assert_eq!(
        ResearchHasher::feature_vector(&live).expect("hash"),
        ResearchHasher::feature_vector(&historical).expect("hash"),
    );
    let ask_sum = live
        .values
        .get(&crate::features::names::structural::NEGRISK_LEG_ASK_SUM)
        .expect("leg ask sum present");
    assert_eq!(
        ask_sum.to_fact_decimal(),
        Some(Decimal::new(102, 2)),
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
        .values
        .get(&crate::features::names::structural::NEGRISK_LEG_ASK_SUM)
        .expect("neg-risk leg ask sum in schema");
    assert_eq!(
        value.null_reason(),
        Some(NullReason::LegBookMissing),
        "missing catalog leg must surface LegBookMissing",
    );
}

#[test]
fn negrisk_from_event_catalog_excludes_non_neg_risk_rows() {
    use std::sync::Arc;

    use chrono::Utc;
    use quant_pivot_models::{
        domain::market::MarketInfo,
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

    let set = NegRiskLegSet::from_event_catalog(&[
        catalog("leg-a", true),
        catalog("leg-b", true),
        catalog("binary", false),
    ]);
    assert_eq!(
        set.expected_legs, 2,
        "non-neg-risk PG rows must not inflate expected_legs"
    );
    assert_eq!(set.legs.len(), 2);
}

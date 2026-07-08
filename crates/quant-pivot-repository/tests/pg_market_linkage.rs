//! Market-linkage ledger bitemporal PIT integration tests (Postgres +
//! testcontainers). Validates the real SQL behind the 11.2.2 remediation R5
//! fix: `valid_at` / `valid_at_for_markets` must never see a revision derived
//! after the queried `as_of`, while `latest_for_markets` (resolver idempotence
//! only) intentionally ignores `as_of` — none of which a mock repository can
//! prove.

use chrono::{Duration as ChronoDuration, TimeZone, Utc};
use quant_pivot_models::{
    domain::{LinkageOutcome, MarketLinkage, UpsertEvent, UpsertMarket},
    enums::{
        common::{CategorySet, MarketCategory, TickSize},
        domain::{DomainFamily, ResolverTier},
        market::{EventStatus, MarketStatus},
    },
    types::{ContentHash, EventId, MarketId, MarketLinkageId, ResolverVersion, TokenId},
};
use quant_pivot_repository::{
    postgres::{PgEventRepository, PgMarketLinkageRepository, PgMarketRepository},
    traits::{EventRepository, MarketLinkageRepository, MarketRepository},
};
use quant_pivot_test_support::pg::setup_pg;

async fn seed_market(db: &sea_orm::DatabaseConnection, market_id: &str) {
    let events = PgEventRepository::new(db.clone());
    events
        .upsert(UpsertEvent {
            event_id: EventId::new("evt-linkage-pit"),
            title: "Bitcoin up or down".to_owned(),
            slug: "btc-updown".to_owned(),
            series_slug: None,
            status: EventStatus::Active,
            tags: vec!["crypto".to_owned()].into(),
            neg_risk: false,
            catalog_market_ids: Vec::new().into(),
            end_date: None,
            raw_gamma: None,
        })
        .await
        .expect("seed event");

    let markets = PgMarketRepository::new(db.clone());
    markets
        .upsert(UpsertMarket {
            market_id: MarketId::new(market_id),
            event_id: EventId::new("evt-linkage-pit"),
            question: "Will BTC be up?".to_owned(),
            slug: "btc-updown-5m-1".to_owned(),
            description: None,
            categories: CategorySet::from(MarketCategory::Crypto),
            status: MarketStatus::Active,
            outcome: None,
            yes_token_id: TokenId::new("111"),
            no_token_id: TokenId::new("222"),
            tick_size: TickSize::Hundredth,
            neg_risk: false,
            end_date: None,
            resolved_at: None,
            fees_enabled: true,
            fee_rate: None,
            fee_exponent: None,
            fee_taker_only: None,
            fee_rebate_rate: None,
            fee_source: None,
            fee_observed_at: None,
        })
        .await
        .expect("seed market");
}

fn linkage(
    market_id: &str,
    outcome: LinkageOutcome,
    derived_at: chrono::DateTime<Utc>,
    seed: u8,
) -> MarketLinkage {
    let market_id = MarketId::new(market_id);
    let metadata_hash =
        ContentHash::parse(format!("blake3:{}", format!("{seed:02x}").repeat(32))).expect("hash");
    let content_hash = MarketLinkage::compute_content_hash(
        &market_id,
        DomainFamily::Crypto,
        &outcome,
        ResolverTier::Tier0Slug,
        ResolverVersion::FIRST,
        &metadata_hash,
    )
    .expect("content hash");
    MarketLinkage {
        linkage_id: MarketLinkageId::from_v7(),
        market_id,
        domain_family: DomainFamily::Crypto,
        outcome,
        confidence: quant_pivot_models::types::Probability::ONE,
        resolver_tier: ResolverTier::Tier0Slug,
        resolver_version: ResolverVersion::FIRST,
        metadata_hash,
        content_hash,
        derived_at,
        created_at: derived_at,
    }
}

#[tokio::test]
#[ignore = "requires Docker"]
async fn valid_at_never_sees_a_revision_derived_after_as_of() {
    let (pool, _container) = setup_pg().await;
    let db = pool.connection().clone();
    seed_market(&db, "0xlinkage1").await;

    let repo = PgMarketLinkageRepository::new(db.clone());
    let early_at = Utc.with_ymd_and_hms(2026, 7, 1, 11, 0, 0).unwrap();
    let late_at = Utc.with_ymd_and_hms(2026, 7, 1, 11, 30, 0).unwrap();

    repo.append(
        linkage(
            "0xlinkage1",
            LinkageOutcome::Unresolved {
                reason: "no template matched".to_owned(),
            },
            early_at,
            1,
        )
        .to_new()
        .expect("new"),
    )
    .await
    .expect("append early");
    repo.append(
        linkage(
            "0xlinkage1",
            LinkageOutcome::Unresolved {
                reason: "metadata revised, still unresolved".to_owned(),
            },
            late_at,
            2,
        )
        .to_new()
        .expect("new"),
    )
    .await
    .expect("append late");

    // Before the late revision: only the early row is PIT-visible.
    let mid = early_at + ChronoDuration::minutes(10);
    let valid = repo
        .valid_at(&MarketId::new("0xlinkage1"), mid)
        .await
        .expect("valid_at")
        .expect("some row");
    assert_eq!(
        valid.derived_at, early_at,
        "must not see the future revision"
    );

    // After the late revision: the late row is visible.
    let after = late_at + ChronoDuration::minutes(1);
    let valid = repo
        .valid_at(&MarketId::new("0xlinkage1"), after)
        .await
        .expect("valid_at")
        .expect("some row");
    assert_eq!(valid.derived_at, late_at);

    // Before either row was derived: no PIT-valid record at all.
    let before = early_at - ChronoDuration::hours(1);
    assert!(
        repo.valid_at(&MarketId::new("0xlinkage1"), before)
            .await
            .expect("valid_at")
            .is_none()
    );
}

#[tokio::test]
#[ignore = "requires Docker"]
async fn valid_at_for_markets_matches_valid_at_batched() {
    let (pool, _container) = setup_pg().await;
    let db = pool.connection().clone();
    seed_market(&db, "0xlinkage2").await;

    let repo = PgMarketLinkageRepository::new(db.clone());
    let early_at = Utc.with_ymd_and_hms(2026, 7, 1, 11, 0, 0).unwrap();
    let late_at = Utc.with_ymd_and_hms(2026, 7, 1, 11, 30, 0).unwrap();
    repo.append(
        linkage(
            "0xlinkage2",
            LinkageOutcome::Unresolved {
                reason: "no template matched".to_owned(),
            },
            early_at,
            3,
        )
        .to_new()
        .expect("new"),
    )
    .await
    .expect("append early");
    repo.append(
        linkage(
            "0xlinkage2",
            LinkageOutcome::Unresolved {
                reason: "metadata revised".to_owned(),
            },
            late_at,
            4,
        )
        .to_new()
        .expect("new"),
    )
    .await
    .expect("append late");

    let mid = early_at + ChronoDuration::minutes(10);
    let market_ids = vec![MarketId::new("0xlinkage2")];

    // The batch method (used by the online domain-availability projector)
    // must agree with the single-market `valid_at` at the SAME `as_of` — this
    // is the exact invariant the R5 fix restores (previously the online path
    // used `latest_for_markets`, which ignores `as_of` entirely).
    let single = repo
        .valid_at(&MarketId::new("0xlinkage2"), mid)
        .await
        .expect("valid_at")
        .expect("row");
    let batch = repo
        .valid_at_for_markets(&market_ids, mid)
        .await
        .expect("batch");
    assert_eq!(batch.len(), 1);
    assert_eq!(batch[0].linkage_id, single.linkage_id);
    assert_eq!(batch[0].derived_at, early_at);

    // `latest_for_markets` intentionally ignores `as_of` and always returns
    // the newest row — proving the two methods are genuinely different, not
    // an accidental duplicate.
    let latest = repo.latest_for_markets(&market_ids).await.expect("latest");
    assert_eq!(latest[0].derived_at, late_at);
}

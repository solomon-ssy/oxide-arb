//! Market-linkage ledger bitemporal PIT integration tests (Postgres +
//! testcontainers). Validates the real SQL behind the 11.2.2 remediation R5
//! fix: PIT reads constrain both source-effective and system-availability
//! clocks, while `latest_for_markets` (resolver idempotence only)
//! intentionally ignores the decision boundary — none of which a mock
//! repository can prove.

use chrono::{Duration as ChronoDuration, Utc};
use quant_pivot_models::{
    domain::{
        DecisionBoundary, DecisionClock, DecisionSource, LinkageOutcome, MarketLinkageDerivation,
        NewMarketLinkage,
    },
    enums::{common::MarketCategory, domain::ResolverTier},
    types::{ContentHash, MarketId, Probability, ResolverVersion},
};
use quant_pivot_repository::{
    postgres::{PgEventRepository, PgMarketLinkageRepository, PgMarketRepository},
    traits::{EventRepository, MarketLinkageRepository, MarketRepository},
};
use quant_pivot_test_support::{
    catalog_fixtures::{make_event, make_market},
    pg::setup_pg,
};

async fn seed_market(db: &sea_orm::DatabaseConnection, market_id: &str) {
    let events = PgEventRepository::new(db.clone());
    events
        .upsert(make_event(
            "evt-linkage-pit",
            "Bitcoin up or down",
            "btc-updown",
            MarketCategory::Crypto,
        ))
        .await
        .expect("seed event");

    let markets = PgMarketRepository::new(db.clone());
    markets
        .upsert(make_market(
            market_id,
            "evt-linkage-pit",
            "Will BTC be up?",
            "btc-updown-5m-1",
            MarketCategory::Crypto,
            None,
        ))
        .await
        .expect("seed market");
}

fn linkage(
    market_id: &str,
    outcome: LinkageOutcome,
    effective_at: chrono::DateTime<Utc>,
    seed: u8,
) -> NewMarketLinkage {
    let market_id = MarketId::new(market_id);
    let metadata_hash =
        ContentHash::parse(format!("blake3:{}", format!("{seed:02x}").repeat(32))).expect("hash");
    let capability_registry_hash =
        ContentHash::parse(format!("blake3:{}", "f".repeat(64))).expect("hash");
    NewMarketLinkage::from_derivation(MarketLinkageDerivation {
        market_id,
        outcome,
        confidence: Probability::ONE,
        resolver_tier: ResolverTier::Tier0Slug,
        resolver_version: ResolverVersion::FIRST,
        metadata_hash,
        capability_registry_hash,
        effective_at,
    })
    .expect("new linkage")
}

fn boundary(decision_at: chrono::DateTime<Utc>, knowledge_lag_secs: u64) -> DecisionBoundary {
    DecisionClock::new(knowledge_lag_secs)
        .boundary(decision_at)
        .expect("decision boundary")
        .with_source_cutoff(DecisionSource::Linkage, 0)
        .expect("linkage cutoff")
}

#[tokio::test]
#[ignore = "requires Docker"]
async fn valid_at_never_sees_a_revision_effective_after_the_source_cutoff() {
    let (pool, _container) = setup_pg().await;
    let db = pool.connection().clone();
    seed_market(&db, "0xlinkage1").await;

    let repo = PgMarketLinkageRepository::new(db.clone());
    let decision_at = Utc::now() + ChronoDuration::minutes(5);
    let early_at = decision_at - ChronoDuration::hours(2);
    let late_at = decision_at - ChronoDuration::hours(1);

    repo.append(linkage(
        "0xlinkage1",
        LinkageOutcome::Unresolved {
            reason: "no template matched".to_owned(),
        },
        early_at,
        1,
    ))
    .await
    .expect("append early");
    repo.append(linkage(
        "0xlinkage1",
        LinkageOutcome::Unresolved {
            reason: "metadata revised, still unresolved".to_owned(),
        },
        late_at,
        2,
    ))
    .await
    .expect("append late");

    // Before the late revision: only the early row is PIT-visible.
    let mid = boundary(decision_at, 5_400);
    let valid = repo
        .valid_at(&MarketId::new("0xlinkage1"), &mid)
        .await
        .expect("valid_at")
        .expect("some row");
    assert_eq!(
        valid.effective_at(),
        early_at,
        "must not see the future revision"
    );

    // After the late revision: the late row is visible.
    let after = boundary(decision_at, 0);
    let valid = repo
        .valid_at(&MarketId::new("0xlinkage1"), &after)
        .await
        .expect("valid_at")
        .expect("some row");
    assert_eq!(valid.effective_at(), late_at);

    // Before either row was derived: no PIT-valid record at all.
    let before = boundary(decision_at, 10_800);
    assert!(
        repo.valid_at(&MarketId::new("0xlinkage1"), &before)
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
    let decision_at = Utc::now() + ChronoDuration::minutes(5);
    let early_at = decision_at - ChronoDuration::hours(2);
    let late_at = decision_at - ChronoDuration::hours(1);
    repo.append(linkage(
        "0xlinkage2",
        LinkageOutcome::Unresolved {
            reason: "no template matched".to_owned(),
        },
        early_at,
        3,
    ))
    .await
    .expect("append early");
    repo.append(linkage(
        "0xlinkage2",
        LinkageOutcome::Unresolved {
            reason: "metadata revised".to_owned(),
        },
        late_at,
        4,
    ))
    .await
    .expect("append late");

    let mid = boundary(decision_at, 5_400);
    let market_ids = vec![MarketId::new("0xlinkage2")];

    // The batch method (used by the online domain-availability projector)
    // must agree with the single-market `valid_at` at the same boundary — this
    // is the exact invariant the R5 fix restores (previously the online path
    // used `latest_for_markets`, which ignores decision-time visibility).
    let single = repo
        .valid_at(&MarketId::new("0xlinkage2"), &mid)
        .await
        .expect("valid_at")
        .expect("row");
    let batch = repo
        .valid_at_for_markets(&market_ids, &mid)
        .await
        .expect("batch");
    assert_eq!(batch.len(), 1);
    assert_eq!(batch[0].linkage_id, single.linkage_id);
    assert_eq!(batch[0].effective_at(), early_at);

    // `latest_for_markets` intentionally ignores the decision boundary and returns
    // the newest row — proving the two methods are genuinely different, not
    // an accidental duplicate.
    let latest = repo.latest_for_markets(&market_ids).await.expect("latest");
    assert_eq!(latest[0].effective_at(), late_at);
}

#[tokio::test]
#[ignore = "requires Docker"]
async fn backdated_row_is_invisible_before_database_availability() {
    let (pool, _container) = setup_pg().await;
    let db = pool.connection().clone();
    seed_market(&db, "0xlinkage-late-created").await;

    let repo = PgMarketLinkageRepository::new(db);
    let effective_at = Utc::now() - ChronoDuration::hours(1);
    let inserted = repo
        .append(linkage(
            "0xlinkage-late-created",
            LinkageOutcome::Unresolved {
                reason: "backdated correction".to_owned(),
            },
            effective_at,
            5,
        ))
        .await
        .expect("append backdated row");
    let market_id = inserted.market_id.clone();

    let before_available = boundary(
        inserted.available_at() - ChronoDuration::milliseconds(1),
        30 * 60,
    );
    assert!(
        repo.valid_at(&market_id, &before_available)
            .await
            .expect("single PIT read")
            .is_none()
    );
    assert!(
        repo.valid_at_for_markets(std::slice::from_ref(&market_id), &before_available)
            .await
            .expect("batch PIT read")
            .is_empty()
    );
    assert!(
        repo.ledger_for_markets(std::slice::from_ref(&market_id), &before_available)
            .await
            .expect("ledger PIT read")
            .is_empty()
    );

    let after_available = boundary(
        inserted.available_at() + ChronoDuration::milliseconds(1),
        30 * 60,
    );
    assert!(
        inserted.available_at() > after_available.cutoff_for(DecisionSource::Linkage),
        "test must distinguish availability <= decision_at from availability <= source_cutoff"
    );
    assert_eq!(
        repo.valid_at(&market_id, &after_available)
            .await
            .expect("visible PIT read")
            .expect("visible row")
            .linkage_id,
        inserted.linkage_id
    );
    assert_eq!(
        repo.ledger_for_markets(std::slice::from_ref(&market_id), &after_available)
            .await
            .expect("visible ledger")
            .len(),
        1
    );
}

#[tokio::test]
#[ignore = "requires Docker"]
async fn append_batch_rolls_back_the_entire_group_when_any_member_is_invalid() {
    let (pool, _container) = setup_pg().await;
    let db = pool.connection().clone();
    seed_market(&db, "0xlinkage-batch-a").await;
    seed_market(&db, "0xlinkage-batch-b").await;

    let repo = PgMarketLinkageRepository::new(db);
    let effective_at = Utc::now() - ChronoDuration::minutes(1);
    let first = linkage(
        "0xlinkage-batch-a",
        LinkageOutcome::Unresolved {
            reason: "valid first member".to_owned(),
        },
        effective_at,
        6,
    );
    let mut invalid = linkage(
        "0xlinkage-batch-b",
        LinkageOutcome::Unresolved {
            reason: "will be corrupted".to_owned(),
        },
        effective_at,
        7,
    );
    invalid.outcome = serde_json::json!({ "invalid": "linkage outcome" });

    assert!(
        repo.append_batch(vec![first.clone(), invalid])
            .await
            .is_err()
    );
    let market_ids = vec![
        MarketId::new("0xlinkage-batch-a"),
        MarketId::new("0xlinkage-batch-b"),
    ];
    assert!(
        repo.latest_for_markets(&market_ids)
            .await
            .expect("latest after rollback")
            .is_empty(),
        "the valid first insert must roll back with the invalid sibling"
    );

    let second = linkage(
        "0xlinkage-batch-b",
        LinkageOutcome::Unresolved {
            reason: "valid second member".to_owned(),
        },
        effective_at,
        8,
    );
    let appended = repo
        .append_batch(vec![first, second])
        .await
        .expect("valid atomic batch");
    assert_eq!(appended.len(), 2);
    assert_eq!(
        repo.latest_for_markets(&market_ids)
            .await
            .expect("latest valid batch")
            .len(),
        2
    );
}

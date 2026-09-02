//! Immutable recommendation-resolution outcome persistence contracts.

use chrono::{DateTime, Duration, TimeZone, Utc};
use quant_pivot_error::storage::StorageError;
use quant_pivot_models::{
    clickhouse::{MarketResolutionFactInput, MarketResolutionRow},
    domain::quant::{
        InsertResolutionOutcomeResult, OutcomeTaskSettlement, RecommendationResolutionOutcomeInfo,
        RecommendationResolutionOutcomePageQuery,
    },
    entities::quant_recommendation_resolution_outcome::{
        ActiveModel as QuantRecommendationResolutionOutcomeActiveModel,
        Entity as QuantRecommendationResolutionOutcomeEntity,
    },
    enums::market::MarketStatus,
    types::{
        ContentHash, EvmBlockHash, EvmTransactionHash, MarketId, PayoutRatio, RecommendationId,
        TokenId, WorkerId,
    },
};
use quant_pivot_repository::{
    postgres::{PgMarketRepository, PgRecommendationResolutionOutcomeRepository},
    traits::{MarketRepository, RecommendationResolutionOutcomeRepository},
};
use quant_pivot_system_tests::{
    postgres::{PostgresClock, setup_pg},
    support::execution_pg_seed::{
        ExecutionTxnIds, ReportSeedConfig, seed_report_on_infra, seed_settlement_report_fixture,
        seed_shared_demo_infra,
    },
};
use rust_decimal_macros::dec;
use sea_orm::{
    ActiveModelTrait, ActiveValue, ConnectionTrait, DatabaseConnection, EntityTrait,
    IntoActiveModel,
};

fn hash(seed: char) -> ContentHash {
    ContentHash::parse(&format!("blake3:{}", seed.to_string().repeat(64))).expect("valid hash")
}

fn resolution_fact(
    ids: &ExecutionTxnIds,
    payout_ratios: [PayoutRatio; 2],
    resolved_at: DateTime<Utc>,
    observed_at: DateTime<Utc>,
    log_index: u64,
) -> MarketResolutionRow {
    resolution_fact_for_tokens(
        ids,
        [TokenId::new(&ids.token), TokenId::new("67890")],
        payout_ratios,
        resolved_at,
        observed_at,
        log_index,
    )
}

fn resolution_fact_for_tokens(
    ids: &ExecutionTxnIds,
    token_ids: [TokenId; 2],
    payout_ratios: [PayoutRatio; 2],
    resolved_at: DateTime<Utc>,
    observed_at: DateTime<Utc>,
    log_index: u64,
) -> MarketResolutionRow {
    MarketResolutionRow::seal(MarketResolutionFactInput {
        market_id: MarketId::new(&ids.market),
        token_ids,
        payout_ratios,
        resolved_at: resolved_at.timestamp_millis(),
        observed_at: observed_at.timestamp_millis(),
        source_block_number: 42,
        source_block_hash: EvmBlockHash::parse(format!("0x{}", "11".repeat(32)))
            .expect("block hash"),
        source_transaction_hash: EvmTransactionHash::parse(format!("0x{}", "22".repeat(32)))
            .expect("transaction hash"),
        source_log_index: log_index,
        source_checkpoint_hash: hash('a'),
    })
    .expect("sealed resolution fact")
}

pub async fn reconcile_fact_idempotent_rejects() {
    let (pool, _database) = setup_pg().await;
    let db = pool.connection().clone();
    let ids = seed_settlement_report_fixture(&db).await;
    let repository = PgRecommendationResolutionOutcomeRepository::new(db);
    let available_at = Utc::now() - Duration::minutes(1);
    let fact = resolution_fact(
        &ids,
        [
            PayoutRatio::try_new(dec!(0.5)).expect("split payout"),
            PayoutRatio::try_new(dec!(0.5)).expect("split payout"),
        ],
        available_at - Duration::hours(1),
        available_at - Duration::seconds(1),
        7,
    );

    let inserted = repository
        .reconcile_fact(&ids.recommendation, &fact)
        .await
        .expect("reconcile resolution fact");
    let inserted = match inserted {
        InsertResolutionOutcomeResult::Inserted(outcome) => outcome,
        InsertResolutionOutcomeResult::AlreadyPresent(_) => {
            panic!("first append must insert the outcome")
        }
    };
    assert_eq!(
        inserted.token_payout_ratio,
        PayoutRatio::try_new(dec!(0.5)).expect("split payout")
    );
    assert!(inserted.source_observed_at <= inserted.available_at);
    assert!(inserted.available_at <= inserted.created_at);

    let duplicate = repository
        .reconcile_fact(&ids.recommendation, &fact)
        .await
        .expect("idempotent outcome retry");
    let duplicate = match duplicate {
        InsertResolutionOutcomeResult::AlreadyPresent(outcome) => outcome,
        InsertResolutionOutcomeResult::Inserted(_) => {
            panic!("exact retry must not insert another outcome")
        }
    };
    assert_eq!(duplicate.outcome_hash, inserted.outcome_hash);

    let mut tampered = fact.clone();
    tampered.source_log_index += 1;
    assert!(matches!(
        repository
            .reconcile_fact(&ids.recommendation, &tampered)
            .await
            .expect_err("tampered fact content address must fail closed"),
        StorageError::InvariantViolation { .. }
    ));

    let foreign_tokens = resolution_fact_for_tokens(
        &ids,
        [TokenId::new("67890"), TokenId::new("98760")],
        [PayoutRatio::ONE, PayoutRatio::ZERO],
        available_at - Duration::hours(1),
        available_at - Duration::seconds(1),
        7,
    );
    assert!(matches!(
        repository
            .reconcile_fact(&ids.recommendation, &foreign_tokens)
            .await
            .expect_err("recommendation token must belong to the canonical payout vector"),
        StorageError::InvariantViolation { .. }
    ));

    let conflict = resolution_fact(
        &ids,
        [PayoutRatio::ONE, PayoutRatio::ZERO],
        available_at - Duration::hours(1),
        available_at - Duration::seconds(1),
        7,
    );
    let error = repository
        .reconcile_fact(&ids.recommendation, &conflict)
        .await
        .expect_err("same recommendation cannot acquire different immutable resolution content");
    assert!(matches!(error, StorageError::StateConflict { .. }));

    let stored = repository
        .find_by_recommendation(&ids.recommendation)
        .await
        .expect("read immutable outcome")
        .expect("stored outcome");
    assert_eq!(stored.outcome_hash, inserted.outcome_hash);
    assert_eq!(
        stored.token_payout_ratio,
        PayoutRatio::try_new(dec!(0.5)).expect("split payout")
    );
}

pub async fn database_owned_availability_enforced() {
    let (pool, _database) = setup_pg().await;
    let db = pool.connection().clone();
    let ids = seed_settlement_report_fixture(&db).await;
    let repository = PgRecommendationResolutionOutcomeRepository::new(db.clone());
    let source_observed_at = Utc::now() - Duration::minutes(1);
    let future_source = resolution_fact(
        &ids,
        [PayoutRatio::ONE, PayoutRatio::ZERO],
        Utc::now(),
        Utc::now() + Duration::days(1),
        11,
    );
    assert!(matches!(
        repository
            .reconcile_fact(&ids.recommendation, &future_source)
            .await
            .expect_err("source observation after database availability must fail"),
        StorageError::InvariantViolation { .. }
    ));

    let fact = resolution_fact(
        &ids,
        [PayoutRatio::ONE, PayoutRatio::ZERO],
        source_observed_at - Duration::hours(1),
        source_observed_at,
        11,
    );
    repository
        .reconcile_fact(&ids.recommendation, &fact)
        .await
        .expect("reconcile valid resolution fact");

    let mutation = db
        .execute_unprepared(
            "UPDATE quant_recommendation_resolution_outcome \
             SET resolution_fact_log_index = resolution_fact_log_index + 1",
        )
        .await;
    assert!(mutation.is_err(), "WORM trigger must reject updates");
    let deletion = db
        .execute_unprepared("DELETE FROM quant_recommendation_resolution_outcome")
        .await;
    assert!(deletion.is_err(), "WORM trigger must reject deletes");

    db.execute_unprepared(
        "ALTER TABLE quant_recommendation_resolution_outcome \
         DISABLE TRIGGER trg_quant_recommendation_resolution_outcome_append_only",
    )
    .await
    .expect("disable trigger to simulate storage corruption");
    db.execute_unprepared(
        "UPDATE quant_recommendation_resolution_outcome \
         SET resolution_fact_log_index = resolution_fact_log_index + 1",
    )
    .await
    .expect("simulate stored semantic-content tampering");
    db.execute_unprepared(
        "ALTER TABLE quant_recommendation_resolution_outcome \
         ENABLE TRIGGER trg_quant_recommendation_resolution_outcome_append_only",
    )
    .await
    .expect("restore append-only trigger");

    assert!(matches!(
        repository
            .find_by_recommendation(&ids.recommendation)
            .await
            .expect_err("read boundary must detect stored content tampering"),
        StorageError::InvariantViolation { .. }
    ));
}

pub async fn keyset_total_ordered_bound() {
    let (pool, _database) = setup_pg().await;
    let db = pool.connection().clone();
    let ids = seed_resolution_recommendations(&db, 4).await;
    let repository = PgRecommendationResolutionOutcomeRepository::new(db.clone());
    let window_start = Utc
        .with_ymd_and_hms(2026, 7, 1, 0, 0, 0)
        .single()
        .expect("valid UTC timestamp");
    let first_available = window_start + Duration::hours(1);
    let cutoff = window_start + Duration::hours(2);
    let availabilities = [
        first_available,
        first_available,
        cutoff,
        cutoff + Duration::milliseconds(1),
    ];

    for (ordinal, (ids, available_at)) in ids.iter().zip(availabilities).enumerate() {
        let fact = resolution_fact(
            ids,
            [PayoutRatio::ONE, PayoutRatio::ZERO],
            available_at - Duration::hours(1),
            available_at - Duration::seconds(1),
            u64::try_from(ordinal).expect("small ordinal"),
        );
        repository
            .reconcile_fact(&ids.recommendation, &fact)
            .await
            .expect("reconcile keyset fixture");
    }
    db.execute_unprepared(
        "ALTER TABLE quant_recommendation_resolution_outcome \
         DISABLE TRIGGER trg_quant_recommendation_resolution_outcome_append_only",
    )
    .await
    .expect("disable WORM trigger for deterministic keyset fixture");
    for (ids, available_at) in ids.iter().zip(availabilities) {
        rewrite_availability_fixture(&db, ids.recommendation, available_at).await;
    }
    db.execute_unprepared(
        "ALTER TABLE quant_recommendation_resolution_outcome \
         ENABLE TRIGGER trg_quant_recommendation_resolution_outcome_append_only",
    )
    .await
    .expect("restore WORM trigger after deterministic keyset fixture");

    let first_page = repository
        .list_available_page(RecommendationResolutionOutcomePageQuery {
            available_from: window_start,
            available_through: cutoff,
            after: None,
            limit: 2,
        })
        .await
        .expect("first keyset page");
    assert_eq!(first_page.outcomes.len(), 2);
    assert!(
        first_page
            .outcomes
            .windows(2)
            .all(|pair| pair[0].cursor() < pair[1].cursor())
    );
    assert!(
        first_page
            .outcomes
            .iter()
            .all(|outcome| outcome.available_at == first_available)
    );

    let second_page = repository
        .list_available_page(RecommendationResolutionOutcomePageQuery {
            available_from: window_start,
            available_through: cutoff,
            after: first_page.next_cursor,
            limit: 2,
        })
        .await
        .expect("second keyset page");
    assert_eq!(second_page.outcomes.len(), 1);
    assert_eq!(second_page.outcomes[0].available_at, cutoff);

    let final_page = repository
        .list_available_page(RecommendationResolutionOutcomePageQuery {
            available_from: window_start,
            available_through: cutoff,
            after: second_page.next_cursor,
            limit: 2,
        })
        .await
        .expect("terminal keyset page");
    assert!(final_page.outcomes.is_empty());
    assert_eq!(final_page.next_cursor, None);
}

pub async fn reconciliation_candidates_terminal_aware() {
    let (pool, _database) = setup_pg().await;
    let db = pool.connection().clone();
    let mut ids = seed_resolution_recommendations(&db, 3).await;
    ids.sort_by_key(|ids| ids.recommendation.as_uuid());
    let markets = PgMarketRepository::new(db.clone());
    for terminal in &ids[..2] {
        markets
            .update_status(
                &MarketId::new(&terminal.market),
                MarketStatus::Settled,
                Some("Yes"),
            )
            .await
            .expect("mark resolution candidate terminal");
    }
    let repository = PgRecommendationResolutionOutcomeRepository::new(db.clone());
    let cutoff = db.statement_time().await + Duration::seconds(30);
    let old_cutoff = cutoff - Duration::days(1);
    assert_eq!(
        repository
            .source_history_start(old_cutoff)
            .await
            .expect("read empty source history before recommendation visibility"),
        None
    );
    assert!(
        repository
            .source_history_start(cutoff)
            .await
            .expect("read earliest source history boundary")
            .is_some()
    );
    assert!(
        repository
            .claim_reconciliation(old_cutoff, WorkerId::from_v7(), 60, 10)
            .await
            .expect("scan resolution candidates before recommendation visibility")
            .is_empty()
    );

    let first_worker = WorkerId::from_v7();
    let first_page = repository
        .claim_reconciliation(cutoff, first_worker, 60, 1)
        .await
        .expect("first resolution reconciliation candidate");
    assert_eq!(first_page.len(), 1);
    assert_eq!(
        first_page[0].candidate.recommendation_id,
        ids[0].recommendation
    );
    assert_eq!(
        first_page[0].candidate.market_id,
        MarketId::new(&ids[0].market)
    );

    let second_worker = WorkerId::from_v7();
    let second_page = repository
        .claim_reconciliation(cutoff, second_worker, 60, 1)
        .await
        .expect("second resolution reconciliation candidate");
    assert_eq!(second_page.len(), 1);
    assert_eq!(
        second_page[0].candidate.recommendation_id,
        ids[1].recommendation
    );

    let fact = resolution_fact(
        &ids[0],
        [PayoutRatio::ONE, PayoutRatio::ZERO],
        Utc::now() - Duration::hours(1),
        Utc::now() - Duration::minutes(1),
        31,
    );
    repository
        .reconcile_fact(&ids[0].recommendation, &fact)
        .await
        .expect("seal first resolution outcome");
    repository
        .settle_reconciliation(
            ids[0].recommendation,
            first_worker,
            OutcomeTaskSettlement::Completed,
        )
        .await
        .expect("complete first durable resolution task");
    repository
        .settle_reconciliation(
            ids[1].recommendation,
            second_worker,
            OutcomeTaskSettlement::RetryAfter {
                delay_secs: 60,
                error: "canonical fact pending".to_owned(),
            },
        )
        .await
        .expect("durably defer second resolution task");
    let no_active_market = repository
        .claim_reconciliation(cutoff, WorkerId::from_v7(), 60, 10)
        .await
        .expect("active markets remain outside the durable resolution queue");
    assert!(no_active_market.is_empty());
}

async fn rewrite_availability_fixture(
    db: &DatabaseConnection,
    recommendation_id: RecommendationId,
    available_at: DateTime<Utc>,
) {
    let row = QuantRecommendationResolutionOutcomeEntity::find_by_id(recommendation_id)
        .one(db)
        .await
        .expect("read keyset fixture")
        .expect("keyset fixture row");
    let mut outcome: RecommendationResolutionOutcomeInfo = row.clone().into();
    outcome.available_at = available_at;
    outcome.outcome_hash = outcome
        .expected_outcome_hash()
        .expect("re-hash deterministic availability");
    let mut active: QuantRecommendationResolutionOutcomeActiveModel = row.into_active_model();
    active.available_at = ActiveValue::Set(outcome.available_at);
    active.outcome_hash = ActiveValue::Set(outcome.outcome_hash);
    active
        .update(db)
        .await
        .expect("rewrite deterministic keyset fixture");
}

async fn seed_resolution_recommendations(
    db: &DatabaseConnection,
    count: usize,
) -> Vec<ExecutionTxnIds> {
    let infra = seed_shared_demo_infra(db).await;
    let mut recommendations = Vec::with_capacity(count);
    for ordinal in 0..count {
        recommendations.push(
            seed_report_on_infra(
                db,
                &infra,
                ReportSeedConfig {
                    event_id: format!("resolution-event-{ordinal}"),
                    market_id: format!("0xresolution-market-{ordinal}"),
                    market_question: format!("Will resolution fixture {ordinal} settle?"),
                    market_slug: format!("resolution-fixture-{ordinal}"),
                    token_id: format!("{}", 10_000 + ordinal),
                    trigger_key: format!("resolution-outcome:{ordinal}"),
                },
            )
            .await,
        );
    }
    recommendations
}

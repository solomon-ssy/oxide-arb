//! Recommendation economic outcome WORM and lineage contracts on real `PostgreSQL`.

use std::time::Duration as StdDuration;

use chrono::{DateTime, Duration, Utc};
use quant_pivot_error::storage::StorageError;
use quant_pivot_models::{
    clickhouse::{MarketResolutionFactInput, MarketResolutionRow},
    domain::quant::{
        EconomicExitEvidenceKind, EconomicOutcomeReconciliationResult, EconomicOutcomeTaskClaim,
        EconomicOutcomeTaskSettlement, NewRecommendationEconomicOutcome,
        RecommendationEconomicAmounts, RecommendationEconomicEvidence,
        RecommendationEconomicOutcomeInput, RecommendationEconomicOutcomePayload,
        RecommendationEconomicStateDetail, RecommendationResolutionOutcomeInfo,
    },
    entities::{
        quant_economic_outcome_reconciliation_task::{
            Column as EconomicTaskColumn, Entity as EconomicTaskEntity,
        },
        quant_recommendation::Entity as RecommendationEntity,
        quant_recommendation_economic_outcome::Entity as EconomicOutcomeEntity,
        quant_recommendation_report::Entity as ReportEntity,
        quant_report_route_run::Entity as RouteRunEntity,
        research_profile_artifact::Entity as ResearchProfileEntity,
    },
    enums::quant::{OutcomeReconciliationTaskStatus, RecommendationEconomicOutcomeState},
    types::{
        ContentHash, EvmBlockHash, EvmTransactionHash, MarketId, PayoutRatio, RecommendationId,
        Shares, TokenId, Usd, WorkerId,
    },
};
use quant_pivot_repository::{
    postgres::{
        PgRecommendationEconomicOutcomeRepository, PgRecommendationResolutionOutcomeRepository,
    },
    traits::{RecommendationEconomicOutcomeRepository, RecommendationResolutionOutcomeRepository},
};
use quant_pivot_research::policy_replay::POLICY_REPLAY_KERNEL_VERSION;
use quant_pivot_system_tests::{
    postgres::{PostgresClock, setup_pg},
    support::{
        economic_outcome_fixtures::seed_report_at,
        execution_pg_seed::{ExecutionTxnIds, fixture_no_token_id, seed_shared_demo_infra},
    },
};
use rust_decimal_macros::dec;
use sea_orm::{
    ColumnTrait, ConnectionTrait, DatabaseConnection, DbBackend, EntityTrait, QueryFilter,
    Statement,
};

async fn liquidation_outcome(
    db: &DatabaseConnection,
    recommendation_id: RecommendationId,
) -> NewRecommendationEconomicOutcome {
    let recommendation = RecommendationEntity::find_by_id(recommendation_id)
        .one(db)
        .await
        .expect("recommendation read")
        .expect("recommendation exists");
    let report = ReportEntity::find_by_id(recommendation.recommendation_report_id)
        .one(db)
        .await
        .expect("report read")
        .expect("report exists");
    let route_run = RouteRunEntity::find_by_id(recommendation.report_route_run_id)
        .one(db)
        .await
        .expect("route run read")
        .expect("route run exists");
    let profile_id = route_run
        .research_profile_artifact_id
        .clone()
        .expect("route profile");
    let profile = ResearchProfileEntity::find_by_id(profile_id.clone())
        .one(db)
        .await
        .expect("profile read")
        .expect("profile exists");
    let horizon_at = report.decision_at
        + Duration::seconds(
            i64::try_from(profile.spec.target_horizon_secs).expect("profile horizon range"),
        );
    NewRecommendationEconomicOutcome::try_seal(RecommendationEconomicOutcomeInput {
        recommendation_id: recommendation.recommendation_id,
        recommendation_report_id: report.recommendation_report_id,
        report_route_run_id: route_run.report_route_run_id,
        decision_policy_snapshot_id: report.decision_policy_snapshot_id,
        economic_tier_id: recommendation.economic_tier_id,
        model_version_id: route_run.model_version_id.expect("route model"),
        trade_policy_artifact_id: route_run.trade_policy_artifact_id.expect("route policy"),
        research_profile_artifact_id: profile_id,
        state: RecommendationEconomicOutcomeState::HorizonLiquidated,
        decision_at: report.decision_at,
        horizon_at,
        source_available_until: horizon_at,
        replay_kernel_version: POLICY_REPLAY_KERNEL_VERSION.to_owned(),
        payload: RecommendationEconomicOutcomePayload {
            detail: RecommendationEconomicStateDetail::HorizonLiquidated {
                entered_at: report.decision_at + Duration::seconds(1),
                liquidated_at: horizon_at,
            },
            amounts: RecommendationEconomicAmounts {
                entry_filled_shares: Shares::new(dec!(10)),
                exited_shares: Shares::new(dec!(10)),
                entry_cost_usd: Usd::new(dec!(10)),
                exit_proceeds_usd: Usd::new(dec!(11)),
                resolution_payout_usd: Usd::ZERO,
                execution_fee_usd: Usd::new(dec!(0.5)),
                expected_maker_rebate_usd: Usd::ZERO,
                net_pnl_usd: Some(Usd::new(dec!(0.5))),
                net_return_bps: Some(dec!(500)),
            },
            evidence: RecommendationEconomicEvidence {
                exit_evidence_kind: EconomicExitEvidenceKind::FullBidLadder,
                full_l2_covered: true,
                fee_covered: true,
                passive_trade_covered: None,
                replay_input_hash: ContentHash::from_bytes([51; 32]),
                replay_output_hash: ContentHash::from_bytes([52; 32]),
            },
        },
        available_at: horizon_at,
    })
    .expect("economic outcome contract")
}

async fn resolve_report(
    db: &DatabaseConnection,
    ids: &ExecutionTxnIds,
    resolved_at: DateTime<Utc>,
    observed_at: DateTime<Utc>,
) -> RecommendationResolutionOutcomeInfo {
    let fact = MarketResolutionRow::seal(MarketResolutionFactInput {
        market_id: MarketId::new(&ids.market),
        token_ids: [
            TokenId::new(&ids.token),
            fixture_no_token_id(&ids.market, &ids.token),
        ],
        payout_ratios: [PayoutRatio::ONE, PayoutRatio::ZERO],
        resolved_at: resolved_at.timestamp_millis(),
        observed_at: observed_at.timestamp_millis(),
        source_block_number: 42,
        source_block_hash: EvmBlockHash::parse(format!("0x{}", "11".repeat(32)))
            .expect("block hash"),
        source_transaction_hash: EvmTransactionHash::parse(format!("0x{}", "22".repeat(32)))
            .expect("transaction hash"),
        source_log_index: 1,
        source_checkpoint_hash: ContentHash::from_bytes([61; 32]),
    })
    .expect("canonical resolution fact");
    let repository = PgRecommendationResolutionOutcomeRepository::new(db.clone());
    repository
        .reconcile_fact(&ids.recommendation, &fact)
        .await
        .expect("project resolution");
    repository
        .find_by_recommendation(&ids.recommendation)
        .await
        .expect("read resolution")
        .expect("canonical resolution exists")
}

fn bind_claim(
    outcome: NewRecommendationEconomicOutcome,
    claim: EconomicOutcomeTaskClaim,
) -> NewRecommendationEconomicOutcome {
    let mut input = RecommendationEconomicOutcomeInput::from(outcome);
    input.source_available_until = claim.source_available_until;
    input.available_at = claim.source_available_until;
    NewRecommendationEconomicOutcome::try_seal(input).expect("claim-bound economic outcome")
}

async fn expire_lease(db: &DatabaseConnection, id: RecommendationId) {
    db.execute_raw(Statement::from_sql_and_values(
        DbBackend::Postgres,
        "UPDATE quant_economic_outcome_reconciliation_task SET lease_expires_at = statement_timestamp() + interval '1 millisecond' WHERE recommendation_id = $1",
        [id.into()],
    )).await.expect("shorten lease at fault point");
    tokio::time::sleep(StdDuration::from_millis(10)).await;
}

async fn release_retry(db: &DatabaseConnection, id: RecommendationId) {
    db.execute_raw(Statement::from_sql_and_values(
        DbBackend::Postgres,
        "UPDATE quant_economic_outcome_reconciliation_task SET next_attempt_at = statement_timestamp() + interval '1 millisecond' WHERE recommendation_id = $1",
        [id.into()],
    )).await.expect("release retry at fault point");
    tokio::time::sleep(StdDuration::from_millis(10)).await;
}

pub async fn worm_lineage_is_enforced() {
    let (pool, _container) = setup_pg().await;
    let db = pool.connection().clone();
    let infra = Box::pin(seed_shared_demo_infra(&db)).await;
    let ids = seed_report_at(&db, &infra, db.statement_time().await - Duration::days(2))
        .await
        .expect("historical economic report");
    let outcome = liquidation_outcome(&db, ids.recommendation).await;
    let repository = PgRecommendationEconomicOutcomeRepository::new(db.clone());
    let future = outcome
        .clone()
        .with_available_at(db.statement_time().await + Duration::days(1))
        .expect("seal future payload for a rejection test");
    assert!(matches!(
        repository.insert(future).await,
        Err(StorageError::InvariantViolation { .. })
    ));
    assert!(
        repository
            .find_by_id(&ids.recommendation)
            .await
            .expect("future insert read")
            .is_none()
    );
    let inserted = repository
        .insert(outcome.clone())
        .await
        .expect("insert economic outcome");
    let replayed = repository
        .insert(outcome)
        .await
        .expect("replay economic outcome");
    assert_eq!(inserted, replayed);
    assert!(
        EconomicOutcomeEntity::delete_by_id(ids.recommendation)
            .exec(&db)
            .await
            .is_err(),
        "economic outcome must be append-only",
    );
}

pub async fn durable_horizon_queue_enforced() {
    let (pool, _container) = setup_pg().await;
    let db = pool.connection().clone();
    let infra = Box::pin(seed_shared_demo_infra(&db)).await;
    let ids = seed_report_at(&db, &infra, db.statement_time().await - Duration::days(2))
        .await
        .expect("historical horizon report");
    let task = EconomicTaskEntity::find_by_id(ids.recommendation)
        .one(&db)
        .await
        .expect("economic task read")
        .expect("published recommendation enqueues economic task");
    assert_eq!(task.status, OutcomeReconciliationTaskStatus::Pending);
    assert!(task.source_cutoff_at.is_none());

    let repository = PgRecommendationEconomicOutcomeRepository::new(db.clone());
    let worker_one = WorkerId::from_v7();
    let worker_two = WorkerId::from_v7();
    let before_horizon = task.horizon_at - Duration::milliseconds(1);
    assert!(
        repository
            .claim_due(before_horizon, worker_one, 60, 300, 1)
            .await
            .expect("claim before horizon")
            .is_empty(),
        "economic work must not run before its frozen horizon",
    );
    let first = repository
        .claim_due(task.horizon_at, worker_one, 60, 300, 1)
        .await
        .expect("first horizon claim");
    assert_eq!(first.len(), 1);
    assert_eq!(first[0].attempt_count, 1);
    assert_eq!(
        first[0].source_cutoff_at,
        task.horizon_at + Duration::seconds(300)
    );
    assert!(
        repository
            .claim_due(task.horizon_at, worker_two, 60, 600, 1)
            .await
            .expect("competing horizon claim")
            .is_empty(),
        "an active lease must exclude a competing worker",
    );

    db.execute_raw(Statement::from_string(
        DbBackend::Postgres,
        format!(
            "UPDATE quant_economic_outcome_reconciliation_task \
             SET lease_expires_at = statement_timestamp() + interval '1 millisecond' \
             WHERE recommendation_id = '{}'",
            ids.recommendation
        ),
    ))
    .await
    .expect("shorten crashed worker lease");
    tokio::time::sleep(StdDuration::from_millis(10)).await;
    let recovered = repository
        .claim_due(task.horizon_at, worker_two, 60, 600, 1)
        .await
        .expect("recover expired horizon lease");
    assert_eq!(recovered.len(), 1);
    assert_eq!(recovered[0].attempt_count, 2);
    assert_eq!(
        recovered[0].source_cutoff_at, first[0].source_cutoff_at,
        "source cutoff must remain immutable across crash recovery and config drift",
    );
    repository
        .retry_task(recovered[0], worker_two, 1, "late_source".to_owned())
        .await
        .expect("durable late-source retry");
    assert!(
        EconomicTaskEntity::find()
            .filter(EconomicTaskColumn::RecommendationId.eq(ids.recommendation))
            .filter(EconomicTaskColumn::Status.eq(OutcomeReconciliationTaskStatus::Retrying))
            .one(&db)
            .await
            .expect("retry task read")
            .is_some()
    );
    db.execute_raw(Statement::from_string(
        DbBackend::Postgres,
        format!(
            "UPDATE quant_economic_outcome_reconciliation_task \
             SET next_attempt_at = statement_timestamp() + interval '1 millisecond' \
             WHERE recommendation_id = '{}'",
            ids.recommendation
        ),
    ))
    .await
    .expect("release retry for deterministic test");
    tokio::time::sleep(StdDuration::from_millis(10)).await;
    let retried = repository
        .claim_due(task.horizon_at, worker_two, 60, 900, 1)
        .await
        .expect("claim released retry");
    assert_eq!(retried[0].attempt_count, 3);
    assert_eq!(retried[0].source_cutoff_at, first[0].source_cutoff_at);
    let outcome = bind_claim(
        liquidation_outcome(&db, ids.recommendation).await,
        retried[0],
    );
    let result = repository
        .complete_task(retried[0], worker_two, outcome)
        .await
        .expect("complete horizon task");
    assert!(matches!(
        result,
        EconomicOutcomeReconciliationResult::Inserted(_)
    ));
    let completed = EconomicTaskEntity::find_by_id(ids.recommendation)
        .one(&db)
        .await
        .expect("completed task read")
        .expect("completed task exists");
    assert_eq!(completed.status, OutcomeReconciliationTaskStatus::Completed);
    assert!(completed.completed_at.is_some());
}

pub async fn early_resolution_freezes_boundary() {
    let (pool, _container) = setup_pg().await;
    let db = pool.connection().clone();
    let infra = Box::pin(seed_shared_demo_infra(&db)).await;
    let ids = seed_report_at(
        &db,
        &infra,
        db.statement_time().await - Duration::minutes(10),
    )
    .await
    .expect("early report");
    let now = db.statement_time().await;
    let resolution = resolve_report(
        &db,
        &ids,
        ids.decision_at - Duration::seconds(5),
        now - Duration::seconds(5),
    )
    .await;
    let repository = PgRecommendationEconomicOutcomeRepository::new(db.clone());
    let worker = WorkerId::from_v7();
    let first = repository
        .claim_due(db.statement_time().await, worker, 60, 300, 1)
        .await
        .expect("early claim");
    assert_eq!(first.len(), 1);
    let claim = first[0];
    assert!(claim.replay_until < claim.horizon_at);
    assert_eq!(claim.replay_until, resolution.source_observed_at);
    assert_eq!(claim.resolution_outcome_hash, Some(resolution.outcome_hash));
    assert_eq!(
        claim.source_cutoff_at,
        resolution.available_at + Duration::seconds(300)
    );
    assert!(claim.source_available_until <= db.statement_time().await);
    let context = repository
        .replay_context(&ids.recommendation)
        .await
        .expect("frozen context");
    assert_eq!(context.resolution_outcome, Some(resolution));
    for column in ["source_cutoff_at", "replay_until", "horizon_at"] {
        assert!(db.execute_raw(Statement::from_sql_and_values(
            DbBackend::Postgres,
            format!("UPDATE quant_economic_outcome_reconciliation_task SET {column} = {column} + interval '1 second' WHERE recommendation_id = $1"),
            [ids.recommendation.into()],
        )).await.is_err(), "frozen {column} changed");
    }
    assert_eq!(
        repository
            .retry_task(claim, worker, 1, "late source".to_owned())
            .await
            .expect("retry"),
        EconomicOutcomeTaskSettlement::Retried
    );
    release_retry(&db, ids.recommendation).await;
    let second = repository
        .claim_due(db.statement_time().await, worker, 60, 600, 1)
        .await
        .expect("retry claim");
    assert_eq!(second.len(), 1);
    assert_eq!(second[0].replay_until, claim.replay_until);
    assert_eq!(
        second[0].resolution_outcome_hash,
        claim.resolution_outcome_hash
    );
    assert_eq!(second[0].source_cutoff_at, claim.source_cutoff_at);
    assert!(second[0].source_available_until >= claim.source_available_until);
}

pub async fn future_terminal_stays_pending() {
    let (pool, _container) = setup_pg().await;
    let db = pool.connection().clone();
    let infra = Box::pin(seed_shared_demo_infra(&db)).await;
    let ids = seed_report_at(
        &db,
        &infra,
        db.statement_time().await - Duration::minutes(10),
    )
    .await
    .expect("future maturity report");
    let repository = PgRecommendationEconomicOutcomeRepository::new(db.clone());
    let worker = WorkerId::from_v7();
    assert!(
        repository
            .claim_due(
                db.statement_time().await + Duration::days(2),
                worker,
                60,
                300,
                1
            )
            .await
            .expect("future caller clock")
            .is_empty()
    );
    let now = db.statement_time().await;
    let resolution = resolve_report(&db, &ids, now - Duration::seconds(1), now).await;
    let context = repository
        .replay_context(&ids.recommendation)
        .await
        .expect("early context");
    let visible_at = resolution.resolved_at
        + Duration::from_std(context.decision_boundary.knowledge_lag()).expect("lag");
    assert!(visible_at > db.statement_time().await);
    assert!(
        repository
            .claim_due(now + Duration::days(2), worker, 60, 300, 1)
            .await
            .expect("future terminal visibility")
            .is_empty()
    );
    let task = EconomicTaskEntity::find_by_id(ids.recommendation)
        .one(&db)
        .await
        .expect("task read")
        .expect("task");
    assert_eq!(task.attempt_count, 0);
    assert!(task.replay_until.is_none() && task.source_cutoff_at.is_none());
}

pub async fn stale_claims_cannot_publish() {
    let (pool, _container) = setup_pg().await;
    let db = pool.connection().clone();
    let infra = Box::pin(seed_shared_demo_infra(&db)).await;
    let ids = seed_report_at(&db, &infra, db.statement_time().await - Duration::days(2))
        .await
        .expect("mature fenced report");
    let repository = PgRecommendationEconomicOutcomeRepository::new(db.clone());
    let worker = WorkerId::from_v7();
    let first = repository
        .claim_due(db.statement_time().await, worker, 60, 300, 1)
        .await
        .expect("first claim")[0];
    let outcome = bind_claim(liquidation_outcome(&db, ids.recommendation).await, first);
    assert!(matches!(
        repository
            .complete_task(first, WorkerId::from_v7(), outcome.clone())
            .await
            .expect("foreign owner"),
        EconomicOutcomeReconciliationResult::ClaimLost
    ));
    expire_lease(&db, ids.recommendation).await;
    assert!(matches!(
        repository
            .complete_task(first, worker, outcome.clone())
            .await
            .expect("expired owner"),
        EconomicOutcomeReconciliationResult::ClaimLost
    ));
    let second = repository
        .claim_due(db.statement_time().await, worker, 60, 900, 1)
        .await
        .expect("same worker reacquire")[0];
    assert!(second.attempt_count > first.attempt_count);
    assert!(matches!(
        repository
            .complete_task(first, worker, outcome.clone())
            .await
            .expect("old attempt"),
        EconomicOutcomeReconciliationResult::ClaimLost
    ));
    assert_eq!(
        repository
            .retry_task(first, worker, 1, "stale retry".to_owned())
            .await
            .expect("old retry"),
        EconomicOutcomeTaskSettlement::ClaimLost
    );
    assert!(
        repository
            .find_by_id(&ids.recommendation)
            .await
            .expect("no orphan outcome")
            .is_none()
    );
    assert!(db.execute_raw(Statement::from_sql_and_values(DbBackend::Postgres,
        "UPDATE quant_economic_outcome_reconciliation_task SET status = 'completed'::qp_outcome_reconciliation_task_status, claim_owner = NULL, lease_expires_at = NULL, next_attempt_at = NULL, last_error = NULL, completed_at = statement_timestamp() WHERE recommendation_id = $1",
        [ids.recommendation.into()])).await.is_err(), "completion without its WORM outcome must fail");
    let mut shortened = RecommendationEconomicOutcomeInput::from(outcome.clone());
    shortened.source_available_until -= Duration::seconds(1);
    let shortened =
        NewRecommendationEconomicOutcome::try_seal(shortened).expect("valid shorter source prefix");
    assert!(matches!(
        repository.complete_task(second, worker, shortened).await,
        Err(StorageError::InvariantViolation { .. })
    ));
    assert!(
        repository
            .find_by_id(&ids.recommendation)
            .await
            .expect("short source did not publish")
            .is_none()
    );
    let result = repository
        .complete_task(second, worker, outcome)
        .await
        .expect("current atomic completion");
    let EconomicOutcomeReconciliationResult::Inserted(stored) = result else {
        panic!("expected atomic insert");
    };
    stored.verify().expect("database availability reseal");
    assert!(stored.available_at <= db.statement_time().await);
    assert!(stored.available_at > stored.horizon_at);
    let task = EconomicTaskEntity::find_by_id(ids.recommendation)
        .one(&db)
        .await
        .expect("completed task read")
        .expect("completed task");
    assert_eq!(task.status, OutcomeReconciliationTaskStatus::Completed);
    assert!(matches!(
        repository
            .complete_task(second, worker, stored.into())
            .await
            .expect("finished attempt"),
        EconomicOutcomeReconciliationResult::ClaimLost
    ));
}

pub async fn lease_expiry_rolls_back() {
    let (pool, _container) = setup_pg().await;
    let db = pool.connection().clone();
    let infra = Box::pin(seed_shared_demo_infra(&db)).await;
    let ids = seed_report_at(&db, &infra, db.statement_time().await - Duration::days(2))
        .await
        .expect("mid-write expiry report");
    let outcome = liquidation_outcome(&db, ids.recommendation).await;
    for statement in [
        "CREATE SEQUENCE public.economic_test_insert_count",
        "CREATE FUNCTION public.economic_test_delay() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN PERFORM nextval('public.economic_test_insert_count'); PERFORM pg_sleep(1.1); RETURN NEW; END; $$",
        "CREATE TRIGGER economic_test_delay BEFORE INSERT ON quant_recommendation_economic_outcome FOR EACH ROW EXECUTE FUNCTION public.economic_test_delay()",
    ] {
        db.execute_raw(Statement::from_string(DbBackend::Postgres, statement))
            .await
            .expect("install bounded write delay");
    }
    let repository = PgRecommendationEconomicOutcomeRepository::new(db.clone());
    let worker = WorkerId::from_v7();
    let claim = repository
        .claim_due(db.statement_time().await, worker, 1, 300, 1)
        .await
        .expect("short lease")[0];
    let result = repository
        .complete_task(claim, worker, bind_claim(outcome, claim))
        .await
        .expect("expired final write fence");
    let inserted = db
        .query_one_raw(Statement::from_string(
            DbBackend::Postgres,
            "SELECT is_called FROM public.economic_test_insert_count",
        ))
        .await
        .expect("fault-point observation")
        .expect("sequence state");
    assert!(
        inserted
            .try_get::<bool>("", "is_called")
            .expect("write fault point reached")
    );
    for statement in [
        "DROP TRIGGER economic_test_delay ON quant_recommendation_economic_outcome",
        "DROP FUNCTION public.economic_test_delay()",
        "DROP SEQUENCE public.economic_test_insert_count",
    ] {
        db.execute_raw(Statement::from_string(DbBackend::Postgres, statement))
            .await
            .expect("remove bounded write delay");
    }
    assert!(matches!(
        result,
        EconomicOutcomeReconciliationResult::ClaimLost
    ));
    assert!(
        repository
            .find_by_id(&ids.recommendation)
            .await
            .expect("rolled back WORM")
            .is_none()
    );
    let task = EconomicTaskEntity::find_by_id(ids.recommendation)
        .one(&db)
        .await
        .expect("unchanged task read")
        .expect("task");
    assert_eq!(task.status, OutcomeReconciliationTaskStatus::Delivering);
    assert!(task.completed_at.is_none());
}

pub async fn existing_outcome_completes_atomically() {
    let (pool, _container) = setup_pg().await;
    let db = pool.connection().clone();
    let infra = Box::pin(seed_shared_demo_infra(&db)).await;
    let ids = seed_report_at(&db, &infra, db.statement_time().await - Duration::days(2))
        .await
        .expect("existing outcome report");
    let repository = PgRecommendationEconomicOutcomeRepository::new(db.clone());
    let stored = repository
        .insert(liquidation_outcome(&db, ids.recommendation).await)
        .await
        .expect("historical WORM fixture");
    let worker = WorkerId::from_v7();
    let claim = repository
        .claim_due(db.statement_time().await, worker, 60, 300, 1)
        .await
        .expect("existing outcome lease")[0];
    assert!(stored.source_available_until < claim.source_available_until);
    let changed = NewRecommendationEconomicOutcome::from(stored.clone())
        .with_available_at(db.statement_time().await)
        .expect("modified availability fixture");
    assert!(matches!(
        repository.complete_task(claim, worker, changed).await,
        Err(StorageError::InvariantViolation { .. })
    ));
    let result = repository
        .complete_task(claim, worker, stored.clone().into())
        .await
        .expect("exact WORM completion");
    assert_eq!(
        result,
        EconomicOutcomeReconciliationResult::AlreadyPresent(stored)
    );
    let task = EconomicTaskEntity::find_by_id(ids.recommendation)
        .one(&db)
        .await
        .expect("completed task read")
        .expect("task");
    assert_eq!(task.status, OutcomeReconciliationTaskStatus::Completed);
}

pub async fn corrupted_resolution_rejects_claim() {
    let (pool, _container) = setup_pg().await;
    let db = pool.connection().clone();
    let infra = Box::pin(seed_shared_demo_infra(&db)).await;
    let ids = seed_report_at(
        &db,
        &infra,
        db.statement_time().await - Duration::minutes(10),
    )
    .await
    .expect("hash corruption report");
    let now = db.statement_time().await;
    resolve_report(
        &db,
        &ids,
        now - Duration::seconds(40),
        now - Duration::seconds(5),
    )
    .await;
    db.execute_raw(Statement::from_string(DbBackend::Postgres,
        "ALTER TABLE quant_recommendation_resolution_outcome DISABLE TRIGGER trg_quant_recommendation_resolution_outcome_append_only"))
        .await.expect("isolate deliberate corruption");
    let corruption = db.execute_raw(Statement::from_sql_and_values(DbBackend::Postgres,
        "UPDATE quant_recommendation_resolution_outcome SET outcome_hash = $1 WHERE recommendation_id = $2",
        [ContentHash::from_bytes([62; 32]).into(), ids.recommendation.into()])).await;
    db.execute_raw(Statement::from_string(DbBackend::Postgres,
        "ALTER TABLE quant_recommendation_resolution_outcome ENABLE TRIGGER trg_quant_recommendation_resolution_outcome_append_only"))
        .await.expect("restore WORM trigger");
    corruption.expect("corrupt projection hash");
    let repository = PgRecommendationEconomicOutcomeRepository::new(db.clone());
    assert!(matches!(
        repository
            .claim_due(db.statement_time().await, WorkerId::from_v7(), 60, 300, 1)
            .await,
        Err(StorageError::InvariantViolation { .. })
    ));
    let task = EconomicTaskEntity::find_by_id(ids.recommendation)
        .one(&db)
        .await
        .expect("unclaimed task read")
        .expect("task");
    assert_eq!(task.attempt_count, 0);
    assert!(task.claim_owner.is_none() && task.replay_until.is_none());
}

//! Durable fresh-boot projection, recovery, and lineage contracts.

use chrono::{DateTime, Duration, Utc};
use quant_pivot_error::storage::StorageError;
use quant_pivot_models::{
    domain::{
        data_plane::{
            CreateHistoryServingHeadSeal, ExchangeHistoryChunkStatus,
            ExchangeHistoryContinuityBasis, ExchangeHistoryFrontier,
            ExchangeHistoryQuarantineEvidence, ExchangeHistoryQuarantineKind, HistorySealChunkRef,
            NewExchangeHistoryChunk, NewExchangeHistoryPlan, NewExchangeHistoryQuarantine,
            NewHistoryServingHeadSeal, ResolveAcceptedHistoryRange,
        },
        quant::{
            AdvanceFreshBootRun, BlockFreshBootRun, DelayFreshBootRun, FreshBootAdvancePatch,
            FreshBootRunContract, FreshBootRunEventInfo, NewFreshBootRun, SupersedeFreshBootRun,
        },
    },
    enums::quant::{
        FreshBootBlockedReason, FreshBootEventKind, FreshBootRetryReason, FreshBootStatus,
    },
    runtime_config::BuyModelRoute,
    types::{
        ContentHash, EvmBlockHash, FreshBootRunId, HistoryServingHeadSealId,
        POOLED_BINARY_1H_BOOTSTRAP_PROFILE_ID, ResearchProfileArtifact, WorkerId,
        builtin_research_profiles,
    },
};
use quant_pivot_repository::{
    postgres::{
        PgExchangeHistoryRepository, PgFreshBootRepository, PgModelRegistryRepository,
        PgPolicyRepository,
    },
    traits::{ExchangeHistoryRepository, FreshBootRepository, PolicyRepository},
};
use quant_pivot_system_tests::{
    postgres::setup_pg, support::policy_fixtures::bootstrap_default_policy_bundle,
};
use sea_orm::{ConnectionTrait, DatabaseConnection, DbBackend, Statement, TransactionTrait};
use uuid::Uuid;

fn hash(seed: char) -> ContentHash {
    ContentHash::parse(&format!("blake3:{}", seed.to_string().repeat(64))).expect("valid hash")
}

fn block_hash(seed: char) -> EvmBlockHash {
    EvmBlockHash::parse(format!("0x{}", seed.to_string().repeat(64))).expect("valid block hash")
}

fn history_chunk(
    chunk_id: Uuid,
    from_block: i64,
    to_block: i64,
    status: ExchangeHistoryChunkStatus,
    now: DateTime<Utc>,
) -> NewExchangeHistoryChunk {
    let accepted = status == ExchangeHistoryChunkStatus::Accepted;
    NewExchangeHistoryChunk {
        chunk_id,
        frontier: ExchangeHistoryFrontier::Retention,
        from_block,
        to_block,
        status,
        attempt_count: 1,
        hypersync_count: accepted.then_some(10),
        attestor_count: accepted.then_some(10),
        hypersync_digest: accepted.then(|| hash('b')),
        attestor_digest: accepted.then(|| hash('b')),
        first_block_hash: accepted.then(|| block_hash('1')),
        last_block_hash: accepted.then(|| block_hash('2')),
        archive_height: accepted.then_some(to_block + 100),
        continuity_basis: accepted
            .then_some(ExchangeHistoryContinuityBasis::HyperSyncBoundaryHeaders),
        continuity_block: accepted.then_some(from_block - 1),
        continuity_hash: accepted.then(|| block_hash('3')),
        effective_through_at: accepted.then_some(now - Duration::minutes(1)),
        state_revision: accepted.then_some(now.timestamp_micros()),
        accepted_at: accepted.then_some(now),
        created_at: now,
        updated_at: now,
    }
}

fn pooled_profile() -> ResearchProfileArtifact {
    builtin_research_profiles()
        .expect("built-in profiles")
        .into_iter()
        .find(|profile| profile.profile_ref.id.as_str() == POOLED_BINARY_1H_BOOTSTRAP_PROFILE_ID)
        .expect("pooled fresh-boot profile")
}

fn assert_initial_timeline(events: &[FreshBootRunEventInfo], replacement_id: FreshBootRunId) {
    assert_eq!(events.len(), 5);
    for (sequence, event) in (0_i64..).zip(events) {
        assert_eq!(event.event_sequence, sequence);
    }
    assert_eq!(events[2].attempt, 1);
    assert_eq!(events[4].result_ref, Some(*replacement_id.as_uuid_ref()));
}

async fn seeded_run(
    db: &DatabaseConnection,
    plan_id: Uuid,
    supersedes_run_id: Option<FreshBootRunId>,
    now: DateTime<Utc>,
) -> NewFreshBootRun {
    let bundle = PgPolicyRepository::new(db.clone())
        .load_current_bundle()
        .await
        .expect("load policy bundle")
        .expect("seeded policy bundle");
    FreshBootRunContract {
        profile_ref: pooled_profile().profile_ref,
        route: BuyModelRoute::Pooled,
        history_plan_id: plan_id,
        history_policy_hash: hash('a'),
        history_from_block: 100,
        history_through_block: 200,
        decision_policy_snapshot_id: bundle.decision_policy_snapshot_id,
        decision_policy_snapshot_hash: bundle.snapshot_hash,
        supersedes_run_id,
    }
    .seal(now - Duration::hours(1), now)
    .expect("seal fresh-boot run")
}

pub async fn recovery_and_lineage_hold() {
    let (pool, _container) = setup_pg().await;
    let db = pool.connection().clone();
    PgModelRegistryRepository::new(db.clone())
        .ensure_builtin_research_profiles()
        .await
        .expect("seed research profiles");
    bootstrap_default_policy_bundle(
        &db,
        "fresh-boot-repository-it",
        "bootstrap fresh-boot recovery policy",
    )
    .await;
    let repo = PgFreshBootRepository::new(db.clone());
    let now = Utc::now();
    let plan_id = Uuid::new_v4();
    let initial = repo
        .create_or_load(seeded_run(&db, plan_id, None, now).await)
        .await
        .expect("create initial run");
    let worker = WorkerId::new(Uuid::new_v4());
    let claimed = repo
        .claim_due(
            worker,
            now + Duration::seconds(1),
            now + Duration::seconds(2),
            3,
        )
        .await
        .expect("claim initial run");
    assert_eq!(claimed.len(), 1);
    assert_eq!(claimed[0].revision, 1);

    let reclaimed = repo
        .claim_due(
            worker,
            now + Duration::seconds(3),
            now + Duration::seconds(4),
            3,
        )
        .await
        .expect("reclaim expired lease");
    assert_eq!(reclaimed.len(), 1);
    assert_eq!(reclaimed[0].retry_count, 1);
    assert_eq!(reclaimed[0].revision, 2);
    let recovered = PgFreshBootRepository::new(db.clone())
        .find(&initial.run_id)
        .await
        .expect("reload after restart")
        .expect("recovered run");
    assert_eq!(recovered.revision, 2);

    let blocked = repo
        .block_terminal(BlockFreshBootRun {
            run_id: initial.run_id,
            expected_revision: recovered.revision,
            reason: FreshBootBlockedReason::QualityGateFailed,
            detail: "quality gate requires governed replacement".to_owned(),
            actor: "system:fresh_boot_orchestrator".to_owned(),
            occurred_at: now + Duration::seconds(4),
        })
        .await
        .expect("block initial run");
    let illegal = repo
        .advance(AdvanceFreshBootRun {
            run_id: blocked.run_id,
            expected_revision: blocked.revision,
            event: FreshBootEventKind::SourceCoverageSatisfied,
            patch: FreshBootAdvancePatch::default(),
            evidence_hash: None,
            actor: "system:fresh_boot_orchestrator".to_owned(),
            detail: None,
            occurred_at: now + Duration::seconds(5),
        })
        .await;
    assert!(illegal.is_err(), "terminal runs must remain immutable");

    let replacement_input = seeded_run(
        &db,
        plan_id,
        Some(blocked.run_id),
        now + Duration::seconds(5),
    )
    .await;
    let replacement_id = replacement_input.run_id;
    let replacement = repo
        .supersede(
            SupersedeFreshBootRun {
                run_id: blocked.run_id,
                expected_revision: blocked.revision,
                replacement_run_id: replacement_id,
                reason: "quality issue was resolved by a governed deployment".to_owned(),
                actor: "operator.one".to_owned(),
                occurred_at: now + Duration::seconds(5),
            },
            replacement_input,
        )
        .await
        .expect("supersede initial run");
    assert_eq!(replacement.supersedes_run_id, Some(blocked.run_id));

    let claimed = repo
        .claim_due(
            worker,
            now + Duration::seconds(6),
            now + Duration::seconds(7),
            3,
        )
        .await
        .expect("claim replacement");
    assert_eq!(claimed.len(), 1);
    let blocked_replacement = repo
        .block_terminal(BlockFreshBootRun {
            run_id: replacement.run_id,
            expected_revision: claimed[0].revision,
            reason: FreshBootBlockedReason::QualityGateFailed,
            detail: "second governed replacement is required".to_owned(),
            actor: "system:fresh_boot_orchestrator".to_owned(),
            occurred_at: now + Duration::seconds(7),
        })
        .await
        .expect("block replacement");
    let next_input = seeded_run(
        &db,
        plan_id,
        Some(blocked_replacement.run_id),
        now + Duration::seconds(8),
    )
    .await;
    let next_id = next_input.run_id;
    repo.supersede(
        SupersedeFreshBootRun {
            run_id: blocked_replacement.run_id,
            expected_revision: blocked_replacement.revision,
            replacement_run_id: next_id,
            reason: "second blocker was independently resolved".to_owned(),
            actor: "operator.two".to_owned(),
            occurred_at: now + Duration::seconds(8),
        },
        next_input,
    )
    .await
    .expect("supersede replacement lineage");

    let latest = repo.list_latest().await.expect("list current runs");
    assert_eq!(latest.len(), 1);
    assert_eq!(latest[0].run_id, next_id);
    let events = repo
        .list_events(initial.run_id)
        .await
        .expect("load initial timeline");
    assert_initial_timeline(&events, replacement_id);
}

pub async fn retry_cas_holds() {
    let (pool, _container) = setup_pg().await;
    let db = pool.connection().clone();
    PgModelRegistryRepository::new(db.clone())
        .ensure_builtin_research_profiles()
        .await
        .expect("seed research profiles");
    bootstrap_default_policy_bundle(
        &db,
        "fresh-boot-repository-it",
        "bootstrap fresh-boot retry policy",
    )
    .await;
    let repo = PgFreshBootRepository::new(db.clone());
    let now = Utc::now();
    let run = repo
        .create_or_load(seeded_run(&db, Uuid::new_v4(), None, now).await)
        .await
        .expect("create retry run");
    let claimed = repo
        .claim_due(
            WorkerId::new(Uuid::new_v4()),
            now + Duration::seconds(1),
            now + Duration::seconds(10),
            1,
        )
        .await
        .expect("claim retry run");
    let delayed = repo
        .delay(DelayFreshBootRun {
            run_id: run.run_id,
            expected_revision: claimed[0].revision,
            status: FreshBootStatus::RetryScheduled,
            reason: FreshBootRetryReason::StorageTransient,
            detail: "temporary database failover".to_owned(),
            next_attempt_at: now + Duration::seconds(30),
            consume_retry: true,
            actor: "system:fresh_boot_orchestrator".to_owned(),
            occurred_at: now + Duration::seconds(2),
        })
        .await
        .expect("schedule retry");
    assert!(
        repo.retry_now(
            run.run_id,
            claimed[0].revision,
            "operator.one".to_owned(),
            "accelerate after database recovery".to_owned(),
            now + Duration::seconds(3),
        )
        .await
        .is_err(),
        "stale CAS revision must fail"
    );
    let accelerated = repo
        .retry_now(
            run.run_id,
            delayed.revision,
            "operator.one".to_owned(),
            "accelerate after database recovery".to_owned(),
            now + Duration::seconds(3),
        )
        .await
        .expect("accelerate retry");
    assert_eq!(
        accelerated.next_attempt_at,
        Some(now + Duration::seconds(3))
    );
    let claimed = repo
        .claim_due(
            WorkerId::new(Uuid::new_v4()),
            now + Duration::seconds(3),
            now + Duration::seconds(13),
            1,
        )
        .await
        .expect("claim accelerated retry");
    assert_eq!(claimed[0].status, FreshBootStatus::Running);
    assert_eq!(claimed[0].retry_count, 1);
}

pub async fn quarantine_resolution_unlocks() {
    let (pool, _container) = setup_pg().await;
    let repo = PgExchangeHistoryRepository::new(pool.connection().clone());
    let now = Utc::now();
    let quarantined_chunk_id = Uuid::new_v4();
    let quarantine_id = Uuid::new_v4();
    repo.quarantine_chunk(
        history_chunk(
            quarantined_chunk_id,
            100,
            199,
            ExchangeHistoryChunkStatus::Quarantined,
            now,
        ),
        NewExchangeHistoryQuarantine {
            quarantine_id,
            chunk_id: quarantined_chunk_id,
            kind: ExchangeHistoryQuarantineKind::ProviderMismatch,
            evidence: ExchangeHistoryQuarantineEvidence::ProviderMismatch {
                extractor_digest: hash('a'),
                attestor_digest: hash('b'),
                extractor_count: 1,
                attestor_count: 2,
            },
            evidence_hash: hash('c'),
            quarantined_at: now,
        },
    )
    .await
    .expect("quarantine divergent history chunk");

    assert!(
        repo.active_quarantine(ExchangeHistoryFrontier::Retention, 1, 99, 10)
            .await
            .expect("query non-overlapping quarantine scope")
            .is_empty()
    );
    let active = repo
        .active_quarantine(ExchangeHistoryFrontier::Retention, 150, 250, 10)
        .await
        .expect("query overlapping quarantine scope");
    assert_eq!(active.len(), 1);
    assert_eq!(active[0].quarantine_id, quarantine_id);

    let broader_chunk_id = Uuid::new_v4();
    repo.save_chunk(history_chunk(
        broader_chunk_id,
        90,
        210,
        ExchangeHistoryChunkStatus::Accepted,
        now + Duration::seconds(1),
    ))
    .await
    .expect("persist broader accepted range");
    assert!(
        repo.resolve_accepted_range(ResolveAcceptedHistoryRange {
            frontier: ExchangeHistoryFrontier::Retention,
            from_block: 100,
            to_block: 199,
            replacement_chunk_id: broader_chunk_id,
            evidence_hash: hash('d'),
            actor: "system:exchange_history_worker".to_owned(),
            resolved_at: now + Duration::seconds(2),
        })
        .await
        .is_err(),
        "accepted replacement resolution must require the exact quarantined range"
    );

    let replacement_chunk_id = Uuid::new_v4();
    repo.save_chunk(history_chunk(
        replacement_chunk_id,
        100,
        199,
        ExchangeHistoryChunkStatus::Accepted,
        now + Duration::seconds(3),
    ))
    .await
    .expect("persist exact accepted replacement");
    let resolved = repo
        .resolve_accepted_range(ResolveAcceptedHistoryRange {
            frontier: ExchangeHistoryFrontier::Retention,
            from_block: 100,
            to_block: 199,
            replacement_chunk_id,
            evidence_hash: hash('e'),
            actor: "system:exchange_history_worker".to_owned(),
            resolved_at: now + Duration::seconds(4),
        })
        .await
        .expect("resolve exact quarantined range");
    assert_eq!(resolved.len(), 1);
    assert_eq!(resolved[0].quarantine_id, quarantine_id);
    assert_eq!(resolved[0].replacement_chunk_id, replacement_chunk_id);
    assert!(
        repo.active_quarantine(ExchangeHistoryFrontier::Retention, 100, 199, 10)
            .await
            .expect("query resolved quarantine scope")
            .is_empty()
    );
    assert_eq!(
        repo.list_quarantine(ExchangeHistoryFrontier::Retention, 10)
            .await
            .expect("load immutable quarantine history")
            .len(),
        1,
        "resolution must preserve the append-only quarantine evidence"
    );
}

pub async fn serving_head_hash_holds() {
    let (pool, _container) = setup_pg().await;
    let db = pool.connection();
    let repo = PgExchangeHistoryRepository::new(db.clone());
    let now = DateTime::from_timestamp_micros(Utc::now().timestamp_micros())
        .expect("PostgreSQL microsecond fixture clock");
    let plan = repo
        .create_or_load_plan(NewExchangeHistoryPlan {
            plan_id: Uuid::now_v7(),
            chain_id: 137,
            policy_hash: hash('a'),
            bootstrap_profile_set_hash: hash('b'),
            finalized_anchor_block: 299,
            finalized_anchor_hash: block_hash('3'),
            finalized_anchor_timestamp: now.timestamp(),
            activation_from_block: 100,
            activation_through_block: 199,
            crypto_required_from_block: 100,
            weather_required_from_block: 100,
            retention_from_block: 1,
            retention_through_block: 99,
            created_at: now,
        })
        .await
        .expect("persist serving-head history plan");
    let mut chunk = history_chunk(
        Uuid::now_v7(),
        100,
        199,
        ExchangeHistoryChunkStatus::Accepted,
        now,
    );
    chunk.frontier = ExchangeHistoryFrontier::Activation;
    let chunk = repo
        .save_chunk(chunk)
        .await
        .expect("persist accepted serving chunk");
    let mut command = CreateHistoryServingHeadSeal {
        seal: NewHistoryServingHeadSeal {
            serving_head_seal_id: HistoryServingHeadSealId::from_v7(),
            seal_hash: ContentHash::from_bytes([0; 32]),
            plan_id: plan.plan_id,
            frontier: ExchangeHistoryFrontier::Activation,
            previous_seal_id: None,
            window_from_block: chunk.from_block,
            accepted_through_block: chunk.to_block,
            effective_through_at: chunk.effective_through_at.expect("accepted-through time"),
            policy_hash: plan.policy_hash,
            created_at: now,
        },
        chunks: vec![HistorySealChunkRef {
            chunk_id: chunk.chunk_id,
            frontier: chunk.frontier,
            state_revision: chunk.state_revision.expect("accepted state revision"),
            from_block: chunk.from_block,
            to_block: chunk.to_block,
        }],
    };
    command.seal.seal_hash = command.derive_hash().expect("derive creation commitment");
    let head = repo
        .create_serving_head(command)
        .await
        .expect("persist immutable serving head");
    assert_eq!(
        head.derive_hash().expect("derive loaded commitment"),
        head.seal.seal_hash
    );
    assert_eq!(
        repo.validate_serving_head(head.seal.serving_head_seal_id, head.seal.seal_hash)
            .await
            .expect("valid serving head passes read-side rehash"),
        head
    );

    // Corruption is isolated to this disposable database. No production path may
    // bypass WORM; the transaction-local setting resets before the validation read.
    let transaction = db
        .begin()
        .await
        .expect("begin isolated corruption injection");
    transaction
        .execute_raw(Statement::from_string(
            DbBackend::Postgres,
            "SET LOCAL session_replication_role = 'replica'",
        ))
        .await
        .expect("scope corruption permission to the disposable transaction");
    let changed = transaction
        .execute_raw(Statement::from_sql_and_values(
            DbBackend::Postgres,
            "UPDATE quant_history_serving_head_seal \
         SET effective_through_at = effective_through_at + interval '1 microsecond' \
         WHERE serving_head_seal_id = $1",
            [head.seal.serving_head_seal_id.as_uuid().into()],
        ))
        .await
        .expect("change only one metadata field without replacing the stored hash");
    assert_eq!(changed.rows_affected(), 1);
    transaction
        .commit()
        .await
        .expect("commit isolated corruption");
    let damaged = repo
        .latest_serving_head(ExchangeHistoryFrontier::Activation)
        .await
        .expect("load injected metadata")
        .expect("serving head still exists");
    assert_eq!(damaged.seal.seal_hash, head.seal.seal_hash);
    assert_eq!(
        damaged.seal.effective_through_at,
        head.seal.effective_through_at + Duration::microseconds(1)
    );
    assert!(matches!(
        repo.validate_serving_head(head.seal.serving_head_seal_id, head.seal.seal_hash)
            .await,
        Err(StorageError::StateConflict {
            entity: "quant_history_seal",
            ..
        })
    ));
}

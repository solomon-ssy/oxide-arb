//! Recoverable resolution/execution outcome producer contracts.

use std::sync::{
    Arc, Mutex,
    atomic::{AtomicBool, AtomicUsize, Ordering},
};

use async_trait::async_trait;
use chrono::{DateTime, Duration, TimeZone, Utc};
use quant_pivot_api::settlement::resolution::{
    FinalizedResolutionBlock, FinalizedResolutionObservation, FinalizedResolutionScan,
    FinalizedResolutionVector, ResolutionSourceReadError, ResolutionSourceReader,
};
use quant_pivot_core::{
    app::outcome_reconciliation_worker::OutcomeReconciliationWorker,
    execution::{
        OutcomeReconciliationPassConfig, OutcomeReconciliationService,
        OutcomeReconciliationServiceDeps,
    },
};
use quant_pivot_error::{
    QuantError,
    execution::ExecutionError,
    storage::{
        StorageError,
        entity::{MARKET_RESOLUTION_EVENT, QUANT_RECOMMENDATION_EXECUTION_OUTCOME},
    },
};
use quant_pivot_models::{
    clickhouse::{
        BookL2LedgerRow, BookMicrostructureRow, DomainObservationRow, MarketResolutionRow,
        MidPriceBucketRow, TradeTapeRow,
    },
    domain::{
        data_plane::{
            DomainCursorStatus, DomainSourceCheckpoint, DomainSourceCursorCasOutcome,
            UpsertDomainSourceCursor,
        },
        quant::{
            ExecutionOutcomeReconciliationResult, RecommendationExecutionOutcomeInfo,
            RecommendationExecutionReconciliationCandidate,
        },
    },
    enums::market::MarketStatus,
    hashing::CanonicalDigest,
    types::{
        ContentHash, DomainInstrumentKey, DomainSourceId, EvmAddress, EvmBlockHash,
        EvmTransactionHash, MarketId, OrderIntentId, Price, RecommendationId, TokenId,
    },
};
use quant_pivot_repository::{
    postgres::{
        PgDomainSourceCursorRepository, PgExecutionSubmissionRepository, PgMarketRepository,
        PgRecommendationExecutionOutcomeRepository, PgRecommendationResolutionOutcomeRepository,
    },
    traits::{
        DomainSourceCursorRepository, FactWriter, MarketRepository, QuantFactReadRepository,
        RecommendationExecutionOutcomeRepository, RecommendationResolutionOutcomeRepository,
    },
};
use quant_pivot_system_tests::{
    postgres::{self, setup_pg},
    support::execution_pg_seed::{
        close_position_full, seed_approved_intent, seed_report_fixture,
        seed_settlement_report_fixture,
    },
};
use rust_decimal_macros::dec;
use sea_orm::DatabaseConnection;

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn outcome_reconciliation_producer_contracts() {
    Box::pin(postgres::with_postgres_suite(async {
        crash_after_recovers_fact().await;
        missing_resolution_fact_rejects().await;
        disorder_never_advances_cursor().await;
        mismatch_never_advances_cursor().await;
        execution_backlog_reconciled_owner().await;
        execution_never_blocks_resolution().await;
        resolution_never_blocks_execution().await;
    }))
    .await
    .expect("start outcome-reconciliation producer PostgreSQL suite");
}

#[derive(Default)]
struct MemoryResolutionFacts {
    rows: Mutex<Vec<MarketResolutionRow>>,
    fail_after_next_persist: AtomicBool,
    write_attempts: AtomicUsize,
}

impl MemoryResolutionFacts {
    fn fail_after_next_persist(&self) {
        self.fail_after_next_persist.store(true, Ordering::SeqCst);
    }

    fn write_attempts(&self) -> usize {
        self.write_attempts.load(Ordering::SeqCst)
    }

    fn row_count(&self) -> usize {
        self.rows.lock().expect("lock facts").len()
    }

    fn persist(&self, rows: Vec<MarketResolutionRow>) -> Result<(), StorageError> {
        self.write_attempts.fetch_add(1, Ordering::SeqCst);
        let mut stored = self.rows.lock().expect("lock facts");
        for row in rows {
            row.validate().map_err(|error| {
                StorageError::invariant_violation(Some(MARKET_RESOLUTION_EVENT), error)
            })?;
            if let Some(existing) = stored
                .iter()
                .find(|existing| existing.source_checkpoint_hash == row.source_checkpoint_hash)
            {
                if existing != &row {
                    return Err(StorageError::state_conflict(
                        MARKET_RESOLUTION_EVENT,
                        Some(row.source_checkpoint_hash),
                        "checkpoint is already bound to different resolution content",
                    ));
                }
            } else {
                stored.push(row);
            }
        }
        drop(stored);
        if self.fail_after_next_persist.swap(false, Ordering::SeqCst) {
            return Err(StorageError::state_conflict(
                MARKET_RESOLUTION_EVENT,
                Option::<ContentHash>::None,
                "simulated acknowledgement loss after durable fact insert",
            ));
        }
        Ok(())
    }
}

#[async_trait]
impl FactWriter<MarketResolutionRow> for MemoryResolutionFacts {
    async fn write_batch(&self, rows: Vec<MarketResolutionRow>) -> Result<(), StorageError> {
        self.persist(rows)
    }

    async fn write_batch_idempotent(
        &self,
        deduplication_token: &ContentHash,
        rows: Vec<MarketResolutionRow>,
    ) -> Result<(), StorageError> {
        if rows.len() != 1 || rows[0].source_checkpoint_hash != *deduplication_token {
            return Err(StorageError::invariant_violation(
                Some(MARKET_RESOLUTION_EVENT),
                "resolution write token must equal its one source checkpoint",
            ));
        }
        self.persist(rows)
    }
}

#[async_trait]
impl QuantFactReadRepository for MemoryResolutionFacts {
    async fn microstructure_window(
        &self,
        _token_ids: Vec<TokenId>,
        _from_ms: i64,
        _to_ms: i64,
        _decision_at_ms: i64,
    ) -> Result<Vec<BookMicrostructureRow>, StorageError> {
        Ok(Vec::new())
    }

    async fn microstructure_series(
        &self,
        _token_ids: Vec<TokenId>,
        _from_ms: i64,
        _to_ms: i64,
        _available_by_ms: i64,
        _minute: bool,
    ) -> Result<Vec<BookMicrostructureRow>, StorageError> {
        Ok(Vec::new())
    }

    async fn last_trades(
        &self,
        _token_ids: Vec<TokenId>,
        _from_ms: i64,
        _to_ms: i64,
        _limit: u64,
    ) -> Result<Vec<TradeTapeRow>, StorageError> {
        Ok(Vec::new())
    }

    async fn market_tape_window(
        &self,
        _market_ids: Vec<MarketId>,
        _from_ms: i64,
        _to_ms: i64,
        _decision_at_ms: i64,
    ) -> Result<Vec<TradeTapeRow>, StorageError> {
        Ok(Vec::new())
    }

    async fn mid_price_series(
        &self,
        _token_ids: Vec<TokenId>,
        _from_ms: i64,
        _to_ms: i64,
        _decision_at_ms: i64,
        _bucket_secs: u32,
    ) -> Result<Vec<MidPriceBucketRow>, StorageError> {
        Ok(Vec::new())
    }

    async fn book_ledger_snapshot_at(
        &self,
        _token_id: &TokenId,
        _source_cutoff_ms: i64,
        _decision_at_ms: i64,
    ) -> Result<Option<BookL2LedgerRow>, StorageError> {
        Ok(None)
    }

    async fn book_ledger_snapshots_between(
        &self,
        _token_ids: Vec<TokenId>,
        _from_ms: i64,
        _to_ms: i64,
        _available_by_ms: i64,
    ) -> Result<Vec<BookL2LedgerRow>, StorageError> {
        Ok(Vec::new())
    }

    async fn resolution_at(
        &self,
        market_id: &MarketId,
        source_cutoff_ms: i64,
        decision_at_ms: i64,
    ) -> Result<Option<MarketResolutionRow>, StorageError> {
        Ok(self
            .rows
            .lock()
            .expect("lock facts")
            .iter()
            .filter(|row| {
                &row.market_id == market_id
                    && row.resolved_at <= source_cutoff_ms
                    && row.observed_at <= decision_at_ms
            })
            .max_by_key(|row| (row.resolved_at, row.observed_at, row.source_log_index))
            .cloned())
    }

    async fn resolution_by_checkpoint(
        &self,
        source_checkpoint_hash: &ContentHash,
    ) -> Result<Option<MarketResolutionRow>, StorageError> {
        let rows = self.rows.lock().expect("lock facts");
        let mut matches = rows
            .iter()
            .filter(|row| &row.source_checkpoint_hash == source_checkpoint_hash);
        let first = matches.next().cloned();
        let has_conflict = matches.any(|row| Some(row) != first.as_ref());
        drop(matches);
        drop(rows);
        if has_conflict {
            return Err(StorageError::invariant_violation(
                Some(MARKET_RESOLUTION_EVENT),
                "source checkpoint has conflicting resolution content",
            ));
        }
        Ok(first)
    }

    async fn resolution_by_market(
        &self,
        market_id: &MarketId,
    ) -> Result<Option<MarketResolutionRow>, StorageError> {
        let rows = self.rows.lock().expect("lock facts");
        let mut matches = rows.iter().filter(|row| &row.market_id == market_id);
        let first = matches.next().cloned();
        let has_conflict = matches.any(|row| Some(row) != first.as_ref());
        drop(matches);
        drop(rows);
        if has_conflict {
            return Err(StorageError::invariant_violation(
                Some(MARKET_RESOLUTION_EVENT),
                "market has conflicting resolution content",
            ));
        }
        Ok(first)
    }

    async fn resolutions_between(
        &self,
        market_ids: Vec<MarketId>,
        from_ms: i64,
        to_ms: i64,
        decision_at_ms: i64,
    ) -> Result<Vec<MarketResolutionRow>, StorageError> {
        Ok(self
            .rows
            .lock()
            .expect("lock facts")
            .iter()
            .filter(|row| {
                market_ids.contains(&row.market_id)
                    && row.resolved_at >= from_ms
                    && row.resolved_at <= to_ms
                    && row.observed_at <= decision_at_ms
            })
            .cloned()
            .collect())
    }

    async fn observed_markets_between(
        &self,
        _from_ms: i64,
        _to_ms: i64,
        _decision_at_ms: i64,
    ) -> Result<Vec<MarketId>, StorageError> {
        Ok(Vec::new())
    }

    async fn domain_observations_between(
        &self,
        _instrument_keys: Vec<DomainInstrumentKey>,
        _from_ms: i64,
        _to_ms: i64,
        _publish_cutoff_ms: i64,
        _decision_at_ms: i64,
    ) -> Result<Vec<DomainObservationRow>, StorageError> {
        Ok(Vec::new())
    }

    async fn domain_observation_at(
        &self,
        _instrument_key: &DomainInstrumentKey,
        _metric: &str,
        _source_cutoff_ms: i64,
        _decision_at_ms: i64,
    ) -> Result<Option<DomainObservationRow>, StorageError> {
        Ok(None)
    }
}

struct FailingExecutionOutcomes;

#[async_trait]
impl RecommendationExecutionOutcomeRepository for FailingExecutionOutcomes {
    async fn reconcile_intent(
        &self,
        _order_intent_id: &OrderIntentId,
    ) -> Result<ExecutionOutcomeReconciliationResult, StorageError> {
        Err(failing_execution_repository())
    }

    async fn find_by_recommendation(
        &self,
        _recommendation_id: &RecommendationId,
    ) -> Result<Option<RecommendationExecutionOutcomeInfo>, StorageError> {
        Ok(None)
    }

    async fn list_reconciliation_candidates(
        &self,
        _after: Option<OrderIntentId>,
        _limit: u64,
    ) -> Result<Vec<RecommendationExecutionReconciliationCandidate>, StorageError> {
        Err(failing_execution_repository())
    }
}

fn failing_execution_repository() -> StorageError {
    StorageError::invariant_violation(
        Some(QUANT_RECOMMENDATION_EXECUTION_OUTCOME),
        "simulated execution outcome repository failure",
    )
}

struct ScriptedResolutionSource {
    head: FinalizedResolutionBlock,
    scan: Option<FinalizedResolutionScan>,
}

#[async_trait]
impl ResolutionSourceReader for ScriptedResolutionSource {
    async fn finalized_head(&self) -> Result<FinalizedResolutionBlock, ResolutionSourceReadError> {
        Ok(self.head.clone())
    }

    async fn block_at_or_before(
        &self,
        _timestamp: DateTime<Utc>,
    ) -> Result<FinalizedResolutionBlock, ResolutionSourceReadError> {
        Ok(self.head.clone())
    }

    async fn scan_finalized(
        &self,
        from_block: u64,
        requested_to_block: u64,
    ) -> Result<Option<FinalizedResolutionScan>, ResolutionSourceReadError> {
        let Some(scan) = self.scan.as_ref() else {
            return Ok(None);
        };
        if from_block > scan.to_block {
            return Ok(None);
        }
        if from_block != scan.from_block || requested_to_block < scan.to_block {
            return Err(ResolutionSourceReadError::InvalidRange {
                from_block,
                to_block: requested_to_block,
            });
        }
        Ok(Some(scan.clone()))
    }
}

async fn crash_after_recovers_fact() {
    let (pool, _container) = setup_pg().await;
    let db = pool.connection().clone();
    let ids = seed_settlement_report_fixture(&db).await;
    settle_market(&db, &MarketId::new(&ids.market)).await;
    let block_time = fixed_time();
    seed_cursor(&db, 100, block_time).await;
    let initial_observation = observation(&ids.market, 101, block_time + Duration::seconds(1), '1');
    let source = Arc::new(ScriptedResolutionSource {
        head: block(101, block_time + Duration::seconds(1), '1'),
        scan: Some(FinalizedResolutionScan {
            from_block: 101,
            to_block: 101,
            to_block_hash: block_hash('1'),
            to_block_time: block_time + Duration::seconds(1),
            observations: vec![initial_observation],
        }),
    });
    let facts = Arc::new(MemoryResolutionFacts::default());
    facts.fail_after_next_persist();
    let reconciliation_service = service(&db, source, Arc::clone(&facts));
    let config = pass_config(block_time + Duration::minutes(1));

    assert!(matches!(
        reconciliation_service
            .run_resolution_pass(config)
            .await
            .expect_err("lost fact acknowledgement must fail the pass"),
        QuantError::Storage(StorageError::StateConflict { .. })
    ));
    assert_eq!(facts.row_count(), 1);
    assert_eq!(facts.write_attempts(), 1);
    assert_eq!(cursor_block(&db).await, 100);
    assert!(
        PgRecommendationResolutionOutcomeRepository::new(db.clone())
            .find_by_recommendation(&ids.recommendation)
            .await
            .expect("read outcome")
            .is_none()
    );

    let recovered = reconciliation_service
        .run_resolution_pass(config)
        .await
        .expect("recover durable resolution fact");
    assert_eq!(recovered.source_facts_recovered, 1);
    assert_eq!(recovered.resolution_inserted, 1);
    assert!(recovered.cursor_advanced);
    assert_eq!(facts.row_count(), 1);
    assert_eq!(facts.write_attempts(), 1);
    assert_eq!(cursor_block(&db).await, 101);
    let outcome = PgRecommendationResolutionOutcomeRepository::new(db.clone())
        .find_by_recommendation(&ids.recommendation)
        .await
        .expect("read recovered outcome")
        .expect("recovered outcome");
    assert_eq!(outcome.token_payout_ratio.inner(), dec!(0.5));

    let conflicting_service = service(
        &db,
        Arc::new(ScriptedResolutionSource {
            head: block(102, block_time + Duration::seconds(2), '2'),
            scan: Some(FinalizedResolutionScan {
                from_block: 102,
                to_block: 102,
                to_block_hash: block_hash('2'),
                to_block_time: block_time + Duration::seconds(2),
                observations: vec![observation(
                    &ids.market,
                    102,
                    block_time + Duration::seconds(2),
                    '2',
                )],
            }),
        }),
        Arc::clone(&facts),
    );
    assert!(matches!(
        conflicting_service
            .run_resolution_pass(pass_config(block_time + Duration::minutes(2)))
            .await
            .expect_err("one market cannot acquire a second resolution checkpoint"),
        QuantError::Execution(ExecutionError::OutcomeReconciliationInvariant { .. })
    ));
    assert_eq!(cursor_block(&db).await, 101);
    assert_eq!(facts.row_count(), 1);
    assert_eq!(facts.write_attempts(), 1);

    let late = seed_settlement_report_fixture(&db).await;
    settle_market(&db, &MarketId::new(&late.market)).await;
    let late_summary = reconciliation_service
        .run_resolution_pass(pass_config(block_time + Duration::minutes(2)))
        .await
        .expect("reconcile late recommendation from existing fact");
    assert_eq!(late_summary.source_observations, 0);
    assert_eq!(late_summary.resolution_inserted, 1);
    assert_eq!(facts.write_attempts(), 1);
    assert!(
        PgRecommendationResolutionOutcomeRepository::new(db)
            .find_by_recommendation(&late.recommendation)
            .await
            .expect("read late outcome")
            .is_some()
    );
}

async fn missing_resolution_fact_rejects() {
    let (pool, _container) = setup_pg().await;
    let db = pool.connection().clone();
    let ids = seed_settlement_report_fixture(&db).await;
    settle_market(&db, &MarketId::new(&ids.market)).await;
    let block_time = fixed_time();
    seed_cursor(&db, 100, block_time).await;
    let service = service(
        &db,
        Arc::new(ScriptedResolutionSource {
            head: block(100, block_time, '0'),
            scan: None,
        }),
        Arc::new(MemoryResolutionFacts::default()),
    );

    assert!(matches!(
        service
            .run_resolution_pass(pass_config(block_time + Duration::minutes(1)))
            .await
            .expect_err("terminal recommendation without source fact must fail"),
        QuantError::Execution(ExecutionError::OutcomeReconciliationInvariant { .. })
    ));
    assert_eq!(cursor_block(&db).await, 100);
    assert!(
        PgRecommendationResolutionOutcomeRepository::new(db)
            .find_by_recommendation(&ids.recommendation)
            .await
            .expect("read missing-source outcome")
            .is_none()
    );
}

async fn disorder_never_advances_cursor() {
    let (pool, _container) = setup_pg().await;
    let db = pool.connection().clone();
    let block_time = fixed_time();
    seed_cursor(&db, 100, block_time).await;
    let facts = Arc::new(MemoryResolutionFacts::default());
    let service = service(
        &db,
        Arc::new(ScriptedResolutionSource {
            head: block(102, block_time + Duration::seconds(2), '2'),
            scan: Some(FinalizedResolutionScan {
                from_block: 101,
                to_block: 102,
                to_block_hash: block_hash('2'),
                to_block_time: block_time + Duration::seconds(2),
                observations: vec![
                    observation(
                        "0xunknown-later",
                        102,
                        block_time + Duration::seconds(2),
                        'c',
                    ),
                    observation(
                        "0xunknown-earlier",
                        101,
                        block_time + Duration::seconds(1),
                        'b',
                    ),
                ],
            }),
        }),
        Arc::clone(&facts),
    );

    assert!(matches!(
        service
            .run_resolution_pass(pass_config(block_time + Duration::minutes(1)))
            .await
            .expect_err("out-of-order source scan must fail"),
        QuantError::Execution(ExecutionError::OutcomeReconciliationInvariant { .. })
    ));
    assert_eq!(cursor_block(&db).await, 100);
    assert_eq!(facts.row_count(), 0);
}

async fn execution_backlog_reconciled_owner() {
    let (pool, _container) = setup_pg().await;
    let db = pool.connection().clone();
    let ids = seed_report_fixture(&db).await;
    let submission = PgExecutionSubmissionRepository::new(db.clone());
    let intent_id = seed_approved_intent(&db, &ids).await;
    close_position_full(
        &db,
        &submission,
        &ids,
        &intent_id,
        Some(Price::new(dec!(0.66))),
    )
    .await;
    let block_time = fixed_time();
    let service = service(
        &db,
        Arc::new(ScriptedResolutionSource {
            head: block(100, block_time, '0'),
            scan: None,
        }),
        Arc::new(MemoryResolutionFacts::default()),
    );

    let summary = service
        .run_execution_pass(pass_config(block_time + Duration::minutes(1)))
        .await
        .expect("reconcile execution backlog");
    assert_eq!(summary.execution_candidates, 1);
    assert_eq!(summary.execution_inserted, 1);
    assert!(
        PgRecommendationExecutionOutcomeRepository::new(db)
            .find_by_recommendation(&ids.recommendation)
            .await
            .expect("read execution outcome")
            .is_some()
    );
}

async fn mismatch_never_advances_cursor() {
    let block_time = fixed_time();
    let duplicate_market = invalid_scan_outcome(
        FinalizedResolutionScan {
            from_block: 101,
            to_block: 102,
            to_block_hash: block_hash('2'),
            to_block_time: block_time + Duration::seconds(2),
            observations: vec![
                observation("0xduplicate", 101, block_time + Duration::seconds(1), 'a'),
                observation("0xduplicate", 102, block_time + Duration::seconds(2), 'b'),
            ],
        },
        block_time,
    )
    .await;
    let tail_hash_mismatch = invalid_scan_outcome(
        FinalizedResolutionScan {
            from_block: 101,
            to_block: 101,
            to_block_hash: block_hash('1'),
            to_block_time: block_time + Duration::seconds(1),
            observations: vec![observation(
                "0xunknown",
                101,
                block_time + Duration::seconds(1),
                'a',
            )],
        },
        block_time,
    )
    .await;
    let first_checkpoint = observation(
        "0xcheckpoint-a",
        101,
        block_time + Duration::seconds(1),
        'a',
    );
    let mut repeated_checkpoint = observation(
        "0xcheckpoint-b",
        102,
        block_time + Duration::seconds(2),
        'b',
    );
    repeated_checkpoint.source_checkpoint_hash = first_checkpoint.source_checkpoint_hash;
    let duplicate_checkpoint = invalid_scan_outcome(
        FinalizedResolutionScan {
            from_block: 101,
            to_block: 102,
            to_block_hash: block_hash('b'),
            to_block_time: block_time + Duration::seconds(2),
            observations: vec![first_checkpoint, repeated_checkpoint],
        },
        block_time,
    )
    .await;
    let source_time_regression = invalid_scan_outcome(
        FinalizedResolutionScan {
            from_block: 101,
            to_block: 101,
            to_block_hash: block_hash('a'),
            to_block_time: block_time + Duration::seconds(1),
            observations: vec![observation(
                "0xregressed-time",
                101,
                block_time - Duration::seconds(1),
                'a',
            )],
        },
        block_time,
    )
    .await;

    assert_eq!(
        (
            duplicate_market,
            tail_hash_mismatch,
            duplicate_checkpoint,
            source_time_regression,
        ),
        (
            (true, 100, 0),
            (true, 100, 0),
            (true, 100, 0),
            (true, 100, 0),
        )
    );
}

async fn invalid_scan_outcome(
    scan: FinalizedResolutionScan,
    block_time: DateTime<Utc>,
) -> (bool, u64, usize) {
    let (pool, _container) = setup_pg().await;
    let db = pool.connection().clone();
    seed_cursor(&db, 100, block_time).await;
    let facts = Arc::new(MemoryResolutionFacts::default());
    let service = service(
        &db,
        Arc::new(ScriptedResolutionSource {
            head: block(scan.to_block, scan.to_block_time, '2'),
            scan: Some(scan),
        }),
        Arc::clone(&facts),
    );
    let result = service
        .run_resolution_pass(pass_config(block_time + Duration::minutes(1)))
        .await;
    let rejected = matches!(
        result,
        Err(QuantError::Execution(
            ExecutionError::OutcomeReconciliationInvariant { .. }
        ))
    );
    (rejected, cursor_block(&db).await, facts.row_count())
}

async fn resolution_never_blocks_execution() {
    let (pool, _container) = setup_pg().await;
    let db = pool.connection().clone();
    let resolution_ids = seed_settlement_report_fixture(&db).await;
    settle_market(&db, &MarketId::new(&resolution_ids.market)).await;
    let execution_ids = seed_report_fixture(&db).await;
    let submission = PgExecutionSubmissionRepository::new(db.clone());
    let intent_id = seed_approved_intent(&db, &execution_ids).await;
    close_position_full(
        &db,
        &submission,
        &execution_ids,
        &intent_id,
        Some(Price::new(dec!(0.66))),
    )
    .await;
    let block_time = fixed_time();
    seed_cursor(&db, 100, block_time).await;
    let worker = OutcomeReconciliationWorker::new(Arc::new(service(
        &db,
        Arc::new(ScriptedResolutionSource {
            head: block(100, block_time, '0'),
            scan: None,
        }),
        Arc::new(MemoryResolutionFacts::default()),
    )));
    let config = pass_config(block_time + Duration::minutes(1));

    assert!(matches!(
        worker
            .run_once(config)
            .await
            .expect_err("resolution lane remains fail-closed"),
        QuantError::Execution(ExecutionError::OutcomeReconciliationInvariant { .. })
    ));
    let execution_outcome = PgRecommendationExecutionOutcomeRepository::new(db.clone())
        .find_by_recommendation(&execution_ids.recommendation)
        .await
        .expect("execution lane remains independently available");
    assert!(execution_outcome.is_some());
    assert!(
        PgRecommendationResolutionOutcomeRepository::new(db)
            .find_by_recommendation(&resolution_ids.recommendation)
            .await
            .expect("read missing resolution outcome")
            .is_none()
    );
}

async fn execution_never_blocks_resolution() {
    let (pool, _container) = setup_pg().await;
    let db = pool.connection().clone();
    let ids = seed_settlement_report_fixture(&db).await;
    settle_market(&db, &MarketId::new(&ids.market)).await;
    let block_time = fixed_time();
    seed_cursor(&db, 100, block_time).await;
    let facts = Arc::new(MemoryResolutionFacts::default());
    let source = Arc::new(ScriptedResolutionSource {
        head: block(101, block_time + Duration::seconds(1), '1'),
        scan: Some(FinalizedResolutionScan {
            from_block: 101,
            to_block: 101,
            to_block_hash: block_hash('1'),
            to_block_time: block_time + Duration::seconds(1),
            observations: vec![observation(
                &ids.market,
                101,
                block_time + Duration::seconds(1),
                '1',
            )],
        }),
    });
    let worker = OutcomeReconciliationWorker::new(Arc::new(service_with_execution_outcomes(
        &db,
        source,
        Arc::clone(&facts),
        Arc::new(FailingExecutionOutcomes),
    )));

    assert!(matches!(
        worker
            .run_once(pass_config(block_time + Duration::minutes(1)))
            .await
            .expect_err("execution repository failure remains fail-closed"),
        QuantError::Storage(StorageError::InvariantViolation { .. })
    ));
    assert_eq!(facts.row_count(), 1);
    assert_eq!(cursor_block(&db).await, 101);
    assert!(
        PgRecommendationResolutionOutcomeRepository::new(db)
            .find_by_recommendation(&ids.recommendation)
            .await
            .expect("read resolution outcome after execution lane failure")
            .is_some()
    );
}

fn service(
    db: &DatabaseConnection,
    source: Arc<ScriptedResolutionSource>,
    facts: Arc<MemoryResolutionFacts>,
) -> OutcomeReconciliationService {
    service_with_execution_outcomes(
        db,
        source,
        facts,
        Arc::new(PgRecommendationExecutionOutcomeRepository::new(db.clone())),
    )
}

fn service_with_execution_outcomes(
    db: &DatabaseConnection,
    source: Arc<ScriptedResolutionSource>,
    facts: Arc<MemoryResolutionFacts>,
    execution_outcomes: Arc<dyn RecommendationExecutionOutcomeRepository>,
) -> OutcomeReconciliationService {
    let resolution_source: Arc<dyn ResolutionSourceReader> = source;
    let resolution_fact_writer = Arc::clone(&facts) as Arc<dyn FactWriter<MarketResolutionRow>>;
    let resolution_facts: Arc<dyn QuantFactReadRepository> = facts;
    OutcomeReconciliationService::new(OutcomeReconciliationServiceDeps {
        resolution_source,
        resolution_fact_writer,
        resolution_facts,
        cursors: Arc::new(PgDomainSourceCursorRepository::new(db.clone())),
        markets: Arc::new(PgMarketRepository::new(db.clone())),
        resolution_outcomes: Arc::new(PgRecommendationResolutionOutcomeRepository::new(db.clone())),
        execution_outcomes,
    })
}

const fn pass_config(pass_started_at: DateTime<Utc>) -> OutcomeReconciliationPassConfig {
    OutcomeReconciliationPassConfig {
        pass_started_at,
        candidate_batch_size: 100,
        source_block_span: 32,
    }
}

async fn settle_market(db: &DatabaseConnection, market_id: &MarketId) {
    PgMarketRepository::new(db.clone())
        .update_status(market_id, MarketStatus::Settled, Some("Split"))
        .await
        .expect("settle market");
}

async fn seed_cursor(db: &DatabaseConnection, block_number: u64, block_time: DateTime<Utc>) {
    let checkpoint_json = DomainSourceCheckpoint::PolymarketCtfResolution {
        finalized_block: block_number,
        block_hash: block_hash('0'),
        block_time,
    };
    let checkpoint_hash =
        CanonicalDigest::content_hash_json(&checkpoint_json).expect("hash cursor");
    let outcome = PgDomainSourceCursorRepository::new(db.clone())
        .compare_and_set(
            None,
            UpsertDomainSourceCursor {
                source_id: DomainSourceId::polymarket_ctf_resolution(),
                instrument_key: DomainInstrumentKey::polymarket_ctf_resolution(),
                checkpoint_json,
                checkpoint_hash,
                status: DomainCursorStatus::Live,
                last_error: None,
                updated_at: Utc::now(),
            },
        )
        .await
        .expect("seed resolution cursor");
    assert!(matches!(outcome, DomainSourceCursorCasOutcome::Advanced(_)));
}

async fn cursor_block(db: &DatabaseConnection) -> u64 {
    let cursor = PgDomainSourceCursorRepository::new(db.clone())
        .find(
            &DomainSourceId::polymarket_ctf_resolution(),
            &DomainInstrumentKey::polymarket_ctf_resolution(),
        )
        .await
        .expect("read cursor")
        .expect("cursor exists");
    match cursor.checkpoint_json {
        DomainSourceCheckpoint::PolymarketCtfResolution {
            finalized_block, ..
        } => finalized_block,
        other => panic!("unexpected cursor kind: {other:?}"),
    }
}

fn observation(
    market_id: &str,
    block_number: u64,
    resolved_at: DateTime<Utc>,
    hash_seed: char,
) -> FinalizedResolutionObservation {
    FinalizedResolutionObservation {
        market_id: MarketId::new(market_id),
        vector: FinalizedResolutionVector::try_from_decimal_parts("2", ["1", "1"])
            .expect("split vector"),
        oracle: EvmAddress::parse("0x1111111111111111111111111111111111111111").expect("oracle"),
        question_id: format!("0x{}", hash_seed.to_string().repeat(64)),
        transaction_hash: transaction_hash(hash_seed),
        block_number,
        block_hash: block_hash(hash_seed),
        log_index: block_number,
        resolved_at,
        source_checkpoint_hash: content_hash(hash_seed),
    }
}

fn block(
    block_number: u64,
    block_time: DateTime<Utc>,
    hash_seed: char,
) -> FinalizedResolutionBlock {
    FinalizedResolutionBlock {
        block_number,
        block_hash: block_hash(hash_seed),
        block_time,
    }
}

fn fixed_time() -> DateTime<Utc> {
    Utc.with_ymd_and_hms(2026, 7, 24, 0, 0, 0)
        .single()
        .expect("fixed time")
}

fn content_hash(seed: char) -> ContentHash {
    ContentHash::parse(&format!("blake3:{}", seed.to_string().repeat(64))).expect("content hash")
}

fn block_hash(seed: char) -> EvmBlockHash {
    EvmBlockHash::parse(format!("0x{}", seed.to_string().repeat(64))).expect("block hash")
}

fn transaction_hash(seed: char) -> EvmTransactionHash {
    EvmTransactionHash::parse(format!("0x{}", seed.to_string().repeat(64)))
        .expect("transaction hash")
}

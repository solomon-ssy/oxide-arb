//! Recoverable resolution/execution outcome producer contracts.

use super::outcome_backfill_evidence::{
    AvailabilityEvidence, ContentHashEvidence, OutcomeBackfillEvidenceInput,
    OutcomeBackfillEvidenceManifest, PlaneCountEvidence, ProfileBindingEvidence, ReplayEvidence,
    SourceFrontierEvidence,
};
use std::{
    collections::VecDeque,
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, AtomicUsize, Ordering},
    },
    time::Duration as StdDuration,
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
        outcome_reconciliation::{
            ExecutionOutcomeBackfillSummary, ResolutionOutcomeBackfillSummary,
        },
    },
    service::recommendation_economic_outcome::{
        EconomicReplayDeferCause, RecommendationEconomicReplayAdapter,
        RecommendationEconomicReplayAttempt, RecommendationEconomicReplayBinding,
        RecommendationEconomicReplaySource,
    },
};
use quant_pivot_error::{
    QuantError,
    execution::ExecutionError,
    storage::{
        StorageError,
        entity::{MARKET_RESOLUTION_EVENT, QUANT_EXECUTION_ATTEMPT_OUTCOME},
    },
};
use quant_pivot_models::{
    clickhouse::{
        BookL2LedgerRow, BookMicrostructureRow, BookStreamSessionRow, DomainObservationRow,
        ExecutionParticipantFactRow, ExecutionParticipantRow, MarketExecutionRow,
        MarketResolutionRow, MidPriceBucketRow, QuantSignalCandidateEventRow,
    },
    domain::{
        data_plane::{
            DomainCursorStatus, DomainSourceCheckpoint, DomainSourceCursorCasOutcome,
            HistorySealChunkRef, UpsertDomainSourceCursor,
        },
        quant::{
            EconomicOutcomeCensorReason, EconomicOutcomeTaskClaim, ExecutionAttemptBarrier,
            ExecutionAttemptOutcomeInfo, ExecutionAttemptReconciliationResult,
            ExecutionAttemptTaskClaim, InsertResolutionOutcomeResult, OutcomeTaskSettlement,
            RecommendationEconomicStateDetail, RecommendationResolutionOutcomeInfo,
        },
    },
    entities::{
        quant_economic_outcome_reconciliation_task::Entity as EconomicTaskEntity,
        quant_execution_attempt_reconciliation_task::Entity as ExecutionTaskEntity,
        quant_recommendation::Entity as RecommendationEntity,
        quant_recommendation_report::Entity as ReportEntity,
        quant_report_route_run::Entity as RouteRunEntity,
    },
    enums::{
        market::MarketStatus,
        quant::{OutcomeReconciliationTaskStatus, OutcomeSide, RecommendationEconomicOutcomeState},
    },
    hashing::CanonicalDigest,
    types::{
        Bps, ContentHash, DomainInstrumentKey, DomainSourceId, EvmAddress, EvmBlockHash,
        EvmTransactionHash, MarketId, ModelVersionId, OrderIntentId, Price, RecommendationId,
        Shares, TokenId, TradePolicyReplayGap, Usd, WorkerId,
        trade_policy_evidence::TradePolicyEvidenceCoverage,
    },
};
use quant_pivot_repository::{
    postgres::{
        PgDomainSourceCursorRepository, PgExecutionAttemptOutcomeRepository,
        PgExecutionSubmissionRepository, PgMarketRepository,
        PgRecommendationEconomicOutcomeRepository, PgRecommendationExecutionRollupRepository,
        PgRecommendationResolutionOutcomeRepository, PgResolutionObservationRepository,
    },
    traits::{
        DomainSourceCursorRepository, ExecutionAttemptOutcomeRepository, FactWriter,
        MarketRepository, QuantFactReadRepository, RecommendationEconomicOutcomeRepository,
        RecommendationResolutionOutcomeRepository,
    },
};
use quant_pivot_research::policy_replay::{PolicyReplayLatency, PolicyReplayOutcome};
use quant_pivot_system_tests::{
    postgres::{self, PostgresClock, setup_pg},
    support::{
        economic_outcome_fixtures::seed_report_at,
        execution_pg_seed::{
            ExecutionTxnIds, ReportSeedConfig, SharedDemoInfra, close_position_full,
            fixture_profile_ref, seed_approved_intent, seed_intent_account_fees,
            seed_report_fixture, seed_report_on_infra, seed_settlement_report_fixture,
            seed_shared_demo_infra,
        },
    },
};
use rust_decimal_macros::dec;
use sea_orm::{ConnectionTrait, DatabaseConnection, DbBackend, EntityTrait, Statement};
use uuid::Uuid;

#[path = "outcome_reconciliation_producer/no_side.rs"]
mod no_side;

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn outcome_reconciliation_producer_contracts() {
    Box::pin(postgres::with_postgres_suite(async {
        payout_vector_boundaries().await;
        Box::pin(crash_after_recovers_fact()).await;
        missing_fact_defers().await;
        deferred_runtime_rotates_fairly().await;
        disorder_never_advances_cursor().await;
        mismatch_never_advances_cursor().await;
        execution_backlog_reconciled_owner().await;
        Box::pin(complete_backfill_replays_exactly()).await;
        execution_never_blocks_resolution().await;
        resolution_late_allows_execution().await;
    }))
    .await
    .expect("start outcome-reconciliation producer PostgreSQL suite");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn economic_reconciliation_contracts() {
    Box::pin(postgres::with_postgres_suite(async {
        Box::pin(economic_late_source_retries()).await;
        Box::pin(economic_cutoff_censors()).await;
        Box::pin(economic_replay_crash_recovers()).await;
        Box::pin(economic_future_clock_fenced()).await;
        Box::pin(economic_early_resolution_bound()).await;
        Box::pin(economic_lost_lease_recovers()).await;
        Box::pin(economic_capacity_retries_fairly()).await;
    }))
    .await
    .expect("start economic-reconciliation PostgreSQL suite");
}

#[derive(Default)]
struct MemoryResolutionFacts {
    rows: Mutex<Vec<MarketResolutionRow>>,
    books: Mutex<Vec<BookL2LedgerRow>>,
    sessions: Mutex<Vec<BookStreamSessionRow>>,
    signals: Mutex<Vec<QuantSignalCandidateEventRow>>,
    book_reads: Mutex<Vec<TokenId>>,
    fail_after_next_persist: AtomicBool,
    write_attempts: AtomicUsize,
}

struct UnexpectedEconomicReplaySource;

#[async_trait]
impl RecommendationEconomicReplaySource for UnexpectedEconomicReplaySource {
    async fn replay(
        &self,
        _claim: EconomicOutcomeTaskClaim,
        _available_through: DateTime<Utc>,
    ) -> Result<RecommendationEconomicReplayAttempt, QuantError> {
        Err(QuantError::config(
            "economic replay was not expected in this isolated lane test",
        ))
    }
}

#[derive(Clone, Copy)]
enum EconomicSourceResponse {
    CapacityDeferred,
    Deferred,
    Ready,
    ExpiringDeferred,
    ExpiringReady,
}

struct ScriptedEconomicReplaySource {
    db: DatabaseConnection,
    responses: Mutex<VecDeque<EconomicSourceResponse>>,
    claims: Mutex<Vec<EconomicOutcomeTaskClaim>>,
    calls: AtomicUsize,
}

impl ScriptedEconomicReplaySource {
    fn new(
        db: DatabaseConnection,
        responses: impl IntoIterator<Item = EconomicSourceResponse>,
    ) -> Self {
        Self {
            db,
            responses: Mutex::new(responses.into_iter().collect()),
            claims: Mutex::new(Vec::new()),
            calls: AtomicUsize::new(0),
        }
    }

    fn calls(&self) -> usize {
        self.calls.load(Ordering::SeqCst)
    }

    fn claims(&self) -> Vec<EconomicOutcomeTaskClaim> {
        self.claims.lock().expect("lock economic claims").clone()
    }

    async fn verify_capacity_retry(
        &self,
        outcomes: &dyn RecommendationEconomicOutcomeRepository,
        recommendations: [RecommendationId; 2],
        attempt: i32,
        window: (DateTime<Utc>, DateTime<Utc>),
        delay_secs: u64,
    ) -> DateTime<Utc> {
        let claims = self.claims();
        assert_eq!(
            claims.len(),
            usize::try_from(attempt).expect("attempt count")
        );
        let first = claims[0];
        let claim = *claims.last().expect("capacity claim");
        assert_eq!(claim.recommendation_id, first.recommendation_id);
        assert_eq!(claim.attempt_count, attempt);
        assert_eq!(claim.horizon_at, first.horizon_at);
        assert_eq!(claim.replay_until, first.replay_until);
        assert_eq!(claim.resolution_outcome_hash, first.resolution_outcome_hash);
        assert_eq!(claim.source_cutoff_at, first.source_cutoff_at);
        assert_eq!(claim.source_available_until, first.source_available_until);
        assert!(claim.source_cutoff_at < window.0);
        assert_eq!(claim.source_available_until, claim.source_cutoff_at);
        let delay = Duration::seconds(i64::try_from(delay_secs).expect("bounded capacity delay"));
        let mut retry_at = None;
        for id in recommendations {
            assert!(
                outcomes
                    .find_by_id(&id)
                    .await
                    .expect("busy WORM read")
                    .is_none()
            );
            let task = EconomicTaskEntity::find_by_id(id)
                .one(&self.db)
                .await
                .expect("busy task read")
                .expect("busy task exists");
            assert!(task.claim_owner.is_none() && task.lease_expires_at.is_none());
            assert!(task.completed_at.is_none());
            if id == claim.recommendation_id {
                assert_eq!(task.status, OutcomeReconciliationTaskStatus::Retrying);
                assert_eq!(task.attempt_count, attempt);
                assert_eq!(task.horizon_at, claim.horizon_at);
                assert_eq!(task.replay_until, Some(claim.replay_until));
                assert_eq!(task.resolution_outcome_hash, claim.resolution_outcome_hash);
                assert_eq!(task.source_cutoff_at, Some(claim.source_cutoff_at));
                assert_eq!(
                    task.last_error.as_deref(),
                    Some("ComputeCapacityUnavailable")
                );
                let due = task
                    .next_attempt_at
                    .expect("durable capacity retry deadline");
                assert!(
                    due >= window.0 + delay && due <= window.1 + delay,
                    "capacity attempt {attempt} must retry within its {delay_secs}s cadence: before={} after={} due={due}",
                    window.0,
                    window.1
                );
                retry_at = Some(due);
            } else {
                assert_eq!(task.status, OutcomeReconciliationTaskStatus::Pending);
                assert_eq!(task.attempt_count, 0);
                assert!(task.replay_until.is_none() && task.source_cutoff_at.is_none());
                assert!(task.resolution_outcome_hash.is_none());
                assert!(task.next_attempt_at.is_none() && task.last_error.is_none());
            }
        }
        retry_at.expect("blocked recommendation belongs to the fixture")
    }
}

#[async_trait]
impl RecommendationEconomicReplaySource for ScriptedEconomicReplaySource {
    async fn replay(
        &self,
        claim: EconomicOutcomeTaskClaim,
        available_through: DateTime<Utc>,
    ) -> Result<RecommendationEconomicReplayAttempt, QuantError> {
        self.calls.fetch_add(1, Ordering::SeqCst);
        assert_eq!(
            available_through, claim.source_available_until,
            "worker must use the DB-clamped claim cutoff"
        );
        self.claims
            .lock()
            .expect("lock economic claims")
            .push(claim);
        let recommendation = RecommendationEntity::find_by_id(claim.recommendation_id)
            .one(&self.db)
            .await?
            .expect("scripted recommendation");
        let report = ReportEntity::find_by_id(recommendation.recommendation_report_id)
            .one(&self.db)
            .await?
            .expect("scripted report");
        let route = RouteRunEntity::find_by_id(recommendation.report_route_run_id)
            .one(&self.db)
            .await?
            .expect("scripted Route run");
        let binding = RecommendationEconomicReplayBinding {
            recommendation_id: claim.recommendation_id,
            recommendation_report_id: report.recommendation_report_id,
            report_route_run_id: route.report_route_run_id,
            decision_policy_snapshot_id: report.decision_policy_snapshot_id,
            economic_tier_id: recommendation.economic_tier_id,
            model_version_id: route.model_version_id.expect("route model"),
            trade_policy_artifact_id: route.trade_policy_artifact_id.expect("route policy"),
            research_profile_artifact_id: route
                .research_profile_artifact_id
                .expect("route profile"),
            decision_at: report.decision_at,
            horizon_at: claim.horizon_at,
            replay_until: claim.replay_until,
            resolution_outcome_hash: claim.resolution_outcome_hash,
            source_cutoff_at: claim.source_cutoff_at,
            source_available_until: claim.source_available_until,
            replay_input_hash: ContentHash::from_bytes([61; 32]),
            available_at: available_through,
        };
        let response = self
            .responses
            .lock()
            .expect("lock economic responses")
            .pop_front()
            .unwrap_or(EconomicSourceResponse::Deferred);
        if matches!(
            response,
            EconomicSourceResponse::ExpiringDeferred | EconomicSourceResponse::ExpiringReady
        ) {
            expire_economic_lease(&self.db).await;
        }
        Ok(match response {
            EconomicSourceResponse::CapacityDeferred => {
                RecommendationEconomicReplayAttempt::CapacityDeferred
            }
            EconomicSourceResponse::Deferred | EconomicSourceResponse::ExpiringDeferred => {
                RecommendationEconomicReplayAttempt::Deferred {
                    binding,
                    token_id: recommendation.token_id,
                    cause: EconomicReplayDeferCause::ResolutionFactUnavailable,
                }
            }
            EconomicSourceResponse::Ready | EconomicSourceResponse::ExpiringReady => {
                RecommendationEconomicReplayAttempt::Ready {
                    binding,
                    replay: Box::new(PolicyReplayOutcome {
                        candidate_id: "economic-source-test".to_owned(),
                        outcome_side: OutcomeSide::Yes,
                        cash_budget: Usd::new(dec!(10)),
                        latency: PolicyReplayLatency {
                            base_delay_ms: 10,
                            stress_multiplier: dec!(1),
                        },
                        entry_triggered_at: None,
                        entered_at: None,
                        terminal_at: None,
                        terminal_reason: None,
                        entry_fill_ratio: dec!(0),
                        entry_fill_latency_ms: None,
                        post_fill_markout_bps: None::<Bps>,
                        exit_fill_ratio: dec!(0),
                        entry_filled_shares: Shares::ZERO,
                        exited_shares: Shares::ZERO,
                        execution_fee_usd: Usd::ZERO,
                        expected_maker_rebate_accrual_usd: Usd::ZERO,
                        expected_net_return_bps: None,
                        risk_net_return_bps: None,
                        ambiguous_touch: false,
                        full_l2_coverage: TradePolicyEvidenceCoverage::Covered,
                        fee_covered: true,
                        passive_rebate_evidence_coverage: TradePolicyEvidenceCoverage::NotRequired,
                        passive_reconciled_trade_covered: None,
                        gap: Some(TradePolicyReplayGap::EntryNotTriggered),
                        fills: Vec::new(),
                    }),
                }
            }
        })
    }
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

    async fn last_executions(
        &self,
        _token_ids: Vec<TokenId>,
        _from_ms: i64,
        _to_ms: i64,
        _limit: u64,
    ) -> Result<Vec<MarketExecutionRow>, StorageError> {
        Ok(Vec::new())
    }

    async fn market_executions_between(
        &self,
        _market_ids: Vec<MarketId>,
        _history_chunks: Vec<HistorySealChunkRef>,
        _from_ms: i64,
        _to_ms: i64,
        _decision_at_ms: i64,
    ) -> Result<Vec<MarketExecutionRow>, StorageError> {
        Ok(Vec::new())
    }

    async fn execution_participants_between(
        &self,
        _market_ids: Vec<MarketId>,
        _history_chunks: Vec<HistorySealChunkRef>,
        _from_ms: i64,
        _to_ms: i64,
        _decision_at_ms: i64,
    ) -> Result<Vec<ExecutionParticipantRow>, StorageError> {
        Ok(Vec::new())
    }

    async fn market_execution_window(
        &self,
        _market_ids: Vec<MarketId>,
        _history_chunks: Vec<HistorySealChunkRef>,
        _from_ms: i64,
        _to_ms: i64,
        _decision_at_ms: i64,
    ) -> Result<Vec<ExecutionParticipantFactRow>, StorageError> {
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
        token_id: &TokenId,
        source_cutoff_ms: i64,
        decision_at_ms: i64,
    ) -> Result<Option<BookL2LedgerRow>, StorageError> {
        self.book_reads
            .lock()
            .expect("book read trace")
            .push(token_id.clone());
        Ok(self
            .books
            .lock()
            .expect("book facts")
            .iter()
            .filter(|row| {
                &row.token_id == token_id
                    && row.venue_event_time <= source_cutoff_ms
                    && row.persisted_time <= decision_at_ms
            })
            .max_by_key(|row| (row.venue_event_time, row.token_sequence))
            .cloned())
    }

    async fn book_l2_ledger_between(
        &self,
        token_ids: Vec<TokenId>,
        from_ms: i64,
        to_ms: i64,
        available_by_ms: i64,
    ) -> Result<Vec<BookL2LedgerRow>, StorageError> {
        self.book_reads
            .lock()
            .expect("book read trace")
            .extend(token_ids.iter().cloned());
        Ok(self
            .books
            .lock()
            .expect("book facts")
            .iter()
            .filter(|row| {
                token_ids.contains(&row.token_id)
                    && row.venue_event_time >= from_ms
                    && row.venue_event_time <= to_ms
                    && row.persisted_time <= available_by_ms
            })
            .cloned()
            .collect())
    }

    async fn book_stream_sessions(
        &self,
        ids: Vec<Uuid>,
        available_by_ms: i64,
    ) -> Result<Vec<BookStreamSessionRow>, StorageError> {
        Ok(self
            .sessions
            .lock()
            .expect("session facts")
            .iter()
            .filter(|row| {
                ids.contains(&row.stream_session_id) && row.recorded_at <= available_by_ms
            })
            .cloned()
            .collect())
    }

    async fn signal_candidates_between(
        &self,
        token_id: &TokenId,
        model_id: &ModelVersionId,
        from_ms: i64,
        to_ms: i64,
        available_by_ms: i64,
    ) -> Result<Vec<QuantSignalCandidateEventRow>, StorageError> {
        Ok(self
            .signals
            .lock()
            .expect("signal facts")
            .iter()
            .filter(|row| {
                &row.token_id == token_id
                    && &row.model_version_id == model_id
                    && row.event_time >= from_ms
                    && row.event_time <= to_ms
                    && row.ingestion_time <= available_by_ms
            })
            .cloned()
            .collect())
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
impl ExecutionAttemptOutcomeRepository for FailingExecutionOutcomes {
    async fn reconcile_intent(
        &self,
        _order_intent_id: &OrderIntentId,
        _available_through: DateTime<Utc>,
    ) -> Result<ExecutionAttemptReconciliationResult, StorageError> {
        Err(failing_execution_repository())
    }

    async fn find_by_intent(
        &self,
        _order_intent_id: &OrderIntentId,
    ) -> Result<Option<ExecutionAttemptOutcomeInfo>, StorageError> {
        Ok(None)
    }

    async fn list_by_recommendations(
        &self,
        _recommendation_ids: &[RecommendationId],
        _cutoff: DateTime<Utc>,
    ) -> Result<Vec<ExecutionAttemptOutcomeInfo>, StorageError> {
        Ok(Vec::new())
    }

    async fn claim_reconciliation(
        &self,
        _available_through: DateTime<Utc>,
        _worker_id: WorkerId,
        _lease_secs: u64,
        _limit: u64,
    ) -> Result<Vec<ExecutionAttemptTaskClaim>, StorageError> {
        Err(failing_execution_repository())
    }

    async fn settle_reconciliation(
        &self,
        _order_intent_id: OrderIntentId,
        _worker_id: WorkerId,
        _settlement: OutcomeTaskSettlement,
    ) -> Result<(), StorageError> {
        Err(failing_execution_repository())
    }

    async fn barrier(
        &self,
        _cutoff: DateTime<Utc>,
    ) -> Result<ExecutionAttemptBarrier, StorageError> {
        Err(failing_execution_repository())
    }
}

fn failing_execution_repository() -> StorageError {
    StorageError::invariant_violation(
        Some(QUANT_EXECUTION_ATTEMPT_OUTCOME),
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

struct BackfillResolutionSource {
    seed: FinalizedResolutionBlock,
    target: FinalizedResolutionBlock,
    block_queries: AtomicUsize,
    scans: Mutex<VecDeque<FinalizedResolutionScan>>,
}

#[async_trait]
impl ResolutionSourceReader for BackfillResolutionSource {
    async fn finalized_head(&self) -> Result<FinalizedResolutionBlock, ResolutionSourceReadError> {
        Ok(self.target.clone())
    }

    async fn block_at_or_before(
        &self,
        _timestamp: DateTime<Utc>,
    ) -> Result<FinalizedResolutionBlock, ResolutionSourceReadError> {
        let query = self.block_queries.fetch_add(1, Ordering::SeqCst);
        Ok(if query == 1 {
            self.seed.clone()
        } else {
            self.target.clone()
        })
    }

    async fn scan_finalized(
        &self,
        from_block: u64,
        requested_to_block: u64,
    ) -> Result<Option<FinalizedResolutionScan>, ResolutionSourceReadError> {
        let mut scans = self.scans.lock().expect("lock backfill source scans");
        let Some(scan) = scans.front() else {
            return Ok(None);
        };
        if scan.from_block != from_block || scan.to_block != requested_to_block {
            return Err(ResolutionSourceReadError::InvalidRange {
                from_block,
                to_block: requested_to_block,
            });
        }
        Ok(scans.pop_front())
    }
}

async fn payout_vector_boundaries() {
    assert!(matches!(
        FinalizedResolutionVector::try_from_decimal_parts("2", ["2", "1"]),
        Err(ResolutionSourceReadError::InvalidPayoutVector { .. })
    ));
    assert!(matches!(
        FinalizedResolutionVector::try_from_decimal_parts("0", ["0", "0"]),
        Err(ResolutionSourceReadError::ConditionNotResolved)
    ));

    let (pool, _container) = setup_pg().await;
    let db = pool.connection().clone();
    let infra = seed_shared_demo_infra(&db).await;
    let block_time = fixed_time();
    let cases = [
        ("zero", ["0", "2"], dec!(0), '1'),
        ("half", ["1", "1"], dec!(0.5), '2'),
        ("one", ["2", "0"], dec!(1), '3'),
    ];
    let mut reports = Vec::with_capacity(cases.len());
    let mut observations = Vec::with_capacity(cases.len());
    for (ordinal, (name, numerators, _, hash_seed)) in cases.iter().enumerate() {
        let ids = seed_report_on_infra(
            &db,
            &infra,
            ReportSeedConfig {
                event_id: format!("payout-{name}-event"),
                market_id: format!("0xpayout-{name}-market"),
                market_question: format!("Will the {name} payout fixture settle?"),
                market_slug: format!("payout-{name}-fixture"),
                token_id: format!("{}", 50_000 + ordinal),
                trigger_key: format!("payout-vector:{name}"),
            },
        )
        .await;
        settle_market(&db, &MarketId::new(&ids.market)).await;
        let block_number = 101 + u64::try_from(ordinal).expect("payout ordinal fits u64");
        observations.push(observation(
            &ids.market,
            block_number,
            block_time
                + Duration::seconds(
                    i64::try_from(block_number - 100).expect("payout block offset fits i64"),
                ),
            *hash_seed,
            *numerators,
        ));
        reports.push(ids);
    }
    seed_cursor(&db, 100, block_time).await;
    let facts = Arc::new(MemoryResolutionFacts::default());
    let reconciliation_service = service(
        &db,
        Arc::new(ScriptedResolutionSource {
            head: block(103, block_time + Duration::seconds(3), '3'),
            scan: Some(FinalizedResolutionScan {
                from_block: 101,
                to_block: 103,
                to_block_hash: block_hash('3'),
                to_block_time: block_time + Duration::seconds(3),
                observations,
            }),
        }),
        Arc::clone(&facts),
    );
    let first_pass = reconciliation_service
        .run_resolution_pass(pass_config(db.statement_time().await))
        .await
        .expect("project exact payout boundary vectors");
    assert_eq!(first_pass.source_observations, 3);
    assert_eq!(first_pass.resolution_inserted, 0);
    assert_eq!(first_pass.resolution_deferred, 3);
    assert_eq!(facts.row_count(), 3);
    assert_eq!(cursor_block(&db).await, 103);

    release_resolution_retries(&db).await;
    let reconciliation = reconciliation_service
        .run_resolution_pass(pass_config(db.statement_time().await))
        .await
        .expect("reconcile projected payout boundary vectors");
    assert_eq!(reconciliation.resolution_inserted, 3);
    assert_eq!(reconciliation.resolution_deferred, 0);

    let outcomes = PgRecommendationResolutionOutcomeRepository::new(db);
    for (ids, (_, _, expected, _)) in reports.iter().zip(cases) {
        let outcome = outcomes
            .find_by_recommendation(&ids.recommendation)
            .await
            .expect("read payout boundary outcome")
            .expect("payout boundary outcome exists");
        assert_eq!(outcome.token_payout_ratio.inner(), expected);
    }
}

async fn release_resolution_retries(db: &DatabaseConnection) {
    db.execute_raw(Statement::from_string(
        DbBackend::Postgres,
        "UPDATE quant_resolution_outcome_reconciliation_task \
         SET next_attempt_at = statement_timestamp() + INTERVAL '1 millisecond' \
         WHERE status = 'retrying'",
    ))
    .await
    .expect("advance resolution retry schedule");
    tokio::time::sleep(StdDuration::from_millis(5)).await;
}

async fn release_economic_retries(db: &DatabaseConnection) {
    db.execute_raw(Statement::from_string(
        DbBackend::Postgres,
        "UPDATE quant_economic_outcome_reconciliation_task \
         SET next_attempt_at = statement_timestamp() + interval '1 millisecond' \
         WHERE status = 'retrying'"
            .to_owned(),
    ))
    .await
    .expect("release economic retries");
    tokio::time::sleep(StdDuration::from_millis(10)).await;
}

async fn expire_economic_lease(db: &DatabaseConnection) {
    db.execute_raw(Statement::from_string(
        DbBackend::Postgres,
        "UPDATE quant_economic_outcome_reconciliation_task \
         SET lease_expires_at = statement_timestamp() + interval '1 millisecond' \
         WHERE status = 'delivering'"
            .to_owned(),
    ))
    .await
    .expect("expire economic task lease");
    tokio::time::sleep(StdDuration::from_millis(10)).await;
}

async fn release_projection_retries(db: &DatabaseConnection) {
    db.execute_raw(Statement::from_string(
        DbBackend::Postgres,
        "UPDATE quant_resolution_observation_projection \
         SET next_attempt_at = statement_timestamp() + INTERVAL '1 millisecond' \
         WHERE status = 'retry_scheduled'",
    ))
    .await
    .expect("advance projection retry schedule");
    tokio::time::sleep(StdDuration::from_millis(5)).await;
}

async fn crash_after_recovers_fact() {
    let (pool, _container) = setup_pg().await;
    let db = pool.connection().clone();
    let ids = seed_settlement_report_fixture(&db).await;
    settle_market(&db, &MarketId::new(&ids.market)).await;
    let block_time = fixed_time();
    seed_cursor(&db, 100, block_time).await;
    let initial_observation = observation(
        &ids.market,
        101,
        block_time + Duration::seconds(1),
        '1',
        ["1", "1"],
    );
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
    let first_pass = reconciliation_service
        .run_resolution_pass(pass_config(db.statement_time().await))
        .await
        .expect("lost fact acknowledgement must enter durable retry");
    assert_eq!(first_pass.source_projection_retries, 1);
    assert_eq!(first_pass.resolution_deferred, 1);
    assert_eq!(facts.row_count(), 1);
    assert_eq!(facts.write_attempts(), 1);
    assert_eq!(cursor_block(&db).await, 101);
    assert!(
        PgRecommendationResolutionOutcomeRepository::new(db.clone())
            .find_by_recommendation(&ids.recommendation)
            .await
            .expect("read outcome")
            .is_none()
    );

    release_projection_retries(&db).await;
    release_resolution_retries(&db).await;
    let recovered = reconciliation_service
        .run_resolution_pass(pass_config(db.statement_time().await))
        .await
        .expect("recover durable resolution fact");
    assert_eq!(recovered.source_facts_recovered, 1);
    assert_eq!(recovered.resolution_inserted, 1);
    assert!(!recovered.cursor_advanced);
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
                    ["1", "1"],
                )],
            }),
        }),
        Arc::clone(&facts),
    );
    assert!(matches!(
        conflicting_service
            .run_resolution_pass(pass_config(db.statement_time().await))
            .await
            .expect_err("one market cannot acquire a second resolution checkpoint"),
        QuantError::Execution(ExecutionError::OutcomeReconciliationInvariant { .. })
    ));
    assert_eq!(cursor_block(&db).await, 102);
    assert_eq!(facts.row_count(), 1);
    assert_eq!(facts.write_attempts(), 1);

    let late = seed_settlement_report_fixture(&db).await;
    settle_market(&db, &MarketId::new(&late.market)).await;
    let late_summary = reconciliation_service
        .run_resolution_pass(pass_config(db.statement_time().await))
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

async fn missing_fact_defers() {
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

    let summary = service
        .run_resolution_pass(pass_config(db.statement_time().await))
        .await
        .expect("late canonical resolution fact must defer without fabricating a label");
    assert_eq!(summary.resolution_candidates, 1);
    assert_eq!(summary.resolution_deferred, 1);
    assert_eq!(summary.resolution_inserted, 0);
    assert_eq!(cursor_block(&db).await, 100);
    assert!(
        PgRecommendationResolutionOutcomeRepository::new(db)
            .find_by_recommendation(&ids.recommendation)
            .await
            .expect("read missing-source outcome")
            .is_none()
    );
}

async fn deferred_runtime_rotates_fairly() {
    let (pool, _container) = setup_pg().await;
    let db = pool.connection().clone();
    let infra = seed_shared_demo_infra(&db).await;
    let mut reports = Vec::new();
    for ordinal in 0..2 {
        let ids = seed_report_on_infra(
            &db,
            &infra,
            ReportSeedConfig {
                event_id: format!("runtime-fairness-event-{ordinal}"),
                market_id: format!("0xruntime-fairness-market-{ordinal}"),
                market_question: format!("Will runtime fairness case {ordinal} settle?"),
                market_slug: format!("runtime-fairness-{ordinal}"),
                token_id: format!("{}", 65_000 + ordinal),
                trigger_key: format!("runtime:fairness:{ordinal}"),
            },
        )
        .await;
        settle_market(&db, &MarketId::new(&ids.market)).await;
        reports.push(ids);
    }
    reports.sort_by_key(|ids| ids.recommendation.as_uuid());
    let block_time = fixed_time();
    seed_cursor(&db, 100, block_time).await;
    let facts = Arc::new(MemoryResolutionFacts::default());
    let reconciliation = service(
        &db,
        Arc::new(ScriptedResolutionSource {
            head: block(101, block_time + Duration::seconds(1), '1'),
            scan: Some(FinalizedResolutionScan {
                from_block: 101,
                to_block: 101,
                to_block_hash: block_hash('1'),
                to_block_time: block_time + Duration::seconds(1),
                observations: vec![observation(
                    &reports[1].market,
                    101,
                    block_time + Duration::seconds(1),
                    '1',
                    ["2", "0"],
                )],
            }),
        }),
        facts,
    );
    let first = reconciliation
        .run_resolution_pass(OutcomeReconciliationPassConfig {
            pass_started_at: db.statement_time().await,
            candidate_batch_size: 1,
            source_block_span: 1,
            economic_source_lateness_secs: 300,
            sweep_secs: 1,
        })
        .await
        .expect("defer first runtime candidate");
    assert_eq!(first.resolution_deferred, 1);
    assert_eq!(first.resolution_inserted, 0);

    let second = reconciliation
        .run_resolution_pass(OutcomeReconciliationPassConfig {
            pass_started_at: db.statement_time().await,
            candidate_batch_size: 1,
            source_block_span: 1,
            economic_source_lateness_secs: 300,
            sweep_secs: 1,
        })
        .await
        .expect("rotate to later runtime candidate");
    assert_eq!(second.resolution_deferred, 0);
    assert_eq!(second.resolution_inserted, 1);
    assert!(
        PgRecommendationResolutionOutcomeRepository::new(db.clone())
            .find_by_recommendation(&reports[1].recommendation)
            .await
            .expect("read later runtime outcome")
            .is_some(),
        "a permanently deferred first key must not starve later ready truth"
    );

    let exhausted = reconciliation
        .run_resolution_pass(OutcomeReconciliationPassConfig {
            pass_started_at: db.statement_time().await,
            candidate_batch_size: 1,
            source_block_span: 1,
            economic_source_lateness_secs: 300,
            sweep_secs: 1,
        })
        .await
        .expect("wrap exhausted runtime cursor");
    assert_eq!(exhausted.resolution_candidates, 0);
    let wrapped = reconciliation
        .run_resolution_pass(OutcomeReconciliationPassConfig {
            pass_started_at: db.statement_time().await,
            candidate_batch_size: 1,
            source_block_span: 1,
            economic_source_lateness_secs: 300,
            sweep_secs: 1,
        })
        .await
        .expect("deferred runtime candidate remains outside its due time");
    assert_eq!(wrapped.resolution_candidates, 0);

    release_resolution_retries(&db).await;
    let due = reconciliation
        .run_resolution_pass(OutcomeReconciliationPassConfig {
            pass_started_at: db.statement_time().await,
            candidate_batch_size: 1,
            source_block_span: 1,
            economic_source_lateness_secs: 300,
            sweep_secs: 1,
        })
        .await
        .expect("retry deferred runtime candidate after durable due time");
    assert_eq!(due.resolution_deferred, 1);
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
                        ["1", "1"],
                    ),
                    observation(
                        "0xunknown-earlier",
                        101,
                        block_time + Duration::seconds(1),
                        'b',
                        ["1", "1"],
                    ),
                ],
            }),
        }),
        Arc::clone(&facts),
    );

    assert!(matches!(
        service
            .run_resolution_pass(pass_config(db.statement_time().await))
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
    seed_intent_account_fees(&db, &intent_id).await;
    let block_time = fixed_time();
    let service = service(
        &db,
        Arc::new(ScriptedResolutionSource {
            head: block(100, block_time, '0'),
            scan: None,
        }),
        Arc::new(MemoryResolutionFacts::default()),
    );

    tokio::time::sleep(StdDuration::from_secs(1)).await;
    let summary = service
        .run_execution_pass(pass_config(db.statement_time().await))
        .await
        .expect("reconcile execution backlog");
    assert_eq!(summary.execution_candidates, 1);
    let task = ExecutionTaskEntity::find_by_id(intent_id)
        .one(&db)
        .await
        .expect("execution task diagnostic read")
        .expect("execution task diagnostic exists");
    assert_eq!(
        summary.execution_inserted, 1,
        "summary={summary:?}; last_error={:?}",
        task.last_error
    );
    assert!(
        PgExecutionAttemptOutcomeRepository::new(db)
            .find_by_intent(&intent_id)
            .await
            .expect("read execution outcome")
            .is_some()
    );
}

struct ResolutionBackfillProof {
    source: ResolutionOutcomeBackfillSummary,
    summary: ResolutionOutcomeBackfillSummary,
    replay: ResolutionOutcomeBackfillSummary,
    cutoff: DateTime<Utc>,
    outcomes: Vec<RecommendationResolutionOutcomeInfo>,
}

struct ExecutionBackfillProof {
    summary: ExecutionOutcomeBackfillSummary,
    replay: ExecutionOutcomeBackfillSummary,
    cutoff: DateTime<Utc>,
    outcomes: Vec<ExecutionAttemptOutcomeInfo>,
}

async fn complete_backfill_replays_exactly() {
    let (pool, _container) = setup_pg().await;
    let db = pool.connection().clone();
    let infra = seed_shared_demo_infra(&db).await;
    let source_time = fixed_time();
    let resolution_ids = seed_resolution_backfill_catalog(&db, &infra).await;
    let facts = Arc::new(MemoryResolutionFacts::default());
    let reconciliation = service(
        &db,
        Arc::new(resolution_backfill_source(&resolution_ids, source_time)),
        Arc::clone(&facts),
    );
    let resolution =
        verify_resolution_backfill(&db, &reconciliation, &facts, &resolution_ids).await;
    let execution_ids = seed_execution_backfill_catalog(&db, &infra).await;
    let execution = verify_execution_backfill(&db, &reconciliation, &execution_ids).await;
    write_backfill_evidence(&facts, &resolution, &execution);
}

async fn seed_resolution_backfill_catalog(
    db: &DatabaseConnection,
    infra: &SharedDemoInfra,
) -> Vec<ExecutionTxnIds> {
    let mut ids_by_market = Vec::new();
    for ordinal in 0..4 {
        let ids = seed_report_on_infra(
            db,
            infra,
            ReportSeedConfig {
                event_id: format!("backfill-resolution-event-{ordinal}"),
                market_id: format!("0xbackfill-resolution-market-{ordinal}"),
                market_question: format!("Will resolution backfill case {ordinal} settle?"),
                market_slug: format!("backfill-resolution-{ordinal}"),
                token_id: format!("{}", 60_000 + ordinal),
                trigger_key: format!("backfill:resolution:{ordinal}"),
            },
        )
        .await;
        settle_market(db, &MarketId::new(&ids.market)).await;
        ids_by_market.push(ids);
    }
    ids_by_market
}

fn resolution_backfill_source(
    ids: &[ExecutionTxnIds],
    source_time: DateTime<Utc>,
) -> BackfillResolutionSource {
    let scans = VecDeque::from([
        FinalizedResolutionScan {
            from_block: 101,
            to_block: 102,
            to_block_hash: block_hash('2'),
            to_block_time: source_time + Duration::seconds(2),
            observations: vec![observation(
                &ids[1].market,
                101,
                source_time + Duration::seconds(1),
                '1',
                ["2", "0"],
            )],
        },
        FinalizedResolutionScan {
            from_block: 103,
            to_block: 104,
            to_block_hash: block_hash('4'),
            to_block_time: source_time + Duration::seconds(4),
            observations: vec![
                observation(
                    &ids[2].market,
                    103,
                    source_time + Duration::seconds(3),
                    '3',
                    ["1", "1"],
                ),
                observation(
                    &ids[3].market,
                    104,
                    source_time + Duration::seconds(4),
                    '4',
                    ["0", "2"],
                ),
            ],
        },
    ]);
    BackfillResolutionSource {
        seed: block(100, source_time, '0'),
        target: block(104, source_time + Duration::seconds(4), '4'),
        block_queries: AtomicUsize::new(0),
        scans: Mutex::new(scans),
    }
}

async fn verify_resolution_backfill(
    db: &DatabaseConnection,
    reconciliation: &OutcomeReconciliationService,
    facts: &MemoryResolutionFacts,
    ids_by_market: &[ExecutionTxnIds],
) -> ResolutionBackfillProof {
    let source_config = OutcomeReconciliationPassConfig {
        pass_started_at: db.statement_time().await,
        candidate_batch_size: 1,
        source_block_span: 2,
        economic_source_lateness_secs: 300,
        sweep_secs: 1,
    };
    let source = reconciliation
        .run_resolution_backfill(source_config)
        .await
        .expect("drain frozen resolution source backfill");
    assert_eq!(source.source_start_block, 100);
    assert_eq!(source.source_target_block, 104);
    assert_eq!(source.source_pages, 2);
    assert_eq!(source.outcome_pages, 4);
    assert_eq!(source.totals.source_observations, 3);
    assert_eq!(source.totals.resolution_inserted, 0);
    assert_eq!(source.totals.resolution_deferred, 4);
    assert_eq!(facts.row_count(), 3);
    assert_eq!(cursor_block(db).await, 104);

    release_resolution_retries(db).await;
    let cutoff = db.statement_time().await;
    let config = OutcomeReconciliationPassConfig {
        pass_started_at: cutoff,
        candidate_batch_size: 1,
        source_block_span: 2,
        economic_source_lateness_secs: 300,
        sweep_secs: 1,
    };
    let summary = reconciliation
        .run_resolution_backfill(config)
        .await
        .expect("seal outcomes in the next frozen resolution window");
    assert_eq!(summary.source_start_block, 104);
    assert_eq!(summary.source_target_block, 104);
    assert_eq!(summary.source_pages, 0);
    assert_eq!(summary.outcome_pages, 4);
    assert_eq!(summary.totals.resolution_inserted, 3);
    assert_eq!(summary.totals.resolution_deferred, 1);

    let repository = PgRecommendationResolutionOutcomeRepository::new(db.clone());
    let mut outcomes = Vec::new();
    for ids in &ids_by_market[1..] {
        let stored = repository
            .find_by_recommendation(&ids.recommendation)
            .await
            .expect("read backfilled resolution outcome")
            .expect("backfilled resolution outcome exists");
        let fact = facts
            .resolution_by_market(&MarketId::new(&ids.market))
            .await
            .expect("read backfilled resolution fact")
            .expect("backfilled resolution fact exists");
        let replay = repository
            .reconcile_fact(&ids.recommendation, &fact)
            .await
            .expect("exact resolution replay");
        let InsertResolutionOutcomeResult::AlreadyPresent(replayed) = replay else {
            panic!("exact resolution replay must return AlreadyPresent");
        };
        assert_eq!(replayed, stored);
        outcomes.push(stored);
    }
    release_resolution_retries(db).await;
    let replay = reconciliation
        .run_resolution_backfill(config)
        .await
        .expect("repeat complete resolution backfill");
    assert_eq!(replay.source_pages, 0);
    assert_eq!(replay.outcome_pages, 1);
    assert_eq!(replay.totals.resolution_inserted, 0);
    assert_eq!(replay.totals.resolution_deferred, 1);
    assert_eq!(facts.row_count(), 3);
    assert!(
        repository
            .find_by_recommendation(&ids_by_market[0].recommendation)
            .await
            .expect("read unresolved recommendation outcome")
            .is_none(),
        "missing canonical resolution must never fabricate an outcome"
    );
    for (ids, expected) in ids_by_market[1..].iter().zip(&outcomes) {
        assert_eq!(
            repository
                .find_by_recommendation(&ids.recommendation)
                .await
                .expect("read exact replay resolution outcome")
                .as_ref(),
            Some(expected)
        );
    }
    ResolutionBackfillProof {
        source,
        summary,
        replay,
        cutoff,
        outcomes,
    }
}

async fn seed_execution_backfill_catalog(
    db: &DatabaseConnection,
    infra: &SharedDemoInfra,
) -> Vec<(ExecutionTxnIds, OrderIntentId)> {
    let submission = PgExecutionSubmissionRepository::new(db.clone());
    let mut ids_by_intent = Vec::new();
    for ordinal in 0..3 {
        let ids = seed_report_on_infra(
            db,
            infra,
            ReportSeedConfig {
                event_id: format!("backfill-execution-event-{ordinal}"),
                market_id: format!("0xbackfill-execution-market-{ordinal}"),
                market_question: format!("Will execution backfill case {ordinal} trade?"),
                market_slug: format!("backfill-execution-{ordinal}"),
                token_id: format!("{}", 70_000 + ordinal),
                trigger_key: format!("backfill:execution:{ordinal}"),
            },
        )
        .await;
        let intent_id = seed_approved_intent(db, &ids).await;
        close_position_full(
            db,
            &submission,
            &ids,
            &intent_id,
            Some(Price::new(dec!(0.66))),
        )
        .await;
        seed_intent_account_fees(db, &intent_id).await;
        ids_by_intent.push((ids, intent_id));
    }
    tokio::time::sleep(StdDuration::from_secs(1)).await;
    ids_by_intent
}

async fn verify_execution_backfill(
    db: &DatabaseConnection,
    reconciliation: &OutcomeReconciliationService,
    ids_by_intent: &[(ExecutionTxnIds, OrderIntentId)],
) -> ExecutionBackfillProof {
    let cutoff = db.statement_time().await;
    let config = OutcomeReconciliationPassConfig {
        pass_started_at: cutoff,
        candidate_batch_size: 1,
        source_block_span: 2,
        economic_source_lateness_secs: 300,
        sweep_secs: 1,
    };
    let summary = reconciliation
        .run_execution_backfill(config)
        .await
        .expect("drain complete frozen execution backfill");
    assert_eq!(summary.outcome_pages, 3);
    assert_eq!(summary.totals.execution_candidates, 3);
    let task_errors = ExecutionTaskEntity::find()
        .all(db)
        .await
        .expect("execution backfill task diagnostics")
        .into_iter()
        .map(|task| (task.order_intent_id, task.last_error))
        .collect::<Vec<_>>();
    assert_eq!(
        summary.totals.execution_inserted, 3,
        "summary={:?}; task_errors={task_errors:?}",
        summary.totals
    );
    assert_eq!(summary.totals.execution_deferred, 0);

    let repository = PgExecutionAttemptOutcomeRepository::new(db.clone());
    let mut outcomes = Vec::new();
    for (_ids, intent_id) in ids_by_intent {
        let stored = repository
            .find_by_intent(intent_id)
            .await
            .expect("read backfilled execution outcome")
            .expect("backfilled execution outcome exists");
        let replay = repository
            .reconcile_intent(intent_id, cutoff)
            .await
            .expect("exact execution replay");
        let ExecutionAttemptReconciliationResult::AlreadyPresent(replayed) = replay else {
            panic!("exact execution replay must return AlreadyPresent");
        };
        assert_eq!(replayed, stored);
        outcomes.push(stored);
    }
    let replay = reconciliation
        .run_execution_backfill(config)
        .await
        .expect("repeat complete execution backfill");
    assert_eq!(replay.outcome_pages, 0);
    assert_eq!(replay.totals.execution_inserted, 0);
    for ((_ids, intent_id), expected) in ids_by_intent.iter().zip(&outcomes) {
        assert_eq!(
            repository
                .find_by_intent(intent_id)
                .await
                .expect("read exact replay execution outcome")
                .as_ref(),
            Some(expected)
        );
    }
    ExecutionBackfillProof {
        summary,
        replay,
        cutoff,
        outcomes,
    }
}

fn backfill_evidence_hashes(
    facts: &MemoryResolutionFacts,
    resolution: &ResolutionBackfillProof,
    execution: &ExecutionBackfillProof,
) -> ContentHashEvidence {
    let mut resolution_fact_hashes = facts
        .rows
        .lock()
        .expect("lock resolution facts for evidence")
        .iter()
        .map(|fact| fact.resolution_fact_hash.to_string())
        .collect::<Vec<_>>();
    resolution_fact_hashes.sort();
    let mut resolution_outcome_hashes = resolution
        .outcomes
        .iter()
        .map(|outcome| outcome.outcome_hash.to_string())
        .collect::<Vec<_>>();
    resolution_outcome_hashes.sort();
    let mut execution_outcome_hashes = execution
        .outcomes
        .iter()
        .map(|outcome| outcome.outcome_hash.to_string())
        .collect::<Vec<_>>();
    execution_outcome_hashes.sort();
    let profile = fixture_profile_ref();
    let aggregate_content_hash = CanonicalDigest::content_hash_json(&(
        &resolution_fact_hashes,
        &resolution_outcome_hashes,
        &execution_outcome_hashes,
        &profile,
    ))
    .expect("hash strict outcome evidence content")
    .to_string();
    ContentHashEvidence {
        resolution_fact_hashes,
        resolution_outcome_hashes,
        execution_outcome_hashes,
        aggregate_content_hash,
    }
}

fn backfill_evidence_input(
    resolution: &ResolutionBackfillProof,
    execution: &ExecutionBackfillProof,
    content_hashes: ContentHashEvidence,
) -> OutcomeBackfillEvidenceInput {
    let resolution_rows =
        u64::try_from(resolution.outcomes.len()).expect("resolution outcome count fits u64");
    let execution_rows =
        u64::try_from(execution.outcomes.len()).expect("execution outcome count fits u64");
    let fact_rows = u64::try_from(content_hashes.resolution_fact_hashes.len())
        .expect("resolution fact count fits u64");
    let profile = fixture_profile_ref();
    OutcomeBackfillEvidenceInput {
        generated_at: Utc::now(),
        source_frontier: SourceFrontierEvidence {
            source_id: "polymarket_ctf_resolution",
            start_block: resolution.source.source_start_block,
            target_block: resolution.source.source_target_block,
            target_block_hash: resolution.source.source_target_hash.to_string(),
            target_block_time: resolution.source.source_target_time,
            source_pages: resolution.source.source_pages,
            scanned_blocks: resolution.source.source_target_block
                - resolution.source.source_start_block,
            observations: resolution.source.totals.source_observations,
            unknown_markets: resolution.source.totals.source_unknown_markets,
            conflicts: 0,
            physical_facts: fact_rows,
            logical_facts: fact_rows,
            physical_duplicates: 0,
            logical_duplicates: 0,
            revision_conflicts: 0,
            supersession_conflicts: 0,
        },
        resolution_plane: PlaneCountEvidence {
            catalog_scanned: resolution.summary.totals.resolution_candidates,
            included: resolution.summary.totals.resolution_inserted,
            excluded: 0,
            deferred: resolution.summary.totals.resolution_deferred,
            conflicts: 0,
            physical_rows: resolution_rows,
            logical_rows: resolution_rows,
            physical_duplicates: 0,
            logical_duplicates: 0,
            revision_conflicts: 0,
            supersession_conflicts: 0,
        },
        execution_plane: PlaneCountEvidence {
            catalog_scanned: execution.summary.totals.execution_candidates,
            included: execution.summary.totals.execution_inserted,
            excluded: 0,
            deferred: execution.summary.totals.execution_deferred,
            conflicts: 0,
            physical_rows: execution_rows,
            logical_rows: execution_rows,
            physical_duplicates: 0,
            logical_duplicates: 0,
            revision_conflicts: 0,
            supersession_conflicts: 0,
        },
        resolution_availability: AvailabilityEvidence {
            first_available_at: resolution
                .outcomes
                .iter()
                .map(|outcome| outcome.available_at)
                .min()
                .expect("resolution evidence is non-empty"),
            last_available_at: resolution
                .outcomes
                .iter()
                .map(|outcome| outcome.available_at)
                .max()
                .expect("resolution evidence is non-empty"),
            earliest_unchanged_after_replay: true,
        },
        execution_availability: AvailabilityEvidence {
            first_available_at: execution
                .outcomes
                .iter()
                .map(|outcome| outcome.available_at)
                .min()
                .expect("execution evidence is non-empty"),
            last_available_at: execution
                .outcomes
                .iter()
                .map(|outcome| outcome.available_at)
                .max()
                .expect("execution evidence is non-empty"),
            earliest_unchanged_after_replay: true,
        },
        profile_binding: ProfileBindingEvidence {
            research_profile_artifact_id: profile.artifact_id().to_string(),
            research_profile_id: profile.id.to_string(),
            profile_version: profile.version,
            profile_content_hash: profile.content_hash.to_string(),
            resolution_outcomes: resolution_rows,
            execution_outcomes: execution_rows,
            labels_emitted: 0,
        },
        replay: ReplayEvidence {
            resolution_cutoff: resolution.cutoff,
            execution_cutoff: execution.cutoff,
            first_resolution_inserts: resolution.summary.totals.resolution_inserted,
            first_execution_inserts: execution.summary.totals.execution_inserted,
            replay_resolution_inserts: resolution.replay.totals.resolution_inserted,
            replay_execution_inserts: execution.replay.totals.execution_inserted,
            resolution_rows_before_replay: resolution_rows,
            resolution_rows_after_replay: resolution_rows,
            execution_rows_before_replay: execution_rows,
            execution_rows_after_replay: execution_rows,
            exact_repository_results: resolution_rows + execution_rows,
        },
        content_hashes,
    }
}

fn write_backfill_evidence(
    facts: &MemoryResolutionFacts,
    resolution: &ResolutionBackfillProof,
    execution: &ExecutionBackfillProof,
) {
    let content_hashes = backfill_evidence_hashes(facts, resolution, execution);
    let artifact = OutcomeBackfillEvidenceManifest::new(backfill_evidence_input(
        resolution,
        execution,
        content_hashes,
    ))
    .write();
    assert!(artifact.path.is_file());
    assert!(ContentHash::parse(&artifact.content_hash).is_ok());
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
                observation(
                    "0xduplicate",
                    101,
                    block_time + Duration::seconds(1),
                    'a',
                    ["1", "1"],
                ),
                observation(
                    "0xduplicate",
                    102,
                    block_time + Duration::seconds(2),
                    'b',
                    ["1", "1"],
                ),
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
                ["1", "1"],
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
        ["1", "1"],
    );
    let mut repeated_checkpoint = observation(
        "0xcheckpoint-b",
        102,
        block_time + Duration::seconds(2),
        'b',
        ["1", "1"],
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
                ["1", "1"],
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
        .run_resolution_pass(pass_config(db.statement_time().await))
        .await;
    let rejected = matches!(
        result,
        Err(QuantError::Execution(
            ExecutionError::OutcomeReconciliationInvariant { .. }
        ))
    );
    (rejected, cursor_block(&db).await, facts.row_count())
}

async fn resolution_late_allows_execution() {
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
    seed_intent_account_fees(&db, &intent_id).await;
    tokio::time::sleep(StdDuration::from_secs(1)).await;
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
    let config = pass_config(db.statement_time().await);

    worker
        .run_once(config)
        .await
        .expect("late resolution remains deferred while execution truth is sealed");
    let execution_outcome = PgExecutionAttemptOutcomeRepository::new(db.clone())
        .find_by_intent(&intent_id)
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

async fn economic_report(db: &DatabaseConnection, horizon_age: Duration) -> ExecutionTxnIds {
    let infra = seed_shared_demo_infra(db).await;
    let profile = fixture_profile_ref()
        .resolve_builtin_research_profile()
        .expect("economic fixture profile");
    let horizon = Duration::seconds(
        i64::try_from(profile.spec.target_horizon_secs).expect("economic fixture horizon"),
    );
    let decision_at = db.statement_time().await - horizon - horizon_age;
    seed_report_at(db, &infra, decision_at)
        .await
        .expect("publish historical economic report")
}

async fn economic_late_source_retries() {
    let (pool, _container) = setup_pg().await;
    let db = pool.connection().clone();
    let ids = economic_report(&db, Duration::seconds(60)).await;
    let task = EconomicTaskEntity::find_by_id(ids.recommendation)
        .one(&db)
        .await
        .expect("economic task read")
        .expect("economic task exists");
    let economic_outcomes = Arc::new(PgRecommendationEconomicOutcomeRepository::new(db.clone()))
        as Arc<dyn RecommendationEconomicOutcomeRepository>;
    let economic_source = Arc::new(ScriptedEconomicReplaySource::new(
        db.clone(),
        [
            EconomicSourceResponse::Deferred,
            EconomicSourceResponse::Ready,
        ],
    ));
    let service = service_with_economic_source(
        &db,
        Arc::new(ScriptedResolutionSource {
            head: block(1, fixed_time(), 'e'),
            scan: None,
        }),
        Arc::new(MemoryResolutionFacts::default()),
        Arc::new(PgExecutionAttemptOutcomeRepository::new(db.clone())),
        Arc::clone(&economic_outcomes),
        Arc::clone(&economic_source) as Arc<dyn RecommendationEconomicReplaySource>,
    );
    let first = service
        .run_economic_pass(pass_config(db.statement_time().await))
        .await
        .expect("defer incomplete economic source");
    assert_eq!(first.economic_deferred, 1);
    let deferred = EconomicTaskEntity::find_by_id(ids.recommendation)
        .one(&db)
        .await
        .expect("read deferred diagnostic")
        .expect("deferred task exists");
    let recommendation = RecommendationEntity::find_by_id(ids.recommendation)
        .one(&db)
        .await
        .expect("read diagnostic recommendation")
        .expect("diagnostic recommendation exists");
    let claim = economic_source
        .claims()
        .first()
        .copied()
        .expect("source claim");
    let detail = deferred
        .last_error
        .as_deref()
        .expect("durable deferred cause");
    for expected in [
        "SourceIncompleteBeforeCutoff".to_owned(),
        format!("recommendation_id={}", ids.recommendation),
        format!("token_id={}", recommendation.token_id),
        format!("replay_until={}", claim.replay_until),
        format!("source_cutoff_at={}", claim.source_cutoff_at),
        format!("source_available_until={}", claim.source_available_until),
        "cause=ResolutionFactUnavailable".to_owned(),
    ] {
        assert!(
            detail.contains(&expected),
            "missing durable {expected}: {detail}"
        );
    }
    assert!(detail.chars().count() <= 4_096);
    assert!(
        economic_outcomes
            .find_by_id(&ids.recommendation)
            .await
            .expect("economic outcome after defer")
            .is_none()
    );
    release_economic_retries(&db).await;
    let second = service
        .run_economic_pass(pass_config(db.statement_time().await))
        .await
        .expect("late economic source recovers before cutoff");
    assert_eq!(second.economic_inserted, 1);
    let outcome = economic_outcomes
        .find_by_id(&ids.recommendation)
        .await
        .expect("economic outcome after late source")
        .expect("late source seals outcome");
    assert_eq!(
        outcome.state,
        RecommendationEconomicOutcomeState::EntryNotTriggered
    );
    assert_eq!(economic_source.calls(), 2);
    let claims = economic_source.claims();
    assert!(
        claims
            .iter()
            .all(|claim| claim.replay_until == task.horizon_at
                && claim.resolution_outcome_hash.is_none()
                && claim.source_available_until >= claim.replay_until
                && claim.source_available_until < claim.source_cutoff_at)
    );
    assert_eq!(claims[0].source_cutoff_at, claims[1].source_cutoff_at);
    assert_eq!(claims[1].attempt_count, claims[0].attempt_count + 1);
    assert!(outcome.available_at >= claims[1].source_available_until);
    assert!(outcome.available_at <= db.statement_time().await);
}

async fn economic_cutoff_censors() {
    let (pool, _container) = setup_pg().await;
    let db = pool.connection().clone();
    let ids = economic_report(&db, Duration::seconds(600)).await;
    let task = EconomicTaskEntity::find_by_id(ids.recommendation)
        .one(&db)
        .await
        .expect("economic task read")
        .expect("economic task exists");
    let economic_outcomes = Arc::new(PgRecommendationEconomicOutcomeRepository::new(db.clone()))
        as Arc<dyn RecommendationEconomicOutcomeRepository>;
    let economic_source = Arc::new(ScriptedEconomicReplaySource::new(
        db.clone(),
        [EconomicSourceResponse::Deferred],
    ));
    let service = service_with_economic_source(
        &db,
        Arc::new(ScriptedResolutionSource {
            head: block(1, fixed_time(), 'f'),
            scan: None,
        }),
        Arc::new(MemoryResolutionFacts::default()),
        Arc::new(PgExecutionAttemptOutcomeRepository::new(db.clone())),
        Arc::clone(&economic_outcomes),
        Arc::clone(&economic_source) as Arc<dyn RecommendationEconomicReplaySource>,
    );
    let cutoff = task.horizon_at + Duration::seconds(300);
    assert!(cutoff < db.statement_time().await);
    let summary = service
        .run_economic_pass(pass_config(db.statement_time().await))
        .await
        .expect("censor source at frozen cutoff");
    assert_eq!(summary.economic_censored, 1);
    let outcome = economic_outcomes
        .find_by_id(&ids.recommendation)
        .await
        .expect("censored economic outcome read")
        .expect("cutoff seals censored outcome");
    assert_eq!(outcome.state, RecommendationEconomicOutcomeState::Censored);
    assert!(matches!(
        outcome.payload_json.detail,
        RecommendationEconomicStateDetail::Censored {
            reason: EconomicOutcomeCensorReason::SourceUnavailable,
            ..
        }
    ));
    assert_eq!(economic_source.claims()[0].source_available_until, cutoff);
    assert_eq!(outcome.source_available_until, cutoff);
    assert!(outcome.available_at >= cutoff);
}

async fn economic_replay_crash_recovers() {
    let (pool, _container) = setup_pg().await;
    let db = pool.connection().clone();
    let ids = economic_report(&db, Duration::seconds(60)).await;
    let task = EconomicTaskEntity::find_by_id(ids.recommendation)
        .one(&db)
        .await
        .expect("economic task read")
        .expect("economic task exists");
    let economic_outcomes = Arc::new(PgRecommendationEconomicOutcomeRepository::new(db.clone()))
        as Arc<dyn RecommendationEconomicOutcomeRepository>;
    let economic_source = Arc::new(ScriptedEconomicReplaySource::new(
        db.clone(),
        [EconomicSourceResponse::Ready, EconomicSourceResponse::Ready],
    ));
    let crashed_worker = WorkerId::from_v7();
    let claim = economic_outcomes
        .claim_due(db.statement_time().await, crashed_worker, 60, 300, 1)
        .await
        .expect("claim economic task before simulated crash")
        .pop()
        .expect("economic claim exists");
    let attempt = economic_source
        .replay(claim, claim.source_available_until)
        .await
        .expect("build economic outcome before simulated crash");
    let RecommendationEconomicReplayAttempt::Ready { binding, replay } = attempt else {
        panic!("scripted source must be ready before simulated crash");
    };
    let uncommitted = RecommendationEconomicReplayAdapter::adapt(binding, &replay)
        .expect("adapt outcome before crash, without publishing it");
    assert_eq!(uncommitted.recommendation_id, ids.recommendation);
    assert!(
        economic_outcomes
            .find_by_id(&ids.recommendation)
            .await
            .expect("read uncommitted outcome")
            .is_none()
    );
    expire_economic_lease(&db).await;
    let calls_before_recovery = economic_source.calls();
    let service = service_with_economic_source(
        &db,
        Arc::new(ScriptedResolutionSource {
            head: block(1, fixed_time(), '9'),
            scan: None,
        }),
        Arc::new(MemoryResolutionFacts::default()),
        Arc::new(PgExecutionAttemptOutcomeRepository::new(db.clone())),
        Arc::clone(&economic_outcomes),
        Arc::clone(&economic_source) as Arc<dyn RecommendationEconomicReplaySource>,
    );
    let summary = service
        .run_economic_pass(pass_config(db.statement_time().await))
        .await
        .expect("recover task after pre-commit replay crash");
    assert_eq!(summary.economic_inserted, 1);
    assert_eq!(
        economic_source.calls(),
        calls_before_recovery + 1,
        "an uncommitted replay must be recomputed by the new lease owner",
    );
    let recovered = EconomicTaskEntity::find_by_id(ids.recommendation)
        .one(&db)
        .await
        .expect("read recovered task")
        .expect("recovered task exists");
    assert_eq!(recovered.status, OutcomeReconciliationTaskStatus::Completed);
    assert_eq!(recovered.attempt_count, claim.attempt_count + 1);
    assert_eq!(recovered.replay_until, Some(task.horizon_at));
    assert_eq!(recovered.source_cutoff_at, Some(claim.source_cutoff_at));
    assert!(recovered.claim_owner.is_none() && recovered.lease_expires_at.is_none());
}

async fn economic_future_clock_fenced() {
    let (pool, _container) = setup_pg().await;
    let db = pool.connection().clone();
    let ids = seed_report_fixture(&db).await;
    let task = EconomicTaskEntity::find_by_id(ids.recommendation)
        .one(&db)
        .await
        .expect("future task read")
        .expect("future task exists");
    assert!(task.horizon_at > db.statement_time().await);
    let outcomes = Arc::new(PgRecommendationEconomicOutcomeRepository::new(db.clone()))
        as Arc<dyn RecommendationEconomicOutcomeRepository>;
    let source = Arc::new(ScriptedEconomicReplaySource::new(
        db.clone(),
        [EconomicSourceResponse::Ready],
    ));
    let service = service_with_economic_source(
        &db,
        Arc::new(ScriptedResolutionSource {
            head: block(1, fixed_time(), 'a'),
            scan: None,
        }),
        Arc::new(MemoryResolutionFacts::default()),
        Arc::new(PgExecutionAttemptOutcomeRepository::new(db.clone())),
        Arc::clone(&outcomes),
        Arc::clone(&source) as Arc<dyn RecommendationEconomicReplaySource>,
    );
    let summary = service
        .run_economic_pass(pass_config(task.horizon_at + Duration::hours(1)))
        .await
        .expect("future caller clock cannot make a task mature");
    assert_eq!(summary.economic_candidates, 0);
    assert_eq!(source.calls(), 0);
    assert!(
        outcomes
            .find_by_id(&ids.recommendation)
            .await
            .expect("future outcome read")
            .is_none()
    );
    let untouched = EconomicTaskEntity::find_by_id(ids.recommendation)
        .one(&db)
        .await
        .expect("untouched future task read")
        .expect("future task remains");
    assert_eq!(untouched.status, OutcomeReconciliationTaskStatus::Pending);
    assert_eq!(untouched.attempt_count, 0);
    assert!(untouched.replay_until.is_none() && untouched.source_cutoff_at.is_none());
}

async fn economic_early_resolution_bound() {
    let (pool, _container) = setup_pg().await;
    let db = pool.connection().clone();
    let infra = seed_shared_demo_infra(&db).await;
    let decision_at = db.statement_time().await - Duration::hours(1);
    let ids = seed_report_at(&db, &infra, decision_at)
        .await
        .expect("early-resolution historical report");
    let task = EconomicTaskEntity::find_by_id(ids.recommendation)
        .one(&db)
        .await
        .expect("early task read")
        .expect("early task exists");
    let resolved_at = db.statement_time().await - Duration::minutes(30);
    assert!(ids.decision_at < resolved_at && resolved_at < task.horizon_at);
    settle_market(&db, &MarketId::new(&ids.market)).await;
    seed_cursor(&db, 100, resolved_at - Duration::seconds(1)).await;
    let outcomes = Arc::new(PgRecommendationEconomicOutcomeRepository::new(db.clone()))
        as Arc<dyn RecommendationEconomicOutcomeRepository>;
    let source = Arc::new(ScriptedEconomicReplaySource::new(
        db.clone(),
        [EconomicSourceResponse::Ready],
    ));
    let facts = Arc::new(MemoryResolutionFacts::default());
    let service = service_with_economic_source(
        &db,
        Arc::new(ScriptedResolutionSource {
            head: block(101, resolved_at + Duration::seconds(1), 'b'),
            scan: Some(FinalizedResolutionScan {
                from_block: 101,
                to_block: 101,
                to_block_hash: block_hash('b'),
                to_block_time: resolved_at + Duration::seconds(1),
                observations: vec![observation(&ids.market, 101, resolved_at, 'b', ["2", "0"])],
            }),
        }),
        Arc::clone(&facts),
        Arc::new(PgExecutionAttemptOutcomeRepository::new(db.clone())),
        Arc::clone(&outcomes),
        Arc::clone(&source) as Arc<dyn RecommendationEconomicReplaySource>,
    );
    let before = service
        .run_economic_pass(pass_config(db.statement_time().await))
        .await
        .expect("settled catalog alone cannot authorize early economic replay");
    assert_eq!(before.economic_candidates, 0);
    assert_eq!(source.calls(), 0);
    service
        .run_resolution_pass(pass_config(db.statement_time().await))
        .await
        .expect("project canonical resolution source");
    release_resolution_retries(&db).await;
    let projected = service
        .run_resolution_pass(pass_config(db.statement_time().await))
        .await
        .expect("seal canonical recommendation resolution");
    assert_eq!(projected.resolution_inserted, 1);
    assert_eq!(facts.row_count(), 1);
    let resolution = PgRecommendationResolutionOutcomeRepository::new(db.clone())
        .find_by_recommendation(&ids.recommendation)
        .await
        .expect("canonical resolution read")
        .expect("canonical resolution exists");
    let context = outcomes
        .replay_context(&ids.recommendation)
        .await
        .expect("early replay frozen feature context");
    let lag =
        Duration::from_std(context.decision_boundary.knowledge_lag()).expect("boundary lag range");
    let replay_until = (resolution.resolved_at + lag).max(resolution.source_observed_at);
    let summary = service
        .run_economic_pass(pass_config(db.statement_time().await))
        .await
        .expect("canonical resolution permits a bounded early replay");
    assert_eq!(summary.economic_inserted, 1);
    let claim = source.claims()[0];
    assert_eq!(claim.horizon_at, task.horizon_at);
    assert_eq!(claim.replay_until, replay_until);
    assert!(claim.replay_until < claim.horizon_at);
    assert_eq!(claim.resolution_outcome_hash, Some(resolution.outcome_hash));
    assert_eq!(
        claim.source_cutoff_at,
        replay_until.max(resolution.available_at) + Duration::seconds(300)
    );
    let outcome = outcomes
        .find_by_id(&ids.recommendation)
        .await
        .expect("early outcome read")
        .expect("early outcome exists");
    assert_eq!(
        outcome.horizon_at, task.horizon_at,
        "early completion never shortens the profile horizon"
    );
    assert!(outcome.available_at < task.horizon_at);
    assert!(outcome.available_at >= claim.source_available_until);
}

async fn economic_lost_lease_recovers() {
    for first_response in [
        EconomicSourceResponse::ExpiringReady,
        EconomicSourceResponse::ExpiringDeferred,
    ] {
        let (pool, _container) = setup_pg().await;
        let db = pool.connection().clone();
        let ids = economic_report(&db, Duration::seconds(60)).await;
        let outcomes = Arc::new(PgRecommendationEconomicOutcomeRepository::new(db.clone()))
            as Arc<dyn RecommendationEconomicOutcomeRepository>;
        let source = Arc::new(ScriptedEconomicReplaySource::new(
            db.clone(),
            [first_response, EconomicSourceResponse::Ready],
        ));
        let service = service_with_economic_source(
            &db,
            Arc::new(ScriptedResolutionSource {
                head: block(1, fixed_time(), 'c'),
                scan: None,
            }),
            Arc::new(MemoryResolutionFacts::default()),
            Arc::new(PgExecutionAttemptOutcomeRepository::new(db.clone())),
            Arc::clone(&outcomes),
            Arc::clone(&source) as Arc<dyn RecommendationEconomicReplaySource>,
        );
        let mut config = pass_config(db.statement_time().await);
        config.candidate_batch_size = 1;
        let first = service
            .run_economic_pass(config)
            .await
            .expect("expired worker loses complete/retry authority");
        assert_eq!(first.economic_inserted, 0);
        assert_eq!(first.economic_censored, 0);
        assert_eq!(first.economic_claim_lost, 1);
        assert_eq!(first.economic_deferred, 0);
        assert!(
            outcomes
                .find_by_id(&ids.recommendation)
                .await
                .expect("lost-lease outcome read")
                .is_none()
        );
        let lost = EconomicTaskEntity::find_by_id(ids.recommendation)
            .one(&db)
            .await
            .expect("lost lease task read")
            .expect("lost lease task exists");
        assert_eq!(lost.status, OutcomeReconciliationTaskStatus::Delivering);
        assert_eq!(lost.attempt_count, 1);
        let database_now = db.statement_time().await;
        assert!(
            lost.lease_expires_at
                .is_some_and(|until| until <= database_now)
        );
        assert!(
            lost.next_attempt_at.is_none(),
            "stale deferred worker must not schedule a retry"
        );
        config.pass_started_at = db.statement_time().await;
        let recovered = service
            .run_economic_pass(config)
            .await
            .expect("new live attempt recovers expired work");
        assert_eq!(recovered.economic_inserted, 1);
        assert_eq!(source.calls(), 2);
        let claims = source.claims();
        assert_eq!(claims[1].attempt_count, claims[0].attempt_count + 1);
        assert_eq!(claims[1].replay_until, claims[0].replay_until);
        assert_eq!(claims[1].source_cutoff_at, claims[0].source_cutoff_at);
    }
}

async fn economic_capacity_retries_fairly() {
    let (pool, _container) = setup_pg().await;
    let db = pool.connection().clone();
    let infra = seed_shared_demo_infra(&db).await;
    let profile = fixture_profile_ref()
        .resolve_builtin_research_profile()
        .expect("capacity fixture profile");
    let horizon = Duration::seconds(
        i64::try_from(profile.spec.target_horizon_secs).expect("capacity fixture horizon"),
    );
    let decision_at = db.statement_time().await - horizon - Duration::seconds(600);
    let first = seed_report_at(&db, &infra, decision_at)
        .await
        .expect("first mature report");
    let second = seed_report_at(&db, &infra, decision_at + Duration::seconds(1))
        .await
        .expect("second mature report");
    let outcomes = Arc::new(PgRecommendationEconomicOutcomeRepository::new(db.clone()))
        as Arc<dyn RecommendationEconomicOutcomeRepository>;
    let source = Arc::new(ScriptedEconomicReplaySource::new(
        db.clone(),
        [EconomicSourceResponse::CapacityDeferred; 8]
            .into_iter()
            .chain([EconomicSourceResponse::Ready; 2]),
    ));
    let service = service_with_economic_source(
        &db,
        Arc::new(ScriptedResolutionSource {
            head: block(1, fixed_time(), 'd'),
            scan: None,
        }),
        Arc::new(MemoryResolutionFacts::default()),
        Arc::new(PgExecutionAttemptOutcomeRepository::new(db.clone())),
        Arc::clone(&outcomes),
        Arc::clone(&source) as Arc<dyn RecommendationEconomicReplaySource>,
    );
    let mut invalid = pass_config(db.statement_time().await);
    invalid.sweep_secs = 0;
    let error = service
        .run_economic_pass(invalid)
        .await
        .expect_err("zero capacity cadence must fail before any source claim");
    assert!(matches!(
        error,
        QuantError::Execution(ExecutionError::OutcomeReconciliationInvariant { .. })
    ));
    assert_eq!(source.calls(), 0);
    let cadences = [1, 2, 60, u64::MAX, 1, 1, 1, 1];
    let mut final_due = None;
    for (index, sweep_secs) in cadences.into_iter().enumerate() {
        let before = db.statement_time().await;
        let mut config = pass_config(before);
        config.sweep_secs = sweep_secs;
        let busy = service
            .run_economic_pass(config)
            .await
            .expect("compute saturation must durably defer even after source cutoff");
        let after = db.statement_time().await;
        assert_eq!(
            busy.economic_candidates, 1,
            "busy pass must stop claiming further work"
        );
        assert_eq!(busy.economic_capacity_deferred, 1);
        assert_eq!(busy.economic_inserted, 0);
        assert_eq!(busy.economic_censored, 0);
        assert_eq!(source.calls(), index + 1);
        final_due = Some(
            source
                .verify_capacity_retry(
                    outcomes.as_ref(),
                    [first.recommendation, second.recommendation],
                    i32::try_from(index + 1).expect("bounded capacity attempt"),
                    (before, after),
                    sweep_secs.min(60),
                )
                .await,
        );
        if index + 1 < cadences.len() {
            // Accelerate test preparation only after checking the real durable delay.
            release_economic_retries(&db).await;
        }
    }
    let final_due = final_due.expect("eighth capacity retry deadline");
    tokio::time::timeout(StdDuration::from_secs(3), async {
        while db.statement_time().await < final_due {
            tokio::time::sleep(StdDuration::from_millis(20)).await;
        }
    })
    .await
    .expect("final one-second capacity retry becomes genuinely due");
    let blocked_claim = source.claims()[0];
    let recovered = service
        .run_economic_pass(pass_config(db.statement_time().await))
        .await
        .expect("available compute permits complete both mature tasks");
    assert_eq!(recovered.economic_inserted, 2);
    assert_eq!(recovered.economic_capacity_deferred, 0);
    assert_eq!(recovered.economic_censored, 0);
    assert_eq!(recovered.economic_candidates, 2);
    assert_eq!(source.calls(), 10);
    let claims = source.claims();
    let retry = claims
        .iter()
        .skip(8)
        .find(|claim| claim.recommendation_id == blocked_claim.recommendation_id)
        .expect("blocked task was retried");
    assert_eq!(retry.attempt_count, blocked_claim.attempt_count + 8);
    assert_eq!(retry.replay_until, blocked_claim.replay_until);
    assert_eq!(retry.source_cutoff_at, blocked_claim.source_cutoff_at);
    assert_eq!(
        retry.source_available_until,
        blocked_claim.source_available_until
    );
    for recommendation_id in [first.recommendation, second.recommendation] {
        let outcome = outcomes
            .find_by_id(&recommendation_id)
            .await
            .expect("recovered outcome read")
            .expect("recovered outcome exists");
        assert_eq!(
            outcome.state,
            RecommendationEconomicOutcomeState::EntryNotTriggered
        );
        assert!(outcome.available_at >= outcome.source_available_until);
        let task = EconomicTaskEntity::find_by_id(recommendation_id)
            .one(&db)
            .await
            .expect("completed task read")
            .expect("completed task exists");
        assert_eq!(task.status, OutcomeReconciliationTaskStatus::Completed);
        assert!(task.claim_owner.is_none() && task.lease_expires_at.is_none());
    }
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
                ["1", "1"],
            )],
        }),
    });
    let reconciliation = Arc::new(service_with_execution_outcomes(
        &db,
        source,
        Arc::clone(&facts),
        Arc::new(FailingExecutionOutcomes),
    ));
    let worker = OutcomeReconciliationWorker::new(Arc::clone(&reconciliation));

    assert!(matches!(
        worker
            .run_once(pass_config(db.statement_time().await))
            .await
            .expect_err("execution repository failure remains fail-closed"),
        QuantError::Storage(StorageError::InvariantViolation { .. })
    ));
    assert_eq!(facts.row_count(), 1);
    assert_eq!(cursor_block(&db).await, 101);
    assert!(
        PgRecommendationResolutionOutcomeRepository::new(db.clone())
            .find_by_recommendation(&ids.recommendation)
            .await
            .expect("read resolution outcome after execution lane failure")
            .is_none()
    );

    release_resolution_retries(&db).await;
    let recovered = reconciliation
        .run_resolution_pass(pass_config(db.statement_time().await))
        .await
        .expect("resolution lane recovers independently of execution");
    assert_eq!(recovered.resolution_inserted, 1);
    assert!(
        PgRecommendationResolutionOutcomeRepository::new(db)
            .find_by_recommendation(&ids.recommendation)
            .await
            .expect("read resolution outcome after independent recovery")
            .is_some()
    );
}

fn service(
    db: &DatabaseConnection,
    source: Arc<dyn ResolutionSourceReader>,
    facts: Arc<MemoryResolutionFacts>,
) -> OutcomeReconciliationService {
    service_with_execution_outcomes(
        db,
        source,
        facts,
        Arc::new(PgExecutionAttemptOutcomeRepository::new(db.clone())),
    )
}

fn service_with_execution_outcomes(
    db: &DatabaseConnection,
    source: Arc<dyn ResolutionSourceReader>,
    facts: Arc<MemoryResolutionFacts>,
    execution_outcomes: Arc<dyn ExecutionAttemptOutcomeRepository>,
) -> OutcomeReconciliationService {
    let economic_outcomes = Arc::new(PgRecommendationEconomicOutcomeRepository::new(db.clone()))
        as Arc<dyn RecommendationEconomicOutcomeRepository>;
    service_with_economic_source(
        db,
        source,
        facts,
        execution_outcomes,
        economic_outcomes,
        Arc::new(UnexpectedEconomicReplaySource),
    )
}

fn service_with_economic_source(
    db: &DatabaseConnection,
    source: Arc<dyn ResolutionSourceReader>,
    facts: Arc<MemoryResolutionFacts>,
    execution_outcomes: Arc<dyn ExecutionAttemptOutcomeRepository>,
    economic_outcomes: Arc<dyn RecommendationEconomicOutcomeRepository>,
    economic_replay: Arc<dyn RecommendationEconomicReplaySource>,
) -> OutcomeReconciliationService {
    let resolution_fact_writer = Arc::clone(&facts) as Arc<dyn FactWriter<MarketResolutionRow>>;
    let resolution_facts: Arc<dyn QuantFactReadRepository> = facts;
    OutcomeReconciliationService::new(OutcomeReconciliationServiceDeps {
        resolution_source: source,
        resolution_fact_writer,
        resolution_facts,
        cursors: Arc::new(PgDomainSourceCursorRepository::new(db.clone())),
        resolution_observations: Arc::new(PgResolutionObservationRepository::new(db.clone())),
        markets: Arc::new(PgMarketRepository::new(db.clone())),
        resolution_outcomes: Arc::new(PgRecommendationResolutionOutcomeRepository::new(db.clone())),
        execution_outcomes,
        execution_rollups: Arc::new(PgRecommendationExecutionRollupRepository::new(db.clone())),
        economic_outcomes,
        economic_replay,
    })
}

const fn pass_config(pass_started_at: DateTime<Utc>) -> OutcomeReconciliationPassConfig {
    OutcomeReconciliationPassConfig {
        pass_started_at,
        candidate_batch_size: 100,
        source_block_span: 32,
        economic_source_lateness_secs: 300,
        sweep_secs: 1,
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
    numerators: [&str; 2],
) -> FinalizedResolutionObservation {
    FinalizedResolutionObservation {
        market_id: MarketId::new(market_id),
        vector: FinalizedResolutionVector::try_from_decimal_parts("2", numerators)
            .expect("canonical payout vector"),
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

//! Recoverable market-resolution and execution-outcome producer.

use std::{
    collections::{HashMap, HashSet},
    sync::Arc,
};

use chrono::{DateTime, Duration, Utc};
use quant_pivot_api::settlement::resolution::{
    FinalizedResolutionBlock, FinalizedResolutionObservation, FinalizedResolutionScan,
    ResolutionSourceReader,
};
use quant_pivot_error::{QuantError, QuantResult, execution::ExecutionError};
use quant_pivot_models::{
    clickhouse::{MarketResolutionFactInput, MarketResolutionRow},
    domain::{
        data_plane::{
            DomainCursorStatus, DomainSourceCheckpoint, DomainSourceCursorCasOutcome,
            DomainSourceCursorInfo, UpsertDomainSourceCursor,
        },
        market::MarketInfo,
        quant::{
            ExecutionOutcomeReconciliationResult, InsertResolutionOutcomeResult,
            RecommendationResolutionReconciliationCandidate, ResolutionOutcomeDeferredReason,
            ResolutionOutcomeReconciliationResult,
        },
    },
    hashing::CanonicalDigest,
    runtime_config::OutcomeReconciliationPolicy,
    types::{
        ContentHash, DomainInstrumentKey, DomainSourceId, EvmBlockHash, MarketId, OrderIntentId,
        RecommendationId,
    },
};
use quant_pivot_repository::traits::{
    DomainSourceCursorRepository, FactWriter, MarketRepository, QuantFactReadRepository,
    RecommendationExecutionOutcomeRepository, RecommendationResolutionOutcomeRepository,
};
use tokio::sync::Mutex;

/// Immutable inputs for one bounded reconciliation pass.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct OutcomeReconciliationPassConfig {
    pub pass_started_at: DateTime<Utc>,
    pub candidate_batch_size: u64,
    pub source_block_span: u64,
}

impl OutcomeReconciliationPassConfig {
    fn validate(self) -> Result<Self, ExecutionError> {
        if self.pass_started_at.timestamp_millis() <= 0 {
            return Err(invariant_error(
                "pass_started_at must be a positive UTC timestamp",
            ));
        }
        if self.candidate_batch_size == 0
            || self.candidate_batch_size > OutcomeReconciliationPolicy::MAX_CANDIDATE_BATCH_SIZE
        {
            return Err(invariant_error(format!(
                "candidate_batch_size must be in 1..={}",
                OutcomeReconciliationPolicy::MAX_CANDIDATE_BATCH_SIZE
            )));
        }
        if self.source_block_span == 0
            || self.source_block_span > OutcomeReconciliationPolicy::MAX_SOURCE_BLOCK_SPAN
        {
            return Err(invariant_error(format!(
                "source_block_span must be in 1..={}",
                OutcomeReconciliationPolicy::MAX_SOURCE_BLOCK_SPAN
            )));
        }
        Ok(self)
    }
}

/// Observable result of one bounded source/backlog pass.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct OutcomeReconciliationPassSummary {
    pub source_cursor_initialized: bool,
    pub source_scans: u64,
    pub source_observations: u64,
    pub source_unknown_markets: u64,
    pub source_facts_written: u64,
    pub source_facts_recovered: u64,
    pub cursor_advanced: bool,
    pub cursor_conflicted: bool,
    pub resolution_candidates: u64,
    pub resolution_inserted: u64,
    pub resolution_existing: u64,
    pub resolution_deferred: u64,
    pub execution_candidates: u64,
    pub execution_inserted: u64,
    pub execution_existing: u64,
    pub execution_deferred: u64,
}

/// Frozen frontier and aggregate proof for a complete resolution backfill.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ResolutionOutcomeBackfillSummary {
    pub frozen_at: DateTime<Utc>,
    pub source_start_block: u64,
    pub source_target_block: u64,
    pub source_target_hash: EvmBlockHash,
    pub source_target_time: DateTime<Utc>,
    pub source_pages: u64,
    pub outcome_pages: u64,
    pub totals: OutcomeReconciliationPassSummary,
}

/// Frozen cutoff and aggregate proof for a complete execution backfill.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ExecutionOutcomeBackfillSummary {
    pub frozen_at: DateTime<Utc>,
    pub outcome_pages: u64,
    pub totals: OutcomeReconciliationPassSummary,
}

/// Runtime dependencies for [`OutcomeReconciliationService`].
pub struct OutcomeReconciliationServiceDeps {
    pub resolution_source: Arc<dyn ResolutionSourceReader>,
    pub resolution_fact_writer: Arc<dyn FactWriter<MarketResolutionRow>>,
    pub resolution_facts: Arc<dyn QuantFactReadRepository>,
    pub cursors: Arc<dyn DomainSourceCursorRepository>,
    pub markets: Arc<dyn MarketRepository>,
    pub resolution_outcomes: Arc<dyn RecommendationResolutionOutcomeRepository>,
    pub execution_outcomes: Arc<dyn RecommendationExecutionOutcomeRepository>,
}

/// Single owner for durable source frontier and both orthogonal outcome backlogs.
pub struct OutcomeReconciliationService {
    resolution_source: Arc<dyn ResolutionSourceReader>,
    resolution_fact_writer: Arc<dyn FactWriter<MarketResolutionRow>>,
    resolution_facts: Arc<dyn QuantFactReadRepository>,
    cursors: Arc<dyn DomainSourceCursorRepository>,
    markets: Arc<dyn MarketRepository>,
    resolution_outcomes: Arc<dyn RecommendationResolutionOutcomeRepository>,
    execution_outcomes: Arc<dyn RecommendationExecutionOutcomeRepository>,
    // Process-local cursors provide bounded round-robin fairness. Durable
    // outcomes remain the only progress truth, so restart safely repeats pages.
    resolution_runtime_after: Mutex<Option<RecommendationId>>,
    execution_runtime_after: Mutex<Option<OrderIntentId>>,
}

impl OutcomeReconciliationService {
    #[must_use]
    pub fn new(deps: OutcomeReconciliationServiceDeps) -> Self {
        Self {
            resolution_source: deps.resolution_source,
            resolution_fact_writer: deps.resolution_fact_writer,
            resolution_facts: deps.resolution_facts,
            cursors: deps.cursors,
            markets: deps.markets,
            resolution_outcomes: deps.resolution_outcomes,
            execution_outcomes: deps.execution_outcomes,
            resolution_runtime_after: Mutex::new(None),
            execution_runtime_after: Mutex::new(None),
        }
    }

    /// Reconcile one bounded resolution-source and resolution-outcome pass.
    ///
    /// This lane is intentionally independent from execution reconciliation:
    /// a missing or contradictory resolution fact must not block sealing
    /// execution truth.
    pub async fn run_resolution_pass(
        &self,
        config: OutcomeReconciliationPassConfig,
    ) -> QuantResult<OutcomeReconciliationPassSummary> {
        let config = config.validate()?;
        let mut summary = OutcomeReconciliationPassSummary::default();
        let target = self.frozen_source_target(config.pass_started_at).await?;
        let cursor = self
            .load_or_initialize_cursor(config.pass_started_at, &target, &mut summary)
            .await?;
        self.reconcile_source_page(config, cursor, &target, &mut summary)
            .await?;
        let mut runtime_after = self.resolution_runtime_after.lock().await;
        *runtime_after = self
            .reconcile_resolution_page(config, *runtime_after, &mut summary)
            .await?;
        drop(runtime_after);
        Ok(summary)
    }

    /// Reconcile one bounded execution-outcome pass independently of market
    /// resolution availability.
    pub async fn run_execution_pass(
        &self,
        config: OutcomeReconciliationPassConfig,
    ) -> QuantResult<OutcomeReconciliationPassSummary> {
        let config = config.validate()?;
        let mut summary = OutcomeReconciliationPassSummary::default();
        let mut runtime_after = self.execution_runtime_after.lock().await;
        *runtime_after = self
            .reconcile_execution_page(config, *runtime_after, &mut summary)
            .await?;
        drop(runtime_after);
        Ok(summary)
    }

    /// Drain the resolution source and terminal recommendation backlog to one
    /// immutable source and database cutoff.
    pub async fn run_resolution_backfill(
        &self,
        config: OutcomeReconciliationPassConfig,
    ) -> QuantResult<ResolutionOutcomeBackfillSummary> {
        let config = config.validate()?;
        let target = self.frozen_source_target(config.pass_started_at).await?;
        let mut totals = OutcomeReconciliationPassSummary::default();
        let mut cursor = self
            .load_or_initialize_cursor(config.pass_started_at, &target, &mut totals)
            .await?;
        let source_start_block = resolution_cursor_state(&cursor)?.block_number;
        let mut source_pages = 0;
        while resolution_cursor_state(&cursor)?.block_number < target.block_number {
            cursor = self
                .reconcile_source_page(config, cursor, &target, &mut totals)
                .await?;
            source_pages += 1;
        }

        let mut outcome_pages = 0;
        let mut after = None;
        while let Some(cursor) = self
            .reconcile_resolution_page(config, after, &mut totals)
            .await?
        {
            after = Some(cursor);
            outcome_pages += 1;
        }
        Ok(ResolutionOutcomeBackfillSummary {
            frozen_at: config.pass_started_at,
            source_start_block,
            source_target_block: target.block_number,
            source_target_hash: target.block_hash,
            source_target_time: target.block_time,
            source_pages,
            outcome_pages,
            totals,
        })
    }

    /// Drain actually submitted terminal execution facts to one immutable
    /// database cutoff without synthesizing missing execution.
    pub async fn run_execution_backfill(
        &self,
        config: OutcomeReconciliationPassConfig,
    ) -> QuantResult<ExecutionOutcomeBackfillSummary> {
        let config = config.validate()?;
        let mut totals = OutcomeReconciliationPassSummary::default();
        let mut outcome_pages = 0;
        let mut after = None;
        while let Some(cursor) = self
            .reconcile_execution_page(config, after, &mut totals)
            .await?
        {
            after = Some(cursor);
            outcome_pages += 1;
        }
        Ok(ExecutionOutcomeBackfillSummary {
            frozen_at: config.pass_started_at,
            outcome_pages,
            totals,
        })
    }

    async fn frozen_source_target(
        &self,
        pass_started_at: DateTime<Utc>,
    ) -> QuantResult<FinalizedResolutionBlock> {
        let target = self
            .resolution_source
            .block_at_or_before(pass_started_at)
            .await
            .map_err(|source| ExecutionError::OutcomeReconciliationSource {
                reason: source.to_string(),
            })?;
        validate_head(&target, pass_started_at)?;
        Ok(target)
    }

    async fn reconcile_source_page(
        &self,
        config: OutcomeReconciliationPassConfig,
        cursor: DomainSourceCursorInfo,
        target: &FinalizedResolutionBlock,
        summary: &mut OutcomeReconciliationPassSummary,
    ) -> QuantResult<DomainSourceCursorInfo> {
        let current = resolution_cursor_state(&cursor)?;
        if current.block_number >= target.block_number {
            if current.block_number == target.block_number
                && (current.block_hash != target.block_hash
                    || current.block_time != target.block_time)
            {
                return Err(invariant_error(
                    "resolution cursor conflicts with the frozen source target",
                )
                .into());
            }
            return Ok(cursor);
        }
        let from_block = current.block_number.checked_add(1).ok_or_else(|| {
            invariant_error("resolution cursor block overflow while computing scan start")
        })?;
        let requested_to_block = from_block
            .checked_add(config.source_block_span - 1)
            .ok_or_else(|| invariant_error("resolution scan block range overflow"))?
            .min(target.block_number);
        let Some(scan) = self
            .resolution_source
            .scan_finalized(from_block, requested_to_block)
            .await
            .map_err(|source| ExecutionError::OutcomeReconciliationSource {
                reason: source.to_string(),
            })?
        else {
            return Err(invariant_error(
                "frozen resolution target became unavailable before the source scan completed",
            )
            .into());
        };
        validate_scan(&scan, &current, requested_to_block, target)?;
        summary.source_scans += 1;
        summary.source_observations += usize_to_u64(scan.observations.len())?;

        let market_ids: Vec<MarketId> = scan
            .observations
            .iter()
            .map(|observation| observation.market_id.clone())
            .collect();
        let markets = self.markets.find_by_ids(&market_ids).await?;
        let markets_by_id: HashMap<MarketId, Arc<MarketInfo>> = markets
            .into_iter()
            .map(|market| (market.market_id.clone(), market))
            .collect();
        for observation in &scan.observations {
            let Some(market) = markets_by_id.get(&observation.market_id) else {
                summary.source_unknown_markets += 1;
                continue;
            };
            self.persist_resolution_fact(observation, market, config.pass_started_at, summary)
                .await?;
        }
        self.advance_resolution_cursor(&current, &scan, summary)
            .await
    }

    async fn load_or_initialize_cursor(
        &self,
        pass_started_at: DateTime<Utc>,
        target: &FinalizedResolutionBlock,
        summary: &mut OutcomeReconciliationPassSummary,
    ) -> QuantResult<DomainSourceCursorInfo> {
        let source_id = resolution_source_id();
        let instrument_key = resolution_instrument_key();
        if let Some(cursor) = self.cursors.find(&source_id, &instrument_key).await? {
            resolution_cursor_state(&cursor)?;
            return Ok(cursor);
        }

        let seed = match self
            .resolution_outcomes
            .source_history_start(pass_started_at)
            .await?
        {
            Some(history_start) => {
                let seed_at = history_start
                    .checked_sub_signed(Duration::seconds(1))
                    .ok_or_else(|| {
                        invariant_error("resolution source history start underflowed UTC")
                    })?;
                self.resolution_source
                    .block_at_or_before(seed_at)
                    .await
                    .map_err(|source| ExecutionError::OutcomeReconciliationSource {
                        reason: source.to_string(),
                    })?
            }
            None => target.clone(),
        };
        validate_head(&seed, pass_started_at)?;
        if seed.block_number > target.block_number
            || (seed.block_number == target.block_number
                && (seed.block_hash != target.block_hash || seed.block_time != target.block_time))
        {
            return Err(
                invariant_error("resolution source seed is ahead of its frozen target").into(),
            );
        }
        let cursor = cursor_update(&seed)?;
        match self.cursors.compare_and_set(None, cursor).await? {
            DomainSourceCursorCasOutcome::Advanced(cursor) => {
                summary.source_cursor_initialized = true;
                resolution_cursor_state(&cursor)?;
                Ok(cursor)
            }
            DomainSourceCursorCasOutcome::Conflict(cursor) => {
                summary.cursor_conflicted = true;
                resolution_cursor_state(&cursor)?;
                Ok(cursor)
            }
        }
    }

    async fn persist_resolution_fact(
        &self,
        observation: &FinalizedResolutionObservation,
        market: &MarketInfo,
        pass_started_at: DateTime<Utc>,
        summary: &mut OutcomeReconciliationPassSummary,
    ) -> QuantResult<()> {
        if let Some(existing) = self
            .resolution_facts
            .resolution_by_checkpoint(&observation.source_checkpoint_hash)
            .await?
        {
            validate_fact_matches(&existing, observation, market)?;
            summary.source_facts_recovered += 1;
            return Ok(());
        }
        if let Some(existing) = self
            .resolution_facts
            .resolution_by_market(&observation.market_id)
            .await?
        {
            validate_fact_matches(&existing, observation, market)?;
            summary.source_facts_recovered += 1;
            return Ok(());
        }

        let fact = fact_from_observation(observation, market, pass_started_at.timestamp_millis())?;
        self.resolution_fact_writer
            .write_batch_idempotent(&observation.source_checkpoint_hash, vec![fact])
            .await?;
        let persisted = self
            .resolution_facts
            .resolution_by_checkpoint(&observation.source_checkpoint_hash)
            .await?
            .ok_or_else(|| {
                invariant_error(format!(
                    "resolution checkpoint {} was acknowledged but cannot be read back",
                    observation.source_checkpoint_hash
                ))
            })?;
        validate_fact_matches(&persisted, observation, market)?;
        summary.source_facts_written += 1;
        Ok(())
    }

    async fn advance_resolution_cursor(
        &self,
        current: &ResolutionCursorState,
        scan: &FinalizedResolutionScan,
        summary: &mut OutcomeReconciliationPassSummary,
    ) -> QuantResult<DomainSourceCursorInfo> {
        let target = FinalizedResolutionBlock {
            block_number: scan.to_block,
            block_hash: scan.to_block_hash.clone(),
            block_time: scan.to_block_time,
        };
        match self
            .cursors
            .compare_and_set(Some(current.checkpoint_hash), cursor_update(&target)?)
            .await?
        {
            DomainSourceCursorCasOutcome::Advanced(cursor) => {
                let advanced = resolution_cursor_state(&cursor)?;
                if advanced.block_number != scan.to_block
                    || advanced.block_hash != scan.to_block_hash
                    || advanced.block_time != scan.to_block_time
                {
                    return Err(invariant_error(
                        "advanced resolution cursor differs from the finalized scan",
                    )
                    .into());
                }
                summary.cursor_advanced = true;
                Ok(cursor)
            }
            DomainSourceCursorCasOutcome::Conflict(cursor) => {
                let winner = resolution_cursor_state(&cursor)?;
                if winner.block_number < scan.to_block
                    || (winner.block_number == scan.to_block
                        && (winner.block_hash != scan.to_block_hash
                            || winner.block_time != scan.to_block_time))
                    || winner.block_time < scan.to_block_time
                {
                    return Err(invariant_error(
                        "competing resolution cursor is behind or conflicts with the finalized scan",
                    )
                    .into());
                }
                summary.cursor_conflicted = true;
                Ok(cursor)
            }
        }
    }

    async fn reconcile_resolution_page(
        &self,
        config: OutcomeReconciliationPassConfig,
        after: Option<RecommendationId>,
        summary: &mut OutcomeReconciliationPassSummary,
    ) -> QuantResult<Option<RecommendationId>> {
        let candidates = self
            .resolution_outcomes
            .list_reconciliation_candidates(
                config.pass_started_at,
                after,
                config.candidate_batch_size,
            )
            .await?;
        let next = candidates
            .last()
            .map(|candidate| candidate.recommendation_id);
        summary.resolution_candidates += usize_to_u64(candidates.len())?;
        for candidate in candidates {
            match self
                .reconcile_resolution_candidate(config, &candidate)
                .await?
            {
                ResolutionOutcomeReconciliationResult::Inserted(_) => {
                    summary.resolution_inserted += 1;
                }
                ResolutionOutcomeReconciliationResult::AlreadyPresent(_) => {
                    summary.resolution_existing += 1;
                }
                ResolutionOutcomeReconciliationResult::Deferred(reason) => {
                    summary.resolution_deferred += 1;
                    tracing::debug!(
                        recommendation_id = %candidate.recommendation_id,
                        market_id = %candidate.market_id,
                        ?reason,
                        "resolution outcome source is unavailable at the frozen cutoff",
                    );
                }
            }
        }
        Ok(next)
    }

    async fn reconcile_resolution_candidate(
        &self,
        config: OutcomeReconciliationPassConfig,
        candidate: &RecommendationResolutionReconciliationCandidate,
    ) -> QuantResult<ResolutionOutcomeReconciliationResult> {
        let Some(fact) = self
            .resolution_facts
            .resolution_at(
                &candidate.market_id,
                config.pass_started_at.timestamp_millis(),
                config.pass_started_at.timestamp_millis(),
            )
            .await?
        else {
            return Ok(ResolutionOutcomeReconciliationResult::Deferred(
                ResolutionOutcomeDeferredReason::CanonicalFactUnavailableAtCutoff,
            ));
        };
        let result = self
            .resolution_outcomes
            .reconcile_fact(&candidate.recommendation_id, &fact)
            .await?;
        Ok(match result {
            InsertResolutionOutcomeResult::Inserted(outcome) => {
                ResolutionOutcomeReconciliationResult::Inserted(outcome)
            }
            InsertResolutionOutcomeResult::AlreadyPresent(outcome) => {
                ResolutionOutcomeReconciliationResult::AlreadyPresent(outcome)
            }
        })
    }

    async fn reconcile_execution_page(
        &self,
        config: OutcomeReconciliationPassConfig,
        after: Option<OrderIntentId>,
        summary: &mut OutcomeReconciliationPassSummary,
    ) -> QuantResult<Option<OrderIntentId>> {
        let candidates = self
            .execution_outcomes
            .list_reconciliation_candidates(
                config.pass_started_at,
                after,
                config.candidate_batch_size,
            )
            .await?;
        let next = candidates.last().map(|candidate| candidate.order_intent_id);
        summary.execution_candidates += usize_to_u64(candidates.len())?;
        for candidate in candidates {
            match self
                .execution_outcomes
                .reconcile_intent(&candidate.order_intent_id, config.pass_started_at)
                .await?
            {
                ExecutionOutcomeReconciliationResult::Inserted(_) => {
                    summary.execution_inserted += 1;
                }
                ExecutionOutcomeReconciliationResult::AlreadyPresent(_) => {
                    summary.execution_existing += 1;
                }
                ExecutionOutcomeReconciliationResult::Deferred(reason) => {
                    summary.execution_deferred += 1;
                    tracing::debug!(
                        order_intent_id = %candidate.order_intent_id,
                        recommendation_id = %candidate.recommendation_id,
                        ?reason,
                        "execution outcome source remains incomplete",
                    );
                }
            }
        }
        Ok(next)
    }
}

#[derive(Debug)]
struct ResolutionCursorState {
    block_number: u64,
    block_hash: EvmBlockHash,
    block_time: DateTime<Utc>,
    checkpoint_hash: ContentHash,
}

fn resolution_source_id() -> DomainSourceId {
    DomainSourceId::polymarket_ctf_resolution()
}

fn resolution_instrument_key() -> DomainInstrumentKey {
    DomainInstrumentKey::polymarket_ctf_resolution()
}

fn cursor_update(block: &FinalizedResolutionBlock) -> Result<UpsertDomainSourceCursor, QuantError> {
    let checkpoint_json = DomainSourceCheckpoint::PolymarketCtfResolution {
        finalized_block: block.block_number,
        block_hash: block.block_hash.clone(),
        block_time: block.block_time,
    };
    let checkpoint_hash = CanonicalDigest::content_hash_json(&checkpoint_json)?;
    Ok(UpsertDomainSourceCursor {
        source_id: resolution_source_id(),
        instrument_key: resolution_instrument_key(),
        checkpoint_json,
        checkpoint_hash,
        status: DomainCursorStatus::Live,
        last_error: None,
        updated_at: Utc::now(),
    })
}

fn resolution_cursor_state(
    cursor: &DomainSourceCursorInfo,
) -> Result<ResolutionCursorState, ExecutionError> {
    if cursor.source_id != resolution_source_id()
        || cursor.instrument_key != resolution_instrument_key()
        || cursor.status != DomainCursorStatus::Live
        || cursor.last_error.is_some()
    {
        return Err(invariant_error(
            "resolution cursor identity or lifecycle state is invalid",
        ));
    }
    match &cursor.checkpoint_json {
        DomainSourceCheckpoint::PolymarketCtfResolution {
            finalized_block,
            block_hash,
            block_time,
        } => Ok(ResolutionCursorState {
            block_number: *finalized_block,
            block_hash: block_hash.clone(),
            block_time: *block_time,
            checkpoint_hash: cursor.checkpoint_hash,
        }),
        _ => Err(invariant_error(
            "resolution cursor contains a checkpoint from another source",
        )),
    }
}

fn validate_head(
    head: &FinalizedResolutionBlock,
    pass_started_at: DateTime<Utc>,
) -> Result<(), ExecutionError> {
    if head.block_time > pass_started_at {
        return Err(invariant_error(
            "finalized resolution head is later than pass_started_at",
        ));
    }
    Ok(())
}

fn validate_scan(
    scan: &FinalizedResolutionScan,
    current: &ResolutionCursorState,
    requested_to_block: u64,
    target: &FinalizedResolutionBlock,
) -> Result<(), ExecutionError> {
    let expected_from = current
        .block_number
        .checked_add(1)
        .ok_or_else(|| invariant_error("resolution cursor block overflow"))?;
    let range_mismatch = scan.from_block != expected_from
        || scan.to_block < scan.from_block
        || scan.to_block != requested_to_block;
    let timeline_mismatch =
        scan.to_block_time < current.block_time || scan.to_block_time > target.block_time;
    let target_mismatch = scan.to_block == target.block_number
        && (scan.to_block_hash != target.block_hash || scan.to_block_time != target.block_time);
    if range_mismatch || timeline_mismatch || target_mismatch {
        return Err(invariant_error(
            "finalized resolution scan range or timeline differs from its cursor request",
        ));
    }
    let mut previous_key = None;
    let mut previous_time = current.block_time;
    let mut markets = HashSet::with_capacity(scan.observations.len());
    let mut checkpoints = HashSet::with_capacity(scan.observations.len());
    let mut block_hashes = HashMap::new();
    for observation in &scan.observations {
        let key = (observation.block_number, observation.log_index);
        let block_hash_conflict = block_hashes
            .insert(observation.block_number, observation.block_hash.clone())
            .is_some_and(|existing| existing != observation.block_hash);
        let tail_hash_mismatch = observation.block_number == scan.to_block
            && observation.block_hash != scan.to_block_hash;
        let duplicate_market = !markets.insert(observation.market_id.clone());
        let duplicate_checkpoint = !checkpoints.insert(observation.source_checkpoint_hash);
        let timeline_regression =
            observation.resolved_at < current.block_time || observation.resolved_at < previous_time;
        if observation.block_number < scan.from_block
            || observation.block_number > scan.to_block
            || observation.resolved_at > scan.to_block_time
            || previous_key.is_some_and(|previous| key <= previous)
            || block_hash_conflict
            || tail_hash_mismatch
            || duplicate_market
            || duplicate_checkpoint
            || timeline_regression
        {
            return Err(invariant_error(
                "finalized resolution observations violate range, identity, hash, or timeline invariants",
            ));
        }
        previous_key = Some(key);
        previous_time = observation.resolved_at;
    }
    Ok(())
}

fn fact_from_observation(
    observation: &FinalizedResolutionObservation,
    market: &MarketInfo,
    observed_at: i64,
) -> Result<MarketResolutionRow, ExecutionError> {
    if observation.market_id != market.market_id {
        return Err(invariant_error(
            "resolution observation and catalog market identities differ",
        ));
    }
    MarketResolutionRow::seal(MarketResolutionFactInput {
        market_id: observation.market_id.clone(),
        token_ids: [market.yes_token_id.clone(), market.no_token_id.clone()],
        payout_ratios: observation.vector.payout_ratios(),
        resolved_at: observation.resolved_at.timestamp_millis(),
        observed_at,
        source_block_number: observation.block_number,
        source_block_hash: observation.block_hash.clone(),
        source_transaction_hash: observation.transaction_hash.clone(),
        source_log_index: observation.log_index,
        source_checkpoint_hash: observation.source_checkpoint_hash,
    })
    .map_err(|error| invariant_error(error.to_string()))
}

fn validate_fact_matches(
    fact: &MarketResolutionRow,
    observation: &FinalizedResolutionObservation,
    market: &MarketInfo,
) -> Result<(), ExecutionError> {
    fact.validate()
        .map_err(|error| invariant_error(error.to_string()))?;
    let expected = fact_from_observation(observation, market, fact.observed_at)?;
    if &expected != fact {
        return Err(invariant_error(
            "persisted resolution fact differs from finalized source and catalog tokens",
        ));
    }
    Ok(())
}

fn invariant_error(reason: impl Into<String>) -> ExecutionError {
    ExecutionError::OutcomeReconciliationInvariant {
        reason: reason.into(),
    }
}

fn usize_to_u64(value: usize) -> Result<u64, ExecutionError> {
    u64::try_from(value)
        .map_err(|error| invariant_error(format!("reconciliation count overflow: {error}")))
}

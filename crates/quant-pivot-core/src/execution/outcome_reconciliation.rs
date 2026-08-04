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
            ExecutionAttemptReconciliationResult, ExecutionRollupReconciliationResult,
            InsertResolutionOutcomeResult, NewResolutionObservationInbox, OutcomeTaskSettlement,
            RecommendationResolutionReconciliationCandidate, ResolutionObservationInboxInfo,
            ResolutionOutcomeDeferredReason, ResolutionOutcomeReconciliationResult,
            ResolutionProjectionClaim, ResolutionProjectionSettlement, ResolutionScanCommitOutcome,
        },
    },
    enums::quant::ResolutionProjectionErrorCode,
    hashing::CanonicalDigest,
    runtime_config::OutcomeReconciliationPolicy,
    types::{
        ArtifactUri, ContentHash, DomainInstrumentKey, DomainSourceId, EvmBlockHash, WorkerId,
    },
};
use quant_pivot_repository::traits::{
    DomainSourceCursorRepository, ExecutionAttemptOutcomeRepository, FactWriter, MarketRepository,
    QuantFactReadRepository, RecommendationExecutionRollupRepository,
    RecommendationResolutionOutcomeRepository, ResolutionObservationRepository,
};
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
    pub source_observations_inserted: u64,
    pub source_observations_recovered: u64,
    pub source_unknown_markets: u64,
    pub source_facts_written: u64,
    pub source_facts_recovered: u64,
    pub source_projection_retries: u64,
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
    pub rollup_candidates: u64,
    pub rollup_inserted: u64,
    pub rollup_existing: u64,
    pub rollup_deferred: u64,
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
    pub rollup_pages: u64,
    pub totals: OutcomeReconciliationPassSummary,
}

/// Runtime dependencies for [`OutcomeReconciliationService`].
pub struct OutcomeReconciliationServiceDeps {
    pub resolution_source: Arc<dyn ResolutionSourceReader>,
    pub resolution_fact_writer: Arc<dyn FactWriter<MarketResolutionRow>>,
    pub resolution_facts: Arc<dyn QuantFactReadRepository>,
    pub cursors: Arc<dyn DomainSourceCursorRepository>,
    pub resolution_observations: Arc<dyn ResolutionObservationRepository>,
    pub markets: Arc<dyn MarketRepository>,
    pub resolution_outcomes: Arc<dyn RecommendationResolutionOutcomeRepository>,
    pub execution_outcomes: Arc<dyn ExecutionAttemptOutcomeRepository>,
    pub execution_rollups: Arc<dyn RecommendationExecutionRollupRepository>,
}

/// Single owner for durable source frontier and both orthogonal outcome backlogs.
pub struct OutcomeReconciliationService {
    resolution_source: Arc<dyn ResolutionSourceReader>,
    resolution_fact_writer: Arc<dyn FactWriter<MarketResolutionRow>>,
    resolution_facts: Arc<dyn QuantFactReadRepository>,
    cursors: Arc<dyn DomainSourceCursorRepository>,
    resolution_observations: Arc<dyn ResolutionObservationRepository>,
    markets: Arc<dyn MarketRepository>,
    resolution_outcomes: Arc<dyn RecommendationResolutionOutcomeRepository>,
    execution_outcomes: Arc<dyn ExecutionAttemptOutcomeRepository>,
    execution_rollups: Arc<dyn RecommendationExecutionRollupRepository>,
    projection_worker_id: WorkerId,
}

impl OutcomeReconciliationService {
    #[must_use]
    pub fn new(deps: OutcomeReconciliationServiceDeps) -> Self {
        Self {
            resolution_source: deps.resolution_source,
            resolution_fact_writer: deps.resolution_fact_writer,
            resolution_facts: deps.resolution_facts,
            cursors: deps.cursors,
            resolution_observations: deps.resolution_observations,
            markets: deps.markets,
            resolution_outcomes: deps.resolution_outcomes,
            execution_outcomes: deps.execution_outcomes,
            execution_rollups: deps.execution_rollups,
            projection_worker_id: WorkerId::from_v7(),
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
        self.project_resolution_batch(config, &mut summary).await?;
        self.reconcile_resolution_page(config, &mut summary).await?;
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
        self.reconcile_execution_page(config, &mut summary).await?;
        self.reconcile_rollup_page(config, &mut summary).await?;
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
            self.project_resolution_batch(config, &mut totals).await?;
        }
        while self
            .project_resolution_batch(config, &mut totals)
            .await?
            .is_some()
        {}

        let mut outcome_pages = 0;
        while self.reconcile_resolution_page(config, &mut totals).await? {
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
        while self.reconcile_execution_page(config, &mut totals).await? {
            outcome_pages += 1;
        }
        let mut rollup_pages = 0;
        while self.reconcile_rollup_page(config, &mut totals).await? {
            rollup_pages += 1;
        }
        Ok(ExecutionOutcomeBackfillSummary {
            frozen_at: config.pass_started_at,
            outcome_pages,
            rollup_pages,
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
        self.commit_resolution_scan(&current, &scan, summary).await
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

    async fn commit_resolution_scan(
        &self,
        current: &ResolutionCursorState,
        scan: &FinalizedResolutionScan,
        summary: &mut OutcomeReconciliationPassSummary,
    ) -> QuantResult<DomainSourceCursorInfo> {
        let observations = scan
            .observations
            .iter()
            .map(inbox_from_observation)
            .collect::<Result<Vec<_>, _>>()?;
        let target = FinalizedResolutionBlock {
            block_number: scan.to_block,
            block_hash: scan.to_block_hash.clone(),
            block_time: scan.to_block_time,
        };
        match self
            .resolution_observations
            .commit_scan(
                current.checkpoint_hash,
                cursor_update(&target)?,
                observations,
            )
            .await?
        {
            ResolutionScanCommitOutcome::Committed {
                cursor,
                inserted,
                existing,
            } => {
                let advanced = resolution_cursor_state(&cursor)?;
                if advanced.block_number != scan.to_block
                    || advanced.block_hash != scan.to_block_hash
                    || advanced.block_time != scan.to_block_time
                {
                    return Err(invariant_error(
                        "committed resolution cursor differs from its immutable inbox page",
                    )
                    .into());
                }
                summary.source_observations_inserted += inserted;
                summary.source_observations_recovered += existing;
                summary.cursor_advanced = true;
                Ok(cursor)
            }
            ResolutionScanCommitOutcome::Conflict(cursor) => {
                let winner = resolution_cursor_state(&cursor)?;
                if winner.block_number < scan.to_block
                    || (winner.block_number == scan.to_block
                        && (winner.block_hash != scan.to_block_hash
                            || winner.block_time != scan.to_block_time))
                    || winner.block_time < scan.to_block_time
                {
                    return Err(invariant_error(
                        "competing resolution cursor is behind or conflicts with the inbox page",
                    )
                    .into());
                }
                summary.cursor_conflicted = true;
                Ok(cursor)
            }
        }
    }

    async fn project_resolution_batch(
        &self,
        config: OutcomeReconciliationPassConfig,
        summary: &mut OutcomeReconciliationPassSummary,
    ) -> QuantResult<Option<()>> {
        let claims = self
            .resolution_observations
            .claim_pending(self.projection_worker_id, 60, config.candidate_batch_size)
            .await?;
        if claims.is_empty() {
            return Ok(None);
        }
        let market_ids = claims
            .iter()
            .map(|claim| claim.observation.market_id.clone())
            .collect::<Vec<_>>();
        let markets = self.markets.find_by_ids(&market_ids).await?;
        let markets_by_id = markets
            .into_iter()
            .map(|market| (market.market_id.clone(), market))
            .collect::<HashMap<_, _>>();
        for claim in claims {
            let Some(market) = markets_by_id.get(&claim.observation.market_id) else {
                self.resolution_observations
                    .settle(
                        claim.observation.resolution_observation_id,
                        self.projection_worker_id,
                        ResolutionProjectionSettlement::MappingBlocked {
                            error_code: ResolutionProjectionErrorCode::CatalogMappingUnavailable,
                            error: format!(
                                "catalog mapping unavailable for market {}",
                                claim.observation.market_id
                            ),
                        },
                    )
                    .await?;
                summary.source_unknown_markets += 1;
                continue;
            };
            match self.persist_inbox_fact(&claim, market, summary).await {
                Ok(canonical_fact_hash) => {
                    self.resolution_observations
                        .settle(
                            claim.observation.resolution_observation_id,
                            self.projection_worker_id,
                            ResolutionProjectionSettlement::Verified {
                                canonical_fact_hash,
                            },
                        )
                        .await?;
                }
                Err(QuantError::Execution(ExecutionError::OutcomeReconciliationInvariant {
                    reason,
                })) => {
                    self.resolution_observations
                        .settle(
                            claim.observation.resolution_observation_id,
                            self.projection_worker_id,
                            ResolutionProjectionSettlement::Quarantined {
                                error_code: ResolutionProjectionErrorCode::InvalidObservation,
                                error: reason.clone(),
                            },
                        )
                        .await?;
                    return Err(ExecutionError::OutcomeReconciliationInvariant { reason }.into());
                }
                Err(error) => {
                    self.resolution_observations
                        .settle(
                            claim.observation.resolution_observation_id,
                            self.projection_worker_id,
                            ResolutionProjectionSettlement::RetryScheduled {
                                retry_delay_secs: 60,
                                error_code: match &error {
                                    QuantError::Storage(_) => {
                                        ResolutionProjectionErrorCode::PersistenceUnavailable
                                    }
                                    QuantError::Api(_)
                                    | QuantError::Rpc(_)
                                    | QuantError::WebSocket(_) => {
                                        ResolutionProjectionErrorCode::ExternalDependencyUnavailable
                                    }
                                    _ => ResolutionProjectionErrorCode::UnexpectedTransient,
                                },
                                error: error.to_string(),
                            },
                        )
                        .await?;
                    summary.source_projection_retries += 1;
                    tracing::warn!(
                        checkpoint_hash = %claim.observation.source_checkpoint_hash,
                        market_id = %claim.observation.market_id,
                        %error,
                        "canonical resolution projection deferred",
                    );
                }
            }
        }
        Ok(Some(()))
    }

    async fn persist_inbox_fact(
        &self,
        claim: &ResolutionProjectionClaim,
        market: &MarketInfo,
        summary: &mut OutcomeReconciliationPassSummary,
    ) -> QuantResult<ContentHash> {
        let observation = &claim.observation;
        if let Some(existing) = self
            .resolution_facts
            .resolution_by_checkpoint(&observation.source_checkpoint_hash)
            .await?
        {
            validate_inbox_fact(&existing, observation, market)?;
            summary.source_facts_recovered += 1;
            return Ok(existing.resolution_fact_hash);
        }
        if let Some(existing) = self
            .resolution_facts
            .resolution_by_market(&observation.market_id)
            .await?
        {
            validate_inbox_fact(&existing, observation, market)?;
            summary.source_facts_recovered += 1;
            return Ok(existing.resolution_fact_hash);
        }

        let fact = fact_from_inbox(observation, market)?;
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
        validate_inbox_fact(&persisted, observation, market)?;
        summary.source_facts_written += 1;
        Ok(persisted.resolution_fact_hash)
    }

    async fn reconcile_resolution_page(
        &self,
        config: OutcomeReconciliationPassConfig,
        summary: &mut OutcomeReconciliationPassSummary,
    ) -> QuantResult<bool> {
        let claims = self
            .resolution_outcomes
            .claim_reconciliation(
                config.pass_started_at,
                self.projection_worker_id,
                60,
                config.candidate_batch_size,
            )
            .await?;
        let had_claims = !claims.is_empty();
        summary.resolution_candidates += usize_to_u64(claims.len())?;
        for claim in claims {
            let candidate = claim.candidate;
            let result = self
                .reconcile_resolution_candidate(config, &candidate)
                .await;
            match result {
                Ok(ResolutionOutcomeReconciliationResult::Inserted(_)) => {
                    summary.resolution_inserted += 1;
                    self.resolution_outcomes
                        .settle_reconciliation(
                            candidate.recommendation_id,
                            self.projection_worker_id,
                            OutcomeTaskSettlement::Completed,
                        )
                        .await?;
                }
                Ok(ResolutionOutcomeReconciliationResult::AlreadyPresent(_)) => {
                    summary.resolution_existing += 1;
                    self.resolution_outcomes
                        .settle_reconciliation(
                            candidate.recommendation_id,
                            self.projection_worker_id,
                            OutcomeTaskSettlement::Completed,
                        )
                        .await?;
                }
                Ok(ResolutionOutcomeReconciliationResult::Deferred(reason)) => {
                    summary.resolution_deferred += 1;
                    self.resolution_outcomes
                        .settle_reconciliation(
                            candidate.recommendation_id,
                            self.projection_worker_id,
                            OutcomeTaskSettlement::RetryAfter {
                                delay_secs: retry_delay_secs(claim.attempt_count),
                                error: format!("{reason:?}"),
                            },
                        )
                        .await?;
                    tracing::debug!(
                        recommendation_id = %candidate.recommendation_id,
                        market_id = %candidate.market_id,
                        ?reason,
                        "resolution outcome source is unavailable at the frozen cutoff",
                    );
                }
                Err(error) => {
                    let settlement = self
                        .resolution_outcomes
                        .settle_reconciliation(
                            candidate.recommendation_id,
                            self.projection_worker_id,
                            OutcomeTaskSettlement::RetryAfter {
                                delay_secs: retry_delay_secs(claim.attempt_count),
                                error: error.to_string(),
                            },
                        )
                        .await;
                    if let Err(settlement_error) = settlement {
                        tracing::error!(
                            recommendation_id = %candidate.recommendation_id,
                            %settlement_error,
                            "failed to release resolution reconciliation lease",
                        );
                    }
                    return Err(error);
                }
            }
        }
        Ok(had_claims)
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
        summary: &mut OutcomeReconciliationPassSummary,
    ) -> QuantResult<bool> {
        let claims = self
            .execution_outcomes
            .claim_reconciliation(
                config.pass_started_at,
                self.projection_worker_id,
                60,
                config.candidate_batch_size,
            )
            .await?;
        let had_claims = !claims.is_empty();
        summary.execution_candidates += usize_to_u64(claims.len())?;
        for claim in claims {
            let candidate = claim.candidate;
            let result = self
                .execution_outcomes
                .reconcile_intent(&candidate.order_intent_id, config.pass_started_at)
                .await;
            match result {
                Ok(ExecutionAttemptReconciliationResult::Inserted(_)) => {
                    summary.execution_inserted += 1;
                    self.execution_outcomes
                        .settle_reconciliation(
                            candidate.order_intent_id,
                            self.projection_worker_id,
                            OutcomeTaskSettlement::Completed,
                        )
                        .await?;
                }
                Ok(ExecutionAttemptReconciliationResult::AlreadyPresent(_)) => {
                    summary.execution_existing += 1;
                    self.execution_outcomes
                        .settle_reconciliation(
                            candidate.order_intent_id,
                            self.projection_worker_id,
                            OutcomeTaskSettlement::Completed,
                        )
                        .await?;
                }
                Ok(ExecutionAttemptReconciliationResult::Deferred(reason)) => {
                    summary.execution_deferred += 1;
                    self.execution_outcomes
                        .settle_reconciliation(
                            candidate.order_intent_id,
                            self.projection_worker_id,
                            OutcomeTaskSettlement::RetryAfter {
                                delay_secs: retry_delay_secs(claim.attempt_count),
                                error: format!("{reason:?}"),
                            },
                        )
                        .await?;
                    tracing::debug!(
                        order_intent_id = %candidate.order_intent_id,
                        recommendation_id = %candidate.recommendation_id,
                        ?reason,
                        "execution outcome source remains incomplete",
                    );
                }
                Err(error) => {
                    let settlement = self
                        .execution_outcomes
                        .settle_reconciliation(
                            candidate.order_intent_id,
                            self.projection_worker_id,
                            OutcomeTaskSettlement::RetryAfter {
                                delay_secs: retry_delay_secs(claim.attempt_count),
                                error: error.to_string(),
                            },
                        )
                        .await;
                    if let Err(settlement_error) = settlement {
                        tracing::error!(
                            order_intent_id = %candidate.order_intent_id,
                            %settlement_error,
                            "failed to release execution reconciliation lease",
                        );
                    }
                    return Err(error.into());
                }
            }
        }
        Ok(had_claims)
    }

    async fn reconcile_rollup_page(
        &self,
        config: OutcomeReconciliationPassConfig,
        summary: &mut OutcomeReconciliationPassSummary,
    ) -> QuantResult<bool> {
        let claims = self
            .execution_rollups
            .claim_reconciliation(
                config.pass_started_at,
                self.projection_worker_id,
                60,
                config.candidate_batch_size,
            )
            .await?;
        let had_claims = !claims.is_empty();
        summary.rollup_candidates += usize_to_u64(claims.len())?;
        for claim in claims {
            let recommendation_id = claim.recommendation_id;
            let result = self
                .execution_rollups
                .reconcile_recommendation(recommendation_id, config.pass_started_at)
                .await;
            match result {
                Ok(ExecutionRollupReconciliationResult::Inserted(_)) => {
                    summary.rollup_inserted += 1;
                    self.execution_rollups
                        .settle_reconciliation(
                            recommendation_id,
                            self.projection_worker_id,
                            OutcomeTaskSettlement::Completed,
                        )
                        .await?;
                }
                Ok(ExecutionRollupReconciliationResult::AlreadyPresent(_)) => {
                    summary.rollup_existing += 1;
                    self.execution_rollups
                        .settle_reconciliation(
                            recommendation_id,
                            self.projection_worker_id,
                            OutcomeTaskSettlement::Completed,
                        )
                        .await?;
                }
                Ok(ExecutionRollupReconciliationResult::Deferred(reason)) => {
                    summary.rollup_deferred += 1;
                    self.execution_rollups
                        .settle_reconciliation(
                            recommendation_id,
                            self.projection_worker_id,
                            OutcomeTaskSettlement::RetryAfter {
                                delay_secs: retry_delay_secs(claim.attempt_count),
                                error: format!("{reason:?}"),
                            },
                        )
                        .await?;
                    tracing::debug!(
                        %recommendation_id,
                        ?reason,
                        "recommendation execution graph remains incomplete",
                    );
                }
                Err(error) => {
                    let settlement = self
                        .execution_rollups
                        .settle_reconciliation(
                            recommendation_id,
                            self.projection_worker_id,
                            OutcomeTaskSettlement::RetryAfter {
                                delay_secs: retry_delay_secs(claim.attempt_count),
                                error: error.to_string(),
                            },
                        )
                        .await;
                    if let Err(settlement_error) = settlement {
                        tracing::error!(
                            %recommendation_id,
                            %settlement_error,
                            "failed to release execution rollup reconciliation lease",
                        );
                    }
                    return Err(error.into());
                }
            }
        }
        Ok(had_claims)
    }
}

fn retry_delay_secs(attempt_count: i32) -> u64 {
    let exponent = u32::try_from(attempt_count.saturating_sub(1))
        .unwrap_or_default()
        .min(8);
    30_u64.saturating_mul(2_u64.saturating_pow(exponent))
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

fn inbox_from_observation(
    observation: &FinalizedResolutionObservation,
) -> Result<NewResolutionObservationInbox, ExecutionError> {
    let payout_ratios = observation.vector.payout_ratios();
    let raw_uri = ArtifactUri::parse(format!(
        "polygon://resolution/{}/{}/{}",
        observation.block_number, observation.transaction_hash, observation.log_index
    ))
    .map_err(|error| invariant_error(error.to_string()))?;
    let mut inbox = NewResolutionObservationInbox {
        source_checkpoint_hash: observation.source_checkpoint_hash,
        source_id: resolution_source_id(),
        instrument_key: resolution_instrument_key(),
        market_id: observation.market_id.clone(),
        denominator: observation.vector.denominator().clone(),
        yes_numerator: observation.vector.numerators()[0].clone(),
        no_numerator: observation.vector.numerators()[1].clone(),
        yes_payout_ratio: payout_ratios[0],
        no_payout_ratio: payout_ratios[1],
        oracle: observation.oracle.clone(),
        question_id: observation.question_id.clone(),
        transaction_hash: observation.transaction_hash.clone(),
        block_number: observation.block_number,
        block_hash: observation.block_hash.clone(),
        log_index: observation.log_index,
        resolved_at: observation.resolved_at,
        raw_payload_hash: ContentHash::from_bytes([0; 32]),
        raw_uri,
        provider_revision: observation.block_hash.clone(),
    };
    inbox.raw_payload_hash = inbox
        .expected_raw_payload_hash()
        .map_err(|error| invariant_error(error.to_string()))?;
    inbox
        .validate()
        .map_err(|error| invariant_error(error.to_string()))?;
    Ok(inbox)
}

fn fact_from_inbox(
    observation: &ResolutionObservationInboxInfo,
    market: &MarketInfo,
) -> Result<MarketResolutionRow, ExecutionError> {
    if observation.market_id != market.market_id {
        return Err(invariant_error(
            "resolution observation and catalog market identities differ",
        ));
    }
    MarketResolutionRow::seal(MarketResolutionFactInput {
        market_id: observation.market_id.clone(),
        token_ids: [market.yes_token_id.clone(), market.no_token_id.clone()],
        payout_ratios: [observation.yes_payout_ratio, observation.no_payout_ratio],
        resolved_at: observation.resolved_at.timestamp_millis(),
        observed_at: observation.available_at.timestamp_millis(),
        source_block_number: u64::try_from(observation.block_number)
            .map_err(|error| invariant_error(format!("source block is negative: {error}")))?,
        source_block_hash: observation.block_hash.clone(),
        source_transaction_hash: observation.transaction_hash.clone(),
        source_log_index: u64::try_from(observation.log_index)
            .map_err(|error| invariant_error(format!("source log index is negative: {error}")))?,
        source_checkpoint_hash: observation.source_checkpoint_hash,
    })
    .map_err(|error| invariant_error(error.to_string()))
}

fn validate_inbox_fact(
    fact: &MarketResolutionRow,
    observation: &ResolutionObservationInboxInfo,
    market: &MarketInfo,
) -> Result<(), ExecutionError> {
    fact.validate()
        .map_err(|error| invariant_error(error.to_string()))?;
    let expected = fact_from_inbox(observation, market)?;
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

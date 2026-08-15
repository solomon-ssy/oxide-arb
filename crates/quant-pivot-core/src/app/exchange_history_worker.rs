//! Finalized exchange-history fresh-boot reconstruction worker.

use std::{
    collections::BTreeMap,
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
    time::Duration,
};

use arc_swap::ArcSwap;
use blake3::Hasher;
use chrono::{DateTime, TimeDelta, Utc};
use quant_pivot_api::exchange::{
    AttestedHistoryChunk, CanonicalBlockHeader, ExchangeHistoryAttestor, ExchangeHistoryExtractor,
    ExchangeHistoryProjection, ExecutionProjectionError, ExtractedHistoryChunk, HistoryClientError,
    HistoryContinuityProofBasis, HistoryDigest, chunks_agree, history_token_ids, project_history,
};
use quant_pivot_error::{
    QuantError, QuantResult, exchange_history::ExchangeHistoryError, storage::StorageError,
};
use quant_pivot_models::{
    clickhouse::{
        ChDigest, ExchangeEventRow, ExchangeHistoryAcceptanceRow, ExchangeLogRawRow,
        ExchangeMatchRow, ExecutionParticipantRow, MarketExecutionRow,
    },
    config::FinalizedExchangeHistoryConfig,
    domain::data_plane::{
        ColdStartSloStatus, ExchangeHistoryChunkInfo, ExchangeHistoryChunkStatus,
        ExchangeHistoryContinuityBasis, ExchangeHistoryFrontier, ExchangeHistoryFrontierProgress,
        ExchangeHistoryPlanInfo, ExchangeHistoryQuarantineReason, ExchangeHistoryStage,
        NewExchangeHistoryChunk, NewExchangeHistoryPlan, NewExchangeHistoryQuarantine,
        ResolveAcceptedHistoryRange,
    },
    domain::ports::ExchangeHistoryProgressPort,
    types::{
        CRYPTO_PRICE_15M_BOOTSTRAP_PROFILE_ID, ContentHash, EvmBlockHash, MarketId,
        ResearchProfileArtifact, ServingAuthority, TokenId,
        WEATHER_FORECAST_24H_BOOTSTRAP_PROFILE_ID, builtin_research_profiles,
    },
};
use quant_pivot_repository::{
    clickhouse::ChFactWriter,
    traits::{ExchangeHistoryRepository, FactWriter, MarketRepository},
};
use serde::Serialize;
use tokio_util::sync::CancellationToken;
use uuid::Uuid;

use crate::infra::periodic_task::PeriodicTask;

const CHUNK_NAMESPACE: Uuid = Uuid::from_u128(0x6f0d_f3a4_7274_5e8f_9d92_7fb3_e3b1_e91a);
const POLYGON_CHAIN_ID: i64 = 137;
#[derive(Debug, Serialize)]
struct AvailabilityPolicyCommitment {
    chain_id: u64,
    finalized_only: bool,
    model_confirmation_blocks: u64,
    provider_agreement: &'static str,
}

struct ProjectionQuarantine {
    chunk_id: Uuid,
    frontier: ExchangeHistoryFrontier,
    from_block: u64,
    to_block: u64,
    attempt: i32,
    reason: ExchangeHistoryQuarantineReason,
    detail: String,
    created_at: DateTime<Utc>,
}

#[derive(Clone)]
pub struct ExchangeHistoryWriters {
    pub raw_logs: Arc<ChFactWriter<ExchangeLogRawRow>>,
    pub events: Arc<ChFactWriter<ExchangeEventRow>>,
    pub matches: Arc<ChFactWriter<ExchangeMatchRow>>,
    pub executions: Arc<ChFactWriter<MarketExecutionRow>>,
    pub participants: Arc<ChFactWriter<ExecutionParticipantRow>>,
    pub acceptance: Arc<ChFactWriter<ExchangeHistoryAcceptanceRow>>,
}

#[derive(Clone)]
pub struct ExchangeHistoryProgressHandle {
    inner: Arc<ArcSwap<ExchangeHistoryFrontierProgress>>,
}

impl ExchangeHistoryProgressHandle {
    #[must_use]
    pub fn fresh_boot() -> Self {
        Self {
            inner: Arc::new(ArcSwap::from_pointee(
                ExchangeHistoryFrontierProgress::fresh_boot(Utc::now()),
            )),
        }
    }

    #[must_use]
    pub fn snapshot(&self) -> Arc<ExchangeHistoryFrontierProgress> {
        self.inner.load_full()
    }

    fn publish(&self, progress: ExchangeHistoryFrontierProgress) {
        self.inner.store(Arc::new(progress));
    }

    pub fn set_stage(&self, stage: ExchangeHistoryStage) {
        let mut progress = self.inner.load().as_ref().clone();
        progress.stage = stage;
        progress.updated_at = Utc::now();
        self.publish(progress);
    }
}

impl ExchangeHistoryProgressPort for ExchangeHistoryProgressHandle {
    fn snapshot(&self) -> ExchangeHistoryFrontierProgress {
        self.inner.load().as_ref().clone()
    }
}

/// Reconstructs a dual-provider-attested semantic execution history. The
/// activation frontier always receives capacity before retention work.
pub struct ExchangeHistoryWorker {
    extractor: Arc<ExchangeHistoryExtractor>,
    attestor: Arc<ExchangeHistoryAttestor>,
    history_repo: Arc<dyn ExchangeHistoryRepository>,
    market_repo: Arc<dyn MarketRepository>,
    writers: ExchangeHistoryWriters,
    config: FinalizedExchangeHistoryConfig,
    policy_hash: ContentHash,
    progress: ExchangeHistoryProgressHandle,
    adaptive_chunk_blocks: AtomicU64,
    adaptive_success_count: AtomicU64,
}

impl ExchangeHistoryWorker {
    pub fn connect(
        history_repo: Arc<dyn ExchangeHistoryRepository>,
        market_repo: Arc<dyn MarketRepository>,
        writers: ExchangeHistoryWriters,
        config: FinalizedExchangeHistoryConfig,
        progress: ExchangeHistoryProgressHandle,
    ) -> QuantResult<Self> {
        let extractor = ExchangeHistoryExtractor::connect(&config)
            .map_err(|error| extraction_failure(&error))?;
        let attestor = ExchangeHistoryAttestor::connect(&config)
            .map_err(|error| attestation_failure(&error))?;
        let policy_hash = policy_hash(&config)?;
        let initial_chunk_blocks = config.max_blocks_per_chunk;
        Ok(Self {
            extractor: Arc::new(extractor),
            attestor: Arc::new(attestor),
            history_repo,
            market_repo,
            writers,
            config,
            policy_hash,
            progress,
            adaptive_chunk_blocks: AtomicU64::new(initial_chunk_blocks),
            adaptive_success_count: AtomicU64::new(0),
        })
    }

    #[must_use]
    pub fn progress(&self) -> ExchangeHistoryProgressHandle {
        self.progress.clone()
    }

    pub async fn run(self: Arc<Self>, shutdown: CancellationToken) -> QuantResult<()> {
        if !self.config.enabled {
            shutdown.cancelled().await;
            return Ok(());
        }
        let poll_secs = self.config.poll_secs;
        let worker = Arc::clone(&self);
        PeriodicTask::run(
            "exchange-history-worker",
            move || Duration::from_secs(poll_secs),
            0.05,
            true,
            shutdown,
            move || {
                let worker = Arc::clone(&worker);
                async move { worker.run_once().await }
            },
        )
        .await
    }

    /// Block application startup until the independent witness proves archive,
    /// finalized-head, block-by-hash, historical-log, and bytecode support.
    pub async fn probe(&self) -> QuantResult<()> {
        let probe = self
            .attestor
            .probe_archive()
            .await
            .map_err(|error| attestation_failure(&error))?;
        let to_block = probe
            .finalized_head
            .number
            .checked_sub(self.config.model_confirmation_blocks)
            .ok_or(ExchangeHistoryError::InvalidTime)?;
        let from_block =
            to_block.saturating_sub(self.config.min_blocks_per_chunk.saturating_sub(1));
        let (extracted, attested) = tokio::join!(
            self.extractor.fetch_chunk(from_block, to_block),
            self.attestor.fetch_chunk(from_block, to_block),
        );
        let extracted = extracted.map_err(|error| extraction_failure(&error))?;
        let attested = attested.map_err(|error| attestation_failure(&error))?;
        let continuity_agrees = self
            .attestor
            .verify_continuity(&extracted.continuity_proof)
            .await
            .map_err(|error| attestation_failure(&error))?;
        if !chunks_agree(&extracted, &attested) || !continuity_agrees {
            return Err(ExchangeHistoryError::ProviderMismatch {
                from_block,
                to_block,
            }
            .into());
        }
        Ok(())
    }

    pub async fn run_once(&self) -> QuantResult<()> {
        let finalized = self
            .attestor
            .finalized_head()
            .await
            .map_err(|error| attestation_failure(&error))?;
        let model_head = finalized
            .number
            .checked_sub(self.config.model_confirmation_blocks)
            .ok_or(ExchangeHistoryError::InvalidTime)?;
        let plan = self.ensure_plan(&finalized, model_head).await?;
        self.verify_plan_anchor(&plan).await?;
        self.reconcile_frontier(ExchangeHistoryFrontier::Activation, model_head)
            .await?;
        self.reconcile_frontier(ExchangeHistoryFrontier::Retention, model_head)
            .await?;
        let activation_start = plan_block(plan.activation_from_block)?;
        let activation_through = plan_block(plan.activation_through_block)?;
        self.publish_plan(&plan).await?;
        if self
            .advance_activation(activation_start, activation_through)
            .await?
        {
            return Ok(());
        }
        self.advance_retention(
            plan_block(plan.retention_from_block)?,
            activation_start,
            plan_block(plan.crypto_required_from_block)?,
            plan_block(plan.weather_required_from_block)?,
        )
        .await?;
        self.publish_ready(activation_through);
        Ok(())
    }

    async fn ensure_plan(
        &self,
        finalized: &CanonicalBlockHeader,
        model_head: u64,
    ) -> QuantResult<ExchangeHistoryPlanInfo> {
        let profiles =
            builtin_research_profiles().map_err(|detail| ExchangeHistoryError::Projection {
                detail: format!("built-in research profiles are invalid: {detail}"),
            })?;
        let mut bootstrap_refs = profiles
            .iter()
            .filter(|profile| {
                profile.spec.serving_authority == ServingAuthority::ReportOnlyWithLiveL2
            })
            .map(|profile| &profile.profile_ref)
            .collect::<Vec<_>>();
        bootstrap_refs.sort_unstable_by(|left, right| left.id.cmp(&right.id));
        if bootstrap_refs.len() != 3 {
            return Err(ExchangeHistoryError::Projection {
                detail: "fresh-boot history plan requires exactly three bootstrap profiles"
                    .to_owned(),
            }
            .into());
        }
        let mut profile_hasher = Hasher::new();
        profile_hasher.update(b"quant-pivot/exchange-history-bootstrap-profiles/v1\0");
        for profile_ref in bootstrap_refs {
            profile_hasher.update(profile_ref.id.as_str().as_bytes());
            profile_hasher.update(b"\0");
            profile_hasher.update(&profile_ref.version.to_be_bytes());
            profile_hasher.update(profile_ref.content_hash.as_bytes());
        }
        let bootstrap_profile_set_hash =
            ContentHash::from_bytes(*profile_hasher.finalize().as_bytes());
        if let Some(plan) = self.history_repo.load_plan(POLYGON_CHAIN_ID).await? {
            if plan.policy_hash != self.policy_hash
                || plan.bootstrap_profile_set_hash != bootstrap_profile_set_hash
            {
                return Err(ExchangeHistoryError::Attestation {
                    detail: "persisted history plan uses different policy or profile contracts"
                        .to_owned(),
                }
                .into());
            }
            return Ok(plan);
        }
        let crypto_days = profile_days(&profiles, CRYPTO_PRICE_15M_BOOTSTRAP_PROFILE_ID)?;
        let weather_days = profile_days(&profiles, WEATHER_FORECAST_24H_BOOTSTRAP_PROFILE_ID)?;
        if crypto_days > self.config.retention_frontier_days
            || weather_days > self.config.retention_frontier_days
        {
            return Err(ExchangeHistoryError::Projection {
                detail: "retention frontier is shorter than a bootstrap profile contract"
                    .to_owned(),
            }
            .into());
        }
        let (activation_from, crypto_from, weather_from, retention_from) = tokio::try_join!(
            self.frontier_start(
                finalized.timestamp,
                self.config.activation_frontier_days,
                model_head,
            ),
            self.frontier_start(finalized.timestamp, crypto_days, model_head),
            self.frontier_start(finalized.timestamp, weather_days, model_head),
            self.frontier_start(
                finalized.timestamp,
                self.config.retention_frontier_days,
                model_head,
            ),
        )?;
        let retention_through = activation_from
            .checked_sub(1)
            .ok_or(ExchangeHistoryError::InvalidTime)?;
        let mut preimage = Vec::with_capacity(128);
        preimage.extend_from_slice(b"quant-pivot/exchange-history-plan/v1\0");
        preimage.extend_from_slice(&POLYGON_CHAIN_ID.to_be_bytes());
        preimage.extend_from_slice(self.policy_hash.as_bytes());
        preimage.extend_from_slice(bootstrap_profile_set_hash.as_bytes());
        preimage.extend_from_slice(&finalized.number.to_be_bytes());
        preimage.extend_from_slice(finalized.hash.as_bytes());
        self.history_repo
            .create_or_load_plan(NewExchangeHistoryPlan {
                plan_id: Uuid::new_v5(&CHUNK_NAMESPACE, &preimage),
                chain_id: POLYGON_CHAIN_ID,
                policy_hash: self.policy_hash,
                bootstrap_profile_set_hash,
                finalized_anchor_block: block_i64(finalized.number)?,
                finalized_anchor_hash: block_hash(&finalized.hash)?,
                finalized_anchor_timestamp: i64::try_from(finalized.timestamp)
                    .map_err(|_| ExchangeHistoryError::InvalidTime)?,
                activation_from_block: block_i64(activation_from)?,
                activation_through_block: block_i64(model_head)?,
                crypto_required_from_block: block_i64(crypto_from)?,
                weather_required_from_block: block_i64(weather_from)?,
                retention_from_block: block_i64(retention_from)?,
                retention_through_block: block_i64(retention_through)?,
                created_at: Utc::now(),
            })
            .await
            .map_err(Into::into)
    }

    async fn verify_plan_anchor(&self, plan: &ExchangeHistoryPlanInfo) -> QuantResult<()> {
        let anchor = self
            .attestor
            .block_header(plan_block(plan.finalized_anchor_block)?)
            .await
            .map_err(|error| attestation_failure(&error))?;
        if anchor.hash != plan.finalized_anchor_hash.as_str()
            || i64::try_from(anchor.timestamp).ok() != Some(plan.finalized_anchor_timestamp)
        {
            return Err(ExchangeHistoryError::ProviderMismatch {
                from_block: anchor.number,
                to_block: anchor.number,
            }
            .into());
        }
        Ok(())
    }

    async fn frontier_start(
        &self,
        head_timestamp: u64,
        days: u32,
        upper_block: u64,
    ) -> QuantResult<u64> {
        let seconds = u64::from(days)
            .checked_mul(86_400)
            .ok_or(ExchangeHistoryError::InvalidTime)?;
        let timestamp = head_timestamp
            .checked_sub(seconds)
            .ok_or(ExchangeHistoryError::InvalidTime)?;
        self.attestor
            .block_at_or_after(timestamp, upper_block)
            .await
            .map(|header| header.number)
            .map_err(|error| attestation_failure(&error).into())
    }

    async fn reconcile_frontier(
        &self,
        frontier: ExchangeHistoryFrontier,
        model_head: u64,
    ) -> QuantResult<()> {
        let start = model_head.saturating_sub(self.config.rollback_buffer_blocks);
        let chunks = self
            .history_repo
            .accepted_from(frontier, block_i64(start)?)
            .await?;
        for chunk in chunks {
            let from_block = u64::try_from(chunk.from_block)
                .map_err(|_| ExchangeHistoryError::FrontierOverflow)?;
            let to_block = u64::try_from(chunk.to_block)
                .map_err(|_| ExchangeHistoryError::FrontierOverflow)?;
            let (first, last) = tokio::join!(
                self.attestor.block_header(from_block),
                self.attestor.block_header(to_block),
            );
            let first = first.map_err(|error| attestation_failure(&error))?;
            let last = last.map_err(|error| attestation_failure(&error))?;
            let hashes_match = chunk
                .first_block_hash
                .as_ref()
                .is_some_and(|hash| hash.as_str() == first.hash)
                && chunk
                    .last_block_hash
                    .as_ref()
                    .is_some_and(|hash| hash.as_str() == last.hash);
            if !hashes_match {
                self.rewind_chunks(frontier, &chunk).await?;
                break;
            }
        }
        Ok(())
    }

    async fn rewind_chunks(
        &self,
        frontier: ExchangeHistoryFrontier,
        divergent: &ExchangeHistoryChunkInfo,
    ) -> QuantResult<()> {
        let now = Utc::now();
        let detail = format!(
            "canonical block hash changed inside rollback buffer at range {}..={}",
            divergent.from_block, divergent.to_block
        );
        let evidence_hash = ContentHash::from_bytes(*blake3::hash(detail.as_bytes()).as_bytes());
        let rewound = self
            .history_repo
            .rewind_from(frontier, divergent.from_block, now)
            .await?;
        self.history_repo
            .quarantine_chunk(
                chunk_from_info(divergent, ExchangeHistoryChunkStatus::Quarantined, now),
                NewExchangeHistoryQuarantine {
                    quarantine_id: Uuid::now_v7(),
                    chunk_id: divergent.chunk_id,
                    reason: ExchangeHistoryQuarantineReason::ContinuityMismatch,
                    evidence_hash,
                    detail,
                    quarantined_at: now,
                },
            )
            .await?;
        for chunk in rewound {
            self.write_revocation(&chunk, now).await?;
        }
        self.publish_quarantine(ExchangeHistoryQuarantineReason::ContinuityMismatch);
        Ok(())
    }

    async fn advance_activation(&self, start: u64, target: u64) -> QuantResult<bool> {
        let latest = self
            .history_repo
            .latest_accepted(ExchangeHistoryFrontier::Activation)
            .await?;
        let next = next_block(latest.as_ref(), start)?;
        self.publish_target(latest.as_ref(), start, target);
        if next > target {
            self.publish_ready(target);
            return Ok(false);
        }
        let budget_end = next
            .saturating_add(self.config.hot_window_blocks_per_tick.saturating_sub(1))
            .min(target);
        self.process_budget(ExchangeHistoryFrontier::Activation, next, budget_end)
            .await?;
        Ok(true)
    }

    async fn advance_retention(
        &self,
        start: u64,
        activation_start: u64,
        crypto_start: u64,
        weather_start: u64,
    ) -> QuantResult<()> {
        let Some(target) = activation_start.checked_sub(1) else {
            return Ok(());
        };
        let earliest = self
            .history_repo
            .earliest_accepted(ExchangeHistoryFrontier::Retention)
            .await?;
        let next_end = earliest.as_ref().map_or(Ok(target), |row| {
            u64::try_from(row.from_block)
                .map(|block| block.saturating_sub(1))
                .map_err(|_| ExchangeHistoryError::FrontierOverflow)
        })?;
        if next_end < start {
            return Ok(());
        }
        let budget_start = next_end
            .saturating_sub(self.config.full_history_blocks_per_tick.saturating_sub(1))
            .max(start);
        self.process_reverse(
            ExchangeHistoryFrontier::Retention,
            budget_start,
            next_end,
            &[crypto_start, weather_start, start],
        )
        .await
    }

    async fn process_reverse(
        &self,
        frontier: ExchangeHistoryFrontier,
        budget_start: u64,
        mut next_end: u64,
        boundaries: &[u64],
    ) -> QuantResult<()> {
        while next_end >= budget_start {
            let desired_span = self.adaptive_chunk_blocks.load(Ordering::Relaxed).clamp(
                self.config.min_blocks_per_chunk,
                self.config.max_blocks_per_chunk,
            );
            let mut desired_start = next_end
                .saturating_sub(desired_span.saturating_sub(1))
                .max(budget_start);
            if let Some(boundary) = boundaries
                .iter()
                .copied()
                .filter(|boundary| *boundary > desired_start && *boundary <= next_end)
                .max()
            {
                desired_start = boundary;
            }
            let (extracted, attested) = self
                .fetch_agreed_reverse(frontier, desired_start, next_end)
                .await?;
            let accepted_start = extracted.from_block;
            self.record_chunk_success(next_end.saturating_sub(accepted_start).saturating_add(1));
            self.accept_chunk(frontier, extracted, attested).await?;
            let Some(previous_end) = accepted_start.checked_sub(1) else {
                break;
            };
            next_end = previous_end;
        }
        Ok(())
    }

    async fn fetch_agreed_reverse(
        &self,
        frontier: ExchangeHistoryFrontier,
        mut from_block: u64,
        to_block: u64,
    ) -> QuantResult<(ExtractedHistoryChunk, AttestedHistoryChunk)> {
        loop {
            match self.fetch_providers(frontier, from_block, to_block).await {
                Ok((extracted, attested)) => return Ok((extracted, attested)),
                Err(FetchFailure::Contract(error)) => return Err(error),
                Err(FetchFailure::Shrink) => {
                    let span = to_block.saturating_sub(from_block).saturating_add(1);
                    if span <= self.config.min_blocks_per_chunk {
                        return Err(ExchangeHistoryError::Attestation {
                            detail: "provider response cannot fit the minimum chunk budget"
                                .to_owned(),
                        }
                        .into());
                    }
                    let contracted = (span / 2).max(self.config.min_blocks_per_chunk);
                    self.adaptive_chunk_blocks
                        .store(contracted, Ordering::Relaxed);
                    self.adaptive_success_count.store(0, Ordering::Relaxed);
                    from_block = to_block.saturating_sub(contracted.saturating_sub(1));
                }
            }
        }
    }

    async fn process_budget(
        &self,
        frontier: ExchangeHistoryFrontier,
        mut next: u64,
        budget_end: u64,
    ) -> QuantResult<()> {
        while next <= budget_end {
            let desired_span = self.adaptive_chunk_blocks.load(Ordering::Relaxed).clamp(
                self.config.min_blocks_per_chunk,
                self.config.max_blocks_per_chunk,
            );
            let desired_end = next
                .saturating_add(desired_span.saturating_sub(1))
                .min(budget_end);
            let (extracted, attested) = self.fetch_agreed(frontier, next, desired_end).await?;
            let accepted_end = extracted.to_block;
            self.record_chunk_success(accepted_end.saturating_sub(next).saturating_add(1));
            self.accept_chunk(frontier, extracted, attested).await?;
            next = accepted_end.saturating_add(1);
        }
        Ok(())
    }

    async fn fetch_agreed(
        &self,
        frontier: ExchangeHistoryFrontier,
        from_block: u64,
        mut to_block: u64,
    ) -> QuantResult<(ExtractedHistoryChunk, AttestedHistoryChunk)> {
        loop {
            match self.fetch_providers(frontier, from_block, to_block).await {
                Ok((extracted, attested)) => return Ok((extracted, attested)),
                Err(FetchFailure::Contract(error)) => return Err(error),
                Err(FetchFailure::Shrink) => {
                    let span = to_block.saturating_sub(from_block).saturating_add(1);
                    if span <= self.config.min_blocks_per_chunk {
                        return Err(ExchangeHistoryError::Attestation {
                            detail: "provider response cannot fit the minimum chunk budget"
                                .to_owned(),
                        }
                        .into());
                    }
                    let contracted = (span / 2).max(self.config.min_blocks_per_chunk);
                    self.adaptive_chunk_blocks
                        .store(contracted, Ordering::Relaxed);
                    self.adaptive_success_count.store(0, Ordering::Relaxed);
                    to_block = from_block.saturating_add(contracted.saturating_sub(1));
                }
            }
        }
    }

    fn record_chunk_success(&self, accepted_span: u64) {
        let current = self.adaptive_chunk_blocks.load(Ordering::Relaxed);
        if current >= self.config.max_blocks_per_chunk || accepted_span < current {
            return;
        }
        let successes = self
            .adaptive_success_count
            .fetch_add(1, Ordering::Relaxed)
            .saturating_add(1);
        if successes < 64 {
            return;
        }
        self.adaptive_chunk_blocks.store(
            current
                .saturating_mul(2)
                .min(self.config.max_blocks_per_chunk),
            Ordering::Relaxed,
        );
        self.adaptive_success_count.store(0, Ordering::Relaxed);
    }

    async fn fetch_providers(
        &self,
        frontier: ExchangeHistoryFrontier,
        from_block: u64,
        to_block: u64,
    ) -> Result<(ExtractedHistoryChunk, AttestedHistoryChunk), FetchFailure> {
        let mut delay = self.config.retry_initial_ms;
        for attempt in 1..=self.config.retry_max_attempts {
            self.publish_stage(ExchangeHistoryStage::Extracting);
            let (extracted, attested) = tokio::join!(
                self.extractor.fetch_chunk(from_block, to_block),
                self.attestor.fetch_chunk(from_block, to_block),
            );
            match (extracted, attested) {
                (Ok(extracted), Ok(attested)) => {
                    self.publish_stage(ExchangeHistoryStage::Attesting);
                    let continuity_agrees = self
                        .attestor
                        .verify_continuity(&extracted.continuity_proof)
                        .await
                        .map_err(|error| {
                            FetchFailure::Contract(attestation_failure(&error).into())
                        })?;
                    if !chunks_agree(&extracted, &attested) || !continuity_agrees {
                        self.quarantine_mismatch(
                            frontier,
                            from_block,
                            to_block,
                            &extracted,
                            &attested,
                            continuity_agrees,
                        )
                        .await
                        .map_err(FetchFailure::Contract)?;
                        return Err(FetchFailure::Contract(
                            ExchangeHistoryError::ProviderMismatch {
                                from_block,
                                to_block,
                            }
                            .into(),
                        ));
                    }
                    return Ok((extracted, attested));
                }
                (Err(error), _) | (_, Err(error)) if shrinkable(&error) => {
                    return Err(FetchFailure::Shrink);
                }
                (extractor_result, attestor_result) => {
                    if attempt == self.config.retry_max_attempts {
                        return Err(FetchFailure::Contract(
                            match (extractor_result, attestor_result) {
                                (Err(error), _) => extraction_failure(&error).into(),
                                (_, Err(error)) => attestation_failure(&error).into(),
                                (Ok(_), Ok(_)) => ExchangeHistoryError::Attestation {
                                    detail: "provider retry state is inconsistent".to_owned(),
                                }
                                .into(),
                            },
                        ));
                    }
                    self.publish_retry(extractor_result.is_err(), attestor_result.is_err());
                    tokio::time::sleep(Duration::from_millis(delay)).await;
                    delay = delay.saturating_mul(2).min(self.config.retry_max_ms);
                }
            }
        }
        Err(FetchFailure::Contract(
            ExchangeHistoryError::Attestation {
                detail: "provider retry loop terminated without a result".to_owned(),
            }
            .into(),
        ))
    }

    async fn accept_chunk(
        &self,
        frontier: ExchangeHistoryFrontier,
        extracted: ExtractedHistoryChunk,
        attested: AttestedHistoryChunk,
    ) -> QuantResult<()> {
        let from_block = extracted.from_block;
        let to_block = extracted.to_block;
        let chunk_id = chunk_content_id(frontier, from_block, to_block, extracted.digest);
        let started_at = Utc::now();
        let attempt = self
            .history_repo
            .find_range(frontier, block_i64(from_block)?, block_i64(to_block)?)
            .await?
            .map_or(1, |row| row.attempt_count.saturating_add(1));
        if let Err(error) = self.validate_boundary(frontier, &extracted).await {
            self.quarantine_projection(ProjectionQuarantine {
                chunk_id,
                frontier,
                from_block,
                to_block,
                attempt,
                reason: ExchangeHistoryQuarantineReason::ParentHashMismatch,
                detail: error.to_string(),
                created_at: started_at,
            })
            .await?;
            return Err(error);
        }
        self.history_repo
            .save_chunk(chunk_row(
                chunk_id,
                frontier,
                from_block,
                to_block,
                ExchangeHistoryChunkStatus::Projecting,
                attempt,
                started_at,
            )?)
            .await?;
        self.publish_stage(ExchangeHistoryStage::Projecting);

        let token_ids = match history_token_ids(&extracted.logs) {
            Ok(token_ids) => token_ids,
            Err(error) => {
                let reason = projection_reason(&error);
                let detail = error.to_string();
                self.quarantine_projection(ProjectionQuarantine {
                    chunk_id,
                    frontier,
                    from_block,
                    to_block,
                    attempt,
                    reason,
                    detail: detail.clone(),
                    created_at: started_at,
                })
                .await?;
                return Err(projection_failure(detail).into());
            }
        };
        let markets = self
            .market_repo
            .find_by_tokens(&token_ids.into_iter().collect::<Vec<_>>())
            .await?;
        let mut identities = BTreeMap::<TokenId, MarketId>::new();
        for market in markets {
            identities.insert(market.yes_token_id.clone(), market.market_id.clone());
            identities.insert(market.no_token_id.clone(), market.market_id.clone());
        }
        let projection = match project_history(
            &extracted.logs,
            extracted.observed_at_millis,
            attested.observed_at_millis,
            self.policy_hash,
            chunk_id,
            |token_id| identities.get(token_id).cloned(),
        ) {
            Ok(projection) => projection,
            Err(error) => {
                let reason = projection_reason(&error);
                let detail = error.to_string();
                self.quarantine_projection(ProjectionQuarantine {
                    chunk_id,
                    frontier,
                    from_block,
                    to_block,
                    attempt,
                    reason,
                    detail: detail.clone(),
                    created_at: started_at,
                })
                .await?;
                return Err(projection_failure(detail).into());
            }
        };
        let now = Utc::now();
        self.write_projection(chunk_id, projection).await?;
        self.write_acceptance(chunk_id, frontier, &extracted, now, true)
            .await?;
        let accepted = accepted_row(
            chunk_id, frontier, &extracted, &attested, attempt, started_at, now,
        )?;
        self.history_repo.save_chunk(accepted).await?;
        self.history_repo
            .resolve_accepted_range(ResolveAcceptedHistoryRange {
                frontier,
                from_block: block_i64(from_block)?,
                to_block: block_i64(to_block)?,
                replacement_chunk_id: chunk_id,
                evidence_hash: ContentHash::from_bytes(extracted.digest.0),
                actor: "exchange_history_worker".to_owned(),
                resolved_at: now,
            })
            .await?;
        self.publish_accepted(frontier, from_block, to_block, extracted.logs.len(), now);
        Ok(())
    }

    async fn validate_boundary(
        &self,
        frontier: ExchangeHistoryFrontier,
        extracted: &ExtractedHistoryChunk,
    ) -> QuantResult<()> {
        let adjacent = match frontier {
            ExchangeHistoryFrontier::Activation => {
                let Some(previous) = self.history_repo.latest_accepted(frontier).await? else {
                    return Ok(());
                };
                let expected = previous
                    .to_block
                    .checked_add(1)
                    .ok_or(ExchangeHistoryError::FrontierOverflow)?;
                let linked = previous
                    .last_block_hash
                    .as_ref()
                    .is_some_and(|hash| hash.as_str() == extracted.first_block.parent_hash);
                expected == block_i64(extracted.from_block)? && linked
            }
            ExchangeHistoryFrontier::Retention => {
                let next = if let Some(next) = self.history_repo.earliest_accepted(frontier).await?
                {
                    next
                } else {
                    self.history_repo
                        .earliest_accepted(ExchangeHistoryFrontier::Activation)
                        .await?
                        .ok_or_else(|| ExchangeHistoryError::Projection {
                            detail: "retention cannot precede an empty activation frontier"
                                .to_owned(),
                        })?
                };
                let expected = extracted
                    .to_block
                    .checked_add(1)
                    .ok_or(ExchangeHistoryError::FrontierOverflow)?;
                let linked = next
                    .continuity_hash
                    .as_ref()
                    .is_some_and(|hash| hash.as_str() == extracted.last_block.hash);
                block_i64(expected)? == next.from_block && linked
            }
        };
        if !adjacent {
            return Err(ExchangeHistoryError::ParentDiscontinuity {
                block: extracted.from_block,
            }
            .into());
        }
        Ok(())
    }

    async fn write_acceptance(
        &self,
        chunk_id: Uuid,
        frontier: ExchangeHistoryFrontier,
        extracted: &ExtractedHistoryChunk,
        accepted_at: DateTime<Utc>,
        active: bool,
    ) -> QuantResult<()> {
        let effective_through_at = DateTime::from_timestamp(
            i64::try_from(extracted.last_block.timestamp)
                .map_err(|_| ExchangeHistoryError::InvalidTime)?,
            0,
        )
        .ok_or(ExchangeHistoryError::InvalidTime)?;
        let row = ExchangeHistoryAcceptanceRow {
            chunk_id,
            frontier: frontier.as_str().to_owned(),
            from_block: extracted.from_block,
            to_block: extracted.to_block,
            log_count: u64::try_from(extracted.logs.len())
                .map_err(|_| ExchangeHistoryError::FrontierOverflow)?,
            provider_digest: ChDigest::new(extracted.digest.0),
            first_block_hash: extracted.first_block.hash.clone(),
            last_block_hash: extracted.last_block.hash.clone(),
            effective_through_at: effective_through_at.timestamp_millis(),
            accepted_at: accepted_at.timestamp_millis(),
            active: u8::from(active),
            state_revision: state_revision(accepted_at)?,
            schema_version: ExchangeHistoryAcceptanceRow::SCHEMA_VERSION,
        };
        self.writers
            .acceptance
            .write_batch_idempotent(&dedup_token("acceptance", row.chunk_id, 0), vec![row])
            .await?;
        Ok(())
    }

    async fn write_revocation(
        &self,
        chunk: &ExchangeHistoryChunkInfo,
        revoked_at: DateTime<Utc>,
    ) -> QuantResult<()> {
        let digest = chunk
            .hypersync_digest
            .ok_or_else(|| projection_failure("accepted chunk lost provider digest".to_owned()))?;
        let first_block_hash = chunk
            .first_block_hash
            .as_ref()
            .ok_or_else(|| projection_failure("accepted chunk lost first block hash".to_owned()))?;
        let last_block_hash = chunk
            .last_block_hash
            .as_ref()
            .ok_or_else(|| projection_failure("accepted chunk lost last block hash".to_owned()))?;
        let effective_through_at = chunk.effective_through_at.ok_or_else(|| {
            projection_failure("accepted chunk lost effective-through timestamp".to_owned())
        })?;
        let accepted_at = chunk
            .accepted_at
            .ok_or_else(|| projection_failure("accepted chunk lost acceptance time".to_owned()))?;
        let row = ExchangeHistoryAcceptanceRow {
            chunk_id: chunk.chunk_id,
            frontier: chunk.frontier.as_str().to_owned(),
            from_block: u64::try_from(chunk.from_block)
                .map_err(|_| ExchangeHistoryError::FrontierOverflow)?,
            to_block: u64::try_from(chunk.to_block)
                .map_err(|_| ExchangeHistoryError::FrontierOverflow)?,
            log_count: u64::try_from(chunk.hypersync_count.unwrap_or_default())
                .map_err(|_| ExchangeHistoryError::FrontierOverflow)?,
            provider_digest: ChDigest::from(digest),
            first_block_hash: first_block_hash.as_str().to_owned(),
            last_block_hash: last_block_hash.as_str().to_owned(),
            effective_through_at: effective_through_at.timestamp_millis(),
            accepted_at: accepted_at.timestamp_millis(),
            active: 0,
            state_revision: state_revision(revoked_at)?,
            schema_version: ExchangeHistoryAcceptanceRow::SCHEMA_VERSION,
        };
        self.writers
            .acceptance
            .write_batch_idempotent(
                &dedup_token("acceptance-revoke", chunk.chunk_id, row.state_revision),
                vec![row],
            )
            .await?;
        Ok(())
    }

    async fn write_projection(
        &self,
        chunk_id: Uuid,
        projection: ExchangeHistoryProjection,
    ) -> QuantResult<()> {
        write_rows(
            &self.writers.raw_logs,
            "raw",
            chunk_id,
            self.config.batch_size,
            projection.raw_logs,
        )
        .await?;
        write_rows(
            &self.writers.events,
            "event",
            chunk_id,
            self.config.batch_size,
            projection.events,
        )
        .await?;
        write_rows(
            &self.writers.matches,
            "match",
            chunk_id,
            self.config.batch_size,
            projection.matches,
        )
        .await?;
        write_rows(
            &self.writers.executions,
            "execution",
            chunk_id,
            self.config.batch_size,
            projection.executions,
        )
        .await?;
        write_rows(
            &self.writers.participants,
            "participant",
            chunk_id,
            self.config.batch_size,
            projection.participants,
        )
        .await?;
        Ok(())
    }

    async fn quarantine_mismatch(
        &self,
        frontier: ExchangeHistoryFrontier,
        from_block: u64,
        to_block: u64,
        extracted: &ExtractedHistoryChunk,
        attested: &AttestedHistoryChunk,
        continuity_agrees: bool,
    ) -> QuantResult<()> {
        let chunk_id = range_attempt_id(frontier, from_block, to_block);
        let now = Utc::now();
        let evidence = mismatch_evidence(extracted.digest, attested.digest);
        let reason = if continuity_agrees {
            ExchangeHistoryQuarantineReason::ProviderMismatch
        } else {
            ExchangeHistoryQuarantineReason::ContinuityMismatch
        };
        self.history_repo
            .quarantine_chunk(
                chunk_row(
                    chunk_id,
                    frontier,
                    from_block,
                    to_block,
                    ExchangeHistoryChunkStatus::Quarantined,
                    1,
                    now,
                )?,
                NewExchangeHistoryQuarantine {
                    quarantine_id: Uuid::now_v7(),
                    chunk_id,
                    reason,
                    evidence_hash: evidence,
                    detail: if continuity_agrees {
                        "provider canonical count/digest/block proof mismatch".to_owned()
                    } else {
                        "HyperSync continuity proof differs from independent archive RPC".to_owned()
                    },
                    quarantined_at: now,
                },
            )
            .await?;
        self.publish_quarantine(reason);
        Ok(())
    }

    async fn quarantine_projection(&self, input: ProjectionQuarantine) -> QuantResult<()> {
        let ProjectionQuarantine {
            chunk_id,
            frontier,
            from_block,
            to_block,
            attempt,
            reason,
            detail,
            created_at,
        } = input;
        let now = Utc::now();
        let evidence_hash = ContentHash::from_bytes(*blake3::hash(detail.as_bytes()).as_bytes());
        self.history_repo
            .quarantine_chunk(
                chunk_row(
                    chunk_id,
                    frontier,
                    from_block,
                    to_block,
                    ExchangeHistoryChunkStatus::Quarantined,
                    attempt,
                    created_at,
                )?,
                NewExchangeHistoryQuarantine {
                    quarantine_id: Uuid::now_v7(),
                    chunk_id,
                    reason,
                    evidence_hash,
                    detail,
                    quarantined_at: now,
                },
            )
            .await?;
        self.publish_quarantine(reason);
        Ok(())
    }

    fn publish_stage(&self, stage: ExchangeHistoryStage) {
        let mut progress = self.progress.snapshot().as_ref().clone();
        progress.stage = stage;
        progress.updated_at = Utc::now();
        self.progress.publish(progress);
    }

    async fn publish_plan(&self, plan: &ExchangeHistoryPlanInfo) -> QuantResult<()> {
        let retention = self
            .history_repo
            .earliest_accepted(ExchangeHistoryFrontier::Retention)
            .await?;
        let mut progress = self.progress.snapshot().as_ref().clone();
        progress.activation_from_block = Some(plan_block(plan.activation_from_block)?);
        progress.target_block = Some(plan_block(plan.activation_through_block)?);
        progress.retention_from_block = Some(plan_block(plan.retention_from_block)?);
        progress.retention_accepted_from_block = retention
            .as_ref()
            .map(|row| plan_block(row.from_block))
            .transpose()?;
        progress.retention_through_block = Some(plan_block(plan.retention_through_block)?);
        progress.crypto_required_from_block = Some(plan_block(plan.crypto_required_from_block)?);
        progress.weather_required_from_block = Some(plan_block(plan.weather_required_from_block)?);
        progress.updated_at = Utc::now();
        self.progress.publish(progress);
        Ok(())
    }

    fn publish_target(&self, accepted: Option<&ExchangeHistoryChunkInfo>, start: u64, target: u64) {
        let mut progress = self.progress.snapshot().as_ref().clone();
        progress.activation_from_block = Some(start);
        progress.target_block = Some(target);
        progress.accepted_through_block = accepted.and_then(|row| u64::try_from(row.to_block).ok());
        progress.updated_at = Utc::now();
        self.progress.publish(progress);
    }

    fn publish_retry(&self, hypersync: bool, attestor: bool) {
        let mut progress = self.progress.snapshot().as_ref().clone();
        progress.hypersync_retry_count = progress
            .hypersync_retry_count
            .saturating_add(u64::from(hypersync));
        progress.attestor_retry_count = progress
            .attestor_retry_count
            .saturating_add(u64::from(attestor));
        progress.updated_at = Utc::now();
        self.progress.publish(progress);
    }

    fn publish_accepted(
        &self,
        frontier: ExchangeHistoryFrontier,
        from_block: u64,
        to_block: u64,
        logs: usize,
        now: DateTime<Utc>,
    ) {
        let mut progress = self.progress.snapshot().as_ref().clone();
        match frontier {
            ExchangeHistoryFrontier::Activation => {
                progress.accepted_through_block = Some(to_block);
            }
            ExchangeHistoryFrontier::Retention => {
                progress.retention_accepted_from_block = Some(from_block);
            }
        }
        progress.blocks_processed = progress
            .blocks_processed
            .saturating_add(to_block.saturating_sub(from_block).saturating_add(1));
        progress.logs_accepted = progress
            .logs_accepted
            .saturating_add(u64::try_from(logs).unwrap_or(u64::MAX));
        let elapsed_ms = now
            .signed_duration_since(progress.started_at)
            .num_milliseconds()
            .max(1);
        let elapsed_ms = u64::try_from(elapsed_ms).unwrap_or(u64::MAX);
        progress.block_rate_milli = progress
            .blocks_processed
            .saturating_mul(1_000_000)
            .checked_div(elapsed_ms)
            .unwrap_or(0);
        if frontier == ExchangeHistoryFrontier::Activation
            && let (Some(target), Some(accepted)) =
                (progress.target_block, progress.accepted_through_block)
            && progress.block_rate_milli > 0
        {
            let remaining = target.saturating_sub(accepted);
            let eta_secs = remaining
                .saturating_mul(1_000)
                .div_ceil(progress.block_rate_milli);
            let eta_secs = i64::try_from(eta_secs).unwrap_or(i64::MAX);
            progress.projected_completion_at = now.checked_add_signed(TimeDelta::seconds(eta_secs));
            progress.slo_status = if eta_secs > 72 * 3_600 {
                ColdStartSloStatus::Violation
            } else if eta_secs > 48 * 3_600 {
                ColdStartSloStatus::Warning
            } else {
                ColdStartSloStatus::OnTrack
            };
        }
        progress.updated_at = now;
        self.progress.publish(progress);
    }

    fn publish_quarantine(&self, reason: ExchangeHistoryQuarantineReason) {
        let mut progress = self.progress.snapshot().as_ref().clone();
        progress.stage = ExchangeHistoryStage::Quarantined;
        progress.quarantine_count = progress.quarantine_count.saturating_add(1);
        if reason == ExchangeHistoryQuarantineReason::UnknownToken {
            progress.unresolved_count = progress.unresolved_count.saturating_add(1);
        }
        progress.updated_at = Utc::now();
        self.progress.publish(progress);
    }

    fn publish_ready(&self, target: u64) {
        let now = Utc::now();
        let mut progress = self.progress.snapshot().as_ref().clone();
        progress.stage = ExchangeHistoryStage::ActivationReady;
        progress.accepted_through_block = Some(target);
        progress.projected_completion_at = None;
        progress.updated_at = now;
        self.progress.publish(progress);
    }
}

enum FetchFailure {
    Shrink,
    Contract(QuantError),
}

async fn write_rows<T>(
    writer: &Arc<ChFactWriter<T>>,
    table_key: &str,
    chunk_id: Uuid,
    batch_size: usize,
    rows: Vec<T>,
) -> Result<(), StorageError>
where
    T: Send + Sync + 'static,
    ChFactWriter<T>: FactWriter<T>,
{
    let size = batch_size.max(1);
    let mut rows = rows.into_iter();
    let mut batch_index = 0_u64;
    loop {
        let batch = rows.by_ref().take(size).collect::<Vec<_>>();
        if batch.is_empty() {
            return Ok(());
        }
        let token = dedup_token(table_key, chunk_id, batch_index);
        writer.write_batch_idempotent(&token, batch).await?;
        batch_index = batch_index.saturating_add(1);
    }
}

fn next_block(latest: Option<&ExchangeHistoryChunkInfo>, start: u64) -> QuantResult<u64> {
    latest.map_or(Ok(start), |row| {
        u64::try_from(row.to_block)
            .map(|block| block.saturating_add(1))
            .map_err(|_| ExchangeHistoryError::FrontierOverflow.into())
    })
}

fn range_attempt_id(frontier: ExchangeHistoryFrontier, from_block: u64, to_block: u64) -> Uuid {
    let frontier = match frontier {
        ExchangeHistoryFrontier::Activation => "activation",
        ExchangeHistoryFrontier::Retention => "retention",
    };
    Uuid::new_v5(
        &CHUNK_NAMESPACE,
        format!("polygon:{frontier}:{from_block}:{to_block}").as_bytes(),
    )
}

fn chunk_content_id(
    frontier: ExchangeHistoryFrontier,
    from_block: u64,
    to_block: u64,
    digest: HistoryDigest,
) -> Uuid {
    let frontier = match frontier {
        ExchangeHistoryFrontier::Activation => "activation",
        ExchangeHistoryFrontier::Retention => "retention",
    };
    let mut preimage = format!("polygon:{frontier}:{from_block}:{to_block}:").into_bytes();
    preimage.extend_from_slice(&digest.0);
    Uuid::new_v5(&CHUNK_NAMESPACE, &preimage)
}

fn state_revision(at: DateTime<Utc>) -> QuantResult<u64> {
    u64::try_from(at.timestamp_micros()).map_err(|_| ExchangeHistoryError::InvalidTime.into())
}

fn chunk_row(
    chunk_id: Uuid,
    frontier: ExchangeHistoryFrontier,
    from_block: u64,
    to_block: u64,
    status: ExchangeHistoryChunkStatus,
    attempt_count: i32,
    created_at: DateTime<Utc>,
) -> QuantResult<NewExchangeHistoryChunk> {
    let now = Utc::now();
    Ok(NewExchangeHistoryChunk {
        chunk_id,
        frontier,
        from_block: block_i64(from_block)?,
        to_block: block_i64(to_block)?,
        status,
        attempt_count,
        hypersync_count: None,
        attestor_count: None,
        hypersync_digest: None,
        attestor_digest: None,
        first_block_hash: None,
        last_block_hash: None,
        archive_height: None,
        continuity_basis: None,
        continuity_block: None,
        continuity_hash: None,
        effective_through_at: None,
        accepted_at: None,
        created_at,
        updated_at: now,
    })
}

fn accepted_row(
    chunk_id: Uuid,
    frontier: ExchangeHistoryFrontier,
    extracted: &ExtractedHistoryChunk,
    attested: &AttestedHistoryChunk,
    attempt_count: i32,
    created_at: DateTime<Utc>,
    accepted_at: DateTime<Utc>,
) -> QuantResult<NewExchangeHistoryChunk> {
    Ok(NewExchangeHistoryChunk {
        chunk_id,
        frontier,
        from_block: block_i64(extracted.from_block)?,
        to_block: block_i64(extracted.to_block)?,
        status: ExchangeHistoryChunkStatus::Accepted,
        attempt_count,
        hypersync_count: Some(count_i64(extracted.logs.len())?),
        attestor_count: Some(count_i64(attested.logs.len())?),
        hypersync_digest: Some(ContentHash::from_bytes(extracted.digest.0)),
        attestor_digest: Some(ContentHash::from_bytes(attested.digest.0)),
        first_block_hash: Some(block_hash(&extracted.first_block.hash)?),
        last_block_hash: Some(block_hash(&extracted.last_block.hash)?),
        archive_height: Some(block_i64(extracted.archive_height)?),
        continuity_basis: Some(match extracted.continuity_proof.basis {
            HistoryContinuityProofBasis::HyperSyncRollbackGuard => {
                ExchangeHistoryContinuityBasis::HyperSyncRollbackGuard
            }
            HistoryContinuityProofBasis::HyperSyncBoundaryHeaders => {
                ExchangeHistoryContinuityBasis::HyperSyncBoundaryHeaders
            }
        }),
        continuity_block: Some(block_i64(
            extracted
                .from_block
                .checked_sub(1)
                .ok_or(ExchangeHistoryError::FrontierOverflow)?,
        )?),
        continuity_hash: Some(block_hash(&extracted.first_block.parent_hash)?),
        effective_through_at: Some(
            DateTime::from_timestamp(
                i64::try_from(extracted.last_block.timestamp)
                    .map_err(|_| ExchangeHistoryError::InvalidTime)?,
                0,
            )
            .ok_or(ExchangeHistoryError::InvalidTime)?,
        ),
        accepted_at: Some(accepted_at),
        created_at,
        updated_at: accepted_at,
    })
}

fn chunk_from_info(
    chunk: &ExchangeHistoryChunkInfo,
    status: ExchangeHistoryChunkStatus,
    updated_at: DateTime<Utc>,
) -> NewExchangeHistoryChunk {
    NewExchangeHistoryChunk {
        chunk_id: chunk.chunk_id,
        frontier: chunk.frontier,
        from_block: chunk.from_block,
        to_block: chunk.to_block,
        status,
        attempt_count: chunk.attempt_count,
        hypersync_count: chunk.hypersync_count,
        attestor_count: chunk.attestor_count,
        hypersync_digest: chunk.hypersync_digest,
        attestor_digest: chunk.attestor_digest,
        first_block_hash: chunk.first_block_hash.clone(),
        last_block_hash: chunk.last_block_hash.clone(),
        archive_height: chunk.archive_height,
        continuity_basis: chunk.continuity_basis,
        continuity_block: chunk.continuity_block,
        continuity_hash: chunk.continuity_hash.clone(),
        effective_through_at: chunk.effective_through_at,
        accepted_at: chunk.accepted_at,
        created_at: chunk.created_at,
        updated_at,
    }
}

fn policy_hash(config: &FinalizedExchangeHistoryConfig) -> QuantResult<ContentHash> {
    let commitment = AvailabilityPolicyCommitment {
        chain_id: 137,
        finalized_only: true,
        model_confirmation_blocks: config.model_confirmation_blocks,
        provider_agreement: "hypersync_plus_independent_archive_rpc_exact_v1",
    };
    let bytes =
        serde_json::to_vec(&commitment).map_err(|error| ExchangeHistoryError::Projection {
            detail: format!("availability policy serialization failed: {error}"),
        })?;
    Ok(ContentHash::from_bytes(*blake3::hash(&bytes).as_bytes()))
}

fn dedup_token(table_key: &str, chunk_id: Uuid, batch_index: u64) -> ContentHash {
    let mut hasher = Hasher::new();
    hasher.update(b"quant-pivot/exchange-history-insert/v1\0");
    hasher.update(table_key.as_bytes());
    hasher.update(chunk_id.as_bytes());
    hasher.update(&batch_index.to_be_bytes());
    ContentHash::from_bytes(*hasher.finalize().as_bytes())
}

fn mismatch_evidence(left: HistoryDigest, right: HistoryDigest) -> ContentHash {
    let mut hasher = Hasher::new();
    hasher.update(b"quant-pivot/exchange-history-mismatch/v1\0");
    hasher.update(&left.0);
    hasher.update(&right.0);
    ContentHash::from_bytes(*hasher.finalize().as_bytes())
}

fn profile_days(profiles: &[ResearchProfileArtifact], profile_id: &str) -> QuantResult<u32> {
    profiles
        .iter()
        .find(|profile| profile.profile_ref.id.as_str() == profile_id)
        .ok_or_else(|| ExchangeHistoryError::Projection {
            detail: format!("built-in bootstrap profile {profile_id} is missing"),
        })?
        .spec
        .required_days()
        .map_err(|detail| ExchangeHistoryError::Projection { detail }.into())
}

fn plan_block(block: i64) -> QuantResult<u64> {
    u64::try_from(block).map_err(|_| ExchangeHistoryError::FrontierOverflow.into())
}

const fn projection_reason(error: &ExecutionProjectionError) -> ExchangeHistoryQuarantineReason {
    match error {
        ExecutionProjectionError::UnknownToken { .. } => {
            ExchangeHistoryQuarantineReason::UnknownToken
        }
        ExecutionProjectionError::MissingAggregate
        | ExecutionProjectionError::AggregateMismatch
        | ExecutionProjectionError::MissingMaker => {
            ExchangeHistoryQuarantineReason::MissingCorrelation
        }
        ExecutionProjectionError::UnknownContract | ExecutionProjectionError::ContractInterval => {
            ExchangeHistoryQuarantineReason::ContractMismatch
        }
        ExecutionProjectionError::DecodeFailure
        | ExecutionProjectionError::InvalidAmount
        | ExecutionProjectionError::ZeroExecution
        | ExecutionProjectionError::InvalidTimestamp
        | ExecutionProjectionError::RemovedLog => ExchangeHistoryQuarantineReason::DecodeFailure,
    }
}

fn shrinkable(error: &HistoryClientError) -> bool {
    match error {
        HistoryClientError::ResponseBudget { .. } => true,
        HistoryClientError::RpcRejected { message, .. } => {
            let message = message.to_ascii_lowercase();
            message.contains("block range")
                || message.contains("response size")
                || message.contains("too many results")
                || message.contains("returned more than")
        }
        _ => false,
    }
}

fn extraction_failure(error: &HistoryClientError) -> ExchangeHistoryError {
    ExchangeHistoryError::Extraction {
        detail: error.to_string(),
    }
}

fn attestation_failure(error: &HistoryClientError) -> ExchangeHistoryError {
    ExchangeHistoryError::Attestation {
        detail: error.to_string(),
    }
}

const fn projection_failure(detail: String) -> ExchangeHistoryError {
    ExchangeHistoryError::Projection { detail }
}

fn block_i64(block: u64) -> QuantResult<i64> {
    i64::try_from(block).map_err(|_| ExchangeHistoryError::FrontierOverflow.into())
}

fn count_i64(count: usize) -> QuantResult<i64> {
    i64::try_from(count).map_err(|_| ExchangeHistoryError::FrontierOverflow.into())
}

fn block_hash(value: &str) -> QuantResult<EvmBlockHash> {
    EvmBlockHash::parse(value).map_err(|error| {
        projection_failure(format!("invalid canonical block hash: {error}")).into()
    })
}

#[cfg(test)]
mod tests {
    use super::{HistoryClientError, shrinkable};

    #[test]
    fn shrink_classifier_is_bounded() {
        assert!(shrinkable(&HistoryClientError::ResponseBudget { limit: 1 }));
        assert!(shrinkable(&HistoryClientError::RpcRejected {
            method: "eth_getLogs",
            code: -32600,
            message: "request exceeds the supported block range".to_owned(),
        }));
        assert!(!shrinkable(&HistoryClientError::RpcRejected {
            method: "eth_getLogs",
            code: -32005,
            message: "rate limit exceeded".to_owned(),
        }));
    }
}

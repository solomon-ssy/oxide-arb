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
use async_trait::async_trait;
use blake3::Hasher;
use chrono::{DateTime, TimeDelta, Utc};
use quant_pivot_api::exchange::{
    execution_projector::{
        ExchangeHistoryProjection, ExecutionProjectionError, history_token_ids, project_history,
    },
    history_client::{
        ArchiveProbe, AttestedHistoryChunk, CanonicalBlockHeader, ExchangeHistoryAttestor,
        ExchangeHistoryExtractor, ExtractedHistoryChunk, HistoryClientError,
        HistoryContinuityProof, HistoryContinuityProofBasis, chunks_agree,
    },
};
use quant_pivot_error::{
    QuantError, QuantResult, exchange_history::ExchangeHistoryError, storage::StorageError,
};
use quant_pivot_models::{
    clickhouse::{
        ChDigest, ExchangeEventRow, ExchangeFeeChargeRow, ExchangeHistoryAcceptanceRow,
        ExchangeLogRawRow, ExchangeMatchRow, ExecutionParticipantRow, MarketExecutionRow,
    },
    config::FinalizedExchangeHistoryConfig,
    domain::data_plane::{
        ColdStartSloStatus, CreateHistoryServingHeadSeal, ExchangeHistoryChunkInfo,
        ExchangeHistoryChunkStatus, ExchangeHistoryContinuityBasis, ExchangeHistoryFrontier,
        ExchangeHistoryFrontierProgress, ExchangeHistoryPlanInfo,
        ExchangeHistoryQuarantineEvidence, ExchangeHistoryQuarantineKind, ExchangeHistoryStage,
        HistorySealChunkRef, NewExchangeHistoryChunk, NewExchangeHistoryPlan,
        NewExchangeHistoryQuarantine, NewHistoryServingHeadSeal, ResolveAcceptedHistoryRange,
    },
    domain::ports::ExchangeHistoryProgressPort,
    types::{
        CRYPTO_PRICE_15M_BOOTSTRAP_PROFILE_ID, ContentHash, EvmBlockHash, HistoryServingHeadSealId,
        MarketId, ResearchProfileArtifact, ServingAuthority, TokenId,
        WEATHER_FORECAST_24H_BOOTSTRAP_PROFILE_ID, builtin_research_profiles,
    },
};
use quant_pivot_repository::traits::{ExchangeHistoryRepository, FactWriter, MarketRepository};
use serde::Serialize;
use tokio_util::sync::CancellationToken;
use uuid::Uuid;

use crate::{infra::periodic_task::PeriodicTask, observability::metrics_hub::MetricsHub};

const CHUNK_NAMESPACE: Uuid = Uuid::from_u128(0x6f0d_f3a4_7274_5e8f_9d92_7fb3_e3b1_e91a);
const POLYGON_CHAIN_ID: i64 = 137;
const SERVING_HEAD_CAS_ATTEMPTS: usize = 4;
#[derive(Debug, Serialize)]
struct AvailabilityPolicyCommitment {
    chain_id: u64,
    finalized_only: bool,
    model_confirmation_blocks: u64,
    rollback_buffer_blocks: u64,
    activation_frontier_days: u32,
    retention_frontier_days: u32,
    provider_agreement: &'static str,
    extractor_provider_id: String,
    attestor_provider_id: String,
}

struct ProjectionQuarantine {
    chunk_id: Uuid,
    frontier: ExchangeHistoryFrontier,
    from_block: u64,
    to_block: u64,
    attempt: i32,
    kind: ExchangeHistoryQuarantineKind,
    evidence: ExchangeHistoryQuarantineEvidence,
    created_at: DateTime<Utc>,
}

#[derive(Clone)]
pub struct ExchangeHistoryWriters {
    pub raw_logs: Arc<dyn FactWriter<ExchangeLogRawRow>>,
    pub events: Arc<dyn FactWriter<ExchangeEventRow>>,
    pub fee_charges: Arc<dyn FactWriter<ExchangeFeeChargeRow>>,
    pub matches: Arc<dyn FactWriter<ExchangeMatchRow>>,
    pub executions: Arc<dyn FactWriter<MarketExecutionRow>>,
    pub participants: Arc<dyn FactWriter<ExecutionParticipantRow>>,
    pub acceptance: Arc<dyn FactWriter<ExchangeHistoryAcceptanceRow>>,
}

#[async_trait]
trait HistoryExtractorSource: Send + Sync {
    async fn fetch_chunk(
        &self,
        from_block: u64,
        to_block: u64,
    ) -> Result<ExtractedHistoryChunk, HistoryClientError>;

    async fn shutdown(&self) -> Result<(), HistoryClientError> {
        Ok(())
    }
}

#[async_trait]
impl HistoryExtractorSource for ExchangeHistoryExtractor {
    async fn fetch_chunk(
        &self,
        from_block: u64,
        to_block: u64,
    ) -> Result<ExtractedHistoryChunk, HistoryClientError> {
        Self::fetch_chunk(self, from_block, to_block).await
    }

    async fn shutdown(&self) -> Result<(), HistoryClientError> {
        Self::shutdown(self).await
    }
}

#[async_trait]
trait HistoryAttestorSource: Send + Sync {
    async fn probe_archive(&self) -> Result<ArchiveProbe, HistoryClientError>;

    async fn finalized_head(&self) -> Result<CanonicalBlockHeader, HistoryClientError>;

    async fn block_header(
        &self,
        block_number: u64,
    ) -> Result<CanonicalBlockHeader, HistoryClientError>;

    async fn block_at_or_after(
        &self,
        timestamp: u64,
        upper_block: u64,
    ) -> Result<CanonicalBlockHeader, HistoryClientError>;

    async fn fetch_chunk(
        &self,
        from_block: u64,
        to_block: u64,
    ) -> Result<AttestedHistoryChunk, HistoryClientError>;

    async fn verify_continuity(
        &self,
        proof: &HistoryContinuityProof,
    ) -> Result<bool, HistoryClientError>;
}

#[async_trait]
impl HistoryAttestorSource for ExchangeHistoryAttestor {
    async fn probe_archive(&self) -> Result<ArchiveProbe, HistoryClientError> {
        Self::probe_archive(self).await
    }

    async fn finalized_head(&self) -> Result<CanonicalBlockHeader, HistoryClientError> {
        Self::finalized_head(self).await
    }

    async fn block_header(
        &self,
        block_number: u64,
    ) -> Result<CanonicalBlockHeader, HistoryClientError> {
        Self::block_header(self, block_number).await
    }

    async fn block_at_or_after(
        &self,
        timestamp: u64,
        upper_block: u64,
    ) -> Result<CanonicalBlockHeader, HistoryClientError> {
        Self::block_at_or_after(self, timestamp, upper_block).await
    }

    async fn fetch_chunk(
        &self,
        from_block: u64,
        to_block: u64,
    ) -> Result<AttestedHistoryChunk, HistoryClientError> {
        Self::fetch_chunk(self, from_block, to_block).await
    }

    async fn verify_continuity(
        &self,
        proof: &HistoryContinuityProof,
    ) -> Result<bool, HistoryClientError> {
        Self::verify_continuity(self, proof).await
    }
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
    extractor: Arc<dyn HistoryExtractorSource>,
    attestor: Arc<dyn HistoryAttestorSource>,
    history_repo: Arc<dyn ExchangeHistoryRepository>,
    market_repo: Arc<dyn MarketRepository>,
    writers: ExchangeHistoryWriters,
    config: FinalizedExchangeHistoryConfig,
    policy_hash: ContentHash,
    progress: ExchangeHistoryProgressHandle,
    metrics: Arc<MetricsHub>,
    adaptive_chunk_blocks: AtomicU64,
    adaptive_success_count: AtomicU64,
}

impl ExchangeHistoryWorker {
    /// Canonical commitment to the configured Polygon history-availability policy.
    pub fn availability_policy_hash(
        config: &FinalizedExchangeHistoryConfig,
    ) -> QuantResult<ContentHash> {
        let commitment = AvailabilityPolicyCommitment {
            chain_id: u64::try_from(POLYGON_CHAIN_ID)
                .map_err(|_| ExchangeHistoryError::InvalidTime)?,
            finalized_only: true,
            model_confirmation_blocks: config.model_confirmation_blocks,
            rollback_buffer_blocks: config.rollback_buffer_blocks,
            activation_frontier_days: config.activation_frontier_days,
            retention_frontier_days: config.retention_frontier_days,
            provider_agreement: "hypersync_plus_independent_archive_rpc_exact_v1",
            extractor_provider_id: config.hypersync.provider_id.clone(),
            attestor_provider_id: config.attestor.provider_id.clone(),
        };
        let bytes =
            serde_json::to_vec(&commitment).map_err(|error| ExchangeHistoryError::Projection {
                detail: format!("availability policy serialization failed: {error}"),
            })?;
        Ok(ContentHash::from_bytes(*blake3::hash(&bytes).as_bytes()))
    }

    /// Canonical identity of one immutable Polygon history plan.
    #[must_use]
    pub fn plan_id(
        finalized_block: u64,
        finalized_hash: &EvmBlockHash,
        policy_hash: ContentHash,
        bootstrap_profile_set_hash: ContentHash,
    ) -> Uuid {
        let mut preimage = Vec::with_capacity(128);
        preimage.extend_from_slice(b"quant-pivot/exchange-history-plan/v1\0");
        preimage.extend_from_slice(&POLYGON_CHAIN_ID.to_be_bytes());
        preimage.extend_from_slice(policy_hash.as_bytes());
        preimage.extend_from_slice(bootstrap_profile_set_hash.as_bytes());
        preimage.extend_from_slice(&finalized_block.to_be_bytes());
        preimage.extend_from_slice(finalized_hash.as_str().as_bytes());
        Uuid::new_v5(&CHUNK_NAMESPACE, &preimage)
    }

    /// Canonical content identity of one accepted Polygon history chunk.
    #[must_use]
    pub fn chunk_id(
        frontier: ExchangeHistoryFrontier,
        from_block: u64,
        to_block: u64,
        digest: ContentHash,
    ) -> Uuid {
        let mut preimage =
            format!("polygon:{}:{from_block}:{to_block}:", frontier.as_str()).into_bytes();
        preimage.extend_from_slice(digest.as_bytes());
        Uuid::new_v5(&CHUNK_NAMESPACE, &preimage)
    }

    /// Canonical idempotency token for the active acceptance commit marker.
    #[must_use]
    pub fn acceptance_token(chunk_id: Uuid) -> ContentHash {
        dedup_token("acceptance", chunk_id, 0)
    }

    /// Canonical identity of one immutable serving-head preimage.
    #[must_use]
    pub fn serving_head_id(
        plan_id: Uuid,
        previous_seal_id: Option<HistoryServingHeadSealId>,
        chunks: &[HistorySealChunkRef],
    ) -> HistoryServingHeadSealId {
        let mut preimage = Vec::with_capacity(chunks.len().saturating_mul(40).saturating_add(96));
        preimage.extend_from_slice(b"quant-pivot/history-serving-head-id/v1\0");
        preimage.extend_from_slice(plan_id.as_bytes());
        match previous_seal_id {
            Some(previous_seal_id) => {
                preimage.push(1);
                preimage.extend_from_slice(previous_seal_id.as_uuid_ref().as_bytes());
            }
            None => preimage.push(0),
        }
        for chunk in chunks {
            preimage.extend_from_slice(chunk.chunk_id.as_bytes());
            preimage.extend_from_slice(&chunk.state_revision.to_be_bytes());
        }
        HistoryServingHeadSealId::new(Uuid::new_v5(&CHUNK_NAMESPACE, &preimage))
    }

    fn is_head_cas_conflict(error: &StorageError) -> bool {
        matches!(
            error,
            StorageError::StateConflict { entity, detail, .. }
                if *entity == "quant_history_serving_head_seal"
                    && detail == "serving head predecessor is not the latest immutable head"
        )
    }

    /// Monotone cross-store revision for an acceptance lifecycle timestamp.
    pub fn state_revision(at: DateTime<Utc>) -> QuantResult<u64> {
        u64::try_from(at.timestamp_micros()).map_err(|_| ExchangeHistoryError::InvalidTime.into())
    }

    /// Canonical commitment to the complete analysis-only bootstrap-profile set.
    pub fn bootstrap_profile_set_hash() -> QuantResult<ContentHash> {
        let profiles =
            builtin_research_profiles().map_err(|detail| ExchangeHistoryError::Projection {
                detail: format!("built-in research profiles are invalid: {detail}"),
            })?;
        Self::hash_bootstrap_profiles(&profiles)
    }

    fn hash_bootstrap_profiles(profiles: &[ResearchProfileArtifact]) -> QuantResult<ContentHash> {
        let mut bootstrap_refs = profiles
            .iter()
            .filter(|profile| {
                profile.spec.serving_authority == ServingAuthority::AnalysisOnlyWithLiveL2
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
        Ok(ContentHash::from_bytes(
            *profile_hasher.finalize().as_bytes(),
        ))
    }

    pub fn connect(
        history_repo: Arc<dyn ExchangeHistoryRepository>,
        market_repo: Arc<dyn MarketRepository>,
        writers: ExchangeHistoryWriters,
        config: FinalizedExchangeHistoryConfig,
        progress: ExchangeHistoryProgressHandle,
        metrics: Arc<MetricsHub>,
    ) -> QuantResult<Self> {
        let extractor = ExchangeHistoryExtractor::connect(&config)
            .map_err(|error| extraction_failure(&error))?;
        let attestor = ExchangeHistoryAttestor::connect(&config)
            .map_err(|error| attestation_failure(&error))?;
        let policy_hash = Self::availability_policy_hash(&config)?;
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
            metrics,
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
        let run_result = PeriodicTask::run(
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
        .await;
        let shutdown_result = self
            .extractor
            .shutdown()
            .await
            .map_err(|error| extraction_failure(&error));
        if let Err(error) = shutdown_result {
            if run_result.is_ok() {
                return Err(error.into());
            }
            tracing::error!(%error, "HyperSync runtime shutdown also failed");
        }
        run_result
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
        let (extracted, attested) = match (extracted, attested) {
            (Ok(extracted), Ok(attested)) => (extracted, attested),
            (Err(error), Ok(_)) => return Err(extraction_failure(&error).into()),
            (Ok(_), Err(error)) => return Err(attestation_failure(&error).into()),
            (Err(extractor), Err(attestor)) => {
                return Err(provider_pair_failure(&extractor, &attestor).into());
            }
        };
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
        self.refresh_quarantine_metrics().await;
        self.metrics
            .set_fresh_boot_slo(slo_label(self.progress.snapshot().slo_status));
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
        let activation_target = Self::activation_target(&plan, model_head)?;
        self.reconcile_frontier(ExchangeHistoryFrontier::Activation, model_head)
            .await?;
        self.reconcile_frontier(ExchangeHistoryFrontier::Retention, model_head)
            .await?;
        self.validate_activation_cursor(activation_target).await?;
        self.sync_serving_head(&plan).await?;
        let activation_start = plan_block(plan.activation_from_block)?;
        self.publish_plan(&plan, activation_target).await?;
        if self
            .advance_activation(
                activation_start,
                activation_target,
                plan_block(plan.activation_through_block)?,
            )
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
        self.publish_ready(activation_target);
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
        let bootstrap_profile_set_hash = Self::hash_bootstrap_profiles(&profiles)?;
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
        let finalized_anchor_hash = block_hash(&finalized.hash)?;
        self.history_repo
            .create_or_load_plan(NewExchangeHistoryPlan {
                plan_id: Self::plan_id(
                    finalized.number,
                    &finalized_anchor_hash,
                    self.policy_hash,
                    bootstrap_profile_set_hash,
                ),
                chain_id: POLYGON_CHAIN_ID,
                policy_hash: self.policy_hash,
                bootstrap_profile_set_hash,
                finalized_anchor_block: block_i64(finalized.number)?,
                finalized_anchor_hash,
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

    fn activation_target(plan: &ExchangeHistoryPlanInfo, model_head: u64) -> QuantResult<u64> {
        // The plan freezes the initial anchor and backfill window. The live
        // activation frontier is deliberately not rewritten into that WORM
        // plan; every pass follows the independently attested model head.
        let initial_target = plan_block(plan.activation_through_block)?;
        if model_head < initial_target {
            return Err(ExchangeHistoryError::Attestation {
                detail: format!(
                    "current model head {model_head} regressed below immutable initial target {initial_target}"
                ),
            }
            .into());
        }
        Ok(model_head)
    }

    async fn validate_activation_cursor(&self, target: u64) -> QuantResult<()> {
        let accepted = self
            .history_repo
            .latest_accepted(ExchangeHistoryFrontier::Activation)
            .await?
            .as_ref()
            .map(|row| block_u64(row.to_block))
            .transpose()?;
        if let Some(accepted) = accepted
            && accepted > target
        {
            return Err(ExchangeHistoryError::Attestation {
                detail: format!(
                    "current model head {target} regressed below accepted activation frontier {accepted}"
                ),
            }
            .into());
        }
        Ok(())
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
        let evidence = ExchangeHistoryQuarantineEvidence::ContinuityMismatch {
            from_block: block_u64(divergent.from_block)?,
            to_block: block_u64(divergent.to_block)?,
            expected: "persisted accepted chunk boundary hashes".to_owned(),
            actual: "canonical block hash changed inside rollback buffer".to_owned(),
        };
        let evidence_hash = quarantine_evidence_hash(&evidence)?;
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
                    kind: ExchangeHistoryQuarantineKind::ContinuityMismatch,
                    evidence,
                    evidence_hash,
                    quarantined_at: now,
                },
            )
            .await?;
        for chunk in rewound {
            self.write_revocation(&chunk, now).await?;
        }
        self.refresh_quarantine_metric(frontier).await;
        self.publish_quarantine(ExchangeHistoryQuarantineKind::ContinuityMismatch);
        Ok(())
    }

    async fn advance_activation(
        &self,
        start: u64,
        target: u64,
        initial_through: u64,
    ) -> QuantResult<bool> {
        let latest = self
            .history_repo
            .latest_accepted(ExchangeHistoryFrontier::Activation)
            .await?;
        if let Some(accepted) = latest
            .as_ref()
            .map(|row| block_u64(row.to_block))
            .transpose()?
            && accepted > target
        {
            return Err(ExchangeHistoryError::Attestation {
                detail: format!(
                    "current model head {target} regressed below accepted activation frontier {accepted}"
                ),
            }
            .into());
        }
        let next = next_block(latest.as_ref(), start)?;
        self.publish_target(latest.as_ref(), start, target);
        if next > target {
            self.publish_ready(target);
            return Ok(false);
        }
        let budget_end = next
            .saturating_add(self.config.hot_window_blocks_per_tick.saturating_sub(1))
            .min(target);
        self.process_budget(
            ExchangeHistoryFrontier::Activation,
            next,
            budget_end,
            initial_through,
        )
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
        initial_through: u64,
    ) -> QuantResult<()> {
        while next <= budget_end {
            let desired_span = self.adaptive_chunk_blocks.load(Ordering::Relaxed).clamp(
                self.config.min_blocks_per_chunk,
                self.config.max_blocks_per_chunk,
            );
            let mut desired_end = next
                .saturating_add(desired_span.saturating_sub(1))
                .min(budget_end);
            // Preserve a whole-chunk endpoint for the immutable initial fit
            // window even if the attested live head advanced during catch-up.
            // The next iteration can consume the remaining live-tail budget.
            if next <= initial_through {
                desired_end = desired_end.min(initial_through);
            }
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
            let retry = match (extracted, attested) {
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
                (Err(error), Ok(_)) if shrinkable(&error) => {
                    return Err(FetchFailure::Shrink);
                }
                (Ok(_), Err(error)) if shrinkable(&error) => {
                    return Err(FetchFailure::Shrink);
                }
                (Err(extractor), Err(attestor))
                    if shrinkable(&extractor) && shrinkable(&attestor) =>
                {
                    return Err(FetchFailure::Shrink);
                }
                (Err(error), Ok(_)) => {
                    if attempt == self.config.retry_max_attempts {
                        return Err(FetchFailure::Contract(extraction_failure(&error).into()));
                    }
                    (true, false)
                }
                (Ok(_), Err(error)) => {
                    if attempt == self.config.retry_max_attempts {
                        return Err(FetchFailure::Contract(attestation_failure(&error).into()));
                    }
                    (false, true)
                }
                (Err(extractor), Err(attestor)) => {
                    if attempt == self.config.retry_max_attempts {
                        return Err(FetchFailure::Contract(
                            provider_pair_failure(&extractor, &attestor).into(),
                        ));
                    }
                    (true, true)
                }
            };
            self.publish_retry(retry.0, retry.1);
            tokio::time::sleep(Duration::from_millis(delay)).await;
            delay = delay.saturating_mul(2).min(self.config.retry_max_ms);
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
        let chunk_id = Self::chunk_id(
            frontier,
            from_block,
            to_block,
            ContentHash::from_bytes(extracted.digest.0),
        );
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
                kind: ExchangeHistoryQuarantineKind::ParentHashMismatch,
                evidence: ExchangeHistoryQuarantineEvidence::ContinuityMismatch {
                    from_block,
                    to_block,
                    expected: "accepted predecessor boundary hash".to_owned(),
                    actual: error.to_string(),
                },
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
                let kind = projection_kind(&error);
                let detail = error.to_string();
                self.quarantine_projection(ProjectionQuarantine {
                    chunk_id,
                    frontier,
                    from_block,
                    to_block,
                    attempt,
                    kind,
                    evidence: projection_evidence(&error),
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
                let kind = projection_kind(&error);
                let detail = error.to_string();
                self.quarantine_projection(ProjectionQuarantine {
                    chunk_id,
                    frontier,
                    from_block,
                    to_block,
                    attempt,
                    kind,
                    evidence: projection_evidence(&error),
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
        self.refresh_quarantine_metric(frontier).await;
        if frontier == ExchangeHistoryFrontier::Activation {
            let plan = self
                .history_repo
                .load_plan(POLYGON_CHAIN_ID)
                .await?
                .ok_or_else(|| ExchangeHistoryError::Projection {
                    detail: "accepted activation chunk has no immutable history plan".to_owned(),
                })?;
            self.sync_serving_head(&plan).await?;
        }
        self.publish_accepted(frontier, from_block, to_block, extracted.logs.len(), now)
            .await?;
        Ok(())
    }

    async fn sync_serving_head(&self, plan: &ExchangeHistoryPlanInfo) -> QuantResult<()> {
        let mut accepted = self
            .history_repo
            .accepted_from(
                ExchangeHistoryFrontier::Activation,
                plan.activation_from_block,
            )
            .await?;
        accepted.sort_unstable_by_key(|chunk| chunk.from_block);
        let mut cursor = plan.activation_from_block;
        let mut chunks = Vec::new();
        let mut effective_through_at = None;
        for chunk in accepted {
            if chunk.from_block != cursor {
                break;
            }
            let state_revision =
                chunk
                    .state_revision
                    .ok_or_else(|| ExchangeHistoryError::Projection {
                        detail: format!("accepted chunk {} has no state revision", chunk.chunk_id),
                    })?;
            effective_through_at = chunk.effective_through_at;
            chunks.push(HistorySealChunkRef {
                chunk_id: chunk.chunk_id,
                frontier: chunk.frontier,
                state_revision,
                from_block: chunk.from_block,
                to_block: chunk.to_block,
            });
            cursor = chunk
                .to_block
                .checked_add(1)
                .ok_or(ExchangeHistoryError::FrontierOverflow)?;
        }
        let Some(last) = chunks.last() else {
            return Ok(());
        };
        let effective_through_at =
            effective_through_at.ok_or_else(|| ExchangeHistoryError::Projection {
                detail: "accepted activation head has no effective-through timestamp".to_owned(),
            })?;
        for _attempt in 0..SERVING_HEAD_CAS_ATTEMPTS {
            let latest = self
                .history_repo
                .latest_serving_head(ExchangeHistoryFrontier::Activation)
                .await?;
            if latest.as_ref().is_some_and(|head| {
                head.seal.accepted_through_block == last.to_block && head.chunks == chunks
            }) {
                return Ok(());
            }
            let previous_seal_id = latest.as_ref().map(|head| head.seal.serving_head_seal_id);
            let seal_id = Self::serving_head_id(plan.plan_id, previous_seal_id, &chunks);
            let mut command = CreateHistoryServingHeadSeal {
                seal: NewHistoryServingHeadSeal {
                    serving_head_seal_id: seal_id,
                    seal_hash: ContentHash::from_bytes([0; 32]),
                    plan_id: plan.plan_id,
                    frontier: ExchangeHistoryFrontier::Activation,
                    previous_seal_id,
                    window_from_block: plan.activation_from_block,
                    accepted_through_block: last.to_block,
                    effective_through_at,
                    policy_hash: plan.policy_hash,
                    created_at: Utc::now(),
                },
                chunks: chunks.clone(),
            };
            command.seal.seal_hash =
                command
                    .derive_hash()
                    .map_err(|error| ExchangeHistoryError::Projection {
                        detail: format!("derive serving-head seal hash: {error}"),
                    })?;
            match self.history_repo.create_serving_head(command).await {
                Ok(_) => return Ok(()),
                Err(error) if Self::is_head_cas_conflict(&error) => {}
                Err(error) => return Err(error.into()),
            }
        }
        Err(StorageError::state_conflict(
            "quant_history_serving_head_seal",
            None::<&Uuid>,
            "serving head predecessor CAS did not converge within its bounded retry budget",
        )
        .into())
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
            state_revision: Self::state_revision(accepted_at)?,
            schema_version: ExchangeHistoryAcceptanceRow::SCHEMA_VERSION,
        };
        self.writers
            .acceptance
            .write_batch_idempotent(&Self::acceptance_token(row.chunk_id), vec![row])
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
            state_revision: Self::state_revision(revoked_at)?,
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
            &self.writers.fee_charges,
            "fee-charge",
            chunk_id,
            self.config.batch_size,
            projection.fee_charges,
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
        let kind = if continuity_agrees {
            ExchangeHistoryQuarantineKind::ProviderMismatch
        } else {
            ExchangeHistoryQuarantineKind::ContinuityMismatch
        };
        let evidence = if continuity_agrees {
            ExchangeHistoryQuarantineEvidence::ProviderMismatch {
                extractor_digest: ContentHash::from_bytes(extracted.digest.0),
                attestor_digest: ContentHash::from_bytes(attested.digest.0),
                extractor_count: u64::try_from(extracted.logs.len()).map_err(|error| {
                    ExchangeHistoryError::Projection {
                        detail: format!("extractor log count overflow: {error}"),
                    }
                })?,
                attestor_count: u64::try_from(attested.logs.len()).map_err(|error| {
                    ExchangeHistoryError::Projection {
                        detail: format!("attestor log count overflow: {error}"),
                    }
                })?,
            }
        } else {
            ExchangeHistoryQuarantineEvidence::ContinuityMismatch {
                from_block,
                to_block,
                expected: "extractor and attestor boundary/anchor proofs agree".to_owned(),
                actual: "independent provider continuity proofs differ".to_owned(),
            }
        };
        let evidence_hash = quarantine_evidence_hash(&evidence)?;
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
                    kind,
                    evidence,
                    evidence_hash,
                    quarantined_at: now,
                },
            )
            .await?;
        self.refresh_quarantine_metric(frontier).await;
        self.publish_quarantine(kind);
        Ok(())
    }

    async fn quarantine_projection(&self, input: ProjectionQuarantine) -> QuantResult<()> {
        let ProjectionQuarantine {
            chunk_id,
            frontier,
            from_block,
            to_block,
            attempt,
            kind,
            evidence,
            created_at,
        } = input;
        let now = Utc::now();
        let evidence_hash = quarantine_evidence_hash(&evidence)?;
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
                    kind,
                    evidence,
                    evidence_hash,
                    quarantined_at: now,
                },
            )
            .await?;
        self.refresh_quarantine_metric(frontier).await;
        self.publish_quarantine(kind);
        Ok(())
    }

    fn publish_stage(&self, stage: ExchangeHistoryStage) {
        let mut progress = self.progress.snapshot().as_ref().clone();
        progress.stage = stage;
        progress.updated_at = Utc::now();
        self.progress.publish(progress);
    }

    async fn publish_plan(
        &self,
        plan: &ExchangeHistoryPlanInfo,
        activation_target: u64,
    ) -> QuantResult<()> {
        let retention = self
            .history_repo
            .earliest_accepted(ExchangeHistoryFrontier::Retention)
            .await?;
        let mut progress = self.progress.snapshot().as_ref().clone();
        progress.activation_from_block = Some(plan_block(plan.activation_from_block)?);
        progress.target_block = Some(activation_target);
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

    async fn publish_accepted(
        &self,
        frontier: ExchangeHistoryFrontier,
        from_block: u64,
        to_block: u64,
        logs: usize,
        now: DateTime<Utc>,
    ) -> QuantResult<()> {
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
        if frontier == ExchangeHistoryFrontier::Activation {
            let activation_from =
                progress
                    .activation_from_block
                    .ok_or_else(|| ExchangeHistoryError::Projection {
                        detail: "activation progress has no immutable start block".to_owned(),
                    })?;
            let accepted_chunks = self
                .history_repo
                .accepted_from(
                    ExchangeHistoryFrontier::Activation,
                    i64::try_from(activation_from)
                        .map_err(|_| ExchangeHistoryError::FrontierOverflow)?,
                )
                .await?;
            let accepted = progress.accepted_through_block.ok_or_else(|| {
                ExchangeHistoryError::Projection {
                    detail: "accepted activation chunk did not advance the frontier".to_owned(),
                }
            })?;
            let target = progress
                .target_block
                .ok_or_else(|| ExchangeHistoryError::Projection {
                    detail: "activation progress has no current model-head target".to_owned(),
                })?;
            let total = target.saturating_sub(activation_from).saturating_add(1);
            let covered = accepted
                .saturating_sub(activation_from)
                .saturating_add(1)
                .min(total);
            let warm = accepted_chunks.len() >= 5
                && covered.saturating_mul(100) >= total.saturating_mul(5);
            if warm {
                let mut rates = accepted_chunks
                    .iter()
                    .rev()
                    .take(5)
                    .filter_map(|chunk| {
                        let accepted_at = chunk.accepted_at?;
                        let elapsed_ms = accepted_at
                            .signed_duration_since(chunk.created_at)
                            .num_milliseconds()
                            .max(1);
                        let elapsed_ms = u64::try_from(elapsed_ms).ok()?;
                        let span = u64::try_from(
                            chunk
                                .to_block
                                .saturating_sub(chunk.from_block)
                                .saturating_add(1),
                        )
                        .ok()?;
                        Some(span.saturating_mul(1_000_000) / elapsed_ms)
                    })
                    .collect::<Vec<_>>();
                rates.sort_unstable();
                let rate = rates.get(rates.len() / 2).copied().unwrap_or_default();
                progress.block_rate_milli = rate;
                if rate > 0 {
                    let remaining = target.saturating_sub(accepted);
                    let eta_secs = remaining.saturating_mul(1_000).div_ceil(rate);
                    let eta_secs = i64::try_from(eta_secs).unwrap_or(i64::MAX);
                    let projected = now.checked_add_signed(TimeDelta::seconds(eta_secs));
                    progress.projected_completion_at = projected;
                    let warning_deadline =
                        progress.started_at.checked_add_signed(TimeDelta::hours(48));
                    let violation_deadline =
                        progress.started_at.checked_add_signed(TimeDelta::hours(72));
                    progress.slo_status = match (projected, warning_deadline, violation_deadline) {
                        (Some(projected), _, Some(deadline)) if projected > deadline => {
                            ColdStartSloStatus::Violation
                        }
                        (Some(projected), Some(deadline), _) if projected > deadline => {
                            ColdStartSloStatus::Warning
                        }
                        (Some(_), Some(_), Some(_)) => ColdStartSloStatus::OnTrack,
                        _ => ColdStartSloStatus::Violation,
                    };
                }
            } else {
                progress.block_rate_milli = 0;
                progress.projected_completion_at = None;
                progress.slo_status = ColdStartSloStatus::WarmingUp;
            }
        }
        progress.updated_at = now;
        self.metrics
            .set_fresh_boot_slo(slo_label(progress.slo_status));
        self.progress.publish(progress);
        Ok(())
    }

    fn publish_quarantine(&self, kind: ExchangeHistoryQuarantineKind) {
        let mut progress = self.progress.snapshot().as_ref().clone();
        progress.stage = ExchangeHistoryStage::Quarantined;
        progress.quarantine_count = progress.quarantine_count.saturating_add(1);
        if kind == ExchangeHistoryQuarantineKind::UnknownToken {
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
        let elapsed = now.signed_duration_since(progress.started_at);
        progress.slo_status = if elapsed > TimeDelta::hours(72) {
            ColdStartSloStatus::Violation
        } else if elapsed > TimeDelta::hours(48) {
            ColdStartSloStatus::Warning
        } else {
            ColdStartSloStatus::OnTrack
        };
        progress.updated_at = now;
        self.metrics
            .set_fresh_boot_slo(slo_label(progress.slo_status));
        self.progress.publish(progress);
    }

    async fn refresh_quarantine_metrics(&self) {
        self.refresh_quarantine_metric(ExchangeHistoryFrontier::Activation)
            .await;
        self.refresh_quarantine_metric(ExchangeHistoryFrontier::Retention)
            .await;
    }

    async fn refresh_quarantine_metric(&self, frontier: ExchangeHistoryFrontier) {
        match self.history_repo.count_active_quarantine(frontier).await {
            Ok(count) => self
                .metrics
                .set_active_history_quarantines(frontier_label(frontier), count),
            Err(error) => tracing::warn!(
                %error,
                frontier = frontier_label(frontier),
                "active history-quarantine metric refresh failed"
            ),
        }
    }
}

const fn frontier_label(frontier: ExchangeHistoryFrontier) -> &'static str {
    match frontier {
        ExchangeHistoryFrontier::Activation => "activation",
        ExchangeHistoryFrontier::Retention => "retention",
    }
}

const fn slo_label(status: ColdStartSloStatus) -> &'static str {
    match status {
        ColdStartSloStatus::WarmingUp => "warming_up",
        ColdStartSloStatus::OnTrack => "on_track",
        ColdStartSloStatus::Warning => "warning",
        ColdStartSloStatus::Violation => "violation",
    }
}

enum FetchFailure {
    Shrink,
    Contract(QuantError),
}

async fn write_rows<T>(
    writer: &Arc<dyn FactWriter<T>>,
    table_key: &str,
    chunk_id: Uuid,
    batch_size: usize,
    rows: Vec<T>,
) -> Result<(), StorageError>
where
    T: Send + Sync + 'static,
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
        state_revision: None,
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
        state_revision: Some(
            i64::try_from(ExchangeHistoryWorker::state_revision(accepted_at)?)
                .map_err(|_| ExchangeHistoryError::FrontierOverflow)?,
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
        state_revision: chunk.state_revision,
        accepted_at: chunk.accepted_at,
        created_at: chunk.created_at,
        updated_at,
    }
}

fn dedup_token(table_key: &str, chunk_id: Uuid, batch_index: u64) -> ContentHash {
    let mut hasher = Hasher::new();
    hasher.update(b"quant-pivot/exchange-history-insert/v1\0");
    hasher.update(table_key.as_bytes());
    hasher.update(chunk_id.as_bytes());
    hasher.update(&batch_index.to_be_bytes());
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

const fn projection_kind(error: &ExecutionProjectionError) -> ExchangeHistoryQuarantineKind {
    match error {
        ExecutionProjectionError::UnknownToken { .. } => {
            ExchangeHistoryQuarantineKind::UnknownToken
        }
        ExecutionProjectionError::InvalidTransactionGrammar { .. } => {
            ExchangeHistoryQuarantineKind::MissingCorrelation
        }
        ExecutionProjectionError::UnknownContract | ExecutionProjectionError::ContractInterval => {
            ExchangeHistoryQuarantineKind::ContractMismatch
        }
        ExecutionProjectionError::DecodeFailure
        | ExecutionProjectionError::FeeConservation { .. }
        | ExecutionProjectionError::InvalidAmount
        | ExecutionProjectionError::ZeroExecution
        | ExecutionProjectionError::InvalidTimestamp
        | ExecutionProjectionError::RemovedLog => ExchangeHistoryQuarantineKind::DecodeFailure,
    }
}

fn projection_evidence(error: &ExecutionProjectionError) -> ExchangeHistoryQuarantineEvidence {
    match error {
        ExecutionProjectionError::InvalidTransactionGrammar {
            version,
            contract,
            transaction_hash,
            log_index,
            expected,
            actual,
        } => ExchangeHistoryQuarantineEvidence::ProjectionFailure {
            version: Some((*version).to_owned()),
            contract_address: Some(contract.clone()),
            transaction_hash: Some(transaction_hash.clone()),
            log_index: Some(*log_index),
            token_id: None,
            expected: Some((*expected).to_owned()),
            actual: (*actual).to_owned(),
        },
        ExecutionProjectionError::UnknownToken { token_id } => {
            ExchangeHistoryQuarantineEvidence::ProjectionFailure {
                version: None,
                contract_address: None,
                transaction_hash: None,
                log_index: None,
                token_id: Some(token_id.clone()),
                expected: Some("token exists in the PIT market identity catalog".to_owned()),
                actual: error.to_string(),
            }
        }
        _ => ExchangeHistoryQuarantineEvidence::ProjectionFailure {
            version: None,
            contract_address: None,
            transaction_hash: None,
            log_index: None,
            token_id: None,
            expected: None,
            actual: error.to_string(),
        },
    }
}

fn quarantine_evidence_hash(
    evidence: &ExchangeHistoryQuarantineEvidence,
) -> QuantResult<ContentHash> {
    let bytes = serde_json::to_vec(evidence).map_err(|error| ExchangeHistoryError::Projection {
        detail: format!("serialize quarantine evidence: {error}"),
    })?;
    let mut hasher = Hasher::new();
    hasher.update(b"quant-pivot/exchange-history-quarantine-evidence/v1\0");
    hasher.update(&bytes);
    Ok(ContentHash::from_bytes(*hasher.finalize().as_bytes()))
}

fn shrinkable(error: &HistoryClientError) -> bool {
    match error {
        HistoryClientError::RpcResponseBodyBudget { .. }
        | HistoryClientError::HyperSyncResponseBodyBudget { .. }
        | HistoryClientError::CanonicalChunkBudget { .. }
        | HistoryClientError::HyperSyncPayloadTooLarge => true,
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

fn provider_pair_failure(
    extractor: &HistoryClientError,
    attestor: &HistoryClientError,
) -> ExchangeHistoryError {
    ExchangeHistoryError::ProviderFailures {
        extractor: extractor.to_string(),
        attestor: attestor.to_string(),
    }
}

const fn projection_failure(detail: String) -> ExchangeHistoryError {
    ExchangeHistoryError::Projection { detail }
}

fn block_i64(block: u64) -> QuantResult<i64> {
    i64::try_from(block).map_err(|_| ExchangeHistoryError::FrontierOverflow.into())
}

fn block_u64(block: i64) -> QuantResult<u64> {
    u64::try_from(block).map_err(|_| ExchangeHistoryError::FrontierOverflow.into())
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
    use std::{
        collections::{BTreeMap, HashSet},
        sync::{
            Arc,
            atomic::{AtomicU64, Ordering},
        },
    };

    use async_trait::async_trait;
    use chrono::{DateTime, Utc};
    use parking_lot::Mutex;
    use quant_pivot_api::exchange::history_client::HistoryDigest;
    use quant_pivot_error::{
        QuantError, exchange_history::ExchangeHistoryError, storage::StorageError,
    };
    use quant_pivot_models::{
        config::FinalizedExchangeHistoryConfig,
        domain::{
            api::MarketPageQuery,
            data_plane::{
                CreateHistoryFitSeal, CreateHistoryServingHeadSeal, ExchangeHistoryChunkInfo,
                ExchangeHistoryChunkStatus, ExchangeHistoryFrontier, ExchangeHistoryPlanInfo,
                ExchangeHistoryQuarantineInfo, ExchangeHistoryQuarantineRead,
                ExchangeHistoryQuarantineRecord, ExchangeHistoryQuarantineResolutionInfo,
                HistoryFitSeal, HistorySealChunkRef, HistoryServingHeadSeal,
                HistoryServingHeadSealInfo, NewExchangeHistoryChunk, NewExchangeHistoryPlan,
                NewExchangeHistoryQuarantine, NewExchangeHistoryQuarantineResolution,
                ResolveAcceptedHistoryRange,
            },
            market::{MarketInfo, UpsertMarket},
            pagination::Paginated,
        },
        enums::market::MarketStatus,
        types::{ContentHash, HistoryFitSealId, HistoryServingHeadSealId, MarketId, TokenId},
    };
    use quant_pivot_repository::traits::{ExchangeHistoryRepository, FactWriter, MarketRepository};
    use uuid::Uuid;

    use super::{
        ArchiveProbe, AttestedHistoryChunk, CanonicalBlockHeader, ExchangeEventRow,
        ExchangeFeeChargeRow, ExchangeHistoryAcceptanceRow, ExchangeHistoryProgressHandle,
        ExchangeHistoryStage, ExchangeHistoryWorker, ExchangeHistoryWriters, ExchangeLogRawRow,
        ExchangeMatchRow, ExecutionParticipantRow, ExtractedHistoryChunk, FetchFailure,
        HistoryAttestorSource, HistoryClientError, HistoryContinuityProof,
        HistoryContinuityProofBasis, HistoryExtractorSource, MarketExecutionRow, MetricsHub,
        block_hash, shrinkable,
    };

    const ACTIVATION_START: u64 = 100;
    const CONFIRMATIONS: u64 = 12;

    #[derive(Default)]
    struct HistoryState {
        plan: Option<ExchangeHistoryPlanInfo>,
        chunks: Vec<ExchangeHistoryChunkInfo>,
        heads: Vec<HistoryServingHeadSeal>,
    }

    #[derive(Default)]
    struct MemoryHistoryRepository {
        state: Mutex<HistoryState>,
    }

    impl MemoryHistoryRepository {
        fn snapshot(&self) -> HistoryState {
            let state = self.state.lock();
            HistoryState {
                plan: state.plan.clone(),
                chunks: state.chunks.clone(),
                heads: state.heads.clone(),
            }
        }

        fn plan_info(plan: NewExchangeHistoryPlan) -> ExchangeHistoryPlanInfo {
            ExchangeHistoryPlanInfo {
                plan_id: plan.plan_id,
                chain_id: plan.chain_id,
                policy_hash: plan.policy_hash,
                bootstrap_profile_set_hash: plan.bootstrap_profile_set_hash,
                finalized_anchor_block: plan.finalized_anchor_block,
                finalized_anchor_hash: plan.finalized_anchor_hash,
                finalized_anchor_timestamp: plan.finalized_anchor_timestamp,
                activation_from_block: plan.activation_from_block,
                activation_through_block: plan.activation_through_block,
                crypto_required_from_block: plan.crypto_required_from_block,
                weather_required_from_block: plan.weather_required_from_block,
                retention_from_block: plan.retention_from_block,
                retention_through_block: plan.retention_through_block,
                created_at: plan.created_at,
            }
        }

        fn chunk_info(chunk: NewExchangeHistoryChunk) -> ExchangeHistoryChunkInfo {
            ExchangeHistoryChunkInfo {
                chunk_id: chunk.chunk_id,
                frontier: chunk.frontier,
                from_block: chunk.from_block,
                to_block: chunk.to_block,
                status: chunk.status,
                attempt_count: chunk.attempt_count,
                hypersync_count: chunk.hypersync_count,
                attestor_count: chunk.attestor_count,
                hypersync_digest: chunk.hypersync_digest,
                attestor_digest: chunk.attestor_digest,
                first_block_hash: chunk.first_block_hash,
                last_block_hash: chunk.last_block_hash,
                archive_height: chunk.archive_height,
                continuity_basis: chunk.continuity_basis,
                continuity_block: chunk.continuity_block,
                continuity_hash: chunk.continuity_hash,
                effective_through_at: chunk.effective_through_at,
                state_revision: chunk.state_revision,
                accepted_at: chunk.accepted_at,
                created_at: chunk.created_at,
                updated_at: chunk.updated_at,
            }
        }

        fn unexpected<T>(operation: &str) -> Result<T, StorageError> {
            Err(StorageError::invariant_violation(
                Some("exchange_history_test"),
                format!("unexpected test repository operation {operation}"),
            ))
        }
    }

    #[async_trait]
    impl ExchangeHistoryRepository for MemoryHistoryRepository {
        async fn create_or_load_plan(
            &self,
            plan: NewExchangeHistoryPlan,
        ) -> Result<ExchangeHistoryPlanInfo, StorageError> {
            let mut state = self.state.lock();
            if let Some(existing) = &state.plan {
                return Ok(existing.clone());
            }
            let plan = Self::plan_info(plan);
            state.plan = Some(plan.clone());
            drop(state);
            Ok(plan)
        }

        async fn load_plan(
            &self,
            chain_id: i64,
        ) -> Result<Option<ExchangeHistoryPlanInfo>, StorageError> {
            Ok(self
                .state
                .lock()
                .plan
                .clone()
                .filter(|plan| plan.chain_id == chain_id))
        }

        async fn find_range(
            &self,
            frontier: ExchangeHistoryFrontier,
            from_block: i64,
            to_block: i64,
        ) -> Result<Option<ExchangeHistoryChunkInfo>, StorageError> {
            Ok(self
                .state
                .lock()
                .chunks
                .iter()
                .find(|chunk| {
                    chunk.frontier == frontier
                        && chunk.from_block == from_block
                        && chunk.to_block == to_block
                })
                .cloned())
        }

        async fn save_chunk(
            &self,
            chunk: NewExchangeHistoryChunk,
        ) -> Result<ExchangeHistoryChunkInfo, StorageError> {
            let chunk = Self::chunk_info(chunk);
            let mut state = self.state.lock();
            if let Some(existing) = state
                .chunks
                .iter_mut()
                .find(|existing| existing.chunk_id == chunk.chunk_id)
            {
                existing.clone_from(&chunk);
            } else {
                state.chunks.push(chunk.clone());
            }
            drop(state);
            Ok(chunk)
        }

        async fn latest_accepted(
            &self,
            frontier: ExchangeHistoryFrontier,
        ) -> Result<Option<ExchangeHistoryChunkInfo>, StorageError> {
            Ok(self
                .state
                .lock()
                .chunks
                .iter()
                .filter(|chunk| {
                    chunk.frontier == frontier
                        && chunk.status == ExchangeHistoryChunkStatus::Accepted
                })
                .max_by_key(|chunk| chunk.to_block)
                .cloned())
        }

        async fn earliest_accepted(
            &self,
            frontier: ExchangeHistoryFrontier,
        ) -> Result<Option<ExchangeHistoryChunkInfo>, StorageError> {
            Ok(self
                .state
                .lock()
                .chunks
                .iter()
                .filter(|chunk| {
                    chunk.frontier == frontier
                        && chunk.status == ExchangeHistoryChunkStatus::Accepted
                })
                .min_by_key(|chunk| chunk.from_block)
                .cloned())
        }

        async fn accepted_from(
            &self,
            frontier: ExchangeHistoryFrontier,
            from_block: i64,
        ) -> Result<Vec<ExchangeHistoryChunkInfo>, StorageError> {
            let mut chunks = self
                .state
                .lock()
                .chunks
                .iter()
                .filter(|chunk| {
                    chunk.frontier == frontier
                        && chunk.status == ExchangeHistoryChunkStatus::Accepted
                        && chunk.to_block >= from_block
                })
                .cloned()
                .collect::<Vec<_>>();
            chunks.sort_unstable_by_key(|chunk| chunk.from_block);
            Ok(chunks)
        }

        async fn rewind_from(
            &self,
            frontier: ExchangeHistoryFrontier,
            from_block: i64,
            updated_at: DateTime<Utc>,
        ) -> Result<Vec<ExchangeHistoryChunkInfo>, StorageError> {
            let mut state = self.state.lock();
            let mut rewound = Vec::new();
            for chunk in &mut state.chunks {
                if chunk.frontier == frontier
                    && chunk.status == ExchangeHistoryChunkStatus::Accepted
                    && chunk.to_block >= from_block
                {
                    rewound.push(chunk.clone());
                    chunk.status = ExchangeHistoryChunkStatus::Rewound;
                    chunk.updated_at = updated_at;
                }
            }
            drop(state);
            Ok(rewound)
        }

        async fn quarantine_chunk(
            &self,
            _chunk: NewExchangeHistoryChunk,
            _quarantine: NewExchangeHistoryQuarantine,
        ) -> Result<ExchangeHistoryQuarantineInfo, StorageError> {
            Self::unexpected("quarantine_chunk")
        }

        async fn list_quarantine(
            &self,
            _frontier: ExchangeHistoryFrontier,
            _limit: u64,
        ) -> Result<Vec<ExchangeHistoryQuarantineInfo>, StorageError> {
            Ok(Vec::new())
        }

        async fn page_quarantine(
            &self,
            _query: ExchangeHistoryQuarantineRead,
        ) -> Result<Vec<ExchangeHistoryQuarantineRecord>, StorageError> {
            Ok(Vec::new())
        }

        async fn active_quarantine(
            &self,
            _frontier: ExchangeHistoryFrontier,
            _from_block: i64,
            _to_block: i64,
            _limit: u64,
        ) -> Result<Vec<ExchangeHistoryQuarantineInfo>, StorageError> {
            Ok(Vec::new())
        }

        async fn count_active_quarantine(
            &self,
            _frontier: ExchangeHistoryFrontier,
        ) -> Result<u64, StorageError> {
            Ok(0)
        }

        async fn resolve_quarantine(
            &self,
            _resolution: NewExchangeHistoryQuarantineResolution,
        ) -> Result<ExchangeHistoryQuarantineResolutionInfo, StorageError> {
            Self::unexpected("resolve_quarantine")
        }

        async fn resolve_accepted_range(
            &self,
            _resolution: ResolveAcceptedHistoryRange,
        ) -> Result<Vec<ExchangeHistoryQuarantineResolutionInfo>, StorageError> {
            Ok(Vec::new())
        }

        async fn create_fit_seal(
            &self,
            _command: CreateHistoryFitSeal,
        ) -> Result<HistoryFitSeal, StorageError> {
            Self::unexpected("create_fit_seal")
        }

        async fn find_fit_seal(
            &self,
            _fit_seal_id: HistoryFitSealId,
        ) -> Result<Option<HistoryFitSeal>, StorageError> {
            Ok(None)
        }

        async fn create_serving_head(
            &self,
            command: CreateHistoryServingHeadSeal,
        ) -> Result<HistoryServingHeadSeal, StorageError> {
            let mut state = self.state.lock();
            if let Some(existing) = state
                .heads
                .iter()
                .find(|head| head.seal.serving_head_seal_id == command.seal.serving_head_seal_id)
            {
                return Ok(existing.clone());
            }
            let seal = HistoryServingHeadSealInfo {
                serving_head_seal_id: command.seal.serving_head_seal_id,
                seal_hash: command.seal.seal_hash,
                plan_id: command.seal.plan_id,
                frontier: command.seal.frontier,
                previous_seal_id: command.seal.previous_seal_id,
                window_from_block: command.seal.window_from_block,
                accepted_through_block: command.seal.accepted_through_block,
                effective_through_at: command.seal.effective_through_at,
                policy_hash: command.seal.policy_hash,
                created_at: command.seal.created_at,
            };
            let head = HistoryServingHeadSeal {
                seal,
                chunks: command.chunks,
            };
            state.heads.push(head.clone());
            drop(state);
            Ok(head)
        }

        async fn latest_serving_head(
            &self,
            frontier: ExchangeHistoryFrontier,
        ) -> Result<Option<HistoryServingHeadSeal>, StorageError> {
            Ok(self
                .state
                .lock()
                .heads
                .iter()
                .rev()
                .find(|head| head.seal.frontier == frontier)
                .cloned())
        }

        async fn serving_head_at(
            &self,
            frontier: ExchangeHistoryFrontier,
            decision_at: DateTime<Utc>,
        ) -> Result<Option<HistoryServingHeadSeal>, StorageError> {
            Ok(self
                .state
                .lock()
                .heads
                .iter()
                .rev()
                .find(|head| head.seal.frontier == frontier && head.seal.created_at <= decision_at)
                .cloned())
        }

        async fn validate_fit_seal(
            &self,
            _fit_seal_id: HistoryFitSealId,
            _seal_hash: ContentHash,
        ) -> Result<HistoryFitSeal, StorageError> {
            Self::unexpected("validate_fit_seal")
        }

        async fn validate_serving_head(
            &self,
            serving_head_seal_id: HistoryServingHeadSealId,
            seal_hash: ContentHash,
        ) -> Result<HistoryServingHeadSeal, StorageError> {
            self.state
                .lock()
                .heads
                .iter()
                .find(|head| {
                    head.seal.serving_head_seal_id == serving_head_seal_id
                        && head.seal.seal_hash == seal_hash
                })
                .cloned()
                .ok_or_else(|| {
                    StorageError::not_found("history_serving_head_seal", serving_head_seal_id)
                })
        }
    }

    struct EmptyMarketRepository;

    impl EmptyMarketRepository {
        fn unexpected<T>(operation: &str) -> Result<T, StorageError> {
            Err(StorageError::invariant_violation(
                Some("market_test"),
                format!("unexpected test repository operation {operation}"),
            ))
        }
    }

    #[async_trait]
    impl MarketRepository for EmptyMarketRepository {
        async fn find_by_id(
            &self,
            _id: &MarketId,
        ) -> Result<Option<Arc<MarketInfo>>, StorageError> {
            Ok(None)
        }

        async fn find_by_ids(
            &self,
            _ids: &[MarketId],
        ) -> Result<Vec<Arc<MarketInfo>>, StorageError> {
            Ok(Vec::new())
        }

        async fn find_by_tokens(
            &self,
            _token_ids: &[TokenId],
        ) -> Result<Vec<Arc<MarketInfo>>, StorageError> {
            Ok(Vec::new())
        }

        async fn page(
            &self,
            _query: MarketPageQuery,
        ) -> Result<Paginated<MarketInfo>, StorageError> {
            Self::unexpected("market_page")
        }

        async fn find_active(&self) -> Result<Arc<[MarketInfo]>, StorageError> {
            Ok(Arc::from([]))
        }

        async fn find_by_event(
            &self,
            _event_id: &str,
        ) -> Result<Vec<Arc<MarketInfo>>, StorageError> {
            Ok(Vec::new())
        }

        async fn find_existing_ids(
            &self,
            _ids: &[MarketId],
        ) -> Result<HashSet<String>, StorageError> {
            Ok(HashSet::new())
        }

        async fn upsert(&self, _market: UpsertMarket) -> Result<Arc<MarketInfo>, StorageError> {
            Self::unexpected("market_upsert")
        }

        async fn upsert_batch(&self, _markets: Vec<UpsertMarket>) -> Result<u64, StorageError> {
            Self::unexpected("market_upsert_batch")
        }

        async fn update_status(
            &self,
            _id: &MarketId,
            _status: MarketStatus,
            _outcome: Option<&str>,
        ) -> Result<(), StorageError> {
            Self::unexpected("market_update_status")
        }
    }

    struct MemoryWriter<T> {
        rows: Mutex<Vec<T>>,
    }

    impl<T> MemoryWriter<T>
    where
        T: Send + Sync + 'static,
    {
        fn shared() -> Arc<dyn FactWriter<T>> {
            Arc::new(Self {
                rows: Mutex::new(Vec::new()),
            })
        }
    }

    #[async_trait]
    impl<T> FactWriter<T> for MemoryWriter<T>
    where
        T: Send + Sync + 'static,
    {
        async fn write_batch(&self, rows: Vec<T>) -> Result<(), StorageError> {
            self.rows.lock().extend(rows);
            Ok(())
        }
    }

    impl ExchangeHistoryWriters {
        fn memory() -> Self {
            Self {
                raw_logs: MemoryWriter::<ExchangeLogRawRow>::shared(),
                events: MemoryWriter::<ExchangeEventRow>::shared(),
                fee_charges: MemoryWriter::<ExchangeFeeChargeRow>::shared(),
                matches: MemoryWriter::<ExchangeMatchRow>::shared(),
                executions: MemoryWriter::<MarketExecutionRow>::shared(),
                participants: MemoryWriter::<ExecutionParticipantRow>::shared(),
                acceptance: MemoryWriter::<ExchangeHistoryAcceptanceRow>::shared(),
            }
        }
    }

    struct RollingHistorySource {
        finalized_head: AtomicU64,
        max_span: AtomicU64,
        requested: Mutex<Vec<(u64, u64)>>,
    }

    impl RollingHistorySource {
        const fn new(finalized_head: u64) -> Self {
            Self {
                finalized_head: AtomicU64::new(finalized_head),
                max_span: AtomicU64::new(u64::MAX),
                requested: Mutex::new(Vec::new()),
            }
        }

        fn advance(&self, finalized_head: u64) {
            self.finalized_head.store(finalized_head, Ordering::SeqCst);
        }

        fn header(block: u64) -> CanonicalBlockHeader {
            CanonicalBlockHeader {
                number: block,
                hash: format!("0x{block:064x}"),
                parent_hash: format!("0x{:064x}", block.saturating_sub(1)),
                timestamp: 1_700_000_000_u64.saturating_add(block.saturating_mul(2)),
            }
        }

        fn extracted(&self, from_block: u64, to_block: u64) -> ExtractedHistoryChunk {
            let confirmation = to_block.saturating_add(CONFIRMATIONS);
            ExtractedHistoryChunk {
                from_block,
                to_block,
                archive_height: self.finalized_head.load(Ordering::SeqCst),
                first_block: Self::header(from_block),
                last_block: Self::header(to_block),
                confirmation_anchor: Self::header(confirmation),
                logs: Vec::new(),
                digest: HistoryDigest([0; 32]),
                continuity_proof: HistoryContinuityProof {
                    basis: HistoryContinuityProofBasis::HyperSyncBoundaryHeaders,
                    attested_block_number: to_block,
                    attested_block_hash: Self::header(to_block).hash,
                    first_block_number: from_block,
                    first_parent_hash: Self::header(from_block).parent_hash,
                },
                observed_at_millis: i64::try_from(Self::header(confirmation).timestamp)
                    .unwrap_or(i64::MAX)
                    .saturating_mul(1_000),
            }
        }

        fn attested(&self, from_block: u64, to_block: u64) -> AttestedHistoryChunk {
            let extracted = self.extracted(from_block, to_block);
            AttestedHistoryChunk {
                from_block,
                to_block,
                first_block: extracted.first_block,
                last_block: extracted.last_block,
                confirmation_anchor: extracted.confirmation_anchor,
                logs: extracted.logs,
                digest: extracted.digest,
                observed_at_millis: extracted.observed_at_millis,
            }
        }
    }

    #[async_trait]
    impl HistoryExtractorSource for RollingHistorySource {
        async fn fetch_chunk(
            &self,
            from_block: u64,
            to_block: u64,
        ) -> Result<ExtractedHistoryChunk, HistoryClientError> {
            self.requested.lock().push((from_block, to_block));
            if to_block.saturating_sub(from_block).saturating_add(1)
                > self.max_span.load(Ordering::SeqCst)
            {
                return Err(HistoryClientError::CanonicalChunkBudget { limit: 1 });
            }
            if to_block.saturating_add(CONFIRMATIONS) > self.finalized_head.load(Ordering::SeqCst) {
                return Err(HistoryClientError::InvalidConfig(
                    "test chunk exceeds finalized head".to_owned(),
                ));
            }
            Ok(self.extracted(from_block, to_block))
        }
    }

    #[async_trait]
    impl HistoryAttestorSource for RollingHistorySource {
        async fn probe_archive(&self) -> Result<ArchiveProbe, HistoryClientError> {
            Ok(ArchiveProbe {
                finalized_head: Self::header(self.finalized_head.load(Ordering::SeqCst)),
                contract_code_hashes: BTreeMap::new(),
            })
        }

        async fn finalized_head(&self) -> Result<CanonicalBlockHeader, HistoryClientError> {
            Ok(Self::header(self.finalized_head.load(Ordering::SeqCst)))
        }

        async fn block_header(
            &self,
            block_number: u64,
        ) -> Result<CanonicalBlockHeader, HistoryClientError> {
            Ok(Self::header(block_number))
        }

        async fn block_at_or_after(
            &self,
            _timestamp: u64,
            upper_block: u64,
        ) -> Result<CanonicalBlockHeader, HistoryClientError> {
            if upper_block < ACTIVATION_START {
                return Err(HistoryClientError::InvalidConfig(
                    "test history frontier exceeds upper block".to_owned(),
                ));
            }
            Ok(Self::header(ACTIVATION_START))
        }

        async fn fetch_chunk(
            &self,
            from_block: u64,
            to_block: u64,
        ) -> Result<AttestedHistoryChunk, HistoryClientError> {
            if to_block.saturating_add(CONFIRMATIONS) > self.finalized_head.load(Ordering::SeqCst) {
                return Err(HistoryClientError::InvalidConfig(
                    "test chunk exceeds finalized head".to_owned(),
                ));
            }
            Ok(self.attested(from_block, to_block))
        }

        async fn verify_continuity(
            &self,
            _proof: &HistoryContinuityProof,
        ) -> Result<bool, HistoryClientError> {
            Ok(true)
        }
    }

    #[derive(Clone, Copy)]
    enum ProviderBehavior {
        Success,
        Shrinkable,
        Fatal,
    }

    impl ProviderBehavior {
        fn failure(self, provider: &'static str) -> Option<HistoryClientError> {
            match self {
                Self::Success => None,
                Self::Shrinkable => Some(HistoryClientError::CanonicalChunkBudget { limit: 1 }),
                Self::Fatal => Some(HistoryClientError::Network {
                    provider,
                    operation: "fetch_chunk",
                }),
            }
        }
    }

    struct ScriptedExtractor {
        source: Arc<RollingHistorySource>,
        behavior: ProviderBehavior,
    }

    #[async_trait]
    impl HistoryExtractorSource for ScriptedExtractor {
        async fn fetch_chunk(
            &self,
            from_block: u64,
            to_block: u64,
        ) -> Result<ExtractedHistoryChunk, HistoryClientError> {
            if let Some(error) = self.behavior.failure("test-hypersync") {
                return Err(error);
            }
            Ok(self.source.extracted(from_block, to_block))
        }
    }

    struct ScriptedAttestor {
        source: Arc<RollingHistorySource>,
        behavior: ProviderBehavior,
    }

    #[async_trait]
    impl HistoryAttestorSource for ScriptedAttestor {
        async fn probe_archive(&self) -> Result<ArchiveProbe, HistoryClientError> {
            Ok(ArchiveProbe {
                finalized_head: RollingHistorySource::header(
                    self.source.finalized_head.load(Ordering::SeqCst),
                ),
                contract_code_hashes: BTreeMap::new(),
            })
        }

        async fn finalized_head(&self) -> Result<CanonicalBlockHeader, HistoryClientError> {
            Ok(RollingHistorySource::header(
                self.source.finalized_head.load(Ordering::SeqCst),
            ))
        }

        async fn block_header(
            &self,
            block_number: u64,
        ) -> Result<CanonicalBlockHeader, HistoryClientError> {
            Ok(RollingHistorySource::header(block_number))
        }

        async fn block_at_or_after(
            &self,
            _timestamp: u64,
            upper_block: u64,
        ) -> Result<CanonicalBlockHeader, HistoryClientError> {
            Ok(RollingHistorySource::header(upper_block))
        }

        async fn fetch_chunk(
            &self,
            from_block: u64,
            to_block: u64,
        ) -> Result<AttestedHistoryChunk, HistoryClientError> {
            if let Some(error) = self.behavior.failure("test-attestor") {
                return Err(error);
            }
            Ok(self.source.attested(from_block, to_block))
        }

        async fn verify_continuity(
            &self,
            _proof: &HistoryContinuityProof,
        ) -> Result<bool, HistoryClientError> {
            Ok(true)
        }
    }

    impl ExchangeHistoryWorker {
        fn test_sources(
            repository: Arc<MemoryHistoryRepository>,
            extractor: Arc<dyn HistoryExtractorSource>,
            attestor: Arc<dyn HistoryAttestorSource>,
        ) -> Self {
            let config = FinalizedExchangeHistoryConfig {
                enabled: true,
                min_blocks_per_chunk: 1,
                max_blocks_per_chunk: 100,
                retry_initial_ms: 1,
                retry_max_ms: 1,
                retry_max_attempts: 1,
                model_confirmation_blocks: CONFIRMATIONS,
                hot_window_blocks_per_tick: 100,
                full_history_blocks_per_tick: 100,
                batch_size: 100,
                ..FinalizedExchangeHistoryConfig::default()
            };
            Self {
                extractor,
                attestor,
                history_repo: repository,
                market_repo: Arc::new(EmptyMarketRepository),
                writers: ExchangeHistoryWriters::memory(),
                policy_hash: Self::availability_policy_hash(&config).expect("test history policy"),
                progress: ExchangeHistoryProgressHandle::fresh_boot(),
                metrics: Arc::new(MetricsHub::new()),
                adaptive_chunk_blocks: AtomicU64::new(config.max_blocks_per_chunk),
                adaptive_success_count: AtomicU64::new(0),
                config,
            }
        }

        fn test(
            repository: Arc<MemoryHistoryRepository>,
            source: Arc<RollingHistorySource>,
        ) -> Self {
            let extractor: Arc<dyn HistoryExtractorSource> =
                Arc::<RollingHistorySource>::clone(&source);
            let attestor: Arc<dyn HistoryAttestorSource> = source;
            Self::test_sources(repository, extractor, attestor)
        }

        fn scripted(
            extractor_behavior: ProviderBehavior,
            attestor_behavior: ProviderBehavior,
        ) -> Self {
            let source = Arc::new(RollingHistorySource::new(112));
            let extractor: Arc<dyn HistoryExtractorSource> = Arc::new(ScriptedExtractor {
                source: Arc::clone(&source),
                behavior: extractor_behavior,
            });
            let attestor: Arc<dyn HistoryAttestorSource> = Arc::new(ScriptedAttestor {
                source,
                behavior: attestor_behavior,
            });
            Self::test_sources(
                Arc::new(MemoryHistoryRepository::default()),
                extractor,
                attestor,
            )
        }
    }

    #[test]
    fn shrink_classifier_is_bounded() {
        assert!(shrinkable(&HistoryClientError::CanonicalChunkBudget {
            limit: 1
        }));
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

    #[tokio::test]
    async fn provider_failure_matrix() {
        enum Expected {
            Success,
            Shrink,
            Extraction,
            Attestation,
            Pair,
        }

        let cases = [
            (
                "both providers succeed",
                ProviderBehavior::Success,
                ProviderBehavior::Success,
                Expected::Success,
            ),
            (
                "extractor alone fails fatally",
                ProviderBehavior::Fatal,
                ProviderBehavior::Success,
                Expected::Extraction,
            ),
            (
                "attestor alone fails fatally",
                ProviderBehavior::Success,
                ProviderBehavior::Fatal,
                Expected::Attestation,
            ),
            (
                "extractor alone requires shrink",
                ProviderBehavior::Shrinkable,
                ProviderBehavior::Success,
                Expected::Shrink,
            ),
            (
                "attestor alone requires shrink",
                ProviderBehavior::Success,
                ProviderBehavior::Shrinkable,
                Expected::Shrink,
            ),
            (
                "both providers require shrink",
                ProviderBehavior::Shrinkable,
                ProviderBehavior::Shrinkable,
                Expected::Shrink,
            ),
            (
                "extractor shrink cannot hide attestor fatal",
                ProviderBehavior::Shrinkable,
                ProviderBehavior::Fatal,
                Expected::Pair,
            ),
            (
                "attestor shrink cannot hide extractor fatal",
                ProviderBehavior::Fatal,
                ProviderBehavior::Shrinkable,
                Expected::Pair,
            ),
            (
                "both providers fail fatally",
                ProviderBehavior::Fatal,
                ProviderBehavior::Fatal,
                Expected::Pair,
            ),
        ];

        for (label, extractor, attestor, expected) in cases {
            let worker = ExchangeHistoryWorker::scripted(extractor, attestor);
            let result = worker
                .fetch_providers(ExchangeHistoryFrontier::Activation, 100, 100)
                .await;
            match (expected, result) {
                (Expected::Success, Ok(_)) | (Expected::Shrink, Err(FetchFailure::Shrink)) => {}
                (
                    Expected::Extraction,
                    Err(FetchFailure::Contract(QuantError::ExchangeHistory(
                        ExchangeHistoryError::Extraction { detail },
                    ))),
                ) => assert_eq!(
                    detail, "test-hypersync request failed during fetch_chunk",
                    "{label}"
                ),
                (
                    Expected::Attestation,
                    Err(FetchFailure::Contract(QuantError::ExchangeHistory(
                        ExchangeHistoryError::Attestation { detail },
                    ))),
                ) => assert_eq!(
                    detail, "test-attestor request failed during fetch_chunk",
                    "{label}"
                ),
                (
                    Expected::Pair,
                    Err(FetchFailure::Contract(QuantError::ExchangeHistory(
                        ExchangeHistoryError::ProviderFailures {
                            extractor,
                            attestor,
                        },
                    ))),
                ) => {
                    assert!(!extractor.is_empty(), "{label}");
                    assert!(!attestor.is_empty(), "{label}");
                }
                _ => panic!("unexpected provider-pair result for {label}"),
            }
        }
    }

    #[tokio::test]
    async fn probe_classifies_provider_pairs() {
        let cases = [
            (
                "both providers succeed",
                ProviderBehavior::Success,
                ProviderBehavior::Success,
                None,
            ),
            (
                "extractor alone fails",
                ProviderBehavior::Fatal,
                ProviderBehavior::Success,
                Some("extraction"),
            ),
            (
                "attestor alone fails",
                ProviderBehavior::Success,
                ProviderBehavior::Fatal,
                Some("attestation"),
            ),
            (
                "both providers fail",
                ProviderBehavior::Fatal,
                ProviderBehavior::Fatal,
                Some("pair"),
            ),
        ];

        for (label, extractor, attestor, expected) in cases {
            let result = ExchangeHistoryWorker::scripted(extractor, attestor)
                .probe()
                .await;
            match (expected, result) {
                (None, Ok(()))
                | (
                    Some("extraction"),
                    Err(QuantError::ExchangeHistory(ExchangeHistoryError::Extraction { .. })),
                )
                | (
                    Some("attestation"),
                    Err(QuantError::ExchangeHistory(ExchangeHistoryError::Attestation { .. })),
                )
                | (
                    Some("pair"),
                    Err(QuantError::ExchangeHistory(ExchangeHistoryError::ProviderFailures {
                        ..
                    })),
                ) => {}
                _ => panic!("unexpected startup probe result for {label}"),
            }
        }
    }

    #[test]
    fn activation_target_tracks_head() {
        let plan = ExchangeHistoryPlanInfo {
            plan_id: Uuid::nil(),
            chain_id: 137,
            policy_hash: ContentHash::from_bytes([1; 32]),
            bootstrap_profile_set_hash: ContentHash::from_bytes([2; 32]),
            finalized_anchor_block: 112,
            finalized_anchor_hash: block_hash(&format!("0x{:064x}", 112)).expect("anchor hash"),
            finalized_anchor_timestamp: 1_700_000_224,
            activation_from_block: 100,
            activation_through_block: 100,
            crypto_required_from_block: 100,
            weather_required_from_block: 100,
            retention_from_block: 100,
            retention_through_block: 99,
            created_at: Utc::now(),
        };

        assert_eq!(
            ExchangeHistoryWorker::activation_target(&plan, 103).expect("rolling target"),
            103
        );
        assert!(ExchangeHistoryWorker::activation_target(&plan, 99).is_err());
    }

    #[test]
    fn serving_id_binds_predecessor() {
        let plan_id = Uuid::from_u128(1);
        let chunks = [HistorySealChunkRef {
            chunk_id: Uuid::from_u128(2),
            frontier: ExchangeHistoryFrontier::Activation,
            state_revision: 3,
            from_block: 100,
            to_block: 110,
        }];
        let predecessor = HistoryServingHeadSealId::new(Uuid::from_u128(4));
        let initial = ExchangeHistoryWorker::serving_head_id(plan_id, None, &chunks);
        let successor = ExchangeHistoryWorker::serving_head_id(plan_id, Some(predecessor), &chunks);

        assert_ne!(initial, successor);
        assert_eq!(
            successor,
            ExchangeHistoryWorker::serving_head_id(plan_id, Some(predecessor), &chunks)
        );
    }

    #[test]
    fn policy_frontiers_are_committed() {
        let baseline = FinalizedExchangeHistoryConfig::default();
        let baseline_hash = ExchangeHistoryWorker::availability_policy_hash(&baseline)
            .expect("hash baseline history policy");

        let mut activation = baseline.clone();
        activation.activation_frontier_days += 1;
        assert_ne!(
            ExchangeHistoryWorker::availability_policy_hash(&activation)
                .expect("hash activation history policy"),
            baseline_hash
        );

        let mut retention = baseline.clone();
        retention.retention_frontier_days += 1;
        assert_ne!(
            ExchangeHistoryWorker::availability_policy_hash(&retention)
                .expect("hash retention history policy"),
            baseline_hash
        );

        let mut rollback = baseline.clone();
        rollback.rollback_buffer_blocks += 1;
        assert_ne!(
            ExchangeHistoryWorker::availability_policy_hash(&rollback)
                .expect("hash rollback history policy"),
            baseline_hash
        );

        let mut scheduling = baseline;
        scheduling.poll_secs += 1;
        scheduling.max_blocks_per_chunk += 1;
        assert_eq!(
            ExchangeHistoryWorker::availability_policy_hash(&scheduling)
                .expect("hash scheduling history policy"),
            baseline_hash
        );
    }

    #[tokio::test]
    async fn catchup_preserves_initial_cut() {
        let repository = Arc::new(MemoryHistoryRepository::default());
        let source = Arc::new(RollingHistorySource::new(116));
        let worker = ExchangeHistoryWorker::test(Arc::clone(&repository), Arc::clone(&source));
        let initial = worker
            .ensure_plan(&RollingHistorySource::header(112), 100)
            .await
            .expect("freeze the initial plan before catch-up starts");
        assert!(repository.snapshot().chunks.is_empty());

        worker
            .run_once()
            .await
            .expect("catch up beyond the frozen target");
        assert_eq!(
            source.requested.lock().as_slice(),
            &[(100, 100), (101, 104)]
        );
        let caught_up = repository.snapshot();
        assert_eq!(caught_up.plan.as_ref(), Some(&initial));
        assert_eq!(
            caught_up
                .heads
                .last()
                .expect("live serving head")
                .seal
                .accepted_through_block,
            104
        );
        assert!(
            caught_up
                .heads
                .iter()
                .any(|head| head.seal.accepted_through_block == 100)
        );
        assert!(
            caught_up
                .chunks
                .iter()
                .all(|chunk| chunk.to_block <= 100 || chunk.from_block > 100)
        );

        source.advance(120);
        worker.run_once().await.expect("append newer live history");
        let rolling = repository.snapshot();
        assert_eq!(rolling.plan.as_ref(), Some(&initial));
        assert_eq!(source.requested.lock().last(), Some(&(105, 108)));
        assert_eq!(
            rolling
                .heads
                .last()
                .expect("new live head")
                .seal
                .accepted_through_block,
            108
        );
        let restarted = ExchangeHistoryWorker::test(Arc::clone(&repository), source);
        restarted
            .run_once()
            .await
            .expect("restart without changing the frozen prefix");
        let replayed = repository.snapshot();
        assert_eq!(replayed.chunks, rolling.chunks);
        assert_eq!(replayed.heads, rolling.heads);
    }

    #[tokio::test]
    async fn catchup_keeps_tick_budget() {
        let repository = Arc::new(MemoryHistoryRepository::default());
        let source = Arc::new(RollingHistorySource::new(192));
        let mut worker = ExchangeHistoryWorker::test(Arc::clone(&repository), Arc::clone(&source));
        worker.config.hot_window_blocks_per_tick = 50;
        worker
            .run_once()
            .await
            .expect("first bounded catch-up pass");
        let initial = repository.snapshot().plan.expect("initial history plan");
        assert_eq!(initial.activation_through_block, 180);
        assert_eq!(source.requested.lock().as_slice(), &[(100, 149)]);

        source.advance(208);
        worker
            .run_once()
            .await
            .expect("cross the preserved cut within one tick budget");
        assert_eq!(
            source.requested.lock().as_slice(),
            &[(100, 149), (150, 180), (181, 196)]
        );
        let final_state = repository.snapshot();
        assert_eq!(final_state.plan.as_ref(), Some(&initial));
        assert_eq!(
            final_state
                .heads
                .last()
                .expect("caught-up live head")
                .seal
                .accepted_through_block,
            196
        );
    }

    #[tokio::test]
    async fn shrinking_preserves_initial_cut() {
        let repository = Arc::new(MemoryHistoryRepository::default());
        let source = Arc::new(RollingHistorySource::new(118));
        source.max_span.store(2, Ordering::SeqCst);
        let worker = ExchangeHistoryWorker::test(Arc::clone(&repository), Arc::clone(&source));
        worker
            .ensure_plan(&RollingHistorySource::header(115), 103)
            .await
            .expect("initial plan");
        worker
            .run_once()
            .await
            .expect("adaptive provider shrink remains bounded by the cut");
        assert!(
            source
                .requested
                .lock()
                .iter()
                .all(|(from, to)| *to <= 103 || *from > 103)
        );
        let state = repository.snapshot();
        let ranges = state
            .chunks
            .iter()
            .filter(|chunk| chunk.status == ExchangeHistoryChunkStatus::Accepted)
            .map(|chunk| (chunk.from_block, chunk.to_block))
            .collect::<Vec<_>>();
        assert_eq!(ranges, vec![(100, 101), (102, 103), (104, 105), (106, 106)]);
        assert_eq!(
            state
                .heads
                .last()
                .expect("fully caught-up head")
                .seal
                .accepted_through_block,
            106
        );
    }

    #[tokio::test]
    async fn rolling_head_is_idempotent() {
        let repository = Arc::new(MemoryHistoryRepository::default());
        let source = Arc::new(RollingHistorySource::new(112));
        let worker = ExchangeHistoryWorker::test(Arc::clone(&repository), Arc::clone(&source));

        worker.run_once().await.expect("initial history pass");
        let first = repository.snapshot();
        let initial_plan = first.plan.clone().expect("initial immutable plan");
        let first_chunks = first
            .chunks
            .iter()
            .filter(|chunk| chunk.status == ExchangeHistoryChunkStatus::Accepted)
            .collect::<Vec<_>>();
        assert_eq!(first_chunks.len(), 1);
        assert_eq!(
            (first_chunks[0].from_block, first_chunks[0].to_block),
            (100, 100)
        );
        assert_eq!(first.heads.len(), 1);
        assert_eq!(first.heads[0].seal.accepted_through_block, 100);

        source.advance(115);
        worker.run_once().await.expect("rolling history pass");
        let second = repository.snapshot();
        let second_plan = second.plan.clone().expect("persisted immutable plan");
        let second_chunks = second
            .chunks
            .iter()
            .filter(|chunk| chunk.status == ExchangeHistoryChunkStatus::Accepted)
            .collect::<Vec<_>>();
        assert_eq!(second_plan, initial_plan);
        assert_eq!(second_plan.finalized_anchor_block, 112);
        assert_eq!(second_plan.activation_through_block, 100);
        assert_eq!(second_chunks.len(), 2);
        assert_eq!(
            (second_chunks[1].from_block, second_chunks[1].to_block),
            (101, 103)
        );
        assert_eq!(second.heads.len(), 2);
        assert_ne!(
            second.heads[0].seal.serving_head_seal_id,
            second.heads[1].seal.serving_head_seal_id
        );
        assert_eq!(
            second.heads[1].seal.previous_seal_id,
            Some(second.heads[0].seal.serving_head_seal_id)
        );
        assert_eq!(second.heads[1].seal.accepted_through_block, 103);
        assert!(
            second.heads[1].seal.effective_through_at > second.heads[0].seal.effective_through_at
        );
        let rolling_progress = worker.progress().snapshot();
        assert_eq!(rolling_progress.target_block, Some(103));
        assert_eq!(rolling_progress.accepted_through_block, Some(103));

        let restarted = ExchangeHistoryWorker::test(Arc::clone(&repository), Arc::clone(&source));
        restarted.run_once().await.expect("restart history pass");
        let replayed = repository.snapshot();
        assert_eq!(replayed.plan, second.plan);
        assert_eq!(replayed.chunks, second.chunks);
        assert_eq!(replayed.heads, second.heads);
        let progress = restarted.progress().snapshot();
        assert_eq!(progress.target_block, Some(103));
        assert_eq!(progress.accepted_through_block, Some(103));
        assert_eq!(progress.stage, ExchangeHistoryStage::ActivationReady);
    }
}

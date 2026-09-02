//! Finalized-execution fact-read fixtures for report/factor E2E harnesses.

use std::{collections::HashMap, sync::Arc};

use async_trait::async_trait;
use chrono::{DateTime, TimeZone, Utc};
use quant_pivot_error::storage::StorageError;
use quant_pivot_models::{
    clickhouse::{
        BookL2LedgerRow, BookMicrostructureRow, ChDigest, ChPrice, ChShares, ChUsd,
        DomainObservationRow, ExecutionParticipantFactRow, ExecutionParticipantRow,
        MarketExecutionRow, MarketResolutionRow, MidPriceBucketRow,
    },
    config::FinalizedExchangeHistoryConfig,
    domain::data_plane::{
        CreateHistoryFitSeal, CreateHistoryServingHeadSeal, ExchangeHistoryChunkInfo,
        ExchangeHistoryChunkStatus, ExchangeHistoryContinuityBasis, ExchangeHistoryFrontier,
        ExchangeHistoryPlanInfo, ExchangeHistoryQuarantineInfo, ExchangeHistoryQuarantineRead,
        ExchangeHistoryQuarantineRecord, ExchangeHistoryQuarantineResolutionInfo, HistoryFitSeal,
        HistorySealChunkRef, HistoryServingHeadSeal, HistoryServingHeadSealInfo,
        NewExchangeHistoryChunk, NewExchangeHistoryPlan, NewExchangeHistoryQuarantine,
        NewExchangeHistoryQuarantineResolution, ResolveAcceptedHistoryRange,
    },
    enums::clickhouse::{ChExchangeSide, ChExecutionParticipantRole},
    types::{
        ContentHash, DomainInstrumentKey, EvmBlockHash, HistoryFitSealId, HistoryServingHeadSealId,
        MarketId, Price, Shares, TokenId, Usd,
    },
};
use quant_pivot_repository::traits::{ExchangeHistoryRepository, QuantFactReadRepository};
use rust_decimal::Decimal;
use uuid::Uuid;

/// Default whale-window execution count (passes the structural gate of 20).
pub const WHALE_FIXTURE_EXECUTION_COUNT: usize = 25;

/// Immutable activation head used by hermetic online feature/report fixtures.
#[must_use]
pub fn live_activation_head() -> HistoryServingHeadSeal {
    live_history_head(ExchangeHistoryFrontier::Activation, None, Utc::now())
}

fn live_history_head(
    frontier: ExchangeHistoryFrontier,
    through_at: Option<DateTime<Utc>>,
    created_at: DateTime<Utc>,
) -> HistoryServingHeadSeal {
    let chunk = live_chunk(frontier, through_at, created_at);
    let mut head = HistoryServingHeadSeal {
        seal: HistoryServingHeadSealInfo {
            serving_head_seal_id: Uuid::from_u128(8).into(),
            seal_hash: ContentHash::from_bytes([0; 32]),
            plan_id: Uuid::from_u128(9),
            frontier,
            previous_seal_id: None,
            window_from_block: chunk.from_block,
            accepted_through_block: chunk.to_block,
            effective_through_at: chunk
                .effective_through_at
                .expect("fixture accepted-through timestamp"),
            policy_hash: chunk
                .hypersync_digest
                .expect("fixture accepted chunk digest"),
            created_at: chunk.created_at,
        },
        chunks: vec![HistorySealChunkRef {
            chunk_id: chunk.chunk_id,
            frontier,
            state_revision: chunk.state_revision.expect("fixture state revision"),
            from_block: chunk.from_block,
            to_block: chunk.to_block,
        }],
    };
    head.seal.seal_hash = head
        .derive_hash()
        .expect("canonical fixture serving-head hash");
    head
}

/// Build bilateral on-chain participant rows with concentrated maker notional.
///
/// One whale address receives `whale_share` of total notional; the remaining
/// makers split the rest evenly. Every economic execution has an equal-notional
/// taker row with the same execution id, so participant notional is exactly
/// twice economic execution notional.
#[must_use]
pub fn whale_execution_rows(
    market_id: &MarketId,
    token_id: &TokenId,
    event_time_ms: i64,
    unique_participants: usize,
    whale_share: Decimal,
    total_notional_usd: Decimal,
) -> Vec<ExecutionParticipantFactRow> {
    assert!(unique_participants >= 2);
    assert!(whale_share > Decimal::ZERO && whale_share < Decimal::ONE);

    let whale_notional = (total_notional_usd * whale_share).round_dp(8);
    let retail_total = total_notional_usd - whale_notional;
    let retail_count = unique_participants - 1;
    let retail_each = (retail_total / Decimal::from(retail_count)).round_dp(8);

    let mut rows = Vec::with_capacity(unique_participants * 2);
    rows.extend(bilateral_execution_rows(
        market_id,
        token_id,
        event_time_ms - i64::try_from(unique_participants).unwrap_or(i64::MAX) * 1_000,
        "0xwhale",
        whale_notional,
        0,
    ));
    for idx in 1..unique_participants {
        rows.extend(bilateral_execution_rows(
            market_id,
            token_id,
            event_time_ms - i64::try_from(unique_participants - idx).unwrap_or(i64::MAX) * 1_000,
            &format!("0xretail{idx:02}"),
            retail_each,
            idx,
        ));
    }
    rows
}

/// Map one market to a whale concentration fixture window.
#[must_use]
pub fn whale_concentration_by_market(
    market_id: &MarketId,
    token_id: &TokenId,
    event_time_ms: i64,
) -> HashMap<MarketId, Vec<ExecutionParticipantFactRow>> {
    HashMap::from([(
        market_id.clone(),
        whale_execution_rows(
            market_id,
            token_id,
            event_time_ms,
            WHALE_FIXTURE_EXECUTION_COUNT,
            Decimal::new(90, 2),
            Decimal::from(10_000),
        ),
    )])
}

fn bilateral_execution_rows(
    market_id: &MarketId,
    token_id: &TokenId,
    event_time_ms: i64,
    maker_address: &str,
    notional: Decimal,
    sequence: usize,
) -> [ExecutionParticipantFactRow; 2] {
    let price_step = i64::try_from(sequence).unwrap_or(i64::MAX - 40);
    let price = Price::new(Decimal::new(40 + price_step, 2));
    let shares = Shares::new((notional / price.inner()).round_dp(8));
    let hash = ContentHash::from_bytes(
        *blake3::hash(
            format!(
                "execution:{}:{}:{sequence}",
                market_id.as_str(),
                token_id.as_str()
            )
            .as_bytes(),
        )
        .as_bytes(),
    );
    let row = |participant_address: String, participant_role| ExecutionParticipantFactRow {
        execution_id: ChDigest::from(hash),
        market_id: market_id.clone(),
        token_id: token_id.clone(),
        participant_address,
        participant_role,
        side: ChExchangeSide::Buy,
        price: ChPrice::from(price),
        size_shares: ChShares::from(shares),
        notional_usd: ChUsd::from(Usd::new(notional)),
        transaction_hash: format!("0x{sequence:064x}"),
        effective_at: event_time_ms,
        observed_at: event_time_ms,
        model_available_at: event_time_ms,
        availability_policy_hash: ChDigest::from(hash),
    };
    [
        row(maker_address.to_owned(), ChExecutionParticipantRole::Maker),
        row(
            format!("0xtaker{sequence:02}"),
            ChExecutionParticipantRole::Taker,
        ),
    ]
}

/// Accepted-frontier repository used by in-memory feature fixtures.
pub struct LiveExchangeHistoryRepo {
    head_available: bool,
    through_at: Option<DateTime<Utc>>,
    created_at: DateTime<Utc>,
}

impl LiveExchangeHistoryRepo {
    /// Freeze a finalized-history watermark instead of using the open fixture horizon.
    #[must_use]
    pub fn through(through_at: DateTime<Utc>) -> Self {
        Self {
            head_available: true,
            through_at: Some(through_at),
            created_at: Utc::now(),
        }
    }
}

fn live_chunk(
    frontier: ExchangeHistoryFrontier,
    through_at: Option<DateTime<Utc>>,
    created_at: DateTime<Utc>,
) -> ExchangeHistoryChunkInfo {
    let through = through_at.unwrap_or_else(|| {
        Utc.with_ymd_and_hms(2099, 1, 1, 0, 0, 0)
            .single()
            .expect("fixture history timestamp")
    });
    let digest = ContentHash::from_bytes([7_u8; 32]);
    let block_hash =
        EvmBlockHash::parse(format!("0x{}", "a".repeat(64))).expect("fixture block hash");
    ExchangeHistoryChunkInfo {
        chunk_id: Uuid::from_u128(7),
        frontier,
        from_block: 1,
        to_block: i64::MAX - 1,
        status: ExchangeHistoryChunkStatus::Accepted,
        attempt_count: 1,
        hypersync_count: Some(1),
        attestor_count: Some(1),
        hypersync_digest: Some(digest),
        attestor_digest: Some(digest),
        first_block_hash: Some(block_hash.clone()),
        last_block_hash: Some(block_hash.clone()),
        archive_height: Some(i64::MAX - 1),
        continuity_basis: Some(ExchangeHistoryContinuityBasis::HyperSyncBoundaryHeaders),
        continuity_block: Some(0),
        continuity_hash: Some(block_hash),
        effective_through_at: Some(through),
        state_revision: Some(1),
        accepted_at: Some(created_at),
        created_at,
        updated_at: created_at,
    }
}

#[async_trait]
impl ExchangeHistoryRepository for LiveExchangeHistoryRepo {
    async fn create_or_load_plan(
        &self,
        _plan: NewExchangeHistoryPlan,
    ) -> Result<ExchangeHistoryPlanInfo, StorageError> {
        Err(StorageError::InvariantViolation {
            entity: Some("exchange_history_plan"),
            detail: "LiveExchangeHistoryRepo is read-only".to_owned(),
        })
    }

    async fn load_plan(
        &self,
        _chain_id: i64,
    ) -> Result<Option<ExchangeHistoryPlanInfo>, StorageError> {
        Ok(None)
    }

    async fn find_range(
        &self,
        _frontier: ExchangeHistoryFrontier,
        _from_block: i64,
        _to_block: i64,
    ) -> Result<Option<ExchangeHistoryChunkInfo>, StorageError> {
        Ok(None)
    }

    async fn save_chunk(
        &self,
        _chunk: NewExchangeHistoryChunk,
    ) -> Result<ExchangeHistoryChunkInfo, StorageError> {
        Err(StorageError::InvariantViolation {
            entity: Some("exchange_history_chunk"),
            detail: "LiveExchangeHistoryRepo is read-only".to_owned(),
        })
    }

    async fn latest_accepted(
        &self,
        frontier: ExchangeHistoryFrontier,
    ) -> Result<Option<ExchangeHistoryChunkInfo>, StorageError> {
        Ok(self
            .head_available
            .then(|| live_chunk(frontier, self.through_at, self.created_at)))
    }

    async fn earliest_accepted(
        &self,
        frontier: ExchangeHistoryFrontier,
    ) -> Result<Option<ExchangeHistoryChunkInfo>, StorageError> {
        Ok(self
            .head_available
            .then(|| live_chunk(frontier, self.through_at, self.created_at)))
    }

    async fn accepted_from(
        &self,
        _frontier: ExchangeHistoryFrontier,
        _from_block: i64,
    ) -> Result<Vec<ExchangeHistoryChunkInfo>, StorageError> {
        Ok(Vec::new())
    }

    async fn rewind_from(
        &self,
        _frontier: ExchangeHistoryFrontier,
        _from_block: i64,
        _updated_at: DateTime<Utc>,
    ) -> Result<Vec<ExchangeHistoryChunkInfo>, StorageError> {
        Ok(Vec::new())
    }

    async fn quarantine_chunk(
        &self,
        _chunk: NewExchangeHistoryChunk,
        _quarantine: NewExchangeHistoryQuarantine,
    ) -> Result<ExchangeHistoryQuarantineInfo, StorageError> {
        Err(StorageError::InvariantViolation {
            entity: Some("exchange_history_quarantine"),
            detail: "LiveExchangeHistoryRepo is read-only".to_owned(),
        })
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
        Err(StorageError::InvariantViolation {
            entity: Some("exchange_history_quarantine_resolution"),
            detail: "LiveExchangeHistoryRepo is read-only".to_owned(),
        })
    }

    async fn resolve_accepted_range(
        &self,
        _resolution: ResolveAcceptedHistoryRange,
    ) -> Result<Vec<ExchangeHistoryQuarantineResolutionInfo>, StorageError> {
        Err(StorageError::InvariantViolation {
            entity: Some("exchange_history_quarantine_resolution"),
            detail: "LiveExchangeHistoryRepo is read-only".to_owned(),
        })
    }

    async fn create_fit_seal(
        &self,
        _command: CreateHistoryFitSeal,
    ) -> Result<HistoryFitSeal, StorageError> {
        Err(StorageError::InvariantViolation {
            entity: Some("history_fit_seal"),
            detail: "LiveExchangeHistoryRepo is read-only".to_owned(),
        })
    }

    async fn find_fit_seal(
        &self,
        _fit_seal_id: HistoryFitSealId,
    ) -> Result<Option<HistoryFitSeal>, StorageError> {
        Ok(None)
    }

    async fn create_serving_head(
        &self,
        _command: CreateHistoryServingHeadSeal,
    ) -> Result<HistoryServingHeadSeal, StorageError> {
        Err(StorageError::InvariantViolation {
            entity: Some("history_serving_head_seal"),
            detail: "LiveExchangeHistoryRepo is read-only".to_owned(),
        })
    }

    async fn latest_serving_head(
        &self,
        frontier: ExchangeHistoryFrontier,
    ) -> Result<Option<HistoryServingHeadSeal>, StorageError> {
        Ok(self
            .head_available
            .then(|| live_history_head(frontier, self.through_at, self.created_at)))
    }

    async fn serving_head_at(
        &self,
        frontier: ExchangeHistoryFrontier,
        _decision_at: DateTime<Utc>,
    ) -> Result<Option<HistoryServingHeadSeal>, StorageError> {
        self.latest_serving_head(frontier).await
    }

    async fn validate_fit_seal(
        &self,
        _fit_seal_id: HistoryFitSealId,
        _seal_hash: ContentHash,
    ) -> Result<HistoryFitSeal, StorageError> {
        Err(StorageError::not_found("history_fit_seal", "fixture"))
    }

    async fn validate_serving_head(
        &self,
        serving_head_seal_id: HistoryServingHeadSealId,
        seal_hash: ContentHash,
    ) -> Result<HistoryServingHeadSeal, StorageError> {
        let seal = self
            .latest_serving_head(ExchangeHistoryFrontier::Activation)
            .await?
            .ok_or_else(|| StorageError::not_found("history_serving_head_seal", "fixture"))?;
        if seal.seal.serving_head_seal_id != serving_head_seal_id
            || seal.seal.seal_hash != seal_hash
        {
            return Err(StorageError::state_conflict(
                "history_serving_head_seal",
                Some(serving_head_seal_id),
                "fixture serving seal mismatch",
            ));
        }
        Ok(seal)
    }
}

/// Healthy ingest cursor repo for tests.
#[must_use]
pub fn live_history_repo() -> Arc<dyn ExchangeHistoryRepository> {
    Arc::new(LiveExchangeHistoryRepo {
        head_available: true,
        through_at: None,
        created_at: Utc::now(),
    })
}

/// Disabled/unwarmed finalized-history source for monitor degradation tests.
#[must_use]
pub fn unavailable_history_repo() -> Arc<dyn ExchangeHistoryRepository> {
    Arc::new(LiveExchangeHistoryRepo {
        head_available: false,
        through_at: None,
        created_at: Utc::now(),
    })
}

/// Enabled finalized-history policy for fixtures with accepted history facts.
#[must_use]
pub fn live_history_config() -> FinalizedExchangeHistoryConfig {
    FinalizedExchangeHistoryConfig {
        enabled: true,
        ..FinalizedExchangeHistoryConfig::default()
    }
}

/// In-memory fact reader that serves finalized executions from a fixture map.
pub struct ConfigurableFactRead {
    inner: Arc<dyn QuantFactReadRepository>,
    execution_history_by_market: HashMap<MarketId, Vec<ExecutionParticipantFactRow>>,
}

impl ConfigurableFactRead {
    #[must_use]
    pub fn new(
        inner: Arc<dyn QuantFactReadRepository>,
        execution_history_by_market: HashMap<MarketId, Vec<ExecutionParticipantFactRow>>,
    ) -> Self {
        Self {
            inner,
            execution_history_by_market,
        }
    }
}

#[async_trait]
impl QuantFactReadRepository for ConfigurableFactRead {
    async fn microstructure_window(
        &self,
        token_ids: Vec<TokenId>,
        from_ms: i64,
        to_ms: i64,
        decision_at_ms: i64,
    ) -> Result<Vec<BookMicrostructureRow>, StorageError> {
        self.inner
            .microstructure_window(token_ids, from_ms, to_ms, decision_at_ms)
            .await
    }

    async fn book_ledger_snapshot_at(
        &self,
        token_id: &TokenId,
        source_cutoff_ms: i64,
        decision_at_ms: i64,
    ) -> Result<Option<BookL2LedgerRow>, StorageError> {
        self.inner
            .book_ledger_snapshot_at(token_id, source_cutoff_ms, decision_at_ms)
            .await
    }

    async fn book_ledger_snapshots_between(
        &self,
        token_ids: Vec<TokenId>,
        from_ms: i64,
        to_ms: i64,
        available_by_ms: i64,
    ) -> Result<Vec<BookL2LedgerRow>, StorageError> {
        self.inner
            .book_ledger_snapshots_between(token_ids, from_ms, to_ms, available_by_ms)
            .await
    }

    async fn resolution_at(
        &self,
        market_id: &MarketId,
        source_cutoff_ms: i64,
        decision_at_ms: i64,
    ) -> Result<Option<MarketResolutionRow>, StorageError> {
        self.inner
            .resolution_at(market_id, source_cutoff_ms, decision_at_ms)
            .await
    }

    async fn resolutions_between(
        &self,
        market_ids: Vec<MarketId>,
        from_ms: i64,
        to_ms: i64,
        decision_at_ms: i64,
    ) -> Result<Vec<MarketResolutionRow>, StorageError> {
        self.inner
            .resolutions_between(market_ids, from_ms, to_ms, decision_at_ms)
            .await
    }

    async fn domain_observations_between(
        &self,
        instrument_keys: Vec<DomainInstrumentKey>,
        from_ms: i64,
        to_ms: i64,
        publish_cutoff_ms: i64,
        decision_at_ms: i64,
    ) -> Result<Vec<DomainObservationRow>, StorageError> {
        self.inner
            .domain_observations_between(
                instrument_keys,
                from_ms,
                to_ms,
                publish_cutoff_ms,
                decision_at_ms,
            )
            .await
    }

    async fn domain_observation_at(
        &self,
        instrument_key: &DomainInstrumentKey,
        metric: &str,
        source_cutoff_ms: i64,
        decision_at_ms: i64,
    ) -> Result<Option<DomainObservationRow>, StorageError> {
        self.inner
            .domain_observation_at(instrument_key, metric, source_cutoff_ms, decision_at_ms)
            .await
    }

    async fn last_executions(
        &self,
        token_ids: Vec<TokenId>,
        from_ms: i64,
        to_ms: i64,
        limit: u64,
    ) -> Result<Vec<MarketExecutionRow>, StorageError> {
        self.inner
            .last_executions(token_ids, from_ms, to_ms, limit)
            .await
    }

    async fn market_execution_window(
        &self,
        market_ids: Vec<MarketId>,
        _history_chunks: Vec<HistorySealChunkRef>,
        from_ms: i64,
        to_ms: i64,
        decision_at_ms: i64,
    ) -> Result<Vec<ExecutionParticipantFactRow>, StorageError> {
        Ok(market_ids
            .into_iter()
            .flat_map(|market_id| {
                self.execution_history_by_market
                    .get(&market_id)
                    .into_iter()
                    .flatten()
                    .filter(|row| {
                        row.effective_at >= from_ms
                            && row.effective_at < to_ms
                            && row.model_available_at <= decision_at_ms
                    })
                    .cloned()
            })
            .collect())
    }

    async fn market_executions_between(
        &self,
        market_ids: Vec<MarketId>,
        history_chunks: Vec<HistorySealChunkRef>,
        from_ms: i64,
        to_ms: i64,
        decision_at_ms: i64,
    ) -> Result<Vec<MarketExecutionRow>, StorageError> {
        self.inner
            .market_executions_between(market_ids, history_chunks, from_ms, to_ms, decision_at_ms)
            .await
    }

    async fn execution_participants_between(
        &self,
        market_ids: Vec<MarketId>,
        history_chunks: Vec<HistorySealChunkRef>,
        from_ms: i64,
        to_ms: i64,
        decision_at_ms: i64,
    ) -> Result<Vec<ExecutionParticipantRow>, StorageError> {
        self.inner
            .execution_participants_between(
                market_ids,
                history_chunks,
                from_ms,
                to_ms,
                decision_at_ms,
            )
            .await
    }

    async fn mid_price_series(
        &self,
        token_ids: Vec<TokenId>,
        from_ms: i64,
        to_ms: i64,
        decision_at_ms: i64,
        bucket_secs: u32,
    ) -> Result<Vec<MidPriceBucketRow>, StorageError> {
        self.inner
            .mid_price_series(token_ids, from_ms, to_ms, decision_at_ms, bucket_secs)
            .await
    }

    async fn microstructure_series(
        &self,
        token_ids: Vec<TokenId>,
        from_ms: i64,
        to_ms: i64,
        available_by_ms: i64,
        minute: bool,
    ) -> Result<Vec<BookMicrostructureRow>, StorageError> {
        self.inner
            .microstructure_series(token_ids, from_ms, to_ms, available_by_ms, minute)
            .await
    }

    async fn observed_markets_between(
        &self,
        from_ms: i64,
        to_ms: i64,
        decision_at_ms: i64,
    ) -> Result<Vec<MarketId>, StorageError> {
        self.inner
            .observed_markets_between(from_ms, to_ms, decision_at_ms)
            .await
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use chrono::{Duration, Utc};
    use quant_pivot_error::storage::StorageError;
    use quant_pivot_models::domain::data_plane::ExchangeHistoryFrontier;
    use quant_pivot_repository::traits::ExchangeHistoryRepository;

    use super::{LiveExchangeHistoryRepo, live_history_repo};

    #[tokio::test]
    async fn repository_metadata_stays_frozen() -> Result<(), StorageError> {
        let through = Utc::now() - Duration::minutes(1);
        for repo in [
            Arc::new(LiveExchangeHistoryRepo::through(through))
                as Arc<dyn ExchangeHistoryRepository>,
            live_history_repo(),
        ] {
            let decision_at = Utc::now();
            let first_head = repo
                .latest_serving_head(ExchangeHistoryFrontier::Activation)
                .await?
                .expect("fixture activation head");
            let first_chunk = repo
                .latest_accepted(ExchangeHistoryFrontier::Activation)
                .await?
                .expect("fixture accepted chunk");
            let validated = repo
                .validate_serving_head(
                    first_head.seal.serving_head_seal_id,
                    first_head.seal.seal_hash,
                )
                .await?;
            let second_head = repo
                .serving_head_at(ExchangeHistoryFrontier::Activation, decision_at)
                .await?
                .expect("same decision-time activation head");
            let second_chunk = repo
                .earliest_accepted(ExchangeHistoryFrontier::Activation)
                .await?
                .expect("same accepted chunk");

            assert_eq!(first_head, validated);
            assert_eq!(
                first_head
                    .derive_hash()
                    .expect("canonical fixture commitment"),
                first_head.seal.seal_hash,
            );
            assert_eq!(first_head, second_head);
            assert_eq!(first_chunk, second_chunk);
            assert!(first_head.seal.created_at <= decision_at);
            assert_eq!(first_head.seal.created_at, first_chunk.created_at);
            assert_eq!(first_chunk.accepted_at, Some(first_chunk.created_at));
            assert_eq!(first_chunk.updated_at, first_chunk.created_at);
        }
        Ok(())
    }
}

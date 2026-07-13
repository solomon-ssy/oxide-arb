//! Trade-tape fact-read fixtures for report/factor E2E harnesses.

use std::collections::HashMap;
use std::sync::Arc;

use async_trait::async_trait;
use chrono::Utc;
use quant_pivot_api::exchange::EXCHANGE_CONTRACTS;
use quant_pivot_error::storage::StorageError;
use quant_pivot_models::{
    clickhouse::{
        BookMicrostructureRow, BookSnapshotRow, ChPrice, ChSchemaVersion, ChShares, ChUsd,
        DomainObservationRow, MarketResolutionRow, MidPriceBucketRow, TickEventRow, TradeTapeRow,
    },
    domain::{TradeTapeBlockCursorInfo, UpsertTradeTapeBlockCursor},
    enums::clickhouse::{ChTradeParticipantRole, ChTradeSide, ChTradeTapeSource},
    types::{DomainInstrumentKey, MarketId, Price, Shares, TokenId, Usd},
};
use quant_pivot_repository::traits::{QuantFactReadRepository, TradeTapeBlockCursorRepository};
use rust_decimal::Decimal;

/// Default whale-window participant count (passes the structural gate of 20).
pub const WHALE_FIXTURE_PARTICIPANT_COUNT: usize = 25;

/// Build maker-primary on-chain rows with extreme notional concentration.
///
/// One whale address receives `whale_share` of total notional; the remaining
/// `unique_participants - 1` retail makers split the rest evenly. Taker rows are
/// omitted so the estimand matches production (maker-primary concentration).
#[must_use]
pub fn whale_concentration_trade_tape_rows(
    market_id: &MarketId,
    token_id: &TokenId,
    event_time_ms: i64,
    unique_participants: usize,
    whale_share: Decimal,
    total_notional_usd: Decimal,
) -> Vec<TradeTapeRow> {
    assert!(unique_participants >= 2);
    assert!(whale_share > Decimal::ZERO && whale_share < Decimal::ONE);

    let whale_notional = (total_notional_usd * whale_share).round_dp(8);
    let retail_total = total_notional_usd - whale_notional;
    let retail_count = unique_participants - 1;
    let retail_each = (retail_total / Decimal::from(retail_count)).round_dp(8);

    let mut rows = Vec::with_capacity(unique_participants);
    rows.push(maker_trade_tape_row(
        market_id,
        token_id,
        event_time_ms,
        "0xwhale",
        whale_notional,
        0,
    ));
    for idx in 1..unique_participants {
        rows.push(maker_trade_tape_row(
            market_id,
            token_id,
            event_time_ms,
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
) -> HashMap<MarketId, Vec<TradeTapeRow>> {
    HashMap::from([(
        market_id.clone(),
        whale_concentration_trade_tape_rows(
            market_id,
            token_id,
            event_time_ms,
            WHALE_FIXTURE_PARTICIPANT_COUNT,
            Decimal::new(90, 2),
            Decimal::from(10_000),
        ),
    )])
}

fn maker_trade_tape_row(
    market_id: &MarketId,
    token_id: &TokenId,
    event_time_ms: i64,
    address: &str,
    notional: Decimal,
    sequence: usize,
) -> TradeTapeRow {
    let price = Price::new(Decimal::new(50, 2));
    let shares = Shares::new((notional / price.inner()).round_dp(8));
    TradeTapeRow {
        market_id: market_id.clone(),
        token_id: token_id.clone(),
        event_time: event_time_ms,
        ingestion_time: event_time_ms,
        participant_address: address.to_owned(),
        participant_role: ChTradeParticipantRole::Maker,
        side: ChTradeSide::Buy,
        price: ChPrice::from(price),
        size_shares: ChShares::from(shares),
        notional_usd: ChUsd::from(Usd::new(notional)),
        tx_hash: Some(format!("0xfixture{sequence:04}")),
        trade_id: format!("fixture:{address}:{sequence}"),
        source: ChTradeTapeSource::OnChain,
        coverage_flags: 0,
        raw_payload_json: None,
        schema_version: ChSchemaVersion::FIRST,
    }
}

/// Block-cursor repo that reports healthy on-chain ingest for all exchange contracts.
///
/// Use in E2E harnesses that read trade tape from an in-memory fact reader but still
/// need `trade_tape_ingest_available` to pass.
pub struct LiveTradeTapeBlockCursorRepo;

#[async_trait]
impl TradeTapeBlockCursorRepository for LiveTradeTapeBlockCursorRepo {
    async fn find(
        &self,
        _source: &str,
        _contract_address: &str,
    ) -> Result<Option<TradeTapeBlockCursorInfo>, StorageError> {
        Ok(None)
    }

    async fn upsert(
        &self,
        _cursor: UpsertTradeTapeBlockCursor,
    ) -> Result<TradeTapeBlockCursorInfo, StorageError> {
        Err(StorageError::InvariantViolation {
            entity: Some("trade_tape_block_cursor"),
            detail: "LiveTradeTapeBlockCursorRepo is read-only".to_owned(),
        })
    }

    async fn list_by_source(
        &self,
        source: &str,
    ) -> Result<Vec<TradeTapeBlockCursorInfo>, StorageError> {
        let now = Utc::now();
        Ok(EXCHANGE_CONTRACTS
            .iter()
            .map(|contract| TradeTapeBlockCursorInfo {
                source: source.to_owned(),
                contract_address: format!("{:#x}", contract.address),
                last_finalized_block: i64::try_from(contract.bootstrap_block)
                    .expect("fixture bootstrap block fits i64"),
                last_log_index: 0,
                head_lag_blocks: 0,
                status: "live".to_owned(),
                created_at: now,
                updated_at: now,
            })
            .collect())
    }
}

/// Healthy ingest cursor repo for tests.
#[must_use]
pub fn live_trade_tape_block_cursor_repo() -> Arc<dyn TradeTapeBlockCursorRepository> {
    Arc::new(LiveTradeTapeBlockCursorRepo)
}

/// In-memory fact reader that serves trade-tape rows from a fixture map.
pub struct ConfigurableFactRead {
    inner: Arc<dyn QuantFactReadRepository>,
    trade_tape_by_market: HashMap<MarketId, Vec<TradeTapeRow>>,
}

impl ConfigurableFactRead {
    #[must_use]
    pub fn new(
        inner: Arc<dyn QuantFactReadRepository>,
        trade_tape_by_market: HashMap<MarketId, Vec<TradeTapeRow>>,
    ) -> Self {
        Self {
            inner,
            trade_tape_by_market,
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

    async fn book_snapshot_at(
        &self,
        token_id: &TokenId,
        source_cutoff_ms: i64,
        decision_at_ms: i64,
    ) -> Result<Option<BookSnapshotRow>, StorageError> {
        self.inner
            .book_snapshot_at(token_id, source_cutoff_ms, decision_at_ms)
            .await
    }

    async fn book_snapshots_between(
        &self,
        token_ids: Vec<TokenId>,
        from_ms: i64,
        to_ms: i64,
        available_by_ms: i64,
    ) -> Result<Vec<BookSnapshotRow>, StorageError> {
        self.inner
            .book_snapshots_between(token_ids, from_ms, to_ms, available_by_ms)
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

    async fn last_trades(
        &self,
        token_ids: Vec<TokenId>,
        from_ms: i64,
        to_ms: i64,
        limit: u64,
    ) -> Result<Vec<TickEventRow>, StorageError> {
        self.inner
            .last_trades(token_ids, from_ms, to_ms, limit)
            .await
    }

    async fn trade_tape_window_by_market(
        &self,
        market_ids: Vec<MarketId>,
        from_ms: i64,
        to_ms: i64,
        decision_at_ms: i64,
    ) -> Result<Vec<TradeTapeRow>, StorageError> {
        Ok(market_ids
            .into_iter()
            .flat_map(|market_id| {
                self.trade_tape_by_market
                    .get(&market_id)
                    .into_iter()
                    .flatten()
                    .filter(|row| {
                        row.event_time >= from_ms
                            && row.event_time < to_ms
                            && row.ingestion_time <= decision_at_ms
                    })
                    .cloned()
            })
            .collect())
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

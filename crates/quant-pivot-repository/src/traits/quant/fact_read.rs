//! Point-in-time read port over quant `ClickHouse` facts.
//!
//! The feature plane pre-fetches windowed microstructure facts once per round
//! (never a query inside the build loop). Online callers read recent facts
//! bounded by the PIT cutoff; the historical, `as_of`-bounded reads
//! (`book_checkpoint_at`, `book_checkpoints_between`, `resolution_at`,
//! `resolutions_between`) materialize backtests / training datasets.
//!
//! Every query states explicit, stable SQL ordering: point-in-time reads order
//! by event time plus the persisted `ingestion_time` / `sequence` tie-breakers,
//! so replay is deterministic.

use quant_pivot_error::storage::StorageError;
use quant_pivot_models::{
    clickhouse::{
        BookL2CheckpointRow, BookL2EventRow, BookMicrostructureRow, BookStreamSessionRow,
        CryptoPriceReportRow, DomainObservationRow, EntryConditionEvaluationEventRow,
        MarketResolutionRow, MidPriceBucketRow, TradeTapeRow, WeatherForecastPointRow,
        WeatherObservationReportRow,
    },
    types::{DomainInstrumentKey, DomainSourceId, EntryConditionInstanceId, MarketId, TokenId},
};
use uuid::Uuid;

/// Read port over persisted quant facts, used to materialize feature windows and
/// point-in-time historical state.
#[async_trait::async_trait]
pub trait QuantFactReadRepository: Send + Sync {
    /// Latest authoritative applied evaluation, explicitly deduplicated by
    /// deterministic `evaluation_id` rather than `MergeTree` background merges.
    async fn latest_applied_entry_condition_evaluation(
        &self,
        _instance_id: &EntryConditionInstanceId,
    ) -> Result<Option<EntryConditionEvaluationEventRow>, StorageError> {
        Ok(None)
    }

    /// Latest source-native Crypto report applicable at `source_timestamp_ms`
    /// and visible by `decision_at_ms`. Chainlink uses its signed
    /// `observations_timestamp`; other sources use `event_time`.
    async fn crypto_price_report_at(
        &self,
        _source_id: &DomainSourceId,
        _instrument_key: &DomainInstrumentKey,
        _source_timestamp_ms: i64,
        _decision_at_ms: i64,
    ) -> Result<Option<CryptoPriceReportRow>, StorageError> {
        Ok(None)
    }

    /// Source-native Crypto facts in `[from_ms, to_ms)`, PIT-visible by the
    /// supplied cutoffs and immediately deduplicated by immutable report identity.
    async fn crypto_price_reports_between(
        &self,
        _instrument_keys: Vec<DomainInstrumentKey>,
        _from_ms: i64,
        _to_ms: i64,
        _publish_cutoff_ms: i64,
        _decision_at_ms: i64,
    ) -> Result<Vec<CryptoPriceReportRow>, StorageError> {
        Ok(Vec::new())
    }

    /// Crypto facts first made visible in `[available_from_ms, available_to_ms)`.
    /// This availability-ordered reader is the canonical live/replay fold input;
    /// it includes late corrections whose economic event time predates the
    /// current evaluator cursor. Exact writer retries are removed explicitly;
    /// this never relies on a background `ReplacingMergeTree` merge.
    async fn crypto_price_reports_available_between(
        &self,
        _instrument_keys: Vec<DomainInstrumentKey>,
        _available_from_ms: i64,
        _available_to_ms: i64,
        _decision_at_ms: i64,
    ) -> Result<Vec<CryptoPriceReportRow>, StorageError> {
        Ok(Vec::new())
    }

    /// Weather observations, explicitly deduplicated by station, observation,
    /// revision, and report hash while retaining later COR revisions.
    async fn weather_observation_reports_between(
        &self,
        _stations: Vec<String>,
        _from_ms: i64,
        _to_ms: i64,
        _publish_cutoff_ms: i64,
        _decision_at_ms: i64,
    ) -> Result<Vec<WeatherObservationReportRow>, StorageError> {
        Ok(Vec::new())
    }

    /// GEFS points, explicitly deduplicated by member and frozen run manifest.
    async fn weather_forecast_points_between(
        &self,
        _stations: Vec<String>,
        _valid_from_ms: i64,
        _valid_to_ms: i64,
        _reference_cutoff_ms: i64,
        _decision_at_ms: i64,
    ) -> Result<Vec<WeatherForecastPointRow>, StorageError> {
        Ok(Vec::new())
    }

    /// One-second microstructure buckets for `token_ids` whose `bucket_time`
    /// falls in `[from_ms, to_ms)` (epoch milliseconds), ordered by token then
    /// time. Used both for the online feature window (`to_ms` = PIT cutoff) and
    /// for the offline forward-label window (callers filter precisely per sample).
    async fn microstructure_window(
        &self,
        token_ids: Vec<TokenId>,
        from_ms: i64,
        to_ms: i64,
        decision_at_ms: i64,
    ) -> Result<Vec<BookMicrostructureRow>, StorageError>;

    /// Microstructure buckets for `token_ids` whose `bucket_time` falls in
    /// `[from_ms, to_ms)` (epoch milliseconds), ordered by token then time.
    /// Reads the 1-minute rollup (`book_microstructure_1m`) when `minute` is
    /// set, otherwise the 1-second table — the two share the same row schema.
    /// Powers the market-detail history charts (not the PIT feature window).
    async fn microstructure_series(
        &self,
        token_ids: Vec<TokenId>,
        from_ms: i64,
        to_ms: i64,
        available_by_ms: i64,
        minute: bool,
    ) -> Result<Vec<BookMicrostructureRow>, StorageError>;

    /// Recent Market-WS trade prints for `token_ids` with `event_time` in
    /// `[from_ms, to_ms)` (epoch milliseconds), newest first, capped at `limit`.
    /// Feeds last-trade overlay markers on the price chart.
    async fn last_trades(
        &self,
        token_ids: Vec<TokenId>,
        from_ms: i64,
        to_ms: i64,
        limit: u64,
    ) -> Result<Vec<TradeTapeRow>, StorageError>;

    /// Trade-tape participant rows for `market_ids` with `event_time` in
    /// `[from_ms, to_ms)` (epoch milliseconds), ordered by market then event time.
    /// Used by structural participant-concentration features and the operator UI.
    async fn trade_tape_window_by_market(
        &self,
        market_ids: Vec<MarketId>,
        from_ms: i64,
        to_ms: i64,
        decision_at_ms: i64,
    ) -> Result<Vec<TradeTapeRow>, StorageError>;

    /// Coarse mid-price series per token for correlation estimation: the last
    /// `mid_price_close` within each `bucket_secs` interval over
    /// `[from_ms, to_ms)` (epoch milliseconds), ordered by token then bucket.
    /// Only rows available by `decision_at_ms` participate. Aggregated
    /// server-side so a multi-day lookback stays bounded. Used only by the
    /// portfolio correlation estimator (off by default).
    async fn mid_price_series(
        &self,
        token_ids: Vec<TokenId>,
        from_ms: i64,
        to_ms: i64,
        decision_at_ms: i64,
        bucket_secs: u32,
    ) -> Result<Vec<MidPriceBucketRow>, StorageError>;

    /// The freshest book snapshot whose source-effective timestamp is at or
    /// before `source_cutoff_ms` and whose ingestion timestamp is visible by
    /// `decision_at_ms`, or `None` when none exists.
    async fn book_checkpoint_at(
        &self,
        token_id: &TokenId,
        source_cutoff_ms: i64,
        decision_at_ms: i64,
    ) -> Result<Option<BookL2CheckpointRow>, StorageError>;

    /// Canonical L2 events in the checkpoint's session, including the anchor
    /// sequence, visible at the supplied PIT boundary.
    async fn book_l2_events_from(
        &self,
        _token_id: &TokenId,
        _stream_session_id: Uuid,
        _from_sequence: u64,
        _source_cutoff_ms: i64,
        _decision_at_ms: i64,
    ) -> Result<Vec<BookL2EventRow>, StorageError> {
        Ok(Vec::new())
    }

    /// Market-WS trade observations interleaved in the same per-token stream.
    async fn market_ws_trades_from(
        &self,
        _token_id: &TokenId,
        _stream_session_id: Uuid,
        _from_sequence: u64,
        _source_cutoff_ms: i64,
        _decision_at_ms: i64,
    ) -> Result<Vec<TradeTapeRow>, StorageError> {
        Ok(Vec::new())
    }

    /// Latest append-only ledger state visible at the PIT boundary.
    async fn book_stream_session_at(
        &self,
        _stream_session_id: Uuid,
        _decision_at_ms: i64,
    ) -> Result<Option<BookStreamSessionRow>, StorageError> {
        Ok(None)
    }

    /// Freshest visible book per token at one source cutoff. Implementations
    /// should override with one grouped query; the default preserves correctness
    /// for test adapters.
    async fn book_checkpoints_at(
        &self,
        token_ids: Vec<TokenId>,
        source_cutoff_ms: i64,
        decision_at_ms: i64,
    ) -> Result<Vec<BookL2CheckpointRow>, StorageError> {
        let mut rows = Vec::with_capacity(token_ids.len());
        for token_id in token_ids {
            if let Some(row) = self
                .book_checkpoint_at(&token_id, source_cutoff_ms, decision_at_ms)
                .await?
            {
                rows.push(row);
            }
        }
        Ok(rows)
    }

    /// All book snapshots for `token_ids` with `event_time` in the inclusive
    /// range `[from_ms, to_ms]`, ordered by token then event time (with
    /// tie-breakers). A batch prefetch for offline dataset materialization; the
    /// caller resolves the per-sample point-in-time snapshot in memory.
    async fn book_checkpoints_between(
        &self,
        token_ids: Vec<TokenId>,
        from_ms: i64,
        to_ms: i64,
        available_by_ms: i64,
    ) -> Result<Vec<BookL2CheckpointRow>, StorageError>;

    /// The latest resolution whose economic timestamp is at or before
    /// `source_cutoff_ms` and whose writer observation is visible by
    /// `decision_at_ms`, or `None` when no such row exists. Stable
    /// `resolved_at DESC, observed_at DESC, sequence DESC` tie-break.
    async fn resolution_at(
        &self,
        market_id: &MarketId,
        source_cutoff_ms: i64,
        decision_at_ms: i64,
    ) -> Result<Option<MarketResolutionRow>, StorageError>;

    /// All settlement events for `market_ids` with `resolved_at` in the inclusive
    /// range `[from_ms, to_ms]` and observed by `decision_at_ms`, ordered by
    /// market then resolution time (with tie-breakers). A batch prefetch for
    /// offline settlement labeling.
    async fn resolutions_between(
        &self,
        market_ids: Vec<MarketId>,
        from_ms: i64,
        to_ms: i64,
        decision_at_ms: i64,
    ) -> Result<Vec<MarketResolutionRow>, StorageError>;

    /// Distinct market ids that had at least one book snapshot with `event_time`
    /// in the inclusive range `[from_ms, to_ms]` and `ingestion_time` no later
    /// than `decision_at_ms`.
    ///
    /// This is the **point-in-time honest** historical candidate set for
    /// offline dataset builds: a market is a candidate iff it was actually
    /// observable (had a book) during the window — independent of its *current*
    /// catalog status. It therefore includes since-`Settled` / `Delisted`
    /// markets (carrying mature settlement labels), eliminating the survivorship
    /// bias of a currently-active-only catalog scan.
    async fn observed_markets_between(
        &self,
        from_ms: i64,
        to_ms: i64,
        decision_at_ms: i64,
    ) -> Result<Vec<MarketId>, StorageError>;

    /// External domain observations for `instrument_keys` with `event_time` in
    /// `[from_ms, to_ms)` (epoch milliseconds), ordered by instrument, metric,
    /// event time, then `ingestion_time` (stable replay order). Feeds the
    /// online domain feature window and the offline materialized domain PIT
    /// engine (Phase 11.2.2).
    async fn domain_observations_between(
        &self,
        instrument_keys: Vec<DomainInstrumentKey>,
        from_ms: i64,
        to_ms: i64,
        publish_cutoff_ms: i64,
        decision_at_ms: i64,
    ) -> Result<Vec<DomainObservationRow>, StorageError>;

    /// The freshest domain observation per `(instrument, metric)` at or before
    /// `as_of_ms`, or `None` when none exists. Point-in-time correct with the
    /// stable `event_time DESC, ingestion_time DESC` tie-break. Powers ingest
    /// health probes and the domain-availability projector.
    async fn domain_observation_at(
        &self,
        instrument_key: &DomainInstrumentKey,
        metric: &str,
        source_cutoff_ms: i64,
        decision_at_ms: i64,
    ) -> Result<Option<DomainObservationRow>, StorageError>;
}

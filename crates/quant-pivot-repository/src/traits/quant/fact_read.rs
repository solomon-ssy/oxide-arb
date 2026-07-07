//! Point-in-time read port over quant `ClickHouse` facts.
//!
//! The feature plane pre-fetches windowed microstructure facts once per round
//! (never a query inside the build loop). Online callers read recent facts
//! bounded by the PIT cutoff; the historical, `as_of`-bounded reads
//! (`book_snapshot_at`, `book_snapshots_between`, `resolution_at`,
//! `resolutions_between`) materialize backtests / training datasets.
//!
//! Every query states explicit, stable SQL ordering: point-in-time reads order
//! by event time plus the persisted `ingestion_time` / `sequence` tie-breakers,
//! so replay is deterministic.

use quant_pivot_error::storage::StorageError;
use quant_pivot_models::{
    clickhouse::{
        BookMicrostructureRow, BookSnapshotRow, DomainObservationRow, MarketResolutionRow,
        MidPriceBucketRow, TickEventRow, TradeTapeRow,
    },
    types::{DomainInstrumentKey, MarketId, TokenId},
};

/// Read port over persisted quant facts, used to materialize feature windows and
/// point-in-time historical state.
#[async_trait::async_trait]
pub trait QuantFactReadRepository: Send + Sync {
    /// One-second microstructure buckets for `token_ids` whose `bucket_time`
    /// falls in `[from_ms, to_ms)` (epoch milliseconds), ordered by token then
    /// time. Used both for the online feature window (`to_ms` = PIT cutoff) and
    /// for the offline forward-label window (callers filter precisely per sample).
    async fn microstructure_window(
        &self,
        token_ids: Vec<TokenId>,
        from_ms: i64,
        to_ms: i64,
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
        minute: bool,
    ) -> Result<Vec<BookMicrostructureRow>, StorageError>;

    /// Recent `LastTrade` tick events for `token_ids` with `event_time` in
    /// `[from_ms, to_ms)` (epoch milliseconds), newest first, capped at `limit`.
    /// Feeds last-trade overlay markers on the price chart.
    async fn last_trades(
        &self,
        token_ids: Vec<TokenId>,
        from_ms: i64,
        to_ms: i64,
        limit: u64,
    ) -> Result<Vec<TickEventRow>, StorageError>;

    /// Trade-tape participant rows for `market_ids` with `event_time` in
    /// `[from_ms, to_ms)` (epoch milliseconds), ordered by market then event time.
    /// Used by structural participant-concentration features and the operator UI.
    async fn trade_tape_window_by_market(
        &self,
        market_ids: Vec<MarketId>,
        from_ms: i64,
        to_ms: i64,
    ) -> Result<Vec<TradeTapeRow>, StorageError>;

    /// Coarse mid-price series per token for correlation estimation: the last
    /// `mid_price_close` within each `bucket_secs` interval over
    /// `[from_ms, to_ms)` (epoch milliseconds), ordered by token then bucket.
    /// Aggregated server-side so a multi-day lookback stays bounded. Used only by
    /// the portfolio correlation estimator (off by default).
    async fn mid_price_series(
        &self,
        token_ids: Vec<TokenId>,
        from_ms: i64,
        to_ms: i64,
        bucket_secs: u32,
    ) -> Result<Vec<MidPriceBucketRow>, StorageError>;

    /// The freshest book snapshot for `token_id` published at or before
    /// `as_of_ms` (epoch milliseconds), or `None` when none exists. Point-in-time
    /// correct: `WHERE event_time <= as_of` with a stable
    /// `event_time DESC, ingestion_time DESC, sequence DESC` tie-break.
    async fn book_snapshot_at(
        &self,
        token_id: &TokenId,
        as_of_ms: i64,
    ) -> Result<Option<BookSnapshotRow>, StorageError>;

    /// All book snapshots for `token_ids` with `event_time` in the inclusive
    /// range `[from_ms, to_ms]`, ordered by token then event time (with
    /// tie-breakers). A batch prefetch for offline dataset materialization; the
    /// caller resolves the per-sample point-in-time snapshot in memory.
    async fn book_snapshots_between(
        &self,
        token_ids: Vec<TokenId>,
        from_ms: i64,
        to_ms: i64,
    ) -> Result<Vec<BookSnapshotRow>, StorageError>;

    /// The resolution in effect for `market_id` as of `as_of_ms` (the latest
    /// settlement event with `resolved_at <= as_of`), or `None` when the market
    /// had not resolved by then. Stable `resolved_at DESC, observed_at DESC,
    /// sequence DESC` tie-break.
    async fn resolution_at(
        &self,
        market_id: &MarketId,
        as_of_ms: i64,
    ) -> Result<Option<MarketResolutionRow>, StorageError>;

    /// All settlement events for `market_ids` with `resolved_at` in the inclusive
    /// range `[from_ms, to_ms]`, ordered by market then resolution time (with
    /// tie-breakers). A batch prefetch for offline settlement labeling.
    async fn resolutions_between(
        &self,
        market_ids: Vec<MarketId>,
        from_ms: i64,
        to_ms: i64,
    ) -> Result<Vec<MarketResolutionRow>, StorageError>;

    /// Distinct market ids that had at least one book snapshot with `event_time`
    /// in the inclusive range `[from_ms, to_ms]`.
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
    ) -> Result<Vec<DomainObservationRow>, StorageError>;

    /// The freshest domain observation per `(instrument, metric)` at or before
    /// `as_of_ms`, or `None` when none exists. Point-in-time correct with the
    /// stable `event_time DESC, ingestion_time DESC` tie-break. Powers ingest
    /// health probes and the domain-availability projector.
    async fn domain_observation_at(
        &self,
        instrument_key: &DomainInstrumentKey,
        metric: &str,
        as_of_ms: i64,
    ) -> Result<Option<DomainObservationRow>, StorageError>;
}

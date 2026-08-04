//! `ClickHouse`-backed read repository for quant facts (feature window inputs +
//! historical point-in-time state).

use std::{collections::HashMap, sync::Arc};

use async_trait::async_trait;
use quant_pivot_error::storage::{StorageError, entity::MARKET_RESOLUTION_EVENT};
use quant_pivot_models::{
    clickhouse::{
        BookL2LedgerRow, BookLedgerReplayAnchor, BookMicrostructureRow, BookStreamSessionRow,
        CryptoPriceReportRow, DomainObservationRow, EntryConditionEvaluationEventRow,
        MarketResolutionRow, MidPriceBucketRow, ReportMarketFunnelCountRow, ReportMarketFunnelRow,
        TradeTapeRow, WeatherForecastFactRow, WeatherObservationFactRow,
    },
    enums::clickhouse::{ChTradeReconciliationStatus, ChTradeTapeSource},
    types::{
        ContentHash, DomainInstrumentKey, DomainSourceId, EntryConditionInstanceId, MarketId,
        RecommendationReportId, ResearchProfileRef, TokenId,
    },
};
use quant_pivot_storage::clickhouse::ClickHousePool;
use uuid::Uuid;

use crate::{
    clickhouse::{
        query_batch::{UUID_INLINE_BYTES, canonical_values, extend_rows, query_chunks},
        query_limits::{
            BOOK_LEDGER_BETWEEN, BOOK_LEDGER_FROM, BOOK_LEDGER_REPLAY_FROM,
            BOOK_LEDGER_SNAPSHOT_AT, BOOK_LEDGER_SNAPSHOTS_AT, BOOK_LEDGER_SNAPSHOTS_BETWEEN,
            BOOK_STREAM_SESSION_AT, BOOK_STREAM_SESSIONS, CRYPTO_REPORT_AT,
            CRYPTO_REPORTS_AVAILABLE, CRYPTO_REPORTS_BETWEEN, DOMAIN_OBSERVATION_AT,
            DOMAIN_OBSERVATIONS_BETWEEN, ENTRY_EVALUATION_LATEST, LAST_TRADES,
            MICROSTRUCTURE_SERIES, MICROSTRUCTURE_WINDOW, MID_PRICE_SERIES,
            OBSERVED_MARKETS_BETWEEN, REPORT_FUNNEL_BETWEEN, REPORT_FUNNEL_COUNT,
            REPORT_FUNNEL_COUNTS, REPORT_FUNNEL_PAGE, RESOLUTION_AT, RESOLUTION_BY_CHECKPOINT,
            RESOLUTION_BY_MARKET, RESOLUTIONS_BETWEEN, TRADE_TAPE_WINDOW,
            WEATHER_FORECASTS_BETWEEN, WEATHER_OBSERVATIONS_BETWEEN,
        },
    },
    traits::QuantFactReadRepository,
};

/// Quant fact source, queried straight from `ClickHouse`.
pub struct ChQuantFactReadRepository {
    pool: Arc<ClickHousePool>,
}

impl ChQuantFactReadRepository {
    /// Build a read repository over a `ClickHouse` pool.
    #[must_use]
    pub const fn new(pool: Arc<ClickHousePool>) -> Self {
        Self { pool }
    }
}

fn validate_market_resolution(row: &MarketResolutionRow) -> Result<(), StorageError> {
    row.validate()
        .map_err(|error| StorageError::InvariantViolation {
            entity: Some(MARKET_RESOLUTION_EVENT),
            detail: error.to_string(),
        })
}

#[async_trait]
impl QuantFactReadRepository for ChQuantFactReadRepository {
    async fn report_market_funnel_counts(
        &self,
        report_id: &RecommendationReportId,
    ) -> Result<Vec<ReportMarketFunnelCountRow>, StorageError> {
        REPORT_FUNNEL_COUNTS
            .query(
                self.pool.client(),
                "SELECT terminal_stage, count() AS row_count \
                 FROM quant_report_market_funnel FINAL \
                 WHERE recommendation_report_id = ? \
                 GROUP BY terminal_stage ORDER BY terminal_stage",
            )
            .bind(*report_id)
            .fetch_all::<ReportMarketFunnelCountRow>()
            .await
            .map_err(Into::into)
    }

    async fn report_market_funnel_count(
        &self,
        report_id: &RecommendationReportId,
        terminal_stage: Option<&str>,
        primary_reason: Option<&str>,
    ) -> Result<u64, StorageError> {
        let mut sql = "SELECT count() AS row_count FROM quant_report_market_funnel FINAL \
                       WHERE recommendation_report_id = ?"
            .to_owned();
        if terminal_stage.is_some() {
            sql.push_str(" AND terminal_stage = ?");
        }
        if primary_reason.is_some() {
            sql.push_str(" AND primary_reason = ?");
        }
        let mut query = REPORT_FUNNEL_COUNT
            .query(self.pool.client(), &sql)
            .bind(*report_id);
        if let Some(stage) = terminal_stage {
            query = query.bind(stage);
        }
        if let Some(reason) = primary_reason {
            query = query.bind(reason);
        }
        query
            .fetch_one::<FunnelTotalRow>()
            .await
            .map(|row| row.row_count)
            .map_err(Into::into)
    }

    async fn report_market_funnel_page(
        &self,
        report_id: &RecommendationReportId,
        terminal_stage: Option<&str>,
        primary_reason: Option<&str>,
        offset: u64,
        limit: u64,
    ) -> Result<Vec<ReportMarketFunnelRow>, StorageError> {
        let mut sql = "SELECT ?fields FROM quant_report_market_funnel FINAL \
                       WHERE recommendation_report_id = ?"
            .to_owned();
        if terminal_stage.is_some() {
            sql.push_str(" AND terminal_stage = ?");
        }
        if primary_reason.is_some() {
            sql.push_str(" AND primary_reason = ?");
        }
        sql.push_str(" ORDER BY market_id LIMIT ? OFFSET ?");
        let mut query = REPORT_FUNNEL_PAGE
            .query(self.pool.client(), &sql)
            .bind(*report_id);
        if let Some(stage) = terminal_stage {
            query = query.bind(stage);
        }
        if let Some(reason) = primary_reason {
            query = query.bind(reason);
        }
        query
            .bind(limit)
            .bind(offset)
            .fetch_all::<ReportMarketFunnelRow>()
            .await
            .map_err(Into::into)
    }

    async fn report_funnel_between(
        &self,
        profile_ref: &ResearchProfileRef,
        from_ms: i64,
        to_ms: i64,
    ) -> Result<Vec<ReportMarketFunnelRow>, StorageError> {
        REPORT_FUNNEL_BETWEEN
            .query(
                self.pool.client(),
                "SELECT ?fields FROM quant_report_market_funnel FINAL \
                 WHERE profile_id = ? AND profile_version = ? \
                   AND profile_content_hash = ? \
                   AND event_time >= fromUnixTimestamp64Milli(?) \
                   AND event_time < fromUnixTimestamp64Milli(?) \
                 ORDER BY recommendation_report_id, market_id",
            )
            .bind(profile_ref.id.as_str())
            .bind(profile_ref.version)
            .bind(profile_ref.content_hash.to_string())
            .bind(from_ms)
            .bind(to_ms)
            .fetch_all::<ReportMarketFunnelRow>()
            .await
            .map_err(Into::into)
    }

    async fn latest_entry_evaluation(
        &self,
        instance_id: &EntryConditionInstanceId,
    ) -> Result<Option<EntryConditionEvaluationEventRow>, StorageError> {
        ENTRY_EVALUATION_LATEST
            .query(
                self.pool.client(),
                "SELECT ?fields FROM (\
                     SELECT *, row_number() OVER (\
                         PARTITION BY evaluation_id \
                         ORDER BY evaluated_at DESC\
                     ) AS dedupe_rank \
                     FROM quant_entry_condition_evaluation_event FINAL \
                     WHERE condition_instance_id = ? AND trace_kind = 'applied'\
                 ) WHERE dedupe_rank = 1 \
                 ORDER BY applied_revision DESC, evaluated_at DESC, evaluation_id DESC \
                 LIMIT 1",
            )
            .bind(instance_id.as_uuid())
            .fetch_optional::<EntryConditionEvaluationEventRow>()
            .await
            .map_err(StorageError::from)
    }

    async fn crypto_price_report_at(
        &self,
        source_id: &DomainSourceId,
        instrument_key: &DomainInstrumentKey,
        source_timestamp_ms: i64,
        decision_at_ms: i64,
    ) -> Result<Option<CryptoPriceReportRow>, StorageError> {
        let row = CRYPTO_REPORT_AT
            .query(
                self.pool.client(),
                "SELECT ?fields FROM (\
                     SELECT *, row_number() OVER (\
                         PARTITION BY source_id, instrument_key, source_sequence, event_time, report_hash \
                         ORDER BY available_at DESC\
                     ) AS dedupe_rank \
                     FROM quant_crypto_price_report \
                     WHERE source_id = ? \
                     AND instrument_key = ? \
                     AND ifNull(observations_timestamp, event_time) <= fromUnixTimestamp64Milli(?) \
                     AND available_at <= fromUnixTimestamp64Milli(?)\
                 ) WHERE dedupe_rank = 1 \
                 ORDER BY ifNull(observations_timestamp, event_time) DESC, \
                 available_at DESC, source_sequence DESC, report_hash DESC \
                 LIMIT 1",
            )
            .bind(source_id.clone())
            .bind(instrument_key.clone())
            .bind(source_timestamp_ms)
            .bind(decision_at_ms)
            .fetch_optional::<CryptoPriceReportRow>()
            .await?;
        Ok(row)
    }

    async fn crypto_price_reports_between(
        &self,
        instrument_keys: Vec<DomainInstrumentKey>,
        from_ms: i64,
        to_ms: i64,
        publish_cutoff_ms: i64,
        decision_at_ms: i64,
    ) -> Result<Vec<CryptoPriceReportRow>, StorageError> {
        let instrument_keys = canonical_values(instrument_keys);
        if instrument_keys.is_empty() {
            return Ok(Vec::new());
        }
        let mut rows = Vec::new();
        for keys in query_chunks(
            &instrument_keys,
            |key| key.as_str().len(),
            "quant_crypto_price_report",
        )? {
            let page = CRYPTO_REPORTS_BETWEEN
                .query(
                    self.pool.client(),
                    "SELECT ?fields FROM (\
                         SELECT *, row_number() OVER (\
                             PARTITION BY source_id, instrument_key, source_sequence, event_time, report_hash \
                             ORDER BY available_at DESC\
                         ) AS dedupe_rank \
                         FROM quant_crypto_price_report \
                         WHERE instrument_key IN ? \
                         AND event_time >= fromUnixTimestamp64Milli(?) \
                         AND event_time < fromUnixTimestamp64Milli(?) \
                         AND published_at <= fromUnixTimestamp64Milli(?) \
                         AND available_at <= fromUnixTimestamp64Milli(?)\
                     ) WHERE dedupe_rank = 1 \
                     ORDER BY instrument_key, event_time, available_at, source_sequence, report_hash",
                )
                .bind(keys.to_vec())
                .bind(from_ms)
                .bind(to_ms)
                .bind(publish_cutoff_ms)
                .bind(decision_at_ms)
                .fetch_all::<CryptoPriceReportRow>()
                .await?;
            extend_rows(
                &mut rows,
                page,
                CRYPTO_REPORTS_BETWEEN,
                "quant_crypto_price_report",
            )?;
        }
        Ok(rows)
    }

    async fn crypto_reports_between(
        &self,
        instrument_keys: Vec<DomainInstrumentKey>,
        available_from_ms: i64,
        available_to_ms: i64,
        decision_at_ms: i64,
    ) -> Result<Vec<CryptoPriceReportRow>, StorageError> {
        let instrument_keys = canonical_values(instrument_keys);
        if instrument_keys.is_empty() {
            return Ok(Vec::new());
        }
        let mut rows = Vec::new();
        for keys in query_chunks(
            &instrument_keys,
            |key| key.as_str().len(),
            "quant_crypto_price_report",
        )? {
            let page = CRYPTO_REPORTS_AVAILABLE
                .query(
                    self.pool.client(),
                    "SELECT ?fields FROM (\
                         SELECT *, row_number() OVER (\
                             PARTITION BY source_id, instrument_key, source_sequence, event_time, report_hash \
                             ORDER BY available_at DESC\
                         ) AS dedupe_rank \
                         FROM quant_crypto_price_report \
                         WHERE instrument_key IN ? \
                         AND available_at >= fromUnixTimestamp64Milli(?) \
                         AND available_at < fromUnixTimestamp64Milli(?) \
                         AND available_at <= fromUnixTimestamp64Milli(?)\
                     ) WHERE dedupe_rank = 1 \
                     ORDER BY instrument_key, available_at, event_time, source_sequence, report_hash",
                )
                .bind(keys.to_vec())
                .bind(available_from_ms)
                .bind(available_to_ms)
                .bind(decision_at_ms)
                .fetch_all::<CryptoPriceReportRow>()
                .await?;
            extend_rows(
                &mut rows,
                page,
                CRYPTO_REPORTS_AVAILABLE,
                "quant_crypto_price_report",
            )?;
        }
        Ok(rows)
    }

    async fn weather_observation_facts_between(
        &self,
        stations: Vec<String>,
        from_ms: i64,
        to_ms: i64,
        publish_cutoff_ms: i64,
        decision_at_ms: i64,
    ) -> Result<Vec<WeatherObservationFactRow>, StorageError> {
        let stations = canonical_values(stations);
        if stations.is_empty() {
            return Ok(Vec::new());
        }
        let mut rows = Vec::new();
        for subjects in query_chunks(&stations, String::len, "quant_weather_observation_fact")? {
            let page = WEATHER_OBSERVATIONS_BETWEEN
                .query(
                    self.pool.client(),
                    "SELECT ?fields FROM (\
                         SELECT *, row_number() OVER (\
                             PARTITION BY instrument_key, variable, observed_at, revision, report_hash \
                             ORDER BY available_at DESC\
                         ) AS dedupe_rank \
                         FROM quant_weather_observation_fact \
                         WHERE subject_key IN ? \
                         AND observed_at >= ? \
                         AND observed_at <= ? \
                         AND observed_at <= ? \
                         AND (valid_from IS NULL OR valid_from <= ?) \
                         AND published_at <= fromUnixTimestamp64Milli(?) \
                         AND available_at <= fromUnixTimestamp64Milli(?)\
                     ) WHERE dedupe_rank = 1 \
                     ORDER BY subject_key, variable, local_date, observed_at, revision, available_at, report_hash",
                )
                .bind(subjects.to_vec())
                .bind(from_ms)
                .bind(to_ms)
                .bind(decision_at_ms)
                .bind(decision_at_ms)
                .bind(publish_cutoff_ms)
                .bind(decision_at_ms)
                .fetch_all::<WeatherObservationFactRow>()
                .await?;
            extend_rows(
                &mut rows,
                page,
                WEATHER_OBSERVATIONS_BETWEEN,
                "quant_weather_observation_fact",
            )?;
        }
        Ok(rows)
    }

    async fn weather_forecast_facts_between(
        &self,
        stations: Vec<String>,
        valid_from_ms: i64,
        valid_to_ms: i64,
        reference_cutoff_ms: i64,
        decision_at_ms: i64,
    ) -> Result<Vec<WeatherForecastFactRow>, StorageError> {
        let stations = canonical_values(stations);
        if stations.is_empty() {
            return Ok(Vec::new());
        }
        let mut rows = Vec::new();
        for subjects in query_chunks(&stations, String::len, "quant_weather_forecast_fact")? {
            let page = WEATHER_FORECASTS_BETWEEN
                .query(
                    self.pool.client(),
                    "SELECT ?fields FROM (\
                         SELECT *, row_number() OVER (\
                             PARTITION BY instrument_key, variable, reference_time, valid_time, member, revision, report_hash \
                             ORDER BY available_at DESC\
                         ) AS dedupe_rank \
                         FROM quant_weather_forecast_fact \
                         WHERE subject_key IN ? \
                         AND valid_time >= fromUnixTimestamp64Milli(?) \
                         AND valid_time < fromUnixTimestamp64Milli(?) \
                         AND reference_time <= fromUnixTimestamp64Milli(?) \
                         AND available_at <= fromUnixTimestamp64Milli(?)\
                     ) WHERE dedupe_rank = 1 \
                     ORDER BY subject_key, variable, reference_time, valid_time, member, revision, available_at, report_hash",
                )
                .bind(subjects.to_vec())
                .bind(valid_from_ms)
                .bind(valid_to_ms)
                .bind(reference_cutoff_ms)
                .bind(decision_at_ms)
                .fetch_all::<WeatherForecastFactRow>()
                .await?;
            extend_rows(
                &mut rows,
                page,
                WEATHER_FORECASTS_BETWEEN,
                "quant_weather_forecast_fact",
            )?;
        }
        Ok(rows)
    }

    async fn microstructure_window(
        &self,
        token_ids: Vec<TokenId>,
        from_ms: i64,
        to_ms: i64,
        decision_at_ms: i64,
    ) -> Result<Vec<BookMicrostructureRow>, StorageError> {
        let token_ids = canonical_values(token_ids);
        if token_ids.is_empty() {
            return Ok(Vec::new());
        }
        let mut rows = Vec::new();
        for tokens in query_chunks(
            &token_ids,
            |token| token.as_str().len(),
            "book_microstructure_1s",
        )? {
            let page = MICROSTRUCTURE_WINDOW
                .query(
                    self.pool.client(),
                    "SELECT ?fields FROM book_microstructure_1s \
                     WHERE token_id IN ? \
                     AND bucket_time >= fromUnixTimestamp64Milli(?) \
                     AND bucket_time < fromUnixTimestamp64Milli(?) \
                     AND available_at <= fromUnixTimestamp64Milli(?) \
                     ORDER BY token_id, bucket_time",
                )
                .bind(tokens.to_vec())
                .bind(from_ms)
                .bind(to_ms)
                .bind(decision_at_ms)
                .fetch_all::<BookMicrostructureRow>()
                .await?;
            extend_rows(
                &mut rows,
                page,
                MICROSTRUCTURE_WINDOW,
                "book_microstructure_1s",
            )?;
        }
        Ok(rows)
    }

    async fn microstructure_series(
        &self,
        token_ids: Vec<TokenId>,
        from_ms: i64,
        to_ms: i64,
        available_by_ms: i64,
        minute: bool,
    ) -> Result<Vec<BookMicrostructureRow>, StorageError> {
        let token_ids = canonical_values(token_ids);
        if token_ids.is_empty() {
            return Ok(Vec::new());
        }
        // The 1s and 1m tables share an identical column schema, so only the
        // relation name differs — never interpolate untrusted input here.
        let sql = if minute {
            "SELECT ?fields FROM book_microstructure_1m \
             WHERE token_id IN ? \
             AND bucket_time >= fromUnixTimestamp64Milli(?) \
             AND bucket_time < fromUnixTimestamp64Milli(?) \
             AND available_at <= fromUnixTimestamp64Milli(?) \
             ORDER BY token_id, bucket_time"
        } else {
            "SELECT ?fields FROM book_microstructure_1s \
             WHERE token_id IN ? \
             AND bucket_time >= fromUnixTimestamp64Milli(?) \
             AND bucket_time < fromUnixTimestamp64Milli(?) \
             AND available_at <= fromUnixTimestamp64Milli(?) \
             ORDER BY token_id, bucket_time"
        };
        let mut rows = Vec::new();
        for tokens in query_chunks(
            &token_ids,
            |token| token.as_str().len(),
            "book_microstructure",
        )? {
            let page = MICROSTRUCTURE_SERIES
                .query(self.pool.client(), sql)
                .bind(tokens.to_vec())
                .bind(from_ms)
                .bind(to_ms)
                .bind(available_by_ms)
                .fetch_all::<BookMicrostructureRow>()
                .await?;
            extend_rows(
                &mut rows,
                page,
                MICROSTRUCTURE_SERIES,
                "book_microstructure",
            )?;
        }
        Ok(rows)
    }

    async fn last_trades(
        &self,
        token_ids: Vec<TokenId>,
        from_ms: i64,
        to_ms: i64,
        limit: u64,
    ) -> Result<Vec<TradeTapeRow>, StorageError> {
        let token_ids = canonical_values(token_ids);
        if token_ids.is_empty() {
            return Ok(Vec::new());
        }
        let mut rows = Vec::new();
        for tokens in query_chunks(&token_ids, |token| token.as_str().len(), "quant_trade_tape")? {
            let page = LAST_TRADES
                .query(
                    self.pool.client(),
                    "SELECT ?fields FROM quant_trade_tape \
                     WHERE token_id IN ? \
                     AND source = ? \
                     AND event_time >= fromUnixTimestamp64Milli(?) \
                     AND event_time < fromUnixTimestamp64Milli(?) \
                     ORDER BY event_time DESC, revision DESC, ingestion_time DESC, token_sequence DESC, \
                     market_id, token_id, participant_role, source_event_id, participant_address \
                     LIMIT 1 BY market_id, token_id, participant_role, event_time, source_event_id, participant_address \
                     LIMIT ?",
                )
                .bind(tokens.to_vec())
                .bind(ChTradeTapeSource::MarketWs)
                .bind(from_ms)
                .bind(to_ms)
                .bind(limit)
                .fetch_all::<TradeTapeRow>()
                .await?;
            extend_rows(&mut rows, page, LAST_TRADES, "quant_trade_tape")?;
        }
        rows.sort_by(|left, right| {
            right
                .event_time
                .cmp(&left.event_time)
                .then_with(|| right.revision.cmp(&left.revision))
                .then_with(|| right.ingestion_time.cmp(&left.ingestion_time))
                .then_with(|| right.token_sequence.cmp(&left.token_sequence))
                .then_with(|| left.market_id.cmp(&right.market_id))
                .then_with(|| left.token_id.cmp(&right.token_id))
                .then_with(|| (left.participant_role as i8).cmp(&(right.participant_role as i8)))
                .then_with(|| left.source_event_id.cmp(&right.source_event_id))
                .then_with(|| left.participant_address.cmp(&right.participant_address))
        });
        let limit = usize::try_from(limit).map_err(|error| {
            StorageError::invariant_violation(
                Some("quant_trade_tape"),
                format!("last-trade limit is not representable: {error}"),
            )
        })?;
        rows.truncate(limit);
        Ok(rows)
    }

    async fn market_tape_window(
        &self,
        market_ids: Vec<MarketId>,
        from_ms: i64,
        to_ms: i64,
        decision_at_ms: i64,
    ) -> Result<Vec<TradeTapeRow>, StorageError> {
        let market_ids = canonical_values(market_ids);
        if market_ids.is_empty() {
            return Ok(Vec::new());
        }
        let mut rows = Vec::new();
        for markets in query_chunks(
            &market_ids,
            |market| market.as_str().len(),
            "quant_trade_tape",
        )? {
            let page = TRADE_TAPE_WINDOW
                .query(
                    self.pool.client(),
                    "SELECT ?fields FROM quant_trade_tape \
                     WHERE market_id IN ? \
                     AND source = ? \
                     AND reconciliation_status = ? \
                     AND event_time >= fromUnixTimestamp64Milli(?) \
                     AND event_time < fromUnixTimestamp64Milli(?) \
                     AND ingestion_time <= fromUnixTimestamp64Milli(?) \
                     ORDER BY ingestion_time DESC, revision DESC, \
                     cityHash64(tuple(side, price, size_shares, notional_usd, \
                         ifNull(tx_hash, ''), source, observed_field_flags, \
                         reconciliation_status, ifNull(matched_source_event_id, ''), \
                         ifNull(raw_payload_json, ''), schema_version)) DESC \
                     LIMIT 1 BY market_id, token_id, participant_role, event_time, source_event_id, participant_address",
                )
                .bind(markets.to_vec())
                .bind(ChTradeTapeSource::OnChainOrderFilled)
                .bind(ChTradeReconciliationStatus::Matched)
                .bind(from_ms)
                .bind(to_ms)
                .bind(decision_at_ms)
                .fetch_all::<TradeTapeRow>()
                .await?;
            extend_rows(&mut rows, page, TRADE_TAPE_WINDOW, "quant_trade_tape")?;
        }
        rows.sort_by(|left, right| {
            (
                left.market_id.as_str(),
                left.event_time,
                left.ingestion_time,
                left.source_event_id.as_str(),
                left.participant_role as i8,
            )
                .cmp(&(
                    right.market_id.as_str(),
                    right.event_time,
                    right.ingestion_time,
                    right.source_event_id.as_str(),
                    right.participant_role as i8,
                ))
        });
        Ok(rows)
    }

    async fn mid_price_series(
        &self,
        token_ids: Vec<TokenId>,
        from_ms: i64,
        to_ms: i64,
        decision_at_ms: i64,
        bucket_secs: u32,
    ) -> Result<Vec<MidPriceBucketRow>, StorageError> {
        let token_ids = canonical_values(token_ids);
        if token_ids.is_empty() {
            return Ok(Vec::new());
        }
        if bucket_secs == 0 {
            return Err(StorageError::invariant_violation(
                Some("book_microstructure_1s"),
                "mid-price bucket_secs must be greater than zero",
            ));
        }
        let mut rows = Vec::new();
        for tokens in query_chunks(
            &token_ids,
            |token| token.as_str().len(),
            "book_microstructure_1s",
        )? {
            let page = MID_PRICE_SERIES
                .query(
                    self.pool.client(),
                    "SELECT token_id, \
                     intDiv(toUnixTimestamp64Milli(bucket_time), toInt64(?) * 1000) \
                     * toInt64(?) * 1000 \
                     AS bucket_ms, \
                     argMax(mid_price_close, tuple( \
                         bucket_time, available_at, \
                         cityHash64(toString(tuple(best_bid_open, best_bid_high, best_bid_low, best_bid_close, \
                             best_ask_open, best_ask_high, best_ask_low, best_ask_close, \
                             spread_bps_min, spread_bps_avg, spread_bps_max, mid_price_open, \
                             mid_price_close, top1_depth_usd_avg, top5_depth_usd_avg, \
                             top20_depth_usd_avg, imbalance_avg, update_count, snapshot_count, \
                             delta_count, delete_count, crossed_count, invalid_level_count, \
                             gap_count, last_trade_count, max_book_age_ms, schema_version))) \
                     )) AS mid_price \
                     FROM book_microstructure_1s \
                     WHERE token_id IN ? \
                     AND bucket_time >= fromUnixTimestamp64Milli(?) \
                     AND bucket_time < fromUnixTimestamp64Milli(?) \
                     AND available_at <= fromUnixTimestamp64Milli(?) \
                     GROUP BY token_id, bucket_ms \
                     ORDER BY token_id, bucket_ms",
                )
                .bind(bucket_secs)
                .bind(bucket_secs)
                .bind(tokens.to_vec())
                .bind(from_ms)
                .bind(to_ms)
                .bind(decision_at_ms)
                .fetch_all::<MidPriceBucketRow>()
                .await?;
            extend_rows(&mut rows, page, MID_PRICE_SERIES, "book_microstructure_1s")?;
        }
        Ok(rows)
    }

    async fn book_ledger_snapshot_at(
        &self,
        token_id: &TokenId,
        source_cutoff_ms: i64,
        decision_at_ms: i64,
    ) -> Result<Option<BookL2LedgerRow>, StorageError> {
        let rows = BOOK_LEDGER_SNAPSHOT_AT
            .query(
                self.pool.client(),
                "SELECT ?fields FROM quant_book_l2_ledger \
                 WHERE token_id = ? AND event_type = 'Snapshot' \
                 AND venue_event_time <= fromUnixTimestamp64Milli(?) \
                 AND persisted_time <= fromUnixTimestamp64Milli(?) \
                 ORDER BY venue_event_time DESC, persisted_time DESC, token_sequence DESC \
                 LIMIT 1",
            )
            .bind(token_id.clone())
            .bind(source_cutoff_ms)
            .bind(decision_at_ms)
            .fetch_all::<BookL2LedgerRow>()
            .await?;
        let row = rows.into_iter().next();
        Ok(row)
    }

    async fn book_l2_ledger_from(
        &self,
        token_id: &TokenId,
        stream_session_id: Uuid,
        from_sequence: u64,
        source_cutoff_ms: i64,
        decision_at_ms: i64,
    ) -> Result<Vec<BookL2LedgerRow>, StorageError> {
        BOOK_LEDGER_FROM
            .query(
                self.pool.client(),
                "SELECT ?fields FROM quant_book_l2_ledger \
                 WHERE token_id = ? \
                 AND stream_session_id = ? \
                 AND token_sequence >= ? \
                 AND venue_event_time <= fromUnixTimestamp64Milli(?) \
                 AND persisted_time <= fromUnixTimestamp64Milli(?) \
                 ORDER BY token_sequence, persisted_time, event_hash",
            )
            .bind(token_id.clone())
            .bind(stream_session_id)
            .bind(from_sequence)
            .bind(source_cutoff_ms)
            .bind(decision_at_ms)
            .fetch_all::<BookL2LedgerRow>()
            .await
            .map_err(StorageError::from)
    }

    async fn book_l2_replay_from(
        &self,
        mut anchors: Vec<BookLedgerReplayAnchor>,
        source_cutoff_ms: i64,
        decision_at_ms: i64,
    ) -> Result<Vec<BookL2LedgerRow>, StorageError> {
        anchors.sort_unstable_by(|left, right| left.token_id.as_str().cmp(right.token_id.as_str()));
        if let Some(duplicate) = anchors
            .windows(2)
            .find(|pair| pair[0].token_id == pair[1].token_id)
        {
            return Err(StorageError::invariant_violation(
                Some("quant_book_l2_ledger"),
                format!(
                    "book replay batch contains duplicate token anchor {}",
                    duplicate[0].token_id
                ),
            ));
        }
        let mut rows = Vec::new();
        for page in query_chunks(
            &anchors,
            |anchor| anchor.token_id.as_str().len() + UUID_INLINE_BYTES + 20,
            "quant_book_l2_ledger",
        )? {
            let token_ids = page
                .iter()
                .map(|anchor| anchor.token_id.clone())
                .collect::<Vec<_>>();
            let session_ids = page
                .iter()
                .map(|anchor| anchor.stream_session_id)
                .collect::<Vec<_>>();
            let sequences = page
                .iter()
                .map(|anchor| anchor.from_sequence)
                .collect::<Vec<_>>();
            let replay = BOOK_LEDGER_REPLAY_FROM
                .query(
                    self.pool.client(),
                    "WITH CAST(? AS Array(String)) AS anchor_token_ids, \
                     CAST(? AS Array(UUID)) AS anchor_session_ids, \
                     CAST(? AS Array(UInt64)) AS anchor_sequences \
                     SELECT ?fields FROM quant_book_l2_ledger \
                     WHERE has(anchor_token_ids, token_id) \
                     AND stream_session_id = anchor_session_ids[indexOf(anchor_token_ids, token_id)] \
                     AND token_sequence >= anchor_sequences[indexOf(anchor_token_ids, token_id)] \
                     AND venue_event_time <= fromUnixTimestamp64Milli(?) \
                     AND persisted_time <= fromUnixTimestamp64Milli(?) \
                     ORDER BY token_id, stream_session_id, token_sequence, persisted_time DESC, event_hash \
                     LIMIT 1 BY token_id, stream_session_id, token_sequence",
                )
                .bind(token_ids)
                .bind(session_ids)
                .bind(sequences)
                .bind(source_cutoff_ms)
                .bind(decision_at_ms)
                .fetch_all::<BookL2LedgerRow>()
                .await?;
            extend_rows(
                &mut rows,
                replay,
                BOOK_LEDGER_REPLAY_FROM,
                "quant_book_l2_ledger",
            )?;
        }
        Ok(rows)
    }

    async fn book_l2_ledger_between(
        &self,
        token_ids: Vec<TokenId>,
        from_ms: i64,
        to_ms: i64,
        available_by_ms: i64,
    ) -> Result<Vec<BookL2LedgerRow>, StorageError> {
        let token_ids = canonical_values(token_ids);
        if token_ids.is_empty() {
            return Ok(Vec::new());
        }
        let mut rows = Vec::new();
        for tokens in query_chunks(
            &token_ids,
            |token| token.as_str().len(),
            "quant_book_l2_ledger",
        )? {
            let page = BOOK_LEDGER_BETWEEN
                .query(
                    self.pool.client(),
                    "SELECT ?fields FROM quant_book_l2_ledger \
                     WHERE token_id IN ? \
                     AND venue_event_time >= fromUnixTimestamp64Milli(?) \
                     AND venue_event_time < fromUnixTimestamp64Milli(?) \
                     AND persisted_time <= fromUnixTimestamp64Milli(?) \
                     ORDER BY token_id, stream_session_id, token_sequence, persisted_time DESC, event_hash \
                     LIMIT 1 BY token_id, stream_session_id, token_sequence",
                )
                .bind(tokens.to_vec())
                .bind(from_ms)
                .bind(to_ms)
                .bind(available_by_ms)
                .fetch_all::<BookL2LedgerRow>()
                .await?;
            extend_rows(&mut rows, page, BOOK_LEDGER_BETWEEN, "quant_book_l2_ledger")?;
        }
        Ok(rows)
    }

    async fn book_stream_session_at(
        &self,
        stream_session_id: Uuid,
        decision_at_ms: i64,
    ) -> Result<Option<BookStreamSessionRow>, StorageError> {
        BOOK_STREAM_SESSION_AT
            .query(
                self.pool.client(),
                "SELECT ?fields FROM quant_book_stream_session \
                 WHERE stream_session_id = ? \
                 AND recorded_at <= fromUnixTimestamp64Milli(?) \
                 ORDER BY ledger_sequence DESC, recorded_at DESC \
                 LIMIT 1",
            )
            .bind(stream_session_id)
            .bind(decision_at_ms)
            .fetch_optional::<BookStreamSessionRow>()
            .await
            .map_err(StorageError::from)
    }

    async fn book_stream_sessions(
        &self,
        stream_session_ids: Vec<Uuid>,
        available_by_ms: i64,
    ) -> Result<Vec<BookStreamSessionRow>, StorageError> {
        let stream_session_ids = canonical_values(stream_session_ids);
        if stream_session_ids.is_empty() {
            return Ok(Vec::new());
        }
        let mut rows = Vec::new();
        for sessions in query_chunks(
            &stream_session_ids,
            |_| UUID_INLINE_BYTES,
            "quant_book_stream_session",
        )? {
            let page = BOOK_STREAM_SESSIONS
                .query(
                    self.pool.client(),
                    "SELECT ?fields FROM quant_book_stream_session \
                     WHERE stream_session_id IN ? \
                     AND recorded_at <= fromUnixTimestamp64Milli(?) \
                     ORDER BY stream_session_id, ledger_sequence DESC, recorded_at DESC \
                     LIMIT 1 BY stream_session_id",
                )
                .bind(sessions.to_vec())
                .bind(available_by_ms)
                .fetch_all::<BookStreamSessionRow>()
                .await?;
            extend_rows(
                &mut rows,
                page,
                BOOK_STREAM_SESSIONS,
                "quant_book_stream_session",
            )?;
        }
        Ok(rows)
    }

    async fn book_ledger_snapshots_at(
        &self,
        token_ids: Vec<TokenId>,
        source_cutoff_ms: i64,
        decision_at_ms: i64,
    ) -> Result<Vec<BookL2LedgerRow>, StorageError> {
        let token_ids = canonical_values(token_ids);
        if token_ids.is_empty() {
            return Ok(Vec::new());
        }
        let mut rows = Vec::new();
        for tokens in query_chunks(
            &token_ids,
            |token| token.as_str().len(),
            "quant_book_l2_ledger",
        )? {
            let page = BOOK_LEDGER_SNAPSHOTS_AT
                .query(
                    self.pool.client(),
                    "SELECT ?fields FROM quant_book_l2_ledger \
                     WHERE token_id IN ? AND event_type = 'Snapshot' \
                     AND venue_event_time <= fromUnixTimestamp64Milli(?) \
                     AND persisted_time <= fromUnixTimestamp64Milli(?) \
                     ORDER BY token_id, venue_event_time DESC, persisted_time DESC, token_sequence DESC, event_hash \
                     LIMIT 1 BY token_id",
                )
                .bind(tokens.to_vec())
                .bind(source_cutoff_ms)
                .bind(decision_at_ms)
                .fetch_all::<BookL2LedgerRow>()
                .await?;
            extend_rows(
                &mut rows,
                page,
                BOOK_LEDGER_SNAPSHOTS_AT,
                "quant_book_l2_ledger",
            )?;
        }
        Ok(rows)
    }

    async fn book_ledger_snapshots_between(
        &self,
        token_ids: Vec<TokenId>,
        from_ms: i64,
        to_ms: i64,
        available_by_ms: i64,
    ) -> Result<Vec<BookL2LedgerRow>, StorageError> {
        let token_ids = canonical_values(token_ids);
        if token_ids.is_empty() {
            return Ok(Vec::new());
        }
        let mut rows = Vec::new();
        for tokens in query_chunks(
            &token_ids,
            |token| token.as_str().len(),
            "quant_book_l2_ledger",
        )? {
            let page = BOOK_LEDGER_SNAPSHOTS_BETWEEN
                .query(
                    self.pool.client(),
                    "SELECT ?fields FROM quant_book_l2_ledger \
                     WHERE token_id IN ? AND event_type = 'Snapshot' \
                     AND venue_event_time >= fromUnixTimestamp64Milli(?) \
                     AND venue_event_time <= fromUnixTimestamp64Milli(?) \
                     AND persisted_time <= fromUnixTimestamp64Milli(?) \
                     ORDER BY token_id, venue_event_time, persisted_time, token_sequence, event_hash",
                )
                .bind(tokens.to_vec())
                .bind(from_ms)
                .bind(to_ms)
                .bind(available_by_ms)
                .fetch_all::<BookL2LedgerRow>()
                .await?;
            extend_rows(
                &mut rows,
                page,
                BOOK_LEDGER_SNAPSHOTS_BETWEEN,
                "quant_book_l2_ledger",
            )?;
        }
        Ok(rows)
    }

    async fn resolution_at(
        &self,
        market_id: &MarketId,
        source_cutoff_ms: i64,
        decision_at_ms: i64,
    ) -> Result<Option<MarketResolutionRow>, StorageError> {
        let rows = RESOLUTION_AT
            .query(
                self.pool.client(),
                "SELECT DISTINCT ?fields FROM market_resolution_event \
                 WHERE market_id = ? \
                 AND resolved_at <= fromUnixTimestamp64Milli(?) \
                 AND observed_at <= fromUnixTimestamp64Milli(?) \
                 ORDER BY resolved_at DESC, observed_at DESC, source_block_number DESC, \
                 source_log_index DESC, resolution_fact_hash DESC \
                 LIMIT 2",
            )
            .bind(market_id.clone())
            .bind(source_cutoff_ms)
            .bind(decision_at_ms)
            .fetch_all::<MarketResolutionRow>()
            .await?;
        validated_unique_resolution(&rows, &format!("market {market_id} at PIT cutoff"))
    }

    async fn resolution_by_checkpoint(
        &self,
        source_checkpoint_hash: &ContentHash,
    ) -> Result<Option<MarketResolutionRow>, StorageError> {
        let rows = RESOLUTION_BY_CHECKPOINT
            .query(
                self.pool.client(),
                "SELECT DISTINCT ?fields FROM market_resolution_event \
                 WHERE source_checkpoint_hash = ? \
                 ORDER BY resolution_fact_hash \
                 LIMIT 2",
            )
            .bind(*source_checkpoint_hash)
            .fetch_all::<MarketResolutionRow>()
            .await?;
        validated_unique_resolution(
            &rows,
            &format!("source checkpoint {source_checkpoint_hash}"),
        )
    }

    async fn resolution_by_market(
        &self,
        market_id: &MarketId,
    ) -> Result<Option<MarketResolutionRow>, StorageError> {
        let rows = RESOLUTION_BY_MARKET
            .query(
                self.pool.client(),
                "SELECT DISTINCT ?fields FROM market_resolution_event \
                 WHERE market_id = ? \
                 ORDER BY resolution_fact_hash \
                 LIMIT 2",
            )
            .bind(market_id.clone())
            .fetch_all::<MarketResolutionRow>()
            .await?;
        validated_unique_resolution(&rows, &format!("market {market_id}"))
    }

    async fn resolutions_between(
        &self,
        market_ids: Vec<MarketId>,
        from_ms: i64,
        to_ms: i64,
        decision_at_ms: i64,
    ) -> Result<Vec<MarketResolutionRow>, StorageError> {
        let market_ids = canonical_values(market_ids);
        if market_ids.is_empty() {
            return Ok(Vec::new());
        }
        let mut rows = Vec::new();
        for markets in query_chunks(
            &market_ids,
            |market| market.as_str().len(),
            "market_resolution_event",
        )? {
            let page = RESOLUTIONS_BETWEEN
                .query(
                    self.pool.client(),
                    "SELECT DISTINCT ?fields FROM market_resolution_event \
                     WHERE market_id IN ? \
                     AND resolved_at >= fromUnixTimestamp64Milli(?) \
                     AND resolved_at <= fromUnixTimestamp64Milli(?) \
                     AND observed_at <= fromUnixTimestamp64Milli(?) \
                     ORDER BY market_id, resolved_at, observed_at, source_block_number, \
                     source_log_index, resolution_fact_hash",
                )
                .bind(markets.to_vec())
                .bind(from_ms)
                .bind(to_ms)
                .bind(decision_at_ms)
                .fetch_all::<MarketResolutionRow>()
                .await?;
            extend_rows(
                &mut rows,
                page,
                RESOLUTIONS_BETWEEN,
                "market_resolution_event",
            )?;
        }
        validate_unique_resolution_markets(&rows)?;
        Ok(rows)
    }

    async fn observed_markets_between(
        &self,
        from_ms: i64,
        to_ms: i64,
        decision_at_ms: i64,
    ) -> Result<Vec<MarketId>, StorageError> {
        // `market_id` is Nullable in the ledger; `assumeNotNull` after the
        // `IS NOT NULL` guard yields a non-nullable column the row can decode.
        let rows = OBSERVED_MARKETS_BETWEEN
            .query(
                self.pool.client(),
                "SELECT DISTINCT assumeNotNull(market_id) AS market_id FROM quant_book_l2_ledger \
                 WHERE market_id IS NOT NULL AND event_type = 'Snapshot' \
                 AND venue_event_time >= fromUnixTimestamp64Milli(?) \
                 AND venue_event_time <= fromUnixTimestamp64Milli(?) \
                 AND persisted_time <= fromUnixTimestamp64Milli(?) \
                 ORDER BY market_id",
            )
            .bind(from_ms)
            .bind(to_ms)
            .bind(decision_at_ms)
            .fetch_all::<ObservedMarketRow>()
            .await?;
        Ok(rows.into_iter().map(|row| row.market_id).collect())
    }

    async fn domain_observations_between(
        &self,
        instrument_keys: Vec<DomainInstrumentKey>,
        from_ms: i64,
        to_ms: i64,
        publish_cutoff_ms: i64,
        decision_at_ms: i64,
    ) -> Result<Vec<DomainObservationRow>, StorageError> {
        let instrument_keys = canonical_values(instrument_keys);
        if instrument_keys.is_empty() {
            return Ok(Vec::new());
        }
        let mut rows = Vec::new();
        for instruments in query_chunks(
            &instrument_keys,
            |instrument| instrument.as_str().len(),
            "quant_domain_observation",
        )? {
            let page = DOMAIN_OBSERVATIONS_BETWEEN
                .query(
                    self.pool.client(),
                    "SELECT ?fields FROM quant_domain_observation \
                     WHERE instrument_key IN ? \
                     AND event_time >= fromUnixTimestamp64Milli(?) \
                     AND event_time < fromUnixTimestamp64Milli(?) \
                     AND publish_time <= fromUnixTimestamp64Milli(?) \
                     AND ingestion_time <= fromUnixTimestamp64Milli(?) \
                     ORDER BY ingestion_time DESC, \
                     cityHash64(tuple(family, source_id, value, publish_time, schema_version)) DESC \
                     LIMIT 1 BY instrument_key, metric, event_time",
                )
                .bind(instruments.to_vec())
                .bind(from_ms)
                .bind(to_ms)
                .bind(publish_cutoff_ms)
                .bind(decision_at_ms)
                .fetch_all::<DomainObservationRow>()
                .await?;
            extend_rows(
                &mut rows,
                page,
                DOMAIN_OBSERVATIONS_BETWEEN,
                "quant_domain_observation",
            )?;
        }
        rows.sort_by(|left, right| {
            (
                left.instrument_key.as_str(),
                left.metric.as_str(),
                left.event_time,
                left.ingestion_time,
            )
                .cmp(&(
                    right.instrument_key.as_str(),
                    right.metric.as_str(),
                    right.event_time,
                    right.ingestion_time,
                ))
        });
        Ok(rows)
    }

    async fn domain_observation_at(
        &self,
        instrument_key: &DomainInstrumentKey,
        metric: &str,
        source_cutoff_ms: i64,
        decision_at_ms: i64,
    ) -> Result<Option<DomainObservationRow>, StorageError> {
        let row = DOMAIN_OBSERVATION_AT
            .query(
                self.pool.client(),
                "SELECT ?fields FROM quant_domain_observation \
                 WHERE instrument_key = ? \
                 AND metric = ? \
                 AND event_time <= fromUnixTimestamp64Milli(?) \
                 AND publish_time <= fromUnixTimestamp64Milli(?) \
                 AND ingestion_time <= fromUnixTimestamp64Milli(?) \
                 ORDER BY event_time DESC, ingestion_time DESC, \
                 cityHash64(tuple(family, source_id, value, publish_time, schema_version)) DESC \
                 LIMIT 1",
            )
            .bind(instrument_key.clone())
            .bind(metric)
            .bind(source_cutoff_ms)
            .bind(source_cutoff_ms)
            .bind(decision_at_ms)
            .fetch_optional::<DomainObservationRow>()
            .await?;
        Ok(row)
    }
}

fn validated_unique_resolution(
    rows: &[MarketResolutionRow],
    identity: &str,
) -> Result<Option<MarketResolutionRow>, StorageError> {
    for row in rows {
        validate_market_resolution(row)?;
    }
    match rows {
        [] => Ok(None),
        [row] => Ok(Some(row.clone())),
        [first, second] => Err(StorageError::invariant_violation(
            Some(MARKET_RESOLUTION_EVENT),
            format!(
                "{identity} binds conflicting resolution facts {} and {}",
                first.resolution_fact_hash, second.resolution_fact_hash
            ),
        )),
        _ => Err(StorageError::invariant_violation(
            Some(MARKET_RESOLUTION_EVENT),
            "bounded exact resolution query returned more than two rows",
        )),
    }
}

fn validate_unique_resolution_markets(rows: &[MarketResolutionRow]) -> Result<(), StorageError> {
    let mut facts = HashMap::with_capacity(rows.len());
    for row in rows {
        validate_market_resolution(row)?;
        if let Some(existing) = facts.insert(row.market_id.clone(), row.resolution_fact_hash)
            && existing != row.resolution_fact_hash
        {
            return Err(StorageError::invariant_violation(
                Some(MARKET_RESOLUTION_EVENT),
                format!(
                    "market {} binds conflicting resolution facts {} and {}",
                    row.market_id, existing, row.resolution_fact_hash
                ),
            ));
        }
    }
    Ok(())
}

/// Single-column projection for [`ChQuantFactReadRepository::observed_markets_between`].
#[derive(clickhouse::Row, serde::Deserialize)]
struct ObservedMarketRow {
    market_id: MarketId,
}

#[derive(clickhouse::Row, serde::Deserialize)]
struct FunnelTotalRow {
    row_count: u64,
}

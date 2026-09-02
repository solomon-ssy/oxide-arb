//! `ClickHouse`-backed read repository for quant facts (feature window inputs +
//! historical point-in-time state).

use std::{collections::HashMap, sync::Arc};

use async_trait::async_trait;
use quant_pivot_error::storage::{StorageError, entity::MARKET_RESOLUTION_EVENT};
use quant_pivot_models::{
    clickhouse::{
        BookL2LedgerRow, BookLedgerReplayAnchor, BookMicrostructureRow, BookStreamSessionRow,
        CryptoPriceReportRow, DomainObservationRow, EntryConditionEvaluationEventRow,
        ExchangeEventRow, ExchangeMatchRow, ExecutionParticipantFactRow, ExecutionParticipantRow,
        MarketExecutionRow, MarketResolutionRow, MidPriceBucketRow, QuantSignalCandidateEventRow,
        ReportMarketFunnelCountRow, ReportMarketFunnelRow, WeatherForecastFactRow,
        WeatherObservationFactRow,
    },
    domain::{data_plane::HistorySealChunkRef, quant::AccountChainEventCursor},
    types::{
        ContentHash, DomainInstrumentKey, DomainSourceId, EntryConditionInstanceId, EvmAddress,
        MarketId, ModelVersionId, OrderId, RecommendationReportId, TokenId,
    },
};
use quant_pivot_storage::clickhouse::ClickHousePool;
use uuid::Uuid;

use crate::{
    clickhouse::{
        query_batch::{UUID_INLINE_BYTES, canonical_values, extend_rows, query_chunks},
        query_limits::{
            ACCOUNT_ORDER_FILLED_EVENTS, BOOK_LEDGER_BETWEEN, BOOK_LEDGER_FROM,
            BOOK_LEDGER_REPLAY_FROM, BOOK_LEDGER_SNAPSHOT_AT, BOOK_LEDGER_SNAPSHOTS_AT,
            BOOK_LEDGER_SNAPSHOTS_BETWEEN, BOOK_STREAM_SESSION_AT, BOOK_STREAM_SESSIONS,
            CRYPTO_REPORT_FRONTIER, CRYPTO_REPORTS_AVAILABLE, CRYPTO_REPORTS_BETWEEN,
            DOMAIN_OBSERVATION_AT, DOMAIN_OBSERVATIONS_BETWEEN, ENTRY_EVALUATION_LATEST,
            EXECUTION_PARTICIPANTS_BETWEEN, LAST_EXECUTIONS, MARKET_EXECUTION_WINDOW,
            MARKET_EXECUTIONS_BETWEEN, MATCHES_FOR_TAKER_ORDERS, MICROSTRUCTURE_SERIES,
            MICROSTRUCTURE_WINDOW, MID_PRICE_SERIES, OBSERVED_MARKETS_BETWEEN, ORDER_FILLED_EVENTS,
            REPORT_FUNNEL_BETWEEN, REPORT_FUNNEL_COUNT, REPORT_FUNNEL_COUNTS, REPORT_FUNNEL_PAGE,
            RESOLUTION_AT, RESOLUTION_BY_CHECKPOINT, RESOLUTION_BY_MARKET, RESOLUTIONS_BETWEEN,
            SIGNAL_CANDIDATES_BETWEEN, WEATHER_FORECASTS_BETWEEN, WEATHER_OBSERVATIONS_BETWEEN,
        },
    },
    traits::{CryptoReportFrontierQuery, CryptoReportsAvailableQuery, QuantFactReadRepository},
};

const ACTIVE_HISTORY_RANGES: &str = "SELECT chunk_id AS accepted_chunk_id, \
    tupleElement(argMax(tuple(frontier, from_block, to_block, active), state_revision), 1) \
        AS accepted_frontier, \
    tupleElement(argMax(tuple(frontier, from_block, to_block, active), state_revision), 2) \
        AS accepted_from_block, \
    tupleElement(argMax(tuple(frontier, from_block, to_block, active), state_revision), 3) \
        AS accepted_to_block, \
    max(state_revision) AS accepted_state_revision \
    FROM quant_exchange_history_acceptance \
    GROUP BY chunk_id \
    HAVING tupleElement( \
        argMax(tuple(frontier, from_block, to_block, active), state_revision), 4 \
    ) = 1";
const FILTERED_ACTIVE_HISTORY_RANGES: &str = "SELECT chunk_id AS accepted_chunk_id, \
    tupleElement(argMax(tuple(frontier, from_block, to_block, active), state_revision), 1) \
        AS accepted_frontier, \
    tupleElement(argMax(tuple(frontier, from_block, to_block, active), state_revision), 2) \
        AS accepted_from_block, \
    tupleElement(argMax(tuple(frontier, from_block, to_block, active), state_revision), 3) \
        AS accepted_to_block, \
    max(state_revision) AS accepted_state_revision \
    FROM quant_exchange_history_acceptance \
    WHERE chunk_id IN ? \
    GROUP BY chunk_id \
    HAVING tupleElement( \
        argMax(tuple(frontier, from_block, to_block, active), state_revision), 4 \
    ) = 1";
const HISTORY_RANGE_JOIN: &str = "history.accepted_chunk_id = fact.chunk_id \
    AND fact.block_number >= history.accepted_from_block \
    AND fact.block_number <= history.accepted_to_block";

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

    async fn validate_history_chunks(
        &self,
        chunks: &[HistorySealChunkRef],
    ) -> Result<Vec<Uuid>, StorageError> {
        if chunks.is_empty() {
            return Err(StorageError::invariant_violation(
                Some("quant_exchange_history_acceptance"),
                "sealed execution-history read requires at least one chunk",
            ));
        }
        let expected = chunks
            .iter()
            .map(|chunk| {
                let revision = u64::try_from(chunk.state_revision).map_err(|error| {
                    StorageError::invariant_violation(
                        Some("quant_exchange_history_acceptance"),
                        format!(
                            "sealed chunk {} has invalid state revision {}: {error}",
                            chunk.chunk_id, chunk.state_revision
                        ),
                    )
                })?;
                if revision == 0 {
                    return Err(StorageError::invariant_violation(
                        Some("quant_exchange_history_acceptance"),
                        format!("sealed chunk {} has zero state revision", chunk.chunk_id),
                    ));
                }
                let from_block = u64::try_from(chunk.from_block).map_err(|error| {
                    StorageError::invariant_violation(
                        Some("quant_exchange_history_acceptance"),
                        format!(
                            "sealed chunk {} has invalid from_block {}: {error}",
                            chunk.chunk_id, chunk.from_block
                        ),
                    )
                })?;
                let to_block = u64::try_from(chunk.to_block).map_err(|error| {
                    StorageError::invariant_violation(
                        Some("quant_exchange_history_acceptance"),
                        format!(
                            "sealed chunk {} has invalid to_block {}: {error}",
                            chunk.chunk_id, chunk.to_block
                        ),
                    )
                })?;
                if from_block > to_block {
                    return Err(StorageError::invariant_violation(
                        Some("quant_exchange_history_acceptance"),
                        format!(
                            "sealed chunk {} has descending block range {}..={}",
                            chunk.chunk_id, chunk.from_block, chunk.to_block
                        ),
                    ));
                }
                Ok((
                    chunk.chunk_id,
                    (
                        chunk.frontier.as_str().to_owned(),
                        from_block,
                        to_block,
                        revision,
                    ),
                ))
            })
            .collect::<Result<HashMap<_, _>, StorageError>>()?;
        if expected.len() != chunks.len() {
            return Err(StorageError::invariant_violation(
                Some("quant_exchange_history_acceptance"),
                "sealed execution-history chunk set contains duplicate ids",
            ));
        }
        for pair in chunks.windows(2) {
            let expected_from = pair[0].to_block.checked_add(1).ok_or_else(|| {
                StorageError::invariant_violation(
                    Some("quant_exchange_history_acceptance"),
                    format!(
                        "sealed chunk {} block range cannot advance past {}",
                        pair[0].chunk_id, pair[0].to_block
                    ),
                )
            })?;
            if pair[1].from_block != expected_from {
                return Err(StorageError::invariant_violation(
                    Some("quant_exchange_history_acceptance"),
                    format!(
                        "sealed chunks {} and {} are not contiguous: expected next from_block {}, got {}",
                        pair[0].chunk_id, pair[1].chunk_id, expected_from, pair[1].from_block
                    ),
                ));
            }
        }
        let chunk_ids = canonical_values(expected.keys().copied().collect::<Vec<_>>());
        let mut actual = HashMap::with_capacity(chunk_ids.len());
        for ids in query_chunks(
            &chunk_ids,
            |_| UUID_INLINE_BYTES,
            "quant_exchange_history_acceptance",
        )? {
            let rows = MARKET_EXECUTION_WINDOW
                .query(
                    self.pool.as_ref(),
                    &format!(
                        "SELECT accepted_chunk_id AS chunk_id, accepted_frontier, \
                         accepted_from_block, accepted_to_block, accepted_state_revision \
                         FROM ({FILTERED_ACTIVE_HISTORY_RANGES})"
                    ),
                )
                .bind(ids.to_vec())
                .fetch_all::<ActiveHistoryContractRow>()
                .await?;
            for row in rows {
                if actual
                    .insert(
                        row.chunk_id,
                        (
                            row.accepted_frontier,
                            row.accepted_from_block,
                            row.accepted_to_block,
                            row.accepted_state_revision,
                        ),
                    )
                    .is_some()
                {
                    return Err(StorageError::invariant_violation(
                        Some("quant_exchange_history_acceptance"),
                        format!(
                            "active history revision query duplicated chunk {}",
                            row.chunk_id
                        ),
                    ));
                }
            }
        }
        if actual != expected {
            return Err(StorageError::state_conflict(
                "quant_exchange_history_acceptance",
                None::<&Uuid>,
                "sealed execution-history contracts are no longer the complete active set",
            ));
        }
        Ok(chunk_ids)
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
    async fn signal_candidates_between(
        &self,
        token_id: &TokenId,
        model_version_id: &ModelVersionId,
        from_ms: i64,
        to_ms: i64,
        available_by_ms: i64,
    ) -> Result<Vec<QuantSignalCandidateEventRow>, StorageError> {
        SIGNAL_CANDIDATES_BETWEEN
            .query(
                self.pool.as_ref(),
                "SELECT ?fields FROM quant_signal_candidate_event \
                 WHERE token_id = ? AND model_version_id = ? \
                 AND event_time >= fromUnixTimestamp64Milli(?) \
                 AND event_time <= fromUnixTimestamp64Milli(?) \
                 AND ingestion_time <= fromUnixTimestamp64Milli(?) \
                 ORDER BY event_time, ingestion_time, model_run_id, route_rank, signal_candidate_id",
            )
            .bind(token_id.clone())
            .bind(*model_version_id)
            .bind(from_ms)
            .bind(to_ms)
            .bind(available_by_ms)
            .fetch_all::<QuantSignalCandidateEventRow>()
            .await
    }

    async fn account_order_filled_events(
        &self,
        funder: &EvmAddress,
        cursor: Option<AccountChainEventCursor>,
        limit: u64,
    ) -> Result<Vec<ExchangeEventRow>, StorageError> {
        if limit == 0 {
            return Ok(Vec::new());
        }
        let address = funder.as_str().to_owned();
        let base = format!(
            "SELECT ?fields FROM quant_exchange_event AS fact \
             INNER JOIN ({ACTIVE_HISTORY_RANGES}) AS history ON {HISTORY_RANGE_JOIN} \
             WHERE fact.maker = ? \
             AND fact.event_kind = 'OrderFilled' \
             AND fact.exchange_version = 'V2'"
        );
        let page = if let Some(cursor) = cursor {
            ACCOUNT_ORDER_FILLED_EVENTS
                .query(
                    self.pool.as_ref(),
                    &format!(
                        "{base} AND (fact.block_number > ? \
                         OR (fact.block_number = ? AND fact.transaction_index > ?) \
                         OR (fact.block_number = ? AND fact.transaction_index = ? AND fact.log_index > ?)) \
                         ORDER BY fact.block_number, fact.transaction_index, fact.log_index LIMIT ?"
                    ),
                )
                .bind(address)
                .bind(cursor.block_number)
                .bind(cursor.block_number)
                .bind(cursor.transaction_index)
                .bind(cursor.block_number)
                .bind(cursor.transaction_index)
                .bind(cursor.log_index)
                .bind(limit)
                .fetch_all::<ExchangeEventRow>()
                .await?
        } else {
            ACCOUNT_ORDER_FILLED_EVENTS
                .query(
                    self.pool.as_ref(),
                    &format!(
                        "{base} ORDER BY fact.block_number, fact.transaction_index, fact.log_index LIMIT ?"
                    ),
                )
                .bind(address)
                .bind(limit)
                .fetch_all::<ExchangeEventRow>()
                .await?
        };
        let mut rows = Vec::new();
        extend_rows(
            &mut rows,
            page,
            ACCOUNT_ORDER_FILLED_EVENTS,
            "quant_exchange_event",
        )?;
        Ok(rows)
    }

    async fn matches_for_taker_orders(
        &self,
        order_ids: Vec<OrderId>,
    ) -> Result<Vec<ExchangeMatchRow>, StorageError> {
        let order_ids = canonical_values(
            order_ids
                .into_iter()
                .map(|order_id| order_id.as_str().to_owned())
                .collect(),
        );
        if order_ids.is_empty() {
            return Ok(Vec::new());
        }
        let mut rows = Vec::new();
        for order_chunk in query_chunks(&order_ids, String::len, "quant_exchange_match")? {
            let page = MATCHES_FOR_TAKER_ORDERS
                .query(
                    self.pool.as_ref(),
                    &format!(
                        "SELECT ?fields FROM quant_exchange_match AS fact \
                         INNER JOIN ({ACTIVE_HISTORY_RANGES}) AS history ON {HISTORY_RANGE_JOIN} \
                         WHERE fact.taker_order_hash IN ? \
                         AND fact.exchange_version = 'V2' \
                         ORDER BY fact.taker_order_hash, fact.block_number, fact.transaction_hash"
                    ),
                )
                .bind(order_chunk.to_vec())
                .fetch_all::<ExchangeMatchRow>()
                .await?;
            extend_rows(
                &mut rows,
                page,
                MATCHES_FOR_TAKER_ORDERS,
                "quant_exchange_match",
            )?;
        }
        Ok(rows)
    }

    async fn order_filled_events(
        &self,
        order_ids: Vec<OrderId>,
    ) -> Result<Vec<ExchangeEventRow>, StorageError> {
        let order_ids = canonical_values(
            order_ids
                .into_iter()
                .map(|order_id| order_id.as_str().to_owned())
                .collect(),
        );
        if order_ids.is_empty() {
            return Ok(Vec::new());
        }
        let mut rows = Vec::new();
        for order_chunk in query_chunks(&order_ids, String::len, "quant_exchange_event")? {
            let page = ORDER_FILLED_EVENTS
                .query(
                    self.pool.as_ref(),
                    &format!(
                        "SELECT ?fields FROM quant_exchange_event AS fact \
                         INNER JOIN ({ACTIVE_HISTORY_RANGES}) AS history ON {HISTORY_RANGE_JOIN} \
                         WHERE fact.order_hash IN ? \
                         AND fact.event_kind = 'OrderFilled' \
                         AND fact.exchange_version = 'V2' \
                         ORDER BY fact.order_hash, fact.block_number, fact.transaction_index, fact.log_index"
                    ),
                )
                .bind(order_chunk.to_vec())
                .fetch_all::<ExchangeEventRow>()
                .await?;
            extend_rows(&mut rows, page, ORDER_FILLED_EVENTS, "quant_exchange_event")?;
        }
        Ok(rows)
    }

    async fn validate_execution_history_chunks(
        &self,
        history_chunks: Vec<HistorySealChunkRef>,
    ) -> Result<(), StorageError> {
        self.validate_history_chunks(&history_chunks)
            .await
            .map(drop)
    }

    async fn report_market_funnel_counts(
        &self,
        report_id: &RecommendationReportId,
    ) -> Result<Vec<ReportMarketFunnelCountRow>, StorageError> {
        REPORT_FUNNEL_COUNTS
            .query(
                self.pool.as_ref(),
                "SELECT terminal_stage, count() AS row_count \
                 FROM quant_report_market_funnel FINAL \
                 WHERE recommendation_report_id = ? \
                 GROUP BY terminal_stage ORDER BY terminal_stage",
            )
            .bind(*report_id)
            .fetch_all::<ReportMarketFunnelCountRow>()
            .await
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
            .query(self.pool.as_ref(), &sql)
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
            .query(self.pool.as_ref(), &sql)
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
    }

    async fn report_funnel_between(
        &self,
        from_ms: i64,
        to_ms: i64,
    ) -> Result<Vec<ReportMarketFunnelRow>, StorageError> {
        REPORT_FUNNEL_BETWEEN
            .query(
                self.pool.as_ref(),
                "SELECT ?fields FROM quant_report_market_funnel FINAL \
                 WHERE event_time >= fromUnixTimestamp64Milli(?) \
                   AND event_time < fromUnixTimestamp64Milli(?) \
                 ORDER BY recommendation_report_id, market_id",
            )
            .bind(from_ms)
            .bind(to_ms)
            .fetch_all::<ReportMarketFunnelRow>()
            .await
    }

    async fn latest_entry_evaluation(
        &self,
        instance_id: &EntryConditionInstanceId,
    ) -> Result<Option<EntryConditionEvaluationEventRow>, StorageError> {
        ENTRY_EVALUATION_LATEST
            .query(
                self.pool.as_ref(),
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
    }

    async fn crypto_price_reports_at(
        &self,
        query: CryptoReportFrontierQuery,
    ) -> Result<Vec<CryptoPriceReportRow>, StorageError> {
        let CryptoReportFrontierQuery {
            source_id,
            instrument_key,
            gap_generation,
            committed_source_sequence,
            committed_published_at_ms,
            source_timestamp_ms,
            decision_at_ms,
        } = query;
        let secondary_order = if source_id == DomainSourceId::polymarket_rtds_binance()
            || source_id == DomainSourceId::polymarket_rtds_chainlink()
        {
            "published_at"
        } else {
            "fromUnixTimestamp64Milli(0)"
        };
        let sql = format!(
            "SELECT ?fields FROM (\
                 SELECT *, dense_rank() OVER (\
                     ORDER BY source_sequence DESC, {secondary_order} DESC\
                 ) AS frontier_rank \
                 FROM (\
                     SELECT *, row_number() OVER (\
                         PARTITION BY source_id, instrument_key, gap_generation, source_sequence, event_time, report_hash \
                         ORDER BY available_at DESC\
                     ) AS dedupe_rank \
                     FROM quant_crypto_price_report \
                     WHERE source_id = ? \
                     AND instrument_key = ? \
                     AND gap_generation = ? \
                     AND (source_sequence < ? OR (source_sequence = ? \
                         AND toUnixTimestamp64Milli(published_at) <= ?)) \
                     AND ifNull(observations_timestamp, event_time) <= fromUnixTimestamp64Milli(?) \
                     AND available_at <= fromUnixTimestamp64Milli(?)\
                 ) WHERE dedupe_rank = 1\
             ) WHERE frontier_rank = 1 \
             ORDER BY report_hash"
        );
        CRYPTO_REPORT_FRONTIER
            .query(self.pool.as_ref(), &sql)
            .bind(source_id)
            .bind(instrument_key)
            .bind(gap_generation)
            .bind(committed_source_sequence)
            .bind(committed_source_sequence)
            .bind(committed_published_at_ms)
            .bind(source_timestamp_ms)
            .bind(decision_at_ms)
            .fetch_all::<CryptoPriceReportRow>()
            .await
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
                    self.pool.as_ref(),
                    "SELECT ?fields FROM (\
                         SELECT *, row_number() OVER (\
                             PARTITION BY source_id, instrument_key, gap_generation, source_sequence, event_time, report_hash \
                             ORDER BY available_at DESC\
                         ) AS dedupe_rank \
                         FROM quant_crypto_price_report \
                         WHERE instrument_key IN ? \
                         AND event_time >= fromUnixTimestamp64Milli(?) \
                         AND event_time < fromUnixTimestamp64Milli(?) \
                         AND published_at <= fromUnixTimestamp64Milli(?) \
                         AND available_at <= fromUnixTimestamp64Milli(?)\
                     ) WHERE dedupe_rank = 1 \
                         ORDER BY instrument_key, gap_generation, event_time, available_at, source_sequence, report_hash",
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
        query: CryptoReportsAvailableQuery,
    ) -> Result<Vec<CryptoPriceReportRow>, StorageError> {
        let CryptoReportsAvailableQuery {
            source_id,
            instrument_key,
            gap_generation,
            committed_source_sequence,
            committed_published_at_ms,
            available_from_ms,
            available_to_ms,
            decision_at_ms,
        } = query;
        CRYPTO_REPORTS_AVAILABLE
            .query(
                self.pool.as_ref(),
                "SELECT ?fields FROM (\
                         SELECT *, row_number() OVER (\
                             PARTITION BY source_id, instrument_key, gap_generation, source_sequence, event_time, report_hash \
                             ORDER BY available_at DESC\
                         ) AS dedupe_rank \
                         FROM quant_crypto_price_report \
                         WHERE source_id = ? \
                         AND instrument_key = ? \
                         AND gap_generation = ? \
                         AND (source_sequence < ? OR (source_sequence = ? \
                             AND toUnixTimestamp64Milli(published_at) <= ?)) \
                         AND available_at >= fromUnixTimestamp64Milli(?) \
                         AND available_at < fromUnixTimestamp64Milli(?) \
                         AND available_at <= fromUnixTimestamp64Milli(?)\
                     ) WHERE dedupe_rank = 1 \
                     ORDER BY instrument_key, gap_generation, available_at, event_time, source_sequence, report_hash",
            )
            .bind(source_id)
            .bind(instrument_key)
            .bind(gap_generation)
            .bind(committed_source_sequence)
            .bind(committed_source_sequence)
            .bind(committed_published_at_ms)
            .bind(available_from_ms)
            .bind(available_to_ms)
            .bind(decision_at_ms)
            .fetch_all::<CryptoPriceReportRow>()
            .await
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
                    self.pool.as_ref(),
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
                    self.pool.as_ref(),
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
                    self.pool.as_ref(),
                    "SELECT ?fields FROM book_microstructure_1s \
                     WHERE token_id IN ? \
                     AND bucket_time >= fromUnixTimestamp64Milli(?) \
                     AND bucket_time < fromUnixTimestamp64Milli(?) \
                     AND bucket_time + toIntervalSecond(1) <= fromUnixTimestamp64Milli(?) \
                     AND available_at <= fromUnixTimestamp64Milli(?) \
                     ORDER BY token_id, bucket_time",
                )
                .bind(tokens.to_vec())
                .bind(from_ms)
                .bind(to_ms)
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
             AND bucket_time + toIntervalMinute(1) <= fromUnixTimestamp64Milli(?) \
             AND available_at <= fromUnixTimestamp64Milli(?) \
             ORDER BY token_id, bucket_time"
        } else {
            "SELECT ?fields FROM book_microstructure_1s \
             WHERE token_id IN ? \
             AND bucket_time >= fromUnixTimestamp64Milli(?) \
             AND bucket_time < fromUnixTimestamp64Milli(?) \
             AND bucket_time + toIntervalSecond(1) <= fromUnixTimestamp64Milli(?) \
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
                .query(self.pool.as_ref(), sql)
                .bind(tokens.to_vec())
                .bind(from_ms)
                .bind(to_ms)
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

    async fn last_executions(
        &self,
        token_ids: Vec<TokenId>,
        from_ms: i64,
        to_ms: i64,
        limit: u64,
    ) -> Result<Vec<MarketExecutionRow>, StorageError> {
        let token_ids = canonical_values(token_ids);
        if token_ids.is_empty() {
            return Ok(Vec::new());
        }
        let mut rows = Vec::new();
        for tokens in query_chunks(
            &token_ids,
            |token| token.as_str().len(),
            "quant_market_execution",
        )? {
            let page = LAST_EXECUTIONS
                .query(
                    self.pool.as_ref(),
                    &format!(
                        "SELECT ?fields FROM quant_market_execution AS fact \
                         INNER JOIN ({ACTIVE_HISTORY_RANGES}) AS history ON {HISTORY_RANGE_JOIN} \
                         WHERE fact.token_id IN ? \
                         AND fact.effective_at >= fromUnixTimestamp64Milli(?) \
                         AND fact.effective_at < fromUnixTimestamp64Milli(?) \
                         ORDER BY fact.effective_at DESC, fact.block_number DESC, \
                         fact.transaction_index DESC, fact.log_index DESC LIMIT ?"
                    ),
                )
                .bind(tokens.to_vec())
                .bind(from_ms)
                .bind(to_ms)
                .bind(limit)
                .fetch_all::<MarketExecutionRow>()
                .await?;
            extend_rows(&mut rows, page, LAST_EXECUTIONS, "quant_market_execution")?;
        }
        rows.sort_by(|left, right| {
            right
                .effective_at
                .cmp(&left.effective_at)
                .then_with(|| right.block_number.cmp(&left.block_number))
                .then_with(|| right.transaction_index.cmp(&left.transaction_index))
                .then_with(|| right.log_index.cmp(&left.log_index))
                .then_with(|| left.market_id.cmp(&right.market_id))
                .then_with(|| left.token_id.cmp(&right.token_id))
        });
        let limit = usize::try_from(limit).map_err(|error| {
            StorageError::invariant_violation(
                Some("quant_market_execution"),
                format!("last-execution limit is not representable: {error}"),
            )
        })?;
        rows.truncate(limit);
        Ok(rows)
    }

    async fn market_execution_window(
        &self,
        market_ids: Vec<MarketId>,
        history_chunks: Vec<HistorySealChunkRef>,
        from_ms: i64,
        to_ms: i64,
        decision_at_ms: i64,
    ) -> Result<Vec<ExecutionParticipantFactRow>, StorageError> {
        let market_ids = canonical_values(market_ids);
        if market_ids.is_empty() {
            return Ok(Vec::new());
        }
        let history_chunk_ids = self.validate_history_chunks(&history_chunks).await?;
        let mut rows = Vec::new();
        for markets in query_chunks(
            &market_ids,
            |market| market.as_str().len(),
            "quant_market_execution",
        )? {
            for history_ids in query_chunks(
                &history_chunk_ids,
                |_| UUID_INLINE_BYTES,
                "quant_exchange_history_acceptance",
            )? {
                let page = MARKET_EXECUTION_WINDOW
                    .query(
                        self.pool.as_ref(),
                        &format!(
                            "SELECT fact.execution_id AS execution_id, fact.market_id AS market_id, \
                             fact.token_id AS token_id, \
                             participant.participant_address AS participant_address, \
                             participant.participant_role AS participant_role, fact.side AS side, \
                             fact.price AS price, fact.size_shares AS size_shares, \
                             fact.notional_usd AS notional_usd, \
                             fact.transaction_hash AS transaction_hash, \
                             fact.effective_at AS effective_at, fact.observed_at AS observed_at, \
                             fact.model_available_at AS model_available_at, \
                             fact.availability_policy_hash AS availability_policy_hash \
                             FROM quant_market_execution AS fact \
                             INNER JOIN quant_execution_participant AS participant \
                             ON participant.execution_id = fact.execution_id \
                             AND participant.chunk_id = fact.chunk_id \
                             AND participant.market_id = fact.market_id \
                             AND participant.token_id = fact.token_id \
                             AND participant.effective_at = fact.effective_at \
                             AND participant.model_available_at = fact.model_available_at \
                             AND participant.availability_policy_hash = fact.availability_policy_hash \
                             INNER JOIN ({FILTERED_ACTIVE_HISTORY_RANGES}) AS history \
                             ON {HISTORY_RANGE_JOIN} \
                             WHERE fact.market_id IN ? \
                             AND fact.effective_at >= fromUnixTimestamp64Milli(?) \
                             AND fact.effective_at < fromUnixTimestamp64Milli(?) \
                             AND fact.model_available_at <= fromUnixTimestamp64Milli(?) \
                             AND participant.model_available_at <= fromUnixTimestamp64Milli(?) \
                             ORDER BY fact.market_id, fact.effective_at, fact.execution_id, \
                             participant.participant_role"
                        ),
                    )
                    .bind(history_ids.to_vec())
                    .bind(markets.to_vec())
                    .bind(from_ms)
                    .bind(to_ms)
                    .bind(decision_at_ms)
                    .bind(decision_at_ms)
                    .fetch_all::<ExecutionParticipantFactRow>()
                    .await?;
                extend_rows(
                    &mut rows,
                    page,
                    MARKET_EXECUTION_WINDOW,
                    "quant_market_execution",
                )?;
            }
        }
        self.validate_history_chunks(&history_chunks).await?;
        rows.sort_by(|left, right| {
            (
                left.market_id.as_str(),
                left.effective_at,
                left.execution_id,
                left.participant_role as i8,
            )
                .cmp(&(
                    right.market_id.as_str(),
                    right.effective_at,
                    right.execution_id,
                    right.participant_role as i8,
                ))
        });
        Ok(rows)
    }

    async fn market_executions_between(
        &self,
        market_ids: Vec<MarketId>,
        history_chunks: Vec<HistorySealChunkRef>,
        from_ms: i64,
        to_ms: i64,
        decision_at_ms: i64,
    ) -> Result<Vec<MarketExecutionRow>, StorageError> {
        let market_ids = canonical_values(market_ids);
        if market_ids.is_empty() {
            return Ok(Vec::new());
        }
        let history_chunk_ids = self.validate_history_chunks(&history_chunks).await?;
        let mut rows = Vec::new();
        for markets in query_chunks(
            &market_ids,
            |market| market.as_str().len(),
            "quant_market_execution",
        )? {
            for history_ids in query_chunks(
                &history_chunk_ids,
                |_| UUID_INLINE_BYTES,
                "quant_exchange_history_acceptance",
            )? {
                let page = MARKET_EXECUTIONS_BETWEEN
                    .query(
                        self.pool.as_ref(),
                        &format!(
                            "SELECT ?fields FROM quant_market_execution AS fact \
                             INNER JOIN ({FILTERED_ACTIVE_HISTORY_RANGES}) AS history \
                             ON {HISTORY_RANGE_JOIN} \
                             WHERE fact.market_id IN ? \
                             AND fact.effective_at >= fromUnixTimestamp64Milli(?) \
                             AND fact.effective_at < fromUnixTimestamp64Milli(?) \
                             AND fact.model_available_at <= fromUnixTimestamp64Milli(?) \
                             ORDER BY fact.market_id, fact.effective_at, fact.execution_id"
                        ),
                    )
                    .bind(history_ids.to_vec())
                    .bind(markets.to_vec())
                    .bind(from_ms)
                    .bind(to_ms)
                    .bind(decision_at_ms)
                    .fetch_all::<MarketExecutionRow>()
                    .await?;
                extend_rows(
                    &mut rows,
                    page,
                    MARKET_EXECUTIONS_BETWEEN,
                    "quant_market_execution",
                )?;
            }
        }
        self.validate_history_chunks(&history_chunks).await?;
        Ok(rows)
    }

    async fn execution_participants_between(
        &self,
        market_ids: Vec<MarketId>,
        history_chunks: Vec<HistorySealChunkRef>,
        from_ms: i64,
        to_ms: i64,
        decision_at_ms: i64,
    ) -> Result<Vec<ExecutionParticipantRow>, StorageError> {
        let market_ids = canonical_values(market_ids);
        if market_ids.is_empty() {
            return Ok(Vec::new());
        }
        let history_chunk_ids = self.validate_history_chunks(&history_chunks).await?;
        let mut rows = Vec::new();
        for markets in query_chunks(
            &market_ids,
            |market| market.as_str().len(),
            "quant_execution_participant",
        )? {
            for history_ids in query_chunks(
                &history_chunk_ids,
                |_| UUID_INLINE_BYTES,
                "quant_exchange_history_acceptance",
            )? {
                let page = EXECUTION_PARTICIPANTS_BETWEEN
                    .query(
                        self.pool.as_ref(),
                        &format!(
                            "SELECT ?fields FROM (\
                                 SELECT participant_fact.execution_id AS execution_id, \
                                 participant_fact.market_id AS market_id, \
                                 participant_fact.token_id AS token_id, \
                                 participant_fact.participant_address AS participant_address, \
                                 participant_fact.participant_role AS participant_role, \
                                 participant_fact.participant_notional AS participant_notional, \
                                 participant_fact.effective_at AS effective_at, \
                                 participant_fact.model_available_at AS model_available_at, \
                                 participant_fact.availability_policy_hash AS availability_policy_hash, \
                                 participant_fact.chunk_id AS chunk_id, \
                                 participant_fact.schema_version AS schema_version \
                                 FROM quant_execution_participant AS participant_fact \
                                 INNER JOIN quant_market_execution AS fact \
                                 ON fact.execution_id = participant_fact.execution_id \
                                 AND fact.chunk_id = participant_fact.chunk_id \
                                 AND fact.market_id = participant_fact.market_id \
                                 AND fact.token_id = participant_fact.token_id \
                                 AND fact.effective_at = participant_fact.effective_at \
                                 AND fact.model_available_at = participant_fact.model_available_at \
                                 AND fact.availability_policy_hash = participant_fact.availability_policy_hash \
                                 INNER JOIN ({FILTERED_ACTIVE_HISTORY_RANGES}) AS history \
                                 ON {HISTORY_RANGE_JOIN} \
                                 WHERE participant_fact.market_id IN ? \
                                 AND participant_fact.effective_at >= fromUnixTimestamp64Milli(?) \
                                 AND participant_fact.effective_at < fromUnixTimestamp64Milli(?) \
                                 AND participant_fact.model_available_at <= fromUnixTimestamp64Milli(?)\
                             ) AS sealed_participant \
                             ORDER BY sealed_participant.market_id, \
                             sealed_participant.effective_at, \
                             sealed_participant.execution_id, \
                             sealed_participant.participant_role"
                        ),
                    )
                    .bind(history_ids.to_vec())
                    .bind(markets.to_vec())
                    .bind(from_ms)
                    .bind(to_ms)
                    .bind(decision_at_ms)
                    .fetch_all::<ExecutionParticipantRow>()
                    .await?;
                extend_rows(
                    &mut rows,
                    page,
                    EXECUTION_PARTICIPANTS_BETWEEN,
                    "quant_execution_participant",
                )?;
            }
        }
        self.validate_history_chunks(&history_chunks).await?;
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
                    self.pool.as_ref(),
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
                     AND bucket_time + toIntervalSecond(1) <= fromUnixTimestamp64Milli(?) \
                     AND available_at <= fromUnixTimestamp64Milli(?) \
                     GROUP BY token_id, bucket_ms \
                     ORDER BY token_id, bucket_ms",
                )
                .bind(bucket_secs)
                .bind(bucket_secs)
                .bind(tokens.to_vec())
                .bind(from_ms)
                .bind(to_ms)
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
                self.pool.as_ref(),
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
                self.pool.as_ref(),
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
                    self.pool.as_ref(),
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
                    self.pool.as_ref(),
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
                self.pool.as_ref(),
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
                    self.pool.as_ref(),
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
                    self.pool.as_ref(),
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
                    self.pool.as_ref(),
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
                self.pool.as_ref(),
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
                self.pool.as_ref(),
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
                self.pool.as_ref(),
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
                    self.pool.as_ref(),
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
        let rows = OBSERVED_MARKETS_BETWEEN
            .query(
                self.pool.as_ref(),
                "SELECT DISTINCT assumeNotNull(market_id) AS market_id \
                 FROM quant_book_l2_ledger \
                 WHERE market_id IS NOT NULL \
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
                    self.pool.as_ref(),
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
                self.pool.as_ref(),
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

#[derive(clickhouse::Row, serde::Deserialize)]
struct ActiveHistoryContractRow {
    #[serde(with = "clickhouse::serde::uuid")]
    chunk_id: Uuid,
    accepted_frontier: String,
    accepted_from_block: u64,
    accepted_to_block: u64,
    accepted_state_revision: u64,
}

#[cfg(test)]
mod tests {
    use super::{ACTIVE_HISTORY_RANGES, FILTERED_ACTIVE_HISTORY_RANGES, HISTORY_RANGE_JOIN};

    #[test]
    fn history_range_sql_contract() {
        for projection in [ACTIVE_HISTORY_RANGES, FILTERED_ACTIVE_HISTORY_RANGES] {
            assert!(
                projection.contains(
                    "argMax(tuple(frontier, from_block, to_block, active), state_revision)"
                )
            );
            for field in [
                "AS accepted_frontier",
                "AS accepted_from_block",
                "AS accepted_to_block",
            ] {
                assert!(projection.contains(field));
            }
            assert!(projection.contains("max(state_revision) AS accepted_state_revision"));
            assert!(projection.contains(
                "argMax(tuple(frontier, from_block, to_block, active), state_revision), 4"
            ));
            assert!(projection.contains(") = 1"));
        }
        assert!(FILTERED_ACTIVE_HISTORY_RANGES.contains("WHERE chunk_id IN ?"));
        assert!(HISTORY_RANGE_JOIN.contains("history.accepted_chunk_id = fact.chunk_id"));
        assert!(HISTORY_RANGE_JOIN.contains("fact.block_number >= history.accepted_from_block"));
        assert!(HISTORY_RANGE_JOIN.contains("fact.block_number <= history.accepted_to_block"));
    }
}

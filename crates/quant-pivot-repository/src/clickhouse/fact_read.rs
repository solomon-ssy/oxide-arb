//! `ClickHouse`-backed read repository for quant facts (feature window inputs +
//! historical point-in-time state).

use std::sync::Arc;

use async_trait::async_trait;
use quant_pivot_error::storage::StorageError;
use quant_pivot_models::{
    clickhouse::{
        BookMicrostructureRow, BookSnapshotRow, CryptoPriceReportRow, DomainObservationRow,
        MarketResolutionRow, MidPriceBucketRow, TickEventRow, TradeTapeRow,
        WeatherForecastPointRow, WeatherObservationReportRow,
    },
    enums::clickhouse::{ChBookEventType, ChTradeTapeSource},
    types::{DomainInstrumentKey, DomainSourceId, MarketId, TokenId},
};
use quant_pivot_storage::clickhouse::ClickHousePool;

use crate::traits::QuantFactReadRepository;

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

#[async_trait]
impl QuantFactReadRepository for ChQuantFactReadRepository {
    async fn crypto_price_report_at(
        &self,
        source_id: &DomainSourceId,
        instrument_key: &DomainInstrumentKey,
        source_timestamp_ms: i64,
        decision_at_ms: i64,
    ) -> Result<Option<CryptoPriceReportRow>, StorageError> {
        let row = self
            .pool
            .client()
            .query(
                "SELECT ?fields FROM quant_crypto_price_report \
                 WHERE source_id = ? \
                 AND instrument_key = ? \
                 AND ifNull(observations_timestamp, event_time) <= fromUnixTimestamp64Milli(?) \
                 AND available_at <= fromUnixTimestamp64Milli(?) \
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
        if instrument_keys.is_empty() {
            return Ok(Vec::new());
        }
        self.pool
            .client()
            .query(
                "SELECT ?fields FROM quant_crypto_price_report \
                 WHERE instrument_key IN ? \
                 AND event_time >= fromUnixTimestamp64Milli(?) \
                 AND event_time < fromUnixTimestamp64Milli(?) \
                 AND published_at <= fromUnixTimestamp64Milli(?) \
                 AND available_at <= fromUnixTimestamp64Milli(?) \
                 ORDER BY instrument_key, event_time, available_at, source_sequence, report_hash",
            )
            .bind(instrument_keys)
            .bind(from_ms)
            .bind(to_ms)
            .bind(publish_cutoff_ms)
            .bind(decision_at_ms)
            .fetch_all::<CryptoPriceReportRow>()
            .await
            .map_err(StorageError::from)
    }

    async fn weather_observation_reports_between(
        &self,
        stations: Vec<String>,
        from_ms: i64,
        to_ms: i64,
        publish_cutoff_ms: i64,
        decision_at_ms: i64,
    ) -> Result<Vec<WeatherObservationReportRow>, StorageError> {
        if stations.is_empty() {
            return Ok(Vec::new());
        }
        self.pool
            .client()
            .query(
                "SELECT ?fields FROM quant_weather_observation_report \
                 WHERE station IN ? \
                 AND observation_time >= fromUnixTimestamp64Milli(?) \
                 AND observation_time < fromUnixTimestamp64Milli(?) \
                 AND published_at <= fromUnixTimestamp64Milli(?) \
                 AND available_at <= fromUnixTimestamp64Milli(?) \
                 ORDER BY station, local_date, observation_time, revision, available_at, report_hash",
            )
            .bind(stations)
            .bind(from_ms)
            .bind(to_ms)
            .bind(publish_cutoff_ms)
            .bind(decision_at_ms)
            .fetch_all::<WeatherObservationReportRow>()
            .await
            .map_err(StorageError::from)
    }

    async fn weather_forecast_points_between(
        &self,
        stations: Vec<String>,
        valid_from_ms: i64,
        valid_to_ms: i64,
        reference_cutoff_ms: i64,
        decision_at_ms: i64,
    ) -> Result<Vec<WeatherForecastPointRow>, StorageError> {
        if stations.is_empty() {
            return Ok(Vec::new());
        }
        self.pool
            .client()
            .query(
                "SELECT ?fields FROM quant_weather_forecast_point \
                 WHERE station IN ? \
                 AND valid_time >= fromUnixTimestamp64Milli(?) \
                 AND valid_time < fromUnixTimestamp64Milli(?) \
                 AND reference_time <= fromUnixTimestamp64Milli(?) \
                 AND available_at <= fromUnixTimestamp64Milli(?) \
                 ORDER BY station, reference_time, valid_time, member, available_at, run_manifest_hash",
            )
            .bind(stations)
            .bind(valid_from_ms)
            .bind(valid_to_ms)
            .bind(reference_cutoff_ms)
            .bind(decision_at_ms)
            .fetch_all::<WeatherForecastPointRow>()
            .await
            .map_err(StorageError::from)
    }

    async fn microstructure_window(
        &self,
        token_ids: Vec<TokenId>,
        from_ms: i64,
        to_ms: i64,
        decision_at_ms: i64,
    ) -> Result<Vec<BookMicrostructureRow>, StorageError> {
        if token_ids.is_empty() {
            return Ok(Vec::new());
        }
        let rows = self
            .pool
            .client()
            .query(
                "SELECT ?fields FROM book_microstructure_1s \
                 WHERE token_id IN ? \
                 AND bucket_time >= fromUnixTimestamp64Milli(?) \
                 AND bucket_time < fromUnixTimestamp64Milli(?) \
                 AND available_at <= fromUnixTimestamp64Milli(?) \
                 ORDER BY token_id, bucket_time",
            )
            .bind(token_ids)
            .bind(from_ms)
            .bind(to_ms)
            .bind(decision_at_ms)
            .fetch_all::<BookMicrostructureRow>()
            .await?;
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
        let rows = self
            .pool
            .client()
            .query(sql)
            .bind(token_ids)
            .bind(from_ms)
            .bind(to_ms)
            .bind(available_by_ms)
            .fetch_all::<BookMicrostructureRow>()
            .await?;
        Ok(rows)
    }

    async fn last_trades(
        &self,
        token_ids: Vec<TokenId>,
        from_ms: i64,
        to_ms: i64,
        limit: u64,
    ) -> Result<Vec<TickEventRow>, StorageError> {
        if token_ids.is_empty() {
            return Ok(Vec::new());
        }
        let rows = self
            .pool
            .client()
            .query(
                "SELECT ?fields FROM tick_events \
                 WHERE token_id IN ? \
                 AND event_type = ? \
                 AND last_trade_price IS NOT NULL \
                 AND event_time >= fromUnixTimestamp64Milli(?) \
                 AND event_time < fromUnixTimestamp64Milli(?) \
                 ORDER BY event_time DESC, ingestion_time DESC, sequence DESC \
                 LIMIT ?",
            )
            .bind(token_ids)
            .bind(ChBookEventType::LastTrade)
            .bind(from_ms)
            .bind(to_ms)
            .bind(limit)
            .fetch_all::<TickEventRow>()
            .await?;
        Ok(rows)
    }

    async fn trade_tape_window_by_market(
        &self,
        market_ids: Vec<MarketId>,
        from_ms: i64,
        to_ms: i64,
        decision_at_ms: i64,
    ) -> Result<Vec<TradeTapeRow>, StorageError> {
        if market_ids.is_empty() {
            return Ok(Vec::new());
        }
        let mut rows = self
            .pool
            .client()
            .query(
                "SELECT ?fields FROM quant_trade_tape \
                 WHERE market_id IN ? \
                 AND source = ? \
                 AND event_time >= fromUnixTimestamp64Milli(?) \
                 AND event_time < fromUnixTimestamp64Milli(?) \
                 AND ingestion_time <= fromUnixTimestamp64Milli(?) \
                 ORDER BY ingestion_time DESC, \
                 cityHash64(tuple(side, price, size_shares, notional_usd, \
                     ifNull(tx_hash, ''), source, coverage_flags, \
                     ifNull(raw_payload_json, ''), schema_version)) DESC \
                 LIMIT 1 BY market_id, token_id, participant_role, event_time, trade_id, participant_address",
            )
            .bind(market_ids)
            .bind(ChTradeTapeSource::OnChain)
            .bind(from_ms)
            .bind(to_ms)
            .bind(decision_at_ms)
            .fetch_all::<TradeTapeRow>()
            .await?;
        rows.sort_by(|left, right| {
            (
                left.market_id.as_str(),
                left.event_time,
                left.ingestion_time,
                left.trade_id.as_str(),
                left.participant_role as i8,
            )
                .cmp(&(
                    right.market_id.as_str(),
                    right.event_time,
                    right.ingestion_time,
                    right.trade_id.as_str(),
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
        if token_ids.is_empty() {
            return Ok(Vec::new());
        }
        if bucket_secs == 0 {
            return Err(StorageError::invariant_violation(
                Some("book_microstructure_1s"),
                "mid-price bucket_secs must be greater than zero",
            ));
        }
        let rows = self
            .pool
            .client()
            .query(
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
            .bind(token_ids)
            .bind(from_ms)
            .bind(to_ms)
            .bind(decision_at_ms)
            .fetch_all::<MidPriceBucketRow>()
            .await?;
        Ok(rows)
    }

    async fn book_snapshot_at(
        &self,
        token_id: &TokenId,
        source_cutoff_ms: i64,
        decision_at_ms: i64,
    ) -> Result<Option<BookSnapshotRow>, StorageError> {
        let rows = self
            .pool
            .client()
            .query(
                "SELECT ?fields FROM book_snapshots \
                 WHERE token_id = ? \
                 AND event_time <= fromUnixTimestamp64Milli(?) \
                 AND ingestion_time <= fromUnixTimestamp64Milli(?) \
                 ORDER BY event_time DESC, ingestion_time DESC, sequence DESC \
                 LIMIT 1",
            )
            .bind(token_id.clone())
            .bind(source_cutoff_ms)
            .bind(decision_at_ms)
            .fetch_all::<BookSnapshotRow>()
            .await?;
        Ok(rows.into_iter().next())
    }

    async fn book_snapshots_at(
        &self,
        token_ids: Vec<TokenId>,
        source_cutoff_ms: i64,
        decision_at_ms: i64,
    ) -> Result<Vec<BookSnapshotRow>, StorageError> {
        if token_ids.is_empty() {
            return Ok(Vec::new());
        }
        let rows = self
            .pool
            .client()
            .query(
                "SELECT ?fields FROM book_snapshots \
                 WHERE token_id IN ? \
                 AND event_time <= fromUnixTimestamp64Milli(?) \
                 AND ingestion_time <= fromUnixTimestamp64Milli(?) \
                 ORDER BY token_id, event_time DESC, ingestion_time DESC, sequence DESC \
                 LIMIT 1 BY token_id",
            )
            .bind(token_ids)
            .bind(source_cutoff_ms)
            .bind(decision_at_ms)
            .fetch_all::<BookSnapshotRow>()
            .await?;
        Ok(rows)
    }

    async fn book_snapshots_between(
        &self,
        token_ids: Vec<TokenId>,
        from_ms: i64,
        to_ms: i64,
        available_by_ms: i64,
    ) -> Result<Vec<BookSnapshotRow>, StorageError> {
        if token_ids.is_empty() {
            return Ok(Vec::new());
        }
        let rows = self
            .pool
            .client()
            .query(
                "SELECT ?fields FROM book_snapshots \
                 WHERE token_id IN ? \
                 AND event_time >= fromUnixTimestamp64Milli(?) \
                 AND event_time <= fromUnixTimestamp64Milli(?) \
                 AND ingestion_time <= fromUnixTimestamp64Milli(?) \
                 ORDER BY token_id, event_time, ingestion_time, sequence",
            )
            .bind(token_ids)
            .bind(from_ms)
            .bind(to_ms)
            .bind(available_by_ms)
            .fetch_all::<BookSnapshotRow>()
            .await?;
        Ok(rows)
    }

    async fn resolution_at(
        &self,
        market_id: &MarketId,
        source_cutoff_ms: i64,
        decision_at_ms: i64,
    ) -> Result<Option<MarketResolutionRow>, StorageError> {
        let rows = self
            .pool
            .client()
            .query(
                "SELECT ?fields FROM market_resolution_event \
                 WHERE market_id = ? \
                 AND resolved_at <= fromUnixTimestamp64Milli(?) \
                 AND observed_at <= fromUnixTimestamp64Milli(?) \
                 ORDER BY resolved_at DESC, observed_at DESC, sequence DESC \
                 LIMIT 1",
            )
            .bind(market_id.clone())
            .bind(source_cutoff_ms)
            .bind(decision_at_ms)
            .fetch_all::<MarketResolutionRow>()
            .await?;
        Ok(rows.into_iter().next())
    }

    async fn resolutions_between(
        &self,
        market_ids: Vec<MarketId>,
        from_ms: i64,
        to_ms: i64,
        decision_at_ms: i64,
    ) -> Result<Vec<MarketResolutionRow>, StorageError> {
        if market_ids.is_empty() {
            return Ok(Vec::new());
        }
        let rows = self
            .pool
            .client()
            .query(
                "SELECT ?fields FROM market_resolution_event \
                 WHERE market_id IN ? \
                 AND resolved_at >= fromUnixTimestamp64Milli(?) \
                 AND resolved_at <= fromUnixTimestamp64Milli(?) \
                 AND observed_at <= fromUnixTimestamp64Milli(?) \
                 ORDER BY market_id, resolved_at, observed_at, sequence",
            )
            .bind(market_ids)
            .bind(from_ms)
            .bind(to_ms)
            .bind(decision_at_ms)
            .fetch_all::<MarketResolutionRow>()
            .await?;
        Ok(rows)
    }

    async fn observed_markets_between(
        &self,
        from_ms: i64,
        to_ms: i64,
        decision_at_ms: i64,
    ) -> Result<Vec<MarketId>, StorageError> {
        // `market_id` is Nullable in `book_snapshots`; `assumeNotNull` after the
        // `IS NOT NULL` guard yields a non-nullable column the row can decode.
        let rows = self
            .pool
            .client()
            .query(
                "SELECT DISTINCT assumeNotNull(market_id) AS market_id FROM book_snapshots \
                 WHERE market_id IS NOT NULL \
                 AND event_time >= fromUnixTimestamp64Milli(?) \
                 AND event_time <= fromUnixTimestamp64Milli(?) \
                 AND ingestion_time <= fromUnixTimestamp64Milli(?) \
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
        if instrument_keys.is_empty() {
            return Ok(Vec::new());
        }
        let mut rows = self
            .pool
            .client()
            .query(
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
            .bind(instrument_keys)
            .bind(from_ms)
            .bind(to_ms)
            .bind(publish_cutoff_ms)
            .bind(decision_at_ms)
            .fetch_all::<DomainObservationRow>()
            .await?;
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
        let row = self
            .pool
            .client()
            .query(
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

/// Single-column projection for [`ChQuantFactReadRepository::observed_markets_between`].
#[derive(clickhouse::Row, serde::Deserialize)]
struct ObservedMarketRow {
    market_id: MarketId,
}

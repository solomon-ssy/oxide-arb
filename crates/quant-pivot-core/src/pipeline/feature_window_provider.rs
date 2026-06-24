//! Online feature-window provider.
//!
//! Once per research round, this pre-fetches the trailing microstructure window
//! for the entire selected-market set in a single `ClickHouse` read and decodes
//! it into source-agnostic [`MarketWindowSnapshot`]s. Feature builders then read
//! these from memory — the build loop never touches the database, and every
//! bucket is bounded by the PIT cutoff (`as_of - source_delay`), so no
//! look-ahead is possible.
//!
//! The historical (`as_of`-bounded, replay) provider for backtests and training
//! datasets is deferred to 3.5; this online provider reads only recent facts.

use std::{collections::HashMap, sync::Arc, time::Duration};

use chrono::{DateTime, Duration as ChronoDuration, TimeZone, Utc};
use quant_pivot_error::QuantResult;
use quant_pivot_models::{
    clickhouse::{BookMicrostructureRow, ChBps, ChDecimal64, ChPrice, ChUsd},
    types::TokenId,
};
use quant_pivot_repository::traits::QuantFactReadRepository;
use quant_pivot_research::{
    features::{MarketWindowSnapshot, MicrostructureBucket},
    selection::SelectedMarket,
};

/// Pre-fetches and decodes microstructure windows for a selected-market set.
pub struct FeatureWindowProvider {
    fact_read: Arc<dyn QuantFactReadRepository>,
}

impl FeatureWindowProvider {
    /// Build a provider over a quant-fact read repository.
    #[must_use]
    pub fn new(fact_read: Arc<dyn QuantFactReadRepository>) -> Self {
        Self { fact_read }
    }

    /// Load a per-token window for every selected market.
    ///
    /// The returned map is keyed by primary token id; markets with no facts in
    /// the window get an empty (but PIT-correct) snapshot, never a missing entry.
    ///
    /// # Errors
    ///
    /// Propagates `ClickHouse` read failures as a storage error.
    pub async fn load_windows(
        &self,
        markets: &[SelectedMarket],
        as_of: DateTime<Utc>,
        lookback: Duration,
        source_delay: Duration,
    ) -> QuantResult<HashMap<TokenId, MarketWindowSnapshot>> {
        let token_ids: Vec<TokenId> = markets
            .iter()
            .map(|market| market.primary_token_id.clone())
            .collect();
        if token_ids.is_empty() {
            return Ok(HashMap::new());
        }

        let cutoff = as_of - to_chrono(source_delay);
        let from = cutoff - to_chrono(lookback);
        let rows = self
            .fact_read
            .microstructure_window(
                token_ids,
                from.timestamp_millis(),
                cutoff.timestamp_millis(),
            )
            .await?;

        let mut grouped: HashMap<TokenId, Vec<MicrostructureBucket>> = HashMap::new();
        for row in rows {
            grouped
                .entry(row.token_id.clone())
                .or_default()
                .push(decode_bucket(&row, as_of));
        }

        let mut windows = HashMap::with_capacity(markets.len());
        for market in markets {
            let token = market.primary_token_id.clone();
            let buckets = grouped.remove(&token).unwrap_or_default();
            windows.insert(
                token.clone(),
                MarketWindowSnapshot {
                    token_id: token,
                    as_of,
                    source_delay,
                    buckets,
                },
            );
        }
        Ok(windows)
    }
}

/// Convert a `std::time::Duration` to a `chrono::Duration`, saturating to zero.
fn to_chrono(duration: Duration) -> ChronoDuration {
    ChronoDuration::from_std(duration).unwrap_or_else(|_| ChronoDuration::zero())
}

/// Convert epoch milliseconds to a UTC instant, falling back to `default`.
fn millis_to_utc(timestamp_ms: i64, default: DateTime<Utc>) -> DateTime<Utc> {
    Utc.timestamp_millis_opt(timestamp_ms)
        .single()
        .unwrap_or(default)
}

/// Decode a `ClickHouse` microstructure row into a compute-domain bucket.
fn decode_bucket(row: &BookMicrostructureRow, default_time: DateTime<Utc>) -> MicrostructureBucket {
    MicrostructureBucket {
        bucket_time: millis_to_utc(row.bucket_time, default_time),
        mid_close: row.mid_price_close.map(ChPrice::to_price),
        spread_bps_avg: row.spread_bps_avg.map(ChBps::to_bps),
        top1_depth_usd_avg: row.top1_depth_usd_avg.map(ChUsd::to_usd),
        top5_depth_usd_avg: row.top5_depth_usd_avg.map(ChUsd::to_usd),
        imbalance_avg: row.imbalance_avg.map(ChDecimal64::to_decimal),
        update_count: row.update_count,
        snapshot_count: row.snapshot_count,
        delta_count: row.delta_count,
        crossed_count: row.crossed_count,
        gap_count: row.gap_count,
        max_book_age_ms: row.max_book_age_ms,
    }
}

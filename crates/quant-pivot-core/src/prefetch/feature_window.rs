//! Online feature-window provider.
//!
//! Once per research round, this pre-fetches the trailing microstructure window
//! for the entire selected-market set in a single `ClickHouse` read and decodes
//! it into source-agnostic [`MarketWindowSnapshot`]s. Feature builders then read
//! these from memory — the build loop never touches the database, and every
//! bucket is bounded by the source cutoff frozen in [`DecisionBoundary`], so no
//! look-ahead is possible.
//!
//! The historical (`as_of`-bounded, replay) provider for backtests and training
//! datasets is deferred to 3.5; this online provider reads only recent facts.

use std::{collections::HashMap, sync::Arc, time::Duration};

use chrono::{DateTime, Duration as ChronoDuration, TimeZone, Utc};
use quant_pivot_error::{QuantError, QuantResult, research::ResearchError};
use quant_pivot_models::{
    clickhouse::{BookMicrostructureRow, ChBps, ChDecimal64, ChPrice, ChUsd},
    domain::{DecisionBoundary, DecisionSource, DomainObservation, TradeTapePrint},
    types::{DomainInstrumentKey, MarketId, TokenId},
};
use quant_pivot_repository::traits::QuantFactReadRepository;
use quant_pivot_research::{
    features::TradeTapeWindowSnapshot,
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
        boundary: &DecisionBoundary,
        lookback: Duration,
    ) -> QuantResult<HashMap<TokenId, MarketWindowSnapshot>> {
        let token_ids: Vec<TokenId> = markets
            .iter()
            .map(|market| market.primary_token_id.clone())
            .collect();
        if token_ids.is_empty() {
            return Ok(HashMap::new());
        }

        let cutoff = boundary.cutoff_for(DecisionSource::Microstructure);
        let from = window_start(cutoff, lookback, "microstructure lookback")?;
        let rows = self
            .fact_read
            .microstructure_window(
                token_ids,
                from.timestamp_millis(),
                cutoff.timestamp_millis(),
                boundary.decision_at().timestamp_millis(),
            )
            .await?;

        let mut grouped: HashMap<TokenId, Vec<MicrostructureBucket>> = HashMap::new();
        for row in rows {
            let bucket = decode_bucket(&row)?;
            if bucket.bucket_time < from || bucket.bucket_time > cutoff {
                return Err(ResearchError::PitResolution {
                    detail: format!(
                        "microstructure row for token {} at {} is outside [{from}, {cutoff}]",
                        row.token_id, bucket.bucket_time
                    ),
                }
                .into());
            }
            if bucket.available_at > boundary.decision_at() {
                return Err(ResearchError::PitResolution {
                    detail: format!(
                        "microstructure row for token {} available at {} is after decision {}",
                        row.token_id,
                        bucket.available_at,
                        boundary.decision_at()
                    ),
                }
                .into());
            }
            grouped
                .entry(row.token_id.clone())
                .or_default()
                .push(bucket);
        }

        let mut windows = HashMap::with_capacity(markets.len());
        for market in markets {
            let token = market.primary_token_id.clone();
            let buckets = grouped.remove(&token).unwrap_or_default();
            windows.insert(
                token.clone(),
                MarketWindowSnapshot {
                    token_id: token,
                    decision_at: boundary.decision_at(),
                    knowledge_cutoff: cutoff,
                    buckets,
                },
            );
        }
        Ok(windows)
    }

    /// Load PIT-bounded domain observations for a set of external instruments
    /// (Phase 11.2.2). Series are ascending by `observed_at`, all inside
    /// `[source_cutoff - lookback, source_cutoff]` and available by `decision_at`.
    ///
    /// # Errors
    ///
    /// Propagates `ClickHouse` read failures as a storage error.
    pub async fn load_domain_observations(
        &self,
        instruments: Vec<DomainInstrumentKey>,
        boundary: &DecisionBoundary,
        lookback: Duration,
    ) -> QuantResult<HashMap<DomainInstrumentKey, Vec<DomainObservation>>> {
        if instruments.is_empty() {
            return Ok(HashMap::new());
        }
        let cutoff = boundary.cutoff_for(DecisionSource::DomainCrypto);
        let from = window_start(cutoff, lookback, "domain lookback")?;
        let rows = self
            .fact_read
            .domain_observations_between(
                instruments,
                from.timestamp_millis(),
                // `[from, to)` — include the cutoff instant itself.
                cutoff
                    .timestamp_millis()
                    .checked_add(1)
                    .ok_or_else(|| QuantError::config("domain cutoff overflowed i64"))?,
                cutoff.timestamp_millis(),
                boundary.decision_at().timestamp_millis(),
            )
            .await?;
        let mut grouped: HashMap<DomainInstrumentKey, Vec<DomainObservation>> = HashMap::new();
        for row in rows {
            let observation = DomainObservation::from_clickhouse_row(&row).ok_or_else(|| {
                ResearchError::PitResolution {
                    detail: format!(
                        "domain observation {} / {} at {} cannot be decoded",
                        row.instrument_key, row.metric, row.event_time
                    ),
                }
            })?;
            if observation.observed_at < from
                || observation.observed_at > cutoff
                || observation.publish_time > cutoff
                || observation
                    .available_at
                    .is_none_or(|available_at| available_at > boundary.decision_at())
            {
                return Err(ResearchError::PitResolution {
                    detail: format!(
                        "domain observation {} at {} (published {}) is outside PIT window [{from}, {cutoff}]",
                        observation.instrument_key,
                        observation.observed_at,
                        observation.publish_time
                    ),
                }
                .into());
            }
            grouped
                .entry(observation.instrument_key.clone())
                .or_default()
                .push(observation);
        }
        Ok(grouped)
    }

    /// Load a per-market trade-tape window for every selected market.
    ///
    /// Empty snapshots are marked source-available because the `ClickHouse` read
    /// completed; a disabled/unavailable source is represented by callers using
    /// [`TradeTapeWindowSnapshot::empty`].
    pub async fn load_trade_tape_windows(
        &self,
        markets: &[SelectedMarket],
        boundary: &DecisionBoundary,
        lookback: Duration,
    ) -> QuantResult<HashMap<MarketId, TradeTapeWindowSnapshot>> {
        let market_ids: Vec<MarketId> = markets
            .iter()
            .map(|market| market.market_id.clone())
            .collect();
        if market_ids.is_empty() {
            return Ok(HashMap::new());
        }

        let cutoff = boundary.cutoff_for(DecisionSource::TradeTape);
        let from = window_start(cutoff, lookback, "trade-tape lookback")?;
        let rows = self
            .fact_read
            .trade_tape_window_by_market(
                market_ids,
                from.timestamp_millis(),
                cutoff.timestamp_millis(),
                boundary.decision_at().timestamp_millis(),
            )
            .await?;

        let mut grouped: HashMap<MarketId, Vec<TradeTapePrint>> = HashMap::new();
        for row in rows {
            let event_time = timestamp_millis(row.event_time, "trade-tape event_time")?;
            let available_at = timestamp_millis(row.ingestion_time, "trade-tape ingestion_time")?;
            if event_time < from || event_time >= cutoff {
                return Err(ResearchError::PitResolution {
                    detail: format!(
                        "trade-tape row for market {} at {event_time} is outside [{from}, {cutoff})",
                        row.market_id
                    ),
                }
                .into());
            }
            if available_at > boundary.decision_at() {
                return Err(ResearchError::PitResolution {
                    detail: format!(
                        "trade-tape row for market {} available at {available_at} is after decision {}",
                        row.market_id,
                        boundary.decision_at()
                    ),
                }
                .into());
            }
            grouped.entry(row.market_id.clone()).or_default().push(
                TradeTapePrint::from_clickhouse_row_at(&row, event_time, available_at),
            );
        }

        let mut windows = HashMap::with_capacity(markets.len());
        for market in markets {
            let market_id = market.market_id.clone();
            let prints = grouped.remove(&market_id).unwrap_or_default();
            windows.insert(
                market_id.clone(),
                TradeTapeWindowSnapshot::available(
                    market_id,
                    boundary.decision_at(),
                    cutoff,
                    prints,
                ),
            );
        }
        Ok(windows)
    }
}

/// Convert a `std::time::Duration` to a `chrono::Duration`, failing closed.
fn to_chrono(duration: Duration, field: &'static str) -> QuantResult<ChronoDuration> {
    ChronoDuration::from_std(duration)
        .map_err(|error| QuantError::config(format!("{field} is outside chrono range: {error}")))
}

fn window_start(
    cutoff: DateTime<Utc>,
    lookback: Duration,
    field: &'static str,
) -> QuantResult<DateTime<Utc>> {
    cutoff
        .checked_sub_signed(to_chrono(lookback, field)?)
        .ok_or_else(|| QuantError::config(format!("{field} start is outside chrono range")))
}

fn timestamp_millis(timestamp_ms: i64, field: &'static str) -> QuantResult<DateTime<Utc>> {
    Utc.timestamp_millis_opt(timestamp_ms)
        .single()
        .ok_or_else(|| {
            ResearchError::PitResolution {
                detail: format!("{field} {timestamp_ms} is outside chrono range"),
            }
            .into()
        })
}

/// Decode a `ClickHouse` microstructure row into a compute-domain bucket.
fn decode_bucket(row: &BookMicrostructureRow) -> QuantResult<MicrostructureBucket> {
    Ok(MicrostructureBucket {
        bucket_time: timestamp_millis(row.bucket_time, "microstructure bucket_time")?,
        available_at: timestamp_millis(row.available_at, "microstructure available_at")?,
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
    })
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use chrono::{Duration as ChronoDuration, TimeZone, Utc};

    use quant_pivot_models::domain::DecisionClock;

    use super::window_start;

    #[test]
    fn report_lag_is_applied_once_to_window_end() {
        let decision_at = Utc.with_ymd_and_hms(2026, 7, 10, 12, 0, 0).unwrap();
        let boundary = DecisionClock::new(120)
            .boundary(decision_at)
            .expect("decision boundary");
        let cutoff = boundary.knowledge_cutoff();

        assert_eq!(cutoff, decision_at - ChronoDuration::seconds(120));
        assert_eq!(
            window_start(cutoff, Duration::from_hours(1), "test lookback").expect("window start"),
            decision_at - ChronoDuration::seconds(3_720)
        );
    }
}

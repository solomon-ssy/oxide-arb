//! Deterministic sample planning.
//!
//! `plan_samples` is a pure function: the same `(request, markets)` always yields
//! the same ordered sample set, which is what makes `dataset_hash` reproducible.

use chrono::{DateTime, Duration, Utc};

use super::{DatasetPlanRequest, ExitTrainingLotRow, LotSamplePlan, PlanMarket, SamplePlan};

/// Generate the deterministic `(market, token, as_of)` sampling grid.
///
/// For each market (iterated in `market_id` order) the window is walked from
/// `window_start` in `sample_interval_secs` steps up to (but excluding)
/// `window_end`. A sample is emitted only while the market is alive at that
/// instant: `as_of >= created_at` (the market exists) and, when an `end_date` is
/// known, `as_of < end_date`. The result is ordered by `(market_id, as_of)`.
#[must_use]
pub fn plan_samples(request: &DatasetPlanRequest, markets: &[PlanMarket]) -> Vec<SamplePlan> {
    let interval_secs = i64::try_from(request.sample_interval_secs.max(1)).unwrap_or(i64::MAX);
    let step = Duration::seconds(interval_secs);

    let mut ordered: Vec<&PlanMarket> = markets.iter().collect();
    ordered.sort_by(|a, b| a.market_id.as_str().cmp(b.market_id.as_str()));

    let mut samples = Vec::new();
    for market in ordered {
        let mut as_of = request.window_start;
        while as_of < request.window_end {
            let alive = as_of >= market.created_at && market.end_date.is_none_or(|end| as_of < end);
            if alive {
                samples.push(SamplePlan {
                    market_id: market.market_id.clone(),
                    token_id: market.token_id.clone(),
                    as_of,
                });
            }
            as_of += step;
        }
    }
    samples
}

/// Count the deterministic `(market, token, as_of)` samples **without**
/// materializing the grid, iterating the identical instants as [`plan_samples`].
///
/// This is the cheap dry-run count path: a full window × selection grid can be
/// millions of rows, and allocating that just to report a count is what made the
/// synchronous `plan` endpoint time out. Iteration stops early once `cap` is
/// exceeded (the caller flags `hard_cap_exceeded`), bounding pathological
/// window/interval combinations to `~cap` arithmetic steps.
#[must_use]
pub fn count_samples(request: &DatasetPlanRequest, markets: &[PlanMarket], cap: u64) -> u64 {
    let interval_secs = i64::try_from(request.sample_interval_secs.max(1)).unwrap_or(i64::MAX);
    let step = Duration::seconds(interval_secs);
    let mut total: u64 = 0;
    for market in markets {
        let mut as_of = request.window_start;
        while as_of < request.window_end {
            let alive = as_of >= market.created_at && market.end_date.is_none_or(|end| as_of < end);
            if alive {
                total += 1;
                if total > cap {
                    return total;
                }
            }
            as_of += step;
        }
    }
    total
}

/// Generate hold-vs-exit decision instants along each closed lot's life.
///
/// Decision instants start at `max(opened_at, window_start)`: ticks before the
/// dataset window have no prefetched book/microstructure facts, so emitting them
/// would only produce dropped rows. The lot's true `opened_at`/`closed_at` are
/// still carried on each plan for point-in-time position-state computation.
#[must_use]
pub fn plan_lot_timeline_samples(
    interval_secs: u64,
    window_start: DateTime<Utc>,
    lots: &[ExitTrainingLotRow],
) -> Vec<LotSamplePlan> {
    let interval_secs = i64::try_from(interval_secs.max(1)).unwrap_or(i64::MAX);
    let step = Duration::seconds(interval_secs);

    let mut ordered: Vec<&ExitTrainingLotRow> = lots.iter().collect();
    ordered.sort_by(|a, b| {
        (a.order_intent_id.to_string(), a.opened_at, a.closed_at).cmp(&(
            b.order_intent_id.to_string(),
            b.opened_at,
            b.closed_at,
        ))
    });

    let mut samples = Vec::new();
    for lot in ordered {
        let mut as_of = lot.opened_at.max(window_start);
        while as_of < lot.closed_at {
            samples.push(LotSamplePlan {
                order_intent_id: lot.order_intent_id.clone(),
                position_id: lot.position_id.clone(),
                market_id: lot.market_id.clone(),
                token_id: lot.token_id.clone(),
                as_of,
                opened_at: lot.opened_at,
                closed_at: lot.closed_at,
            });
            as_of += step;
        }
    }
    samples
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::{Duration, TimeZone, Utc};
    use quant_pivot_models::{
        enums::quant::DatasetPurpose,
        types::{
            MarketId, ModelSpecId, OrderIntentId, PositionId, Price, RuntimeConfigVersionId,
            SchemaVersion, Shares, TokenId, Usd, default_sample_sources,
        },
    };
    use rust_decimal::Decimal;

    fn request(interval_secs: u64) -> DatasetPlanRequest {
        let start = Utc.timestamp_opt(1_000_000, 0).single().expect("ts");
        DatasetPlanRequest {
            model_spec_id: ModelSpecId::from_v7(),
            runtime_config_version_id: RuntimeConfigVersionId::from_v7(),
            window_start: start,
            window_end: start + Duration::seconds(300),
            sample_interval_secs: interval_secs,
            horizons_secs: vec![60],
            source_delay_secs: 10,
            feature_schema_version: SchemaVersion::FIRST,
            sample_sources: default_sample_sources(),
            training_dataset_id: None,
            purpose: DatasetPurpose::default(),
        }
    }

    fn market(id: &str, created_offset: i64, end_offset: Option<i64>) -> PlanMarket {
        let start = Utc.timestamp_opt(1_000_000, 0).single().expect("ts");
        PlanMarket {
            market_id: MarketId::new(id),
            token_id: TokenId::new(format!("{id}-yes")),
            created_at: start + Duration::seconds(created_offset),
            end_date: end_offset.map(|o| start + Duration::seconds(o)),
        }
    }

    #[test]
    fn plan_samples_is_deterministic_and_ordered() {
        let req = request(60);
        let markets = vec![market("zzz", -100, None), market("aaa", -100, None)];
        let first = plan_samples(&req, &markets);
        let shuffled = vec![market("aaa", -100, None), market("zzz", -100, None)];
        let second = plan_samples(&req, &shuffled);
        assert_eq!(
            first, second,
            "same inputs must yield identical sample sets"
        );
        // 2 markets × 5 instants (0,60,120,180,240) = 10.
        assert_eq!(first.len(), 10);
        // Ordered by (market_id, as_of): aaa before zzz.
        assert_eq!(first[0].market_id.as_str(), "aaa");
        assert!(first[0].as_of < first[1].as_of);
    }

    fn exit_lot(opened_offset: i64, closed_offset: i64) -> ExitTrainingLotRow {
        let start = Utc.timestamp_opt(1_000_000, 0).single().expect("ts");
        ExitTrainingLotRow {
            order_intent_id: OrderIntentId::from_v7(),
            position_id: PositionId::from_v7(),
            market_id: MarketId::new("0xmkt"),
            token_id: TokenId::new("token-1"),
            opened_at: start + Duration::seconds(opened_offset),
            closed_at: start + Duration::seconds(closed_offset),
            entry_shares: Shares::new(Decimal::from(100)),
            avg_price: Price::new(Decimal::new(5, 1)),
            peak_mark_price: None,
            max_hold_secs: 86_400,
            total_net_proceeds: Usd::new(Decimal::from(60)),
            exit_events: Vec::new(),
        }
    }

    #[test]
    fn lot_timeline_clamps_to_window_start() {
        let start = Utc.timestamp_opt(1_000_000, 0).single().expect("ts");
        // Lot opened 100s before the window; closes 120s into it.
        let lots = vec![exit_lot(-100, 120)];
        let samples = plan_lot_timeline_samples(60, start, &lots);
        // Ticks start at window_start (0), not opened_at (-100): 0, 60 (< 120).
        assert_eq!(samples.len(), 2);
        assert_eq!(samples[0].as_of, start);
        assert_eq!(samples[1].as_of, start + Duration::seconds(60));
        // The true lifetime is still carried for position-state computation.
        assert_eq!(samples[0].opened_at, start - Duration::seconds(100));
    }

    #[test]
    fn count_samples_matches_plan_samples_len() {
        let req = request(60);
        let markets = vec![market("aaa", -100, None), market("m", 120, Some(240))];
        let expected = plan_samples(&req, &markets).len() as u64;
        assert_eq!(count_samples(&req, &markets, u64::MAX), expected);
    }

    #[test]
    fn count_samples_stops_past_cap() {
        let req = request(60);
        // Single market alive for all 5 window instants; cap of 2 must short-circuit.
        let markets = vec![market("aaa", -100, None)];
        let counted = count_samples(&req, &markets, 2);
        assert!(counted > 2, "count must exceed the cap to flag overflow");
        assert!(
            counted <= 3,
            "count must stop shortly after exceeding the cap"
        );
    }

    #[test]
    fn plan_samples_respects_market_lifetime() {
        let req = request(60);
        // Created at +120s and resolves at +240s ⇒ only instants 120, 180.
        let markets = vec![market("m", 120, Some(240))];
        let samples = plan_samples(&req, &markets);
        assert_eq!(samples.len(), 2);
        assert_eq!(samples[0].as_of, req.window_start + Duration::seconds(120));
        assert_eq!(samples[1].as_of, req.window_start + Duration::seconds(180));
    }
}

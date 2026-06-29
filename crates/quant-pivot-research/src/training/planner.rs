//! Deterministic sample planning.
//!
//! `plan_samples` is a pure function: the same `(request, markets)` always yields
//! the same ordered sample set, which is what makes `dataset_hash` reproducible.

use chrono::Duration;

use super::{DatasetPlanRequest, PlanMarket, SamplePlan};

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

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::{Duration, TimeZone, Utc};
    use quant_pivot_models::types::{
        MarketId, ModelSpecId, RuntimeConfigVersionId, SchemaVersion, TokenId,
        default_sample_sources,
    };

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
            feature_schema_version: SchemaVersion::new(1),
            sample_sources: default_sample_sources(),
            training_dataset_id: None,
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

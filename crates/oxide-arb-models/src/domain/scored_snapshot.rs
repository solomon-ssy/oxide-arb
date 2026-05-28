use crate::{
    domain::opportunity::Opportunity,
    enums::{
        calibration::{DurationBucket, PriceZone},
        common::StalenessLevel,
    },
};
use num_traits::ToPrimitive;
use serde::{Deserialize, Serialize};

/// Slim snapshot of scored opportunity fields for post-trade audit persistence.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ScoredOpportunitySnapshot {
    pub resolution_prob: f64,
    pub confidence: f64,
    pub convergence_secs: u32,
    pub price_zone: PriceZone,
    pub duration_bucket: DurationBucket,
    pub depth_used_pct: f64,
    pub staleness: StalenessLevel,
}

impl ScoredOpportunitySnapshot {
    #[must_use]
    pub fn from_opportunity(opp: &Opportunity) -> Self {
        Self {
            resolution_prob: opp.resolution_adjust.to_f64().unwrap_or(0.0),
            confidence: opp.meta.confidence.to_f64().unwrap_or(0.0),
            convergence_secs: u32::try_from(
                opp.meta.convergence_duration_secs.min(u64::from(u32::MAX)),
            )
            .unwrap_or(u32::MAX),
            price_zone: opp.meta.price_zone,
            duration_bucket: opp.meta.duration_bucket,
            depth_used_pct: opp.depth_used_pct.to_f64().unwrap_or(0.0),
            staleness: opp.staleness,
        }
    }
}

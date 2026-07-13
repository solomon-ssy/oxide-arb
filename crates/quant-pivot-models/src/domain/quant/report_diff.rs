//! Structural diff between two recommendation reports.
//!
//! A [`ReportDiff`] is a pure, computed read projection (no persistence):
//! [`compute_report_diff`] builds it from two reports plus their
//! recommendations, and the web layer projects it through `ReportDiffView`.
//!
//! Recommendations are matched by their `(market_id, outcome_side)` identity
//! (the stable economic key of a buy-to-open position), so the diff answers
//! "what did this report add / drop / re-weight versus the other one".

use crate::{
    domain::{RecommendationInfo, RecommendationReportInfo},
    enums::quant::OutcomeSide,
    types::{EligibilitySummary, MarketId, RecommendationId, RecommendationReportId, Usd},
};
use std::collections::BTreeMap;

/// How a single `(market, outcome_side)` position changed between two reports.
///
/// For an added position the `base_*` fields are `None`; for a removed position
/// the `compare_*` fields are `None`; for a retained position both sides are
/// present and `rank` / `suggested_usd` may differ. `suggested_usd_delta` is
/// always `compare − base` (treating an absent side as zero), so it is signed.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RecommendationDelta {
    /// Market the position is opened in.
    pub market_id: MarketId,
    /// Outcome token side (the economic identity together with `market_id`).
    pub outcome_side: OutcomeSide,
    /// Recommendation id in the base report, when present.
    pub base_recommendation_id: Option<RecommendationId>,
    /// Recommendation id in the compare report, when present.
    pub compare_recommendation_id: Option<RecommendationId>,
    /// Rank in the base report, when present.
    pub base_rank: Option<i32>,
    /// Rank in the compare report, when present.
    pub compare_rank: Option<i32>,
    /// Suggested USD in the base report, when present.
    pub base_suggested_usd: Option<Usd>,
    /// Suggested USD in the compare report, when present.
    pub compare_suggested_usd: Option<Usd>,
    /// `compare − base` suggested USD (absent side counts as zero); signed.
    pub suggested_usd_delta: Usd,
}

/// Report-level execution-eligibility shift across the two reports.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct EligibilityShift {
    /// Eligibility roll-up of the base report.
    pub base: EligibilitySummary,
    /// Eligibility roll-up of the compare report.
    pub compare: EligibilitySummary,
}

/// Computed diff between a `base` and a `compare` recommendation report.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ReportDiff {
    /// The report the comparison is anchored on.
    pub base_report_id: RecommendationReportId,
    /// The report being compared against the base.
    pub compare_report_id: RecommendationReportId,
    /// Positions present in `compare` but not in `base`.
    pub added: Vec<RecommendationDelta>,
    /// Positions present in `base` but not in `compare`.
    pub removed: Vec<RecommendationDelta>,
    /// Positions present in both (rank / suggested USD may differ).
    pub retained: Vec<RecommendationDelta>,
    /// Total suggested USD across the base report's recommendations.
    pub base_total_suggested_usd: Usd,
    /// Total suggested USD across the compare report's recommendations.
    pub compare_total_suggested_usd: Usd,
    /// `compare − base` total suggested USD; signed.
    pub total_suggested_usd_delta: Usd,
    /// Report-level execution-eligibility shift.
    pub eligibility: EligibilityShift,
}

/// Stable position identity: market id text plus outcome side code.
type PositionKey = (String, i8);

fn position_key(rec: &RecommendationInfo) -> PositionKey {
    (rec.market_id.to_string(), rec.outcome_side.as_i8())
}

fn total_suggested(recs: &[RecommendationInfo]) -> Usd {
    recs.iter()
        .filter_map(|rec| rec.trade_plan.sizing().map(|sizing| sizing.suggested_usd))
        .sum()
}

/// Compute the diff of `compare` against `base`.
///
/// Positions are matched by `(market_id, outcome_side)`; the result is
/// deterministic (sorted by that key within each bucket).
#[must_use]
pub fn compute_report_diff(
    base: &RecommendationReportInfo,
    base_recs: &[RecommendationInfo],
    compare: &RecommendationReportInfo,
    compare_recs: &[RecommendationInfo],
) -> ReportDiff {
    let base_by_key: BTreeMap<PositionKey, &RecommendationInfo> = base_recs
        .iter()
        .map(|rec| (position_key(rec), rec))
        .collect();
    let compare_by_key: BTreeMap<PositionKey, &RecommendationInfo> = compare_recs
        .iter()
        .map(|rec| (position_key(rec), rec))
        .collect();

    let mut keys: Vec<&PositionKey> = base_by_key.keys().chain(compare_by_key.keys()).collect();
    keys.sort_unstable();
    keys.dedup();

    let mut added = Vec::new();
    let mut removed = Vec::new();
    let mut retained = Vec::new();
    for key in keys {
        let base_rec = base_by_key.get(key).copied();
        let compare_rec = compare_by_key.get(key).copied();
        let delta = recommendation_delta(base_rec, compare_rec);
        match (base_rec, compare_rec) {
            (Some(_), Some(_)) => retained.push(delta),
            (None, Some(_)) => added.push(delta),
            (Some(_), None) => removed.push(delta),
            (None, None) => unreachable!("key originates from one of the two maps"),
        }
    }

    let base_total = total_suggested(base_recs);
    let compare_total = total_suggested(compare_recs);

    ReportDiff {
        base_report_id: base.recommendation_report_id.clone(),
        compare_report_id: compare.recommendation_report_id.clone(),
        added,
        removed,
        retained,
        base_total_suggested_usd: base_total,
        compare_total_suggested_usd: compare_total,
        total_suggested_usd_delta: compare_total - base_total,
        eligibility: EligibilityShift {
            base: base.summary_json.execution_eligibility_summary,
            compare: compare.summary_json.execution_eligibility_summary,
        },
    }
}

fn recommendation_delta(
    base: Option<&RecommendationInfo>,
    compare: Option<&RecommendationInfo>,
) -> RecommendationDelta {
    let anchor = base.or(compare).expect("at least one side present");
    let base_usd = base.and_then(|rec| rec.trade_plan.sizing().map(|sizing| sizing.suggested_usd));
    let compare_usd =
        compare.and_then(|rec| rec.trade_plan.sizing().map(|sizing| sizing.suggested_usd));
    let delta = compare_usd.unwrap_or(Usd::ZERO) - base_usd.unwrap_or(Usd::ZERO);
    RecommendationDelta {
        market_id: anchor.market_id.clone(),
        outcome_side: anchor.outcome_side,
        base_recommendation_id: base.map(|rec| rec.recommendation_id.clone()),
        compare_recommendation_id: compare.map(|rec| rec.recommendation_id.clone()),
        base_rank: base.map(|rec| rec.rank),
        compare_rank: compare.map(|rec| rec.rank),
        base_suggested_usd: base_usd,
        compare_suggested_usd: compare_usd,
        suggested_usd_delta: delta,
    }
}

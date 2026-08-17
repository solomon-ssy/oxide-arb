//! Structural diff between two recommendation reports.
//!
//! A [`ReportDiff`] is a pure, computed read projection (no persistence):
//! [`RecommendationReportInfo::diff`] builds it from two reports plus their
//! recommendations, and the web layer projects it through `ReportDiffView`.
//!
//! Recommendations are matched by their `(market_id, outcome_side)` identity
//! (the stable economic key of a buy-to-open position), so the diff answers
//! "what did this report add / drop / re-weight versus the other one".

use std::collections::BTreeMap;

use chrono::{DateTime, Utc};

use crate::{
    domain::quant::{RecommendationInfo, RecommendationReportInfo},
    enums::quant::OutcomeSide,
    types::{
        EligibilitySummary, ExecutionEligibility, MarketId, RecommendationFactorBreakdown,
        RecommendationId, RecommendationReportId, RecommendationTradePlan, Usd,
    },
};

use super::RecommendationEconomics;

/// A typed field whose decision semantics changed between two snapshots.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum RecommendationChangedField {
    Rank,
    ProfitProbability,
    NominalExpectedNetUsd,
    RobustExpectedNetUsd,
    MaximumLossUsd,
    CvarContributionUsd,
    CapitalOccupancyUsdHours,
    MarginalPortfolioValueUsd,
    Sizing,
    Validity,
    Eligibility,
    Entry,
    Exit,
    FactorBreakdown,
}

/// Decision-relevant state of one recommendation on one side of a diff.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RecommendationDiffSnapshot {
    pub recommendation_id: RecommendationId,
    pub rank: i32,
    pub economics: RecommendationEconomics,
    pub valid_from: DateTime<Utc>,
    pub valid_until: DateTime<Utc>,
    pub execution_eligibility: ExecutionEligibility,
    pub trade_plan: RecommendationTradePlan,
    pub factor_breakdown: RecommendationFactorBreakdown,
}

impl From<&RecommendationInfo> for RecommendationDiffSnapshot {
    fn from(rec: &RecommendationInfo) -> Self {
        Self {
            recommendation_id: rec.recommendation_id,
            rank: rec.rank,
            economics: rec.economics_json,
            valid_from: rec.valid_from,
            valid_until: rec.valid_until,
            execution_eligibility: rec.execution_eligibility.clone(),
            trade_plan: rec.trade_plan.clone(),
            factor_breakdown: rec.factor_breakdown.clone(),
        }
    }
}

/// How a single `(market, outcome_side)` position changed between two reports.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RecommendationDelta {
    /// Market the position is opened in.
    pub market_id: MarketId,
    /// Outcome token side (the economic identity together with `market_id`).
    pub outcome_side: OutcomeSide,
    /// Decision state in the base report; absent for an addition.
    pub base: Option<RecommendationDiffSnapshot>,
    /// Decision state in the compare report; absent for a removal.
    pub compare: Option<RecommendationDiffSnapshot>,
    /// Stable, typed summary used by the UI to prioritize high-signal changes.
    pub changed_fields: Vec<RecommendationChangedField>,
    /// `compare − base` hard-reserved cash (absent side counts as zero); signed.
    pub hard_reserved_cash_usd_delta: Usd,
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
    /// Positions present in both (rank / hard-reserved cash may differ).
    pub retained: Vec<RecommendationDelta>,
    /// Total hard-reserved cash across the base report's recommendations.
    pub base_total_hard_reserved_cash_usd: Usd,
    /// Total hard-reserved cash across the compare report's recommendations.
    pub compare_total_hard_reserved_cash_usd: Usd,
    /// `compare − base` total hard-reserved cash; signed.
    pub total_hard_reserved_cash_usd_delta: Usd,
    /// Report-level execution-eligibility shift.
    pub eligibility: EligibilityShift,
}

/// Stable position identity: market id text plus outcome side code.
type PositionKey = (String, i8);

impl RecommendationInfo {
    fn position_key(&self) -> PositionKey {
        (self.market_id.to_string(), self.outcome_side.as_i8())
    }
}

fn total_hard_reservation(recs: &[RecommendationInfo]) -> Usd {
    recs.iter()
        .map(|rec| rec.trade_plan.sizing.hard_reserved_cash_usd)
        .sum()
}

impl RecommendationReportInfo {
    /// Compute the diff of `compare` against this base report.
    ///
    /// Positions are matched by `(market_id, outcome_side)`; the result is
    /// deterministic (sorted by that key within each bucket).
    #[must_use]
    pub fn diff(
        &self,
        base_recs: &[RecommendationInfo],
        compare: &Self,
        compare_recs: &[RecommendationInfo],
    ) -> ReportDiff {
        let base_by_key: BTreeMap<PositionKey, &RecommendationInfo> = base_recs
            .iter()
            .map(|rec| ((rec).position_key(), rec))
            .collect();
        let compare_by_key: BTreeMap<PositionKey, &RecommendationInfo> = compare_recs
            .iter()
            .map(|rec| ((rec).position_key(), rec))
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
            if let Some(delta) = recommendation_delta(base_rec, compare_rec) {
                match (base_rec, compare_rec) {
                    (Some(_), Some(_)) => retained.push(delta),
                    (None, Some(_)) => added.push(delta),
                    (Some(_), None) => removed.push(delta),
                    (None, None) => {}
                }
            }
        }

        let base_total = total_hard_reservation(base_recs);
        let compare_total = total_hard_reservation(compare_recs);

        ReportDiff {
            base_report_id: self.recommendation_report_id,
            compare_report_id: compare.recommendation_report_id,
            added,
            removed,
            retained,
            base_total_hard_reserved_cash_usd: base_total,
            compare_total_hard_reserved_cash_usd: compare_total,
            total_hard_reserved_cash_usd_delta: compare_total - base_total,
            eligibility: EligibilityShift {
                base: self.summary_json.execution_eligibility_summary,
                compare: compare.summary_json.execution_eligibility_summary,
            },
        }
    }
}

fn recommendation_delta(
    base: Option<&RecommendationInfo>,
    compare: Option<&RecommendationInfo>,
) -> Option<RecommendationDelta> {
    let ((Some(anchor), _) | (None, Some(anchor))) = (base, compare) else {
        return None;
    };
    let base_usd = base.map(|rec| rec.trade_plan.sizing.hard_reserved_cash_usd);
    let compare_usd = compare.map(|rec| rec.trade_plan.sizing.hard_reserved_cash_usd);
    let delta = compare_usd.unwrap_or(Usd::ZERO) - base_usd.unwrap_or(Usd::ZERO);
    Some(RecommendationDelta {
        market_id: anchor.market_id.clone(),
        outcome_side: anchor.outcome_side,
        base: base.map(Into::into),
        compare: compare.map(Into::into),
        changed_fields: recommendation_changed_fields(base, compare),
        hard_reserved_cash_usd_delta: delta,
    })
}

fn recommendation_changed_fields(
    base: Option<&RecommendationInfo>,
    compare: Option<&RecommendationInfo>,
) -> Vec<RecommendationChangedField> {
    let (Some(base), Some(compare)) = (base, compare) else {
        return vec![
            RecommendationChangedField::Rank,
            RecommendationChangedField::ProfitProbability,
            RecommendationChangedField::NominalExpectedNetUsd,
            RecommendationChangedField::RobustExpectedNetUsd,
            RecommendationChangedField::MaximumLossUsd,
            RecommendationChangedField::CvarContributionUsd,
            RecommendationChangedField::CapitalOccupancyUsdHours,
            RecommendationChangedField::MarginalPortfolioValueUsd,
            RecommendationChangedField::Sizing,
            RecommendationChangedField::Validity,
            RecommendationChangedField::Eligibility,
            RecommendationChangedField::Entry,
            RecommendationChangedField::Exit,
            RecommendationChangedField::FactorBreakdown,
        ];
    };

    let mut changed = Vec::new();
    push_if_changed(
        &mut changed,
        base.rank != compare.rank,
        RecommendationChangedField::Rank,
    );
    push_if_changed(
        &mut changed,
        base.economics_json.profit_probability_bps != compare.economics_json.profit_probability_bps,
        RecommendationChangedField::ProfitProbability,
    );
    push_if_changed(
        &mut changed,
        base.economics_json.nominal_expected_net_usd
            != compare.economics_json.nominal_expected_net_usd,
        RecommendationChangedField::NominalExpectedNetUsd,
    );
    push_if_changed(
        &mut changed,
        base.economics_json.robust_expected_net_usd
            != compare.economics_json.robust_expected_net_usd,
        RecommendationChangedField::RobustExpectedNetUsd,
    );
    push_if_changed(
        &mut changed,
        base.economics_json.max_loss_usd != compare.economics_json.max_loss_usd,
        RecommendationChangedField::MaximumLossUsd,
    );
    push_if_changed(
        &mut changed,
        base.economics_json.cvar_contribution_usd != compare.economics_json.cvar_contribution_usd,
        RecommendationChangedField::CvarContributionUsd,
    );
    push_if_changed(
        &mut changed,
        base.economics_json.capital_occupancy_usd_hours
            != compare.economics_json.capital_occupancy_usd_hours,
        RecommendationChangedField::CapitalOccupancyUsdHours,
    );
    push_if_changed(
        &mut changed,
        base.economics_json.marginal_portfolio_value_usd
            != compare.economics_json.marginal_portfolio_value_usd,
        RecommendationChangedField::MarginalPortfolioValueUsd,
    );
    push_if_changed(
        &mut changed,
        base.trade_plan.sizing != compare.trade_plan.sizing,
        RecommendationChangedField::Sizing,
    );
    push_if_changed(
        &mut changed,
        base.valid_from != compare.valid_from || base.valid_until != compare.valid_until,
        RecommendationChangedField::Validity,
    );
    push_if_changed(
        &mut changed,
        base.execution_eligibility != compare.execution_eligibility,
        RecommendationChangedField::Eligibility,
    );
    push_if_changed(
        &mut changed,
        base.trade_plan.entry != compare.trade_plan.entry,
        RecommendationChangedField::Entry,
    );
    push_if_changed(
        &mut changed,
        base.trade_plan.exit != compare.trade_plan.exit,
        RecommendationChangedField::Exit,
    );
    push_if_changed(
        &mut changed,
        base.factor_breakdown != compare.factor_breakdown,
        RecommendationChangedField::FactorBreakdown,
    );
    changed
}

fn push_if_changed(
    changed: &mut Vec<RecommendationChangedField>,
    predicate: bool,
    field: RecommendationChangedField,
) {
    if predicate {
        changed.push(field);
    }
}

//! Which evidence metrics block `ProductionIneligible` vs report-only unavailable.

use oxide_arb_models::domain::evidence::EvidenceMetric;

use super::{execution, portfolio};

fn metric_missing<T>(metric: &EvidenceMetric<T>) -> usize {
    usize::from(!metric.is_available())
}

/// P0 execution metrics required before a stage is production-eligible.
///
/// `book_age_fill_correlation_bps` is listed in the Phase 5.3 plan but not yet
/// materialized in `execution::build_report` (always `Unavailable`); it must not
/// block replay validation until correlation is implemented.
#[must_use]
pub fn execution_production_required_missing_count(
    report: &execution::ExecutionEvidenceReport,
) -> usize {
    [
        metric_missing(&report.simulated_vwap_p50_bps),
        metric_missing(&report.simulated_vwap_p95_bps),
        metric_missing(&report.realized_slippage_p50_bps),
        metric_missing(&report.realized_slippage_p95_bps),
        metric_missing(&report.depth_consumed_pct_p50_bps),
        metric_missing(&report.depth_consumed_pct_p95_bps),
        metric_missing(&report.latency_shifted_miss_rate_bps),
        metric_missing(&report.adverse_selection_loss_p95_bps),
    ]
    .into_iter()
    .sum()
}

/// P0 portfolio metrics required before a stage is production-eligible.
///
/// Drawdown, loss streak, settlement backlog, and stale-metrics windows are
/// intentionally `Unavailable` in Phase 5.3 until equity / lifecycle timelines exist.
#[must_use]
pub fn portfolio_production_required_missing_count(
    report: &portfolio::PortfolioRiskEvidenceReport,
) -> usize {
    [
        metric_missing(&report.peak_reserved_usd),
        metric_missing(&report.peak_potential_loss_usd),
        metric_missing(&report.peak_total_exposure_usd),
        metric_missing(&report.peak_open_positions),
    ]
    .into_iter()
    .sum()
}

#[cfg(test)]
mod tests {
    use oxide_arb_models::domain::evidence::EvidenceMetric;

    use super::{
        execution_production_required_missing_count, portfolio_production_required_missing_count,
    };
    use crate::evidence::{execution, portfolio};

    fn unavailable_u64(code: &str) -> EvidenceMetric<u64> {
        EvidenceMetric::Unavailable {
            code: code.to_owned(),
            reason: "fixture".to_owned(),
        }
    }

    fn available_u64(value: u64) -> EvidenceMetric<u64> {
        EvidenceMetric::Available { value }
    }

    fn available_str(value: &str) -> EvidenceMetric<String> {
        EvidenceMetric::Available {
            value: value.to_owned(),
        }
    }

    #[test]
    fn execution_correlation_unavailable_does_not_block_production_gate() {
        let report = ExecutionEvidenceReportFixture::complete().report;
        assert_eq!(execution_production_required_missing_count(&report), 0);
        assert!(!report.book_age_fill_correlation_bps.is_available());
    }

    #[test]
    fn portfolio_p2_proxy_metrics_unavailable_do_not_block_production_gate() {
        let report = PortfolioEvidenceReportFixture::complete().report;
        assert_eq!(portfolio_production_required_missing_count(&report), 0);
        assert!(!report.max_drawdown_pct_bps.is_available());
        assert!(!report.loss_streak_max.is_available());
    }

    struct ExecutionEvidenceReportFixture {
        report: execution::ExecutionEvidenceReport,
    }

    impl ExecutionEvidenceReportFixture {
        fn complete() -> Self {
            Self {
                report: execution::ExecutionEvidenceReport {
                    strict_fok_fill_rate_bps: 10_000,
                    live_fill_rate_bps: 10_000,
                    true_fill_count: 1,
                    true_miss_count: 0,
                    false_fill_count: 0,
                    false_miss_count: 0,
                    simulated_vwap_p50_bps: available_u64(9_500),
                    simulated_vwap_p95_bps: available_u64(9_500),
                    realized_slippage_p50_bps: available_u64(100),
                    realized_slippage_p95_bps: available_u64(100),
                    depth_consumed_pct_p50_bps: available_u64(10_000),
                    depth_consumed_pct_p95_bps: available_u64(10_000),
                    latency_shifted_miss_rate_bps: available_u64(0),
                    adverse_selection_loss_p95_bps: available_u64(0),
                    book_age_fill_correlation_bps: EvidenceMetric::Unavailable {
                        code: "execution.correlation_sample_missing".to_owned(),
                        reason: "not implemented".to_owned(),
                    },
                    missing_attribution_count: 0,
                    query_fingerprints: Vec::new(),
                },
            }
        }
    }

    struct PortfolioEvidenceReportFixture {
        report: portfolio::PortfolioRiskEvidenceReport,
    }

    impl PortfolioEvidenceReportFixture {
        fn complete() -> Self {
            Self {
                report: portfolio::PortfolioRiskEvidenceReport {
                    peak_reserved_usd: available_str("95"),
                    peak_potential_loss_usd: available_str("6"),
                    peak_total_exposure_usd: available_str("94"),
                    peak_open_positions: available_u64(1),
                    max_drawdown_pct_bps: unavailable_u64("risk.equity_timeline_missing"),
                    loss_streak_max: unavailable_u64("risk.settlement_sequence_missing"),
                    risk_denial_count: 0,
                    sizing_denial_count: 0,
                    settlement_backlog_max: unavailable_u64("settlement.backlog_timeline_missing"),
                    stale_metrics_window_ms: unavailable_u64(
                        "risk.metrics_freshness_timeline_missing",
                    ),
                    insufficient_reasons: Vec::new(),
                    query_fingerprints: Vec::new(),
                },
            }
        }
    }
}

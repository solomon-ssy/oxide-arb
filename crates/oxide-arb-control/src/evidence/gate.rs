use oxide_arb_models::{
    domain::control_factor::StageCoverageReport,
    enums::control_factor::{EvidenceStageStatus, MaterializationOutputPolicy},
};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EvidenceStageGate {
    pub output_policy: MaterializationOutputPolicy,
    pub coverage: StageCoverageReport,
    pub blocking_issue_count: usize,
    pub warning_count: usize,
    pub required_metric_missing_count: usize,
}

impl EvidenceStageGate {
    #[must_use]
    pub fn decide(&self) -> EvidenceStageStatus {
        if !self.coverage.is_sufficient() {
            return EvidenceStageStatus::InsufficientCoverage;
        }
        if self.blocking_issue_count > 0 || self.required_metric_missing_count > 0 {
            return EvidenceStageStatus::ProductionIneligible;
        }
        if self.warning_count > 0
            || matches!(
                self.output_policy,
                MaterializationOutputPolicy::ReportOnly
                    | MaterializationOutputPolicy::NoFactorOutput
            )
        {
            return EvidenceStageStatus::CompletedWithWarnings;
        }
        EvidenceStageStatus::Completed
    }
}

#[cfg(test)]
mod tests {
    use oxide_arb_models::{
        domain::control_factor::StageCoverageReport,
        enums::control_factor::{EvidenceStageStatus, MaterializationOutputPolicy},
    };
    use rust_decimal::Decimal;

    use crate::evidence::gate::EvidenceStageGate;

    #[test]
    fn missing_required_metric_blocks_production() {
        let status = EvidenceStageGate {
            output_policy: MaterializationOutputPolicy::EmitDraftCandidates,
            coverage: StageCoverageReport::complete(1),
            blocking_issue_count: 0,
            warning_count: 0,
            required_metric_missing_count: 1,
        }
        .decide();

        assert_eq!(status, EvidenceStageStatus::ProductionIneligible);
    }

    #[test]
    fn insufficient_coverage_takes_precedence() {
        let status = EvidenceStageGate {
            output_policy: MaterializationOutputPolicy::EmitDraftCandidates,
            coverage: StageCoverageReport {
                expected_rows: 2,
                observed_rows: 1,
                missing_rows: 1,
                coverage_ratio: Decimal::new(5, 1),
                insufficient_reasons: vec!["missing row".to_owned()],
            },
            blocking_issue_count: 1,
            warning_count: 0,
            required_metric_missing_count: 1,
        }
        .decide();

        assert_eq!(status, EvidenceStageStatus::InsufficientCoverage);
    }
}

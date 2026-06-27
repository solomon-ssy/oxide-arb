//! [`DefaultAdmissionEngine`]: the fixed-order, short-circuiting evaluator.

use std::sync::Arc;
use std::time::Instant;

use async_trait::async_trait;
use quant_pivot_error::QuantResult;
use quant_pivot_models::enums::execution::AdmissionOutcome;

use super::checks::{
    BookFreshnessCheck, CapitalBudgetCheck, CategoryExposureCheck, CredentialReadinessCheck,
    DataQualityCheck, EntryTriggerCheck, EventExposureCheck, ExitMonitorReadinessCheck,
    IntentStateCheck, KillSwitchCheck, LiquidityDepthCheck, ManualBlockCheck, MarketExposureCheck,
    ModelPublicationCheck, RecommendationFreshnessCheck, ReportStatusCheck, RiskEnvelopeHashCheck,
    RuntimeModeCheck, SlippageCheck, VenueGuardCheck,
};
use super::{AdmissionCheck, AdmissionDecision, AdmissionInput, ExecutionAdmissionEngine};
use crate::observability::metrics_hub::MetricsHub;

/// The 20-check admission set (parent §4.2). A fixed-size array makes the count
/// a compile-time invariant.
const ADMISSION_CHECK_COUNT: usize = 20;

/// Default admission engine: holds the 20 checks in canonical order and folds
/// their outcomes into a single decision.
pub struct DefaultAdmissionEngine {
    checks: [Box<dyn AdmissionCheck>; ADMISSION_CHECK_COUNT],
    metrics: Arc<MetricsHub>,
}

impl DefaultAdmissionEngine {
    /// Assemble the engine with the canonical check order.
    #[must_use]
    pub fn new(metrics: Arc<MetricsHub>) -> Self {
        let checks: [Box<dyn AdmissionCheck>; ADMISSION_CHECK_COUNT] = [
            Box::new(IntentStateCheck),
            Box::new(RecommendationFreshnessCheck),
            Box::new(ReportStatusCheck),
            Box::new(RuntimeModeCheck),
            Box::new(ModelPublicationCheck),
            Box::new(DataQualityCheck),
            Box::new(BookFreshnessCheck),
            Box::new(EntryTriggerCheck),
            Box::new(RiskEnvelopeHashCheck),
            Box::new(CapitalBudgetCheck),
            Box::new(MarketExposureCheck),
            Box::new(EventExposureCheck),
            Box::new(CategoryExposureCheck),
            Box::new(LiquidityDepthCheck),
            Box::new(SlippageCheck),
            Box::new(ManualBlockCheck),
            Box::new(KillSwitchCheck),
            Box::new(VenueGuardCheck),
            Box::new(CredentialReadinessCheck),
            Box::new(ExitMonitorReadinessCheck),
        ];
        Self { checks, metrics }
    }

    /// Run the checks. With `full == false` the first hard deny short-circuits;
    /// with `full == true` every check runs (the outcome is identical, only the
    /// trace is complete). The deny metric is incremented exactly once, for the
    /// check that determines a `Deny` outcome.
    fn decide(&self, input: &AdmissionInput, full: bool) -> AdmissionDecision {
        let start = Instant::now();
        let mut trace = Vec::with_capacity(ADMISSION_CHECK_COUNT);
        let mut outcome = AdmissionOutcome::Allow;
        let mut denial_reason: Option<String> = None;

        for check in &self.checks {
            let check_start = Instant::now();
            let mut entry = check.run(input);
            entry.elapsed_us = elapsed_us(check_start);

            match entry.outcome {
                AdmissionOutcome::Deny => {
                    if outcome != AdmissionOutcome::Deny {
                        outcome = AdmissionOutcome::Deny;
                        denial_reason = Some(format!("{}: {}", entry.check.as_str(), entry.detail));
                        self.metrics
                            .admission_denied
                            .with_label_values(&[entry.check.as_str()])
                            .inc();
                    }
                    trace.push(entry);
                    if !full {
                        break;
                    }
                }
                AdmissionOutcome::Defer => {
                    if outcome == AdmissionOutcome::Allow {
                        outcome = AdmissionOutcome::Defer;
                    }
                    trace.push(entry);
                }
                AdmissionOutcome::Allow => trace.push(entry),
            }
        }

        AdmissionDecision {
            outcome,
            trace,
            state_version: input.state_version.clone(),
            elapsed_ms: elapsed_ms(start),
            denial_reason,
        }
    }
}

#[async_trait]
impl ExecutionAdmissionEngine for DefaultAdmissionEngine {
    async fn evaluate(&self, input: AdmissionInput) -> QuantResult<AdmissionDecision> {
        Ok(self.decide(&input, false))
    }

    async fn evaluate_full(&self, input: AdmissionInput) -> QuantResult<AdmissionDecision> {
        Ok(self.decide(&input, true))
    }
}

fn elapsed_us(start: Instant) -> u64 {
    u64::try_from(start.elapsed().as_micros()).unwrap_or(u64::MAX)
}

fn elapsed_ms(start: Instant) -> u64 {
    u64::try_from(start.elapsed().as_millis()).unwrap_or(u64::MAX)
}

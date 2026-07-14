//! Runtime-mode execution gate: published recommendation → intent policy.

use crate::runtime_config::RuntimeConfigStore;
use async_trait::async_trait;
use quant_pivot_error::QuantResult;
use quant_pivot_models::{
    domain::RecommendationInfo,
    enums::{execution::ModeDenialReason, quant::QuantRuntimeMode},
    types::{ContentHash, Usd},
};
use std::{sync::Arc, time::Duration};

/// Policy result for turning a recommendation into an order intent.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum IntentPolicyDecision {
    /// Runtime is report-only; no intent should be created.
    ReportOnly,
    /// Semi-auto intent must wait for operator approval (RBAC enforced at API).
    RequiresApproval { approval_ttl: Duration },
    /// Auto policy approved the intent.
    ApprovedByPolicy {
        policy_id: String,
        policy_hash: Option<ContentHash>,
        reason: String,
    },
    /// Runtime or recommendation state denies intent creation.
    Denied { reason: ModeDenialReason },
}

/// Runtime mode gate used before intent creation.
#[async_trait]
pub trait RuntimeModeGate: Send + Sync {
    async fn evaluate_intent_policy(
        &self,
        mode: QuantRuntimeMode,
        recommendation: &RecommendationInfo,
    ) -> QuantResult<IntentPolicyDecision>;
}

/// Default mode gate driven by the active runtime config and the recommendation's
/// frozen [`ExecutionEligibility`](quant_pivot_models::types::ExecutionEligibility)
/// + [`RiskEnvelope`](quant_pivot_models::types::RiskEnvelope).
///
/// Deterministic and side-effect free: it reads the hot config snapshot for the
/// approval TTL and the auto-execution switch, then maps
/// `(mode, recommendation)` to one of the five [`IntentPolicyDecision`] arms.
pub struct DefaultRuntimeModeGate {
    config: Arc<RuntimeConfigStore>,
}

impl DefaultRuntimeModeGate {
    /// Build the gate over the shared runtime-config snapshot.
    #[must_use]
    pub const fn new(config: Arc<RuntimeConfigStore>) -> Self {
        Self { config }
    }
}

#[async_trait]
impl RuntimeModeGate for DefaultRuntimeModeGate {
    async fn evaluate_intent_policy(
        &self,
        mode: QuantRuntimeMode,
        recommendation: &RecommendationInfo,
    ) -> QuantResult<IntentPolicyDecision> {
        // 1. report_only never creates an intent.
        if mode == QuantRuntimeMode::ReportOnly {
            return Ok(IntentPolicyDecision::ReportOnly);
        }

        // 2. The recommendation must be eligible for this exact mode.
        let eligibility = &recommendation.execution_eligibility;
        if !eligibility.is_eligible(mode) {
            return Ok(IntentPolicyDecision::Denied {
                reason: ModeDenialReason::RecommendationIneligible,
            });
        }

        // 3. The risk envelope must be usable (positive caps).
        let Some((_, _, _, _, envelope)) = recommendation.trade_plan.frozen() else {
            return Ok(IntentPolicyDecision::Denied {
                reason: ModeDenialReason::RecommendationIneligible,
            });
        };
        if envelope.max_position_usd <= Usd::ZERO || envelope.max_loss_usd <= Usd::ZERO {
            return Ok(IntentPolicyDecision::Denied {
                reason: ModeDenialReason::RiskEnvelopeInvalid,
            });
        }

        let config = self.config.current();
        let semi_auto = &config.execution.semi_auto;

        match mode {
            QuantRuntimeMode::ReportOnly => Ok(IntentPolicyDecision::ReportOnly),
            QuantRuntimeMode::SemiAuto => Ok(IntentPolicyDecision::RequiresApproval {
                approval_ttl: Duration::from_secs(semi_auto.approval_ttl_secs),
            }),
            QuantRuntimeMode::AutoExecution => Ok(IntentPolicyDecision::Denied {
                reason: ModeDenialReason::AutoExecutionNotAllowed,
            }),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::{DefaultRuntimeModeGate, IntentPolicyDecision, RuntimeModeGate};
    use crate::runtime_config::RuntimeConfigStore;
    use quant_pivot_models::{
        domain::RecommendationInfo,
        enums::{
            execution::ModeDenialReason,
            quant::{OutcomeSide, QuantRuntimeMode},
        },
        runtime_config::RuntimeConfig,
        types::{
            RecommendationId, RecommendationReportId, RecommendationTradePlan, RiskEnvelope, Usd,
        },
    };
    use quant_pivot_test_support::report_fixtures;
    use rust_decimal_macros::dec;
    use std::sync::Arc;

    fn rec() -> RecommendationInfo {
        report_fixtures::recommendation(
            RecommendationReportId::from_v7(),
            RecommendationId::from_v7(),
            1,
            "0xmkt",
            OutcomeSide::Yes,
            Usd::new(dec!(250)),
        )
    }

    fn gate(config: RuntimeConfig) -> DefaultRuntimeModeGate {
        DefaultRuntimeModeGate::new(Arc::new(RuntimeConfigStore::new(config)))
    }

    fn risk_envelope(rec: &mut RecommendationInfo) -> &mut RiskEnvelope {
        match &mut rec.trade_plan {
            RecommendationTradePlan::Frozen { risk_envelope, .. } => risk_envelope,
            RecommendationTradePlan::Unavailable { .. } => panic!("fixture must be frozen"),
        }
    }

    #[tokio::test]
    async fn report_only_never_creates_intent() {
        let gate = gate(RuntimeConfig::default());
        let mut rec = rec();
        rec.execution_eligibility.eligible_modes =
            vec![QuantRuntimeMode::ReportOnly, QuantRuntimeMode::SemiAuto];
        let decision = gate
            .evaluate_intent_policy(QuantRuntimeMode::ReportOnly, &rec)
            .await
            .expect("policy");
        assert_eq!(decision, IntentPolicyDecision::ReportOnly);
    }

    #[tokio::test]
    async fn semi_auto_eligible_requires_approval() {
        let gate = gate(RuntimeConfig::default());
        let mut rec = rec();
        rec.execution_eligibility.eligible_modes = vec![QuantRuntimeMode::SemiAuto];
        let decision = gate
            .evaluate_intent_policy(QuantRuntimeMode::SemiAuto, &rec)
            .await
            .expect("policy");
        match decision {
            IntentPolicyDecision::RequiresApproval { approval_ttl } => {
                assert_eq!(approval_ttl.as_secs(), 900);
            }
            other => panic!("expected RequiresApproval, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn semi_auto_ineligible_is_denied() {
        let gate = gate(RuntimeConfig::default());
        let mut rec = rec();
        rec.execution_eligibility.eligible_modes = vec![QuantRuntimeMode::ReportOnly];
        let decision = gate
            .evaluate_intent_policy(QuantRuntimeMode::SemiAuto, &rec)
            .await
            .expect("policy");
        assert_eq!(
            decision,
            IntentPolicyDecision::Denied {
                reason: ModeDenialReason::RecommendationIneligible,
            }
        );
    }

    #[tokio::test]
    async fn runtime_v13_blocks_auto_execution_even_when_legacy_flags_allow_it() {
        let mut config = RuntimeConfig::default();
        config.execution.auto_execution.enabled = true;
        let gate = gate(config);
        let mut rec = rec();
        rec.execution_eligibility.eligible_modes = vec![QuantRuntimeMode::AutoExecution];
        rec.execution_eligibility.ineligibility_reasons = Vec::new();
        rec.execution_eligibility.auto_policy_id = Some("policy-7".to_owned());
        risk_envelope(&mut rec).auto_execution_allowed = true;
        let decision = gate
            .evaluate_intent_policy(QuantRuntimeMode::AutoExecution, &rec)
            .await
            .expect("policy");
        assert_eq!(
            decision,
            IntentPolicyDecision::Denied {
                reason: ModeDenialReason::AutoExecutionNotAllowed,
            }
        );
    }

    #[tokio::test]
    async fn auto_execution_denied_when_envelope_disallows() {
        let mut config = RuntimeConfig::default();
        config.execution.auto_execution.enabled = true;
        let gate = gate(config);
        let mut rec = rec();
        rec.execution_eligibility.eligible_modes = vec![QuantRuntimeMode::AutoExecution];
        rec.execution_eligibility.ineligibility_reasons = Vec::new();
        rec.execution_eligibility.auto_policy_id = Some("policy-7".to_owned());
        risk_envelope(&mut rec).auto_execution_allowed = false;
        let decision = gate
            .evaluate_intent_policy(QuantRuntimeMode::AutoExecution, &rec)
            .await
            .expect("policy");
        assert_eq!(
            decision,
            IntentPolicyDecision::Denied {
                reason: ModeDenialReason::AutoExecutionNotAllowed,
            }
        );
    }

    #[tokio::test]
    async fn degenerate_envelope_is_denied() {
        let gate = gate(RuntimeConfig::default());
        let mut rec = rec();
        rec.execution_eligibility.eligible_modes = vec![QuantRuntimeMode::SemiAuto];
        risk_envelope(&mut rec).max_position_usd = Usd::ZERO;
        let decision = gate
            .evaluate_intent_policy(QuantRuntimeMode::SemiAuto, &rec)
            .await
            .expect("policy");
        assert_eq!(
            decision,
            IntentPolicyDecision::Denied {
                reason: ModeDenialReason::RiskEnvelopeInvalid,
            }
        );
    }
}

//! Runtime-mode execution gate: published recommendation → intent policy.

use std::{sync::Arc, time::Duration};

use async_trait::async_trait;
use quant_pivot_error::QuantResult;
use quant_pivot_models::{
    domain::quant::RecommendationInfo,
    enums::{execution::ModeDenialReason, quant::QuantRuntimeMode},
    types::{ContentHash, Usd},
};

use crate::runtime_config::DecisionPolicyStore;

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
/// Deterministic and side-effect free: it reads the hot policy bundle for the
/// approval TTL and exact active-policy identity, then maps
/// `(mode, recommendation)` to one of the five [`IntentPolicyDecision`] arms.
pub struct DefaultRuntimeModeGate {
    config: Arc<DecisionPolicyStore>,
}

impl DefaultRuntimeModeGate {
    /// Build the gate over the shared runtime-config snapshot.
    #[must_use]
    pub const fn new(config: Arc<DecisionPolicyStore>) -> Self {
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
        let envelope = &recommendation.trade_plan.risk_envelope;
        if envelope.max_position_usd <= Usd::ZERO || envelope.max_loss_usd <= Usd::ZERO {
            return Ok(IntentPolicyDecision::Denied {
                reason: ModeDenialReason::RiskEnvelopeInvalid,
            });
        }

        match mode {
            QuantRuntimeMode::ReportOnly => Ok(IntentPolicyDecision::ReportOnly),
            QuantRuntimeMode::SemiAuto => {
                let config = self.config.current();
                Ok(IntentPolicyDecision::RequiresApproval {
                    approval_ttl: Duration::from_secs(
                        config
                            .execution_automation_policy
                            .semi_auto
                            .approval_ttl_secs,
                    ),
                })
            }
            QuantRuntimeMode::AutoExecution => {
                let expected_policy_id = recommendation.evidence_refs.decision_policy_snapshot_id;
                let expected_policy_text = expected_policy_id.to_string();
                let exact_frozen_policy =
                    eligibility.auto_policy_id.as_deref() == Some(expected_policy_text.as_str());
                let Some(active_bundle) = self.config.current_bundle() else {
                    return Ok(IntentPolicyDecision::Denied {
                        reason: ModeDenialReason::AutoExecutionNotAllowed,
                    });
                };
                if !envelope.auto_execution_allowed
                    || !exact_frozen_policy
                    || active_bundle.decision_policy_snapshot_id != expected_policy_id
                {
                    return Ok(IntentPolicyDecision::Denied {
                        reason: ModeDenialReason::AutoExecutionNotAllowed,
                    });
                }
                Ok(IntentPolicyDecision::ApprovedByPolicy {
                    policy_id: expected_policy_text,
                    policy_hash: Some(active_bundle.snapshot_hash),
                    reason:
                        "frozen recommendation and active decision policy authorize auto execution"
                            .to_owned(),
                })
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use quant_pivot_models::{
        domain::quant::RecommendationInfo,
        enums::{
            execution::ModeDenialReason,
            quant::{OutcomeSide, QuantRuntimeMode},
        },
        runtime_config::{ActivePolicyBundle, DecisionPolicySnapshot},
        types::{
            DecisionPolicySnapshotId, PolicyBundleGeneration, RecommendationId,
            RecommendationReportId, RiskEnvelope, Usd,
        },
    };
    use rust_decimal_macros::dec;

    use super::{DefaultRuntimeModeGate, IntentPolicyDecision, RuntimeModeGate};
    use crate::{runtime_config::DecisionPolicyStore, test_fixtures::report_fixtures};

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

    fn gate(config: DecisionPolicySnapshot) -> DefaultRuntimeModeGate {
        DefaultRuntimeModeGate::new(Arc::new(DecisionPolicyStore::new(config)))
    }

    fn active_gate(
        config: DecisionPolicySnapshot,
        policy_id: DecisionPolicySnapshotId,
    ) -> DefaultRuntimeModeGate {
        let hash = config.persistence_hash().expect("hash policy snapshot");
        let bundle = ActivePolicyBundle::from_parts(
            PolicyBundleGeneration::try_new(1).expect("positive generation"),
            policy_id,
            hash,
            config,
        );
        DefaultRuntimeModeGate::new(Arc::new(DecisionPolicyStore::new_active(bundle)))
    }

    fn risk_envelope(rec: &mut RecommendationInfo) -> &mut RiskEnvelope {
        &mut rec.trade_plan.risk_envelope
    }

    #[tokio::test]
    async fn report_never_creates_intent() {
        let gate = gate(DecisionPolicySnapshot::default());
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
    async fn semi_auto_requires_approval() {
        let gate = gate(DecisionPolicySnapshot::default());
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
    async fn semi_auto_ineligible_denied() {
        let gate = gate(DecisionPolicySnapshot::default());
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
    async fn exact_active_allows_execution() {
        let config = DecisionPolicySnapshot::default();
        let mut rec = rec();
        let policy_id = rec.evidence_refs.decision_policy_snapshot_id;
        let gate = active_gate(config, policy_id);
        rec.execution_eligibility.eligible_modes = vec![QuantRuntimeMode::AutoExecution];
        rec.execution_eligibility.ineligibility_reasons = Vec::new();
        rec.execution_eligibility.auto_policy_id = Some(policy_id.to_string());
        risk_envelope(&mut rec).auto_execution_allowed = true;
        let decision = gate
            .evaluate_intent_policy(QuantRuntimeMode::AutoExecution, &rec)
            .await
            .expect("policy");
        assert!(matches!(
            decision,
            IntentPolicyDecision::ApprovedByPolicy { policy_id: id, .. } if id == policy_id.to_string()
        ));
    }

    #[tokio::test]
    async fn auto_execution_denied_disallows() {
        let config = DecisionPolicySnapshot::default();
        let mut rec = rec();
        let policy_id = rec.evidence_refs.decision_policy_snapshot_id;
        let gate = active_gate(config, policy_id);
        rec.execution_eligibility.eligible_modes = vec![QuantRuntimeMode::AutoExecution];
        rec.execution_eligibility.ineligibility_reasons = Vec::new();
        rec.execution_eligibility.auto_policy_id = Some(policy_id.to_string());
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
        let gate = gate(DecisionPolicySnapshot::default());
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

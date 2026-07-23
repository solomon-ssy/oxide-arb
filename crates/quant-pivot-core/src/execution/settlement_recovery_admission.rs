//! Fresh, typed settlement-recovery admission for risk-increasing entries.
//!
//! A `HoldToResolution + Auto` entry is allowed only when the configured
//! execution account has a verified current V2 recovery route and the runtime
//! settlement write policy matches the execution mode. This check runs both
//! when an intent is created and immediately before order submission.

use std::fmt::{Display, Formatter, Result as FmtResult};

use async_trait::async_trait;
use chrono::{DateTime, Utc};
use quant_pivot_error::QuantResult;
use quant_pivot_models::{
    enums::{
        quant::{ExecutionWalletKind, ExitSettlementMode, QuantRuntimeMode, RedeemPolicy},
        settlement::{SettlementRoute, SettlementWritePolicy},
    },
    types::{ContentHash, EvmAddress, EvmBlockHash, ExecutionAccountId, ExitPolicySpec},
};

/// Exact account and runtime policy whose recovery path must be verified.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SettlementRecoveryAdmissionRequest {
    pub execution_account_id: ExecutionAccountId,
    pub route: SettlementRoute,
    pub runtime_mode: QuantRuntimeMode,
    pub write_policy: SettlementWritePolicy,
}

/// Immutable evidence captured by a successful recovery-admission check.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SettlementRecoveryAdmissionEvidence {
    pub route: SettlementRoute,
    pub wallet_kind: ExecutionWalletKind,
    pub target_adapter: EvmAddress,
    pub deployment_digest: ContentHash,
    pub verified_block_number: u64,
    pub verified_block_hash: EvmBlockHash,
    pub confirmed_canary: bool,
}

/// Closed reason set for a required recovery path that cannot admit new risk.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SettlementRecoveryAdmissionBlockReason {
    ExecutionAccountMismatch,
    RuntimePolicyMismatch {
        runtime_mode: QuantRuntimeMode,
        write_policy: SettlementWritePolicy,
    },
    DeploymentNotReady,
    /// Route fingerprint is ready, but ERC-1155 operator approval is absent.
    OperatorApprovalMissing,
    ConfirmedCanaryMissing,
}

impl Display for SettlementRecoveryAdmissionBlockReason {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> FmtResult {
        match self {
            Self::ExecutionAccountMismatch => {
                formatter.write_str("execution account does not match settlement wallet")
            }
            Self::RuntimePolicyMismatch {
                runtime_mode,
                write_policy,
            } => write!(
                formatter,
                "runtime mode {runtime_mode} is incompatible with settlement write policy \
                 {write_policy}"
            ),
            Self::DeploymentNotReady => {
                formatter.write_str("current V2 settlement deployment is not ready")
            }
            Self::OperatorApprovalMissing => formatter.write_str(
                "HoldToResolution + Auto requires live ERC-1155 operator approval for the \
                 current V2 adapter",
            ),
            Self::ConfirmedCanaryMissing => formatter.write_str(
                "auto settlement requires confirmed on-chain canary evidence for this \
                 account, route, and deployment",
            ),
        }
    }
}

/// Frozen result consumed by the pure admission engine.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SettlementRecoveryAdmission {
    NotRequired,
    Ready(SettlementRecoveryAdmissionEvidence),
    Blocked(SettlementRecoveryAdmissionBlockReason),
}

impl SettlementRecoveryAdmission {
    /// Deployment digest included in the replayable admission state version.
    #[must_use]
    pub const fn deployment_digest(&self) -> Option<ContentHash> {
        match self {
            Self::Ready(evidence) => Some(evidence.deployment_digest),
            Self::NotRequired | Self::Blocked(_) => None,
        }
    }

    /// Canonical verification block included in the replayable admission state.
    #[must_use]
    pub fn verified_block_hash(&self) -> Option<EvmBlockHash> {
        match self {
            Self::Ready(evidence) => Some(evidence.verified_block_hash.clone()),
            Self::NotRequired | Self::Blocked(_) => None,
        }
    }
}

/// Signer-free boundary used by intent creation and pre-order admission.
///
/// Implementations must perform a fresh capability verification. UI readiness
/// caches are never valid inputs to this money-path boundary.
#[async_trait]
pub trait SettlementRecoveryAdmissionPort: Send + Sync {
    async fn evaluate_recovery_admission(
        &self,
        request: SettlementRecoveryAdmissionRequest,
        checked_at: DateTime<Utc>,
    ) -> QuantResult<SettlementRecoveryAdmission>;
}

/// Whether this exact frozen exit policy promises automatic resolution recovery.
#[must_use]
pub const fn requires_automatic_settlement_recovery(exit: &ExitPolicySpec) -> bool {
    matches!(
        (exit.settlement_mode, exit.redeem_policy),
        (ExitSettlementMode::HoldToResolution, RedeemPolicy::Auto)
    )
}

#[cfg(test)]
mod tests {
    use chrono::Utc;
    use quant_pivot_models::{
        enums::quant::{ExitSettlementMode, RedeemPolicy},
        types::{
            Bps, ExitPolicySpec, OpportunisticExitPolicy, Price, Probability,
            ThesisInvalidationPolicy,
        },
    };
    use rust_decimal_macros::dec;

    use super::requires_automatic_settlement_recovery;

    fn exit(settlement_mode: ExitSettlementMode, redeem_policy: RedeemPolicy) -> ExitPolicySpec {
        ExitPolicySpec {
            take_profit_price: None,
            take_profit_pct: None,
            stop_loss_price: None,
            stop_loss_pct: None,
            time_exit_at: None,
            max_hold_secs: None,
            trailing_stop: None,
            thesis_invalidation: ThesisInvalidationPolicy {
                min_score_retention: dec!(0.6),
                min_expected_return_bps: Bps::ZERO,
                require_execution_eligibility: true,
            },
            opportunistic_exit: OpportunisticExitPolicy {
                min_confidence: Probability::new(dec!(0.6)),
                min_expected_alpha_bps: Bps::ZERO,
                min_p_exit_better: Probability::new(dec!(0.5)),
                max_cumulative_exit_pct: dec!(1),
                min_incremental_exit_pct: dec!(0.1),
            },
            scale_out_targets: Vec::new(),
            settlement_mode,
            redeem_policy,
            manual_review_at: Some(Utc::now()),
            entry_reference_price: Price::new(dec!(0.5)),
            entry_composite_score: Probability::new(dec!(0.5)),
        }
    }

    #[test]
    fn only_hold_to_resolution_auto_requires_system_recovery() {
        assert!(requires_automatic_settlement_recovery(&exit(
            ExitSettlementMode::HoldToResolution,
            RedeemPolicy::Auto,
        )));
        assert!(!requires_automatic_settlement_recovery(&exit(
            ExitSettlementMode::HoldToResolution,
            RedeemPolicy::Manual,
        )));
        assert!(!requires_automatic_settlement_recovery(&exit(
            ExitSettlementMode::ExitBeforeResolution,
            RedeemPolicy::Auto,
        )));
    }
}

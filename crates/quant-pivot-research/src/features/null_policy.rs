//! The null-policy engine: the only sanctioned way to handle a missing or
//! out-of-range feature value.
//!
//! Silent zero is forbidden. Every absent value resolves to exactly one of three
//! decisions, derived from the feature's [`NullPolicy`], its `critical` flag,
//! whether the active model requires it, and the runtime domain/staleness
//! policies.

use quant_pivot_models::{
    runtime_config::{DataQualityConfig, FeatureStalenessPolicy},
    types::Probability,
};
use rust_decimal::{Decimal, prelude::ToPrimitive};

use crate::features::{
    schema::{FeatureSpec, NullPolicy},
    value::{FeatureValue, FeatureValueKind, NullReason},
};

/// The resolution of a missing / out-of-range feature value.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum NullDecision {
    /// The market must not enter the candidate set.
    Reject(NullReason),
    /// Substitute an audited neutral value.
    Substitute {
        /// The substituted value.
        value: FeatureValue,
    },
    /// Keep the value missing; the model proceeds.
    KeepMissing {
        /// Why the value is missing.
        reason: NullReason,
        /// Whether this missingness degrades the vector's data quality.
        degrade: bool,
    },
}

/// Stateless decision engine over the four-state null policy.
pub struct NullPolicyEngine;

impl NullPolicyEngine {
    /// Decide how to handle an absent value for `spec`.
    ///
    /// `reason` is why the value is absent (unavailable, stale, out of range,
    /// domain gap, insufficient history). `is_required` is true when the active
    /// model lists this feature in its required set.
    ///
    /// Domain-missing values are always kept missing (the generic model
    /// proceeds): imputation of a vertical gap is a governed model-layer concern
    /// (3.4), never a silent feature-plane substitution.
    #[must_use]
    pub fn decide(
        spec: &FeatureSpec,
        reason: NullReason,
        data_quality: &DataQualityConfig,
        is_required: bool,
    ) -> NullDecision {
        // Required or critical features must be present — with one exception:
        // a stale value under an `AllowDegraded` staleness policy degrades
        // rather than rejects.
        if is_required || spec.critical {
            if reason == NullReason::StaleBeyondPolicy
                && data_quality.feature_staleness_policy == FeatureStalenessPolicy::AllowDegraded
            {
                return NullDecision::KeepMissing {
                    reason,
                    degrade: true,
                };
            }
            return NullDecision::Reject(reason);
        }

        match &spec.null_policy {
            NullPolicy::RejectMarket => NullDecision::Reject(reason),
            // A neutral value only exists for numeric kinds; a non-numeric kind
            // (e.g. category) configured with a neutral policy cannot substitute
            // and rejects instead of fabricating a value.
            NullPolicy::NeutralValue(neutral) => neutral_value(spec.value_kind, *neutral)
                .map_or(NullDecision::Reject(reason), |value| {
                    NullDecision::Substitute { value }
                }),
            NullPolicy::Penalize => NullDecision::KeepMissing {
                reason,
                degrade: true,
            },
            NullPolicy::DomainMissing => NullDecision::KeepMissing {
                reason,
                degrade: false,
            },
        }
    }
}

/// Build a neutral value of the given kind from an audited decimal, or `None`
/// when the kind has no representable neutral value (categorical).
fn neutral_value(kind: FeatureValueKind, value: Decimal) -> Option<FeatureValue> {
    match kind {
        FeatureValueKind::Decimal => Some(FeatureValue::Decimal(value)),
        FeatureValueKind::Probability => Some(FeatureValue::Probability(Probability::new(value))),
        FeatureValueKind::Bps => Some(FeatureValue::Bps(value)),
        FeatureValueKind::Usd => Some(FeatureValue::Usd(value.into())),
        FeatureValueKind::Count => Some(FeatureValue::Count(value.to_u64().unwrap_or(0))),
        FeatureValueKind::Bool => Some(FeatureValue::Bool(!value.is_zero())),
        FeatureValueKind::Category => None,
    }
}

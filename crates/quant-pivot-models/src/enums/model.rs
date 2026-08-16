//! Model family taxonomy persisted on `quant_model_spec.model_family`.

wire_enum! {
    /// Classical payout-classification model kind (smartcore-backed).
    pub enum ClassicalKind {
        LogisticRegression => "logistic_regression",
    }
}
pg_enum! {
    type_name = "qp_model_family",
    @derive(schemars::JsonSchema)
    pub enum ModelFamily {
        WeightedFactor => "weighted_factor",
        ClassicalLogisticRegression => "classical_logistic_regression",
        /// Sell-side hold-vs-exit weighted scorer. Distinct family
        /// from the Buy-side `WeightedFactor` ranker so a Sell artifact can never
        /// be confused for a Buy artifact at the registry / runtime boundary.
        HoldVsExitWeighted => "hold_vs_exit_weighted",
    }
}

/// Canonical serving authority of a model family.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ServingEligibility {
    /// Buy-side family that may become active after payout calibration is sealed.
    ActiveBuyCapable,
    /// Research/shadow inference only; never admitted to a report or route.
    ShadowOnly,
    /// Sell-side hold-vs-exit serving only.
    ExitOnly,
}

impl ModelFamily {
    /// Single family-level serving eligibility used across every admission path.
    #[must_use]
    pub const fn serving_eligibility(self) -> ServingEligibility {
        match self {
            Self::WeightedFactor => ServingEligibility::ActiveBuyCapable,
            Self::ClassicalLogisticRegression => ServingEligibility::ShadowOnly,
            Self::HoldVsExitWeighted => ServingEligibility::ExitOnly,
        }
    }

    /// Whether this family is a classical (smartcore-backed) estimator.
    #[must_use]
    pub const fn is_classical(self) -> bool {
        !matches!(self, Self::WeightedFactor | Self::HoldVsExitWeighted)
    }

    /// Whether this family scores the Sell-side hold-vs-exit decision.
    #[must_use]
    pub const fn is_exit_scorer(self) -> bool {
        matches!(self, Self::HoldVsExitWeighted)
    }

    #[must_use]
    pub const fn classical_kind(self) -> Option<ClassicalKind> {
        match self {
            Self::WeightedFactor | Self::HoldVsExitWeighted => None,
            Self::ClassicalLogisticRegression => Some(ClassicalKind::LogisticRegression),
        }
    }

    #[must_use]
    pub const fn from_classical(kind: ClassicalKind) -> Self {
        match kind {
            ClassicalKind::LogisticRegression => Self::ClassicalLogisticRegression,
        }
    }
}

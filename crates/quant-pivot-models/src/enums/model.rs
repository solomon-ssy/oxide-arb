//! Model family taxonomy persisted on `quant_model_spec.model_family`.

wire_enum! {
    /// Classical ML model kind (smartcore-backed).
    pub enum ClassicalKind {
        GradientBoostedTrees => "gradient_boosted_trees",
        RandomForest => "random_forest",
        ExtraTrees => "extra_trees",
        LogisticRegression => "logistic_regression",
        Ridge => "ridge",
        Lasso => "lasso",
        ElasticNet => "elastic_net",
    }
}
pg_enum! {
    type_name = "qp_model_family",
    @derive(schemars::JsonSchema)
    pub enum ModelFamily {
        WeightedFactor => "weighted_factor",
        ClassicalGradientBoostedTrees => "classical_gradient_boosted_trees",
        ClassicalRandomForest => "classical_random_forest",
        ClassicalExtraTrees => "classical_extra_trees",
        ClassicalLogisticRegression => "classical_logistic_regression",
        ClassicalRidge => "classical_ridge",
        ClassicalLasso => "classical_lasso",
        ClassicalElasticNet => "classical_elastic_net",
        /// Sell-side hold-vs-exit weighted scorer. Distinct family
        /// from the Buy-side `WeightedFactor` ranker so a Sell artifact can never
        /// be confused for a Buy artifact at the registry / runtime boundary.
        HoldVsExitWeighted => "hold_vs_exit_weighted",
    }
}

impl ModelFamily {
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
            Self::ClassicalGradientBoostedTrees => Some(ClassicalKind::GradientBoostedTrees),
            Self::ClassicalRandomForest => Some(ClassicalKind::RandomForest),
            Self::ClassicalExtraTrees => Some(ClassicalKind::ExtraTrees),
            Self::ClassicalLogisticRegression => Some(ClassicalKind::LogisticRegression),
            Self::ClassicalRidge => Some(ClassicalKind::Ridge),
            Self::ClassicalLasso => Some(ClassicalKind::Lasso),
            Self::ClassicalElasticNet => Some(ClassicalKind::ElasticNet),
        }
    }

    #[must_use]
    pub const fn from_classical(kind: ClassicalKind) -> Self {
        match kind {
            ClassicalKind::GradientBoostedTrees => Self::ClassicalGradientBoostedTrees,
            ClassicalKind::RandomForest => Self::ClassicalRandomForest,
            ClassicalKind::ExtraTrees => Self::ClassicalExtraTrees,
            ClassicalKind::LogisticRegression => Self::ClassicalLogisticRegression,
            ClassicalKind::Ridge => Self::ClassicalRidge,
            ClassicalKind::Lasso => Self::ClassicalLasso,
            ClassicalKind::ElasticNet => Self::ClassicalElasticNet,
        }
    }
}

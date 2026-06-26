//! Model family taxonomy persisted on `quant_model_spec.model_family`.

crate::wire_enum! {
    /// Classical ML model kind (smartcore-backed).
    pub enum ClassicalKind {
        RandomForest => "random_forest",
        ExtraTrees => "extra_trees",
        LogisticRegression => "logistic_regression",
        Ridge => "ridge",
        Lasso => "lasso",
        ElasticNet => "elastic_net",
    }
}

crate::pg_enum! {
    type_name = "qp_model_family",
    pub enum ModelFamily {
        WeightedFactor => "weighted_factor",
        ClassicalRandomForest => "classical_random_forest",
        ClassicalExtraTrees => "classical_extra_trees",
        ClassicalLogisticRegression => "classical_logistic_regression",
        ClassicalRidge => "classical_ridge",
        ClassicalLasso => "classical_lasso",
        ClassicalElasticNet => "classical_elastic_net",
    }
}

impl ModelFamily {
    #[must_use]
    pub const fn is_classical(self) -> bool {
        !matches!(self, Self::WeightedFactor)
    }

    #[must_use]
    pub const fn classical_kind(self) -> Option<ClassicalKind> {
        match self {
            Self::WeightedFactor => None,
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
            ClassicalKind::RandomForest => Self::ClassicalRandomForest,
            ClassicalKind::ExtraTrees => Self::ClassicalExtraTrees,
            ClassicalKind::LogisticRegression => Self::ClassicalLogisticRegression,
            ClassicalKind::Ridge => Self::ClassicalRidge,
            ClassicalKind::Lasso => Self::ClassicalLasso,
            ClassicalKind::ElasticNet => Self::ClassicalElasticNet,
        }
    }
}

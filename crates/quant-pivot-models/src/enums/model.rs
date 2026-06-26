//! Model family taxonomy persisted on `quant_model_spec.model_family`.

use std::{
    fmt::{self, Display, Formatter},
    str::FromStr,
};

/// Classical ML model kind (smartcore-backed).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ClassicalKind {
    RandomForest,
    ExtraTrees,
    LogisticRegression,
    Ridge,
    Lasso,
    ElasticNet,
}

impl ClassicalKind {
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::RandomForest => "random_forest",
            Self::ExtraTrees => "extra_trees",
            Self::LogisticRegression => "logistic_regression",
            Self::Ridge => "ridge",
            Self::Lasso => "lasso",
            Self::ElasticNet => "elastic_net",
        }
    }
}

impl Display for ClassicalKind {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

impl FromStr for ClassicalKind {
    type Err = ParseModelFamilyError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "random_forest" => Ok(Self::RandomForest),
            "extra_trees" => Ok(Self::ExtraTrees),
            "logistic_regression" => Ok(Self::LogisticRegression),
            "ridge" => Ok(Self::Ridge),
            "lasso" => Ok(Self::Lasso),
            "elastic_net" => Ok(Self::ElasticNet),
            other => Err(ParseModelFamilyError {
                value: other.to_owned(),
            }),
        }
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

impl FromStr for ModelFamily {
    type Err = ParseModelFamilyError;

    fn from_str(label: &str) -> Result<Self, Self::Err> {
        use sea_orm::Iterable;

        Self::iter()
            .find(|variant| variant.as_str() == label)
            .ok_or_else(|| ParseModelFamilyError {
                value: label.to_owned(),
            })
    }
}

/// Error parsing a [`ModelFamily`] / [`ClassicalKind`] label.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ParseModelFamilyError {
    pub value: String,
}

impl Display for ParseModelFamilyError {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        write!(f, "unrecognized model family: {:?}", self.value)
    }
}

impl std::error::Error for ParseModelFamilyError {}

//! Canonical evidence contracts shared by materialization stages and builders.

pub mod metric;
pub mod query;
pub mod training;

pub use metric::EvidenceMetric;
pub use query::{
    EvidenceIssue, EvidenceIssueSeverity, EvidenceQueryResult, EvidenceSourceRef,
    EvidenceStageOutcome, QueryContract,
};
pub use training::{
    EvidenceSourceRefs, FactorFeature, FactorFeatureRef, FactorFeatureValue, FactorFeatureVector,
    FactorLabel, FactorLabelRef, FactorTrainingExample,
};

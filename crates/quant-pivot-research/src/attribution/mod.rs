//! Immutable prediction explanation, policy counterfactual, association, and
//! execution-trajectory artifacts.
//!
//! The vocabulary is intentionally split along epistemic boundaries:
//! prediction explanations allocate a model output, decision counterfactuals
//! replay a versioned model/policy intervention, and outcome associations are
//! explicitly non-causal. Execution trajectories and alternative-policy
//! outcomes are derived artifacts; they never mutate canonical execution truth.

mod artifacts;
mod tree_shap;

pub use artifacts::{
    AlternativeExitPolicy, AssociationEstimate, AssociationInterpretation, AttributionArtifact,
    AttributionArtifactCodec, AttributionLineage, CounterfactualIntervention, DecisionCandidateKey,
    DecisionCandidateScore, DecisionCounterfactualArtifact, DecisionCounterfactualInput,
    DecisionReplay, DecisionReplayPolicy, DecisionReplayScope, ExecutionTrajectoryArtifact,
    ExecutionTrajectoryInput, OutcomeAssociationArtifact, OutcomeAssociationInput,
    OutcomeAssociationSample, OutcomeAssociationTarget, PolicyCounterfactualOutcome,
    PredictionContribution, PredictionExplanationArtifact, PredictionExplanationMethod,
    PredictionOutputKind, TrajectoryPoint, WeightedExplanationInput,
    WeightedFactorExplanationInput, WeightedTerm,
};
pub use tree_shap::{
    DecisionTreeSpec, MissingBranch, TreeEnsembleInput, TreeEnsembleSpec, TreeNode,
    TreeShapExplainer, TreeShapModelContract,
};

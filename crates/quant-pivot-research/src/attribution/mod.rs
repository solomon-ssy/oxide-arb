//! Immutable prediction explanation, decision intervention replay, policy
//! counterfactual, association, and
//! execution-trajectory artifacts.
//!
//! The vocabulary is intentionally split along epistemic boundaries:
//! prediction explanations allocate a model output, model interventions stop
//! at the explicit global-economic reoptimization boundary, and outcome
//! associations are explicitly non-causal. Execution trajectories and
//! alternative-policy outcomes are derived artifacts; they never mutate
//! canonical execution truth.

mod artifacts;
mod tree_shap;

pub use artifacts::{
    ActualBaselineNotEvaluableReason, ActualExecutionBaseline, AlternativeExitPolicy,
    AssociationEstimate, AssociationInterpretation, AttributionArtifact, AttributionArtifactCodec,
    AttributionLineage, DecisionCandidateKey, DecisionComputationGraph, DecisionGraphEdge,
    DecisionGraphNode, DecisionGraphNodeKind, DecisionGraphPath, DecisionIntervention,
    DecisionInterventionAttempt, DecisionInterventionEvaluation,
    DecisionInterventionNotEvaluableReason, DecisionInterventionOutcome,
    DecisionInterventionReplayArtifact, DecisionInterventionReplayInput,
    DecisionInterventionSupport, DecisionReplay, DecisionReplayPolicy, DecisionReplayScope,
    ExecutionOutcomeAssociationArtifact, ExecutionOutcomeAssociationInput,
    ExecutionOutcomeAssociationSample, ExecutionOutcomeAssociationTarget, ExecutionOutcomeBinding,
    ExecutionTrajectoryArtifact, ExecutionTrajectoryInput, PolicyCounterfactualEvaluation,
    PolicyCounterfactualNotEvaluableReason, PolicyCounterfactualOutcome, PredictionContribution,
    PredictionExplanationArtifact, PredictionExplanationMethod, PredictionOutputKind,
    ResolutionOutcomeAssociationArtifact, ResolutionOutcomeAssociationInput,
    ResolutionOutcomeAssociationSample, ResolutionOutcomeAssociationTarget,
    TrajectoryExcursionEvaluation, TrajectoryPoint, TrajectoryPointEconomics,
    TrajectoryPointNotEvaluableReason, WeightedExplanationInput, WeightedFactorExplanationInput,
    WeightedTerm,
};
pub use tree_shap::{
    DecisionTreeSpec, MissingBranch, TreeEnsembleInput, TreeEnsembleSpec, TreeInputSupport,
    TreeNode, TreeShapExplainer, TreeShapModelContract,
};

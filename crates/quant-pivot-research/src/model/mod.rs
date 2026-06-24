//! Model plane: unified runtime contract, strongly-typed signal candidate,
//! serialized artifacts, and the training contract.
//!
//! Online closure terminus (3.4): `FactorValue → QuantModelRuntime →
//! SignalCandidate`. The runtime/factory traits and the artifact shell are
//! fixed here; 3.4 implements the weighted-factor runtime and 3.6 the
//! classical trainer/runtime.

pub mod artifact;
pub mod degrade;
pub mod factory;
pub mod runtime;
pub mod signal;
pub mod trainer;
pub mod weighted;

pub use artifact::{
    CalibratedReturnModel, ClassicalModelArtifact, DataQualityMultipliers, FactorWeight,
    HeuristicReturnModel, HorizonMultipliers, LiquidityMultipliers, LiquidityTier, ModelArtifact,
    ModelArtifactHeader, ReturnCurvePoint, ReturnEstimate, ReturnModelSpec, ScoreMultiplierSpec,
    SubstitutionConfidenceRules, TrainingObjectiveReport, WeightedFactorModelArtifact,
};
pub use degrade::{DegradeAction, InferenceStage, degrade_action};
pub use factory::{
    ActiveSchemaBinding, DefaultModelRuntimeFactory, DefaultModelRuntimeFactoryBuilder,
    ModelRuntimeFactoryBuilder,
};
pub use runtime::{
    ClassicalKind, FactorInferenceRow, FactorInferenceTable, InferenceMatrix, InferenceMatrixRow,
    MarketInferenceContext, ModelFamily, ModelRuntimeFactory, ModelRuntimeInput,
    ModelRuntimeMetrics, ModelRuntimeOutput, ModelRuntimeWarning, ParseModelFamilyError,
    QuantModelRuntime,
};
pub use signal::{
    FactorContribution, ModelExplanation, SignalCandidate, SignalWarning, signal_candidate_event,
    signal_candidate_events,
};
pub use trainer::{ModelTrainer, TrainModelRequest, TrainedModelArtifact};
pub use weighted::WeightedFactorRuntime;

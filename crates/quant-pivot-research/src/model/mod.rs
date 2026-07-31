//! Model plane: unified runtime contract, strongly-typed signal candidate,
//! serialized artifacts, and the training contract.
//!
//! Online closure terminus: `FactorValue → QuantModelRuntime →
//! SignalCandidate`. The runtime contract and artifact shell support the
//! weighted-factor runtime and the feature-gated classical trainer/runtime.

pub mod artifact;
pub mod calibrator;
pub mod category_scope;
#[cfg(feature = "ml-classical")]
pub mod classical;
#[cfg(feature = "ml-classical")]
pub mod classical_runtime;
pub mod degrade;
pub mod factor_heads;
pub mod favorite_longshot;
pub mod objective;
#[cfg(feature = "optimize")]
pub mod optimize;
pub mod reliability;
pub mod runtime;
pub mod score_percentile;
pub mod sell_scorer;
pub mod signal;
pub mod trainer;
pub mod weighted;

pub use artifact::{
    CalibratedReturnModel, ClassicalModelMetrics, ClassicalOutputSemantics, DataQualityMultipliers,
    EncodedColumn, EncodedColumnKind, EncodedColumnName, FeatureImportance, FittedInputColumn,
    FittedInputTransform, HeuristicReturnModel, HorizonMultipliers, LiquidityMultipliers,
    LiquidityTier, ModelArtifact, ModelArtifactHeader, ReturnEstimate, ReturnModelSpec,
    ScoreMultiplierSpec, SellScorerOutputSpec, SubstitutionConfidenceRules,
    TrainingObjectiveReport, model_input_contract_hash,
};
pub use calibrator::{
    CalibrationArtifactLoader, ProbabilityCalibrator, ResolvedCalibration, apply_mapping,
    isotonic::IsotonicCalibrator, platt::PlattCalibrator, validate_mapping,
};
#[cfg(feature = "ml-classical")]
pub use classical::{
    CLASSICAL_CRATE_NAME, CLASSICAL_CRATE_VERSION, ClassicalAdapterRegistry, ClassicalModelAdapter,
    ClassicalParams, ClassicalTrainOutput, ForestParams, LinearParams, replay_training_matrix,
};
#[cfg(feature = "ml-classical")]
pub use classical_runtime::{ClassicalDecisionProjection, ClassicalRuntime};
pub use degrade::{DegradeAction, InferenceStage};
pub use favorite_longshot::{BiasFitConfig, BiasSample, FavoriteLongshotBiasTable};
pub use objective::{ObjectiveComponentReport, RankingDiagnostics};
pub use reliability::{ReliabilitySample, compute_reliability};
pub use runtime::{
    FactorInferenceRow, FactorInferenceTable, InferenceMatrix, InferenceMatrixRow,
    MarketInferenceContext, ModelInputAuditRow, ModelInputAuditState, ModelRuntimeInput,
    ModelRuntimeMetrics, ModelRuntimeOutput, QuantModelRuntime,
};
pub use score_percentile::annotate;
pub use sell_scorer::{
    LotStateInput, PositionStateFeatures, SellScore, SellScoreInput, SellScorerRuntime,
    SellScorerTrainer, SellSignalPolicy, TrainSellScorerRequest, WeightedSellScorerRuntime,
    sell_signal_fires, sell_signal_target,
};
pub use signal::{
    FactorContribution, ModelExplanation, SignalCandidate, SignalWarning,
    canonical_business_prediction_hash, signal_candidate_event, signal_candidate_events,
};
pub use trainer::{
    CancellationProbe, LabelSelector, ModelTrainer, TrainModelRequest, ValidationReport,
    ValidationSpec, WeightedFactorTrainer, fit_frozen_reference_quantiles,
    weighted_training_input_hash,
};
pub use weighted::WeightedFactorRuntime;

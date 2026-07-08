//! Model plane: unified runtime contract, strongly-typed signal candidate,
//! serialized artifacts, and the training contract.
//!
//! Online closure terminus (3.4): `FactorValue → QuantModelRuntime →
//! SignalCandidate`. The runtime/factory traits and the artifact shell are
//! fixed here; 3.4 implements the weighted-factor runtime and 3.6 the
//! classical trainer/runtime.

pub mod artifact;
pub mod calibration;
#[cfg(feature = "ml-classical")]
pub mod classical;
#[cfg(feature = "ml-classical")]
pub mod classical_runtime;
pub mod degrade;
pub mod factory;
pub mod favorite_longshot;
#[cfg(feature = "optimize")]
pub mod optimize;
pub mod overlay;
pub mod rank_scores;
pub mod routing;
pub mod runtime;
pub mod score_percentile;
pub mod sell_scorer;
pub mod signal;
pub mod trainer;
pub mod weighted;

pub use artifact::{
    CalibratedReturnModel, ClassicalModelArtifact, ClassicalModelMetrics, DataQualityMultipliers,
    FactorWeight, FeatureImportance, HeuristicReturnModel, HorizonMultipliers,
    LiquidityMultipliers, LiquidityTier, ModelArtifact, ModelArtifactHeader, PreprocessingArtifact,
    ReturnCurvePoint, ReturnEstimate, ReturnModelSpec, ScoreMultiplierSpec, SellScorerArtifact,
    SellScorerOutputSpec, SubstitutionConfidenceRules, TrainingObjectiveReport,
    WeightedFactorModelArtifact,
};
pub use calibration::{
    CalibrationReport, CalibrationResult, CalibrationSample, StratumFit,
    calibrate_horizon_multipliers, calibrate_liquidity_multipliers, calibrate_return_model,
    calibrate_score_multipliers, calibrate_substitution_rules, calibrate_weighted_artifact,
};
#[cfg(feature = "ml-classical")]
pub use classical::{
    CLASSICAL_CRATE_NAME, CLASSICAL_CRATE_VERSION, ClassicalAdapterRegistry, ClassicalModelAdapter,
    ClassicalParams, ClassicalTrainOutput, ForestParams, LinearParams,
};
#[cfg(feature = "ml-classical")]
pub use classical_runtime::ClassicalRuntime;
pub use degrade::{DegradeAction, InferenceStage, degrade_action};
pub use factory::{
    ActiveSchemaBinding, DefaultModelRuntimeFactory, DefaultModelRuntimeFactoryBuilder,
    ModelRuntimeFactoryBuilder, load_hash_verified_artifact,
};
pub use favorite_longshot::{
    BiasFitConfig, BiasSample, CategoryBiasCurve, FavoriteLongshotBiasTable, PriceBiasBin,
};
pub use overlay::{WeightOverlay, WeightSource};
pub use rank_scores::{RankScores, attach as attach_rank_scores};
pub use routing::{
    ModelRouting, generic_model_version_id, resolve_model_route, version_id_for_category,
};
pub use runtime::{
    ClassicalKind, FactorInferenceRow, FactorInferenceTable, InferenceMatrix, InferenceMatrixRow,
    MarketInferenceContext, ModelFamily, ModelFamilyParseError, ModelRuntimeFactory,
    ModelRuntimeInput, ModelRuntimeMetrics, ModelRuntimeOutput, ModelRuntimeWarning,
    QuantModelRuntime,
};
pub use score_percentile::annotate;
pub use sell_scorer::{
    LotStateInput, PositionStateFeatures, SellScore, SellScoreInput, SellScorerRuntime,
    SellScorerTrainer, TrainSellScorerRequest, WeightedSellScorerRuntime,
    position_state_factor_values, position_state_features,
};
pub use signal::{
    FactorContribution, ModelExplanation, SignalCandidate, SignalWarning, signal_candidate_event,
    signal_candidate_events,
};
pub use trainer::{
    LabelSelector, ModelTrainer, Regularization, TrainModelRequest, TrainedModelArtifact,
    TrainingObjective, ValidationReport, ValidationSpec, WeightedFactorTrainer,
};
pub use weighted::WeightedFactorRuntime;

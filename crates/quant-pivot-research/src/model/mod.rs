//! Model plane: unified runtime contract, strongly-typed signal candidate,
//! serialized artifacts, and the training contract.
//!
//! Online closure terminus: `FactorValue → QuantModelRuntime →
//! SignalCandidate`. The runtime/factory traits and artifact shell support the
//! weighted-factor runtime and the feature-gated classical trainer/runtime.

pub mod artifact;
pub mod calibration;
pub mod calibrator;
pub mod category_scope;
#[cfg(feature = "ml-classical")]
pub mod classical;
#[cfg(feature = "ml-classical")]
pub mod classical_runtime;
pub mod degrade;
pub mod factory;
pub mod favorite_longshot;
pub mod objective;
#[cfg(feature = "optimize")]
pub mod optimize;
pub mod overlay;
pub mod reliability;
pub mod routing;
pub mod runtime;
pub mod score_percentile;
pub mod sell_scorer;
pub mod signal;
pub mod trainer;
pub mod weighted;

pub use artifact::{
    CalibratedReturnModel, ClassicalModelArtifact, ClassicalModelMetrics, ClassicalOutputSemantics,
    DataQualityMultipliers, EncodedColumn, EncodedColumnKind, EncodedColumnName, FactorWeight,
    FeatureImportance, FittedInputColumn, FittedInputTransform, HeuristicReturnModel,
    HorizonMultipliers, LiquidityMultipliers, LiquidityTier, ModelArtifact, ModelArtifactHeader,
    ReturnEstimate, ReturnModelSpec, ScoreMultiplierSpec, SellScorerArtifact, SellScorerOutputSpec,
    SubstitutionConfidenceRules, TrainingObjectiveReport, WeightedFactorModelArtifact,
    model_input_contract_hash,
};
pub use calibration::{
    CalibrationResult, CalibrationSample, calibrate_horizon_multipliers,
    calibrate_liquidity_multipliers, calibrate_score_multipliers, calibrate_substitution_rules,
    calibrate_weighted_artifact,
};
pub use calibrator::{
    CalibrationArtifactLoader, ProbabilityCalibrator, ResolvedCalibration, apply_mapping,
    isotonic::IsotonicCalibrator, platt::PlattCalibrator, validate_mapping,
};
pub use category_scope::{infer_training_category_scope, validate_category_scope_weights};
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
pub use favorite_longshot::{BiasFitConfig, BiasSample, FavoriteLongshotBiasTable};
pub use objective::{ObjectiveComponentReport, RankingDiagnostics};
pub use overlay::WeightOverlay;
pub use reliability::{ReliabilitySample, compute_reliability};
pub use routing::{
    ModelRouting, generic_model_version_id, resolve_model_route, version_id_for_category,
};
pub use runtime::{
    FactorInferenceRow, FactorInferenceTable, InferenceMatrix, InferenceMatrixRow,
    MarketInferenceContext, ModelInputAuditRow, ModelInputAuditState, ModelRuntimeFactory,
    ModelRuntimeInput, ModelRuntimeMetrics, ModelRuntimeOutput, QuantModelRuntime,
};
pub use score_percentile::annotate;
pub use sell_scorer::{
    LotStateInput, PositionStateFeatures, SellScore, SellScoreInput, SellScorerRuntime,
    SellScorerTrainer, SellSignalPolicy, TrainSellScorerRequest, WeightedSellScorerRuntime,
    position_state_features, position_state_signed, sell_signal_fires, sell_signal_target,
};
pub use signal::{
    FactorContribution, ModelExplanation, SignalCandidate, SignalWarning,
    canonical_business_prediction_hash, signal_candidate_event, signal_candidate_events,
};
pub use trainer::{
    LabelSelector, ModelTrainer, TrainModelRequest, TrainedModelArtifact, ValidationReport,
    ValidationSpec, WeightedFactorTrainer, fit_frozen_reference_quantiles,
    weighted_training_input_hash,
};
pub use weighted::WeightedFactorRuntime;

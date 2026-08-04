//! Classical-ML adapters over `smartcore` behind the `ml-classical` feature.
//!
//! The business layer never sees a `smartcore` concrete type: training produces
//! a [`ClassicalTrainOutput`] (serialized estimator bytes + metadata) and the
//! runtime ([`crate::model::classical_runtime`]) consumes it behind
//! `dyn QuantModelRuntime`. The estimator union is `bincode`-serialized; loading
//! verifies the recorded crate version before deserializing.
//!
//! Seven production kinds are supported, dispatched through
//! [`ClassicalAdapterRegistry`]: the tree ensembles (`RandomForest`,
//! `ExtraTrees`, `GradientBoostedTrees`) and penalized linear models (`Ridge`,
//! `Lasso`, `ElasticNet`) are continuous return-proxy **regressors** (their
//! output ranks markets); the `LogisticRegression` **classifier** emits a true
//! yes-probability. GBDT additionally freezes an exact portable tree ensemble,
//! its training-background covers, and `TreeSHAP` verification. Every kind shares
//! the frozen standardization preprocessing so inference applies the exact
//! transform the model was fit on, and a shared time-ordered holdout produces
//! an out-of-sample validation objective.

use ndarray::Array1;
use quant_pivot_error::{QuantError, QuantResult, research::ResearchError};
use quant_pivot_models::{
    enums::{model::ClassicalKind, quant::ModelSerializationFormat},
    hashing::CanonicalDigest,
    types::{ContentHash, ModelInputContract, ModelInputRequiredness, ModelInputSpec},
};
use rust_decimal::{Decimal, prelude::FromPrimitive};
use serde::{Deserialize, Serialize};
use smartcore::{
    ensemble::{
        extra_trees_regressor::{ExtraTreesRegressor, ExtraTreesRegressorParameters},
        random_forest_regressor::{RandomForestRegressor, RandomForestRegressorParameters},
    },
    error::Failed,
    linalg::basic::{
        arrays::{Array, MutArray},
        matrix::DenseMatrix,
    },
    linear::{
        elastic_net::{ElasticNet, ElasticNetParameters},
        lasso::{Lasso, LassoParameters},
        logistic_regression::{LogisticRegression, LogisticRegressionParameters},
        ridge_regression::{RidgeRegression, RidgeRegressionParameters},
    },
    xgboost::{
        XGRegressor, XGRegressorParameters,
        xgb_regressor::{XG_TYPED_EXPORT_REVISION, XGModelExport, XGTreeNodeExport},
    },
};

use crate::{
    attribution::{
        DecisionTreeSpec, MissingBranch, TreeEnsembleInput, TreeEnsembleSpec, TreeInputSupport,
        TreeNode, TreeShapModelContract,
    },
    model::{
        artifact::{
            ClassicalModelMetrics, FeatureImportance, FittedInputTransform,
            model_input_contract_hash,
        },
        trainer::{CancellationProbe, ValidationReport, ValidationSpec},
    },
    precision::RESEARCH_DECIMAL_SCALE,
    stats,
    training::{DenseInputMatrix, TrainingMatrix, training_input_hash},
    validation::{DefaultPurgedSplitter, PurgeConfig, PurgedSplitter, TimelineGroup},
};

/// The ML crate + version stamped onto every classical artifact (load-time
/// mismatch is rejected.
pub const CLASSICAL_CRATE_NAME: &str = "smartcore";
/// Recorded crate version (major.minor of the workspace `smartcore` dependency).
pub const CLASSICAL_CRATE_VERSION: &str = "0.5";

/// Concrete `smartcore` regressor type aliases (`f64` features + targets, dense
/// matrix design, dense target vector).
type Forest = RandomForestRegressor<f64, f64, DenseMatrix<f64>, Vec<f64>>;
type ExtraForest = ExtraTreesRegressor<f64, f64, DenseMatrix<f64>, Vec<f64>>;
type GradientBoostedTrees = XGRegressor<f64, f64, DenseMatrix<f64>, Vec<f64>>;
type RidgeModel = RidgeRegression<f64, f64, DenseMatrix<f64>, Vec<f64>>;
type LassoModel = Lasso<f64, f64, DenseMatrix<f64>, Vec<f64>>;
type ElasticNetModel = ElasticNet<f64, f64, DenseMatrix<f64>, Vec<f64>>;
/// Binary logistic classifier: `i64` class labels (`{0, 1}`).
type Logistic = LogisticRegression<f64, i64, DenseMatrix<f64>, Vec<i64>>;

/// Tree-ensemble hyperparameters (`RandomForest`, `ExtraTrees`).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ForestParams {
    /// Deterministic RNG seed (reproducible forests).
    pub seed: u64,
    /// Number of trees in the ensemble.
    pub n_trees: usize,
    /// Maximum tree depth (`None` = unbounded).
    pub max_depth: Option<u16>,
    /// Minimum samples per leaf.
    pub min_samples_leaf: usize,
}

impl Default for ForestParams {
    fn default() -> Self {
        Self {
            seed: 0,
            n_trees: 100,
            max_depth: Some(8),
            min_samples_leaf: 1,
        }
    }
}

/// Penalized-linear hyperparameters (`Ridge`, `Lasso`, `ElasticNet`, `Logistic`).
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct LinearParams {
    /// Regularization strength (`alpha`); higher shrinks coefficients more.
    pub alpha: f64,
    /// Elastic-net mixing in `[0, 1]` (`0` = ridge, `1` = lasso); ignored by the
    /// pure ridge / lasso / logistic kinds.
    pub l1_ratio: f64,
    /// Maximum solver iterations (coordinate descent / L-BFGS).
    pub max_iter: usize,
}

impl Default for LinearParams {
    fn default() -> Self {
        Self {
            alpha: 0.01,
            l1_ratio: 0.5,
            max_iter: 1_000,
        }
    }
}

/// The full hyperparameter set for any classical kind (only the subset a kind
/// reads is consulted, so one struct serves every adapter).
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct ClassicalParams {
    /// Tree-ensemble hyperparameters.
    pub forest: ForestParams,
    /// Penalized-linear hyperparameters.
    pub linear: LinearParams,
}

impl ClassicalParams {
    /// Production-tuned defaults for a kind.
    #[must_use]
    pub fn defaults_for(kind: ClassicalKind) -> Self {
        let linear = match kind {
            ClassicalKind::Ridge => LinearParams {
                alpha: 1.0,
                ..LinearParams::default()
            },
            ClassicalKind::LogisticRegression => LinearParams {
                alpha: 0.0,
                ..LinearParams::default()
            },
            _ => LinearParams::default(),
        };
        Self {
            forest: ForestParams::default(),
            linear,
        }
    }
}

/// The output of a classical training run: the serialized estimator + the
/// metadata the core needs to assemble a sealed `ClassicalModelPayload`.
#[derive(Debug, Clone)]
pub struct ClassicalTrainOutput {
    /// The fitted classical kind.
    pub kind: ClassicalKind,
    /// ML crate name.
    pub crate_name: String,
    /// ML crate version.
    pub crate_version: String,
    /// Estimator serialization format.
    pub serialization_format: ModelSerializationFormat,
    /// Serialized estimator bytes (content-addressed + stored by the core).
    pub model_bytes: Vec<u8>,
    /// BLAKE3 hash of the exact serialized estimator bytes.
    pub model_bytes_hash: ContentHash,
    /// Frozen shared training/serving input transform.
    pub input_transform: FittedInputTransform,
    /// Exact ordered raw-input contract owned by the model specification.
    pub input_contract: ModelInputContract,
    /// Canonical hash of [`Self::input_contract`].
    pub input_contract_hash: ContentHash,
    /// Complete fitted transform hash.
    pub input_transform_hash: ContentHash,
    /// Exact estimator-ready rows plus aligned label vector hash.
    pub training_input_hash: ContentHash,
    /// Exact portable `TreeSHAP` contract for GBDT; absent for other families.
    pub tree_shap: Option<TreeShapModelContract>,
    /// Training metrics + feature importances.
    pub metrics: ClassicalModelMetrics,
}

/// Replay a freshly trained estimator over the exact raw training matrix.
///
/// The frozen transform makes this the production-parity verification boundary
/// used by the model train/replay gate; serialized bytes and estimator family
/// are validated before prediction.
pub fn replay_training_matrix(
    output: &ClassicalTrainOutput,
    matrix: &TrainingMatrix,
) -> QuantResult<Vec<f64>> {
    let actual_hash = CanonicalDigest::content_hash_bytes(&output.model_bytes);
    if actual_hash != output.model_bytes_hash {
        return Err(ResearchError::InvalidModelArtifact {
            detail: "classical train output bytes do not match their recorded hash".to_owned(),
        }
        .into());
    }
    let model: SmartcoreModel = bincode::deserialize(&output.model_bytes).map_err(|error| {
        ResearchError::Serialization {
            detail: format!("bincode deserialize classical replay model: {error}"),
        }
    })?;
    if !model.matches_kind(output.kind) {
        return Err(ResearchError::InvalidModelArtifact {
            detail: "classical train output estimator family mismatch".to_owned(),
        }
        .into());
    }
    let encoded = output
        .input_transform
        .apply_range(&matrix.cells, 0..matrix.row_count())?;
    let predictions = model.predict(&(encoded).dense_matrix()?)?;
    if predictions.len() != matrix.row_count()
        || predictions.iter().any(|prediction| !prediction.is_finite())
    {
        return Err(ResearchError::MatrixBuild {
            detail: "classical train/replay produced an invalid prediction vector".to_owned(),
        }
        .into());
    }
    Ok(predictions)
}

/// A classical training adapter. Concrete impls wrap a `smartcore` estimator and
/// its frozen hyperparameters.
pub trait ClassicalModelAdapter: Send + Sync {
    /// The classical kind this adapter produces.
    fn kind(&self) -> ClassicalKind;

    /// Fit the estimator on a standardized training matrix.
    fn train(&self, matrix: &TrainingMatrix) -> QuantResult<ClassicalTrainOutput>;

    /// Out-of-sample walk-forward validation over `validation.folds` contiguous
    /// time-ordered blocks (same `ValidationSpec` purge/embargo semantics as the
    /// weighted trainer).
    fn validate(
        &self,
        matrix: &TrainingMatrix,
        validation: ValidationSpec,
        cancellation: &CancellationProbe,
    ) -> QuantResult<ValidationReport>;
}

/// Constructs the [`ClassicalModelAdapter`] for a kind with production-tuned
/// default hyperparameters. The single dispatch point so the core never matches
/// on a concrete `smartcore` type.
pub struct ClassicalAdapterRegistry;

impl ClassicalAdapterRegistry {
    /// The adapter for `kind`, with default hyperparameters.
    #[must_use]
    pub fn adapter_for(kind: ClassicalKind) -> Box<dyn ClassicalModelAdapter> {
        Box::new(SmartcoreAdapter {
            kind,
            params: ClassicalParams::defaults_for(kind),
        })
    }

    /// Construct the adapter for `kind` with an explicit hyperparameter
    /// override from the governed classical trial grid instead of
    /// [`ClassicalParams::defaults_for`]. Production training always uses
    /// [`Self::adapter_for`]; only CPCV/trial-grid validation folds need to
    /// vary hyperparameters away from the governed production defaults.
    #[must_use]
    pub fn adapter_with_params(
        kind: ClassicalKind,
        params: ClassicalParams,
    ) -> Box<dyn ClassicalModelAdapter> {
        Box::new(SmartcoreAdapter { kind, params })
    }
}

/// The single `smartcore`-backed adapter; the kind selects the estimator.
struct SmartcoreAdapter {
    kind: ClassicalKind,
    params: ClassicalParams,
}

impl ClassicalModelAdapter for SmartcoreAdapter {
    fn kind(&self) -> ClassicalKind {
        self.kind
    }

    fn train(&self, matrix: &TrainingMatrix) -> QuantResult<ClassicalTrainOutput> {
        let rows = (matrix).validate_matrix()?;
        let input_contract = ModelInputContract {
            inputs: matrix
                .columns
                .iter()
                .map(|column| ModelInputSpec {
                    feature_name: column.feature.to_string(),
                    requiredness: if column.required {
                        ModelInputRequiredness::Required
                    } else {
                        ModelInputRequiredness::Optional
                    },
                })
                .collect(),
        };
        let input_contract_hash = model_input_contract_hash(&input_contract)?;
        let (input_transform, standardized) = FittedInputTransform::fit(matrix)?;
        let cols = input_transform.encoded_columns.len();
        let input_transform_hash = input_transform.transform_hash()?;
        let y: Vec<f64> = matrix.labels.to_vec();
        let training_input_hash = training_input_hash(&standardized, &matrix.labels)?;
        let standardized_rows = standardized.rows().map(<[f64]>::to_vec).collect::<Vec<_>>();
        let mut x = (standardized).dense_matrix()?;
        let model = fit_kind(self.kind, &self.params, &x, &y)?;

        let predictions = model.predict(&x)?;
        let importances = ablation_importances(&model, &mut x, &predictions, &input_transform)?;
        let validation_objective = rank_ic_f64(&predictions, &y)?;

        let model_bytes =
            bincode::serialize(&model).map_err(|error| ResearchError::Serialization {
                detail: format!("bincode serialize classical model: {error}"),
            })?;
        let model_bytes_hash = CanonicalDigest::content_hash_bytes(&model_bytes);
        let tree_shap = match self.kind {
            ClassicalKind::GradientBoostedTrees => Some(extract_tree_shap_contract(
                &model,
                model_bytes_hash,
                input_contract_hash,
                input_transform
                    .encoded_columns
                    .iter()
                    .map(|column| column.name.to_string())
                    .collect(),
                &standardized_rows,
                &predictions,
            )?),
            _ => None,
        };

        Ok(ClassicalTrainOutput {
            kind: self.kind,
            crate_name: CLASSICAL_CRATE_NAME.to_owned(),
            crate_version: CLASSICAL_CRATE_VERSION.to_owned(),
            serialization_format: ModelSerializationFormat::Bincode,
            model_bytes,
            model_bytes_hash,
            input_transform,
            input_contract,
            input_contract_hash,
            input_transform_hash,
            training_input_hash,
            tree_shap,
            metrics: ClassicalModelMetrics {
                train_samples: u64::try_from(rows).map_err(|error| ResearchError::MatrixBuild {
                    detail: format!("classical training row count does not fit u64: {error}"),
                })?,
                feature_count: u32::try_from(cols).map_err(|error| ResearchError::MatrixBuild {
                    detail: format!("encoded feature width does not fit u32: {error}"),
                })?,
                validation_objective: validation_objective.round_dp(RESEARCH_DECIMAL_SCALE),
                feature_importances: importances,
            },
        })
    }

    fn validate(
        &self,
        matrix: &TrainingMatrix,
        validation: ValidationSpec,
        cancellation: &CancellationProbe,
    ) -> QuantResult<ValidationReport> {
        rolling_validation(self.kind, &self.params, matrix, validation, cancellation)
    }
}

/// The serde-serializable union of supported `smartcore` estimators.
///
/// `bincode`-encoded into the artifact store; never exposed to the business
/// layer. The logistic variant stores the extracted binary coefficients +
/// intercept (not the estimator) so inference is a pure dot-product + sigmoid,
/// decoupled from `smartcore`'s internal linear-algebra representation.
#[derive(Serialize, Deserialize)]
pub(crate) enum SmartcoreModel {
    /// Gradient-boosted tree regressor.
    GradientBoostedTrees(GradientBoostedTrees),
    /// Random-forest regressor.
    RandomForest(Forest),
    /// Extra-trees regressor.
    ExtraTrees(ExtraForest),
    /// Ridge (L2) linear regressor.
    Ridge(RidgeModel),
    /// Lasso (L1) linear regressor.
    Lasso(LassoModel),
    /// Elastic-net linear regressor.
    ElasticNet(ElasticNetModel),
    /// Binary logistic classifier (extracted coefficients + intercept).
    Logistic {
        /// Per-feature coefficients (standardized-feature space).
        coefficients: Vec<f64>,
        /// Bias term.
        intercept: f64,
    },
}

impl SmartcoreModel {
    /// Predict a per-row score over a standardized dense matrix.
    ///
    /// Regressors return the model's continuous output (a return-proxy ranking
    /// score); the logistic classifier returns the yes-probability `σ(wᵀx + b)`.
    pub(crate) fn predict(&self, x: &DenseMatrix<f64>) -> QuantResult<Vec<f64>> {
        match self {
            Self::GradientBoostedTrees(model) => regressor_predict(model.predict(x)),
            Self::RandomForest(model) => regressor_predict(model.predict(x)),
            Self::ExtraTrees(model) => regressor_predict(model.predict(x)),
            Self::Ridge(model) => regressor_predict(model.predict(x)),
            Self::Lasso(model) => regressor_predict(model.predict(x)),
            Self::ElasticNet(model) => regressor_predict(model.predict(x)),
            Self::Logistic {
                coefficients,
                intercept,
            } => logistic_proba(coefficients, *intercept, x),
        }
    }

    /// Whether the serialized estimator union matches the governed artifact kind.
    pub(crate) const fn matches_kind(&self, kind: ClassicalKind) -> bool {
        matches!(
            (self, kind),
            (
                Self::GradientBoostedTrees(_),
                ClassicalKind::GradientBoostedTrees
            ) | (Self::RandomForest(_), ClassicalKind::RandomForest)
                | (Self::ExtraTrees(_), ClassicalKind::ExtraTrees)
                | (Self::Ridge(_), ClassicalKind::Ridge)
                | (Self::Lasso(_), ClassicalKind::Lasso)
                | (Self::ElasticNet(_), ClassicalKind::ElasticNet)
                | (Self::Logistic { .. }, ClassicalKind::LogisticRegression)
        )
    }

    /// Validate estimator compatibility with the declared encoded width.
    pub(crate) fn validate_width(&self, width: usize) -> QuantResult<()> {
        if width == 0 {
            return Err(ResearchError::InvalidModelArtifact {
                detail: "classical estimator input width must be positive".to_owned(),
            }
            .into());
        }
        if let Self::Logistic {
            coefficients,
            intercept,
        } = self
        {
            if coefficients.len() != width || !intercept.is_finite() {
                return Err(ResearchError::InvalidModelArtifact {
                    detail: format!(
                        "logistic estimator width/finite mismatch: coefficients={}, transform={width}",
                        coefficients.len()
                    ),
                }
                .into());
            }
            if coefficients.iter().any(|weight| !weight.is_finite()) {
                return Err(ResearchError::InvalidModelArtifact {
                    detail: "logistic estimator contains a non-finite coefficient".to_owned(),
                }
                .into());
            }
        }
        let probe = (DenseInputMatrix::from_rows(vec![vec![0.0; width]])?).dense_matrix()?;
        let predictions = self.predict(&probe)?;
        if predictions.len() != 1 || predictions.iter().any(|value| !value.is_finite()) {
            return Err(ResearchError::InvalidModelArtifact {
                detail: "classical estimator failed the declared-width finite prediction probe"
                    .to_owned(),
            }
            .into());
        }
        Ok(())
    }
}

/// Fit the `smartcore` estimator for `kind` over a standardized design matrix.
fn fit_kind(
    kind: ClassicalKind,
    params: &ClassicalParams,
    x: &DenseMatrix<f64>,
    y: &[f64],
) -> QuantResult<SmartcoreModel> {
    let forest = &params.forest;
    let linear = &params.linear;
    let model = match kind {
        ClassicalKind::GradientBoostedTrees => {
            let parameters = XGRegressorParameters::default()
                .with_n_estimators(forest.n_trees)
                .with_max_depth(forest.max_depth.unwrap_or(8))
                .with_min_child_weight(forest.min_samples_leaf)
                .with_learning_rate(0.1)
                .with_subsample(1.0)
                .with_seed(forest.seed);
            SmartcoreModel::GradientBoostedTrees(fit(GradientBoostedTrees::fit(
                x,
                &y.to_vec(),
                parameters,
            ))?)
        }
        ClassicalKind::RandomForest => {
            let mut parameters = RandomForestRegressorParameters::default()
                .with_n_trees(forest.n_trees)
                .with_min_samples_leaf(forest.min_samples_leaf)
                .with_seed(forest.seed);
            if let Some(depth) = forest.max_depth {
                parameters = parameters.with_max_depth(depth);
            }
            SmartcoreModel::RandomForest(fit(Forest::fit(x, &y.to_vec(), parameters))?)
        }
        ClassicalKind::ExtraTrees => {
            let mut parameters = ExtraTreesRegressorParameters::default()
                .with_n_trees(forest.n_trees)
                .with_min_samples_leaf(forest.min_samples_leaf)
                .with_seed(forest.seed);
            if let Some(depth) = forest.max_depth {
                parameters = parameters.with_max_depth(depth);
            }
            SmartcoreModel::ExtraTrees(fit(ExtraForest::fit(x, &y.to_vec(), parameters))?)
        }
        ClassicalKind::Ridge => {
            let parameters = RidgeRegressionParameters::default().with_alpha(linear.alpha);
            SmartcoreModel::Ridge(fit(RidgeModel::fit(x, &y.to_vec(), parameters))?)
        }
        ClassicalKind::Lasso => {
            let parameters = LassoParameters::default()
                .with_alpha(linear.alpha)
                .with_max_iter(linear.max_iter);
            SmartcoreModel::Lasso(fit(LassoModel::fit(x, &y.to_vec(), parameters))?)
        }
        ClassicalKind::ElasticNet => {
            let parameters = ElasticNetParameters::default()
                .with_alpha(linear.alpha)
                .with_l1_ratio(linear.l1_ratio)
                .with_max_iter(linear.max_iter);
            SmartcoreModel::ElasticNet(fit(ElasticNetModel::fit(x, &y.to_vec(), parameters))?)
        }
        ClassicalKind::LogisticRegression => {
            let classes = y
                .iter()
                .enumerate()
                .map(|(row, value)| {
                    if *value == 0.0 {
                        Ok(0_i64)
                    } else if value.to_bits() == 1.0_f64.to_bits() {
                        Ok(1_i64)
                    } else {
                        Err(ResearchError::MatrixBuild {
                            detail: format!(
                                "logistic regression requires exact binary token payout ratios \
                                 0 or 1; row {row} has split payout {value}"
                            ),
                        }
                        .into())
                    }
                })
                .collect::<QuantResult<Vec<_>>>()?;
            if classes.iter().all(|c| *c == classes[0]) {
                return Err(ResearchError::MatrixBuild {
                    detail: "logistic regression needs both classes present in the label"
                        .to_owned(),
                }
                .into());
            }
            let parameters = LogisticRegressionParameters::default().with_alpha(linear.alpha);
            let estimator = fit(Logistic::fit(x, &classes, parameters))?;
            let (coefficients, intercept) = extract_logistic(&estimator)?;
            SmartcoreModel::Logistic {
                coefficients,
                intercept,
            }
        }
    };
    Ok(model)
}

/// Walk-forward purged validation: for each interior fold boundary, fit only on
/// rows strictly before the validation block (after label-horizon purge +
/// embargo), then score the held-out block.
fn rolling_validation(
    kind: ClassicalKind,
    params: &ClassicalParams,
    matrix: &TrainingMatrix,
    validation: ValidationSpec,
    cancellation: &CancellationProbe,
) -> QuantResult<ValidationReport> {
    cancellation.check("classical validation setup")?;
    let rows = matrix.row_count();
    if matrix.row_decision_at.len() != rows || matrix.row_label_horizon_end.len() != rows {
        return Err(ResearchError::ValidationMethodology {
            detail: format!(
                "classical TrainingMatrix timeline metadata length mismatch: \
                 rows={rows} as_of={} horizon={}",
                matrix.row_decision_at.len(),
                matrix.row_label_horizon_end.len()
            ),
        }
        .into());
    }
    let folds = usize::try_from(validation.folds.max(2)).map_err(|error| {
        ResearchError::ValidationMethodology {
            detail: format!("classical fold count does not fit usize: {error}"),
        }
    })?;
    let boundaries = (0..=folds)
        .map(|k| {
            rows.checked_mul(k)
                .map(|product| product / folds)
                .ok_or_else(|| ResearchError::ValidationMethodology {
                    detail: "classical fold boundary arithmetic overflow".to_owned(),
                })
        })
        .collect::<Result<Vec<_>, _>>()?;
    let timeline: Vec<TimelineGroup> = (0..rows)
        .map(|idx| TimelineGroup {
            decision_at: matrix.row_decision_at[idx],
            label_horizon_end: matrix.row_label_horizon_end[idx],
        })
        .collect();
    let purge = PurgeConfig {
        embargo_pct: validation.embargo_pct,
        min_embargo_secs: validation.min_embargo_secs,
    };
    let splitter = DefaultPurgedSplitter::new();

    let mut fold_objectives = Vec::new();
    for k in 1..folds {
        cancellation.check("classical validation fold")?;
        let (val_start, val_end) = (boundaries[k], boundaries[k + 1]);
        if val_end <= val_start {
            continue;
        }
        let test_indices: Vec<usize> = (val_start..val_end).collect();
        let purged = splitter.split(&timeline, &test_indices, &purge)?;
        // Walk-forward: never train on rows at or after the validation block.
        let train_indices: Vec<usize> = purged
            .train_indices
            .into_iter()
            .filter(|&idx| idx < val_start)
            .collect();
        if train_indices.len() < 2 {
            continue;
        }
        let train = select_rows(matrix, &train_indices)?;
        let (input_transform, std_train) = FittedInputTransform::fit(&train)?;
        let x_train = (std_train).dense_matrix()?;
        let model = fit_kind(kind, params, &x_train, &train.labels.to_vec())?;

        let std_val = input_transform.apply_range(&matrix.cells, val_start..val_end)?;
        let x_val = (std_val).dense_matrix()?;
        let predictions = model.predict(&x_val)?;
        let labels: Vec<f64> = (val_start..val_end).map(|i| matrix.labels[i]).collect();
        fold_objectives.push(rank_ic_f64(&predictions, &labels)?.round_dp(RESEARCH_DECIMAL_SCALE));
    }

    if fold_objectives.is_empty() {
        return Err(ResearchError::ValidationMethodology {
            detail: format!(
                "classical rolling validation produced no evaluable folds after PIT purge/embargo (rows={rows}, requested_folds={folds})"
            ),
        }
        .into());
    }
    let evaluated_folds = u64::try_from(fold_objectives.len()).map_err(|error| {
        ResearchError::ValidationMethodology {
            detail: format!("classical evaluated fold count does not fit u64: {error}"),
        }
    })?;
    let mean_objective = (fold_objectives.iter().sum::<Decimal>() / Decimal::from(evaluated_folds))
        .round_dp(RESEARCH_DECIMAL_SCALE);
    let sample_count =
        u64::try_from(rows).map_err(|error| ResearchError::ValidationMethodology {
            detail: format!("classical validation sample count does not fit u64: {error}"),
        })?;
    Ok(ValidationReport {
        held_out_objective: mean_objective,
        held_out_components: None,
        held_out_diagnostics: None,
        fold_objectives,
        fold_components: Vec::new(),
        sample_count,
        dropped_singleton_groups: 0,
        dropped_singleton_rows: 0,
        // Classical adapters do not run coordinate_search; DSR N is trial-grid only.
        coord_search_effective_n: 0,
    })
}

/// Select an arbitrary set of row indices into a new training matrix.
fn select_rows(matrix: &TrainingMatrix, indices: &[usize]) -> QuantResult<TrainingMatrix> {
    let mut labels = Array1::<f64>::zeros(indices.len());
    let mut row_decision_at = Vec::with_capacity(indices.len());
    let mut row_label_horizon_end = Vec::with_capacity(indices.len());
    for (dst, &src) in indices.iter().enumerate() {
        labels[dst] = matrix.labels[src];
        row_decision_at.push(matrix.row_decision_at[src]);
        row_label_horizon_end.push(matrix.row_label_horizon_end[src]);
    }
    Ok(TrainingMatrix {
        cells: matrix.cells.select(indices)?,
        labels,
        columns: matrix.columns.clone(),
        rejected_rows: 0,
        row_decision_at,
        row_label_horizon_end,
    })
}

/// Map a `smartcore` regressor prediction `Result` into our error domain.
fn regressor_predict(result: Result<Vec<f64>, Failed>) -> QuantResult<Vec<f64>> {
    result.map_err(|error| {
        ResearchError::Inference {
            detail: format!("classical predict failed: {error}"),
        }
        .into()
    })
}

/// Per-row binary logistic probability `σ(wᵀx + b)` over a standardized matrix.
fn logistic_proba(
    coefficients: &[f64],
    intercept: f64,
    x: &DenseMatrix<f64>,
) -> QuantResult<Vec<f64>> {
    let (rows, cols) = x.shape();
    if coefficients.len() != cols || !intercept.is_finite() {
        return Err(ResearchError::InvalidModelArtifact {
            detail: format!(
                "logistic coefficient width {} does not match matrix width {cols}",
                coefficients.len()
            ),
        }
        .into());
    }
    let predictions = (0..rows)
        .map(|r| {
            let mut z = intercept;
            for (c, coefficient) in coefficients.iter().enumerate() {
                z = (*x.get((r, c))).mul_add(*coefficient, z);
            }
            sigmoid(z)
        })
        .collect::<Vec<_>>();
    if predictions.iter().any(|value| !value.is_finite()) {
        return Err(ResearchError::Inference {
            detail: "logistic inference produced a non-finite probability".to_owned(),
        }
        .into());
    }
    Ok(predictions)
}

/// Numerically stable logistic sigmoid.
fn sigmoid(z: f64) -> f64 {
    if z >= 0.0 {
        1.0 / (1.0 + (-z).exp())
    } else {
        let e = z.exp();
        e / (1.0 + e)
    }
}

impl TrainingMatrix {
    /// Validate the matrix shape, returning `(rows, cols)`.
    fn validate_matrix(&self) -> QuantResult<usize> {
        let rows = self.row_count();
        let cols = self.input_count();
        if rows < 2 || cols == 0 {
            return Err(ResearchError::MatrixBuild {
                detail: format!(
                    "classical training needs >= 2 rows and >= 1 column, got {rows}x{cols}"
                ),
            }
            .into());
        }
        Ok(rows)
    }
}

/// Map a `smartcore` fit `Result` into our error domain.
fn fit<M>(result: Result<M, Failed>) -> QuantResult<M> {
    result.map_err(|error| {
        ResearchError::MatrixBuild {
            detail: format!("classical fit failed: {error}"),
        }
        .into()
    })
}

/// Extract the binary logistic coefficients + intercept into plain `f64`s, so
/// inference never touches `smartcore`'s internal matrix representation.
fn extract_logistic(model: &Logistic) -> QuantResult<(Vec<f64>, f64)> {
    let coefficients = model.coefficients();
    let (class_rows, cols) = coefficients.shape();
    if class_rows != 1 {
        return Err(QuantError::from(ResearchError::MatrixBuild {
            detail: format!(
                "logistic regression expected a binary (1×M) coefficient matrix, got {class_rows}×{cols}"
            ),
        }));
    }
    let weights: Vec<f64> = (0..cols).map(|c| *coefficients.get((0, c))).collect();
    let intercept = *model.intercept().get((0, 0));
    Ok((weights, intercept))
}

fn extract_tree_shap_contract(
    model: &SmartcoreModel,
    serialized_model_hash: ContentHash,
    input_contract_hash: ContentHash,
    feature_names: Vec<String>,
    standardized_rows: &[Vec<f64>],
    reference_predictions: &[f64],
) -> QuantResult<TreeShapModelContract> {
    let SmartcoreModel::GradientBoostedTrees(estimator) = model else {
        return Err(ResearchError::InvalidModelArtifact {
            detail: "TreeSHAP extraction requires a GBDT estimator".to_owned(),
        }
        .into());
    };
    if standardized_rows.is_empty()
        || standardized_rows.len() != reference_predictions.len()
        || standardized_rows
            .iter()
            .any(|row| row.len() != feature_names.len())
    {
        return Err(ResearchError::MatrixBuild {
            detail: "GBDT TreeSHAP background rows are empty or misaligned".to_owned(),
        }
        .into());
    }
    let export = estimator
        .export_model()
        .map_err(|error| ResearchError::InvalidModelArtifact {
            detail: format!("typed GBDT export failed: {error}"),
        })?;
    validate_typed_export(&export)?;
    let background_bits = standardized_rows
        .iter()
        .map(|row| row.iter().map(|value| value.to_bits()).collect::<Vec<_>>())
        .collect::<Vec<_>>();
    let background_distribution_hash = CanonicalDigest::content_hash_typed(
        "quant-pivot/tree-shap-background",
        1,
        &(&feature_names, background_bits),
    )?;
    let trees = export
        .trees
        .iter()
        .map(|tree| portable_tree(tree, export.learning_rate, standardized_rows))
        .collect::<QuantResult<Vec<_>>>()?;
    let feature_supports = tree_input_supports(standardized_rows, feature_names.len())?;
    let ensemble = TreeEnsembleSpec {
        serialized_model_hash,
        input_contract_hash,
        background_distribution_hash,
        feature_names,
        feature_supports,
        base_value: decimal(export.base_score, "GBDT base score")?,
        trees,
    };
    let inputs = standardized_rows
        .iter()
        .map(|row| {
            Ok(TreeEnsembleInput {
                values: row
                    .iter()
                    .map(|value| decimal(*value, "GBDT TreeSHAP input").map(Some))
                    .collect::<QuantResult<Vec<_>>>()?,
            })
        })
        .collect::<QuantResult<Vec<_>>>()?;
    let references = reference_predictions
        .iter()
        .map(|value| decimal(*value, "GBDT reference prediction"))
        .collect::<QuantResult<Vec<_>>>()?;
    TreeShapModelContract::verify(ensemble, &inputs, &references)
}

fn tree_input_supports(
    standardized_rows: &[Vec<f64>],
    feature_count: usize,
) -> QuantResult<Vec<TreeInputSupport>> {
    (0..feature_count)
        .map(|feature_index| {
            let mut values = standardized_rows
                .iter()
                .map(|row| decimal(row[feature_index], "GBDT intervention support observation"));
            let first = values.next().ok_or_else(|| ResearchError::MatrixBuild {
                detail: "GBDT intervention support has no training observations".to_owned(),
            })??;
            let (minimum, maximum) =
                values.try_fold((first, first), |(minimum, maximum), value| {
                    let value = value?;
                    Ok::<_, QuantError>((minimum.min(value), maximum.max(value)))
                })?;
            TreeInputSupport::try_new(minimum, maximum)
        })
        .collect()
}

fn validate_typed_export(export: &XGModelExport) -> QuantResult<()> {
    if export.revision != XG_TYPED_EXPORT_REVISION {
        return Err(ResearchError::InvalidModelArtifact {
            detail: format!(
                "unsupported typed GBDT export revision {}; expected {}",
                export.revision, XG_TYPED_EXPORT_REVISION
            ),
        }
        .into());
    }
    if export.trees.is_empty() {
        return Err(ResearchError::InvalidModelArtifact {
            detail: "typed GBDT export omitted its fitted trees".to_owned(),
        }
        .into());
    }
    if !export.base_score.is_finite() {
        return Err(ResearchError::InvalidModelArtifact {
            detail: "GBDT base score must be finite".to_owned(),
        }
        .into());
    }
    Ok(())
}

fn portable_tree(
    root: &XGTreeNodeExport,
    learning_rate: f64,
    background: &[Vec<f64>],
) -> QuantResult<DecisionTreeSpec> {
    if !learning_rate.is_finite() || learning_rate <= 0.0 {
        return Err(ResearchError::InvalidModelArtifact {
            detail: "GBDT learning rate must be finite and positive".to_owned(),
        }
        .into());
    }
    let mut nodes = Vec::new();
    flatten_xg_tree(root, learning_rate, &mut nodes)?;
    let mut covers = vec![0_u64; nodes.len()];
    for row in background {
        let mut node_index = 0_usize;
        loop {
            let cover =
                covers
                    .get_mut(node_index)
                    .ok_or_else(|| ResearchError::InvalidModelArtifact {
                        detail: "portable GBDT traversal reached an unknown node".to_owned(),
                    })?;
            *cover = cover
                .checked_add(1)
                .ok_or_else(|| ResearchError::InvalidModelArtifact {
                    detail: "portable GBDT cover count overflowed u64".to_owned(),
                })?;
            match &nodes[node_index] {
                TreeNode::Leaf { .. } => break,
                TreeNode::Split {
                    feature_index,
                    threshold,
                    left_child,
                    right_child,
                    ..
                } => {
                    let value = row.as_slice().get(*feature_index).ok_or_else(|| {
                        ResearchError::InvalidModelArtifact {
                            detail: format!(
                                "portable GBDT split feature {feature_index} exceeds row width {}",
                                row.len()
                            ),
                        }
                    })?;
                    let goes_left = decimal(*value, "GBDT background value")? <= *threshold;
                    node_index = if goes_left { *left_child } else { *right_child };
                }
            }
        }
    }
    for (node, cover) in nodes.iter_mut().zip(covers) {
        let cover = Decimal::from(cover);
        match node {
            TreeNode::Split {
                cover: node_cover, ..
            }
            | TreeNode::Leaf {
                cover: node_cover, ..
            } => *node_cover = cover,
        }
    }
    Ok(DecisionTreeSpec { nodes })
}

fn flatten_xg_tree(
    node: &XGTreeNodeExport,
    learning_rate: f64,
    nodes: &mut Vec<TreeNode>,
) -> QuantResult<usize> {
    let node_index = nodes.len();
    nodes.push(TreeNode::Leaf {
        value: Decimal::ZERO,
        cover: Decimal::ZERO,
    });
    match node {
        XGTreeNodeExport::Leaf { value } => {
            nodes[node_index] = TreeNode::Leaf {
                value: decimal(*value * learning_rate, "GBDT leaf value")?,
                cover: Decimal::ZERO,
            };
        }
        XGTreeNodeExport::Split {
            feature_index,
            threshold,
            left,
            right,
        } => {
            let left_child = flatten_xg_tree(left, learning_rate, nodes)?;
            let right_child = flatten_xg_tree(right, learning_rate, nodes)?;
            nodes[node_index] = TreeNode::Split {
                feature_index: *feature_index,
                threshold: decimal(*threshold, "GBDT split threshold")?,
                missing_branch: MissingBranch::Left,
                left_child,
                right_child,
                cover: Decimal::ZERO,
            };
        }
    }
    Ok(node_index)
}

impl DenseInputMatrix {
    /// Build a `smartcore` dense matrix from standardized rows.
    fn dense_matrix(self) -> QuantResult<DenseMatrix<f64>> {
        let (row_count, column_count, values) = self.into_parts();
        DenseMatrix::new(row_count, column_count, values, false).map_err(|error| {
            ResearchError::MatrixBuild {
                detail: format!("dense matrix build failed: {error}"),
            }
            .into()
        })
    }
}

/// Model-agnostic ablation feature importance: for each column, the mean
/// absolute change in prediction when the column is reset to its (standardized)
/// mean of zero.
fn ablation_importances(
    model: &SmartcoreModel,
    standardized: &mut DenseMatrix<f64>,
    baseline: &[f64],
    input_transform: &FittedInputTransform,
) -> QuantResult<Vec<FeatureImportance>> {
    let (rows, matrix_columns) = standardized.shape();
    let cols = input_transform.encoded_columns.len();
    if matrix_columns != cols {
        return Err(ResearchError::MatrixBuild {
            detail: format!(
                "ablation matrix width {matrix_columns} differs from transform width {cols}"
            ),
        }
        .into());
    }
    let mut importances = Vec::with_capacity(cols);
    let mut original_column = vec![0.0; rows];
    for c in 0..cols {
        for (row, original) in original_column.iter_mut().enumerate() {
            *original = *standardized.get((row, c));
            standardized.set((row, c), 0.0);
        }
        let predictions = model.predict(standardized)?;
        for (row, original) in original_column.iter().copied().enumerate() {
            standardized.set((row, c), original);
        }
        let total: f64 = baseline
            .iter()
            .zip(&predictions)
            .map(|(base, predicted)| (base - predicted).abs())
            .sum();
        let importance = total / count_f64(baseline.len().max(1))?;
        importances.push(FeatureImportance {
            feature: input_transform.encoded_columns[c].name.clone(),
            importance: decimal(importance, "feature importance")?.round_dp(RESEARCH_DECIMAL_SCALE),
        });
    }
    Ok(importances)
}

/// Spearman rank IC over `f64` series (delegates to the Decimal stats).
fn rank_ic_f64(predicted: &[f64], labels: &[f64]) -> QuantResult<Decimal> {
    let p = predicted
        .iter()
        .map(|value| decimal(*value, "prediction"))
        .collect::<QuantResult<Vec<_>>>()?;
    let l = labels
        .iter()
        .map(|value| decimal(*value, "label"))
        .collect::<QuantResult<Vec<_>>>()?;
    Ok(stats::spearman(&p, &l))
}

/// Convert a finite `f64` to `Decimal` without fabricating a fallback.
fn decimal(value: f64, field: &str) -> QuantResult<Decimal> {
    if !value.is_finite() {
        return Err(ResearchError::MatrixBuild {
            detail: format!("{field} is not finite"),
        }
        .into());
    }
    Decimal::from_f64(value).ok_or_else(|| {
        ResearchError::MatrixBuild {
            detail: format!("{field} cannot be represented as Decimal"),
        }
        .into()
    })
}

/// Lossless-enough count → `f64` (saturating at `u32::MAX`; sample counts never
/// approach that), avoiding a `usize as f64` precision-loss cast.
fn count_f64(n: usize) -> QuantResult<f64> {
    let count = u32::try_from(n).map_err(|error| ResearchError::MatrixBuild {
        detail: format!("row count does not fit the exact f64 conversion boundary: {error}"),
    })?;
    Ok(f64::from(count))
}

#[cfg(test)]
mod tests {
    use chrono::{DateTime, TimeZone, Utc};
    use ndarray::Array1;
    use quant_pivot_models::enums::model::ClassicalKind;
    use rust_decimal::Decimal;
    use rust_decimal_macros::dec;

    use super::{ClassicalAdapterRegistry, ClassicalParams, SmartcoreModel, rolling_validation};
    use crate::{
        features::{FeatureName, FeatureUnit, FeatureValueKind},
        model::trainer::{CancellationProbe, ValidationSpec},
        training::{
            DenseInputMatrix, FeatureColumnSpec, ModelInputCell, RawInputMatrix, TrainingMatrix,
        },
    };

    /// Fixture epoch offset for row `i` (test matrices are tiny; index always fits).
    fn fixture_row_secs(i: usize) -> i64 {
        i64::try_from(i).expect("fixture row index fits i64")
    }

    fn fixture_ts(offset_secs: i64) -> DateTime<Utc> {
        Utc.timestamp_opt(1_700_000_000 + offset_secs, 0)
            .single()
            .expect("ts")
    }

    /// A linearly separable matrix: label = 1 when feature-0 is high.
    impl TrainingMatrix {
        fn classical_fixture() -> Self {
            let rows = 60usize;
            let mut labels = Array1::<f64>::zeros(rows);
            let mut cells = Vec::with_capacity(rows);
            for i in 0..rows {
                let high = i % 2 == 0;
                let f0 = if high { dec!(0.9) } else { dec!(0.1) };
                let f1 = Decimal::from(i % 5) / Decimal::from(5);
                cells.push(vec![
                    ModelInputCell::Observed(f0),
                    ModelInputCell::Observed(f1),
                ]);
                labels[i] = if high { 1.0 } else { 0.0 };
            }
            Self {
                cells: RawInputMatrix::from_rows(cells).expect("raw input matrix"),
                labels,
                columns: vec![
                    FeatureColumnSpec {
                        feature: FeatureName::new("f0"),
                        unit: FeatureUnit::Ratio,
                        value_kind: FeatureValueKind::Decimal,
                        required: true,
                    },
                    FeatureColumnSpec {
                        feature: FeatureName::new("f1"),
                        unit: FeatureUnit::Ratio,
                        value_kind: FeatureValueKind::Decimal,
                        required: true,
                    },
                ],
                rejected_rows: 0,
                row_decision_at: (0..rows).map(|i| fixture_ts(fixture_row_secs(i))).collect(),
                row_label_horizon_end: (0..rows).map(|i| fixture_ts(fixture_row_secs(i))).collect(),
            }
        }
    }

    #[test]
    fn walk_forward_train_start() {
        let purged_train = [1usize, 5, 10, 20, 25];
        let val_start = 15usize;
        let train: Vec<_> = purged_train
            .into_iter()
            .filter(|idx| *idx < val_start)
            .collect();
        assert_eq!(train, vec![1, 5, 10]);
    }

    #[test]
    fn rolling_validation_purges_horizons() {
        // Long label horizons so expanding-prefix CV would leak; purged CV must
        // still produce a finite held-out objective (or skip thin folds).
        let rows = 40usize;
        let mut labels = Array1::<f64>::zeros(rows);
        let mut cells = Vec::with_capacity(rows);
        for i in 0..rows {
            cells.push(vec![
                ModelInputCell::Observed(rust_decimal::Decimal::from(i % 7)),
                ModelInputCell::Observed(rust_decimal::Decimal::from(i % 3)),
            ]);
            labels[i] = if i % 2 == 0 { 1.0 } else { 0.0 };
        }
        let matrix = TrainingMatrix {
            cells: RawInputMatrix::from_rows(cells).expect("raw input matrix"),
            labels,
            columns: vec![
                FeatureColumnSpec {
                    feature: FeatureName::new("f0"),
                    unit: FeatureUnit::Count,
                    value_kind: FeatureValueKind::Count,
                    required: true,
                },
                FeatureColumnSpec {
                    feature: FeatureName::new("f1"),
                    unit: FeatureUnit::Count,
                    value_kind: FeatureValueKind::Count,
                    required: true,
                },
            ],
            rejected_rows: 0,
            row_decision_at: (0..rows)
                .map(|i| fixture_ts(fixture_row_secs(i) * 60))
                .collect(),
            row_label_horizon_end: (0..rows)
                .map(|i| fixture_ts(fixture_row_secs(i) * 60 + 600))
                .collect(),
        };
        let report = rolling_validation(
            ClassicalKind::Ridge,
            &ClassicalParams::defaults_for(ClassicalKind::Ridge),
            &matrix,
            ValidationSpec {
                folds: 4,
                ..ValidationSpec::default()
            },
            &CancellationProbe::default(),
        )
        .expect("purged rolling validation");
        assert_eq!(report.sample_count, 40);
        assert_eq!(report.coord_search_effective_n, 0);
    }

    #[test]
    fn rolling_rejects_no_fold() {
        let mut matrix = TrainingMatrix::classical_fixture();
        let terminal = fixture_ts(1_000_000);
        matrix.row_label_horizon_end.fill(terminal);
        let error = rolling_validation(
            ClassicalKind::Ridge,
            &ClassicalParams::defaults_for(ClassicalKind::Ridge),
            &matrix,
            ValidationSpec {
                folds: 4,
                ..ValidationSpec::default()
            },
            &CancellationProbe::default(),
        )
        .expect_err("an empty validation fold set must fail closed");
        assert!(error.to_string().contains("no evaluable folds"));
    }

    /// Every supported kind trains, validates out-of-sample, serializes, and
    /// `bincode`-roundtrips into the expected estimator union variant.
    #[test]
    fn classical_kind_validates_roundtrips() {
        let matrix = TrainingMatrix::classical_fixture();
        for kind in [
            ClassicalKind::GradientBoostedTrees,
            ClassicalKind::RandomForest,
            ClassicalKind::ExtraTrees,
            ClassicalKind::Ridge,
            ClassicalKind::Lasso,
            ClassicalKind::ElasticNet,
            ClassicalKind::LogisticRegression,
        ] {
            let adapter = ClassicalAdapterRegistry::adapter_for(kind);
            let output = adapter
                .train(&matrix)
                .unwrap_or_else(|e| panic!("train {kind}: {e}"));
            assert_eq!(output.kind, kind);
            assert!(
                !output.model_bytes.is_empty(),
                "{kind}: estimator serialized"
            );
            assert_eq!(output.metrics.feature_importances.len(), 2, "{kind}");
            assert_eq!(
                output.tree_shap.is_some(),
                kind == ClassicalKind::GradientBoostedTrees,
                "{kind}"
            );

            let validation = adapter
                .validate(
                    &matrix,
                    ValidationSpec {
                        folds: 3,
                        ..ValidationSpec::default()
                    },
                    &CancellationProbe::default(),
                )
                .unwrap_or_else(|e| panic!("validate {kind}: {e}"));
            assert_eq!(validation.sample_count, 60, "{kind}");

            let model: SmartcoreModel = bincode::deserialize(&output.model_bytes)
                .unwrap_or_else(|e| panic!("{kind} bincode roundtrip: {e}"));
            match (kind, &model) {
                (ClassicalKind::GradientBoostedTrees, SmartcoreModel::GradientBoostedTrees(_))
                | (ClassicalKind::RandomForest, SmartcoreModel::RandomForest(_))
                | (ClassicalKind::ExtraTrees, SmartcoreModel::ExtraTrees(_))
                | (ClassicalKind::Ridge, SmartcoreModel::Ridge(_))
                | (ClassicalKind::Lasso, SmartcoreModel::Lasso(_))
                | (ClassicalKind::ElasticNet, SmartcoreModel::ElasticNet(_))
                | (ClassicalKind::LogisticRegression, SmartcoreModel::Logistic { .. }) => {}
                _ => panic!("{kind}: unexpected estimator variant"),
            }
        }
    }

    /// The logistic classifier emits a yes-probability in `[0, 1]` that ranks the
    /// high-feature rows above the low-feature rows.
    #[test]
    fn logistic_emits_probability_ranking() {
        let output = ClassicalAdapterRegistry::adapter_for(ClassicalKind::LogisticRegression)
            .train(&TrainingMatrix::classical_fixture())
            .expect("train logistic");
        let model: SmartcoreModel = bincode::deserialize(&output.model_bytes).expect("decode");
        let SmartcoreModel::Logistic { .. } = &model else {
            panic!("logistic variant");
        };
        let x = (DenseInputMatrix::from_rows(vec![vec![2.0, 0.0], vec![-2.0, 0.0]])
            .expect("dense input"))
        .dense_matrix()
        .expect("matrix");
        let proba = model.predict(&x).expect("predict");
        assert!(
            proba.iter().all(|p| (0.0..=1.0).contains(p)),
            "probabilities"
        );
        assert!(proba[0] > proba[1], "high feature ⇒ higher yes-probability");
    }

    #[test]
    fn logistic_rejects_split_payout() {
        let mut matrix = TrainingMatrix::classical_fixture();
        matrix.labels[1] = 0.5;

        let error = ClassicalAdapterRegistry::adapter_for(ClassicalKind::LogisticRegression)
            .train(&matrix)
            .expect_err("binary estimator must reject a split payout");

        assert!(
            error.to_string().contains("split payout"),
            "typed failure must explain the unsupported label: {error}"
        );
    }
}

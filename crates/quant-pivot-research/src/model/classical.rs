//! Classical-ML adapters over `smartcore` (Phase 3.6, `ml-classical` feature).
//!
//! The business layer never sees a `smartcore` concrete type: training produces
//! a [`ClassicalTrainOutput`] (serialized estimator bytes + metadata) and the
//! runtime ([`crate::model::classical_runtime`]) consumes it behind
//! `dyn QuantModelRuntime`. The estimator union is `bincode`-serialized; loading
//! verifies the recorded crate version (§15.6) before deserializing.
//!
//! Six production kinds are supported, dispatched through
//! [`ClassicalAdapterRegistry`]: the tree ensembles (`RandomForest`,
//! `ExtraTrees`) and penalized linear models (`Ridge`, `Lasso`, `ElasticNet`) are
//! continuous return-proxy **regressors** (their output ranks markets); the
//! `LogisticRegression` **classifier** emits a true yes-probability. Every kind
//! shares the frozen standardization preprocessing so inference applies the exact
//! transform the model was fit on, and a shared time-ordered holdout produces an
//! out-of-sample validation objective.

use ndarray::s;
use quant_pivot_error::{QuantError, QuantResult, research::ResearchError};
use quant_pivot_models::enums::quant::ModelSerializationFormat;
use rust_decimal::{
    Decimal,
    prelude::{FromPrimitive, ToPrimitive},
};
use serde::{Deserialize, Serialize};
use smartcore::{
    ensemble::{
        extra_trees_regressor::{ExtraTreesRegressor, ExtraTreesRegressorParameters},
        random_forest_regressor::{RandomForestRegressor, RandomForestRegressorParameters},
    },
    linalg::basic::{arrays::Array, matrix::DenseMatrix},
    linear::{
        elastic_net::{ElasticNet, ElasticNetParameters},
        lasso::{Lasso, LassoParameters},
        logistic_regression::{LogisticRegression, LogisticRegressionParameters},
        ridge_regression::{RidgeRegression, RidgeRegressionParameters},
    },
};

use crate::{
    model::{
        artifact::{ClassicalModelMetrics, FeatureImportance, PreprocessingArtifact},
        runtime::ClassicalKind,
        trainer::ValidationReport,
    },
    precision::RESEARCH_DECIMAL_SCALE,
    stats,
    training::TrainingMatrix,
};

/// The ML crate + version stamped onto every classical artifact (load-time
/// mismatch is rejected; §15.6).
pub const CLASSICAL_CRATE_NAME: &str = "smartcore";
/// Recorded crate version (major.minor of the workspace `smartcore` dependency).
pub const CLASSICAL_CRATE_VERSION: &str = "0.5";

/// Concrete `smartcore` regressor type aliases (`f64` features + targets, dense
/// matrix design, dense target vector).
type Forest = RandomForestRegressor<f64, f64, DenseMatrix<f64>, Vec<f64>>;
type ExtraForest = ExtraTreesRegressor<f64, f64, DenseMatrix<f64>, Vec<f64>>;
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
/// metadata the core needs to assemble a `ClassicalModelArtifact`.
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
    /// Frozen standardization preprocessing.
    pub preprocessing: PreprocessingArtifact,
    /// Training metrics + feature importances.
    pub metrics: ClassicalModelMetrics,
}

/// A classical training adapter. Concrete impls wrap a `smartcore` estimator and
/// its frozen hyperparameters.
pub trait ClassicalModelAdapter: Send + Sync {
    /// The classical kind this adapter produces.
    fn kind(&self) -> ClassicalKind;

    /// Fit the estimator on a standardized training matrix.
    fn train(&self, matrix: &TrainingMatrix) -> QuantResult<ClassicalTrainOutput>;

    /// Out-of-sample rolling-holdout validation objective over `folds`
    /// time-ordered folds (the same `ValidationSpec` semantics the weighted
    /// trainer uses).
    fn validate(&self, matrix: &TrainingMatrix, folds: u32) -> QuantResult<ValidationReport>;
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
    /// override (Phase 11.5 §3.5's governed classical trial grid) instead of
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
        let (rows, cols) = validate_matrix(matrix)?;
        let (standardized, preprocessing) = standardize(matrix);
        let x = dense_matrix(&standardized)?;
        let y: Vec<f64> = matrix.labels.to_vec();
        let model = fit_kind(self.kind, &self.params, &x, &y)?;

        let predictions = model.predict(&x)?;
        let importances = ablation_importances(&model, &standardized, &predictions, matrix);
        let validation_objective = rank_ic_f64(&predictions, &y);

        let model_bytes =
            bincode::serialize(&model).map_err(|error| ResearchError::Serialization {
                detail: format!("bincode serialize classical model: {error}"),
            })?;

        Ok(ClassicalTrainOutput {
            kind: self.kind,
            crate_name: CLASSICAL_CRATE_NAME.to_owned(),
            crate_version: CLASSICAL_CRATE_VERSION.to_owned(),
            serialization_format: ModelSerializationFormat::Bincode,
            model_bytes,
            preprocessing,
            metrics: ClassicalModelMetrics {
                train_samples: rows as u64,
                feature_count: u32::try_from(cols).unwrap_or(u32::MAX),
                validation_objective: validation_objective.round_dp(RESEARCH_DECIMAL_SCALE),
                feature_importances: importances,
            },
        })
    }

    fn validate(&self, matrix: &TrainingMatrix, folds: u32) -> QuantResult<ValidationReport> {
        rolling_validation(self.kind, &self.params, matrix, folds)
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
            Self::RandomForest(model) => regressor_predict(model.predict(x)),
            Self::ExtraTrees(model) => regressor_predict(model.predict(x)),
            Self::Ridge(model) => regressor_predict(model.predict(x)),
            Self::Lasso(model) => regressor_predict(model.predict(x)),
            Self::ElasticNet(model) => regressor_predict(model.predict(x)),
            Self::Logistic {
                coefficients,
                intercept,
            } => Ok(logistic_proba(coefficients, *intercept, x)),
        }
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
            let classes: Vec<i64> = y.iter().map(|v| i64::from(*v >= 0.5)).collect();
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

/// Time-ordered rolling-holdout validation: for each fold boundary, fit on the
/// expanding prefix and score the next contiguous block out-of-sample, using the
/// prefix's standardization (no validation-fold leakage).
fn rolling_validation(
    kind: ClassicalKind,
    params: &ClassicalParams,
    matrix: &TrainingMatrix,
    folds: u32,
) -> QuantResult<ValidationReport> {
    let rows = matrix.features.nrows();
    let folds = folds.max(2) as usize;
    let boundaries: Vec<usize> = (0..=folds).map(|k| rows * k / folds).collect();

    let mut fold_objectives = Vec::new();
    for k in 1..folds {
        let train_end = boundaries[k];
        let (val_start, val_end) = (boundaries[k], boundaries[k + 1]);
        if train_end < 2 || val_end <= val_start {
            continue;
        }
        let train = slice_matrix(matrix, 0, train_end);
        let (std_train, preprocessing) = standardize(&train);
        let x_train = dense_matrix(&std_train)?;
        let model = fit_kind(kind, params, &x_train, &train.labels.to_vec())?;

        let std_val = standardize_with(matrix, &preprocessing, val_start, val_end);
        let x_val = dense_matrix(&std_val)?;
        let predictions = model.predict(&x_val)?;
        let labels: Vec<f64> = (val_start..val_end).map(|i| matrix.labels[i]).collect();
        fold_objectives.push(rank_ic_f64(&predictions, &labels).round_dp(RESEARCH_DECIMAL_SCALE));
    }

    let mean_objective = if fold_objectives.is_empty() {
        Decimal::ZERO
    } else {
        (fold_objectives.iter().sum::<Decimal>() / Decimal::from(fold_objectives.len() as u64))
            .round_dp(RESEARCH_DECIMAL_SCALE)
    };
    Ok(ValidationReport {
        held_out_objective: mean_objective,
        held_out_components: None,
        held_out_diagnostics: None,
        fold_objectives,
        fold_components: Vec::new(),
        sample_count: rows as u64,
        dropped_singleton_groups: 0,
        dropped_singleton_rows: 0,
        // Classical adapters do not run coordinate_search; DSR N is trial-grid only.
        coord_search_effective_n: 0,
    })
}

/// A contiguous row slice of a training matrix (`[start, end)`).
fn slice_matrix(matrix: &TrainingMatrix, start: usize, end: usize) -> TrainingMatrix {
    TrainingMatrix {
        features: matrix.features.slice(s![start..end, ..]).to_owned(),
        labels: matrix.labels.slice(s![start..end]).to_owned(),
        feature_names: matrix.feature_names.clone(),
        rejected_rows: 0,
    }
}

/// Standardize rows `[start, end)` of `matrix` using a frozen preprocessing
/// transform (the train-fold means/stds), so the validation fold never leaks.
fn standardize_with(
    matrix: &TrainingMatrix,
    preprocessing: &PreprocessingArtifact,
    start: usize,
    end: usize,
) -> Vec<Vec<f64>> {
    // `means` / `stds` are built per column in `standardize`, so they are exactly
    // `cols` long and safe to index. (Direct indexing also avoids `smartcore`'s
    // in-scope `Array::get`, which shadows `Vec::get`.)
    let cols = matrix.features.ncols();
    (start..end)
        .map(|r| {
            (0..cols)
                .map(|c| {
                    let mean = preprocessing.means[c].to_f64().unwrap_or(0.0);
                    let std_raw = preprocessing.stds[c].to_f64().unwrap_or(1.0);
                    let std = if std_raw.abs() > f64::EPSILON {
                        std_raw
                    } else {
                        1.0
                    };
                    (matrix.features[[r, c]] - mean) / std
                })
                .collect()
        })
        .collect()
}

/// Map a `smartcore` regressor prediction `Result` into our error domain.
fn regressor_predict(result: Result<Vec<f64>, smartcore::error::Failed>) -> QuantResult<Vec<f64>> {
    result.map_err(|error| {
        ResearchError::Inference {
            detail: format!("classical predict failed: {error}"),
        }
        .into()
    })
}

/// Per-row binary logistic probability `σ(wᵀx + b)` over a standardized matrix.
fn logistic_proba(coefficients: &[f64], intercept: f64, x: &DenseMatrix<f64>) -> Vec<f64> {
    let (rows, cols) = x.shape();
    (0..rows)
        .map(|r| {
            let mut z = intercept;
            for c in 0..cols {
                let w = if c < coefficients.len() {
                    coefficients[c]
                } else {
                    0.0
                };
                z = (*x.get((r, c))).mul_add(w, z);
            }
            sigmoid(z)
        })
        .collect()
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

/// Validate the matrix shape, returning `(rows, cols)`.
fn validate_matrix(matrix: &TrainingMatrix) -> QuantResult<(usize, usize)> {
    let rows = matrix.features.nrows();
    let cols = matrix.features.ncols();
    if rows < 2 || cols == 0 {
        return Err(ResearchError::MatrixBuild {
            detail: format!(
                "classical training needs >= 2 rows and >= 1 column, got {rows}x{cols}"
            ),
        }
        .into());
    }
    Ok((rows, cols))
}

/// Map a `smartcore` fit `Result` into our error domain.
fn fit<M>(result: Result<M, smartcore::error::Failed>) -> QuantResult<M> {
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

/// Standardize each column to zero mean / unit variance; zero-variance columns
/// are left centered (std treated as 1). Returns the standardized rows + the
/// recorded transform.
fn standardize(matrix: &TrainingMatrix) -> (Vec<Vec<f64>>, PreprocessingArtifact) {
    let rows = matrix.features.nrows();
    let cols = matrix.features.ncols();
    let row_count = count_f64(rows);
    let mut means = vec![0.0_f64; cols];
    let mut stds = vec![1.0_f64; cols];

    for c in 0..cols {
        let column = matrix.features.column(c);
        let mean = column.sum() / row_count;
        let var = column.iter().map(|v| (v - mean).powi(2)).sum::<f64>() / row_count;
        let std = var.sqrt();
        means[c] = mean;
        stds[c] = if std > f64::EPSILON { std } else { 1.0 };
    }

    let standardized: Vec<Vec<f64>> = (0..rows)
        .map(|r| {
            (0..cols)
                .map(|c| (matrix.features[[r, c]] - means[c]) / stds[c])
                .collect()
        })
        .collect();

    let preprocessing = PreprocessingArtifact {
        feature_names: matrix.feature_names.clone(),
        means: means.iter().map(|m| decimal(*m)).collect(),
        stds: stds.iter().map(|s| decimal(*s)).collect(),
    };
    (standardized, preprocessing)
}

/// Build a `smartcore` dense matrix from standardized rows.
fn dense_matrix(rows: &[Vec<f64>]) -> QuantResult<DenseMatrix<f64>> {
    DenseMatrix::from_2d_vec(&rows.to_vec()).map_err(|error| {
        ResearchError::MatrixBuild {
            detail: format!("dense matrix build failed: {error}"),
        }
        .into()
    })
}

/// Model-agnostic ablation feature importance: for each column, the mean
/// absolute change in prediction when the column is reset to its (standardized)
/// mean of zero.
fn ablation_importances(
    model: &SmartcoreModel,
    standardized: &[Vec<f64>],
    baseline: &[f64],
    matrix: &TrainingMatrix,
) -> Vec<FeatureImportance> {
    let cols = matrix.feature_names.len();
    let mut importances = Vec::with_capacity(cols);
    for c in 0..cols {
        let ablated: Vec<Vec<f64>> = standardized
            .iter()
            .map(|row| {
                let mut row = row.clone();
                if c < row.len() {
                    row[c] = 0.0; // standardized mean
                }
                row
            })
            .collect();
        let importance = dense_matrix(&ablated)
            .and_then(|x| model.predict(&x))
            .map_or(0.0, |preds| {
                let total: f64 = baseline
                    .iter()
                    .zip(&preds)
                    .map(|(b, p)| (b - p).abs())
                    .sum();
                total / count_f64(baseline.len().max(1))
            });
        importances.push(FeatureImportance {
            feature: matrix.feature_names[c].clone(),
            importance: decimal(importance).round_dp(RESEARCH_DECIMAL_SCALE),
        });
    }
    importances
}

/// Spearman rank IC over `f64` series (delegates to the Decimal stats).
fn rank_ic_f64(predicted: &[f64], labels: &[f64]) -> Decimal {
    let p: Vec<Decimal> = predicted.iter().map(|v| decimal(*v)).collect();
    let l: Vec<Decimal> = labels.iter().map(|v| decimal(*v)).collect();
    stats::spearman(&p, &l)
}

/// Convert an `f64` to `Decimal` (training-matrix boundary).
fn decimal(value: f64) -> Decimal {
    Decimal::from_f64(value).unwrap_or(Decimal::ZERO)
}

/// Lossless-enough count → `f64` (saturating at `u32::MAX`; sample counts never
/// approach that), avoiding a `usize as f64` precision-loss cast.
fn count_f64(n: usize) -> f64 {
    f64::from(u32::try_from(n).unwrap_or(u32::MAX))
}

#[cfg(test)]
mod tests {
    use super::{ClassicalAdapterRegistry, SmartcoreModel};
    use ndarray::{Array1, Array2};

    use crate::{
        features::FeatureName,
        model::{classical::dense_matrix, runtime::ClassicalKind},
        training::TrainingMatrix,
    };

    /// A linearly separable matrix: label = 1 when feature-0 is high.
    fn training_matrix() -> TrainingMatrix {
        let rows = 60usize;
        let mut features = Array2::<f64>::zeros((rows, 2));
        let mut labels = Array1::<f64>::zeros(rows);
        for i in 0..rows {
            let high = i % 2 == 0;
            features[[i, 0]] = if high { 0.9 } else { 0.1 };
            features[[i, 1]] = f64::from(u8::try_from(i % 5).unwrap_or(0)) / 5.0;
            labels[i] = if high { 1.0 } else { 0.0 };
        }
        TrainingMatrix {
            features,
            labels,
            feature_names: vec![FeatureName::new("f0"), FeatureName::new("f1")],
            rejected_rows: 0,
        }
    }

    /// Every supported kind trains, validates out-of-sample, serializes, and
    /// `bincode`-roundtrips into the expected estimator union variant.
    #[test]
    fn every_classical_kind_trains_validates_and_roundtrips() {
        let matrix = training_matrix();
        for kind in [
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

            let validation = adapter
                .validate(&matrix, 3)
                .unwrap_or_else(|e| panic!("validate {kind}: {e}"));
            assert_eq!(validation.sample_count, 60, "{kind}");

            let model: SmartcoreModel = bincode::deserialize(&output.model_bytes)
                .unwrap_or_else(|e| panic!("{kind} bincode roundtrip: {e}"));
            match (kind, &model) {
                (ClassicalKind::RandomForest, SmartcoreModel::RandomForest(_))
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
            .train(&training_matrix())
            .expect("train logistic");
        let model: SmartcoreModel = bincode::deserialize(&output.model_bytes).expect("decode");
        let SmartcoreModel::Logistic { .. } = &model else {
            panic!("logistic variant");
        };
        let x = dense_matrix(&[vec![2.0, 0.0], vec![-2.0, 0.0]]).expect("matrix");
        let proba = model.predict(&x).expect("predict");
        assert!(
            proba.iter().all(|p| (0.0..=1.0).contains(p)),
            "probabilities"
        );
        assert!(proba[0] > proba[1], "high feature ⇒ higher yes-probability");
    }
}

//! [`ClassicalRuntime`]: the online `QuantModelRuntime` for a classical
//! (smartcore-backed) model (Phase 3.6, `ml-classical` feature).
//!
//! Loading is fail-closed: the recorded crate version must match (§15.6) and the
//! serialization format must be `bincode` before the estimator is deserialized.
//! Inference standardizes each row with the frozen preprocessing, predicts a
//! yes-probability, and maps it to a sided [`SignalCandidate`] priced from the
//! row's [`MarketInferenceContext`] — the same candidate contract the weighted
//! runtime emits, so the business layer never sees a smartcore type.

use std::time::Instant;

use async_trait::async_trait;
use chrono::{DateTime, Utc};
use quant_pivot_error::{QuantResult, research::ResearchError};
use quant_pivot_models::{
    enums::quant::{ModelSerializationFormat, SignalSide},
    types::{
        ContentHash, ModelRunId, ModelVersionId, Price, Probability, SignalCandidateId, TokenId,
    },
};
use rust_decimal::{
    Decimal,
    prelude::{FromPrimitive, ToPrimitive},
};
use smartcore::linalg::basic::matrix::DenseMatrix;

use crate::{
    features::{FeatureName, FeatureValue},
    model::{
        artifact::ClassicalModelArtifact,
        classical::{CLASSICAL_CRATE_VERSION, SmartcoreModel},
        runtime::{
            InferenceMatrix, InferenceMatrixRow, MarketInferenceContext, ModelFamily,
            ModelRuntimeInput, ModelRuntimeMetrics, ModelRuntimeOutput, QuantModelRuntime,
        },
        signal::{ModelExplanation, SignalCandidate},
    },
    precision::RESEARCH_DECIMAL_SCALE,
};

/// Heuristic max expected return (bps) at full conviction (classical models do
/// not carry a calibrated return curve; provenance is heuristic).
const MAX_EXPECTED_RETURN_BPS: i64 = 300;
/// Heuristic max downside (bps) at zero conviction.
const MAX_DOWNSIDE_BPS: i64 = 500;

/// A loaded classical model runtime.
pub struct ClassicalRuntime {
    artifact: ClassicalModelArtifact,
    model: SmartcoreModel,
    means: Vec<f64>,
    stds: Vec<f64>,
}

impl ClassicalRuntime {
    /// Load a classical runtime from its artifact + serialized estimator bytes,
    /// verifying the crate version and serialization format.
    ///
    /// # Errors
    ///
    /// Returns [`ResearchError::RuntimeUnavailable`] on a crate-version mismatch
    /// and [`ResearchError::Serialization`] on a decode failure.
    pub fn load(artifact: ClassicalModelArtifact, model_bytes: &[u8]) -> QuantResult<Self> {
        if artifact.crate_version != CLASSICAL_CRATE_VERSION {
            return Err(ResearchError::RuntimeUnavailable {
                family: artifact.kind.to_string(),
                detail: format!(
                    "classical crate version mismatch: artifact `{}`, runtime `{}`",
                    artifact.crate_version, CLASSICAL_CRATE_VERSION
                ),
            }
            .into());
        }
        if artifact.serialization_format != ModelSerializationFormat::Bincode {
            return Err(ResearchError::InvalidModelArtifact {
                detail: format!(
                    "classical runtime expects bincode bytes, got {:?}",
                    artifact.serialization_format
                ),
            }
            .into());
        }
        let model: SmartcoreModel =
            bincode::deserialize(model_bytes).map_err(|error| ResearchError::Serialization {
                detail: format!("bincode deserialize classical model: {error}"),
            })?;
        let means = artifact
            .preprocessing
            .means
            .iter()
            .map(|m| m.to_f64().unwrap_or(0.0))
            .collect();
        let stds = artifact
            .preprocessing
            .stds
            .iter()
            .map(|s| {
                let v = s.to_f64().unwrap_or(1.0);
                if v.abs() > f64::EPSILON { v } else { 1.0 }
            })
            .collect();
        Ok(Self {
            artifact,
            model,
            means,
            stds,
        })
    }

    /// Standardize one inference row into the artifact's column order.
    fn standardized_row(&self, row: &InferenceMatrixRow, names: &[FeatureName]) -> Vec<f64> {
        self.artifact
            .preprocessing
            .feature_names
            .iter()
            .enumerate()
            .map(|(idx, feature)| {
                let raw = names
                    .iter()
                    .position(|n| n == feature)
                    .and_then(|pos| row.features.get(pos))
                    .and_then(feature_scalar_f64)
                    .unwrap_or_else(|| self.means.get(idx).copied().unwrap_or(0.0));
                let mean = self.means.get(idx).copied().unwrap_or(0.0);
                let std = self.stds.get(idx).copied().unwrap_or(1.0);
                (raw - mean) / std
            })
            .collect()
    }

    /// Build a sided candidate from a predicted yes-probability.
    fn candidate(
        prediction: f64,
        row: &InferenceMatrixRow,
        model_run_id: &ModelRunId,
        as_of: DateTime<Utc>,
    ) -> Option<SignalCandidate> {
        let p_hat = prediction.clamp(0.0, 1.0);
        let net = p_hat - 0.5;
        if net.abs() < f64::EPSILON {
            return None;
        }
        let side = if net >= 0.0 {
            SignalSide::BuyYes
        } else {
            SignalSide::BuyNo
        };
        let (token_id, entry_price_ref) = resolve_entry(row, side)?;
        let conviction = (net.abs() * 2.0).clamp(0.0, 1.0);
        let conviction_dp = f64_to_decimal(conviction);
        let composite_score = Probability::new(conviction_dp);
        let confidence = composite_score;
        let expected_return_bps = (conviction_dp * Decimal::from(MAX_EXPECTED_RETURN_BPS))
            .round_dp(RESEARCH_DECIMAL_SCALE);
        let downside_bps = ((Decimal::ONE - conviction_dp) * Decimal::from(MAX_DOWNSIDE_BPS))
            .round_dp(RESEARCH_DECIMAL_SCALE);

        Some(SignalCandidate {
            signal_candidate_id: SignalCandidateId::from_v7(),
            model_run_id: model_run_id.clone(),
            market_id: row.market_id.clone(),
            token_id,
            side,
            composite_score,
            confidence,
            expected_return_bps,
            downside_bps,
            entry_price_ref,
            suggested_horizon_secs: 0,
            factor_breakdown: Vec::new(),
            model_explanation: ModelExplanation {
                headline: format!("classical {side}: yes_prob {p_hat:.4}"),
                top_positive: Vec::new(),
                top_negative: Vec::new(),
            },
            rejection_warnings: Vec::new(),
            rank_before_portfolio: 0,
            as_of,
        })
    }
}

#[async_trait]
impl QuantModelRuntime for ClassicalRuntime {
    fn model_version_id(&self) -> ModelVersionId {
        self.artifact.header.model_version_id.clone()
    }

    fn model_family(&self) -> ModelFamily {
        ModelFamily::Classical(self.artifact.kind)
    }

    fn feature_schema_hash(&self) -> ContentHash {
        self.artifact.header.feature_schema_hash.clone()
    }

    fn required_features(&self) -> Vec<FeatureName> {
        self.artifact.preprocessing.feature_names.clone()
    }

    async fn infer_batch(&self, input: ModelRuntimeInput) -> QuantResult<ModelRuntimeOutput> {
        let started = Instant::now();
        let matrix = expect_feature_matrix(input)?;
        let markets_scored = u32::try_from(matrix.rows.len()).unwrap_or(u32::MAX);

        if matrix.rows.is_empty() {
            return Ok(ModelRuntimeOutput {
                candidates: Vec::new(),
                runtime_metrics: ModelRuntimeMetrics {
                    markets_scored: 0,
                    candidates_emitted: 0,
                    inference_duration_ms: 0,
                },
                warnings: Vec::new(),
            });
        }

        let rows: Vec<Vec<f64>> = matrix
            .rows
            .iter()
            .map(|row| self.standardized_row(row, &matrix.feature_names))
            .collect();
        let dense = DenseMatrix::from_2d_vec(&rows).map_err(|error| ResearchError::Inference {
            detail: format!("classical dense matrix build failed: {error}"),
        })?;
        let predictions = self.model.predict(&dense)?;

        let model_run_id = synthetic_run_id(&matrix);
        let mut candidates: Vec<SignalCandidate> = matrix
            .rows
            .iter()
            .zip(predictions)
            .filter_map(|(row, prediction)| {
                Self::candidate(prediction, row, &model_run_id, matrix.as_of)
            })
            .collect();
        candidates.sort_by(|a, b| {
            b.composite_score
                .inner()
                .cmp(&a.composite_score.inner())
                .then_with(|| a.market_id.as_str().cmp(b.market_id.as_str()))
        });
        for (index, candidate) in candidates.iter_mut().enumerate() {
            candidate.rank_before_portfolio = u32::try_from(index + 1).unwrap_or(u32::MAX);
        }

        let candidates_emitted = u32::try_from(candidates.len()).unwrap_or(u32::MAX);
        Ok(ModelRuntimeOutput {
            candidates,
            runtime_metrics: ModelRuntimeMetrics {
                markets_scored,
                candidates_emitted,
                inference_duration_ms: u64::try_from(started.elapsed().as_millis())
                    .unwrap_or(u64::MAX),
            },
            warnings: Vec::new(),
        })
    }
}

/// The classical path is fed a [`FactorInferenceTable`]'s `model_run_id`-less
/// matrix; reuse a stable per-batch id derived from `as_of` is unnecessary, so a
/// fresh id is minted (candidates carry it for audit only).
fn synthetic_run_id(_matrix: &InferenceMatrix) -> ModelRunId {
    ModelRunId::from_v7()
}

/// Resolve the target token + entry price for the chosen side.
fn resolve_entry(row: &InferenceMatrixRow, side: SignalSide) -> Option<(TokenId, Price)> {
    let context: &MarketInferenceContext = &row.context;
    match side {
        SignalSide::BuyYes => Some((row.token_id.clone(), context.yes_price)),
        SignalSide::BuyNo => {
            let token = context.secondary_token_id.clone()?;
            let price = context
                .no_price
                .unwrap_or_else(|| complement(context.yes_price));
            Some((token, price))
        }
        SignalSide::SellYes | SignalSide::SellNo => None,
    }
}

/// Binary complement price `1 - p`, clamped to `[0, 1]`.
fn complement(price: Price) -> Price {
    Price::new((Decimal::ONE - price.inner()).clamp(Decimal::ZERO, Decimal::ONE))
}

/// Extract the feature matrix input, rejecting a factor-table (weighted only).
fn expect_feature_matrix(input: ModelRuntimeInput) -> QuantResult<InferenceMatrix> {
    match input {
        ModelRuntimeInput::FeatureMatrix(matrix) => Ok(matrix),
        ModelRuntimeInput::FactorTable(_) => Err(ResearchError::Inference {
            detail: "classical runtime requires a feature matrix, got a factor table".to_owned(),
        }
        .into()),
    }
}

/// Project a [`FeatureValue`] to a finite `f64`, mirroring the training matrix.
fn feature_scalar_f64(value: &FeatureValue) -> Option<f64> {
    let decimal = match value {
        FeatureValue::Decimal(d) | FeatureValue::Bps(d) => *d,
        FeatureValue::Probability(p) => p.inner(),
        FeatureValue::Usd(u) => u.inner(),
        FeatureValue::Count(c) => Decimal::from(*c),
        FeatureValue::Bool(b) => {
            if *b {
                Decimal::ONE
            } else {
                Decimal::ZERO
            }
        }
        FeatureValue::Category(_) | FeatureValue::Missing(_) => return None,
    };
    decimal.to_f64().filter(|v| v.is_finite())
}

/// Convert an `f64` to a research-scale `Decimal`.
fn f64_to_decimal(value: f64) -> Decimal {
    Decimal::from_f64(value)
        .unwrap_or(Decimal::ZERO)
        .round_dp(RESEARCH_DECIMAL_SCALE)
}

#[cfg(test)]
mod tests {
    use super::ClassicalRuntime;
    use crate::{
        features::{FeatureName, FeatureValue},
        model::{
            artifact::{ClassicalModelArtifact, ModelArtifactHeader},
            classical::{
                CLASSICAL_CRATE_NAME, CLASSICAL_CRATE_VERSION, ClassicalAdapterRegistry,
                ClassicalTrainOutput,
            },
            runtime::{
                ClassicalKind, InferenceMatrix, InferenceMatrixRow, MarketInferenceContext,
                ModelFamily, ModelRuntimeInput, QuantModelRuntime,
            },
        },
        training::TrainingMatrix,
    };
    use chrono::Utc;
    use ndarray::{Array1, Array2};
    use quant_pivot_models::{
        enums::quant::{DataQualityStatus, SignalSide},
        types::{
            ArtifactUri, ContentHash, MarketId, ModelArtifactId, ModelVersionId, Price, TokenId,
        },
    };
    use rust_decimal::Decimal;
    use rust_decimal_macros::dec;

    fn hash(seed: &str) -> ContentHash {
        use std::fmt::Write;
        let hex = seed.bytes().fold(String::new(), |mut acc, byte| {
            let _ = write!(acc, "{byte:02x}");
            acc
        });
        let padded = format!("{hex:0<64}");
        ContentHash::parse(format!("blake3:{}", &padded[..64])).expect("hash")
    }

    /// A linearly separable matrix: label = 1 when feature-0 is high.
    fn training_matrix() -> TrainingMatrix {
        let rows = 40usize;
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

    fn artifact(output: &ClassicalTrainOutput) -> ClassicalModelArtifact {
        ClassicalModelArtifact {
            header: ModelArtifactHeader {
                model_version_id: ModelVersionId::from_v7(),
                model_family: ModelFamily::Classical(ClassicalKind::RandomForest),
                feature_schema_hash: hash("feat"),
                factor_schema_hash: hash("fac"),
            },
            artifact_id: ModelArtifactId::from_v7(),
            kind: output.kind,
            crate_name: output.crate_name.clone(),
            crate_version: output.crate_version.clone(),
            label_schema_hash: hash("lab"),
            training_dataset_hash: hash("ds"),
            serialized_model_uri: ArtifactUri::parse("file:///tmp/model.bin").expect("uri"),
            serialization_format: output.serialization_format,
            preprocessing: output.preprocessing.clone(),
            metrics: output.metrics.clone(),
        }
    }

    fn inference_row(feature0: Decimal) -> InferenceMatrixRow {
        InferenceMatrixRow {
            market_id: MarketId::new("0xm"),
            token_id: TokenId::new("yes"),
            features: vec![
                FeatureValue::Decimal(feature0),
                FeatureValue::Decimal(dec!(0.4)),
            ],
            context: MarketInferenceContext {
                secondary_token_id: Some(TokenId::new("no")),
                yes_price: Price::new(dec!(0.5)),
                no_price: None,
                liquidity_usd: None,
                data_quality: DataQualityStatus::Fresh,
                time_to_resolution_secs: Some(3_600),
                substitutions: Vec::new(),
            },
        }
    }

    #[tokio::test]
    async fn classical_adapter_train_predict_roundtrip() {
        let output = ClassicalAdapterRegistry::adapter_for(ClassicalKind::RandomForest)
            .train(&training_matrix())
            .expect("train");
        assert_eq!(output.crate_name, CLASSICAL_CRATE_NAME);
        assert!(!output.model_bytes.is_empty(), "estimator serialized");
        assert_eq!(output.metrics.feature_importances.len(), 2);

        let runtime = ClassicalRuntime::load(artifact(&output), &output.model_bytes).expect("load");
        assert_eq!(
            runtime.model_family(),
            ModelFamily::Classical(ClassicalKind::RandomForest)
        );

        let matrix = InferenceMatrix {
            as_of: Utc::now(),
            feature_names: vec![FeatureName::new("f0"), FeatureName::new("f1")],
            rows: vec![inference_row(dec!(0.9)), inference_row(dec!(0.1))],
        };
        let out = runtime
            .infer_batch(ModelRuntimeInput::FeatureMatrix(matrix))
            .await
            .expect("infer");
        // The high-feature row should resolve to a BuyYes candidate.
        let bull = out.candidates.iter().find(|c| c.side == SignalSide::BuyYes);
        assert!(bull.is_some(), "high feature ⇒ BuyYes candidate");
    }

    #[tokio::test]
    async fn classical_artifact_serialize_and_reload_with_version_check() {
        let output = ClassicalAdapterRegistry::adapter_for(ClassicalKind::RandomForest)
            .train(&training_matrix())
            .expect("train");

        // Correct version reloads.
        let ok = ClassicalRuntime::load(artifact(&output), &output.model_bytes);
        assert!(ok.is_ok(), "matching crate version reloads");
        assert_eq!(output.crate_version, CLASSICAL_CRATE_VERSION);

        // A version mismatch is rejected.
        let mut tampered = artifact(&output);
        tampered.crate_version = "9.9".to_owned();
        let err = ClassicalRuntime::load(tampered, &output.model_bytes);
        assert!(err.is_err(), "crate version mismatch must be rejected");
    }
}

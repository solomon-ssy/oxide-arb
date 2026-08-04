//! Model runtime contract: [`QuantModelRuntime`] and the runtime I/O types.
//!
//! [`ModelFamily`] / [`ClassicalKind`] are owned by `quant_pivot_models`
//! (flat Postgres `qp_model_family` labels).

use async_trait::async_trait;
use chrono::{DateTime, Utc};
use quant_pivot_error::{QuantResult, research::ResearchError};
pub(crate) use quant_pivot_models::{
    enums::{
        common::MarketCategory,
        model::ModelFamily,
        quant::{DataQualityStatus, ModelWeightSource, OutcomeSide},
    },
    runtime_config::FactorCrossSectionConfig,
    types::{
        ContentHash, MarketId, ModelRunId, ModelVersionId, Price, Probability, TokenId, Usd,
        factor::FactorServingPlane,
    },
};
use rust_decimal::{Decimal, prelude::ToPrimitive};
use serde::{Deserialize, Serialize};

use crate::{
    factors::{FactorValue, FrozenReferenceQuantiles, NormalizedFactor},
    features::{FeatureCell, FeatureName, NullReason},
    model::signal::SignalCandidate,
    training::LabelName,
};

/// Per-market context the scorer needs beyond the factor vector.
///
/// Carries executable prices (for the entry reference + side selection), liquidity
/// and data-quality (for the governed multipliers), the resolution horizon, and
/// the audited feature substitutions (for the governed confidence penalty).
///
/// Projected by the core `ModelRunner` from the selection snapshot and the
/// market's feature vector — never re-derived inside the runtime.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct MarketInferenceContext {
    /// The NO / secondary outcome token, when binary (targeted by `BuyNo`).
    pub secondary_token_id: Option<TokenId>,
    /// Executable reference price of the YES outcome token.
    pub yes_price: Price,
    /// Executable reference price of the NO outcome token, when binary.
    pub no_price: Option<Price>,
    /// Reported visible liquidity, when known (liquidity multiplier input).
    pub liquidity_usd: Option<Usd>,
    /// Aggregate data-quality classification (data-quality multiplier input).
    pub data_quality: DataQualityStatus,
    /// Seconds until market resolution, when known (horizon multiplier input).
    pub time_to_resolution_secs: Option<u64>,
    /// Reasons of substituted feature cells (confidence-penalty input).
    pub substitution_reasons: Vec<NullReason>,
}

/// One market's factor row for factor-table inference.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct FactorInferenceRow {
    /// Market id.
    pub market_id: MarketId,
    /// Outcome token id (the YES / primary token).
    pub token_id: TokenId,
    /// Computed factor values for the market.
    pub factors: Vec<FactorValue>,
    /// Per-market scoring context (prices, liquidity, quality, horizon).
    pub context: MarketInferenceContext,
}

/// A batch of per-market factor rows (weighted-factor scorer input).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct FactorInferenceTable {
    /// The owning model run; stamped onto every emitted candidate.
    pub model_run_id: ModelRunId,
    /// Frozen decision time for the inference batch.
    pub decision_at: DateTime<Utc>,
    /// Per-market factor rows.
    pub rows: Vec<FactorInferenceRow>,
}

/// Immutable model/transform identity stamped onto weighted input evidence.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct WeightedInputAuditContract {
    pub model_version_id: ModelVersionId,
    pub input_contract_hash: ContentHash,
    pub transform_hash: ContentHash,
    pub training_input_hash: ContentHash,
}

impl FactorInferenceTable {
    /// Project the exact normalized factor inputs consumed by the weighted
    /// runtime. Historical serving fixtures and live inference share this one
    /// projection so their durable evidence cannot drift.
    pub fn weighted_input_audit(
        &self,
        contract: WeightedInputAuditContract,
    ) -> QuantResult<Vec<ModelInputAuditRow>> {
        let row_count = self
            .rows
            .iter()
            .try_fold(0usize, |count, row| count.checked_add(row.factors.len()))
            .ok_or_else(|| ResearchError::Inference {
                detail: "weighted model-input audit row count overflow".to_owned(),
            })?;
        let mut audit = Vec::with_capacity(row_count);
        for row in &self.rows {
            for factor in &row.factors {
                let (raw_state, score) = match &factor.normalization {
                    NormalizedFactor::Scored { score, .. } => {
                        (ModelInputAuditState::Scored, Some(score.inner()))
                    }
                    NormalizedFactor::MissingInput => (ModelInputAuditState::MissingInput, None),
                    NormalizedFactor::NotApplicable => (ModelInputAuditState::NotApplicable, None),
                    NormalizedFactor::Indeterminate { .. } => {
                        (ModelInputAuditState::Indeterminate, None)
                    }
                };
                let encoded_value_bits = score
                    .map(|value| {
                        value
                            .to_f64()
                            .filter(|value| value.is_finite())
                            .ok_or_else(|| ResearchError::Inference {
                                detail: format!(
                                    "weighted factor `{}` normalized score cannot be represented as finite f64",
                                    factor.name
                                ),
                            })
                            .map(f64::to_bits)
                    })
                    .transpose()?;
                audit.push(ModelInputAuditRow {
                    model_version_id: contract.model_version_id,
                    model_family: ModelFamily::WeightedFactor,
                    market_id: row.market_id.clone(),
                    raw_input_name: factor.name.to_string(),
                    raw_state,
                    raw_value: factor.raw_value.map(|value| value.to_string()),
                    encoded_column: format!("{}.normalized_score", factor.name),
                    encoded_value_bits,
                    input_contract_hash: contract.input_contract_hash,
                    transform_hash: contract.transform_hash,
                    training_input_hash: contract.training_input_hash,
                });
            }
        }
        Ok(audit)
    }
}

/// One market's dense feature row for matrix inference.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct InferenceMatrixRow {
    /// Market id.
    pub market_id: MarketId,
    /// Outcome token id.
    pub token_id: TokenId,
    /// Feature cells, column-aligned with [`InferenceMatrix::feature_names`].
    pub features: Vec<FeatureCell>,
    /// Per-market scoring context (prices, liquidity, quality, horizon) — the
    /// classical runtime prices and sides its candidates from this, exactly like
    /// the weighted factor-table path.
    pub context: MarketInferenceContext,
}

/// A dense feature matrix (classical-ML input).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct InferenceMatrix {
    /// Owning model run; stamped onto every emitted candidate.
    pub model_run_id: ModelRunId,
    /// Frozen decision time for the inference batch.
    pub decision_at: DateTime<Utc>,
    /// Column order for every row.
    pub feature_names: Vec<FeatureName>,
    /// Per-market feature rows.
    pub rows: Vec<InferenceMatrixRow>,
}

/// Input to a model runtime's batch inference.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum ModelRuntimeInput {
    /// Per-market factor table (weighted-factor scorer).
    FactorTable(FactorInferenceTable),
    /// Dense feature matrix (classical ML).
    FeatureMatrix(InferenceMatrix),
    // Additional runtime families are intentionally unsupported until they
    // have complete artifact, validation, and serving contracts.
}

impl ModelRuntimeInput {
    /// Frozen decision time for this inference batch.
    #[must_use]
    pub const fn decision_at(&self) -> DateTime<Utc> {
        match self {
            Self::FactorTable(table) => table.decision_at,
            Self::FeatureMatrix(matrix) => matrix.decision_at,
        }
    }

    /// The per-market scoring context for every row, regardless of input shape —
    /// so the backtester can attribute realized outcomes to the data-quality /
    /// liquidity / horizon / substitution stratum each market was scored under.
    #[must_use]
    pub fn market_contexts(&self) -> Vec<(&MarketId, &MarketInferenceContext)> {
        match self {
            Self::FactorTable(table) => table
                .rows
                .iter()
                .map(|row| (&row.market_id, &row.context))
                .collect(),
            Self::FeatureMatrix(matrix) => matrix
                .rows
                .iter()
                .map(|row| (&row.market_id, &row.context))
                .collect(),
        }
    }
}

/// Throughput / latency metrics for one inference batch.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct ModelRuntimeMetrics {
    /// Number of markets scored.
    pub markets_scored: u32,
    /// Number of candidates emitted.
    pub candidates_emitted: u32,
    /// Wall-clock inference duration, in milliseconds.
    pub inference_duration_ms: u64,
}

/// Semantic state of one raw value consumed by a serving model.
///
/// Classical inputs retain the source [`FeatureCell`] state. Weighted inputs
/// retain the factor engine's normalization outcome, so an absent factor score
/// is never encoded as a numeric zero in serving evidence.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ModelInputAuditState {
    Observed,
    Substituted,
    Missing,
    NotApplicable,
    Scored,
    MissingInput,
    Indeterminate,
}

impl ModelInputAuditState {
    /// Stable `ClickHouse` wire value.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Observed => "observed",
            Self::Substituted => "substituted",
            Self::Missing => "missing",
            Self::NotApplicable => "not_applicable",
            Self::Scored => "scored",
            Self::MissingInput => "missing_input",
            Self::Indeterminate => "indeterminate",
        }
    }
}

/// Exact evidence emitted by the same inference transform invocation that fed
/// the estimator.
///
/// `encoded_value_bits` is the IEEE-754 payload consumed by a classical
/// estimator (or the normalized weighted-factor score). It is absent for a
/// factor that was not scored; no sentinel numeric value is permitted.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ModelInputAuditRow {
    pub model_version_id: ModelVersionId,
    pub model_family: ModelFamily,
    pub market_id: MarketId,
    pub raw_input_name: String,
    pub raw_state: ModelInputAuditState,
    pub raw_value: Option<String>,
    pub encoded_column: String,
    pub encoded_value_bits: Option<u64>,
    pub input_contract_hash: ContentHash,
    pub transform_hash: ContentHash,
    pub training_input_hash: ContentHash,
}

/// One calibration-admissible model score before decision deadband, ranking,
/// `TopN`, portfolio allocation, or execution economics.
///
/// The score is bound to the outcome token whose realized payout is the
/// calibration target. Keeping this population separate from emitted
/// [`SignalCandidate`]s prevents a decision gate from censoring probability
/// calibration evidence.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ModelCalibrationScore {
    pub market_id: MarketId,
    pub token_id: TokenId,
    pub outcome_side: OutcomeSide,
    pub composite_score: Probability,
    pub prediction_horizon_secs: u64,
}

impl From<&SignalCandidate> for ModelCalibrationScore {
    fn from(candidate: &SignalCandidate) -> Self {
        Self {
            market_id: candidate.market_id.clone(),
            token_id: candidate.token_id.clone(),
            outcome_side: candidate.outcome_side,
            composite_score: candidate.composite_score,
            prediction_horizon_secs: candidate.suggested_horizon_secs,
        }
    }
}

/// Exact supervised target whose realized value is comparable with a model's
/// canonical ranking output.
///
/// The binding is carried with every score so a replay cannot accidentally
/// compare a signed canonical-YES prediction with the selected side's payout,
/// or compare a forward-return regressor with a settlement label.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ModelRankTarget {
    pub label_name: LabelName,
    pub label_horizon_secs: u64,
}

/// One allocation-independent ranking prediction in the estimator's canonical
/// supervised-target space.
///
/// Unlike [`ModelCalibrationScore`], `score` is deliberately not constrained to
/// `[0, 1]`: weighted-factor models emit signed canonical-YES alpha and
/// classical regressors emit their raw target-unit prediction.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ModelRankScore {
    pub market_id: MarketId,
    /// Canonical token bound to the training row, never the side selected for a
    /// downstream buy decision.
    pub token_id: TokenId,
    pub score: Decimal,
    pub target: ModelRankTarget,
}

/// Output of a model runtime's batch inference.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ModelRuntimeOutput {
    /// Complete calibration-admissible score population. This is intentionally
    /// upstream of the decision deadband represented by `candidates`.
    pub calibration_scores: Vec<ModelCalibrationScore>,
    /// Complete canonical ranking-score population. Rank-quality validation
    /// consumes only this field and the exactly matching frozen target labels.
    pub rank_scores: Vec<ModelRankScore>,
    /// Emitted candidates (pre-portfolio).
    pub candidates: Vec<SignalCandidate>,
    /// Runtime metrics for the batch.
    pub runtime_metrics: ModelRuntimeMetrics,
    /// Exact model inputs produced during this inference call.
    pub input_audit: Vec<ModelInputAuditRow>,
}

/// Unified inference entry point. Business layers depend only on this trait.
#[async_trait]
pub trait QuantModelRuntime: Send + Sync {
    /// The immutable model version this runtime serves.
    fn model_version_id(&self) -> ModelVersionId;

    /// The model family.
    fn model_family(&self) -> ModelFamily;

    /// The feature-schema hash this runtime was built against; a mismatch with
    /// the active schema must abort inference.
    fn feature_schema_hash(&self) -> ContentHash;

    /// Features this model requires; surfaced to the selector so a market
    /// missing any of them is filtered before it reaches inference. Empty means
    /// the model imposes no extra selection requirement.
    fn required_features(&self) -> Vec<FeatureName>;

    /// Ordered raw inputs needed to assemble this runtime's inference payload.
    ///
    /// Defaults to required features. Classical runtimes override this to add
    /// optional inputs without making them pre-selection rejection gates.
    fn input_features(&self) -> Vec<FeatureName> {
        self.required_features()
    }

    /// The single market category this runtime's frozen artifact declares
    /// itself scoped to, or `None` for the pooled route. Atomic serving
    /// generation preparation requires every configured pointer to equal its
    /// sealed route scope before any generation is published. Defaults to
    /// `None`; only the weighted-factor artifact carries a scope today.
    fn category_scope(&self) -> Option<MarketCategory> {
        None
    }

    /// Immutable estimator-weight provenance. Serving artifacts always return
    /// [`ModelWeightSource::Artifact`].
    fn weight_source(&self) -> ModelWeightSource {
        ModelWeightSource::Artifact
    }

    /// Weighted-only normalization policy frozen in the artifact.
    fn factor_cross_section(&self) -> Option<&FactorCrossSectionConfig> {
        None
    }

    /// Exact content-addressed factor plane frozen in a factor-native artifact.
    ///
    /// Classical runtimes return `None` because their serving contracts carry
    /// the canonical empty plane and never enter factor computation.
    fn factor_serving_plane(&self) -> Option<&FactorServingPlane> {
        None
    }

    /// Weighted-only train-fitted reference distributions. Classical runtimes
    /// return `None` because they never enter the factor plane.
    fn frozen_reference_quantiles(&self) -> Option<&FrozenReferenceQuantiles> {
        None
    }

    /// Score a batch, producing candidates.
    async fn infer_batch(&self, input: ModelRuntimeInput) -> QuantResult<ModelRuntimeOutput>;
}

#[cfg(test)]
mod tests {
    use std::str::FromStr;

    use quant_pivot_models::enums::model::{ClassicalKind, ModelFamily};

    #[test]
    fn model_family_string_roundtrip() {
        for family in [
            ModelFamily::WeightedFactor,
            ModelFamily::from_classical(ClassicalKind::RandomForest),
            ModelFamily::from_classical(ClassicalKind::ElasticNet),
        ] {
            let text = family.to_string();
            let parsed = ModelFamily::from_str(&text).expect("round-trip");
            assert_eq!(parsed, family);
        }
    }

    #[test]
    fn unknown_family_is_rejected() {
        assert!(ModelFamily::from_str("mystery").is_err());
        assert!(ModelFamily::from_str("classical:transformer").is_err());
    }
}

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
    enums::quant::{ModelSerializationFormat, OutcomeSide},
    hashing::CanonicalDigest,
    types::{
        ContentHash, ModelRunId, ModelVersionId, Price, Probability, SignalCandidateId, TokenId,
        Usd,
    },
};
use rust_decimal::{Decimal, prelude::FromPrimitive};
use smartcore::linalg::basic::matrix::DenseMatrix;

use crate::{
    features::{FeatureCell, FeatureCellState, FeatureName},
    model::{
        CLASSICAL_CRATE_NAME,
        artifact::{ClassicalModelArtifact, ClassicalOutputSemantics},
        classical::{CLASSICAL_CRATE_VERSION, SmartcoreModel},
        runtime::{
            InferenceMatrix, InferenceMatrixRow, MarketInferenceContext, ModelFamily,
            ModelInputAuditRow, ModelInputAuditState, ModelRuntimeInput, ModelRuntimeMetrics,
            ModelRuntimeOutput, QuantModelRuntime,
        },
        signal::{ModelExplanation, SignalCandidate, SignalWarning},
    },
    precision::RESEARCH_DECIMAL_SCALE,
    training::model_input_cell,
};

/// A long binary outcome token can lose at most its full cost basis.
const MAX_LONG_DOWNSIDE_BPS: i64 = 10_000;

/// A loaded classical model runtime.
pub struct ClassicalRuntime {
    artifact: ClassicalModelArtifact,
    model: SmartcoreModel,
}

struct TransformedInferenceRow {
    encoded_values: Vec<f64>,
    input_audit: Vec<ModelInputAuditRow>,
}

struct ClassicalEconomicProjection {
    outcome_side: OutcomeSide,
    token_id: TokenId,
    entry_price_ref: Price,
    expected_return_bps: Decimal,
    raw_prediction: Decimal,
}

struct ClassicalCandidateScores {
    composite_score: Probability,
    confidence: Probability,
    liquidity_score: Probability,
    data_quality_score: Probability,
    suggested_horizon_secs: u64,
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
        artifact.validate()?;
        if artifact.crate_name != CLASSICAL_CRATE_NAME {
            return Err(ResearchError::RuntimeUnavailable {
                family: artifact.kind.to_string(),
                detail: format!(
                    "classical crate name mismatch: artifact `{}`, runtime `{}`",
                    artifact.crate_name, CLASSICAL_CRATE_NAME
                ),
            }
            .into());
        }
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
        let actual_hash = ContentHash::parse(CanonicalDigest::prefixed_bytes(model_bytes))?;
        if actual_hash != artifact.serialized_model_hash {
            return Err(ResearchError::InvalidModelArtifact {
                detail: format!(
                    "classical estimator bytes hash mismatch: expected {}, got {}",
                    artifact.serialized_model_hash, actual_hash
                ),
            }
            .into());
        }
        let model: SmartcoreModel =
            bincode::deserialize(model_bytes).map_err(|error| ResearchError::Serialization {
                detail: format!("bincode deserialize classical model: {error}"),
            })?;
        if !model.matches_kind(artifact.kind) {
            return Err(ResearchError::InvalidModelArtifact {
                detail: format!(
                    "serialized estimator variant does not match artifact kind {}",
                    artifact.kind
                ),
            }
            .into());
        }
        model.validate_width(artifact.input_transform.encoded_columns.len())?;
        Ok(Self { artifact, model })
    }

    /// Apply the exact frozen training transform to one inference row.
    fn transformed_row(
        &self,
        row: &InferenceMatrixRow,
        names: &[FeatureName],
    ) -> QuantResult<TransformedInferenceRow> {
        if row.features.len() != names.len() {
            return Err(ResearchError::Inference {
                detail: format!(
                    "classical row width {} does not match raw input-name width {}",
                    row.features.len(),
                    names.len()
                ),
            }
            .into());
        }
        if self.artifact.input_transform.inputs.len() != names.len() {
            return Err(ResearchError::InvalidModelArtifact {
                detail: format!(
                    "classical transform raw input width {} does not match artifact name width {}",
                    self.artifact.input_transform.inputs.len(),
                    names.len()
                ),
            }
            .into());
        }
        let cells = self
            .artifact
            .input_transform
            .inputs
            .iter()
            .zip(names)
            .zip(&row.features)
            .map(|((input, name), cell)| {
                if &input.feature != name {
                    return Err(ResearchError::InvalidModelArtifact {
                        detail: format!(
                            "classical transform input `{}` is misaligned with ordered raw input `{name}`",
                            input.feature
                        ),
                    }
                    .into());
                }
                model_input_cell(Some(cell), name, input.value_kind)
            })
            .collect::<QuantResult<Vec<_>>>()?;
        let encoded_values = self.artifact.input_transform.apply_cells(&cells)?;
        let input_audit = self.project_input_audit(row, names, &encoded_values)?;
        Ok(TransformedInferenceRow {
            encoded_values,
            input_audit,
        })
    }

    fn project_input_audit(
        &self,
        row: &InferenceMatrixRow,
        names: &[FeatureName],
        encoded_values: &[f64],
    ) -> QuantResult<Vec<ModelInputAuditRow>> {
        if encoded_values.len() != self.artifact.input_transform.encoded_columns.len() {
            return Err(ResearchError::Inference {
                detail: format!(
                    "classical audit width mismatch: transform emitted {}, contract declares {}",
                    encoded_values.len(),
                    self.artifact.input_transform.encoded_columns.len()
                ),
            }
            .into());
        }
        let input_contract_hash = self.artifact.input_contract_hash.clone();
        let transform_hash = self.artifact.input_transform_hash.clone();
        let training_input_hash = self.artifact.training_input_hash.clone();
        self.artifact
            .input_transform
            .encoded_columns
            .iter()
            .zip(encoded_values)
            .map(|(column, value)| {
                if !value.is_finite() {
                    return Err(ResearchError::Inference {
                        detail: format!(
                            "encoded column `{}` produced a non-finite audit value",
                            column.name
                        ),
                    }
                    .into());
                }
                let source_index = names
                    .iter()
                    .position(|name| name == &column.source_feature)
                    .ok_or_else(|| ResearchError::InvalidModelArtifact {
                        detail: format!(
                            "encoded column `{}` references absent raw input `{}`",
                            column.name, column.source_feature
                        ),
                    })?;
                let cell =
                    row.features
                        .get(source_index)
                        .ok_or_else(|| ResearchError::Inference {
                            detail: format!(
                                "raw input `{}` index {source_index} is absent from inference row",
                                column.source_feature
                            ),
                        })?;
                let (raw_state, raw_value) = classical_raw_evidence(cell)?;
                Ok(ModelInputAuditRow {
                    model_version_id: self.artifact.header.model_version_id.clone(),
                    model_family: self.artifact.header.model_family,
                    market_id: row.market_id.clone(),
                    raw_input_name: column.source_feature.to_string(),
                    raw_state,
                    raw_value,
                    encoded_column: column.name.to_string(),
                    encoded_value_bits: Some(value.to_bits()),
                    input_contract_hash: input_contract_hash.clone(),
                    transform_hash: transform_hash.clone(),
                    training_input_hash: training_input_hash.clone(),
                })
            })
            .collect()
    }

    /// Build a shadow-only sided candidate from the artifact's frozen output
    /// semantics. No classical estimator output is interpreted by convention.
    fn candidate(
        &self,
        prediction: f64,
        row: &InferenceMatrixRow,
        model_run_id: &ModelRunId,
        decision_at: DateTime<Utc>,
    ) -> QuantResult<Option<SignalCandidate>> {
        let raw_prediction = f64_to_decimal(prediction)?;
        let Some(projection) = self.project_economics(raw_prediction, row)? else {
            return Ok(None);
        };
        let context = &row.context;
        let Some(scores) = self.candidate_scores(&projection, context)? else {
            return Ok(None);
        };
        let semantics = match self.artifact.output_semantics {
            ClassicalOutputSemantics::ForwardReturnBps => "forward_return_bps",
            ClassicalOutputSemantics::SettlementProbability => "settlement_probability",
        };

        Ok(Some(SignalCandidate {
            signal_candidate_id: SignalCandidateId::from_v7(),
            model_run_id: model_run_id.clone(),
            market_id: row.market_id.clone(),
            token_id: projection.token_id,
            outcome_side: projection.outcome_side,
            composite_score: scores.composite_score,
            confidence: scores.confidence,
            expected_return_bps: projection.expected_return_bps,
            downside_bps: Decimal::from(MAX_LONG_DOWNSIDE_BPS),
            // Classical is explicitly ShadowOnly until 11.9 binds an
            // independently validated probability/return/downside calibration.
            win_probability: None,
            entry_price_ref: projection.entry_price_ref,
            suggested_horizon_secs: scores.suggested_horizon_secs,
            factor_breakdown: Vec::new(),
            model_explanation: ModelExplanation {
                headline: format!(
                    "classical shadow buy {}: {semantics} raw {}, projected edge {} bps",
                    projection.outcome_side,
                    projection.raw_prediction,
                    projection.expected_return_bps
                ),
                top_positive: Vec::new(),
                top_negative: Vec::new(),
            },
            rejection_warnings: candidate_warnings(context),
            rank_before_portfolio: 0,
            liquidity_score: scores.liquidity_score,
            data_quality_score: scores.data_quality_score,
            model_score_percentile: Probability::ZERO,
            decision_at,
        }))
    }

    fn candidate_scores(
        &self,
        projection: &ClassicalEconomicProjection,
        context: &MarketInferenceContext,
    ) -> QuantResult<Option<ClassicalCandidateScores>> {
        let data_quality_score = clamp_unit(
            self.artifact
                .multipliers
                .data_quality
                .multiplier_for(context.data_quality),
        );
        let liquidity_score = clamp_unit(
            self.artifact
                .multipliers
                .liquidity
                .multiplier_for(context.liquidity_usd.map(Usd::inner)),
        );
        let horizon_multiplier = self.artifact.multipliers.horizon.multiplier_for(
            context.time_to_resolution_secs,
            self.artifact.prediction_horizon_secs,
        );
        let edge_strength = bounded_edge_strength(projection.expected_return_bps)?;
        let composite_score = clamp_unit(checked_product(
            "classical composite score",
            &[
                edge_strength,
                data_quality_score.inner(),
                liquidity_score.inner(),
                horizon_multiplier,
            ],
        )?);
        let substitution_penalty =
            context
                .substitution_reasons
                .iter()
                .try_fold(Decimal::ONE, |acc, reason| {
                    checked_mul(
                        "classical substitution confidence",
                        acc,
                        self.artifact
                            .substitution_confidence_rules
                            .multiplier_for(*reason),
                    )
                })?;
        let confidence = clamp_unit(checked_mul(
            "classical confidence",
            data_quality_score.inner(),
            substitution_penalty,
        )?);
        let suggested_horizon_secs = context
            .time_to_resolution_secs
            .map_or(self.artifact.prediction_horizon_secs, |remaining| {
                remaining.min(self.artifact.prediction_horizon_secs)
            });
        Ok(
            (suggested_horizon_secs > 0).then_some(ClassicalCandidateScores {
                composite_score,
                confidence,
                liquidity_score,
                data_quality_score,
                suggested_horizon_secs,
            }),
        )
    }

    fn project_economics(
        &self,
        raw_prediction: Decimal,
        row: &InferenceMatrixRow,
    ) -> QuantResult<Option<ClassicalEconomicProjection>> {
        match self.artifact.output_semantics {
            ClassicalOutputSemantics::ForwardReturnBps => {
                project_forward_return(raw_prediction, row)
            }
            ClassicalOutputSemantics::SettlementProbability => {
                project_settlement_probability(raw_prediction, row)
            }
        }
    }
}

fn candidate_warnings(context: &MarketInferenceContext) -> Vec<SignalWarning> {
    let mut warnings = vec![SignalWarning::Other(
        "shadow_only: classical output has no independent probability-to-return/downside calibration"
            .to_owned(),
    )];
    if context.liquidity_usd.is_none() {
        warnings.push(SignalWarning::ThinLiquidity);
    }
    if !context.substitution_reasons.is_empty() {
        warnings.push(SignalWarning::Other(format!(
            "{} governed input substitutions applied",
            context.substitution_reasons.len()
        )));
    }
    warnings
}

#[async_trait]
impl QuantModelRuntime for ClassicalRuntime {
    fn model_version_id(&self) -> ModelVersionId {
        self.artifact.header.model_version_id.clone()
    }

    fn model_family(&self) -> ModelFamily {
        ModelFamily::from_classical(self.artifact.kind)
    }

    fn feature_schema_hash(&self) -> ContentHash {
        self.artifact.header.feature_schema_hash.clone()
    }

    fn required_features(&self) -> Vec<FeatureName> {
        self.artifact.required_features()
    }

    fn input_features(&self) -> Vec<FeatureName> {
        self.artifact.input_features()
    }

    async fn infer_batch(&self, input: ModelRuntimeInput) -> QuantResult<ModelRuntimeOutput> {
        let started = Instant::now();
        let matrix = expect_feature_matrix(input)?;
        let markets_scored =
            u32::try_from(matrix.rows.len()).map_err(|error| ResearchError::Inference {
                detail: format!("classical market count does not fit u32: {error}"),
            })?;

        if matrix.rows.is_empty() {
            return Ok(ModelRuntimeOutput {
                candidates: Vec::new(),
                runtime_metrics: ModelRuntimeMetrics {
                    markets_scored: 0,
                    candidates_emitted: 0,
                    inference_duration_ms: 0,
                },
                input_audit: Vec::new(),
            });
        }

        let expected_names = self.artifact.input_features();
        if matrix.feature_names != expected_names {
            return Err(ResearchError::Inference {
                detail: "classical raw input contract/order does not match its artifact".to_owned(),
            }
            .into());
        }
        let transformed = matrix
            .rows
            .iter()
            .map(|row| self.transformed_row(row, &matrix.feature_names))
            .collect::<QuantResult<Vec<_>>>()?;
        let rows = transformed
            .iter()
            .map(|row| row.encoded_values.clone())
            .collect::<Vec<_>>();
        let dense = DenseMatrix::from_2d_vec(&rows).map_err(|error| ResearchError::Inference {
            detail: format!("classical dense matrix build failed: {error}"),
        })?;
        let predictions = self.model.predict(&dense)?;

        let mut candidates = Vec::new();
        for (row, prediction) in matrix.rows.iter().zip(predictions) {
            if let Some(candidate) =
                self.candidate(prediction, row, &matrix.model_run_id, matrix.decision_at)?
            {
                candidates.push(candidate);
            }
        }
        candidates.sort_by(|a, b| {
            b.composite_score
                .inner()
                .cmp(&a.composite_score.inner())
                .then_with(|| a.market_id.as_str().cmp(b.market_id.as_str()))
        });
        for (index, candidate) in candidates.iter_mut().enumerate() {
            candidate.rank_before_portfolio =
                u32::try_from(index + 1).map_err(|error| ResearchError::Inference {
                    detail: format!("classical candidate rank does not fit u32: {error}"),
                })?;
        }

        let candidates_emitted =
            u32::try_from(candidates.len()).map_err(|error| ResearchError::Inference {
                detail: format!("classical candidate count does not fit u32: {error}"),
            })?;
        let inference_duration_ms =
            u64::try_from(started.elapsed().as_millis()).map_err(|error| {
                ResearchError::Inference {
                    detail: format!("classical inference duration does not fit u64: {error}"),
                }
            })?;
        Ok(ModelRuntimeOutput {
            candidates,
            runtime_metrics: ModelRuntimeMetrics {
                markets_scored,
                candidates_emitted,
                inference_duration_ms,
            },
            input_audit: transformed
                .into_iter()
                .flat_map(|row| row.input_audit)
                .collect(),
        })
    }
}

fn classical_raw_evidence(
    cell: &FeatureCell,
) -> QuantResult<(ModelInputAuditState, Option<String>)> {
    let state = match cell.state {
        FeatureCellState::Observed => ModelInputAuditState::Observed,
        FeatureCellState::Substituted => ModelInputAuditState::Substituted,
        FeatureCellState::Missing => ModelInputAuditState::Missing,
        FeatureCellState::NotApplicable => ModelInputAuditState::NotApplicable,
    };
    let raw_value = cell
        .value()
        .map(|value| {
            serde_json::to_string(value).map_err(|error| ResearchError::Serialization {
                detail: format!("serialize classical raw model input: {error}"),
            })
        })
        .transpose()?;
    Ok((state, raw_value))
}

fn project_forward_return(
    raw_return_bps: Decimal,
    row: &InferenceMatrixRow,
) -> QuantResult<Option<ClassicalEconomicProjection>> {
    let yes_entry = row.context.yes_price.inner();
    if yes_entry <= Decimal::ZERO || yes_entry > Decimal::ONE {
        return Err(ResearchError::Inference {
            detail: format!("classical YES entry price must be within (0, 1], got {yes_entry}"),
        }
        .into());
    }
    let growth = checked_add(
        "classical forward-return growth",
        Decimal::ONE,
        checked_div(
            "classical forward-return bps conversion",
            raw_return_bps,
            Decimal::from(MAX_LONG_DOWNSIDE_BPS),
        )?,
    )?;
    let yes_exit = checked_mul("classical projected YES exit", yes_entry, growth)?
        .clamp(Decimal::ZERO, Decimal::ONE);
    let yes_return_bps = return_bps(yes_entry, yes_exit)?;
    let yes = positive_projection(
        OutcomeSide::Yes,
        row.token_id.clone(),
        row.context.yes_price,
        yes_return_bps,
        raw_return_bps,
    );

    let no = if let Some((token_id, entry_price)) = resolve_entry(row, OutcomeSide::No) {
        let no_exit = checked_sub("classical projected NO exit", Decimal::ONE, yes_exit)?;
        let no_return_bps = return_bps(entry_price.inner(), no_exit)?;
        positive_projection(
            OutcomeSide::No,
            token_id,
            entry_price,
            no_return_bps,
            raw_return_bps,
        )
    } else {
        None
    };
    Ok(stronger_projection(yes, no))
}

fn project_settlement_probability(
    yes_probability: Decimal,
    row: &InferenceMatrixRow,
) -> QuantResult<Option<ClassicalEconomicProjection>> {
    if !(Decimal::ZERO..=Decimal::ONE).contains(&yes_probability) {
        return Err(ResearchError::Inference {
            detail: format!(
                "logistic classical prediction must be within [0, 1], got {yes_probability}"
            ),
        }
        .into());
    }
    let yes = expected_settlement_projection(
        OutcomeSide::Yes,
        row.token_id.clone(),
        row.context.yes_price,
        yes_probability,
        yes_probability,
    )?;
    let no = resolve_entry(row, OutcomeSide::No)
        .map(|(token_id, entry_price)| {
            let no_probability = checked_sub(
                "classical NO settlement probability",
                Decimal::ONE,
                yes_probability,
            )?;
            expected_settlement_projection(
                OutcomeSide::No,
                token_id,
                entry_price,
                no_probability,
                yes_probability,
            )
        })
        .transpose()?
        .flatten();
    Ok(stronger_projection(yes, no))
}

fn expected_settlement_projection(
    outcome_side: OutcomeSide,
    token_id: TokenId,
    entry_price: Price,
    win_probability: Decimal,
    raw_prediction: Decimal,
) -> QuantResult<Option<ClassicalEconomicProjection>> {
    let price = entry_price.inner();
    if price <= Decimal::ZERO || price > Decimal::ONE {
        return Err(ResearchError::Inference {
            detail: format!(
                "classical {outcome_side} entry price must be within (0, 1], got {price}"
            ),
        }
        .into());
    }
    let gross_multiple = checked_div(
        "classical settlement expected gross multiple",
        win_probability,
        price,
    )?;
    let expected_return_bps = checked_mul(
        "classical settlement expected return",
        checked_sub(
            "classical settlement net multiple",
            gross_multiple,
            Decimal::ONE,
        )?,
        Decimal::from(MAX_LONG_DOWNSIDE_BPS),
    )?
    .round_dp(RESEARCH_DECIMAL_SCALE);
    Ok(positive_projection(
        outcome_side,
        token_id,
        entry_price,
        expected_return_bps,
        raw_prediction,
    ))
}

fn positive_projection(
    outcome_side: OutcomeSide,
    token_id: TokenId,
    entry_price_ref: Price,
    expected_return_bps: Decimal,
    raw_prediction: Decimal,
) -> Option<ClassicalEconomicProjection> {
    (expected_return_bps > Decimal::ZERO).then_some(ClassicalEconomicProjection {
        outcome_side,
        token_id,
        entry_price_ref,
        expected_return_bps: expected_return_bps.round_dp(RESEARCH_DECIMAL_SCALE),
        raw_prediction,
    })
}

fn stronger_projection(
    yes: Option<ClassicalEconomicProjection>,
    no: Option<ClassicalEconomicProjection>,
) -> Option<ClassicalEconomicProjection> {
    match (yes, no) {
        (Some(yes), Some(no)) if no.expected_return_bps > yes.expected_return_bps => Some(no),
        (Some(yes), _) => Some(yes),
        (None, no) => no,
    }
}

fn return_bps(entry: Decimal, exit: Decimal) -> QuantResult<Decimal> {
    if entry <= Decimal::ZERO {
        return Err(ResearchError::Inference {
            detail: "classical return projection requires a positive entry price".to_owned(),
        }
        .into());
    }
    let change = checked_sub("classical projected price change", exit, entry)?;
    let fraction = checked_div("classical projected return fraction", change, entry)?;
    checked_mul(
        "classical projected return bps",
        fraction,
        Decimal::from(MAX_LONG_DOWNSIDE_BPS),
    )
    .map(|value| value.round_dp(RESEARCH_DECIMAL_SCALE))
}

fn bounded_edge_strength(expected_return_bps: Decimal) -> QuantResult<Decimal> {
    if expected_return_bps <= Decimal::ZERO {
        return Err(ResearchError::Inference {
            detail: "classical candidate requires a positive projected return".to_owned(),
        }
        .into());
    }
    let denominator = checked_add(
        "classical edge-strength denominator",
        expected_return_bps,
        Decimal::from(MAX_LONG_DOWNSIDE_BPS),
    )?;
    checked_div(
        "classical bounded edge strength",
        expected_return_bps,
        denominator,
    )
}

fn checked_product(label: &str, values: &[Decimal]) -> QuantResult<Decimal> {
    values
        .iter()
        .try_fold(Decimal::ONE, |acc, value| checked_mul(label, acc, *value))
}

fn checked_add(label: &str, left: Decimal, right: Decimal) -> QuantResult<Decimal> {
    left.checked_add(right)
        .ok_or_else(|| ResearchError::Inference {
            detail: format!("{label} overflow"),
        })
        .map_err(Into::into)
}

fn checked_sub(label: &str, left: Decimal, right: Decimal) -> QuantResult<Decimal> {
    left.checked_sub(right)
        .ok_or_else(|| ResearchError::Inference {
            detail: format!("{label} overflow"),
        })
        .map_err(Into::into)
}

fn checked_mul(label: &str, left: Decimal, right: Decimal) -> QuantResult<Decimal> {
    left.checked_mul(right)
        .ok_or_else(|| ResearchError::Inference {
            detail: format!("{label} overflow"),
        })
        .map_err(Into::into)
}

fn checked_div(label: &str, numerator: Decimal, denominator: Decimal) -> QuantResult<Decimal> {
    numerator
        .checked_div(denominator)
        .ok_or_else(|| ResearchError::Inference {
            detail: format!("{label} is undefined or overflowed"),
        })
        .map_err(Into::into)
}

fn clamp_unit(value: Decimal) -> Probability {
    Probability::new(value.clamp(Decimal::ZERO, Decimal::ONE))
}

/// Resolve the target token + entry price for the chosen outcome.
fn resolve_entry(row: &InferenceMatrixRow, outcome_side: OutcomeSide) -> Option<(TokenId, Price)> {
    let context: &MarketInferenceContext = &row.context;
    match outcome_side {
        OutcomeSide::Yes => Some((row.token_id.clone(), context.yes_price)),
        OutcomeSide::No => {
            let token = context.secondary_token_id.clone()?;
            let price = context.no_price?;
            Some((token, price))
        }
    }
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

/// Convert an `f64` to a research-scale `Decimal`.
fn f64_to_decimal(value: f64) -> QuantResult<Decimal> {
    if !value.is_finite() {
        return Err(ResearchError::Inference {
            detail: "classical prediction is not finite".to_owned(),
        }
        .into());
    }
    Decimal::from_f64(value)
        .map(|value| value.round_dp(RESEARCH_DECIMAL_SCALE))
        .ok_or_else(|| {
            ResearchError::Inference {
                detail: "classical prediction cannot be represented as Decimal".to_owned(),
            }
            .into()
        })
}

#[cfg(test)]
mod tests {
    use super::{ClassicalRuntime, project_settlement_probability};
    use crate::{
        features::{
            FeatureCell, FeatureName, FeatureStaleness, FeatureUnit, FeatureValue, FeatureValueKind,
        },
        model::{
            ModelInputAuditState, SignalWarning,
            artifact::{
                ClassicalModelArtifact, ClassicalOutputSemantics, ModelArtifactHeader,
                ScoreMultiplierSpec, SubstitutionConfidenceRules, model_input_contract_hash,
            },
            classical::{
                CLASSICAL_CRATE_NAME, CLASSICAL_CRATE_VERSION, ClassicalAdapterRegistry,
                ClassicalTrainOutput,
            },
            runtime::{
                ClassicalKind, InferenceMatrix, InferenceMatrixRow, MarketInferenceContext,
                ModelFamily, ModelRuntimeInput, QuantModelRuntime,
            },
        },
        training::{FeatureColumnSpec, ModelInputCell, TrainingMatrix},
    };
    use chrono::{DateTime, TimeZone, Utc};
    use ndarray::Array1;
    use quant_pivot_models::{
        enums::quant::{DataQualityStatus, OutcomeSide},
        types::{
            ArtifactUri, ContentHash, MarketId, ModelArtifactId, ModelInputRequiredness,
            ModelInputSpec, ModelRunId, ModelVersionId, Price, TokenId,
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

    fn fixture_row_secs(i: usize) -> i64 {
        i64::try_from(i).expect("fixture row index fits i64")
    }

    fn fixture_ts(offset_secs: i64) -> DateTime<Utc> {
        Utc.timestamp_opt(1_700_000_000 + offset_secs, 0)
            .single()
            .expect("ts")
    }

    /// A linearly separable matrix: label = 1 when feature-0 is high.
    fn training_matrix() -> TrainingMatrix {
        let rows = 40usize;
        let mut labels = Array1::<f64>::zeros(rows);
        let mut cells = Vec::with_capacity(rows);
        for i in 0..rows {
            let high = i % 2 == 0;
            cells.push(vec![
                ModelInputCell::Observed(if high { dec!(0.9) } else { dec!(0.1) }),
                ModelInputCell::Observed(
                    Decimal::from(u64::try_from(i % 5).expect("small remainder"))
                        / Decimal::from(5),
                ),
            ]);
            labels[i] = if high { 1.0 } else { 0.0 };
        }
        TrainingMatrix {
            cells,
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
            row_label_horizon_end: (0..rows)
                .map(|i| fixture_ts(fixture_row_secs(i) + 60))
                .collect(),
        }
    }

    fn artifact(output: &ClassicalTrainOutput) -> ClassicalModelArtifact {
        ClassicalModelArtifact {
            header: ModelArtifactHeader {
                model_version_id: ModelVersionId::from_v7(),
                model_family: ModelFamily::from_classical(ClassicalKind::RandomForest),
                feature_schema_hash: hash("feat"),
                factor_schema_hash: hash("fac"),
                trade_policy_artifact_id: None,
                trade_policy_hash: None,
            },
            artifact_id: ModelArtifactId::from_v7(),
            kind: output.kind,
            crate_name: output.crate_name.clone(),
            crate_version: output.crate_version.clone(),
            label_schema_hash: hash("lab"),
            training_dataset_hash: hash("ds"),
            prediction_horizon_secs: 3_600,
            output_semantics: ClassicalOutputSemantics::ForwardReturnBps,
            multipliers: ScoreMultiplierSpec::conservative(),
            substitution_confidence_rules: SubstitutionConfidenceRules::conservative(),
            input_contract: output.input_contract.clone(),
            input_contract_hash: output.input_contract_hash.clone(),
            input_transform_hash: output.input_transform_hash.clone(),
            training_input_hash: output.training_input_hash.clone(),
            serialized_model_uri: ArtifactUri::parse("file:///tmp/model.bin").expect("uri"),
            serialized_model_hash: output.model_bytes_hash.clone(),
            serialization_format: output.serialization_format,
            input_transform: output.input_transform.clone(),
            metrics: output.metrics.clone(),
        }
    }

    fn inference_row(feature0: Decimal) -> InferenceMatrixRow {
        InferenceMatrixRow {
            market_id: MarketId::new("0xm"),
            token_id: TokenId::new("yes"),
            features: vec![
                FeatureCell::observed(
                    FeatureValue::Decimal(feature0),
                    None,
                    FeatureStaleness::Unknown,
                ),
                FeatureCell::observed(
                    FeatureValue::Decimal(dec!(0.4)),
                    None,
                    FeatureStaleness::Unknown,
                ),
            ],
            context: MarketInferenceContext {
                secondary_token_id: Some(TokenId::new("no")),
                yes_price: Price::new(dec!(0.5)),
                no_price: Some(Price::new(dec!(0.52))),
                liquidity_usd: None,
                data_quality: DataQualityStatus::Fresh,
                time_to_resolution_secs: Some(3_600),
                substitution_reasons: Vec::new(),
            },
        }
    }

    #[test]
    fn classical_no_projection_requires_the_exact_secondary_ask() {
        let mut row = inference_row(dec!(0.1));
        row.context.no_price = None;
        let missing =
            project_settlement_probability(dec!(0.1), &row).expect("valid settlement probability");
        assert!(
            missing.is_none(),
            "a bearish prediction must be rejected when the NO ask is missing"
        );

        row.context.no_price = Some(Price::new(dec!(0.57)));
        let no = project_settlement_probability(dec!(0.1), &row)
            .expect("valid settlement probability")
            .expect("quoted NO side has positive edge");
        assert_eq!(no.outcome_side, OutcomeSide::No);
        assert_eq!(no.entry_price_ref.inner(), dec!(0.57));
        assert_ne!(
            no.entry_price_ref.inner(),
            Decimal::ONE - row.context.yes_price.inner(),
            "the executable NO price is never synthesized from the YES quote"
        );
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
            ModelFamily::from_classical(ClassicalKind::RandomForest)
        );

        let matrix = InferenceMatrix {
            model_run_id: ModelRunId::from_v7(),
            decision_at: Utc::now(),
            feature_names: vec![FeatureName::new("f0"), FeatureName::new("f1")],
            rows: vec![inference_row(dec!(0.9)), inference_row(dec!(0.1))],
        };
        let out = runtime
            .infer_batch(ModelRuntimeInput::FeatureMatrix(matrix))
            .await
            .expect("infer");
        let expected_bits = [
            vec![
                ModelInputCell::Observed(dec!(0.9)),
                ModelInputCell::Observed(dec!(0.4)),
            ],
            vec![
                ModelInputCell::Observed(dec!(0.1)),
                ModelInputCell::Observed(dec!(0.4)),
            ],
        ]
        .iter()
        .flat_map(|cells| {
            output
                .input_transform
                .apply_cells(cells)
                .expect("fixture transform")
                .into_iter()
                .map(f64::to_bits)
        })
        .collect::<Vec<_>>();
        assert_eq!(
            out.input_audit
                .iter()
                .map(|row| row.encoded_value_bits.expect("classical encoded bits"))
                .collect::<Vec<_>>(),
            expected_bits,
            "serving evidence must preserve the exact estimator input bytes"
        );
        assert!(out.input_audit.iter().all(|row| {
            let state_matches = row.raw_state == ModelInputAuditState::Observed;
            let contract_matches = row.input_contract_hash == output.input_contract_hash;
            let transform_matches = row.transform_hash == output.input_transform_hash;
            let training_input_matches = row.training_input_hash == output.training_input_hash;
            state_matches
                && row.raw_value.is_some()
                && contract_matches
                && transform_matches
                && training_input_matches
        }));
        // The high-feature row should resolve to a buy-Yes candidate.
        let bull = out
            .candidates
            .iter()
            .find(|c| c.outcome_side == OutcomeSide::Yes);
        assert!(bull.is_some(), "high feature ⇒ buy-Yes candidate");
        let bull = bull.expect("bull candidate");
        assert!(bull.expected_return_bps > Decimal::ZERO);
        assert_eq!(bull.downside_bps, dec!(10000));
        assert_eq!(bull.suggested_horizon_secs, 3_600);
        assert!(bull.liquidity_score.inner() > Decimal::ZERO);
        assert_eq!(bull.data_quality_score.inner(), Decimal::ONE);
        assert!(bull.rejection_warnings.iter().any(|warning| {
            matches!(warning, SignalWarning::Other(detail) if detail.starts_with("shadow_only:"))
        }));
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

    #[test]
    fn classical_artifact_freezes_the_typed_contract_and_rejects_drift() {
        let output = ClassicalAdapterRegistry::adapter_for(ClassicalKind::RandomForest)
            .train(&training_matrix())
            .expect("train");
        assert_eq!(
            output.input_contract.inputs,
            vec![
                ModelInputSpec::required("f0"),
                ModelInputSpec::required("f1")
            ]
        );
        assert_eq!(
            output.input_contract_hash,
            model_input_contract_hash(&output.input_contract).expect("typed contract hash")
        );

        let mut stale_hash = artifact(&output);
        stale_hash.input_contract.inputs[0].requiredness = ModelInputRequiredness::Optional;
        assert!(
            ClassicalRuntime::load(stale_hash, &output.model_bytes).is_err(),
            "requiredness drift without a new canonical hash must fail closed"
        );

        let mut swapped = artifact(&output);
        swapped.input_contract.inputs.swap(0, 1);
        swapped.input_contract_hash =
            model_input_contract_hash(&swapped.input_contract).expect("swapped contract hash");
        assert!(
            ClassicalRuntime::load(swapped, &output.model_bytes).is_err(),
            "an internally hashed contract still cannot diverge from the fitted transform"
        );

        let mut malformed = artifact(&output);
        malformed
            .input_contract
            .inputs
            .push(malformed.input_contract.inputs[0].clone());
        assert!(
            ClassicalRuntime::load(malformed, &output.model_bytes).is_err(),
            "duplicate raw inputs must fail closed"
        );

        let mut zero_horizon = artifact(&output);
        zero_horizon.prediction_horizon_secs = 0;
        assert!(
            ClassicalRuntime::load(zero_horizon, &output.model_bytes).is_err(),
            "zero classical horizon must fail closed"
        );

        let mut wrong_semantics = artifact(&output);
        wrong_semantics.output_semantics = ClassicalOutputSemantics::SettlementProbability;
        assert!(
            ClassicalRuntime::load(wrong_semantics, &output.model_bytes).is_err(),
            "regressor artifact cannot masquerade as a settlement probability"
        );
    }
}

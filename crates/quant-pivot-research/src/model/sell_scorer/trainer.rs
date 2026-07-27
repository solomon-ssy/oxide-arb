//! Header-free Sell scorer payload preparation.
//!
//! A Sell scorer has two distinct estimator inputs: a governed market
//! [`FactorHeadSpec`] and exactly four model-intrinsic position inputs. This
//! module deliberately does not collapse them into one pseudo-factor simplex.
//! It also does not calibrate business outputs on the same rows used to fit an
//! estimator. A future estimator refit must consume typed OOF predictions.

use std::sync::Arc;

use quant_pivot_error::{QuantResult, research::ResearchError};
use quant_pivot_models::{
    runtime_config::FactorCrossSectionConfig,
    types::{
        ContentHash, MarketId, ModelInputContract, TokenId, factor::FactorServingPlane,
        model_training::GovernedSellFitStatus,
    },
};
use rust_decimal::Decimal;
use serde::Serialize;

use crate::{
    hashing::ResearchHasher,
    model::{
        artifact::{
            SellEstimatorSpec, SellScorerOutputSpec, SellScorerPayload, model_input_contract_hash,
        },
        factor_heads::FactorHeadSpec,
        trainer::{
            CancellationProbe, LabelSelector, fit_frozen_reference_quantiles,
            weighted_training_input_hash,
        },
    },
    training::TrainingExample,
};

/// Metrics emitted while preparing a Sell family payload.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SellTrainingMetrics {
    pub resolved_label_rows: u64,
    pub position_state_rows: u64,
    pub fit_status: GovernedSellFitStatus,
}

/// Header-free Sell family training request.
#[derive(Debug, Clone)]
pub struct TrainSellScorerRequest {
    /// Cooperative cancellation observed at validation/hash boundaries.
    pub cancellation: CancellationProbe,
    /// Frozen point-in-time exit-decision examples.
    pub examples: Arc<[TrainingExample]>,
    /// Target label used to select exact estimator rows.
    pub label: LabelSelector,
    /// Complete governed factor plane.
    pub factor_plane: FactorServingPlane,
    /// Complete governed market head. No position intrinsic may appear here.
    pub factor_head: FactorHeadSpec,
    /// Content-addressed market-head/intrinsic composition.
    pub estimator: SellEstimatorSpec,
    /// Governed post-estimator exit policy.
    pub output_spec: SellScorerOutputSpec,
    /// Exact ordered raw-input contract frozen by the model spec.
    pub input_contract: ModelInputContract,
    /// Fitted small-cross-section transform policy.
    pub factor_cross_section: FactorCrossSectionConfig,
}

/// Verified header-free Sell training output.
#[derive(Debug, Clone)]
pub struct SellModelTrainingOutput {
    pub payload: SellScorerPayload,
    pub training_input_hash: ContentHash,
    pub input_contract_hash: ContentHash,
    pub input_transform_hash: ContentHash,
    pub metrics: SellTrainingMetrics,
}

/// Sell-side hold-vs-exit payload preparer.
#[derive(Debug, Clone, Default)]
pub struct SellScorerTrainer;

impl SellScorerTrainer {
    #[must_use]
    pub const fn new() -> Self {
        Self
    }

    /// Validate and freeze a self-consistent Sell family payload.
    ///
    /// This does not claim a fitted estimator. The returned typed status makes
    /// the OOF prerequisite explicit until the leakage-safe refit pipeline is
    /// available.
    pub fn train_sell_scorer(
        &self,
        request: &TrainSellScorerRequest,
    ) -> QuantResult<SellModelTrainingOutput> {
        request.cancellation.check("sell payload validation")?;
        request.factor_head.validate(&request.factor_plane)?;
        request.estimator.validate()?;

        let reference_factors = request
            .factor_plane
            .definitions()
            .iter()
            .filter(|revision| revision.definition().normalization.is_cross_sectional())
            .map(|revision| revision.factor_name().clone())
            .collect::<Vec<_>>();
        let frozen_reference_quantiles = fit_frozen_reference_quantiles(
            &request.examples,
            &request.label,
            &reference_factors,
            Some(&request.factor_cross_section),
        )?;
        request.cancellation.check("sell input commitment")?;
        let factor_inputs = request
            .factor_plane
            .definitions()
            .iter()
            .filter(|revision| !revision.definition().is_diagnostic())
            .map(|revision| revision.factor_name().clone())
            .collect::<Vec<_>>();
        let market_input_hash = weighted_training_input_hash(
            &request.examples,
            &request.label,
            &factor_inputs,
            &frozen_reference_quantiles,
            Some(&request.factor_cross_section),
        )?;
        let (training_input_hash, metrics) = sell_training_input_hash(request, market_input_hash)?;
        let payload = SellScorerPayload {
            factor_head: request.factor_head.clone(),
            estimator: request.estimator.clone(),
            output_spec: request.output_spec.clone(),
            input_contract: request.input_contract.clone(),
            factor_cross_section: request.factor_cross_section.clone(),
            frozen_reference_quantiles,
        };
        payload.validate_for_plane(&request.factor_plane)?;
        let input_contract_hash = model_input_contract_hash(&request.input_contract)?;
        let input_transform_hash = payload.input_transform_hash()?;
        payload.model_payload_hash()?;
        Ok(SellModelTrainingOutput {
            payload,
            training_input_hash,
            input_contract_hash,
            input_transform_hash,
            metrics,
        })
    }
}

#[derive(Serialize)]
struct SellTrainingCommitment {
    market_input_hash: ContentHash,
    rows: Vec<SellIntrinsicRow>,
}

#[derive(Serialize)]
struct SellIntrinsicRow {
    market_id: MarketId,
    token_id: TokenId,
    take_profit_pressure: Option<Decimal>,
    stop_loss_pressure: Option<Decimal>,
    time_in_trade: Decimal,
    peak_drawdown: Option<Decimal>,
}

fn sell_training_input_hash(
    request: &TrainSellScorerRequest,
    market_input_hash: ContentHash,
) -> QuantResult<(ContentHash, SellTrainingMetrics)> {
    let mut examples = request
        .examples
        .iter()
        .filter(|example| {
            example.labels.iter().any(|row| {
                (&row.label_name, row.horizon_secs)
                    == (&request.label.name, request.label.horizon_secs)
            })
        })
        .collect::<Vec<_>>();
    examples.sort_by(|left, right| {
        left.decision_at()
            .cmp(&right.decision_at())
            .then_with(|| left.market_id.as_str().cmp(right.market_id.as_str()))
            .then_with(|| left.token_id.as_str().cmp(right.token_id.as_str()))
    });
    let resolved_label_rows =
        u64::try_from(examples.len()).map_err(|error| ResearchError::DatasetBuild {
            detail: format!("sell resolved-row count conversion failed: {error}"),
        })?;
    let mut rows = Vec::with_capacity(examples.len());
    for example in examples {
        let state = example
            .position_state
            .as_ref()
            .ok_or_else(|| ResearchError::DatasetBuild {
                detail: format!(
                    "sell estimator row {}:{} has no position-state inputs",
                    example.market_id.as_str(),
                    example.token_id.as_str()
                ),
            })?;
        let evidence = state.direct_exit_evidence();
        let [take_profit, stop_loss, time_in_trade, peak_drawdown] = evidence.as_slice() else {
            return Err(ResearchError::Determinism {
                detail: "sell position-state projection did not emit exactly four inputs"
                    .to_owned(),
            }
            .into());
        };
        rows.push(SellIntrinsicRow {
            market_id: example.market_id.clone(),
            token_id: example.token_id.clone(),
            take_profit_pressure: take_profit.1,
            stop_loss_pressure: stop_loss.1,
            time_in_trade: time_in_trade.1.ok_or_else(|| ResearchError::DatasetBuild {
                detail: "sell time-in-trade input is unexpectedly missing".to_owned(),
            })?,
            peak_drawdown: peak_drawdown.1,
        });
    }
    let training_input_hash = ResearchHasher::canonical(&SellTrainingCommitment {
        market_input_hash,
        rows,
    })?;
    Ok((
        training_input_hash,
        SellTrainingMetrics {
            resolved_label_rows,
            position_state_rows: resolved_label_rows,
            fit_status: GovernedSellFitStatus::OofPredictionsRequired,
        },
    ))
}

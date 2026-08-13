//! Fold-scoped calibrated runtime used by nested CPCV estimators.

use async_trait::async_trait;
use quant_pivot_error::{QuantResult, research::ResearchError};
use quant_pivot_models::{
    enums::{common::MarketCategory, model::ModelFamily, quant::ModelWeightSource},
    runtime_config::FactorCrossSectionConfig,
    types::{ContentHash, ModelVersionId, factor::FactorServingPlane, stable_name::FeatureName},
};

use crate::{
    factors::FrozenReferenceQuantiles,
    model::{ModelRuntimeInput, ModelRuntimeOutput, QuantModelRuntime, ResolvedCalibration},
};

/// Decorates one fold-trained estimator with calibration fitted exclusively on
/// that outer fold's purge/embargo-isolated nested holdout.
pub struct CrossFittedRuntime {
    inner: Box<dyn QuantModelRuntime>,
    calibration: ResolvedCalibration,
}

impl CrossFittedRuntime {
    #[must_use]
    pub const fn new(inner: Box<dyn QuantModelRuntime>, calibration: ResolvedCalibration) -> Self {
        Self { inner, calibration }
    }

    #[must_use]
    pub const fn calibration(&self) -> &ResolvedCalibration {
        &self.calibration
    }
}

#[async_trait]
impl QuantModelRuntime for CrossFittedRuntime {
    fn model_version_id(&self) -> ModelVersionId {
        self.inner.model_version_id()
    }

    fn model_family(&self) -> ModelFamily {
        self.inner.model_family()
    }

    fn feature_schema_hash(&self) -> ContentHash {
        self.inner.feature_schema_hash()
    }

    fn required_features(&self) -> Vec<FeatureName> {
        self.inner.required_features()
    }

    fn input_features(&self) -> Vec<FeatureName> {
        self.inner.input_features()
    }

    fn category_scope(&self) -> Option<MarketCategory> {
        self.inner.category_scope()
    }

    fn weight_source(&self) -> ModelWeightSource {
        self.inner.weight_source()
    }

    fn factor_cross_section(&self) -> Option<&FactorCrossSectionConfig> {
        self.inner.factor_cross_section()
    }

    fn factor_serving_plane(&self) -> Option<&FactorServingPlane> {
        self.inner.factor_serving_plane()
    }

    fn frozen_reference_quantiles(&self) -> Option<&FrozenReferenceQuantiles> {
        self.inner.frozen_reference_quantiles()
    }

    async fn infer_batch(&self, input: ModelRuntimeInput) -> QuantResult<ModelRuntimeOutput> {
        let mut output = self.inner.infer_batch(input).await?;
        for candidate in &mut output.candidates {
            let estimate = self
                .calibration
                .estimate_return(candidate.composite_score.inner(), candidate.entry_price_ref)?;
            let payout_distribution = estimate.payout_distribution.ok_or_else(|| {
                ResearchError::Inference {
                    detail: format!(
                        "cross-fitted calibration emitted no payout distribution for candidate {}",
                        candidate.signal_candidate_id
                    ),
                }
            })?;
            candidate.expected_return_bps = estimate.expected_return_bps;
            candidate.downside_bps = estimate.downside_bps;
            candidate.payout_distribution = Some(payout_distribution);
        }
        Ok(output)
    }
}

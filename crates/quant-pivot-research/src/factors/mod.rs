//! Factor plane: the [`FactorComputer`] contract and its compute-domain value
//! types.
//!
//! Each computer turns a [`FeatureVector`](crate::features::FeatureVector) into
//! an explainable [`FactorValue`]. The registry, engine (single + batch for
//! cross-sectional normalization), and the nine generic factors land in 3.3;
//! 3.0 fixes the trait + value contract.

mod value;

pub use value::{
    DomainKind, FactorDefinitionSpec, FactorDriver, FactorExplanation, FactorFamily, FactorName,
    FactorOutputKind, FactorQualityGate, FactorSet, FactorValue, NormalizationSpec,
};

use quant_pivot_error::QuantResult;
use quant_pivot_models::types::FactorDefinitionId;

use crate::features::FeatureVector;

/// Computes one factor value from a feature vector.
///
/// `compute` is synchronous and side-effect free: factors are pure functions of
/// their inputs. Cross-sectional factors (e.g. [`NormalizationSpec::Rank`]) are
/// computed via the batch engine in 3.3, never faked per-market.
pub trait FactorComputer: Send + Sync {
    /// Governing factor-definition id.
    fn definition_id(&self) -> FactorDefinitionId;

    /// The governed specification this computer implements.
    fn spec(&self) -> &FactorDefinitionSpec;

    /// Compute the factor value for a single feature vector.
    fn compute(&self, features: &FeatureVector) -> QuantResult<FactorValue>;
}

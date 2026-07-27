//! Read-only factor-definition catalog HTTP contract.

use chrono::{DateTime, Utc};
use quant_pivot_macros::NormalizePageQuery;
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};

use crate::{
    domain::{pagination::PageRequest, quant::FactorDefinitionInfo},
    enums::factor::{FactorDefinitionScope, FactorFamily},
    types::factor::FactorOutputSemantics,
};

/// Outbound projection of an immutable factor definition.
///
/// The normalization method, output semantics, input features, and requiredness are
/// projected out of the governed typed definition so the catalog surfaces the
/// factor's contract without shipping the raw blob.
#[derive(Debug, Clone, Serialize)]
pub struct FactorDefinitionView {
    pub factor_definition_id: String,
    pub definition_hash: String,
    pub feature_contract_hash: String,
    pub name: String,
    pub factor_family: String,
    pub scope: String,
    pub input_schema_version: String,
    pub output_schema_version: String,
    /// Normalization method (`winsorized_zscore` / `rank` / `min_max`).
    pub normalization: String,
    /// Tagged outcome-alpha or side-neutral context semantics.
    pub output: FactorOutputSemantics,
    /// Stable feature names this factor consumes.
    pub input_features: Vec<String>,
    /// Whether missing/indeterminate output rejects the market.
    pub required: bool,
    pub created_at: DateTime<Utc>,
}

/// Which factor value plane the collinearity matrix is computed over.
///
/// `Raw` (the default) correlates the **pre-normalization** factor values — the
/// methodologically correct plane for detecting the audit #2 root cause (two
/// factors that are the same underlying signal), unbiased by mixing different
/// normalization methods. `Normalized` correlates the post-normalization scores,
/// offered as a secondary view of how the *scored* plane looks to the model.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum FactorCollinearitySource {
    /// Correlate raw (pre-normalization) factor values (default).
    #[default]
    Raw,
    /// Correlate normalized `[0, 1]` scores.
    Normalized,
}

/// One collinear factor pair in the analysis report.
#[derive(Debug, Clone, Serialize)]
pub struct CollinearPairView {
    pub left: String,
    pub right: String,
    pub correlation: Decimal,
}

/// Factor-collinearity analysis over a recent window of factor values.
#[derive(Debug, Clone, Serialize)]
pub struct FactorCollinearityView {
    /// Factor names, index-aligned with `matrix` rows/columns.
    pub factors: Vec<String>,
    /// Symmetric Spearman rank-correlation matrix (`matrix[i][j]`), diagonal `1`.
    pub matrix: Vec<Vec<Decimal>>,
    /// Pairs whose `|ρ|` exceeds the tolerance.
    pub violations: Vec<CollinearPairView>,
    /// The absolute-correlation tolerance applied.
    pub threshold: Decimal,
    /// Number of joint observations the correlations were computed over.
    pub observation_count: usize,
    /// The lookback window (seconds) the sample was drawn from.
    pub lookback_secs: u64,
    /// Which value plane (raw / normalized) the matrix was computed over.
    pub panel_source: FactorCollinearitySource,
}

/// Query for `GET /research/factors/collinearity`.
#[derive(Debug, Clone, Deserialize)]
pub struct FactorCollinearityQuery {
    /// Rolling lookback in seconds (defaults to 7 days when omitted).
    pub lookback_secs: Option<u64>,
    /// Absolute-correlation tolerance (defaults to the runtime
    /// `factors.orthogonalize.max_correlation` when omitted).
    pub threshold: Option<String>,
    /// Which value plane to correlate (defaults to `raw`).
    pub source: Option<FactorCollinearitySource>,
}

/// Paginated filter for the factor-definition catalog.
///
/// `factor_family` and `scope` slice the taxonomy. The pagination window is the
/// shared [`PageRequest`].
#[derive(Debug, Clone, Default, Deserialize, NormalizePageQuery)]
pub struct FactorDefinitionListQuery {
    pub factor_family: Option<FactorFamily>,
    pub scope: Option<FactorDefinitionScope>,
    #[normalize_page]
    #[serde(flatten)]
    pub page: PageRequest,
}

impl From<FactorDefinitionInfo> for FactorDefinitionView {
    fn from(info: FactorDefinitionInfo) -> Self {
        let definition = info.definition;
        let normalization = definition.normalization.to_string();
        let output = definition.output;
        let input_features = definition
            .input_features
            .into_iter()
            .map(|feature| feature.to_string())
            .collect();
        let required = definition.required;
        Self {
            factor_definition_id: info.factor_definition_id.to_string(),
            definition_hash: info.definition_hash.to_string(),
            feature_contract_hash: info.feature_contract_hash.to_string(),
            name: info.name,
            factor_family: info.factor_family.to_string(),
            scope: info.scope.to_string(),
            input_schema_version: info.input_schema_version.to_string(),
            output_schema_version: info.output_schema_version.to_string(),
            required,
            normalization,
            output,
            input_features,
            created_at: info.created_at,
        }
    }
}

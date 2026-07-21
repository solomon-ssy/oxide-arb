//! Factor-definition governance HTTP contract.
//!
//! | Method | Path | Permission | Purpose |
//! |--------|------|------------|---------|
//! | POST | `/research/factors/register` | `factor_definition:create` | Register enabled definitions as draft |
//! | POST | `/research/factors/publish-batch` | `factor_definition:publish` | Publish a batch of definitions |
//! | POST | `/research/factors/{id}/publish` | `factor_definition:publish` | Promote draft/retired definition |
//! | POST | `/research/factors/{id}/retire` | `factor_definition:retire` | Retire a published definition |

use chrono::{DateTime, Utc};
use quant_pivot_macros::NormalizePageQuery;
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};
use validator::Validate;

use crate::{
    domain::{pagination::PageRequest, quant::FactorDefinitionInfo},
    enums::{
        factor::{FactorDefinitionScope, FactorFamily},
        quant::PublicationStatus,
    },
    types::FactorDefinitionId,
};

/// Inbound body for `POST /research/factors/{id}/publish`.
#[derive(Debug, Clone, Deserialize, Validate)]
pub struct PublishFactorRequest {
    /// Operator reason recorded on the HTTP operation log.
    #[validate(length(min = 1, max = 1024))]
    pub reason: String,
}

/// Inbound body for `POST /research/factors/{id}/retire`.
#[derive(Debug, Clone, Deserialize, Validate)]
pub struct RetireFactorRequest {
    /// Operator reason recorded on the HTTP operation log.
    #[validate(length(min = 1, max = 1024))]
    pub reason: String,
}

/// Inbound body for `POST /research/factors/register`.
///
/// The enabled factor set is resolved server-side from the active runtime
/// config; the operator only supplies the audit reason.
#[derive(Debug, Clone, Deserialize, Validate)]
pub struct RegisterFactorDefinitionsRequest {
    /// Operator reason recorded on the HTTP operation log.
    #[validate(length(min = 1, max = 1024))]
    pub reason: String,
}

/// Inbound body for `POST /research/factors/publish-batch`.
#[derive(Debug, Clone, Deserialize, Validate)]
pub struct PublishFactorsBatchRequest {
    /// Factor definitions to publish (already-published ids are a no-op).
    #[validate(length(min = 1))]
    pub factor_definition_ids: Vec<FactorDefinitionId>,
    /// Operator reason recorded on the HTTP operation log.
    #[validate(length(min = 1, max = 1024))]
    pub reason: String,
}

/// Outbound projection of a governed factor definition.
///
/// The normalization method, direction, input features, and quality gates are
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
    pub status: String,
    /// Normalization method (`winsorized_zscore` / `rank` / `min_max`).
    pub normalization: String,
    /// Default contribution direction (`positive` / `negative` / `neutral`).
    pub direction: String,
    /// Stable feature names this factor consumes.
    pub input_features: Vec<String>,
    /// Whether the factor is required (declares at least one quality gate).
    pub required: bool,
    /// Names of the quality gates governing this factor.
    pub quality_gates: Vec<String>,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
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

/// Paginated filter for the factor-definition governance catalog.
///
/// `factor_family` / `scope` slice the taxonomy; `status` narrows the
/// publication lifecycle. The pagination window is the shared [`PageRequest`].
#[derive(Debug, Clone, Default, Deserialize, NormalizePageQuery)]
pub struct FactorDefinitionListQuery {
    pub factor_family: Option<FactorFamily>,
    pub scope: Option<FactorDefinitionScope>,
    pub status: Option<PublicationStatus>,
    #[normalize_page]
    #[serde(flatten)]
    pub page: PageRequest,
}

impl From<FactorDefinitionInfo> for FactorDefinitionView {
    fn from(info: FactorDefinitionInfo) -> Self {
        let definition = info.definition;
        let normalization = definition.normalization.as_str().to_owned();
        let direction = definition.default_direction.as_str().to_owned();
        let input_features = definition
            .input_features
            .into_iter()
            .map(|feature| feature.as_str().to_owned())
            .collect();
        let quality_gates: Vec<_> = definition
            .quality_gates
            .into_iter()
            .map(|gate| gate.name)
            .collect();
        Self {
            factor_definition_id: info.factor_definition_id.to_string(),
            definition_hash: info.definition_hash.to_string(),
            feature_contract_hash: info.feature_contract_hash.to_string(),
            name: info.name,
            factor_family: info.factor_family.as_str().to_owned(),
            scope: info.scope.as_str().to_owned(),
            input_schema_version: info.input_schema_version.to_string(),
            output_schema_version: info.output_schema_version.to_string(),
            status: info.status.as_str().to_owned(),
            required: !quality_gates.is_empty(),
            normalization,
            direction,
            input_features,
            quality_gates,
            created_at: info.created_at,
            updated_at: info.updated_at,
        }
    }
}

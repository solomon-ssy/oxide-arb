//! Quant model registry HTTP contract types.

use crate::{
    domain::{ModelSpecInfo, pagination::PageRequest},
    enums::quant::PublicationStatus,
};
use quant_pivot_macros::NormalizePageQuery;
use serde::{Deserialize, Serialize};

/// Outbound projection for a model specification row (the training entry point:
/// the operator picks a spec before planning a dataset or training a version).
#[derive(Debug, Clone, Serialize)]
pub struct QuantModelSpecView {
    pub model_spec_id: String,
    pub name: String,
    pub model_family: String,
    pub prediction_horizon_secs: i64,
    pub status: String,
    pub created_at: chrono::DateTime<chrono::Utc>,
    pub updated_at: chrono::DateTime<chrono::Utc>,
}

impl From<ModelSpecInfo> for QuantModelSpecView {
    fn from(info: ModelSpecInfo) -> Self {
        Self {
            model_spec_id: info.model_spec_id.to_string(),
            name: info.name,
            model_family: info.model_family.as_str().to_owned(),
            prediction_horizon_secs: info.prediction_horizon_secs,
            status: info.status.as_str().to_owned(),
            created_at: info.created_at,
            updated_at: info.updated_at,
        }
    }
}

/// Paginated filter for the model-spec catalog (training/dataset selector source).
#[derive(Debug, Clone, Default, Deserialize, NormalizePageQuery)]
pub struct ModelSpecListQuery {
    /// Narrow by publication lifecycle (`draft`/`published`/`retired`/…).
    pub status: Option<PublicationStatus>,
    #[normalize_page]
    #[serde(flatten)]
    pub page: PageRequest,
}

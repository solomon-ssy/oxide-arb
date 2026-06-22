//! Quant model registry HTTP contract types.

use crate::domain::{ModelSpecInfo, ModelVersionInfo};
use serde::Serialize;

/// Outbound projection for a model specification row.
#[derive(Debug, Clone, Serialize)]
pub struct QuantModelSpecView {
    pub model_spec_id: String,
    pub name: String,
    pub model_family: String,
    pub status: String,
}

impl From<ModelSpecInfo> for QuantModelSpecView {
    fn from(info: ModelSpecInfo) -> Self {
        Self {
            model_spec_id: info.model_spec_id.to_string(),
            name: info.name,
            model_family: info.model_family,
            status: info.status.as_str().to_owned(),
        }
    }
}

/// Outbound projection for a model version row.
#[derive(Debug, Clone, Serialize)]
pub struct QuantModelVersionView {
    pub model_version_id: String,
    pub model_spec_id: String,
    pub version: i32,
    pub publication_status: String,
}

impl From<ModelVersionInfo> for QuantModelVersionView {
    fn from(info: ModelVersionInfo) -> Self {
        Self {
            model_version_id: info.model_version_id.to_string(),
            model_spec_id: info.model_spec_id.to_string(),
            version: info.version,
            publication_status: info.publication_status.as_str().to_owned(),
        }
    }
}

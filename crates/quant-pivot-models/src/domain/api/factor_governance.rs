//! Factor-definition governance HTTP contract (Phase 05.7).
//!
//! | Method | Path | Permission | Purpose |
//! |--------|------|------------|---------|
//! | POST | `/research/factors/{id}/publish` | `factor_definition:publish` | Promote draft/retired definition |
//! | POST | `/research/factors/{id}/retire` | `factor_definition:retire` | Retire a published definition |

use chrono::{DateTime, Utc};
use quant_pivot_macros::NormalizePageQuery;
use serde::{Deserialize, Serialize};
use validator::Validate;

use crate::{
    domain::{FactorDefinitionInfo, pagination::PageRequest},
    enums::{
        factor::{FactorDefinitionScope, FactorFamily},
        quant::PublicationStatus,
    },
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

/// Outbound projection of a governed factor definition.
#[derive(Debug, Clone, Serialize)]
pub struct FactorDefinitionView {
    pub factor_definition_id: String,
    pub name: String,
    pub factor_family: String,
    pub scope: String,
    pub input_schema_version: String,
    pub output_schema_version: String,
    pub status: String,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
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
        Self {
            factor_definition_id: info.factor_definition_id.to_string(),
            name: info.name,
            factor_family: info.factor_family.as_str().to_owned(),
            scope: info.scope.as_str().to_owned(),
            input_schema_version: info.input_schema_version.to_string(),
            output_schema_version: info.output_schema_version.to_string(),
            status: info.status.as_str().to_owned(),
            created_at: info.created_at,
            updated_at: info.updated_at,
        }
    }
}

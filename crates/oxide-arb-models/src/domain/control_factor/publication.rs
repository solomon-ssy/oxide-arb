//! Publication pointer and in-memory snapshot types.

use super::value::ControlFactorValue;
use crate::{
    enums::control_factor::{PublicationMode, PublicationStatus},
    types::{ControlFactorId, FactorPublicationId},
};
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

/// Publication pointer consumed by live refreshers. Live behavior follows the active row.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ControlFactorPublication {
    pub publication_id: FactorPublicationId,
    pub mode: PublicationMode,
    pub factor_ids: Vec<ControlFactorId>,
    pub previous_publication_id: Option<FactorPublicationId>,
    pub status: PublicationStatus,
    pub effective_from: DateTime<Utc>,
    pub expires_at: DateTime<Utc>,
    pub approved_by: Option<String>,
    pub approval_reason: String,
    pub publication_hash: String,
}

/// Immutable in-memory control-factor snapshot.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ControlFactorSnapshot {
    pub publication: ControlFactorPublication,
    pub factors: Vec<ControlFactorValue>,
    pub loaded_at: DateTime<Utc>,
    pub schema_version: u32,
}

impl ControlFactorSnapshot {
    #[must_use]
    pub fn is_expired_at(&self, now: DateTime<Utc>) -> bool {
        self.publication.expires_at <= now
            || self.factors.iter().any(|factor| factor.expires_at <= now)
    }
}

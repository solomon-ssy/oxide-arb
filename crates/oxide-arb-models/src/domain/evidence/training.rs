use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

use crate::{
    domain::{
        control_factor::{FactorDimensions, StageArtifactRef},
        evidence::EvidenceSourceRef,
    },
    enums::control_factor::ControlFactorType,
    types::{MarketId, OpportunityId},
};

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct FactorFeatureVector {
    pub schema_version: u32,
    pub entries: Vec<FactorFeature>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct FactorLabel {
    pub schema_version: u32,
    pub entries: Vec<FactorLabelRef>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct EvidenceSourceRefs {
    pub query_refs: Vec<EvidenceSourceRef>,
    pub artifact_refs: Vec<StageArtifactRef>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct FactorFeatureRef {
    pub name: String,
    pub source_ref: EvidenceSourceRef,
    pub point_in_time_visible: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct FactorFeature {
    pub name: String,
    pub value: FactorFeatureValue,
    pub source_ref: EvidenceSourceRef,
    pub observed_at: DateTime<Utc>,
    pub point_in_time_visible: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", tag = "kind", content = "value")]
pub enum FactorFeatureValue {
    Bool(bool),
    I64(i64),
    U64(u64),
    Decimal(String),
    Text(String),
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct FactorLabelRef {
    pub name: String,
    pub source_ref: EvidenceSourceRef,
    pub available_at: DateTime<Utc>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct FactorTrainingExample {
    pub opportunity_id: OpportunityId,
    pub market_id: MarketId,
    pub factor_type: ControlFactorType,
    pub entity_key: FactorDimensions,
    pub event_time: DateTime<Utc>,
    pub features: FactorFeatureVector,
    pub label: Option<FactorLabel>,
    pub outcome_available_at: Option<DateTime<Utc>>,
    pub source_refs: EvidenceSourceRefs,
}

//! Durable report-fact outbox persistence contracts.

use chrono::{DateTime, Utc};
use sea_orm::{DeriveIntoActiveModel, DerivePartialModel};
use serde::{Deserialize, Serialize};

use crate::{
    entities::quant_report_fact_delivery,
    enums::quant::ReportFactDeliveryStatus,
    types::{ArtifactUri, ContentHash, RecommendationReportId, WorkerId},
};

#[derive(Debug, Clone, Serialize, Deserialize, DerivePartialModel)]
#[sea_orm(entity = "crate::entities::quant_report_fact_delivery::Entity")]
pub struct ReportFactDeliveryInfo {
    pub recommendation_report_id: RecommendationReportId,
    pub status: ReportFactDeliveryStatus,
    pub bundle_uri: ArtifactUri,
    pub bundle_hash: ContentHash,
    pub bundle_bytes: i64,
    pub recommendation_row_count: i64,
    pub recommendation_row_chain_hash: ContentHash,
    pub funnel_row_count: i64,
    pub funnel_row_chain_hash: ContentHash,
    pub attempt_count: i32,
    pub claim_owner: Option<WorkerId>,
    pub lease_expires_at: Option<DateTime<Utc>>,
    pub next_attempt_at: Option<DateTime<Utc>>,
    pub last_error: Option<String>,
    pub verified_at: Option<DateTime<Utc>>,
    pub announced_at: Option<DateTime<Utc>>,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

info_from_model!(
    ReportFactDeliveryInfo,
    quant_report_fact_delivery::Model,
    {
        recommendation_report_id,
        status,
        bundle_uri,
        bundle_hash,
        bundle_bytes,
        recommendation_row_count,
        recommendation_row_chain_hash,
        funnel_row_count,
        funnel_row_chain_hash,
        attempt_count,
        claim_owner,
        lease_expires_at,
        next_attempt_at,
        last_error,
        verified_at,
        announced_at,
        created_at,
        updated_at,
    }
);

#[derive(Debug, Clone, Serialize, Deserialize, DeriveIntoActiveModel)]
#[sea_orm(active_model = "crate::entities::quant_report_fact_delivery::ActiveModel")]
pub struct NewReportFactDelivery {
    pub recommendation_report_id: RecommendationReportId,
    pub status: ReportFactDeliveryStatus,
    pub bundle_uri: ArtifactUri,
    pub bundle_hash: ContentHash,
    pub bundle_bytes: i64,
    pub recommendation_row_count: i64,
    pub recommendation_row_chain_hash: ContentHash,
    pub funnel_row_count: i64,
    pub funnel_row_chain_hash: ContentHash,
}

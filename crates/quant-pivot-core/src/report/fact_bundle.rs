//! Immutable report fact-bundle preparation.

use std::sync::Arc;

use quant_pivot_error::{QuantResult, report::ReportError};
use quant_pivot_models::{
    clickhouse::{QuantReportRecommendationFactRow, ReportMarketFunnelRow},
    domain::quant::NewReportFactDelivery,
    enums::quant::ReportFactDeliveryStatus,
    hashing::CanonicalDigest,
    types::{
        REPORT_FACT_BUNDLE_FORMAT_VERSION, RecommendationReportId, ReportFactBundleV1,
        ReportFactNotificationRecommendationV1, ReportFactNotificationV1,
        ReportFactTableCommitment,
    },
};
use quant_pivot_research::artifact::{ArtifactKey, ArtifactNamespace, ArtifactStore};

use super::types::ComposedReport;

const RECOMMENDATION_TABLE: &str = "quant_report_recommendation_fact";
const FUNNEL_TABLE: &str = "quant_report_market_funnel";

/// Serialize, persist, and read-after-write verify one immutable report bundle.
pub async fn prepare_report_fact_bundle(
    artifacts: &Arc<dyn ArtifactStore>,
    composed: &mut ComposedReport,
) -> QuantResult<()> {
    let report_id = composed.transaction.report.recommendation_report_id;
    let mut recommendation_rows = composed.ch_rows.clone();
    recommendation_rows.sort_by(|left, right| {
        left.rank.cmp(&right.rank).then_with(|| {
            left.recommendation_id
                .as_uuid()
                .cmp(&right.recommendation_id.as_uuid())
        })
    });
    let mut funnel_rows = composed.funnel_rows.clone();
    funnel_rows.sort_by(|left, right| left.market_id.cmp(&right.market_id));

    ensure_report_binding(&report_id, &recommendation_rows, &funnel_rows)?;
    let recommendation_hash = CanonicalDigest::content_hash_json(&recommendation_rows)?;
    let funnel_hash = CanonicalDigest::content_hash_json(&funnel_rows)?;
    let bundle = ReportFactBundleV1 {
        format_version: REPORT_FACT_BUNDLE_FORMAT_VERSION,
        recommendation_report_id: report_id,
        created_at: composed.transaction.report.decision_at,
        delivery_policy: composed.delivery_policy,
        notify_operators: composed.notify_operators,
        notification: ReportFactNotificationV1 {
            kind: composed.notification.kind,
            status: composed.notification.status.clone(),
            runtime_mode: composed.notification.runtime_mode,
            published_count: composed.notification.published_count,
            total_suggested_usd: composed.notification.total_suggested_usd,
            top3: composed
                .notification
                .top3
                .iter()
                .map(|recommendation| ReportFactNotificationRecommendationV1 {
                    market_id: recommendation.market_id.clone(),
                    outcome_side: recommendation.outcome_side,
                    score: recommendation.score,
                    suggested_usd: recommendation.suggested_usd,
                })
                .collect(),
            warnings: composed.notification.warnings.clone(),
            empty_reason: composed.notification.empty_reason,
        },
        recommendation_commitment: ReportFactTableCommitment {
            table: RECOMMENDATION_TABLE.to_owned(),
            row_count: usize_to_u64("recommendation_row_count", recommendation_rows.len())?,
            row_chain_hash: recommendation_hash,
        },
        funnel_commitment: ReportFactTableCommitment {
            table: FUNNEL_TABLE.to_owned(),
            row_count: usize_to_u64("funnel_row_count", funnel_rows.len())?,
            row_chain_hash: funnel_hash,
        },
        recommendation_rows,
        funnel_rows,
    };
    let bytes = serde_json::to_vec(&bundle).map_err(|error| ReportError::InvariantViolation {
        stage: "report_fact_bundle",
        detail: format!("bundle serialization failed: {error}"),
    })?;
    let bundle_hash = CanonicalDigest::content_hash_bytes(&bytes);
    let bundle_uri = artifacts
        .put(
            ArtifactKey::new(ArtifactNamespace::ReportFacts, bundle_hash.hex(), "json")?,
            &bytes,
        )
        .await?;
    let metadata = artifacts.metadata(&bundle_uri).await?;
    let expected_bytes = usize_to_i64("bundle_bytes", bytes.len())?;
    if metadata.byte_size
        != u64::try_from(bytes.len()).map_err(|error| ReportError::NumericOverflow {
            field: "report_fact_bundle.bundle_bytes",
            detail: error.to_string(),
        })?
    {
        return Err(ReportError::InvariantViolation {
            stage: "report_fact_bundle",
            detail: format!("persisted bundle {bundle_uri} has an unexpected size"),
        }
        .into());
    }
    let persisted = artifacts.get(&bundle_uri).await?;
    let persisted_hash = CanonicalDigest::content_hash_bytes(&persisted);
    if persisted_hash != bundle_hash {
        return Err(ReportError::InvariantViolation {
            stage: "report_fact_bundle",
            detail: format!(
                "persisted bundle hash mismatch: expected {bundle_hash}, found {persisted_hash}"
            ),
        }
        .into());
    }

    composed.ch_rows = bundle.recommendation_rows;
    composed.funnel_rows = bundle.funnel_rows;
    composed.transaction.fact_delivery = Some(NewReportFactDelivery {
        recommendation_report_id: report_id,
        status: ReportFactDeliveryStatus::Pending,
        bundle_uri,
        bundle_hash,
        bundle_bytes: expected_bytes,
        recommendation_row_count: usize_to_i64("recommendation_row_count", composed.ch_rows.len())?,
        recommendation_row_chain_hash: recommendation_hash,
        funnel_row_count: usize_to_i64("funnel_row_count", composed.funnel_rows.len())?,
        funnel_row_chain_hash: funnel_hash,
    });
    Ok(())
}

fn ensure_report_binding(
    report_id: &RecommendationReportId,
    recommendations: &[QuantReportRecommendationFactRow],
    funnel: &[ReportMarketFunnelRow],
) -> QuantResult<()> {
    if recommendations
        .iter()
        .any(|row| &row.recommendation_report_id != report_id)
        || funnel
            .iter()
            .any(|row| &row.recommendation_report_id != report_id)
    {
        return Err(ReportError::InvariantViolation {
            stage: "report_fact_bundle",
            detail: "every fact row must bind the exact report id".to_owned(),
        }
        .into());
    }
    Ok(())
}

fn usize_to_u64(field: &'static str, value: usize) -> QuantResult<u64> {
    u64::try_from(value).map_err(|error| {
        ReportError::NumericOverflow {
            field,
            detail: error.to_string(),
        }
        .into()
    })
}

fn usize_to_i64(field: &'static str, value: usize) -> QuantResult<i64> {
    i64::try_from(value).map_err(|error| {
        ReportError::NumericOverflow {
            field,
            detail: error.to_string(),
        }
        .into()
    })
}

//! Recommendation + evidence + attribution HTTP endpoints (Phase 04.4 / 05.7).
//!
//! # UI integration contract
//!
//! | Method | Path | Permission | Purpose |
//! |--------|------|------------|---------|
//! | GET  | `/quant/recommendations/{id}` | `quant_report:read` | One recommendation |
//! | GET  | `/quant/recommendations/{id}/evidence` | `quant_report:read` | Replay handles |
//! | GET  | `/quant/recommendations/{id}/entry-condition` | `quant_report:read` | Durable condition state and tree |
//! | GET  | `/quant/recommendations/{id}/entry-condition/audits` | `quant_report:read` | WORM condition timeline |
//! | GET  | `/quant/recommendations/{id}/attribution` | `recommendation_attribution:read` | Final WORM attribution |
//!
//! Creating an order intent from a recommendation is `POST /api/quant/intents`
//! (see [`super::quant_intents`]), the governed execution surface added in
//! Phase 05.2.

use actix_web::{http::Method, web};
use chrono::{DateTime, Utc};
use quant_pivot_models::{
    clickhouse::EntryConditionEvaluationEventRow,
    domain::{
        EntryConditionArtifactView, EntryConditionAuditView, EntryConditionDetailView,
        EntryConditionEvaluationView, EntryConditionInstanceSummaryView,
        EntryConditionLeafEvidenceView, QuantEvidenceView, QuantRecommendationView,
        RecommendationAttributionView,
    },
    enums::rbac::{Operation, ResourceType},
    types::{ConditionUnavailableReason, RecommendationId},
};

use crate::{
    auth::casbin::Rule,
    error::WebError,
    response::WebResponse,
    routes::registry::{RouteSpec, spec},
    state::AppState,
};

/// Recommendation + evidence routes.
pub(crate) fn route_specs() -> Vec<RouteSpec> {
    vec![
        spec(
            Method::GET,
            "/quant/recommendations/{id}",
            Rule::ResourceOp(ResourceType::QuantReport, Operation::Read),
            get,
        ),
        spec(
            Method::GET,
            "/quant/recommendations/{id}/evidence",
            Rule::ResourceOp(ResourceType::QuantReport, Operation::Read),
            evidence,
        ),
        spec(
            Method::GET,
            "/quant/recommendations/{id}/attribution",
            Rule::ResourceOp(ResourceType::RecommendationAttribution, Operation::Read),
            attribution,
        ),
        spec(
            Method::GET,
            "/quant/recommendations/{id}/entry-condition",
            Rule::ResourceOp(ResourceType::QuantReport, Operation::Read),
            entry_condition,
        ),
        spec(
            Method::GET,
            "/quant/recommendations/{id}/entry-condition/audits",
            Rule::ResourceOp(ResourceType::QuantReport, Operation::Read),
            entry_condition_audits,
        ),
    ]
}

/// Durable recommendation-owned condition state and immutable artifact.
pub async fn entry_condition(
    state: web::Data<AppState>,
    id: web::Path<RecommendationId>,
) -> Result<WebResponse<EntryConditionDetailView>, WebError> {
    let instance = state
        .entry_conditions
        .find_by_recommendation(&id)
        .await?
        .ok_or_else(|| WebError::NotFound(format!("entry condition not found: {id}")))?;
    let artifact = match instance.artifact_id.as_ref() {
        Some(artifact_id) => {
            let info = state
                .entry_conditions
                .find_artifact(artifact_id)
                .await?
                .ok_or_else(|| {
                    WebError::Internal(format!(
                        "condition instance {} references missing artifact {artifact_id}",
                        instance.condition_instance_id
                    ))
                })?;
            if instance.artifact_hash.as_ref() != Some(&info.content_hash) {
                return Err(WebError::Internal(format!(
                    "condition instance {} artifact hash mismatch",
                    instance.condition_instance_id
                )));
            }
            let nodes = info
                .payload_json
                .root
                .preorder_nodes()
                .map_err(|error| WebError::Internal(error.to_string()))?;
            Some(EntryConditionArtifactView::from_info(info, nodes))
        }
        None => None,
    };
    let latest_authoritative_evaluation = state
        .quant_facts
        .latest_applied_entry_condition_evaluation(&instance.condition_instance_id)
        .await?
        .map(evaluation_view)
        .transpose()?;
    Ok(WebResponse::ok(EntryConditionDetailView {
        instance: EntryConditionInstanceSummaryView::from(instance),
        artifact,
        latest_authoritative_evaluation,
    }))
}

fn evaluation_view(
    row: EntryConditionEvaluationEventRow,
) -> Result<EntryConditionEvaluationView, WebError> {
    let applied_revision = row.applied_revision.ok_or_else(|| {
        WebError::Internal("applied condition trace has no applied_revision".to_owned())
    })?;
    let evaluated_at = DateTime::from_timestamp_millis(row.evaluated_at).ok_or_else(|| {
        WebError::Internal("condition trace evaluated_at is outside chrono range".to_owned())
    })?;
    let tree = serde_json::from_str::<serde_json::Value>(&row.tree_json).map_err(|error| {
        WebError::Internal(format!("condition trace tree is invalid JSON: {error}"))
    })?;
    let mut leaf_evidence = Vec::new();
    collect_leaf_evidence(&tree, evaluated_at, &mut leaf_evidence)?;
    Ok(EntryConditionEvaluationView {
        evaluation_id: row.evaluation_id,
        applied_revision,
        evaluator_version: row.evaluator_version,
        evaluated_at,
        state: row.state,
        truth: row.truth,
        evaluation_hash: row.evaluation_hash,
        input_fingerprint: row.input_fingerprint,
        continuity_hash: row.continuity_hash,
        tree,
        leaf_evidence,
    })
}

fn collect_leaf_evidence(
    node: &serde_json::Value,
    evaluated_at: DateTime<Utc>,
    output: &mut Vec<EntryConditionLeafEvidenceView>,
) -> Result<(), WebError> {
    if let (Some(node_id), Some(truth), Some(evidence)) = (
        node.get("node_id").and_then(serde_json::Value::as_u64),
        node.get("truth"),
        node.get("evidence").filter(|value| !value.is_null()),
    ) {
        let node_id = u16::try_from(node_id)
            .map_err(|_| WebError::Internal("condition trace node_id overflow".to_owned()))?;
        let (observed_at, available_at, source_checkpoint) = evidence_metadata(evidence)?;
        let freshness_ms = observed_at.map(|observed_at| {
            evaluated_at
                .signed_duration_since(observed_at)
                .num_milliseconds()
        });
        let unavailable_reason = truth
            .get("reason")
            .cloned()
            .map(serde_json::from_value::<ConditionUnavailableReason>)
            .transpose()
            .map_err(|error| {
                WebError::Internal(format!("condition unavailable reason is invalid: {error}"))
            })?;
        output.push(EntryConditionLeafEvidenceView {
            node_id,
            truth: truth.clone(),
            evidence: evidence.clone(),
            observed_at,
            available_at,
            freshness_ms,
            source_checkpoint,
            unavailable_reason,
        });
    }
    if let Some(children) = node.get("children").and_then(serde_json::Value::as_array) {
        for child in children {
            collect_leaf_evidence(child, evaluated_at, output)?;
        }
    }
    Ok(())
}

type EvidenceMetadata = (
    Option<DateTime<Utc>>,
    Option<DateTime<Utc>>,
    Option<serde_json::Value>,
);

fn evidence_metadata(evidence: &serde_json::Value) -> Result<EvidenceMetadata, WebError> {
    let kind = evidence
        .get("kind")
        .and_then(serde_json::Value::as_str)
        .unwrap_or_default();
    let (observed, available, checkpoint) = match kind {
        "price" => (
            evidence.get("observed_at"),
            evidence.get("available_at"),
            Some(serde_json::json!({
                "token_id": evidence.get("token_id"),
                "observed_at": evidence.get("observed_at"),
                "available_at": evidence.get("available_at"),
                "gap_generation": evidence.get("gap_generation"),
            })),
        ),
        "factor" => (
            evidence.get("observed_at"),
            evidence.get("available_at"),
            Some(serde_json::json!({
                "definition_hash": evidence.get("definition_hash"),
                "model_version_id": evidence.get("model_version_id"),
                "snapshot_hash": evidence.get("snapshot_hash"),
            })),
        ),
        "weather" => (
            evidence.get("observation_time"),
            evidence.get("available_at"),
            Some(serde_json::json!({
                "revision": evidence.get("revision"),
                "report_hash": evidence.get("report_hash"),
                "gap_generation": evidence.get("gap_generation"),
            })),
        ),
        "crypto" => {
            let input = evidence.get("input");
            let latest = input
                .and_then(|value| value.get("reports"))
                .and_then(serde_json::Value::as_array)
                .and_then(|reports| reports.last());
            (
                latest.and_then(|value| value.get("event_at")),
                latest.and_then(|value| value.get("available_at")),
                Some(serde_json::json!({
                    "source_sequence": latest.and_then(|value| value.get("source_sequence")),
                    "report_hash": latest.and_then(|value| value.get("report_hash")),
                    "gap_generation": input.and_then(|value| value.get("gap_generation")),
                    "discontinuity_epoch": evidence.pointer("/state/discontinuity_epoch"),
                    "latched": evidence.pointer("/state/latched"),
                })),
            )
        }
        _ => (None, None, None),
    };
    Ok((
        parse_optional_datetime(observed)?,
        parse_optional_datetime(available)?,
        checkpoint,
    ))
}

fn parse_optional_datetime(
    value: Option<&serde_json::Value>,
) -> Result<Option<DateTime<Utc>>, WebError> {
    value
        .and_then(serde_json::Value::as_str)
        .map(|value| {
            value.parse::<DateTime<Utc>>().map_err(|error| {
                WebError::Internal(format!("condition evidence timestamp is invalid: {error}"))
            })
        })
        .transpose()
}

/// WORM condition lifecycle timeline ordered by revision.
pub async fn entry_condition_audits(
    state: web::Data<AppState>,
    id: web::Path<RecommendationId>,
) -> Result<WebResponse<Vec<EntryConditionAuditView>>, WebError> {
    let instance = state
        .entry_conditions
        .find_by_recommendation(&id)
        .await?
        .ok_or_else(|| WebError::NotFound(format!("entry condition not found: {id}")))?;
    let audits = state
        .entry_conditions
        .audits(&instance.condition_instance_id)
        .await?
        .into_iter()
        .map(EntryConditionAuditView::from)
        .collect();
    Ok(WebResponse::ok(audits))
}

/// `GET /api/quant/recommendations/{id}` — one recommendation.
pub async fn get(
    state: web::Data<AppState>,
    id: web::Path<RecommendationId>,
) -> Result<WebResponse<QuantRecommendationView>, WebError> {
    let view = state
        .quant_reports
        .find_recommendation(&id)
        .await?
        .ok_or_else(|| WebError::NotFound(format!("recommendation not found: {id}")))?;
    Ok(WebResponse::ok(view))
}

/// `GET /api/quant/recommendations/{id}/evidence` — replay handles.
pub async fn evidence(
    state: web::Data<AppState>,
    id: web::Path<RecommendationId>,
) -> Result<WebResponse<QuantEvidenceView>, WebError> {
    let view = state
        .quant_reports
        .find_evidence(&id)
        .await?
        .ok_or_else(|| WebError::NotFound(format!("recommendation not found: {id}")))?;
    Ok(WebResponse::ok(view))
}

/// `GET /api/quant/recommendations/{id}/attribution` — final WORM attribution.
pub async fn attribution(
    state: web::Data<AppState>,
    id: web::Path<RecommendationId>,
) -> Result<WebResponse<RecommendationAttributionView>, WebError> {
    let info = state
        .execution_read
        .get_recommendation_attribution(&id)
        .await?
        .ok_or_else(|| WebError::NotFound(format!("recommendation attribution not found: {id}")))?;
    Ok(WebResponse::ok(RecommendationAttributionView::from(info)))
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::evidence_metadata;

    #[test]
    fn price_evidence_exposes_a_structured_source_checkpoint() {
        let observed_at = "2026-07-14T04:29:45.570Z";
        let evidence = json!({
            "kind": "price",
            "token_id": "token-1",
            "observed_at": observed_at,
            "available_at": observed_at,
            "gap_generation": 0,
            "price": "0.62",
        });

        let (_, _, checkpoint) = evidence_metadata(&evidence).expect("price evidence metadata");
        let checkpoint = checkpoint.expect("price source checkpoint");
        assert_eq!(checkpoint["token_id"], "token-1");
        assert_eq!(checkpoint["gap_generation"], 0);
        assert_eq!(checkpoint["observed_at"], observed_at);
    }
}

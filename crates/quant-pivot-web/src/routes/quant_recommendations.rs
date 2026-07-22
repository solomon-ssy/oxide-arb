//! Recommendation, evidence, and attribution HTTP endpoints.
//!
//! # UI integration contract
//!
//! | Method | Path | Permission | Purpose |
//! |--------|------|------------|---------|
//! | GET | `/quant/recommendations/{id}` | `quant_report:read` | One recommendation |
//! | GET | `/quant/recommendations/{id}/evidence` | `quant_report:read` | Replay handles |
//! | GET | `/quant/recommendations/{id}/entry-condition` | `quant_report:read` | Durable condition state and tree |
//! | GET | `/quant/recommendations/{id}/entry-condition/audits` | `quant_report:read` | WORM condition timeline |
//! | GET | `/quant/recommendations/{id}/attribution` | `recommendation_attribution:read` | Final WORM attribution |
//!
//! Creating an order intent from a recommendation is `POST /api/quant/intents`
//! (see [`super::quant_intents`]), the governed execution surface.

use actix_web::{
    http::Method,
    web::{Data, Path},
};
use chrono::{DateTime, Utc};
use quant_pivot_models::{
    clickhouse::EntryConditionEvaluationEventRow,
    domain::api::{
        EntryConditionArtifactView, EntryConditionAuditView, EntryConditionDetailView,
        EntryConditionEvaluationView, EntryConditionInstanceSummaryView,
        EntryConditionLeafEvidenceView, EntryConditionSourceCheckpointView, QuantEvidenceView,
        QuantRecommendationView, RecommendationAttributionView,
    },
    enums::{
        quant::EntryConditionState,
        rbac::{Operation, ResourceType},
    },
    types::{ConditionLeafEvidence, ConditionNodeEvaluation, ConditionTruth, RecommendationId},
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
    state: Data<AppState>,
    id: Path<RecommendationId>,
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
        .map(|row| evaluation_view(&row))
        .transpose()?;
    Ok(WebResponse::ok(EntryConditionDetailView {
        instance: EntryConditionInstanceSummaryView::from(instance),
        artifact,
        latest_authoritative_evaluation,
    }))
}

fn evaluation_view(
    row: &EntryConditionEvaluationEventRow,
) -> Result<EntryConditionEvaluationView, WebError> {
    let applied_revision = row.applied_revision.ok_or_else(|| {
        WebError::Internal("applied condition trace has no applied_revision".to_owned())
    })?;
    let evaluated_at = DateTime::from_timestamp_millis(row.evaluated_at).ok_or_else(|| {
        WebError::Internal("condition trace evaluated_at is outside chrono range".to_owned())
    })?;
    let tree =
        serde_json::from_str::<ConditionNodeEvaluation>(&row.tree_json).map_err(|error| {
            WebError::Internal(format!("condition trace tree is invalid JSON: {error}"))
        })?;
    let state = row.state.parse::<EntryConditionState>().map_err(|error| {
        WebError::Internal(format!("condition trace state is invalid: {error}"))
    })?;
    if row.truth != condition_truth_label(&tree.truth) {
        return Err(WebError::Internal(format!(
            "condition trace truth label {} disagrees with typed tree root",
            row.truth
        )));
    }
    let mut leaf_evidence = Vec::new();
    collect_leaf_evidence(&tree, evaluated_at, &mut leaf_evidence);
    Ok(EntryConditionEvaluationView {
        evaluation_id: row.evaluation_id,
        applied_revision,
        evaluator_version: row.evaluator_version,
        evaluated_at,
        state,
        truth: tree.truth.clone(),
        evaluation_hash: row.evaluation_hash,
        input_fingerprint: row.input_fingerprint,
        continuity_hash: row.continuity_hash,
        tree,
        leaf_evidence,
    })
}

fn collect_leaf_evidence(
    node: &ConditionNodeEvaluation,
    evaluated_at: DateTime<Utc>,
    output: &mut Vec<EntryConditionLeafEvidenceView>,
) {
    if let Some(evidence) = &node.evidence {
        let (observed_at, available_at, source_checkpoint) = evidence_metadata(evidence);
        let freshness_ms = observed_at.map(|observed_at| {
            evaluated_at
                .signed_duration_since(observed_at)
                .num_milliseconds()
        });
        let unavailable_reason = match &node.truth {
            ConditionTruth::Unavailable(reason) => Some(reason.clone()),
            ConditionTruth::Satisfied | ConditionTruth::Unsatisfied => None,
        };
        output.push(EntryConditionLeafEvidenceView {
            node_id: node.node_id,
            truth: node.truth.clone(),
            evidence: evidence.clone(),
            observed_at,
            available_at,
            freshness_ms,
            source_checkpoint,
            unavailable_reason,
        });
    }
    for child in &node.children {
        collect_leaf_evidence(child, evaluated_at, output);
    }
}

type EvidenceMetadata = (
    Option<DateTime<Utc>>,
    Option<DateTime<Utc>>,
    Option<EntryConditionSourceCheckpointView>,
);

fn evidence_metadata(evidence: &ConditionLeafEvidence) -> EvidenceMetadata {
    match evidence {
        ConditionLeafEvidence::Price(input) => (
            Some(input.observed_at),
            Some(input.available_at),
            Some(EntryConditionSourceCheckpointView::Price {
                token_id: input.token_id.clone(),
                observed_at: input.observed_at,
                available_at: input.available_at,
                gap_generation: input.gap_generation,
            }),
        ),
        ConditionLeafEvidence::Factor(input) => (
            Some(input.observed_at),
            Some(input.available_at),
            Some(EntryConditionSourceCheckpointView::Factor {
                definition_hash: input.definition_hash,
                model_version_id: input.model_version_id,
                snapshot_hash: input.snapshot_hash,
            }),
        ),
        ConditionLeafEvidence::Weather(input) => (
            Some(input.observation_time),
            Some(input.available_at),
            Some(EntryConditionSourceCheckpointView::Weather {
                revision: input.revision,
                report_hash: input.report_hash,
                gap_generation: input.gap_generation,
            }),
        ),
        ConditionLeafEvidence::Crypto { input, state } => {
            let latest = input.reports.last();
            (
                latest.map(|report| report.event_at),
                latest.map(|report| report.available_at),
                Some(EntryConditionSourceCheckpointView::Crypto {
                    source_sequence: latest.map(|report| report.source_sequence),
                    report_hash: latest.map(|report| report.report_hash),
                    gap_generation: input.gap_generation,
                    discontinuity_epoch: state.discontinuity_epoch,
                    latched: state.latched,
                }),
            )
        }
        ConditionLeafEvidence::Clock { .. } | ConditionLeafEvidence::Unavailable(_) => {
            (None, None, None)
        }
    }
}

const fn condition_truth_label(truth: &ConditionTruth) -> &'static str {
    match truth {
        ConditionTruth::Satisfied => "satisfied",
        ConditionTruth::Unsatisfied => "unsatisfied",
        ConditionTruth::Unavailable(_) => "unavailable",
    }
}

/// WORM condition lifecycle timeline ordered by revision.
pub async fn entry_condition_audits(
    state: Data<AppState>,
    id: Path<RecommendationId>,
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
    state: Data<AppState>,
    id: Path<RecommendationId>,
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
    state: Data<AppState>,
    id: Path<RecommendationId>,
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
    state: Data<AppState>,
    id: Path<RecommendationId>,
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
    use quant_pivot_models::{
        domain::api::EntryConditionSourceCheckpointView, types::ConditionLeafEvidence,
    };
    use serde_json::json;

    use super::evidence_metadata;

    #[test]
    fn price_evidence_exposes_a_structured_source_checkpoint() {
        let observed_at = "2026-07-14T04:29:45.570Z";
        let evidence = serde_json::from_value::<ConditionLeafEvidence>(json!({
            "kind": "price",
            "token_id": "token-1",
            "observed_at": observed_at,
            "available_at": observed_at,
            "gap_generation": 0,
            "price": "0.62",
        }))
        .expect("typed price evidence");

        let (_, _, checkpoint) = evidence_metadata(&evidence);
        let checkpoint = checkpoint.expect("price source checkpoint");
        let EntryConditionSourceCheckpointView::Price {
            token_id,
            observed_at: actual_observed_at,
            gap_generation,
            ..
        } = checkpoint
        else {
            panic!("expected price source checkpoint");
        };
        assert_eq!(token_id.as_str(), "token-1");
        assert_eq!(gap_generation, 0);
        assert_eq!(
            actual_observed_at.to_rfc3339_opts(chrono::SecondsFormat::Millis, true),
            observed_at
        );
    }
}

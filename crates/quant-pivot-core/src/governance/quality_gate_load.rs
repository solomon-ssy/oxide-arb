//! Load-time quality-gate checks for online model inference (Phase 3.7).
//!
//! These are distinct from the offline [`ModelQualityGate`] evaluation at publish
//! time:
//!
//! - **Staleness** applies only to non-`Published` candidate / shadow versions.
//!   A published production active model keeps serving until operator re-runs
//!   backtest + publish; its gate report age is not re-checked on every tick.
//! - **Passed** rejects shadow loads when a prior gate evaluation recorded
//!   `passed = false` (quality-failed models must not shadow).
//! - **Publication status** enforces that config `active_model_version_id` resolves
//!   to a `Published` row and shadow ids resolve to `Candidate` / `Shadow`.

use chrono::{DateTime, Utc};
use quant_pivot_models::{domain::ModelVersionInfo, enums::quant::PublicationStatus};

/// Whether the persisted gate report is fresh enough for a non-published load.
///
/// `Published` versions are exempt: production active models are gated at publish
/// time, not on every inference tick. A `0` budget disables the check entirely.
/// Versions without `evaluated_at` in the report are not subject to staleness.
pub fn quality_gate_staleness_ok(
    version: &ModelVersionInfo,
    min_age_secs: u64,
    now: DateTime<Utc>,
) -> Result<(), String> {
    if version.publication_status == PublicationStatus::Published || min_age_secs == 0 {
        return Ok(());
    }
    let Some(timestamp) = version
        .quality_gate_report
        .get("evaluated_at")
        .and_then(serde_json::Value::as_str)
    else {
        return Ok(());
    };
    let evaluated_at = DateTime::parse_from_rfc3339(timestamp)
        .map_err(|error| format!("unparseable evaluated_at `{timestamp}`: {error}"))?
        .with_timezone(&Utc);
    let age_secs = now.signed_duration_since(evaluated_at).num_seconds();
    if age_secs > i64::try_from(min_age_secs).unwrap_or(i64::MAX) {
        return Err(format!(
            "quality gate report is {age_secs}s old (max {min_age_secs}s)"
        ));
    }
    Ok(())
}

/// Whether the version is allowed to load on the shadow path.
///
/// Rejects models whose persisted gate report explicitly recorded `passed = false`.
/// An absent `passed` field (never gated) is allowed so fresh candidates can shadow.
pub fn quality_gate_passed_ok(version: &ModelVersionInfo) -> Result<(), String> {
    match version.quality_gate_report.get("passed") {
        Some(serde_json::Value::Bool(false)) => {
            Err("quality gate report recorded passed=false; shadow inference blocked".to_owned())
        }
        _ => Ok(()),
    }
}

/// Whether the config active pointer resolves to a production-published version.
pub fn active_publication_status_ok(version: &ModelVersionInfo) -> Result<(), String> {
    if version.publication_status == PublicationStatus::Published {
        return Ok(());
    }
    Err(format!(
        "active model {} must be published (status {})",
        version.model_version_id,
        version.publication_status.as_str()
    ))
}

/// Whether the config shadow pointer resolves to an experimental candidate version.
pub fn shadow_publication_status_ok(version: &ModelVersionInfo) -> Result<(), String> {
    match version.publication_status {
        PublicationStatus::Candidate | PublicationStatus::Shadow => Ok(()),
        status => Err(format!(
            "shadow model {} must be candidate or shadow (status {})",
            version.model_version_id,
            status.as_str()
        )),
    }
}

/// Combined load-time checks for the config shadow model pointer.
pub fn shadow_load_ok(
    version: &ModelVersionInfo,
    min_age_secs: u64,
    now: DateTime<Utc>,
) -> Result<(), String> {
    shadow_publication_status_ok(version)?;
    quality_gate_passed_ok(version)?;
    quality_gate_staleness_ok(version, min_age_secs, now)
}

/// Combined load-time checks for the config active model pointer.
pub fn active_load_ok(
    version: &ModelVersionInfo,
    min_age_secs: u64,
    now: DateTime<Utc>,
) -> Result<(), String> {
    active_publication_status_ok(version)?;
    quality_gate_staleness_ok(version, min_age_secs, now)
}

#[cfg(test)]
mod tests {
    use super::{
        active_publication_status_ok, quality_gate_passed_ok, quality_gate_staleness_ok,
        shadow_publication_status_ok,
    };
    use chrono::{Duration, Utc};
    use quant_pivot_models::{
        domain::ModelVersionInfo,
        enums::quant::PublicationStatus,
        types::{ContentHash, ModelSpecId, ModelVersionId},
    };

    fn version(status: PublicationStatus, report: serde_json::Value) -> ModelVersionInfo {
        ModelVersionInfo {
            model_version_id: ModelVersionId::from_v7(),
            model_spec_id: ModelSpecId::from_v7(),
            version: 1,
            artifact_hash: ContentHash::parse(format!("blake3:{}", "0".repeat(64))).expect("hash"),
            training_dataset_id: None,
            publish_path_set_id: None,
            metrics_json: serde_json::json!({}),
            training_objective_json: serde_json::json!({"kind": "not_trained"}),
            quality_gate_report: report,
            publication_status: status,
            published_at: None,
            retired_at: None,
            created_at: Utc::now(),
        }
    }

    #[test]
    fn published_active_is_exempt_from_staleness() {
        let now = Utc::now();
        let stale = now - Duration::seconds(90_000);
        let report = serde_json::json!({ "evaluated_at": stale.to_rfc3339() });
        assert!(
            quality_gate_staleness_ok(&version(PublicationStatus::Published, report), 86_400, now,)
                .is_ok()
        );
    }

    #[test]
    fn candidate_staleness_is_enforced() {
        let now = Utc::now();
        let stale = now - Duration::seconds(90_000);
        let report = serde_json::json!({ "evaluated_at": stale.to_rfc3339() });
        assert!(
            quality_gate_staleness_ok(&version(PublicationStatus::Candidate, report), 86_400, now,)
                .is_err()
        );
    }

    #[test]
    fn shadow_rejects_failed_gate_report() {
        let report = serde_json::json!({ "passed": false });
        assert!(quality_gate_passed_ok(&version(PublicationStatus::Candidate, report,),).is_err());
    }

    #[test]
    fn active_must_be_published() {
        assert!(
            active_publication_status_ok(&version(
                PublicationStatus::Retired,
                serde_json::json!({}),
            ),)
            .is_err()
        );
        assert!(
            active_publication_status_ok(&version(
                PublicationStatus::Published,
                serde_json::json!({}),
            ),)
            .is_ok()
        );
    }

    #[test]
    fn shadow_must_be_candidate_or_shadow() {
        assert!(
            shadow_publication_status_ok(&version(
                PublicationStatus::Published,
                serde_json::json!({}),
            ),)
            .is_err()
        );
        assert!(
            shadow_publication_status_ok(&version(
                PublicationStatus::Candidate,
                serde_json::json!({}),
            ),)
            .is_ok()
        );
    }
}

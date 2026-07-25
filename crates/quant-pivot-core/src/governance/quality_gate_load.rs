//! Load-time quality-gate checks for online model inference.
//!
//! These are distinct from the offline model-quality-gate evaluation at publish
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
use quant_pivot_models::{domain::quant::ModelVersionInfo, enums::quant::PublicationStatus};

/// Whether the persisted gate report is fresh enough for a non-published load.
///
/// `Published` versions are exempt: production active models are gated at publish
/// time, not on every inference tick. A `0` budget disables the check entirely.
/// Versions without a gate report are not subject to staleness.
pub fn quality_gate_staleness_ok(
    version: &ModelVersionInfo,
    min_age_secs: u64,
    now: DateTime<Utc>,
) -> Result<(), String> {
    if version.publication_status == PublicationStatus::Published || min_age_secs == 0 {
        return Ok(());
    }
    let Some(report) = &version.quality_gate_report else {
        return Ok(());
    };
    let age_secs = now.signed_duration_since(report.evaluated_at).num_seconds();
    if age_secs > i64::try_from(min_age_secs).unwrap_or(i64::MAX) {
        return Err(format!(
            "quality gate report is {age_secs}s old (max {min_age_secs}s)"
        ));
    }
    Ok(())
}

/// Whether the version is allowed to load on the shadow path.
///
/// Rejects models whose persisted gate report recorded `passed = false`.
/// An absent report is allowed so fresh candidates can shadow.
pub fn quality_gate_passed_ok(version: &ModelVersionInfo) -> Result<(), String> {
    if version
        .quality_gate_report
        .as_ref()
        .is_some_and(|report| !report.passed)
    {
        return Err(
            "quality gate report recorded passed=false; shadow inference blocked".to_owned(),
        );
    }
    Ok(())
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
    use chrono::{DateTime, Duration, Utc};
    use quant_pivot_models::{
        domain::quant::ModelVersionInfo,
        enums::{model::ModelFamily, quant::PublicationStatus},
        types::{
            ContentHash, ModelSpecId, ModelVersionId,
            model_metrics::ModelVersionMetrics,
            model_quality::{
                GateIntent, GateSubject, QUALITY_GATE_REPORT_FORMAT_VERSION, QualityGateReport,
            },
            model_training::ModelTrainingObjective,
        },
    };

    use super::{
        active_publication_status_ok, quality_gate_passed_ok, quality_gate_staleness_ok,
        shadow_publication_status_ok,
    };
    use crate::test_fixtures::{
        execution_pg_seed::fixture_profile_ref, model_spec_fixtures::model_spec_lineage_fixture,
    };

    fn report(evaluated_at: DateTime<Utc>, passed: bool) -> QualityGateReport {
        QualityGateReport {
            format_version: QUALITY_GATE_REPORT_FORMAT_VERSION,
            subject: GateSubject::ModelVersion(ModelVersionId::from_v7()),
            intent: GateIntent::Publish,
            evaluated_at,
            gates: Vec::new(),
            hard_failures: Vec::new(),
            soft_warnings: Vec::new(),
            passed,
            report_hash: ContentHash::parse(&format!("blake3:{}", "1".repeat(64))).expect("hash"),
        }
    }

    fn version(
        status: PublicationStatus,
        quality_gate_report: Option<QualityGateReport>,
    ) -> ModelVersionInfo {
        let (model_spec_thesis, model_spec_definition_hash) =
            model_spec_lineage_fixture("quality-gate-load-test-spec");
        ModelVersionInfo {
            model_version_id: ModelVersionId::from_v7(),
            model_spec_id: ModelSpecId::from_v7(),
            model_spec_name: "quality-gate-load-test-spec".to_owned(),
            model_family: ModelFamily::WeightedFactor,
            model_spec_thesis,
            model_spec_definition_hash,
            version: 1,
            artifact_hash: ContentHash::parse(&format!("blake3:{}", "0".repeat(64))).expect("hash"),
            category_scope: None,
            profile_ref: fixture_profile_ref(),
            training_dataset_id: None,
            trade_policy_artifact_id: None,
            trade_policy_hash: None,
            publish_path_set_id: None,
            derivation_kind: ModelVersionInfo::training_derivation_kind(),
            parent_model_version_id: None,
            calibration_artifact_id: None,
            derivation_evidence_hash: None,
            metrics: ModelVersionMetrics::not_measured("test fixture"),
            training_objective: ModelTrainingObjective::hand_authored("test fixture"),
            quality_gate_report,
            publication_status: status,
            published_at: None,
            retired_at: None,
            created_at: Utc::now(),
        }
    }

    #[test]
    fn published_active_exempt_staleness() {
        let now = Utc::now();
        let stale = now - Duration::seconds(90_000);
        assert!(
            quality_gate_staleness_ok(
                &version(PublicationStatus::Published, Some(report(stale, true))),
                86_400,
                now,
            )
            .is_ok()
        );
    }

    #[test]
    fn candidate_staleness_is_enforced() {
        let now = Utc::now();
        let stale = now - Duration::seconds(90_000);
        assert!(
            quality_gate_staleness_ok(
                &version(PublicationStatus::Candidate, Some(report(stale, true))),
                86_400,
                now,
            )
            .is_err()
        );
    }

    #[test]
    fn shadow_rejects_failed_report() {
        assert!(
            quality_gate_passed_ok(&version(
                PublicationStatus::Candidate,
                Some(report(Utc::now(), false)),
            ))
            .is_err()
        );
    }

    #[test]
    fn active_must_be_published() {
        assert!(
            active_publication_status_ok(&version(PublicationStatus::Retired, None,),).is_err()
        );
        assert!(
            active_publication_status_ok(&version(PublicationStatus::Published, None,),).is_ok()
        );
    }

    #[test]
    fn shadow_candidate_shadow() {
        assert!(
            shadow_publication_status_ok(&version(PublicationStatus::Published, None,),).is_err()
        );
        assert!(
            shadow_publication_status_ok(&version(PublicationStatus::Candidate, None,),).is_ok()
        );
    }
}

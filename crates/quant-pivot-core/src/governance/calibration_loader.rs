//! [`CoreCalibrationArtifactLoader`]: resolves `model_score` calibration artifacts.
//!
//! Loads a [`CalibrationArtifactId`] into compute-domain [`ResolvedCalibration`] for
//! `quant-pivot-research`'s `DefaultModelRuntimeFactory` (Phase 11.3 §5).
//!
//! Implements the research-crate-owned [`CalibrationArtifactLoader`] port over
//! the persistence-crate `CalibrationArtifactRepository` — the same
//! dependency-inversion shape as [`crate::artifact`]'s `ArtifactStore` for
//! model bytes, keeping `quant-pivot-research` free of any persistence-crate
//! dependency.

use std::sync::Arc;

use async_trait::async_trait;
use quant_pivot_error::{QuantError, QuantResult, research::ResearchError};
use quant_pivot_models::{
    domain::query::TimeWindow,
    enums::quant::CalibrationKind,
    hashing::CanonicalDigest,
    types::{CalibrationArtifactId, ContentHash, ModelVersionId, TrainingDatasetId},
};
use quant_pivot_repository::traits::CalibrationArtifactRepository;
use quant_pivot_research::model::{
    CalibrationArtifactLoader, ModelArtifact, MonotoneMapping, ResolvedCalibration,
};
use serde::Serialize;

/// Resolve a model artifact's return-model calibration state through the
/// **single** deep check every production consumer shares (Phase 11.3
/// closed-loop hardening).
///
/// Publish gate, report builder, admission, and intent creation all call this —
/// never their own independent re-implementation. Before this consolidation,
/// `publish` only inspected the `ReturnModelSpec` enum tag
/// (`ModelArtifact::return_model_is_calibrated`) while `report`/`admission`
/// re-verified calibrator liveness + content hash, so a calibrator deactivated
/// between `bind_calibration` and `publish` could pass the gate yet fail every
/// downstream consumer — the exact "judged once, drifts later" gap this function
/// closes.
///
/// - `Heuristic` (or a family with no return-model concept) ⇒ `Ok(None)` —
///   never an error; a cold-start bootstrap candidate is a valid, if
///   unpublishable / unexecutable, state.
/// - `Calibrated` whose bound calibrator loads clean (active, hash-verified)
///   ⇒ `Ok(Some(resolved))`.
/// - `Calibrated` whose calibrator is missing / inactive / hash-mismatched ⇒
///   `Err` — fail-closed, never silently downgraded to "uncalibrated".
///
/// # Errors
///
/// Propagates [`CalibrationArtifactLoader::load`]'s fail-closed errors.
pub async fn resolve_return_model_calibration(
    loader: &dyn CalibrationArtifactLoader,
    artifact: &ModelArtifact,
) -> QuantResult<Option<ResolvedCalibration>> {
    let Some(calibrator_ref) = artifact.calibrator_ref() else {
        return Ok(None);
    };
    let resolved = loader.load(calibrator_ref).await?;
    Ok(Some(resolved))
}

/// Payload shape stored in `quant_calibration_artifact.payload_json` for
/// `kind = model_score` rows.
///
/// Carries its own fit provenance (`model_version_id` /
/// `calibration_dataset_id`) so the content hash — and an operator inspecting
/// the artifact — never need external context to verify or explain it,
/// mirroring `market_price_bias`'s self-contained `by_category` payload.
/// Mirrors [`crate::service::model_calibration_fit`]'s persist step — the single
/// source of truth for this JSON shape.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct ModelScoreCalibrationPayload {
    pub model_version_id: ModelVersionId,
    pub calibration_dataset_id: TrainingDatasetId,
    pub mapping: MonotoneMapping,
    pub reliability: quant_pivot_research::model::ReliabilityReport,
}

/// Canonical projection for the `model_score` content hash — recomputable
/// purely from fields persisted on the row (`fit_window`,
/// `calibration_split_hash`, and the self-contained payload), the same
/// "verify with no external context" shape `market_price_bias`'s
/// `BiasTableCanonical` uses. Shared by the fit service (mints the hash) and
/// this loader (re-verifies it fail-closed on every load).
#[derive(Serialize)]
struct ModelScoreCanonical<'a> {
    fit_window: &'a TimeWindow,
    calibration_split_hash: &'a ContentHash,
    payload: &'a ModelScoreCalibrationPayload,
}

/// Compute the content-addressed hash for a `model_score` calibration
/// artifact from its persisted, self-contained fields.
///
/// # Errors
///
/// Returns a [`ResearchError::DatasetBuild`] when serialization fails.
pub fn model_score_content_hash(
    fit_window: &TimeWindow,
    calibration_split_hash: &ContentHash,
    payload: &ModelScoreCalibrationPayload,
) -> QuantResult<ContentHash> {
    CanonicalDigest::content_hash_json(&ModelScoreCanonical {
        fit_window,
        calibration_split_hash,
        payload,
    })
    .map_err(|error| {
        QuantError::from(ResearchError::DatasetBuild {
            detail: format!("model-score calibration content hash failed: {error}"),
        })
    })
}

/// Loads and validates `model_score` calibration artifacts.
pub struct CoreCalibrationArtifactLoader {
    repo: Arc<dyn CalibrationArtifactRepository>,
}

impl CoreCalibrationArtifactLoader {
    #[must_use]
    pub const fn new(repo: Arc<dyn CalibrationArtifactRepository>) -> Self {
        Self { repo }
    }
}

#[async_trait]
impl CalibrationArtifactLoader for CoreCalibrationArtifactLoader {
    async fn load(&self, artifact_id: &CalibrationArtifactId) -> QuantResult<ResolvedCalibration> {
        let info = self
            .repo
            .find_by_id(artifact_id)
            .await
            .map_err(QuantError::from)?
            .ok_or_else(|| {
                QuantError::from(ResearchError::DatasetBuild {
                    detail: format!(
                        "calibration artifact `{artifact_id}` not found — a `Calibrated` \
                         return model must never load with a missing calibrator"
                    ),
                })
            })?;
        if info.kind != CalibrationKind::ModelScore {
            return Err(QuantError::from(ResearchError::DatasetBuild {
                detail: format!(
                    "calibration artifact `{artifact_id}` is kind `{}`, expected `model_score`",
                    info.kind.as_str()
                ),
            }));
        }
        // Fail-closed governance: a calibrator that was never bound/activated
        // (or was superseded — Phase 11.3 `active` lifecycle) must never
        // resolve. The `Calibrated` return model's `calibrator_ref` is only
        // ever set by `bind_calibration`, which activates the target row in
        // the same transaction, so a well-formed reference is always active;
        // a mismatch here means the artifact was deactivated after binding.
        if !info.active {
            return Err(QuantError::from(ResearchError::DatasetBuild {
                detail: format!(
                    "calibration artifact `{artifact_id}` is not active — a superseded or \
                     never-activated calibrator must never resolve"
                ),
            }));
        }
        let fit_window = TimeWindow::new(info.fit_window_start, info.fit_window_end);
        let payload: ModelScoreCalibrationPayload = serde_json::from_value(
            info.payload_json.clone(),
        )
        .map_err(|error| {
            QuantError::from(ResearchError::DatasetBuild {
                detail: format!(
                    "calibration artifact `{artifact_id}` payload deserialization failed: {error}"
                ),
            })
        })?;
        // Fail-closed integrity: recompute the content hash from the
        // persisted, self-contained fields and verify it against the stored
        // hash — symmetric with `FavoriteLongshotBiasTable::from_persisted`.
        // A tampered or corrupted payload must never silently bind.
        let recomputed =
            model_score_content_hash(&fit_window, &info.calibration_split_hash, &payload)?;
        if recomputed != info.content_hash {
            return Err(QuantError::from(ResearchError::DatasetBuild {
                detail: format!(
                    "calibration artifact `{artifact_id}` content hash mismatch: stored {} \
                     recomputed {recomputed}",
                    info.content_hash
                ),
            }));
        }
        Ok(ResolvedCalibration {
            artifact_id: artifact_id.clone(),
            mapping: payload.mapping,
            reliability: payload.reliability,
        })
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Mutex;

    use chrono::Utc;
    use quant_pivot_error::storage::StorageError;
    use quant_pivot_models::domain::{
        CalibrationArtifactInfo, CalibrationArtifactListQuery, NewCalibrationArtifact, Paginated,
    };
    use quant_pivot_research::model::ReliabilityReport;
    use rust_decimal_macros::dec;

    use super::{
        CalibrationArtifactId, CalibrationArtifactLoader, CalibrationArtifactRepository,
        CalibrationKind, ContentHash, CoreCalibrationArtifactLoader, ModelScoreCalibrationPayload,
        ModelVersionId, TimeWindow, TrainingDatasetId, model_score_content_hash,
    };

    /// In-memory fake repository — the loader's fail-closed branches (not
    /// found / wrong kind / inactive / hash mismatch) are pure logic over
    /// whatever `find_by_id` returns, so no database is needed to exercise
    /// them.
    struct FakeRepo {
        row: Mutex<Option<CalibrationArtifactInfo>>,
    }

    #[async_trait::async_trait]
    impl CalibrationArtifactRepository for FakeRepo {
        async fn create(
            &self,
            _artifact: NewCalibrationArtifact,
        ) -> Result<CalibrationArtifactInfo, StorageError> {
            unimplemented!("not exercised by loader tests")
        }

        async fn find_by_id(
            &self,
            _artifact_id: &CalibrationArtifactId,
        ) -> Result<Option<CalibrationArtifactInfo>, StorageError> {
            Ok(self.row.lock().expect("lock").clone())
        }

        async fn page(
            &self,
            _query: CalibrationArtifactListQuery,
        ) -> Result<Paginated<CalibrationArtifactInfo>, StorageError> {
            unimplemented!("not exercised by loader tests")
        }

        async fn mark_active(
            &self,
            _artifact_id: &CalibrationArtifactId,
        ) -> Result<CalibrationArtifactInfo, StorageError> {
            unimplemented!("not exercised by loader tests")
        }
    }

    fn fake_content_hash(seed: u8) -> ContentHash {
        let hex: String = format!("{seed:02x}").chars().cycle().take(64).collect();
        ContentHash::parse(format!("blake3:{hex}")).expect("hash")
    }

    fn valid_row(active: bool) -> CalibrationArtifactInfo {
        let fit_window = TimeWindow::new(
            Utc::now() - chrono::Duration::days(30),
            Utc::now() - chrono::Duration::days(1),
        );
        let calibration_split_hash = fake_content_hash(1);
        let payload = dummy_payload();
        let content_hash =
            model_score_content_hash(&fit_window, &calibration_split_hash, &payload).expect("hash");
        CalibrationArtifactInfo {
            artifact_id: CalibrationArtifactId::from_v7(),
            kind: CalibrationKind::ModelScore,
            content_hash,
            fit_window_start: fit_window.from,
            fit_window_end: fit_window.to,
            calibration_split_hash,
            sample_count: 1_000,
            payload_json: serde_json::to_value(&payload).expect("payload json"),
            active,
            created_at: Utc::now(),
        }
    }

    fn dummy_payload() -> ModelScoreCalibrationPayload {
        ModelScoreCalibrationPayload {
            model_version_id: ModelVersionId::from_v7(),
            calibration_dataset_id: TrainingDatasetId::from_v7(),
            mapping: quant_pivot_research::model::MonotoneMapping::Isotonic { knots: Vec::new() },
            reliability: ReliabilityReport {
                bins: Vec::new(),
                brier_score: dec!(0.1),
                log_loss: dec!(0.3),
                ece: dec!(0.02),
                n_samples: 1_000,
            },
        }
    }

    fn loader(row: Option<CalibrationArtifactInfo>) -> CoreCalibrationArtifactLoader {
        CoreCalibrationArtifactLoader::new(std::sync::Arc::new(FakeRepo {
            row: Mutex::new(row),
        }))
    }

    #[tokio::test]
    async fn load_fails_closed_when_artifact_missing() {
        let loader = loader(None);
        let result = loader.load(&CalibrationArtifactId::from_v7()).await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn load_fails_closed_on_wrong_kind() {
        let mut row = valid_row(true);
        row.kind = CalibrationKind::MarketPriceBias;
        row.payload_json = serde_json::json!({});
        let artifact_id = row.artifact_id.clone();
        let loader = loader(Some(row));
        let result = loader.load(&artifact_id).await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn load_rejects_inactive_artifact() {
        let row = valid_row(false);
        let artifact_id = row.artifact_id.clone();
        let loader = loader(Some(row));
        let result = loader.load(&artifact_id).await;
        assert!(
            result.is_err(),
            "an inactive (never-activated or superseded) calibrator must never resolve"
        );
    }

    #[tokio::test]
    async fn load_rejects_content_hash_mismatch() {
        let mut row = valid_row(true);
        // Tamper with the payload after the hash was computed.
        let mut payload: ModelScoreCalibrationPayload =
            serde_json::from_value(row.payload_json.clone()).expect("payload");
        payload.reliability.ece = dec!(0.99);
        row.payload_json = serde_json::to_value(&payload).expect("payload json");
        let artifact_id = row.artifact_id.clone();
        let loader = loader(Some(row));
        let result = loader.load(&artifact_id).await;
        assert!(
            result.is_err(),
            "a tampered payload must fail content-hash reverification"
        );
    }

    #[tokio::test]
    async fn load_succeeds_for_active_valid_artifact() {
        let row = valid_row(true);
        let artifact_id = row.artifact_id.clone();
        let loader = loader(Some(row));
        let resolved = loader.load(&artifact_id).await.expect("resolve");
        assert_eq!(resolved.artifact_id, artifact_id);
    }
}

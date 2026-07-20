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
    domain::{CalibrationArtifactPayload, query::TimeWindow},
    enums::quant::CalibrationKind,
    hashing::CanonicalDigest,
    types::{CalibrationArtifactId, ContentHash, calibration::ModelScoreCalibrationPayload},
};
use quant_pivot_repository::traits::CalibrationArtifactRepository;
use quant_pivot_research::model::{
    CalibrationArtifactLoader, ModelArtifact, ResolvedCalibration, validate_mapping,
};
use rust_decimal::Decimal;
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
        let CalibrationArtifactPayload::ModelScore(payload) = &info.payload else {
            return Err(QuantError::from(ResearchError::DatasetBuild {
                detail: format!(
                    "calibration artifact `{artifact_id}` kind/payload discriminator mismatch"
                ),
            }));
        };
        // Fail-closed integrity: recompute the content hash from the
        // persisted, self-contained fields and verify it against the stored
        // hash — symmetric with `FavoriteLongshotBiasTable::from_persisted`.
        // A tampered or corrupted payload must never silently bind.
        let recomputed =
            model_score_content_hash(&fit_window, &info.calibration_split_hash, payload)?;
        if recomputed != info.content_hash {
            return Err(QuantError::from(ResearchError::DatasetBuild {
                detail: format!(
                    "calibration artifact `{artifact_id}` content hash mismatch: stored {} \
                     recomputed {recomputed}",
                    info.content_hash
                ),
            }));
        }
        validate_model_score_payload(payload, info.sample_count)?;
        Ok(ResolvedCalibration {
            artifact_id: artifact_id.clone(),
            mapping: payload.mapping.clone(),
            reliability: payload.reliability.clone(),
        })
    }
}

fn validate_model_score_payload(
    payload: &ModelScoreCalibrationPayload,
    persisted_sample_count: i64,
) -> QuantResult<()> {
    validate_mapping(&payload.mapping)?;
    let reliability = &payload.reliability;
    let persisted_sample_count =
        u64::try_from(persisted_sample_count).map_err(|error| ResearchError::DatasetBuild {
            detail: format!("calibration artifact sample_count is invalid: {error}"),
        })?;
    if reliability.n_samples == 0
        || reliability.n_samples != persisted_sample_count
        || reliability.bins.is_empty()
    {
        return Err(ResearchError::DatasetBuild {
            detail: format!(
                "calibration reliability sample contract is invalid: persisted={persisted_sample_count}, report={}, bins={}",
                reliability.n_samples,
                reliability.bins.len()
            ),
        }
        .into());
    }
    if reliability.brier_score < Decimal::ZERO
        || reliability.brier_score > Decimal::ONE
        || reliability.log_loss < Decimal::ZERO
        || reliability.ece < Decimal::ZERO
        || reliability.ece > Decimal::ONE
    {
        return Err(ResearchError::DatasetBuild {
            detail: "calibration reliability metrics are outside their valid ranges".to_owned(),
        }
        .into());
    }
    let mut sample_total = 0_u64;
    let mut previous_hi = Decimal::ZERO;
    for bin in &reliability.bins {
        if bin.sample_count == 0
            || bin.predicted_lo < Decimal::ZERO
            || bin.predicted_lo >= bin.predicted_hi
            || bin.predicted_hi > Decimal::ONE
            || bin.predicted_lo < previous_hi
            || bin.mean_predicted.inner() < bin.predicted_lo
            || bin.mean_predicted.inner() > bin.predicted_hi
            || bin.empirical_frequency.inner() < Decimal::ZERO
            || bin.empirical_frequency.inner() > Decimal::ONE
            || bin.wilson_ci.0.inner() < Decimal::ZERO
            || bin.wilson_ci.0.inner() > bin.wilson_ci.1.inner()
            || bin.wilson_ci.1.inner() > Decimal::ONE
        {
            return Err(ResearchError::DatasetBuild {
                detail: format!(
                    "calibration reliability bin [{}, {}] is structurally invalid",
                    bin.predicted_lo, bin.predicted_hi
                ),
            }
            .into());
        }
        sample_total = sample_total.checked_add(bin.sample_count).ok_or_else(|| {
            ResearchError::DatasetBuild {
                detail: "calibration reliability sample count overflow".to_owned(),
            }
        })?;
        previous_hi = bin.predicted_hi;
    }
    if sample_total != reliability.n_samples {
        return Err(ResearchError::DatasetBuild {
            detail: format!(
                "calibration reliability bins contain {sample_total} samples, expected {}",
                reliability.n_samples
            ),
        }
        .into());
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use std::sync::{Arc, Mutex};

    use chrono::Utc;
    use quant_pivot_error::storage::StorageError;
    use quant_pivot_models::{
        domain::{
            CalibrationArtifactInfo, CalibrationArtifactListQuery, NewCalibrationArtifact,
            Paginated,
        },
        types::{
            ModelVersionId, Probability, TrainingDatasetId,
            calibration::{
                IsotonicKnot, ModelScoreCalibrationPayload, MonotoneMapping,
                PublishedWeatherStationLeadBias, ReliabilityBin, ReliabilityReport,
            },
        },
    };
    use rust_decimal_macros::dec;

    use super::{
        CalibrationArtifactId, CalibrationArtifactLoader, CalibrationArtifactPayload,
        CalibrationArtifactRepository, CalibrationKind, ContentHash, CoreCalibrationArtifactLoader,
        TimeWindow, model_score_content_hash,
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

        async fn find_by_content_hash(
            &self,
            _content_hash: &ContentHash,
        ) -> Result<Option<CalibrationArtifactInfo>, StorageError> {
            unimplemented!("not exercised by loader tests")
        }

        async fn page(
            &self,
            _query: CalibrationArtifactListQuery,
        ) -> Result<Paginated<CalibrationArtifactInfo>, StorageError> {
            unimplemented!("not exercised by loader tests")
        }

        async fn published_weather_through(
            &self,
            _at: chrono::DateTime<Utc>,
        ) -> Result<Vec<PublishedWeatherStationLeadBias>, StorageError> {
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
            payload: CalibrationArtifactPayload::ModelScore(payload),
            active,
            created_at: Utc::now(),
        }
    }

    fn dummy_payload() -> ModelScoreCalibrationPayload {
        ModelScoreCalibrationPayload {
            model_version_id: ModelVersionId::from_v7(),
            calibration_dataset_id: TrainingDatasetId::from_v7(),
            mapping: MonotoneMapping::Isotonic {
                knots: vec![IsotonicKnot {
                    score: dec!(0.5),
                    probability: dec!(0.5),
                }],
            },
            reliability: ReliabilityReport {
                bins: vec![ReliabilityBin {
                    predicted_lo: dec!(0),
                    predicted_hi: dec!(1),
                    sample_count: 1_000,
                    mean_predicted: Probability::new(dec!(0.5)),
                    empirical_frequency: Probability::new(dec!(0.5)),
                    wilson_ci: (Probability::new(dec!(0.45)), Probability::new(dec!(0.55))),
                    mean_adverse_excursion_bps: Some(dec!(-500)),
                }],
                brier_score: dec!(0.1),
                log_loss: dec!(0.3),
                ece: dec!(0.02),
                n_samples: 1_000,
            },
        }
    }

    fn loader(row: Option<CalibrationArtifactInfo>) -> CoreCalibrationArtifactLoader {
        CoreCalibrationArtifactLoader::new(Arc::new(FakeRepo {
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
    async fn load_rejects_hash_valid_but_empty_mapping() {
        let mut row = valid_row(true);
        let CalibrationArtifactPayload::ModelScore(mut payload) = row.payload else {
            panic!("model-score payload")
        };
        payload.mapping = MonotoneMapping::Isotonic { knots: Vec::new() };
        row.payload = CalibrationArtifactPayload::ModelScore(payload.clone());
        row.content_hash = model_score_content_hash(
            &TimeWindow::new(row.fit_window_start, row.fit_window_end),
            &row.calibration_split_hash,
            &payload,
        )
        .expect("hash");
        let artifact_id = row.artifact_id.clone();
        assert!(loader(Some(row)).load(&artifact_id).await.is_err());
    }

    #[tokio::test]
    async fn load_rejects_content_hash_mismatch() {
        let mut row = valid_row(true);
        // Tamper with the payload after the hash was computed.
        let CalibrationArtifactPayload::ModelScore(mut payload) = row.payload else {
            panic!("model-score payload")
        };
        payload.reliability.ece = dec!(0.99);
        row.payload = CalibrationArtifactPayload::ModelScore(payload);
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

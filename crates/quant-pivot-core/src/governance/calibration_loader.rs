//! [`CoreCalibrationArtifactLoader`]: resolves `model_score` calibration artifacts.
//!
//! Loads a [`CalibrationArtifactId`] into compute-domain [`ResolvedCalibration`].
//!
//! Implements the research-crate-owned [`CalibrationArtifactLoader`] port over
//! the persistence-crate `CalibrationArtifactRepository` — the same
//! dependency-inversion shape as the artifact store for
//! model bytes, keeping `quant-pivot-research` free of any persistence-crate
//! dependency.

use std::sync::Arc;

use async_trait::async_trait;
use quant_pivot_error::{QuantError, QuantResult, research::ResearchError};
use quant_pivot_models::{
    domain::{
        quant::{CalibrationArtifactInfo, CalibrationArtifactPayload},
        query::TimeWindow,
    },
    enums::quant::CalibrationKind,
    types::{CalibrationArtifactId, ContentHash, calibration::ModelScoreCalibrationPayload},
};
use quant_pivot_repository::traits::CalibrationArtifactRepository;
use quant_pivot_research::model::{CalibrationArtifactLoader, ModelArtifact, ResolvedCalibration};

/// Resolve a model artifact's return-model calibration state through the
/// **single** deep check shared by every production consumer.
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
    payload
        .content_hash(fit_window.from, fit_window.to, calibration_split_hash)
        .map_err(|error| QuantError::from(ResearchError::DatasetBuild { detail: error }))
}

/// Integrity-gated immutable projection of a persisted `model_score` row.
///
/// Lifecycle is intentionally not part of this value: fitting creates an
/// inactive artifact and governance must verify its immutable preimage before
/// activating it, while serving additionally requires the row to be active.
/// Both paths therefore share this exact content/payload verifier without
/// weakening their distinct lifecycle rules.
pub(crate) struct VerifiedModelScoreCalibration {
    artifact_id: CalibrationArtifactId,
    content_hash: ContentHash,
    fit_window: TimeWindow,
    payload: ModelScoreCalibrationPayload,
}

impl VerifiedModelScoreCalibration {
    #[must_use]
    pub(crate) const fn artifact_id(&self) -> CalibrationArtifactId {
        self.artifact_id
    }

    #[must_use]
    pub(crate) const fn content_hash(&self) -> ContentHash {
        self.content_hash
    }

    #[must_use]
    pub(crate) const fn fit_window(&self) -> &TimeWindow {
        &self.fit_window
    }

    #[must_use]
    pub(crate) const fn payload(&self) -> &ModelScoreCalibrationPayload {
        &self.payload
    }
}

impl TryFrom<&CalibrationArtifactInfo> for VerifiedModelScoreCalibration {
    type Error = QuantError;

    fn try_from(info: &CalibrationArtifactInfo) -> Result<Self, Self::Error> {
        if info.kind != CalibrationKind::ModelScore {
            return Err(ResearchError::DatasetBuild {
                detail: format!(
                    "calibration artifact `{}` is kind `{}`, expected `model_score`",
                    info.artifact_id,
                    info.kind.as_str()
                ),
            }
            .into());
        }
        let CalibrationArtifactPayload::ModelScore(payload) = &info.payload else {
            return Err(ResearchError::DatasetBuild {
                detail: format!(
                    "calibration artifact `{}` kind/payload discriminator mismatch",
                    info.artifact_id
                ),
            }
            .into());
        };
        let fit_window = TimeWindow::new(info.fit_window_start, info.fit_window_end);
        let recomputed =
            model_score_content_hash(&fit_window, &info.calibration_split_hash, payload)?;
        if recomputed != info.content_hash {
            return Err(ResearchError::DatasetBuild {
                detail: format!(
                    "calibration artifact `{}` content hash mismatch: stored {}, recomputed \
                     {recomputed}",
                    info.artifact_id, info.content_hash
                ),
            }
            .into());
        }
        validate_model_score_payload(payload, info.sample_count)?;
        Ok(Self {
            artifact_id: info.artifact_id,
            content_hash: info.content_hash,
            fit_window,
            payload: payload.as_ref().clone(),
        })
    }
}

impl From<VerifiedModelScoreCalibration> for ResolvedCalibration {
    fn from(verified: VerifiedModelScoreCalibration) -> Self {
        Self {
            artifact_id: verified.artifact_id,
            mapping: verified.payload.mapping,
            reliability: verified.payload.reliability,
        }
    }
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
        let verified = VerifiedModelScoreCalibration::try_from(&info)?;
        // Fail-closed governance: a calibrator that was never bound/activated
        // (or was superseded — `active` lifecycle) must never
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
        Ok(ResolvedCalibration::from(verified))
    }
}

fn validate_model_score_payload(
    payload: &ModelScoreCalibrationPayload,
    persisted_sample_count: i64,
) -> QuantResult<()> {
    let persisted_sample_count =
        u64::try_from(persisted_sample_count).map_err(|error| ResearchError::DatasetBuild {
            detail: format!("calibration artifact sample_count is invalid: {error}"),
        })?;
    payload.validate(persisted_sample_count).map_err(|detail| {
        ResearchError::DatasetBuild {
            detail: format!("model-score calibration payload is invalid: {detail}"),
        }
        .into()
    })
}

#[cfg(test)]
mod tests {
    use std::sync::{Arc, Mutex};

    use chrono::{DateTime, Duration, Utc};
    use quant_pivot_error::storage::StorageError;
    use quant_pivot_models::{
        domain::{
            api::CalibrationArtifactListQuery,
            pagination::Paginated,
            quant::{
                CalibrationArtifactInfo, ModelScoreCalibrationCommit,
                ModelScoreCalibrationCommitOutcome, NewCalibrationArtifact,
            },
        },
        enums::model::ModelFamily,
        types::{
            DecisionPolicySnapshotId, ModelSpecId, ModelVersionId, Probability, TrainingDatasetId,
            builtin_research_profiles,
            calibration::{
                IsotonicKnot, MODEL_SCORE_CALIBRATION_FORMAT_VERSION,
                ModelScoreCalibrationDatasetBinding, ModelScoreCalibrationFitContract,
                ModelScoreCalibrationModelBinding, ModelScoreCalibrationPayload,
                ModelScoreCalibrationPolicyBinding, MonotoneMapping,
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

        async fn commit_model_score(
            &self,
            _commit: ModelScoreCalibrationCommit,
        ) -> Result<ModelScoreCalibrationCommitOutcome, StorageError> {
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
            _at: DateTime<Utc>,
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
        ContentHash::parse(&format!("blake3:{hex}")).expect("hash")
    }

    fn valid_row(active: bool) -> CalibrationArtifactInfo {
        let fit_window = TimeWindow::new(
            Utc::now() - Duration::days(30),
            Utc::now() - Duration::days(1),
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
            payload: CalibrationArtifactPayload::ModelScore(Box::new(payload)),
            active,
            created_at: Utc::now(),
        }
    }

    fn dummy_payload() -> ModelScoreCalibrationPayload {
        let profile = builtin_research_profiles()
            .expect("builtin profiles")
            .into_iter()
            .next()
            .expect("builtin profile");
        let training_dataset_id = TrainingDatasetId::from_v7();
        let snapshot_hash = fake_content_hash(11);
        ModelScoreCalibrationPayload {
            format_version: MODEL_SCORE_CALIBRATION_FORMAT_VERSION,
            fit_contract: ModelScoreCalibrationFitContract {
                model: ModelScoreCalibrationModelBinding {
                    model_version_id: ModelVersionId::from_v7(),
                    artifact_hash: fake_content_hash(2),
                    serving_contract_hash: fake_content_hash(3),
                    model_spec_id: ModelSpecId::from_v7(),
                    model_spec_definition_hash: fake_content_hash(4),
                    model_family: ModelFamily::WeightedFactor,
                    profile_ref: profile.profile_ref.clone(),
                    category_scope: profile.spec.category,
                    prediction_horizon_secs: profile.spec.target_horizon_secs,
                    training_dataset_id,
                    training_dataset_hash: fake_content_hash(5),
                },
                calibration_dataset: ModelScoreCalibrationDatasetBinding {
                    calibration_dataset_id: TrainingDatasetId::from_v7(),
                    dataset_hash: fake_content_hash(6),
                    manifest_hash: fake_content_hash(7),
                    artifact_bytes_hash: fake_content_hash(8),
                    source_slice_manifest_hash: fake_content_hash(9),
                    feature_schema_hash: fake_content_hash(10),
                    factor_schema_hash: fake_content_hash(12),
                    label_schema_hash: fake_content_hash(13),
                },
                policy_snapshot: ModelScoreCalibrationPolicyBinding {
                    decision_policy_snapshot_id: DecisionPolicySnapshotId::from_content_hash(
                        &snapshot_hash,
                    ),
                    snapshot_hash,
                },
            },
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
    async fn load_rejects_artifact_missing() {
        let loader = loader(None);
        let result = loader.load(&CalibrationArtifactId::from_v7()).await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn load_rejects_wrong_kind() {
        let mut row = valid_row(true);
        row.kind = CalibrationKind::MarketPriceBias;
        let artifact_id = row.artifact_id;
        let loader = loader(Some(row));
        let result = loader.load(&artifact_id).await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn load_rejects_inactive_artifact() {
        let row = valid_row(false);
        let artifact_id = row.artifact_id;
        let loader = loader(Some(row));
        let result = loader.load(&artifact_id).await;
        assert!(
            result.is_err(),
            "an inactive (never-activated or superseded) calibrator must never resolve"
        );
    }

    #[tokio::test]
    async fn load_rejects_empty_mapping() {
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
        let artifact_id = row.artifact_id;
        assert!(loader(Some(row)).load(&artifact_id).await.is_err());
    }

    #[tokio::test]
    async fn load_rejects_content_mismatch() {
        let mut row = valid_row(true);
        // Tamper with the payload after the hash was computed.
        let CalibrationArtifactPayload::ModelScore(mut payload) = row.payload else {
            panic!("model-score payload")
        };
        payload.reliability.ece = dec!(0.99);
        row.payload = CalibrationArtifactPayload::ModelScore(payload);
        let artifact_id = row.artifact_id;
        let loader = loader(Some(row));
        let result = loader.load(&artifact_id).await;
        assert!(
            result.is_err(),
            "a tampered payload must fail content-hash reverification"
        );
    }

    #[tokio::test]
    async fn load_succeeds_active_artifact() {
        let row = valid_row(true);
        let artifact_id = row.artifact_id;
        let loader = loader(Some(row));
        let resolved = loader.load(&artifact_id).await.expect("resolve");
        assert_eq!(resolved.artifact_id, artifact_id);
    }
}

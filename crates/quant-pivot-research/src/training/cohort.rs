//! Canonical immutable feedback-cohort artifact codec.

use quant_pivot_error::{QuantResult, research::ResearchError};
use quant_pivot_models::{
    hashing::CanonicalDigest,
    types::{ContentHash, ModelLearningCohortArtifact, ModelScoreCohortArtifact},
};

/// Strict canonical-JSON codec for included `ModelLearning` rows.
pub struct ModelLearningCohortCodec;

impl ModelLearningCohortCodec {
    pub fn encode(artifact: &ModelLearningCohortArtifact) -> QuantResult<Vec<u8>> {
        artifact
            .validate()
            .map_err(|error| ResearchError::Serialization {
                detail: format!("invalid model-learning cohort artifact: {error}"),
            })?;
        CanonicalDigest::canonical_json_bytes(artifact).map_err(Into::into)
    }

    pub fn decode(bytes: &[u8]) -> QuantResult<ModelLearningCohortArtifact> {
        let artifact =
            serde_json::from_slice::<ModelLearningCohortArtifact>(bytes).map_err(|error| {
                ResearchError::Serialization {
                    detail: format!("decode model-learning cohort artifact: {error}"),
                }
            })?;
        artifact
            .validate()
            .map_err(|error| ResearchError::Serialization {
                detail: format!("invalid model-learning cohort artifact: {error}"),
            })?;
        let canonical = Self::encode(&artifact)?;
        if canonical != bytes {
            return Err(ResearchError::Serialization {
                detail: "model-learning cohort artifact is not canonical JSON".to_owned(),
            }
            .into());
        }
        Ok(artifact)
    }

    pub fn bytes_hash(bytes: &[u8]) -> ContentHash {
        CanonicalDigest::content_hash_bytes(bytes)
    }

    pub fn schema_hash() -> QuantResult<ContentHash> {
        ModelLearningCohortArtifact::schema_hash()
            .map_err(|error| ResearchError::Serialization {
                detail: format!("derive model-learning cohort schema hash: {error}"),
            })
            .map_err(Into::into)
    }
}

/// Strict canonical-JSON codec for complete scored-serving rows.
pub struct ModelScoreCohortCodec;

impl ModelScoreCohortCodec {
    pub fn encode(artifact: &ModelScoreCohortArtifact) -> QuantResult<Vec<u8>> {
        artifact
            .validate()
            .map_err(|error| ResearchError::Serialization {
                detail: format!("invalid model-score cohort artifact: {error}"),
            })?;
        CanonicalDigest::canonical_json_bytes(artifact).map_err(Into::into)
    }

    pub fn decode(bytes: &[u8]) -> QuantResult<ModelScoreCohortArtifact> {
        let artifact =
            serde_json::from_slice::<ModelScoreCohortArtifact>(bytes).map_err(|error| {
                ResearchError::Serialization {
                    detail: format!("decode model-score cohort artifact: {error}"),
                }
            })?;
        artifact
            .validate()
            .map_err(|error| ResearchError::Serialization {
                detail: format!("invalid model-score cohort artifact: {error}"),
            })?;
        let canonical = Self::encode(&artifact)?;
        if canonical != bytes {
            return Err(ResearchError::Serialization {
                detail: "model-score cohort artifact is not canonical JSON".to_owned(),
            }
            .into());
        }
        Ok(artifact)
    }

    pub fn bytes_hash(bytes: &[u8]) -> ContentHash {
        CanonicalDigest::content_hash_bytes(bytes)
    }

    pub fn schema_hash() -> QuantResult<ContentHash> {
        ModelScoreCohortArtifact::schema_hash()
            .map_err(|error| ResearchError::Serialization {
                detail: format!("derive model-score cohort schema hash: {error}"),
            })
            .map_err(Into::into)
    }
}

#[cfg(test)]
mod tests {
    use chrono::{DateTime, Duration, TimeZone, Utc};
    use quant_pivot_models::{
        domain::quant::{FeedbackCohortWindow, FeedbackResolutionEvidence},
        enums::{
            common::MarketCategory,
            quant::{
                CohortCensorReason, CohortExclusionReason, OutcomeSide,
                RecommendationResolutionKind,
            },
        },
        types::{
            BookSnapshotRef, BookSnapshotSource, CohortCensorCount, CohortExclusionCount,
            ContentHash, DatasetCohortCounts, DecisionPolicySnapshotId, EventId, FeatureVectorId,
            MODEL_LEARNING_COHORT_FORMAT_VERSION, MarketId, MarketSelectionId,
            ModelLearningCohortArtifact, ModelLearningCohortRow, ModelRunId, ModelVersionId,
            NewModelLearningCohortRow, PayoutRatio, RecommendationId, RecommendationReportId,
            ReportDataQualitySnapshotId, TokenId, builtin_research_profiles,
        },
    };
    use rust_decimal_macros::dec;
    use uuid::Uuid;

    use super::ModelLearningCohortCodec;

    fn instant(seconds: i64) -> DateTime<Utc> {
        Utc.timestamp_opt(seconds, 0).single().expect("instant")
    }

    fn cohort_window() -> FeedbackCohortWindow {
        let profile_ref = builtin_research_profiles()
            .expect("built-in profiles")
            .into_iter()
            .next()
            .expect("profile")
            .profile_ref;
        FeedbackCohortWindow::try_new(profile_ref, instant(1_000), instant(4_000))
            .expect("cohort window")
    }

    fn cohort_row(
        seed: u128,
        outcome_side: OutcomeSide,
        recommendation_payout: PayoutRatio,
        model_payout: PayoutRatio,
    ) -> ModelLearningCohortRow {
        let decision_at = instant(1_100 + i64::try_from(seed).expect("seed"));
        let recommendation_token_id = TokenId::new(format!("recommendation-token-{seed}"));
        let model_token_id = match outcome_side {
            OutcomeSide::Yes => recommendation_token_id.clone(),
            OutcomeSide::No => TokenId::new(format!("model-token-{seed}")),
        };
        let source_hash = ContentHash::from_bytes([u8::try_from(seed).expect("byte seed"); 32]);
        ModelLearningCohortRow::try_seal(NewModelLearningCohortRow {
            recommendation_id: RecommendationId::new(Uuid::from_u128(seed)),
            recommendation_report_id: RecommendationReportId::new(Uuid::from_u128(100 + seed)),
            category: MarketCategory::Crypto,
            market_id: MarketId::new(format!("market-{seed}")),
            event_id: EventId::new(format!("event-{seed}")),
            recommendation_token_id: recommendation_token_id.clone(),
            model_token_id,
            outcome_side,
            decision_at,
            candidate_available_at: decision_at + Duration::seconds(1),
            decision_policy_snapshot_id: DecisionPolicySnapshotId::new(Uuid::from_u128(200 + seed)),
            market_selection_id: MarketSelectionId::new(Uuid::from_u128(300 + seed)),
            feature_vector_id: FeatureVectorId::new(Uuid::from_u128(400 + seed)),
            model_run_id: ModelRunId::new(Uuid::from_u128(500 + seed)),
            model_version_id: ModelVersionId::new(Uuid::from_u128(600 + seed)),
            factor_definition_versions: Vec::new(),
            book_snapshot_ref: BookSnapshotRef {
                token_id: recommendation_token_id,
                source: BookSnapshotSource::CanonicalL2 {
                    stream_session_id: Uuid::from_u128(700 + seed),
                    token_sequence: u64::try_from(seed).expect("sequence"),
                    source_event_hash: source_hash,
                    event_time_ms: decision_at.timestamp_millis(),
                    ingestion_time_ms: decision_at.timestamp_millis(),
                },
                content_hash: source_hash,
            },
            data_quality_snapshot_id: ReportDataQualitySnapshotId::new(Uuid::from_u128(800 + seed)),
            resolution: FeedbackResolutionEvidence {
                resolution_kind: RecommendationResolutionKind::SplitPayout,
                token_payout_ratio: recommendation_payout,
                resolved_at: decision_at + Duration::minutes(5),
                available_at: decision_at + Duration::minutes(6),
                outcome_hash: source_hash,
            },
            model_token_payout_ratio: model_payout,
        })
        .expect("valid model-learning row")
    }

    fn cohort_artifact() -> ModelLearningCohortArtifact {
        ModelLearningCohortArtifact {
            format_version: MODEL_LEARNING_COHORT_FORMAT_VERSION,
            window: cohort_window(),
            counts: DatasetCohortCounts::try_new(
                4,
                2,
                2,
                vec![CohortExclusionCount {
                    reason: CohortExclusionReason::RecommendationNotPublished,
                    count: 1,
                }],
                vec![CohortCensorCount {
                    reason: CohortCensorReason::ResolutionUnavailableAtCutoff,
                    count: 1,
                }],
            )
            .expect("reconciled counts"),
            rows: vec![
                cohort_row(
                    1,
                    OutcomeSide::Yes,
                    PayoutRatio::try_new(dec!(0.5)).expect("split ratio"),
                    PayoutRatio::try_new(dec!(0.5)).expect("split ratio"),
                ),
                cohort_row(
                    2,
                    OutcomeSide::No,
                    PayoutRatio::try_new(dec!(0.25)).expect("split ratio"),
                    PayoutRatio::try_new(dec!(0.75)).expect("split complement"),
                ),
            ],
        }
    }

    #[test]
    fn canonical_roundtrip_rejects_tamper() {
        let artifact = cohort_artifact();
        let bytes = ModelLearningCohortCodec::encode(&artifact).expect("encode cohort");
        assert_eq!(
            ModelLearningCohortCodec::decode(&bytes).expect("decode cohort"),
            artifact
        );
        assert_eq!(
            ModelLearningCohortCodec::bytes_hash(&bytes),
            ModelLearningCohortCodec::bytes_hash(
                &ModelLearningCohortCodec::encode(&artifact).expect("re-encode cohort")
            )
        );
        assert_eq!(
            ModelLearningCohortCodec::schema_hash().expect("schema hash"),
            ModelLearningCohortCodec::schema_hash().expect("stable schema hash")
        );

        let mut tampered = bytes;
        let payout = br#""model_token_payout_ratio":"0.75""#;
        let offset = tampered
            .windows(payout.len())
            .position(|window| window == payout)
            .expect("payout bytes");
        tampered[offset + payout.len() - 2] = b'6';
        assert!(ModelLearningCohortCodec::decode(&tampered).is_err());

        let mut reordered = artifact;
        reordered.rows.reverse();
        assert!(ModelLearningCohortCodec::encode(&reordered).is_err());
    }

    #[test]
    fn split_projection_is_exact() {
        let artifact = cohort_artifact();
        assert_eq!(
            artifact.rows[0].model_token_payout_ratio,
            PayoutRatio::try_new(dec!(0.5)).expect("split ratio")
        );
        assert_eq!(
            artifact.rows[1].model_token_payout_ratio,
            PayoutRatio::try_new(dec!(0.75)).expect("split complement")
        );
        assert_eq!(
            artifact.rows[1].example_id,
            cohort_artifact().rows[1].example_id,
            "immutable lineage must derive the same example id"
        );

        let mut invalid = artifact.rows[1].clone();
        invalid.model_token_payout_ratio =
            PayoutRatio::try_new(dec!(0.25)).expect("wrong projection");
        assert!(invalid.validate().is_err());
    }
}

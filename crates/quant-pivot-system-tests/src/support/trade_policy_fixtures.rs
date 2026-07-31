//! Complete immutable `TradePolicy` preimages for serving-system tests.

use std::{collections::BTreeSet, sync::Arc};

use chrono::{DateTime, Duration, Utc};
use quant_pivot_core::service::{
    research_readiness::{
        EvidenceAttestor, EvidenceScopeIdentity, ResearchReadinessEvidenceService,
        ResearchReadinessEvidenceWriter,
    },
    trade_policy_evidence::{
        TradePolicyEvidenceDurability, TradePolicyEvidenceVerifier, TradePolicyEvidenceVerifierDeps,
    },
};
use quant_pivot_error::{QuantResult, research::ResearchError, storage::StorageError};
use quant_pivot_models::{
    config::{
        ArtifactStoreDeployConfig, ArtifactStoreKind, ClickHouseConfig, EvidenceAttestationConfig,
    },
    domain::{
        api::{FitTradePolicyRequest, TradePolicyFitJobParams, TradePolicyFitSelection},
        quant::{
            ModelVersionInfo, NewModelVersion, NewResearchJob, NewTradePolicyArtifact,
            NewTradePolicyGovernanceAudit, NewTradePolicyTrialAttempt,
            ResearchReadinessEvidenceInfo, TrainingDatasetInfo,
        },
    },
    enums::{
        common::{MarketCategory, Side},
        execution::ExitReason,
        model::ModelFamily,
        quant::{
            DatasetPurpose, ExitSettlementMode, FillRequirement, OutcomeSide, RedeemPolicy,
            ResearchJobKind, ResearchJobStatus, ResearchReadinessEvidenceKind,
            TradePolicyGovernanceAction, TradePolicyStatus, TradePolicyTrialScope,
            TradePolicyTrialStatus,
        },
    },
    hashing::CanonicalDigest,
    types::{
        ArtifactUri, Bps, ContentHash, DecisionPolicySnapshotId, EntryConditionTemplate,
        EntryOrderTemplate, ExecutablePriceBasis, ExitExecutionTemplate, MarketId,
        ModelInputContract, ModelSpecId, ModelTrainingContract, ModelVersionId,
        OpportunisticExitPolicy, Price, Probability, ResearchEvaluationTrack, ResearchJobId,
        ResearchJobParams, ResearchProfileArtifact, ResearchReadinessEvidencePayload,
        ResidualSharePolicy, RoleCode, SHADOW_LATENCY_PROFILE_FORMAT_VERSION, SchemaVersion,
        ShadowLatencyProfileV1, Shares, SourceSliceManifest, StructuralVolatilityOosEvidence,
        StructuralVolatilityOosFoldRow, TRADE_POLICY_ARTIFACT_FORMAT_VERSION,
        TRADE_POLICY_EVIDENCE_BUNDLE_FORMAT_VERSION, TokenId, TradePolicyArtifactId,
        TradePolicyArtifactPayload, TradePolicyCandidateId, TradePolicyCandidateSpec,
        TradePolicyCandidateTrialRow, TradePolicyCohort, TradePolicyCohortDimension,
        TradePolicyCohortKey, TradePolicyCohortProvenance, TradePolicyCohortTrialRow,
        TradePolicyCpcvPathRow, TradePolicyEvidenceBundleManifest, TradePolicyEvidenceBundleRef,
        TradePolicyEvidenceFillOutcome, TradePolicyEvidenceLiquidityRole,
        TradePolicyEvidenceObjectKind, TradePolicyEvidenceObjectRef, TradePolicyExecutionEvidence,
        TradePolicyExitTemplate, TradePolicyFillEvidenceRow, TradePolicyFitContract,
        TradePolicyGovernanceAuditId, TradePolicyLatencyScenario, TradePolicyObservationCapability,
        TradePolicyObservationEligibilityRow, TradePolicyParameterSource,
        TradePolicyPitCutoffEvidence, TradePolicyStatisticalSummaryRow, TradePolicyTrialAttemptId,
        TradePolicyTrialMetrics, TradePolicyValidationEvidence, TrainingDatasetId,
        TrainingExampleId, Usd, UserId, VerticalActivationTarget, VerticalGateEvidence,
        VerticalGateKind, builtin_research_profiles, factor::FactorServingPlane,
        model_metrics::ModelVersionMetrics, model_training::ModelTrainingObjective,
    },
};
use quant_pivot_repository::{
    postgres::{
        PgModelRegistryRepository, PgPolicyRepository, PgResearchJobRepository,
        PgResearchReadinessEvidenceRepository, PgTradePolicyRepository,
    },
    traits::{
        ModelRegistryRepository, PolicyRepository, ResearchJobRepository,
        ResearchReadinessEvidenceRepository, TradePolicyRepository,
    },
};
use quant_pivot_research::{
    artifact::{ArtifactKey, ArtifactNamespace, ArtifactStore},
    execution_semantics::EXECUTION_SEMANTICS_VERSION,
    factors::FactorEngine,
    features::FeatureSchema,
    hashing::ResearchHasher,
    model::ReturnModelSpec,
    policy_evidence::{PolicyEvidenceParquetCodec, PolicyEvidenceRecord},
    training::POLICY_NET_RETURN_BPS,
};
use rust_decimal::Decimal;
use rust_decimal_macros::dec;
use sea_orm::DatabaseConnection;
use uuid::Uuid;

use super::{
    model_serving_fixtures::{
        ModelArtifactFixtureSeed, ModelDatasetLedgerFixture, ModelDatasetLedgerSeed,
        ModelPayloadFixture, ModelVersionFixture, SealedModelFixture,
    },
    model_spec_fixtures::{new_model_spec_fixture, weather_horizon_secs},
};

const POLICY_CANDIDATE_ID: &str = "immediate";
const POLICY_SAMPLE_COUNT: u64 = 500;
/// Canonical disposable attestation key used by fixture evidence producers and
/// every real-binary consumer launched against the same seeded database.
pub const SYSTEM_EVIDENCE_SIGNING_KEY: &str =
    "abababababababababababababababababababababababababababababababab";

/// Published policy plus the exact immutable subjects needed to verify it.
pub struct PublishedTradePolicyFixture {
    provenance: TradePolicyCohortProvenance,
    subject_model_version_id: ModelVersionId,
    source_dataset_id: TrainingDatasetId,
}

impl PublishedTradePolicyFixture {
    /// Exact production-class deployment scope used by the fixture writer and
    /// every consumer-side verifier.
    pub fn evidence_scope() -> QuantResult<EvidenceScopeIdentity> {
        EvidenceScopeIdentity::from_config(
            &ClickHouseConfig::default(),
            &ArtifactStoreDeployConfig {
                kind: ArtifactStoreKind::S3,
                bucket: "system-policy-evidence".to_owned(),
                prefix: "system/trade-policy-evidence".to_owned(),
                region: "us-east-1".to_owned(),
                endpoint: None,
                path_style: true,
                require_object_lock: true,
                require_versioning: true,
            },
        )
    }

    /// Exact signing identity used by both fixture evidence production and
    /// consumer-side verification.
    pub fn evidence_attestor() -> QuantResult<EvidenceAttestor> {
        EvidenceAttestor::from_config(&EvidenceAttestationConfig {
            signing_key: SYSTEM_EVIDENCE_SIGNING_KEY.into(),
            previous_signing_keys: Vec::new(),
        })?
        .ok_or_else(|| {
            ResearchError::ValidationMethodology {
                detail: "system policy fixture attestor is disabled".to_owned(),
            }
            .into()
        })
    }

    /// Materialize a complete Weather policy preimage and publish its immutable
    /// row through the repository state machine.
    pub async fn persist(
        db: &DatabaseConnection,
        store: &Arc<dyn ArtifactStore>,
        decision_policy_snapshot_id: DecisionPolicySnapshotId,
        scope: &str,
        training_window_start: DateTime<Utc>,
    ) -> QuantResult<Self> {
        let context = Box::pin(TradePolicyFixtureContext::load(
            db,
            store,
            decision_policy_snapshot_id,
            scope,
            training_window_start,
        ))
        .await?;
        let sealed = PolicyEvidenceFixture::persist(&context).await?;
        let blockers = sealed.payload.publication_blockers();
        if !blockers.is_empty() {
            return Err(ResearchError::ValidationMethodology {
                detail: format!("system policy fixture is not publishable: {blockers:?}"),
            }
            .into());
        }
        sealed
            .evidence_verifier
            .verify(&sealed.payload, TradePolicyEvidenceDurability::Production)
            .await?;
        let artifact_hash = ResearchHasher::canonical(&sealed.payload)?;
        let artifact_id = TradePolicyArtifactId::from_content_hash(&artifact_hash);
        let policies = PgTradePolicyRepository::new(db.clone());
        policies
            .insert(NewTradePolicyArtifact {
                artifact_id,
                content_hash: artifact_hash,
                status: TradePolicyStatus::Validated,
                source_dataset_id: context.source_dataset.training_dataset_id,
                payload_json: sealed.payload,
            })
            .await?;
        policies
            .transition(
                &artifact_id,
                TradePolicyStatus::Validated,
                TradePolicyStatus::Published,
                NewTradePolicyGovernanceAudit {
                    audit_id: TradePolicyGovernanceAuditId::from_v7(),
                    artifact_id,
                    action: TradePolicyGovernanceAction::Publish,
                    from_status: TradePolicyStatus::Validated,
                    to_status: TradePolicyStatus::Published,
                    content_hash: artifact_hash,
                    actor_id: UserId::new(Uuid::nil()),
                    reason: "publish deeply verified system-test policy preimage".to_owned(),
                },
            )
            .await?;
        Ok(Self {
            provenance: TradePolicyCohortProvenance {
                artifact_id,
                artifact_hash,
                cohort_index: 0,
                cohort_key: context.cohort_key,
            },
            subject_model_version_id: context.subject.model_version_id,
            source_dataset_id: context.source_dataset.training_dataset_id,
        })
    }

    #[must_use]
    pub const fn provenance(&self) -> &TradePolicyCohortProvenance {
        &self.provenance
    }

    #[must_use]
    pub const fn subject_model_version_id(&self) -> ModelVersionId {
        self.subject_model_version_id
    }

    #[must_use]
    pub const fn source_dataset_id(&self) -> TrainingDatasetId {
        self.source_dataset_id
    }

    #[must_use]
    pub fn target_training_contract(&self) -> ModelTrainingContract {
        ModelTrainingContract {
            target_label_name: POLICY_NET_RETURN_BPS.to_string(),
            target_label_horizon_secs: 0,
            validation_folds: 3,
            trade_policy_artifact_id: Some(self.provenance.artifact_id),
        }
    }
}

struct TradePolicyFixtureContext<'a> {
    db: &'a DatabaseConnection,
    store: &'a Arc<dyn ArtifactStore>,
    decision_policy_snapshot_id: DecisionPolicySnapshotId,
    profile: ResearchProfileArtifact,
    research_program_hash: ContentHash,
    fit_window_start: DateTime<Utc>,
    fit_window_end: DateTime<Utc>,
    embargo_secs: u64,
    subject: ModelVersionInfo,
    source_dataset: TrainingDatasetInfo,
    cohort_key: TradePolicyCohortKey,
    attestor: EvidenceAttestor,
    evidence_scope: EvidenceScopeIdentity,
    readiness: Arc<ResearchReadinessEvidenceService>,
}

impl<'a> TradePolicyFixtureContext<'a> {
    async fn load(
        db: &'a DatabaseConnection,
        store: &'a Arc<dyn ArtifactStore>,
        decision_policy_snapshot_id: DecisionPolicySnapshotId,
        scope: &str,
        training_window_start: DateTime<Utc>,
    ) -> QuantResult<Self> {
        let profile = builtin_research_profiles()
            .map_err(|detail| ResearchError::ValidationMethodology { detail })?
            .into_iter()
            .find(|profile| {
                profile.spec.category == Some(MarketCategory::Weather)
                    && profile.spec.activation_eligibility
                        == ResearchEvaluationTrack::SemiAutoCandidate
            })
            .ok_or_else(|| ResearchError::ValidationMethodology {
                detail: "built-in SemiAuto Weather ResearchProfile is missing".to_owned(),
            })?;
        let policy = PgPolicyRepository::new(db.clone())
            .load_snapshot(&decision_policy_snapshot_id)
            .await?
            .ok_or_else(|| StorageError::NotFound {
                entity: "decision_policy_snapshot",
                id: decision_policy_snapshot_id.to_string(),
            })?;
        let features = &policy.snapshot.profile_artifacts.features.definition;
        let scoring = &policy.snapshot.profile_artifacts.scoring.definition;
        let domain = &policy.snapshot.profile_artifacts.domain.definition;
        let feature_schema_hash = ResearchHasher::feature_schema(&FeatureSchema::build(features)?)?;
        let factor_plane =
            FactorEngine::for_model_scope(scoring, features, domain, profile.spec.category, None)
                .serving_plane()?
                .clone();
        let research_program_hash =
            ResearchHasher::canonical(&("system-trade-policy-program-v1", scope))?;
        let fit_span = Duration::days(i64::from(profile.spec.fit_span_days));
        let fit_span_secs = u64::try_from(fit_span.num_seconds()).map_err(|error| {
            ResearchError::ValidationMethodology {
                detail: format!("system policy fit span does not fit u64: {error}"),
            }
        })?;
        let embargo_secs = fit_span_secs
            .checked_mul(2)
            .and_then(|value| value.checked_add(99))
            .map(|value| value / 100)
            .ok_or_else(|| ResearchError::ValidationMethodology {
                detail: "system policy embargo overflows u64".to_owned(),
            })?
            .max(profile.spec.max_feature_lookback_secs);
        let horizon = Duration::seconds(i64::try_from(profile.spec.target_horizon_secs).map_err(
            |error| ResearchError::ValidationMethodology {
                detail: format!("system policy target horizon does not fit i64: {error}"),
            },
        )?);
        let embargo = Duration::seconds(i64::try_from(embargo_secs).map_err(|error| {
            ResearchError::ValidationMethodology {
                detail: format!("system policy embargo does not fit i64: {error}"),
            }
        })?);
        let fit_window_end = training_window_start
            .checked_sub_signed(embargo)
            .and_then(|value| value.checked_sub_signed(horizon))
            .ok_or_else(|| ResearchError::ValidationMethodology {
                detail: "system policy timeline underflows before model training".to_owned(),
            })?;
        let fit_window_start = fit_window_end.checked_sub_signed(fit_span).ok_or_else(|| {
            ResearchError::ValidationMethodology {
                detail: "system policy fit window underflows".to_owned(),
            }
        })?;
        let subject = Box::pin(PolicySubjectFixture::persist(PolicySubjectSeed {
            db,
            store,
            scope,
            decision_policy_snapshot_id,
            profile: &profile,
            feature_schema_hash,
            factor_plane: &factor_plane,
            research_program_hash,
            fit_window_start,
        }))
        .await?;
        let source_dataset = PolicyFitDatasetFixture::persist(PolicyFitDatasetSeed {
            db,
            store,
            scope,
            decision_policy_snapshot_id,
            profile: &profile,
            subject: &subject,
            feature_schema_hash,
            factor_plane: &factor_plane,
            research_program_hash,
            fit_window_start,
            fit_window_end,
        })
        .await?;
        let training_not_before = source_dataset
            .pit_cutoff
            .checked_add_signed(embargo)
            .ok_or_else(|| ResearchError::ValidationMethodology {
                detail: "system policy PIT cutoff plus embargo overflows".to_owned(),
            })?;
        if training_window_start < training_not_before {
            return Err(ResearchError::ValidationMethodology {
                detail: format!(
                    "system policy fixture training starts at {training_window_start} before \
                     PIT cutoff embargo ends at {training_not_before}"
                ),
            }
            .into());
        }
        let cohort_key = policy_cohort_key(&profile)?;
        let attestor = PublishedTradePolicyFixture::evidence_attestor()?;
        let readiness_repo: Arc<dyn ResearchReadinessEvidenceRepository> =
            Arc::new(PgResearchReadinessEvidenceRepository::new(db.clone()));
        let evidence_scope = PublishedTradePolicyFixture::evidence_scope()?;
        let readiness = Arc::new(ResearchReadinessEvidenceService::new(
            Arc::clone(&readiness_repo),
            Arc::clone(store),
            Some(attestor.clone()),
            &evidence_scope,
        )?);
        Ok(Self {
            db,
            store,
            decision_policy_snapshot_id,
            profile,
            research_program_hash,
            fit_window_start,
            fit_window_end,
            embargo_secs,
            subject,
            source_dataset,
            cohort_key,
            attestor,
            evidence_scope,
            readiness,
        })
    }
}

struct PolicySubjectSeed<'a> {
    db: &'a DatabaseConnection,
    store: &'a Arc<dyn ArtifactStore>,
    scope: &'a str,
    decision_policy_snapshot_id: DecisionPolicySnapshotId,
    profile: &'a ResearchProfileArtifact,
    feature_schema_hash: ContentHash,
    factor_plane: &'a FactorServingPlane,
    research_program_hash: ContentHash,
    fit_window_start: DateTime<Utc>,
}

struct PolicySubjectFixture;

impl PolicySubjectFixture {
    async fn persist(seed: PolicySubjectSeed<'_>) -> QuantResult<ModelVersionInfo> {
        let registry = PgModelRegistryRepository::new(seed.db.clone());
        let model_spec_id = ModelSpecId::from_v7();
        let input_contract = ModelInputContract::single_required("book.mid");
        let spec = new_model_spec_fixture(
            model_spec_id,
            format!("{}-policy-subject", seed.scope),
            ModelFamily::WeightedFactor,
            weather_horizon_secs(),
            input_contract.clone(),
            ModelTrainingContract::settlement_default(),
        );
        let definition_hash = spec.definition_hash;
        registry.create_model_spec(spec).await?;
        let training_window_end = seed.fit_window_start - Duration::days(1);
        let dataset = ModelDatasetLedgerFixture::persist(
            seed.db,
            seed.store,
            ModelDatasetLedgerSeed {
                scope: format!("{}-policy-subject", seed.scope),
                model_spec_id,
                model_family: ModelFamily::WeightedFactor,
                model_spec_definition_hash: definition_hash,
                factor_serving_plane: seed.factor_plane.clone(),
                feature_schema_version: SchemaVersion::FIRST,
                feature_schema_hash: seed.feature_schema_hash,
                decision_policy_snapshot_id: seed.decision_policy_snapshot_id,
                profile_ref: seed.profile.profile_ref.clone(),
                prediction_horizon_secs: seed.profile.spec.target_horizon_secs,
                purpose: DatasetPurpose::Training,
                window_start: training_window_end - Duration::days(1),
                window_end: training_window_end,
                research_program_hash: seed.research_program_hash,
                sample_count: POLICY_SAMPLE_COUNT,
                decision_interval_secs: 1,
                trade_policy: None,
            },
        )
        .await?;
        let policy = PgPolicyRepository::new(seed.db.clone())
            .load_snapshot(&seed.decision_policy_snapshot_id)
            .await?
            .ok_or_else(|| StorageError::NotFound {
                entity: "decision_policy_snapshot",
                id: seed.decision_policy_snapshot_id.to_string(),
            })?;
        let scoring = &policy.snapshot.profile_artifacts.scoring.definition;
        let model_version_id = ModelVersionId::from_v7();
        let payload = ModelPayloadFixture::weighted(
            seed.factor_plane,
            &scoring.factor_head,
            input_contract,
            ReturnModelSpec::heuristic_default(),
            scoring.cross_section.clone(),
        )?;
        let fixture = SealedModelFixture::seal(
            seed.db,
            ModelArtifactFixtureSeed {
                model_version_id,
                training_dataset_id: dataset.training_dataset_id,
                payload,
                training_input_hash: ResearchHasher::canonical(&(
                    "system-trade-policy-subject-input-v1",
                    seed.scope,
                ))?,
                category_scope: seed.profile.spec.category,
                calibration: None,
                bias_table: None,
            },
        )
        .await?;
        fixture.store(seed.store).await?;
        let contract = fixture.serving_contract().clone();
        let bindings = contract.bindings();
        let category_scope = bindings.model.category_scope;
        let profile_ref = bindings.model.profile_ref.clone();
        ModelVersionFixture::persist_route_candidate(
            seed.db,
            NewModelVersion {
                model_version_id,
                model_spec_id,
                version: 1,
                artifact_hash: fixture.artifact_hash(),
                serving_contract: contract,
                category_scope,
                profile_ref,
                training_dataset_id: Some(dataset.training_dataset_id),
                trade_policy_artifact_id: None,
                trade_policy_hash: None,
                derivation: NewModelVersion::training_derivation(),
                metrics: ModelVersionMetrics::not_measured("system policy subject"),
                training_objective: ModelTrainingObjective::hand_authored("system policy subject"),
            },
        )
        .await
    }
}

struct PolicyFitDatasetSeed<'a> {
    db: &'a DatabaseConnection,
    store: &'a Arc<dyn ArtifactStore>,
    scope: &'a str,
    decision_policy_snapshot_id: DecisionPolicySnapshotId,
    profile: &'a ResearchProfileArtifact,
    subject: &'a ModelVersionInfo,
    feature_schema_hash: ContentHash,
    factor_plane: &'a FactorServingPlane,
    research_program_hash: ContentHash,
    fit_window_start: DateTime<Utc>,
    fit_window_end: DateTime<Utc>,
}

struct PolicyFitDatasetFixture;

impl PolicyFitDatasetFixture {
    async fn persist(seed: PolicyFitDatasetSeed<'_>) -> QuantResult<TrainingDatasetInfo> {
        ModelDatasetLedgerFixture::persist(
            seed.db,
            seed.store,
            ModelDatasetLedgerSeed {
                scope: format!("{}-policy-fit", seed.scope),
                model_spec_id: seed.subject.model_spec_id,
                model_family: seed.subject.model_family,
                model_spec_definition_hash: seed.subject.model_spec_definition_hash,
                factor_serving_plane: seed.factor_plane.clone(),
                feature_schema_version: SchemaVersion::FIRST,
                feature_schema_hash: seed.feature_schema_hash,
                decision_policy_snapshot_id: seed.decision_policy_snapshot_id,
                profile_ref: seed.profile.profile_ref.clone(),
                prediction_horizon_secs: seed.profile.spec.target_horizon_secs,
                purpose: DatasetPurpose::PolicyFit,
                window_start: seed.fit_window_start,
                window_end: seed.fit_window_end,
                research_program_hash: seed.research_program_hash,
                sample_count: POLICY_SAMPLE_COUNT,
                decision_interval_secs: 1,
                trade_policy: None,
            },
        )
        .await
    }
}

struct SealedPolicyEvidence {
    payload: TradePolicyArtifactPayload,
    evidence_verifier: TradePolicyEvidenceVerifier,
}

struct PolicyEvidenceFixture;

impl PolicyEvidenceFixture {
    async fn persist(context: &TradePolicyFixtureContext<'_>) -> QuantResult<SealedPolicyEvidence> {
        let latency = Self::persist_latency(context).await?;
        let source_manifest = Self::source_manifest(context).await?;
        let methodology_hash = ResearchHasher::canonical(&"system-weather-policy-methodology-v1")?;
        let candidates = policy_candidates();
        let candidate_set_hash = ResearchHasher::canonical(&candidates)?;
        let cohort = policy_cohort(context.cohort_key.clone());
        let cohort_hash = ResearchHasher::canonical(&context.cohort_key)?;
        let evidence_objects = PolicyEvidenceObjects::persist(
            context.store,
            EvidenceRowContext {
                now: context.fit_window_end,
                cohort_hash,
                cohort: &cohort,
                candidates: &candidates,
            },
        )
        .await?;
        let fit_job_id = Self::persist_fit_job(context, &candidates).await?;
        let mut payload = Self::payload(
            context,
            &latency,
            methodology_hash,
            candidates,
            candidate_set_hash,
            cohort,
        )?;
        let experiment_family_hash = TradePolicyEvidenceVerifier::experiment_family_hash(&payload)?;
        let (trial_ledger_cutoff, trial_ledger_hash) = Self::persist_trial(
            context,
            &payload,
            &evidence_objects,
            fit_job_id,
            experiment_family_hash,
        )
        .await?;
        let simulator_hash = TradePolicyEvidenceVerifier::active_simulator_hash()?;
        let replay_kernel_hash = TradePolicyEvidenceVerifier::active_replay_hash()?;
        let catalog_ledger_hash =
            CanonicalDigest::content_hash_json(&source_manifest.catalog_proof)?;
        let manifest = TradePolicyEvidenceBundleManifest {
            format_version: TRADE_POLICY_EVIDENCE_BUNDLE_FORMAT_VERSION,
            source_dataset_hash: payload.source_dataset_hash,
            candidate_set_hash,
            simulator_hash,
            replay_kernel_hash,
            methodology_hash,
            latency_evidence_id: latency.evidence_id,
            latency_profile_hash: latency.payload_hash,
            catalog_ledger_hash,
            source_slice_manifest_hash: context
                .source_dataset
                .source_lineage
                .source_slice
                .manifest_hash,
            fit_job_id,
            trial_ledger_cutoff,
            trial_ledger_hash,
            objects: evidence_objects.objects,
        };
        let (manifest_uri, manifest_hash) =
            PolicyEvidenceObjects::persist_manifest(context.store, &manifest).await?;
        payload.evidence_bundle = Some(TradePolicyEvidenceBundleRef {
            manifest_uri,
            manifest_hash,
            simulator_hash,
            replay_kernel_hash,
            methodology_hash,
            latency_evidence_id: latency.evidence_id,
            latency_profile_hash: latency.payload_hash,
            catalog_ledger_hash,
            source_slice_manifest_hash: context
                .source_dataset
                .source_lineage
                .source_slice
                .manifest_hash,
            fit_job_id,
            trial_ledger_hash,
        });
        payload.validation.trial_ledger_cutoff = Some(trial_ledger_cutoff);
        payload.validation.trial_ledger_hash = Some(trial_ledger_hash);
        let evidence_verifier = TradePolicyEvidenceVerifier::new(TradePolicyEvidenceVerifierDeps {
            artifacts: Arc::clone(context.store),
            policies: Arc::new(PgTradePolicyRepository::new(context.db.clone())),
            readiness: Arc::clone(&context.readiness),
        });
        Ok(SealedPolicyEvidence {
            payload,
            evidence_verifier,
        })
    }

    async fn persist_latency(
        context: &TradePolicyFixtureContext<'_>,
    ) -> QuantResult<ResearchReadinessEvidenceInfo> {
        let observed_at = context.fit_window_end;
        let window_start = observed_at - Duration::hours(24);
        let payload =
            ResearchReadinessEvidencePayload::ShadowLatencyProfile(ShadowLatencyProfileV1 {
                format_version: SHADOW_LATENCY_PROFILE_FORMAT_VERSION,
                window_start,
                window_end: observed_at,
                observed_at,
                book_event_count: 10_000,
                book_age_p50_ms: 10,
                book_age_p95_ms: 30,
                book_age_p99_ms: 60,
                decision_prepared_count: 1_000,
                decision_prepared_p95_ms: Some(20),
                endpoint_rtt_count: 1_000,
                endpoint_rtt_p95_ms: Some(25),
                market_delay_count: 1_000,
                market_delay_p95_ms: Some(35),
            });
        ResearchReadinessEvidenceWriter::new(
            Arc::new(PgResearchReadinessEvidenceRepository::new(
                context.db.clone(),
            )),
            Arc::clone(context.store),
            Some(context.attestor.clone()),
            context.evidence_scope.clone(),
        )
        .persist(
            ResearchReadinessEvidenceKind::ShadowLatencyProfile,
            payload,
            window_start,
            observed_at,
            observed_at,
        )
        .await
    }

    async fn source_manifest(
        context: &TradePolicyFixtureContext<'_>,
    ) -> QuantResult<SourceSliceManifest> {
        let source = &context.source_dataset.source_lineage.source_slice;
        let bytes = context.store.get(&source.manifest_uri).await?;
        let hash = CanonicalDigest::content_hash_bytes(&bytes);
        if hash != source.manifest_hash {
            return Err(ResearchError::ValidationMethodology {
                detail: "PolicyFit Source Slice manifest byte hash drifted".to_owned(),
            }
            .into());
        }
        serde_json::from_slice(&bytes).map_err(|error| {
            ResearchError::Serialization {
                detail: format!("decode PolicyFit Source Slice manifest: {error}"),
            }
            .into()
        })
    }

    async fn persist_fit_job(
        context: &TradePolicyFixtureContext<'_>,
        candidates: &[TradePolicyCandidateSpec],
    ) -> QuantResult<ResearchJobId> {
        let fit_job_id = ResearchJobId::from_v7();
        PgResearchJobRepository::new(context.db.clone())
            .enqueue(NewResearchJob {
                job_id: fit_job_id,
                feedback_cycle_id: None,
                feedback_stage: None,
                kind: ResearchJobKind::TradePolicyFit,
                status: ResearchJobStatus::Queued,
                model_spec_id: Some(context.subject.model_spec_id),
                decision_policy_snapshot_id: Some(context.decision_policy_snapshot_id),
                params_json: ResearchJobParams::TradePolicyFit(TradePolicyFitJobParams {
                    training_dataset_id: context.source_dataset.training_dataset_id,
                    request: FitTradePolicyRequest {
                        selection: TradePolicyFitSelection {
                            profile_ref: context.profile.profile_ref.clone(),
                            pit_cutoff: context.source_dataset.pit_cutoff,
                        },
                        evaluation_track: ResearchEvaluationTrack::SemiAutoCandidate,
                        candidates: candidates.to_vec(),
                        reason: "build complete system TradePolicy preimage".to_owned(),
                        idempotency_key: fit_job_id.to_string(),
                    },
                }),
                requested_by: None,
                acting_role: RoleCode::new("system"),
                parent_job_id: None,
                recovery_attempt: 0,
                max_recovery_attempts: 3,
            })
            .await?;
        Ok(fit_job_id)
    }

    async fn persist_trial(
        context: &TradePolicyFixtureContext<'_>,
        payload: &TradePolicyArtifactPayload,
        evidence: &PolicyEvidenceObjects,
        fit_job_id: ResearchJobId,
        experiment_family_hash: ContentHash,
    ) -> QuantResult<(DateTime<Utc>, ContentHash)> {
        let object = evidence
            .objects
            .iter()
            .find(|object| object.kind == TradePolicyEvidenceObjectKind::CandidateTrials)
            .ok_or_else(|| ResearchError::ValidationMethodology {
                detail: "CandidateTrials evidence object is missing".to_owned(),
            })?;
        let candidate =
            payload
                .candidates
                .first()
                .ok_or_else(|| ResearchError::ValidationMethodology {
                    detail: "policy fixture candidate set is empty".to_owned(),
                })?;
        let mut attempt = NewTradePolicyTrialAttempt {
            trial_attempt_id: TradePolicyTrialAttemptId::from_fit_job_ordinal(&fit_job_id, 0),
            fit_job_id,
            attempt_ordinal: 0,
            experiment_family_hash,
            research_program_hash: context.research_program_hash,
            candidate_id: TradePolicyCandidateId::parse(&candidate.candidate_id).map_err(
                |error| ResearchError::ValidationMethodology {
                    detail: error.to_string(),
                },
            )?,
            candidate_hash: CanonicalDigest::content_hash_json(candidate)?,
            scope: TradePolicyTrialScope::Candidate,
            fold_index: None,
            path_index: None,
            status: TradePolicyTrialStatus::Succeeded,
            metrics_json: Some(TradePolicyTrialMetrics {
                sample_count: POLICY_SAMPLE_COUNT,
                effective_sample_size: Decimal::from(POLICY_SAMPLE_COUNT),
                net_return_bps: dec!(25),
                sharpe_ratio: Some(Decimal::ONE),
                executable_coverage: Decimal::ONE,
                full_l2_coverage: Decimal::ONE,
                fee_catalog_coverage: Decimal::ONE,
                ambiguous_touch_rate: Decimal::ZERO,
                depth_failure_rate: Decimal::ZERO,
                latency_stress_multiplier: Decimal::ONE,
            }),
            evidence_uri: Some(object.uri.clone()),
            evidence_hash: Some(object.byte_hash),
            evidence_row_count: Some(i64::try_from(object.row_count).map_err(|error| {
                ResearchError::ValidationMethodology {
                    detail: format!("policy evidence row count does not fit i64: {error}"),
                }
            })?),
            failure_detail: None,
            row_hash: ResearchHasher::canonical(&"pending-system-policy-trial-row")?,
        };
        attempt.row_hash =
            attempt
                .expected_row_hash()
                .map_err(|error| ResearchError::ValidationMethodology {
                    detail: format!("hash system policy trial row: {error}"),
                })?;
        let policies = PgTradePolicyRepository::new(context.db.clone());
        policies.append_trial_attempt(attempt).await?;
        let ledger = policies.list_trial_attempts(&fit_job_id, None).await?;
        let cutoff = ledger
            .last()
            .map(|attempt| attempt.created_at)
            .ok_or_else(|| ResearchError::ValidationMethodology {
                detail: "policy fixture trial ledger is empty".to_owned(),
            })?;
        let ledger_hash = ResearchHasher::canonical(&(
            "trade_policy_trial_ledger_v1",
            fit_job_id,
            ledger
                .iter()
                .map(|attempt| (attempt.attempt_ordinal, &attempt.row_hash))
                .collect::<Vec<_>>(),
        ))?;
        Ok((cutoff, ledger_hash))
    }

    fn payload(
        context: &TradePolicyFixtureContext<'_>,
        latency: &ResearchReadinessEvidenceInfo,
        methodology_hash: ContentHash,
        candidates: Vec<TradePolicyCandidateSpec>,
        candidate_set_hash: ContentHash,
        cohort: TradePolicyCohort,
    ) -> QuantResult<TradePolicyArtifactPayload> {
        let materialization = context.source_dataset.materialization().ok_or_else(|| {
            ResearchError::ValidationMethodology {
                detail: "PolicyFit Dataset has no complete materialization".to_owned(),
            }
        })?;
        let vertical_gate = weather_vertical_gate(context.fit_window_end, methodology_hash);
        Ok(TradePolicyArtifactPayload {
            format_version: TRADE_POLICY_ARTIFACT_FORMAT_VERSION,
            activation_target: VerticalActivationTarget::SemiAuto,
            fit_contract: TradePolicyFitContract {
                profile_ref: context.profile.profile_ref.clone(),
                evaluation_track: ResearchEvaluationTrack::SemiAutoCandidate,
                research_program_hash: context.research_program_hash,
                source_dataset_id: context.source_dataset.training_dataset_id,
                model_version_id: context.subject.model_version_id,
                decision_policy_snapshot_id: context.decision_policy_snapshot_id,
                fit_window_start: context.fit_window_start,
                fit_window_end: context.fit_window_end,
                pit_cutoff: context.source_dataset.pit_cutoff,
                target_horizon_secs: context.profile.spec.target_horizon_secs,
                cash_budget_tiers: context.profile.spec.allowed_cash_budget_tiers.clone(),
                methodology_hash,
                latency_evidence_id: latency.evidence_id,
                latency_profile_hash: latency.payload_hash,
                quality_gate: context.profile.spec.quality_gate.clone(),
            },
            source_dataset_hash: *materialization.dataset_hash,
            feature_schema_hash: *materialization.feature_schema_hash,
            label_schema_hash: *materialization.label_schema_hash,
            fill_simulator_version: EXECUTION_SEMANTICS_VERSION.to_owned(),
            embargo_secs: context.embargo_secs,
            pit_cutoff_evidence: Some(TradePolicyPitCutoffEvidence {
                filtered_sample_count: POLICY_SAMPLE_COUNT,
                labels_matured_by_cutoff: POLICY_SAMPLE_COUNT,
                labels_excluded_after_cutoff: 0,
                filtered_sample_hash: ResearchHasher::canonical(&(
                    "system-policy-filtered-sample-v1",
                    materialization.dataset_hash,
                    context.source_dataset.pit_cutoff,
                ))?,
            }),
            execution_evidence: TradePolicyExecutionEvidence {
                entry_basis: Some(ExecutablePriceBasis::FullL2Vwap),
                exit_basis: Some(ExecutablePriceBasis::FullL2Vwap),
                full_l2_sample_count: POLICY_SAMPLE_COUNT,
                full_l2_coverage: Some(Decimal::ONE),
                fee_model_hash: Some(ResearchHasher::canonical(&"system-policy-fee-model-v1")?),
                gaps: Vec::new(),
            },
            candidate_set_hash,
            candidates,
            evidence_bundle: None,
            vertical_gate_evidence: vec![vertical_gate],
            structural_volatility_oos: structural_volatility_evidence(),
            cohorts: vec![cohort],
            validation: TradePolicyValidationEvidence {
                trial_ledger_cutoff: None,
                trial_ledger_hash: None,
                attempted_candidate_count: Some(1),
                cpcv_path_count: Some(21),
                deflated_sharpe_ratio: Some(Decimal::ONE),
                probability_of_backtest_overfitting: Some(Decimal::ZERO),
                effective_sample_size: Some(Decimal::from(POLICY_SAMPLE_COUNT)),
                ambiguous_touch_rate: Some(Decimal::ZERO),
                depth_failure_rate: Some(Decimal::ZERO),
                common_candidate_support: Some(Decimal::ONE),
                fee_catalog_coverage: Some(Decimal::ONE),
                eligible_market_coverage: Some(Decimal::ONE),
            },
        })
    }
}

struct PolicyEvidenceObjects {
    objects: Vec<TradePolicyEvidenceObjectRef>,
}

struct EvidenceRowContext<'a> {
    now: DateTime<Utc>,
    cohort_hash: ContentHash,
    cohort: &'a TradePolicyCohort,
    candidates: &'a [TradePolicyCandidateSpec],
}

impl PolicyEvidenceObjects {
    const fn object_slug(kind: TradePolicyEvidenceObjectKind) -> &'static str {
        match kind {
            TradePolicyEvidenceObjectKind::ObservationEligibility => "observation-eligibility",
            TradePolicyEvidenceObjectKind::Fills => "fills",
            TradePolicyEvidenceObjectKind::CandidateTrials => "candidate-trials",
            TradePolicyEvidenceObjectKind::CohortTrials => "cohort-trials",
            TradePolicyEvidenceObjectKind::CpcvPaths => "cpcv-paths",
            TradePolicyEvidenceObjectKind::CoverageGaps => "coverage-gaps",
            TradePolicyEvidenceObjectKind::StatisticalSummaries => "statistical-summaries",
            TradePolicyEvidenceObjectKind::VerticalGates => "vertical-gates",
            TradePolicyEvidenceObjectKind::StructuralVolatilityOos => "structural-volatility-oos",
        }
    }

    async fn persist(
        store: &Arc<dyn ArtifactStore>,
        context: EvidenceRowContext<'_>,
    ) -> QuantResult<Self> {
        let mut objects = Vec::with_capacity(TradePolicyEvidenceObjectKind::REQUIRED.len());
        for kind in TradePolicyEvidenceObjectKind::REQUIRED {
            let records = evidence_records(kind, &context)?;
            let bytes = PolicyEvidenceParquetCodec::encode(&records)?;
            let byte_hash = CanonicalDigest::content_hash_bytes(&bytes);
            let uri = store
                .put(
                    ArtifactKey::new(
                        ArtifactNamespace::PolicyEvidence,
                        format!("system-{}-{}", Self::object_slug(kind), byte_hash.hex()),
                        "parquet",
                    )?,
                    &bytes,
                )
                .await?;
            let persisted = store.get(&uri).await?;
            let decoded = PolicyEvidenceParquetCodec::decode(&persisted)?;
            if persisted != bytes || decoded != records {
                return Err(ResearchError::ValidationMethodology {
                    detail: format!("policy evidence {kind:?} changed during persistence"),
                }
                .into());
            }
            objects.push(TradePolicyEvidenceObjectRef {
                kind,
                uri,
                byte_hash,
                row_chain_hash: PolicyEvidenceParquetCodec::row_chain_hash(&decoded)?,
                row_count: u64::try_from(decoded.len()).map_err(|error| {
                    ResearchError::ValidationMethodology {
                        detail: format!("policy evidence row count overflow: {error}"),
                    }
                })?,
            });
        }
        Ok(Self { objects })
    }

    async fn persist_manifest(
        store: &Arc<dyn ArtifactStore>,
        manifest: &TradePolicyEvidenceBundleManifest,
    ) -> QuantResult<(ArtifactUri, ContentHash)> {
        manifest
            .validate()
            .map_err(|detail| ResearchError::ValidationMethodology { detail })?;
        let bytes = serde_json::to_vec(manifest).map_err(|error| ResearchError::Serialization {
            detail: format!("serialize policy evidence manifest: {error}"),
        })?;
        let hash = CanonicalDigest::content_hash_bytes(&bytes);
        let uri = store
            .put(
                ArtifactKey::new(
                    ArtifactNamespace::PolicyEvidence,
                    format!("system-manifest-{}", hash.hex()),
                    "json",
                )?,
                &bytes,
            )
            .await?;
        if store.get(&uri).await? != bytes {
            return Err(ResearchError::ValidationMethodology {
                detail: "policy evidence manifest changed during persistence".to_owned(),
            }
            .into());
        }
        Ok((uri, hash))
    }
}

fn evidence_records(
    kind: TradePolicyEvidenceObjectKind,
    context: &EvidenceRowContext<'_>,
) -> QuantResult<Vec<PolicyEvidenceRecord>> {
    let candidate =
        context
            .candidates
            .first()
            .ok_or_else(|| ResearchError::ValidationMethodology {
                detail: "policy evidence candidate set is empty".to_owned(),
            })?;
    let subject = EvidenceSubject {
        example: TrainingExampleId::from_v7(),
        market: MarketId::new("system-policy-market"),
        token: TokenId::new("system-policy-token"),
        candidate: &candidate.candidate_id,
    };
    let record = match kind {
        TradePolicyEvidenceObjectKind::ObservationEligibility => {
            Some(observation_record(context, &subject)?)
        }
        TradePolicyEvidenceObjectKind::Fills => Some(fill_record(context, &subject)?),
        TradePolicyEvidenceObjectKind::CandidateTrials => {
            Some(candidate_trial_record(context, &subject)?)
        }
        TradePolicyEvidenceObjectKind::CohortTrials => {
            Some(cohort_trial_record(context, &subject)?)
        }
        TradePolicyEvidenceObjectKind::CpcvPaths => Some(cpcv_record(context)?),
        TradePolicyEvidenceObjectKind::CoverageGaps => None,
        TradePolicyEvidenceObjectKind::StatisticalSummaries => {
            Some(summary_record(context, &subject)?)
        }
        TradePolicyEvidenceObjectKind::VerticalGates => Some(vertical_gate_record(context)?),
        TradePolicyEvidenceObjectKind::StructuralVolatilityOos => Some(volatility_record(context)?),
    };
    Ok(record.into_iter().collect())
}

struct EvidenceSubject<'a> {
    example: TrainingExampleId,
    market: MarketId,
    token: TokenId,
    candidate: &'a str,
}

fn observation_record(
    context: &EvidenceRowContext<'_>,
    subject: &EvidenceSubject<'_>,
) -> QuantResult<PolicyEvidenceRecord> {
    let capabilities = BTreeSet::from([
        TradePolicyObservationCapability::FullL2,
        TradePolicyObservationCapability::PitFeeSchedule,
        TradePolicyObservationCapability::ModelReinference,
        TradePolicyObservationCapability::WeatherLinkage,
    ]);
    let scenarios = BTreeSet::from([
        TradePolicyLatencyScenario::Base1x,
        TradePolicyLatencyScenario::Stress2x,
    ]);
    PolicyEvidenceRecord::from_typed(
        "observation-0001",
        Some(context.now),
        &TradePolicyObservationEligibilityRow {
            example_id: subject.example,
            market_id: subject.market.clone(),
            token_id: subject.token.clone(),
            decision_at: context.now,
            label_horizon_end: context.now + Duration::days(1),
            cohort_hash: context.cohort_hash,
            candidate_count: 1,
            available_capabilities: capabilities,
            common_candidate_eligible_scenarios: scenarios,
        },
    )
}

fn fill_record(
    context: &EvidenceRowContext<'_>,
    subject: &EvidenceSubject<'_>,
) -> QuantResult<PolicyEvidenceRecord> {
    PolicyEvidenceRecord::from_typed(
        "fill-0001",
        Some(context.now),
        &TradePolicyFillEvidenceRow {
            example_id: subject.example,
            cohort_hash: context.cohort_hash,
            candidate_id: subject.candidate.to_owned(),
            outcome_side: OutcomeSide::Yes,
            latency_multiplier: Decimal::ONE,
            leg_ordinal: 0,
            side: Side::Buy,
            exit_reason: None,
            triggered_at: context.now,
            filled_at: context.now,
            liquidity_role: TradePolicyEvidenceLiquidityRole::Taker,
            outcome: TradePolicyEvidenceFillOutcome::Filled,
            requested_shares: Some(Shares::new(dec!(10))),
            filled_shares: Shares::new(dec!(10)),
            vwap: Some(Price::new(dec!(0.5))),
            gross_amount: Usd::new(dec!(5)),
            fee: Usd::ZERO,
            cash_delta: dec!(-5),
            fee_schedule_hash: Some(ResearchHasher::canonical(&"system-policy-fee-schedule-v1")?),
            stream_session_id: Some(Uuid::nil()),
            token_sequence: Some(1),
            source_event_hash: Some(ResearchHasher::canonical(&"system-policy-source-event-v1")?),
        },
    )
}

fn candidate_trial_record(
    context: &EvidenceRowContext<'_>,
    subject: &EvidenceSubject<'_>,
) -> QuantResult<PolicyEvidenceRecord> {
    PolicyEvidenceRecord::from_typed(
        "candidate-trial-0001",
        Some(context.now),
        &TradePolicyCandidateTrialRow {
            example_id: subject.example,
            market_id: subject.market.clone(),
            token_id: subject.token.clone(),
            candidate_id: subject.candidate.to_owned(),
            cohort_hash: context.cohort_hash,
            outcome_side: OutcomeSide::Yes,
            latency_multiplier: Decimal::ONE,
            entry_triggered_at: Some(context.now),
            entered_at: Some(context.now),
            terminal_at: Some(context.now + Duration::hours(1)),
            terminal_reason: Some(ExitReason::TimeExit),
            entry_fill_ratio: Decimal::ONE,
            exit_fill_ratio: Decimal::ONE,
            entry_filled_shares: Shares::new(dec!(10)),
            exited_shares: Shares::new(dec!(10)),
            total_fees: Usd::ZERO,
            net_return_bps: Some(dec!(25)),
            ambiguous_touch: false,
            full_l2: true,
            fee_covered: true,
            passive_reconciled_trade_covered: None,
            gap: None,
        },
    )
}

fn cohort_trial_record(
    context: &EvidenceRowContext<'_>,
    subject: &EvidenceSubject<'_>,
) -> QuantResult<PolicyEvidenceRecord> {
    PolicyEvidenceRecord::from_typed(
        "cohort-trial-0001",
        Some(context.now),
        &TradePolicyCohortTrialRow {
            cohort: context.cohort.key.clone(),
            cohort_hash: context.cohort_hash,
            candidate_id: subject.candidate.to_owned(),
            latency_multiplier: Decimal::ONE,
            sample_count: POLICY_SAMPLE_COUNT,
            effective_sample_size: Decimal::from(POLICY_SAMPLE_COUNT),
            weighted_mean_return_bps: dec!(25),
            sharpe_ratio: Decimal::ONE,
            executable_coverage: Decimal::ONE,
            full_l2_coverage: Decimal::ONE,
            fee_catalog_coverage: Decimal::ONE,
            ambiguous_touch_rate: Decimal::ZERO,
            depth_failure_rate: Decimal::ZERO,
        },
    )
}

fn cpcv_record(context: &EvidenceRowContext<'_>) -> QuantResult<PolicyEvidenceRecord> {
    PolicyEvidenceRecord::from_typed(
        "cpcv-path-0001",
        Some(context.now),
        &TradePolicyCpcvPathRow {
            cohort_hash: context.cohort_hash,
            latency_multiplier: Decimal::ONE,
            path_index: 0,
            group_returns: vec![dec!(0.01), dec!(0.02)],
            sharpe_ratio: Decimal::ONE,
            max_drawdown: dec!(0.01),
            tail_loss: dec!(0.02),
        },
    )
}

fn summary_record(
    context: &EvidenceRowContext<'_>,
    subject: &EvidenceSubject<'_>,
) -> QuantResult<PolicyEvidenceRecord> {
    PolicyEvidenceRecord::from_typed(
        "statistical-summary-0001",
        Some(context.now),
        &TradePolicyStatisticalSummaryRow {
            cohort_hash: context.cohort_hash,
            selected_candidate_id: subject.candidate.to_owned(),
            latency_multiplier: Decimal::ONE,
            sample_count: POLICY_SAMPLE_COUNT,
            common_sample_count: POLICY_SAMPLE_COUNT,
            common_candidate_support: Decimal::ONE,
            effective_sample_size: Decimal::from(POLICY_SAMPLE_COUNT),
            cpcv_combination_count: 56,
            cpcv_path_count: 21,
            deflated_sharpe_ratio: Decimal::ONE,
            dsr_benchmark_sharpe: Decimal::ZERO,
            probability_of_backtest_overfitting: Decimal::ZERO,
            lower_confidence_utility_bps: Bps::new(dec!(2)),
            passed: true,
        },
    )
}

fn vertical_gate_record(context: &EvidenceRowContext<'_>) -> QuantResult<PolicyEvidenceRecord> {
    PolicyEvidenceRecord::from_typed(
        "vertical-gate-0001",
        Some(context.now),
        &weather_vertical_gate(
            context.now,
            ResearchHasher::canonical(&"system-weather-policy-methodology-v1")?,
        ),
    )
}

fn volatility_record(context: &EvidenceRowContext<'_>) -> QuantResult<PolicyEvidenceRecord> {
    PolicyEvidenceRecord::from_typed(
        "structural-volatility-0001",
        Some(context.now),
        &StructuralVolatilityOosFoldRow {
            fold_index: 0,
            training_window_start: context.now - Duration::days(60),
            training_window_end: context.now - Duration::days(30),
            test_window_start: context.now - Duration::days(30),
            test_window_end: context.now,
            training_sample_count: 500,
            forecast_count: 100,
            test_volume_weight: Usd::new(dec!(10_000)),
            fitted_nonnegative_k: Decimal::ONE,
            deadline_vw_interval_score: dec!(0.5),
            dr_as_vw_interval_score: dec!(0.4),
            deadline_volume_weighted_coverage: dec!(0.94),
            dr_as_volume_weighted_coverage: dec!(0.95),
        },
    )
}

fn policy_candidates() -> Vec<TradePolicyCandidateSpec> {
    vec![TradePolicyCandidateSpec {
        candidate_id: POLICY_CANDIDATE_ID.to_owned(),
        entry_condition: EntryConditionTemplate::Immediate,
        entry_execution: EntryOrderTemplate::Aggressive {
            fill_requirement: FillRequirement::AllOrNothing,
            max_slippage_bps: Bps::new(dec!(50)),
            max_book_age_ms: 2_000,
        },
        exit: policy_exit_template(),
    }]
}

fn policy_exit_template() -> TradePolicyExitTemplate {
    TradePolicyExitTemplate {
        upper_barrier_bps: Bps::new(dec!(1_000)),
        lower_barrier_bps: Bps::new(dec!(1_000)),
        vertical_barrier_secs: 3_600,
        scale_out_targets: Vec::new(),
        trailing_stop: None,
        min_score_retention: dec!(0.6),
        min_expected_return_bps: Bps::ZERO,
        require_execution_eligibility: true,
        opportunistic_exit: OpportunisticExitPolicy {
            min_confidence: Probability::new(dec!(0.65)),
            min_expected_alpha_bps: Bps::new(dec!(50)),
            min_p_exit_better: Probability::new(dec!(0.5)),
            max_cumulative_exit_pct: Decimal::ONE,
            min_incremental_exit_pct: dec!(0.1),
        },
        settlement_mode: ExitSettlementMode::HoldToResolution,
        redeem_policy: RedeemPolicy::Manual,
        reason_execution: ExitReason::ALL
            .into_iter()
            .map(|reason| ExitExecutionTemplate {
                reason,
                fill_requirement: FillRequirement::AllowPartial,
                max_attempts: 3,
                retry_cadence_ms: 1_000,
                max_slippage_bps: Bps::new(dec!(50)),
                residual_share_policy: ResidualSharePolicy::HoldToSettlement,
            })
            .collect(),
    }
}

fn policy_cohort_key(profile: &ResearchProfileArtifact) -> QuantResult<TradePolicyCohortKey> {
    let dimension = TradePolicyCohortDimension {
        methodology_id: "system-structural-volatility-v1".to_owned(),
        methodology_hash: ResearchHasher::canonical(
            &"system-structural-volatility-methodology-v1",
        )?,
        bucket_id: "all-weather".to_owned(),
    };
    Ok(TradePolicyCohortKey {
        profile_ref: profile.profile_ref.clone(),
        category: MarketCategory::Weather,
        horizon_secs: profile.spec.target_horizon_secs,
        entry_price_min: Price::new(dec!(0.01)),
        entry_price_max: Price::new(dec!(0.99)),
        cash_budget_tier: Usd::new(dec!(25)),
        liquidity: dimension.clone(),
        volatility: dimension,
    })
}

fn policy_cohort(key: TradePolicyCohortKey) -> TradePolicyCohort {
    let exit = policy_exit_template();
    TradePolicyCohort {
        key,
        selected_candidate_id: POLICY_CANDIDATE_ID.to_owned(),
        entry_condition: EntryConditionTemplate::Immediate,
        entry_order: EntryOrderTemplate::Aggressive {
            fill_requirement: FillRequirement::AllOrNothing,
            max_slippage_bps: Bps::new(dec!(50)),
            max_book_age_ms: 2_000,
        },
        max_slippage_bps: Bps::new(dec!(50)),
        max_book_age_ms: 2_000,
        upper_barrier_bps: exit.upper_barrier_bps,
        lower_barrier_bps: exit.lower_barrier_bps,
        vertical_barrier_secs: exit.vertical_barrier_secs,
        scale_out_targets: exit.scale_out_targets,
        trailing_stop: exit.trailing_stop,
        min_score_retention: exit.min_score_retention,
        min_expected_return_bps: exit.min_expected_return_bps,
        require_execution_eligibility: exit.require_execution_eligibility,
        opportunistic_exit: exit.opportunistic_exit,
        settlement_mode: exit.settlement_mode,
        redeem_policy: exit.redeem_policy,
        sample_count: 100,
        effective_sample_size: Decimal::from(100),
        executable_sample_count: 100,
        executable_coverage: Decimal::ONE,
        full_l2_coverage: Decimal::ONE,
        common_candidate_support: Decimal::ONE,
        passive_reconciled_trade_coverage: None,
        fee_catalog_coverage: Decimal::ONE,
        cpcv_path_count: 21,
        trial_count: 1,
        deflated_sharpe_ratio: Decimal::ONE,
        probability_of_backtest_overfitting: Decimal::ZERO,
        ambiguous_touch_rate: Decimal::ZERO,
        depth_failure_rate: Decimal::ZERO,
        lower_confidence_utility_bps: Some(Bps::new(dec!(2))),
        parameter_source: TradePolicyParameterSource {
            relaxed_dimensions: Vec::new(),
            source_sample_count: 100,
            source_effective_sample_size: Decimal::from(100),
            source_selector_hash: ResearchHasher::canonical(&"system-policy-source-selector-v1")
                .expect("static policy selector hash"),
        },
    }
}

fn weather_vertical_gate(
    now: DateTime<Utc>,
    methodology_hash: ContentHash,
) -> VerticalGateEvidence {
    VerticalGateEvidence {
        gate: VerticalGateKind::WeatherNoaaProxy,
        target: VerticalActivationTarget::SemiAuto,
        methodology_hash,
        evidence_window_start: now - Duration::days(31),
        evidence_window_end: now,
        sample_count: 500,
        distinct_subject_count: 20,
        distinct_local_dates: 30,
        availability: dec!(0.99),
        agreement_wilson_lower_bound: dec!(0.95),
        target_subject_sample_count: Some(20),
        target_subject_wilson_lower_bound: Some(dec!(0.90)),
        unresolved_mismatch_count: 0,
        gaps_recovered: true,
    }
}

fn structural_volatility_evidence() -> StructuralVolatilityOosEvidence {
    StructuralVolatilityOosEvidence {
        methodology_hash: ResearchHasher::canonical(&"system-structural-volatility-methodology-v1")
            .expect("static structural methodology hash"),
        active_update_only: true,
        activity_proxy: "sqrt_reconciled_hourly_volume_usd".to_owned(),
        minimum_contract_observations: 48,
        fold_count: 2,
        forecast_count: 100,
        deadline_vw_interval_score: dec!(0.5),
        dr_as_vw_interval_score: dec!(0.4),
        deadline_volume_weighted_coverage: dec!(0.94),
        dr_as_volume_weighted_coverage: dec!(0.95),
        valid: true,
    }
}

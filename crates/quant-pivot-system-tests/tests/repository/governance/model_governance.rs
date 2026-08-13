//! Shadow-comparison and model-governance audit persistence system contracts.
//! (Postgres + testcontainers).

use chrono::{DateTime, Duration as ChronoDuration, Utc};
use quant_pivot_models::{
    domain::quant::{
        ModelGovernanceAuditDetail, NewModelGovernanceAudit, NewShadowComparison,
        ShadowObservationQuery,
    },
    enums::{
        common::MarketCategory,
        model::ModelFamily,
        quant::{ModelGovernanceAction, ModelWeightSource},
    },
    types::{
        AuditEventId, CalibrationArtifactId, ContentHash, DecisionPolicySnapshotId,
        ModelGovernanceAuditId, ModelInputContract, ModelSpecId, ModelTrainingContract,
        ModelVersionId, PolicyBundleGeneration, Probability, ResearchProfileArtifactId, RoleCode,
        ShadowComparisonId,
        shadow::{ShadowRankDelta, ShadowScoreDelta},
    },
};
use quant_pivot_repository::{
    postgres::{
        PgModelGovernanceAuditRepository, PgModelRegistryRepository, PgPolicyRepository,
        PgShadowComparisonRepository,
    },
    traits::{
        ModelGovernanceAuditRepository, ModelRegistryRepository, PolicyRepository,
        ShadowComparisonRepository, ShadowComparisonWriteOutcome,
    },
};
use quant_pivot_system_tests::{
    postgres::setup_pg,
    support::{
        model_serving_fixtures::{ModelVersionFixture, ModelVersionFixtureSeed},
        model_spec_fixtures,
    },
};
use rust_decimal_macros::dec;
use sea_orm::DatabaseConnection;

fn content_hash(seed: char) -> ContentHash {
    let pair = format!("{:02x}", seed as u32);
    let hex: String = pair.chars().cycle().take(64).collect();
    ContentHash::parse(&format!("blake3:{hex}")).expect("hash")
}

struct ShadowFixture {
    champion_model_version_id: ModelVersionId,
    candidate_model_version_id: ModelVersionId,
    champion_serving_contract_hash: ContentHash,
    candidate_serving_contract_hash: ContentHash,
    research_profile_artifact_id: ResearchProfileArtifactId,
    category_scope: Option<MarketCategory>,
    decision_policy_snapshot_id: DecisionPolicySnapshotId,
    decision_policy_snapshot_hash: ContentHash,
    policy_bundle_generation: PolicyBundleGeneration,
}

impl ShadowFixture {
    fn comparison(
        &self,
        decision_at: DateTime<Utc>,
        topn_decision_overlap: Probability,
        hard_divergence: bool,
        weight_source: ModelWeightSource,
        hash_seed: char,
    ) -> NewShadowComparison {
        NewShadowComparison {
            shadow_comparison_id: ShadowComparisonId::from_v7(),
            champion_model_version_id: self.champion_model_version_id,
            candidate_model_version_id: self.candidate_model_version_id,
            champion_serving_contract_hash: self.champion_serving_contract_hash,
            candidate_serving_contract_hash: self.candidate_serving_contract_hash,
            research_profile_artifact_id: self.research_profile_artifact_id.clone(),
            category_scope: self.category_scope,
            decision_policy_snapshot_id: self.decision_policy_snapshot_id,
            decision_policy_snapshot_hash: self.decision_policy_snapshot_hash,
            policy_bundle_generation: self.policy_bundle_generation,
            weight_source,
            decision_at,
            topn_decision_overlap,
            rank_delta_json: ShadowRankDelta {
                mean_abs_rank_delta: dec!(1),
                max_rank_delta: 2,
                spearman: dec!(0.8),
                common_markets: 10,
            },
            score_delta_json: ShadowScoreDelta {
                mean_abs_score_delta: dec!(0.05),
                max_score_delta: dec!(0.1),
                side_disagreement_rate: dec!(0.2),
            },
            matured_outcome_json: None,
            hard_divergence,
            comparison_hash: content_hash(hash_seed),
        }
    }

    fn query(
        &self,
        window_start: DateTime<Utc>,
        window_end: DateTime<Utc>,
    ) -> ShadowObservationQuery {
        ShadowObservationQuery {
            champion_model_version_id: self.champion_model_version_id,
            candidate_model_version_id: self.candidate_model_version_id,
            champion_serving_contract_hash: self.champion_serving_contract_hash,
            candidate_serving_contract_hash: self.candidate_serving_contract_hash,
            research_profile_artifact_id: self.research_profile_artifact_id.clone(),
            category_scope: self.category_scope,
            decision_policy_snapshot_id: self.decision_policy_snapshot_id,
            decision_policy_snapshot_hash: self.decision_policy_snapshot_hash,
            policy_bundle_generation: self.policy_bundle_generation,
            window_start,
            window_end,
        }
    }
}

async fn seed_two_versions(db: &DatabaseConnection) -> ShadowFixture {
    let registry = PgModelRegistryRepository::new(db.clone());
    let model_spec_id = ModelSpecId::from_v7();
    registry
        .create_model_spec(model_spec_fixtures::new_model_spec_fixture(
            model_spec_id,
            "pg-governance-it",
            ModelFamily::WeightedFactor,
            model_spec_fixtures::pooled_horizon_secs(),
            ModelInputContract::single_required("book.mid"),
            ModelTrainingContract::outcome_default(),
        ))
        .await
        .expect("model spec");

    let mut versions = Vec::new();
    for seed in ['a', 'b'] {
        let id = ModelVersionId::from_v7();
        let version = ModelVersionFixture::prepare(
            db,
            ModelVersionFixtureSeed::training(
                format!("model-governance:{id}"),
                id,
                model_spec_id,
                content_hash(seed),
            ),
        )
        .await
        .expect("prepare exact model version");
        let version = registry
            .create_model_version(version)
            .await
            .expect("model version");
        versions.push(version);
    }
    let shadow = versions.pop().expect("shadow version");
    let active = versions.pop().expect("active version");
    let active_bindings = active
        .verified_serving_contract()
        .expect("active serving contract")
        .bindings();
    let shadow_bindings = shadow
        .verified_serving_contract()
        .expect("shadow serving contract")
        .bindings();
    assert_eq!(
        active_bindings.policy_snapshot, shadow_bindings.policy_snapshot,
        "shadow subjects must share one policy snapshot"
    );
    assert_eq!(
        active.profile_ref, shadow.profile_ref,
        "shadow subjects must share one ResearchProfile"
    );
    assert_ne!(
        active.serving_contract_hash, shadow.serving_contract_hash,
        "shadow contracts must be distinct"
    );
    let bundle = PgPolicyRepository::new(db.clone())
        .load_current_bundle()
        .await
        .expect("load current policy bundle")
        .expect("current policy bundle");
    assert_eq!(
        bundle.decision_policy_snapshot_id,
        active_bindings.policy_snapshot.decision_policy_snapshot_id
    );
    assert_eq!(
        bundle.snapshot_hash,
        active_bindings.policy_snapshot.snapshot_hash
    );
    ShadowFixture {
        champion_model_version_id: active.model_version_id,
        candidate_model_version_id: shadow.model_version_id,
        champion_serving_contract_hash: active.serving_contract_hash,
        candidate_serving_contract_hash: shadow.serving_contract_hash,
        research_profile_artifact_id: active.profile_ref.artifact_id(),
        category_scope: active.category_scope,
        decision_policy_snapshot_id: bundle.decision_policy_snapshot_id,
        decision_policy_snapshot_hash: bundle.snapshot_hash,
        policy_bundle_generation: bundle.generation,
    }
}

pub async fn quant_shadow_comparison_crud() {
    let (pool, _container) = setup_pg().await;
    let db = pool.connection().clone();
    let fixture = seed_two_versions(&db).await;
    let repo = PgShadowComparisonRepository::new(db.clone());

    let now = Utc::now();
    for (overlap, hard, hours_ago, weight_source, hash_seed) in [
        (dec!(0.80), false, 23_i64, ModelWeightSource::Artifact, 'g'),
        (dec!(0.60), true, 1, ModelWeightSource::Artifact, 'h'),
        (dec!(1), false, 0, ModelWeightSource::Artifact, 'i'),
    ] {
        repo.create(fixture.comparison(
            now - ChronoDuration::hours(hours_ago),
            Probability::new(overlap),
            hard,
            weight_source,
            hash_seed,
        ))
        .await
        .expect("create comparison");
    }

    let replay = repo
        .create(fixture.comparison(
            now - ChronoDuration::hours(23),
            Probability::new(dec!(0.80)),
            false,
            ModelWeightSource::Artifact,
            'g',
        ))
        .await
        .expect("replay exact comparison");
    assert!(matches!(
        replay,
        ShadowComparisonWriteOutcome::AlreadyPresent(_)
    ));
    assert!(
        repo.create(fixture.comparison(
            now - ChronoDuration::hours(23),
            Probability::new(dec!(0.81)),
            false,
            ModelWeightSource::Artifact,
            'g',
        ))
        .await
        .is_err(),
        "a reused content hash with different immutable content must fail closed"
    );

    let summary = repo
        .summary(
            &fixture.candidate_model_version_id,
            now - ChronoDuration::days(1),
        )
        .await
        .expect("summary");
    assert_eq!(summary.sample_count, 3);
    assert!(
        summary.any_hard_divergence,
        "the window includes a divergence"
    );
    // Mean of 0.80, 0.60, and 1.00.
    assert_eq!(
        summary.mean_topn_decision_overlap,
        Probability::new(dec!(0.80))
    );
    assert!(summary.window_start.is_some() && summary.window_end.is_some());
}

pub async fn shadow_window_is_exact() {
    let (pool, _container) = setup_pg().await;
    let db = pool.connection().clone();
    let fixture = seed_two_versions(&db).await;
    let repo = PgShadowComparisonRepository::new(db);
    let now = Utc::now();
    let window_start = now - ChronoDuration::minutes(5);
    let window_end = now + ChronoDuration::minutes(5);

    for (decision_at, overlap, hard, seed) in [
        (window_start, dec!(0.80), false, 'j'),
        (now, dec!(0.60), true, 'k'),
        (window_end, dec!(0.10), true, 'l'),
    ] {
        repo.create(fixture.comparison(
            decision_at,
            Probability::new(overlap),
            hard,
            ModelWeightSource::Artifact,
            seed,
        ))
        .await
        .expect("insert exact-window fixture row");
    }

    let mut wrong_contract = fixture.comparison(
        now,
        Probability::new(dec!(0.10)),
        true,
        ModelWeightSource::Artifact,
        'm',
    );
    wrong_contract.champion_serving_contract_hash = content_hash('1');
    repo.create(wrong_contract)
        .await
        .expect("insert mismatched-contract row");

    let mut wrong_policy = fixture.comparison(
        now,
        Probability::new(dec!(0.10)),
        true,
        ModelWeightSource::Artifact,
        'n',
    );
    wrong_policy.decision_policy_snapshot_hash = content_hash('2');
    repo.create(wrong_policy)
        .await
        .expect("insert mismatched-policy row");

    let mut wrong_generation = fixture.comparison(
        now,
        Probability::new(dec!(0.10)),
        true,
        ModelWeightSource::Artifact,
        'o',
    );
    wrong_generation.policy_bundle_generation = fixture
        .policy_bundle_generation
        .checked_next()
        .expect("next policy generation");
    repo.create(wrong_generation)
        .await
        .expect("insert mismatched-generation row");

    let mut wrong_category = fixture.comparison(
        now,
        Probability::new(dec!(0.10)),
        true,
        ModelWeightSource::Artifact,
        'p',
    );
    wrong_category.category_scope = match fixture.category_scope {
        Some(_) => None,
        None => Some(MarketCategory::Crypto),
    };
    repo.create(wrong_category)
        .await
        .expect("insert mismatched-category row");

    let observed = repo
        .observation_window(&fixture.query(window_start, window_end))
        .await
        .expect("load exact observation window");
    assert_eq!(observed.sample_count, 2);
    assert_eq!(observed.first_decision_at, Some(window_start));
    assert_eq!(observed.last_decision_at, Some(now));
    assert_eq!(
        observed.mean_topn_decision_overlap,
        Some(Probability::new(dec!(0.70)))
    );
    assert!(observed.any_hard_divergence);

    let sealed_start = now - ChronoDuration::hours(2);
    let sealed_end = now - ChronoDuration::hours(1);
    repo.create(fixture.comparison(
        sealed_start + ChronoDuration::minutes(30),
        Probability::new(dec!(0.99)),
        false,
        ModelWeightSource::Artifact,
        'r',
    ))
    .await
    .expect("insert late-backfill row");
    let sealed = repo
        .observation_window(&fixture.query(sealed_start, sealed_end))
        .await
        .expect("load sealed observation window");
    assert_eq!(sealed.sample_count, 0);
    assert_eq!(sealed.first_decision_at, None);
    assert_eq!(sealed.last_decision_at, None);
    assert_eq!(sealed.mean_topn_decision_overlap, None);
    assert!(!sealed.any_hard_divergence);
}

pub async fn quant_model_governance_crud() {
    let (pool, _container) = setup_pg().await;
    let db = pool.connection().clone();
    let fixture = seed_two_versions(&db).await;
    let repo = PgModelGovernanceAuditRepository::new(db.clone());

    let audit_id = ModelGovernanceAuditId::from_v7();
    let calibrated_version_id = ModelVersionId::from_v7();
    let created = repo
        .create(NewModelGovernanceAudit {
            audit_id,
            model_version_id: Some(fixture.champion_model_version_id),
            training_dataset_id: None,
            action: ModelGovernanceAction::SealCalibration,
            actor_user_id: None,
            actor_username: "operator".to_owned(),
            actor_role: Some(RoleCode::new("risk_manager")),
            reason: "seal calibrated child artifact".to_owned(),
            detail: ModelGovernanceAuditDetail::SealCalibration {
                source_version_id: fixture.champion_model_version_id,
                source_artifact_hash: content_hash('a'),
                calibrated_version_id,
                calibrated_artifact_hash: content_hash('g'),
                calibrator_id: CalibrationArtifactId::from_v7(),
            },
            audit_event_id: AuditEventId::from_v7(),
        })
        .await
        .expect("create audit");
    assert_eq!(created.audit_id, audit_id);
    assert_eq!(created.action, ModelGovernanceAction::SealCalibration);
    assert!(matches!(
        created.detail,
        ModelGovernanceAuditDetail::SealCalibration {
            calibrated_version_id: id,
            ..
        } if id == calibrated_version_id
    ));

    let listed = repo
        .list_by_version(&fixture.champion_model_version_id)
        .await
        .expect("list");
    assert_eq!(listed.len(), 1);
    assert_eq!(
        listed[0].actor_role.as_ref().map(RoleCode::as_str),
        Some("risk_manager")
    );
}

//! Shadow-comparison and model-governance audit persistence system contracts.
//! (Postgres + testcontainers).

use chrono::{Duration as ChronoDuration, Utc};
use quant_pivot_models::{
    domain::quant::{
        ModelGovernanceAuditDetail, NewModelGovernanceAudit, NewModelVersion, NewShadowComparison,
    },
    enums::{
        model::ModelFamily,
        quant::{ModelGovernanceAction, ModelWeightSource, PublicationStatus},
    },
    types::{
        AuditEventId, ContentHash, FeatureParityStateId, ModelGovernanceAuditId,
        ModelInputContract, ModelSpecId, ModelTrainingContract, ModelVersionId, Probability,
        RoleCode, ShadowComparisonId,
        model_metrics::ModelVersionMetrics,
        model_training::ModelTrainingObjective,
        shadow::{ShadowRankDelta, ShadowScoreDelta},
    },
};
use quant_pivot_repository::{
    postgres::{
        PgModelGovernanceAuditRepository, PgModelRegistryRepository, PgShadowComparisonRepository,
    },
    traits::{ModelGovernanceAuditRepository, ModelRegistryRepository, ShadowComparisonRepository},
};
use quant_pivot_system_tests::{
    postgres::setup_pg,
    support::{execution_pg_seed, model_spec_fixtures},
};
use rust_decimal_macros::dec;
use sea_orm::DatabaseConnection;

fn content_hash(seed: char) -> ContentHash {
    let pair = format!("{:02x}", seed as u32);
    let hex: String = pair.chars().cycle().take(64).collect();
    ContentHash::parse(&format!("blake3:{hex}")).expect("hash")
}

async fn seed_two_versions(db: &DatabaseConnection) -> (ModelVersionId, ModelVersionId) {
    let registry = PgModelRegistryRepository::new(db.clone());
    let model_spec_id = ModelSpecId::from_v7();
    registry
        .create_model_spec(model_spec_fixtures::new_model_spec_fixture(
            model_spec_id,
            "pg-governance-it",
            ModelFamily::WeightedFactor,
            86_400,
            ModelInputContract::single_required("book.mid"),
            ModelTrainingContract::settlement_default(),
        ))
        .await
        .expect("model spec");

    let mut ids = Vec::new();
    for (index, seed) in [('a', 1), ('b', 2)].into_iter().enumerate() {
        let id = ModelVersionId::from_v7();
        registry
            .create_model_version(NewModelVersion {
                model_version_id: id,
                model_spec_id,
                version: i32::try_from(index + 1).unwrap_or(1),
                artifact_hash: content_hash(seed.0),
                category_scope: None,
                profile_ref: execution_pg_seed::fixture_profile_ref(),
                training_dataset_id: None,
                trade_policy_artifact_id: None,
                trade_policy_hash: None,
                publish_path_set_id: None,
                derivation: NewModelVersion::training_derivation(),
                metrics: ModelVersionMetrics::not_measured("test fixture"),
                training_objective: ModelTrainingObjective::hand_authored("test fixture"),
                quality_gate_report: None,
                publication_status: PublicationStatus::Candidate,
                published_at: None,
                retired_at: None,
            })
            .await
            .expect("model version");
        ids.push(id);
    }
    (ids[0], ids[1])
}

pub async fn quant_shadow_comparison_crud() {
    let (pool, _container) = setup_pg().await;
    let db = pool.connection().clone();
    let (active, shadow) = seed_two_versions(&db).await;
    let repo = PgShadowComparisonRepository::new(db.clone());

    let now = Utc::now();
    for (overlap, hard, hours_ago, weight_source, hash_seed) in [
        (dec!(0.80), false, 23_i64, ModelWeightSource::Artifact, 'g'),
        (dec!(0.60), true, 1, ModelWeightSource::Artifact, 'h'),
        (dec!(1), false, 0, ModelWeightSource::ConfigOverlay, 'i'),
    ] {
        repo.create(NewShadowComparison {
            shadow_comparison_id: ShadowComparisonId::from_v7(),
            active_model_version_id: active,
            shadow_model_version_id: shadow,
            weight_source,
            decision_at: now - ChronoDuration::hours(hours_ago),
            topn_overlap: Probability::new(overlap),
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
            hard_divergence: hard,
            comparison_hash: content_hash(hash_seed),
        })
        .await
        .expect("create comparison");
    }

    let summary = repo
        .summary(&shadow, now - ChronoDuration::days(1))
        .await
        .expect("summary");
    assert_eq!(summary.sample_count, 2);
    // Config-overlay comparisons describe behavior that is not frozen in the
    // artifact and therefore cannot count as publication stability evidence.
    assert!(
        summary.any_hard_divergence,
        "the window includes a divergence"
    );
    // Mean of 0.80 and 0.60.
    assert_eq!(summary.mean_topn_overlap, Probability::new(dec!(0.70)));
    assert!(summary.window_start.is_some() && summary.window_end.is_some());
}

pub async fn quant_model_governance_crud() {
    let (pool, _container) = setup_pg().await;
    let db = pool.connection().clone();
    let (active, _predecessor) = seed_two_versions(&db).await;
    let repo = PgModelGovernanceAuditRepository::new(db.clone());

    let audit_id = ModelGovernanceAuditId::from_v7();
    let created = repo
        .create(NewModelGovernanceAudit {
            audit_id,
            model_version_id: Some(active),
            training_dataset_id: None,
            action: ModelGovernanceAction::Publish,
            actor_user_id: None,
            actor_username: "operator".to_owned(),
            actor_role: Some(RoleCode::new("risk_manager")),
            reason: "publish after gate pass".to_owned(),
            before_status: PublicationStatus::Candidate,
            after_status: PublicationStatus::Published,
            detail: ModelGovernanceAuditDetail::Publish {
                artifact_hash: content_hash('a'),
                gate_report_hash: content_hash('g'),
                shadow_samples: 5,
                shadow_mean_overlap: Probability::new(dec!(0.75)),
                feature_parity_state_id: FeatureParityStateId::from_v7(),
                required_shadow_window_secs: 86_400,
            },
            audit_event_id: AuditEventId::from_v7(),
        })
        .await
        .expect("create audit");
    assert_eq!(created.audit_id, audit_id);
    assert_eq!(created.action, ModelGovernanceAction::Publish);
    assert!(matches!(
        created.detail,
        ModelGovernanceAuditDetail::Publish {
            shadow_samples: 5,
            ..
        }
    ));

    let listed = repo.list_by_version(&active).await.expect("list");
    assert_eq!(listed.len(), 1);
    assert_eq!(
        listed[0].actor_role.as_ref().map(RoleCode::as_str),
        Some("risk_manager")
    );
}

//! Shadow-comparison + model-governance audit ledger integration tests
//! (Postgres + testcontainers).

use chrono::{Duration as ChronoDuration, Utc};
use quant_pivot_models::{
    domain::{NewModelGovernanceAudit, NewModelSpec, NewModelVersion, NewShadowComparison},
    enums::{
        model::ModelFamily,
        quant::{ModelGovernanceAction, PublicationStatus},
    },
    types::{
        AuditEventId, ContentHash, ModelGovernanceAuditId, ModelSpecId, ModelVersionId,
        Probability, SchemaVersion, ShadowComparisonId,
    },
};
use quant_pivot_repository::{
    postgres::{
        PgModelGovernanceAuditRepository, PgModelRegistryRepository, PgShadowComparisonRepository,
    },
    traits::{ModelGovernanceAuditRepository, ModelRegistryRepository, ShadowComparisonRepository},
};
use quant_pivot_test_support::pg::setup_pg;
use rust_decimal_macros::dec;

fn content_hash(seed: char) -> ContentHash {
    let pair = format!("{:02x}", seed as u32);
    let hex: String = pair.chars().cycle().take(64).collect();
    ContentHash::parse(format!("blake3:{hex}")).expect("hash")
}

async fn seed_two_versions(db: &sea_orm::DatabaseConnection) -> (ModelVersionId, ModelVersionId) {
    let registry = PgModelRegistryRepository::new(db.clone());
    let model_spec_id = ModelSpecId::from_v7();
    registry
        .create_model_spec(NewModelSpec {
            model_spec_id: model_spec_id.clone(),
            name: "pg-governance-it".to_owned(),
            model_family: ModelFamily::WeightedFactor,
            prediction_horizon_secs: 86_400,
            feature_schema_version: SchemaVersion::FIRST,
            label_schema_version: SchemaVersion::FIRST,
            spec_json: serde_json::json!({}),
            feature_requirements: serde_json::json!({}),
            status: PublicationStatus::Published,
        })
        .await
        .expect("model spec");

    let mut ids = Vec::new();
    for (index, seed) in [('a', 1), ('b', 2)].into_iter().enumerate() {
        let id = ModelVersionId::from_v7();
        registry
            .create_model_version(NewModelVersion {
                model_version_id: id.clone(),
                model_spec_id: model_spec_id.clone(),
                version: i32::try_from(index + 1).unwrap_or(1),
                artifact_hash: content_hash(seed.0),
                training_dataset_id: None,
                metrics_json: serde_json::json!({}),
                training_objective_json: serde_json::json!({"kind": "not_trained"}),
                quality_gate_report: serde_json::json!({}),
                publication_status: PublicationStatus::Candidate,
                published_at: None,
                retired_at: None,
            })
            .await
            .expect("model version");
        ids.push(id);
    }
    (ids[0].clone(), ids[1].clone())
}

#[tokio::test]
#[ignore = "requires Docker"]
async fn quant_shadow_comparison_migration_and_crud() {
    let (pool, _container) = setup_pg().await;
    let db = pool.connection().clone();
    let (active, shadow) = seed_two_versions(&db).await;
    let repo = PgShadowComparisonRepository::new(db.clone());

    let now = Utc::now();
    for (overlap, hard, hours_ago) in [(dec!(0.80), false, 23_i64), (dec!(0.60), true, 1)] {
        repo.create(NewShadowComparison {
            shadow_comparison_id: ShadowComparisonId::from_v7(),
            active_model_version_id: active.clone(),
            shadow_model_version_id: shadow.clone(),
            as_of: now - ChronoDuration::hours(hours_ago),
            topn_overlap: Probability::new(overlap),
            rank_delta_json: serde_json::json!({ "mean_abs_rank_delta": "1" }),
            score_delta_json: serde_json::json!({ "mean_abs_score_delta": "0.05" }),
            matured_outcome_json: None,
            hard_divergence: hard,
            comparison_hash: content_hash(if hard { 'h' } else { 'g' }),
        })
        .await
        .expect("create comparison");
    }

    let summary = repo
        .summary(&shadow, now - ChronoDuration::days(1))
        .await
        .expect("summary");
    assert_eq!(summary.sample_count, 2);
    assert!(
        summary.any_hard_divergence,
        "the window includes a divergence"
    );
    // Mean of 0.80 and 0.60.
    assert_eq!(summary.mean_topn_overlap, Probability::new(dec!(0.70)));
    assert!(summary.window_start.is_some() && summary.window_end.is_some());
}

#[tokio::test]
#[ignore = "requires Docker"]
async fn quant_model_governance_audit_migration_and_crud() {
    let (pool, _container) = setup_pg().await;
    let db = pool.connection().clone();
    let (active, predecessor) = seed_two_versions(&db).await;
    let repo = PgModelGovernanceAuditRepository::new(db.clone());

    let audit_id = ModelGovernanceAuditId::from_v7();
    let created = repo
        .create(NewModelGovernanceAudit {
            audit_id: audit_id.clone(),
            model_version_id: Some(active.clone()),
            training_dataset_id: None,
            action: ModelGovernanceAction::Publish,
            actor_username: "operator".to_owned(),
            actor_role: Some("risk_manager".to_owned()),
            reason: "publish after gate pass".to_owned(),
            before_status: PublicationStatus::Candidate,
            after_status: PublicationStatus::Published,
            before_hash: Some(content_hash('b').as_str().to_owned()),
            after_hash: Some(content_hash('a').as_str().to_owned()),
            quality_gate_passed: true,
            rollback_target_version_id: Some(predecessor.clone()),
            shadow_window_secs: Some(86_400),
            detail_json: serde_json::json!({ "shadow_samples": 5 }),
            audit_event_id: Some(AuditEventId::from_v7()),
        })
        .await
        .expect("create audit");
    assert_eq!(created.audit_id, audit_id);
    assert_eq!(created.action, ModelGovernanceAction::Publish);
    assert!(created.quality_gate_passed);

    let listed = repo.list_by_version(&active).await.expect("list");
    assert_eq!(listed.len(), 1);
    assert_eq!(listed[0].rollback_target_version_id, Some(predecessor));
    assert_eq!(listed[0].actor_role.as_deref(), Some("risk_manager"));
}

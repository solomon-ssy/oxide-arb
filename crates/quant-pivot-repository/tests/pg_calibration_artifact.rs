//! Unified calibration-artifact ledger repository integration tests
//! (Postgres + testcontainers). Covers Phase 11.3 `active` governance
//! semantics (`mark_active`) and duplicate-`content_hash` error mapping.

use chrono::Utc;
use quant_pivot_error::storage::{StorageError, entity};
use quant_pivot_models::{
    domain::NewCalibrationArtifact,
    enums::quant::CalibrationKind,
    types::{CalibrationArtifactId, ContentHash},
};
use quant_pivot_repository::{
    postgres::PgCalibrationArtifactRepository, traits::CalibrationArtifactRepository,
};
use quant_pivot_test_support::pg::setup_pg;

fn content_hash(seed: u8) -> ContentHash {
    let hex: String = format!("{seed:02x}").chars().cycle().take(64).collect();
    ContentHash::parse(format!("blake3:{hex}")).expect("hash")
}

fn new_artifact(kind: CalibrationKind, seed: u8) -> NewCalibrationArtifact {
    let now = Utc::now();
    NewCalibrationArtifact {
        artifact_id: CalibrationArtifactId::from_v7(),
        kind,
        content_hash: content_hash(seed),
        fit_window_start: now - chrono::Duration::days(30),
        fit_window_end: now,
        calibration_split_hash: content_hash(seed.wrapping_add(100)),
        sample_count: 1_000,
        payload_json: serde_json::json!({}),
        active: false,
    }
}

#[tokio::test]
#[ignore = "requires Docker"]
async fn create_duplicate_content_hash_maps_to_storage_duplicate() {
    let (pool, _container) = setup_pg().await;
    let repo = PgCalibrationArtifactRepository::new(pool.connection().clone());

    let first = new_artifact(CalibrationKind::ModelScore, 1);
    let hash = first.content_hash.clone();
    repo.create(first).await.expect("first insert");

    let dup = repo
        .create(NewCalibrationArtifact {
            artifact_id: CalibrationArtifactId::from_v7(),
            ..new_artifact(CalibrationKind::ModelScore, 1)
        })
        .await;
    assert!(
        matches!(
            &dup,
            Err(StorageError::Duplicate {
                entity: entity::QUANT_CALIBRATION_ARTIFACT,
                key,
            }) if *key == hash.to_string()
        ),
        "expected Duplicate, got {dup:?}"
    );
}

#[tokio::test]
#[ignore = "requires Docker"]
async fn mark_active_missing_artifact_is_not_found() {
    let (pool, _container) = setup_pg().await;
    let repo = PgCalibrationArtifactRepository::new(pool.connection().clone());

    let result = repo.mark_active(&CalibrationArtifactId::from_v7()).await;
    assert!(matches!(
        result,
        Err(StorageError::NotFound {
            entity: entity::QUANT_CALIBRATION_ARTIFACT,
            ..
        })
    ));
}

#[tokio::test]
#[ignore = "requires Docker"]
async fn activate_market_price_bias_deactivates_previous_active() {
    let (pool, _container) = setup_pg().await;
    let repo = PgCalibrationArtifactRepository::new(pool.connection().clone());

    let first = repo
        .create(new_artifact(CalibrationKind::MarketPriceBias, 10))
        .await
        .expect("first bias table");
    let second = repo
        .create(new_artifact(CalibrationKind::MarketPriceBias, 20))
        .await
        .expect("second bias table");

    let first = repo
        .mark_active(&first.artifact_id)
        .await
        .expect("activate first");
    assert!(first.active);

    // Activating the second must deactivate the first — a bias table is
    // referenced by exactly one global runtime-config pointer, so the ledger
    // must never carry two concurrently active `market_price_bias` rows.
    let second = repo
        .mark_active(&second.artifact_id)
        .await
        .expect("activate second");
    assert!(second.active);

    let first_reloaded = repo
        .find_by_id(&first.artifact_id)
        .await
        .expect("find first")
        .expect("first exists");
    assert!(
        !first_reloaded.active,
        "activating the second bias table must deactivate the first"
    );
}

#[tokio::test]
#[ignore = "requires Docker"]
async fn activate_model_score_does_not_deactivate_other_model_score_artifacts() {
    let (pool, _container) = setup_pg().await;
    let repo = PgCalibrationArtifactRepository::new(pool.connection().clone());

    // Two `model_score` calibrators bound to two different model versions
    // (e.g. a published version and an independently-developed candidate)
    // must both be able to stay `active` at once — no cross-model exclusivity.
    let first = repo
        .create(new_artifact(CalibrationKind::ModelScore, 30))
        .await
        .expect("first calibrator");
    let second = repo
        .create(new_artifact(CalibrationKind::ModelScore, 40))
        .await
        .expect("second calibrator");

    repo.mark_active(&first.artifact_id)
        .await
        .expect("activate first");
    repo.mark_active(&second.artifact_id)
        .await
        .expect("activate second");

    let first_reloaded = repo
        .find_by_id(&first.artifact_id)
        .await
        .expect("find first")
        .expect("first exists");
    let second_reloaded = repo
        .find_by_id(&second.artifact_id)
        .await
        .expect("find second")
        .expect("second exists");
    assert!(first_reloaded.active, "first model_score must stay active");
    assert!(second_reloaded.active, "second model_score must be active");
}

#[tokio::test]
#[ignore = "requires Docker"]
async fn activate_model_score_does_not_deactivate_active_market_price_bias() {
    let (pool, _container) = setup_pg().await;
    let repo = PgCalibrationArtifactRepository::new(pool.connection().clone());

    let bias_table = repo
        .create(new_artifact(CalibrationKind::MarketPriceBias, 50))
        .await
        .expect("bias table");
    let calibrator = repo
        .create(new_artifact(CalibrationKind::ModelScore, 60))
        .await
        .expect("model calibrator");

    repo.mark_active(&bias_table.artifact_id)
        .await
        .expect("activate bias table");
    repo.mark_active(&calibrator.artifact_id)
        .await
        .expect("activate calibrator");

    let bias_table_reloaded = repo
        .find_by_id(&bias_table.artifact_id)
        .await
        .expect("find bias table")
        .expect("bias table exists");
    assert!(
        bias_table_reloaded.active,
        "activating a model_score calibrator must not affect an unrelated market_price_bias row"
    );
}

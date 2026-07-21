//! Unified calibration-artifact ledger persistence system contracts.
//! (Postgres + testcontainers). Covers `active` governance
//! semantics (`mark_active`) and duplicate-`content_hash` error mapping.

use std::collections::BTreeMap;

use chrono::{Duration, Utc};
use quant_pivot_error::storage::{StorageError, entity};
use quant_pivot_models::{
    domain::quant::{CalibrationArtifactPayload, NewCalibrationArtifact},
    enums::quant::CalibrationKind,
    types::{
        CalibrationArtifactId, ContentHash, ModelVersionId, Probability, TrainingDatasetId,
        calibration::{
            IsotonicKnot, MarketPriceBiasPayload, ModelScoreCalibrationPayload, MonotoneMapping,
            ReliabilityBin, ReliabilityReport,
        },
    },
};
use quant_pivot_repository::{
    postgres::PgCalibrationArtifactRepository, traits::CalibrationArtifactRepository,
};
use quant_pivot_system_tests::postgres::setup_pg;
use rust_decimal::Decimal;

fn content_hash(seed: u8) -> ContentHash {
    let hex: String = format!("{seed:02x}").chars().cycle().take(64).collect();
    ContentHash::parse(format!("blake3:{hex}")).expect("hash")
}

fn new_artifact(kind: CalibrationKind, seed: u8) -> NewCalibrationArtifact {
    let now = Utc::now();
    let payload = match kind {
        CalibrationKind::ModelScore => {
            CalibrationArtifactPayload::ModelScore(ModelScoreCalibrationPayload {
                model_version_id: ModelVersionId::from_v7(),
                calibration_dataset_id: TrainingDatasetId::from_v7(),
                mapping: MonotoneMapping::Isotonic {
                    knots: vec![IsotonicKnot {
                        score: Decimal::ZERO,
                        probability: Decimal::ONE,
                    }],
                },
                reliability: ReliabilityReport {
                    bins: vec![ReliabilityBin {
                        predicted_lo: Decimal::ZERO,
                        predicted_hi: Decimal::ONE,
                        sample_count: 1_000,
                        mean_predicted: Probability::ONE,
                        empirical_frequency: Probability::ONE,
                        wilson_ci: (Probability::ONE, Probability::ONE),
                        mean_adverse_excursion_bps: None,
                    }],
                    brier_score: Decimal::ZERO,
                    log_loss: Decimal::ZERO,
                    ece: Decimal::ZERO,
                    n_samples: 1_000,
                },
            })
        }
        CalibrationKind::MarketPriceBias => {
            CalibrationArtifactPayload::MarketPriceBias(MarketPriceBiasPayload {
                by_category: BTreeMap::new(),
            })
        }
        CalibrationKind::WeatherStationLeadBias => {
            panic!("weather artifacts use the dedicated fixture")
        }
    };
    NewCalibrationArtifact {
        artifact_id: CalibrationArtifactId::from_v7(),
        kind,
        content_hash: content_hash(seed),
        fit_window_start: now - Duration::days(30),
        fit_window_end: now,
        calibration_split_hash: content_hash(seed.wrapping_add(100)),
        sample_count: 1_000,
        payload,
        active: false,
    }
}

pub async fn create_duplicate_content_hash_maps_to_storage_duplicate() {
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

pub async fn mark_active_missing_artifact_is_not_found() {
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

pub async fn activate_market_price_bias_deactivates_previous_active() {
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

pub async fn activate_model_score_does_not_deactivate_other_model_score_artifacts() {
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

pub async fn activate_model_score_does_not_deactivate_active_market_price_bias() {
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

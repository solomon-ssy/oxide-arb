//! First-boot registry bootstrap integration tests (Postgres + testcontainers).
//!
//! Proves the production write paths that seed a fresh system's research
//! registry: authoring a model spec ([`ModelSpecService`]) and registering +
//! batch-publishing the enabled factor set ([`FactorGovernanceService`]). These
//! are the steps a brand-new deployment must run before it can build a training
//! dataset, train a model, or produce a non-empty live report.

use std::sync::Arc;

use quant_pivot_core::governance::{
    FactorGovernanceDeps, FactorGovernanceService, ModelSpecDeps, ModelSpecService,
};
use quant_pivot_models::{
    domain::{
        CreateModelSpecCommand, FactorGovernancePort, GovernanceActor, ModelSpecPort,
        PublishFactorsBatchCommand, RegisterFactorDefinitionsCommand,
    },
    enums::{model::ModelFamily, quant::PublicationStatus},
    runtime_config::{FactorsConfig, FeaturesConfig},
    types::SchemaVersion,
};
use quant_pivot_repository::{
    postgres::{PgFactorRepository, PgModelRegistryRepository},
    traits::{FactorRepository, ModelRegistryRepository},
};
use quant_pivot_test_support::pg::setup_pg;

fn actor() -> GovernanceActor {
    GovernanceActor {
        username: "bootstrap-it".to_owned(),
        role: Some("risk_owner".to_owned()),
    }
}

#[tokio::test]
#[ignore = "requires Docker"]
async fn model_spec_service_authors_draft_spec() {
    let (pool, _container) = setup_pg().await;
    let registry: Arc<dyn ModelRegistryRepository> =
        Arc::new(PgModelRegistryRepository::new(pool.connection().clone()));
    let service = ModelSpecService::new(ModelSpecDeps {
        model_registry: Arc::clone(&registry),
    });

    let created = service
        .create(
            CreateModelSpecCommand {
                name: "buy-weighted-baseline".to_owned(),
                model_family: ModelFamily::WeightedFactor,
                prediction_horizon_secs: 86_400,
                feature_schema_version: SchemaVersion::FIRST,
                label_schema_version: SchemaVersion::FIRST,
                spec_json: serde_json::json!({ "notes": "day-1 cold-start ranker" }),
                reason: "bootstrap the first model spec".to_owned(),
            },
            actor(),
        )
        .await
        .expect("create model spec");

    assert_eq!(created.name, "buy-weighted-baseline");
    assert_eq!(created.model_family, ModelFamily::WeightedFactor);
    assert_eq!(created.prediction_horizon_secs, 86_400);
    assert_eq!(
        created.status,
        PublicationStatus::Draft,
        "a freshly authored spec must be a draft"
    );

    let found = service
        .find(&created.model_spec_id)
        .await
        .expect("find model spec")
        .expect("spec row present");
    assert_eq!(found.model_spec_id, created.model_spec_id);
}

#[tokio::test]
#[ignore = "requires Docker"]
async fn factor_register_then_publish_batch_seeds_catalog() {
    let (pool, _container) = setup_pg().await;
    let factor_repo: Arc<dyn FactorRepository> =
        Arc::new(PgFactorRepository::new(pool.connection().clone()));
    let service = FactorGovernanceService::new(FactorGovernanceDeps {
        factor_repo: Arc::clone(&factor_repo),
    });

    let factors = FactorsConfig::default();
    let features = FeaturesConfig::default();

    // First register: every enabled definition lands as Draft.
    let registered = service
        .register_enabled_definitions(
            RegisterFactorDefinitionsCommand {
                factors: factors.clone(),
                features: features.clone(),
                reason: "bootstrap register".to_owned(),
            },
            actor(),
        )
        .await
        .expect("register enabled definitions");
    assert!(
        !registered.is_empty(),
        "the default factor set must register at least one definition"
    );
    assert!(
        registered
            .iter()
            .all(|def| def.status == PublicationStatus::Draft),
        "freshly registered definitions must be Draft"
    );

    // Idempotent: a second register is a no-op upsert with the same cardinality.
    let reregistered = service
        .register_enabled_definitions(
            RegisterFactorDefinitionsCommand {
                factors: factors.clone(),
                features: features.clone(),
                reason: "bootstrap register (idempotent)".to_owned(),
            },
            actor(),
        )
        .await
        .expect("re-register enabled definitions");
    assert_eq!(reregistered.len(), registered.len());

    // Batch publish flips every registered definition to Published.
    let ids: Vec<_> = registered
        .iter()
        .map(|def| def.factor_definition_id.clone())
        .collect();
    let published = service
        .publish_batch(
            PublishFactorsBatchCommand {
                factor_definition_ids: ids.clone(),
                reason: "bootstrap publish".to_owned(),
            },
            actor(),
        )
        .await
        .expect("publish batch");
    assert_eq!(published.len(), ids.len());
    assert!(
        published
            .iter()
            .all(|def| def.status == PublicationStatus::Published),
        "batch publish must promote every definition to Published"
    );

    // Re-publishing an already-published batch is idempotent (no-op, no error).
    let republished = service
        .publish_batch(
            PublishFactorsBatchCommand {
                factor_definition_ids: ids,
                reason: "bootstrap publish (idempotent)".to_owned(),
            },
            actor(),
        )
        .await
        .expect("re-publish batch");
    assert!(
        republished.is_empty(),
        "already-published ids are skipped as a no-op"
    );
}

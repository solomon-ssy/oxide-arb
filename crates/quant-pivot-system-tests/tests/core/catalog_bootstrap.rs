//! First-boot registry bootstrap system contracts.
//!
//! Proves the production write paths that seed a fresh system's research
//! registry: authoring a model spec ([`ModelSpecService`]) and content-addressed
//! factor registration during training.

use std::sync::Arc;

use quant_pivot_core::{
    governance::{ModelSpecDeps, ModelSpecService},
    runtime_config::DecisionPolicyStore,
};
use quant_pivot_models::{
    domain::{
        api::FactorDefinitionListQuery,
        ports::{CreateModelSpecCommand, GovernanceActor, ModelSpecPort},
        quant::{FactorRegistrationOutcome, NewFactorDefinition},
    },
    enums::model::ModelFamily,
    runtime_config::{DecisionPolicySnapshot, DomainConfig, FactorsConfig, FeaturesConfig},
    types::{
        ModelInputContract, ModelTrainingContract, RoleCode, SchemaVersion,
        model_spec::ModelSpecThesis,
    },
};
use quant_pivot_repository::{
    postgres::{PgFactorRepository, PgModelRegistryRepository},
    traits::{FactorRepository, ModelRegistryRepository},
};
use quant_pivot_research::factors::FactorEngine;
use quant_pivot_system_tests::postgres::setup_pg;

fn actor() -> GovernanceActor {
    GovernanceActor {
        user_id: None,
        username: "bootstrap-it".to_owned(),
        role: Some(RoleCode::new("risk_owner")),
    }
}

pub async fn model_spec_service_spec() {
    let (pool, _container) = setup_pg().await;
    let registry: Arc<dyn ModelRegistryRepository> =
        Arc::new(PgModelRegistryRepository::new(pool.connection().clone()));
    let service = ModelSpecService::new(ModelSpecDeps {
        model_registry: Arc::clone(&registry),
        runtime_config: Arc::new(DecisionPolicyStore::new(DecisionPolicySnapshot::default())),
    });

    let created = service
        .create(
            CreateModelSpecCommand {
                name: "buy-weighted-baseline".to_owned(),
                model_family: ModelFamily::WeightedFactor,
                prediction_horizon_secs: 86_400,
                feature_schema_version: SchemaVersion::FIRST,
                label_schema_version: SchemaVersion::FIRST,
                thesis: ModelSpecThesis {
                    summary: "Day-1 cold-start ranker".to_owned(),
                    hypothesis: "Governed factors predict positive forward net returns".to_owned(),
                    limitations: vec![
                        "Valid only under the frozen Polymarket research contract".to_owned(),
                    ],
                },
                input_contract: ModelInputContract::single_required("book.mid"),
                training_contract: ModelTrainingContract::outcome_default(),
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
        created
            .definition()
            .content_hash()
            .expect("definition hash"),
        created.definition_hash,
        "the persisted immutable definition must verify"
    );

    let found = service
        .find(&created.model_spec_id)
        .await
        .expect("find model spec")
        .expect("spec row present");
    assert_eq!(found.model_spec_id, created.model_spec_id);
}

pub async fn factor_training_registration_catalog() {
    let (pool, _container) = setup_pg().await;
    let factor_repo: Arc<dyn FactorRepository> =
        Arc::new(PgFactorRepository::new(pool.connection().clone()));

    let factors = FactorsConfig::default();
    let features = FeaturesConfig::default();
    let engine = FactorEngine::new(&factors, &features, &DomainConfig::default(), None);
    let definitions = engine
        .serving_plane()
        .expect("seal serving plane")
        .definitions()
        .iter()
        .cloned()
        .map(NewFactorDefinition::from)
        .collect::<Vec<_>>();

    let registered = factor_repo
        .register_definitions(definitions.clone())
        .await
        .expect("register enabled definitions");
    assert!(
        !registered.is_empty(),
        "the default factor set must register at least one definition"
    );
    assert!(
        registered
            .iter()
            .all(|outcome| matches!(outcome, FactorRegistrationOutcome::Inserted(_))),
        "fresh training registration must insert every immutable revision"
    );

    let reregistered = factor_repo
        .register_definitions(definitions)
        .await
        .expect("re-register enabled definitions");
    assert_eq!(reregistered.len(), registered.len());
    assert!(
        reregistered
            .iter()
            .all(|outcome| matches!(outcome, FactorRegistrationOutcome::AlreadyPresent(_))),
        "exact training retry must resolve every existing immutable revision"
    );

    let catalog = factor_repo
        .page_definitions(FactorDefinitionListQuery::default())
        .await
        .expect("read immutable factor catalog");
    assert_eq!(catalog.total, registered.len() as u64);
}

//! Immutable factor-definition revision integration tests (Postgres).

use quant_pivot_error::storage::StorageError;
use quant_pivot_models::{
    domain::NewFactorDefinition,
    enums::{
        factor::{FactorDefinitionScope, FactorFamily},
        quant::PublicationStatus,
    },
    types::{ContentHash, FactorDefinitionId, SchemaVersion},
};
use quant_pivot_repository::{postgres::PgFactorRepository, traits::FactorRepository};
use quant_pivot_test_support::pg::setup_pg;

fn content_hash(seed: char) -> ContentHash {
    ContentHash::parse(format!("blake3:{}", seed.to_string().repeat(64))).expect("hash")
}

fn revision(name: &str, seed: char) -> NewFactorDefinition {
    let definition_hash = content_hash(seed);
    NewFactorDefinition {
        factor_definition_id: FactorDefinitionId::from_definition_hash(&definition_hash),
        definition_hash,
        feature_contract_hash: content_hash('f'),
        name: name.to_owned(),
        factor_family: FactorFamily::Momentum,
        scope: FactorDefinitionScope::Generic,
        input_schema_version: SchemaVersion::FIRST,
        output_schema_version: SchemaVersion::FIRST,
        definition_json: serde_json::json!({"name": name, "revision": seed}),
        status: PublicationStatus::Draft,
        created_by: None,
    }
}

#[tokio::test]
#[ignore = "requires Docker"]
async fn registration_is_insert_only_and_publication_retires_prior_revision() {
    let (pool, _container) = setup_pg().await;
    let repo = PgFactorRepository::new(pool.connection().clone());
    let first = revision("momentum", '1');
    let first_id = first.factor_definition_id.clone();

    repo.create_definition(first.clone())
        .await
        .expect("register first revision");
    repo.publish_definition(&first_id)
        .await
        .expect("publish first revision");

    let idempotent = repo
        .create_definition(first)
        .await
        .expect("identical registration");
    assert_eq!(idempotent.status, PublicationStatus::Published);

    let second = revision("momentum", '2');
    let second_id = second.factor_definition_id.clone();
    repo.create_definition(second)
        .await
        .expect("register second revision");
    repo.publish_definition(&second_id)
        .await
        .expect("publish second revision");

    let retired = repo
        .find_definition(&first_id)
        .await
        .expect("find first")
        .expect("first row");
    let published = repo
        .find_definition(&second_id)
        .await
        .expect("find second")
        .expect("second row");
    assert_eq!(retired.status, PublicationStatus::Retired);
    assert_eq!(published.status, PublicationStatus::Published);
}

#[tokio::test]
#[ignore = "requires Docker"]
async fn batch_publication_is_atomic_and_rejects_content_address_collisions() {
    let (pool, _container) = setup_pg().await;
    let repo = PgFactorRepository::new(pool.connection().clone());
    let left = revision("left", '3');
    let left_id = left.factor_definition_id.clone();
    let right = revision("right", '4');
    let right_id = right.factor_definition_id.clone();
    repo.create_definition(left.clone())
        .await
        .expect("register left");
    repo.create_definition(right).await.expect("register right");

    let missing = FactorDefinitionId::from_definition_hash(&content_hash('5'));
    let failed = repo.publish_definitions(&[left_id.clone(), missing]).await;
    assert!(matches!(failed, Err(StorageError::NotFound { .. })));
    assert_eq!(
        repo.find_definition(&left_id)
            .await
            .expect("find left")
            .expect("left row")
            .status,
        PublicationStatus::Draft,
        "validation failure must roll back the whole batch"
    );

    let published = repo
        .publish_definitions(&[left_id.clone(), right_id])
        .await
        .expect("publish batch");
    assert_eq!(published.len(), 2);
    assert!(
        published
            .iter()
            .all(|row| row.status == PublicationStatus::Published)
    );

    let mut collision = left;
    collision.name = "tampered-logical-name".to_owned();
    collision.definition_json = serde_json::json!({"tampered": true});
    assert!(matches!(
        repo.create_definition(collision).await,
        Err(StorageError::InvariantViolation { .. })
    ));
}

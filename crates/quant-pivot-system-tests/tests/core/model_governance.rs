//! Model artifact and serving-authority system contracts.

use quant_pivot_models::entities::quant_model_version::Entity as ModelVersionEntity;
use quant_pivot_repository::{
    postgres::PgModelRegistryRepository, traits::ModelRegistryRepository,
};
use quant_pivot_system_tests::{
    postgres::setup_pg, support::execution_pg_seed::seed_shared_demo_infra,
};
use sea_orm::{ConnectionTrait, DatabaseBackend, EntityTrait, Statement, TryGetable};

pub async fn model_artifact_append_only() {
    let (pool, _container) = setup_pg().await;
    let db = pool.connection().clone();
    let infra = seed_shared_demo_infra(&db).await;
    let before = ModelVersionEntity::find_by_id(infra.model_version_id)
        .one(&db)
        .await
        .expect("load immutable model")
        .expect("seeded model exists");

    let update = Statement::from_sql_and_values(
        DatabaseBackend::Postgres,
        "UPDATE quant_model_version
         SET artifact_hash = decode($1::text, 'hex')
         WHERE model_version_id = $2",
        ["ab".repeat(32).into(), infra.model_version_id.into()],
    );
    let update_error = db
        .execute_raw(update)
        .await
        .expect_err("model artifact mutation must be denied");
    assert!(
        update_error.to_string().contains("append-only"),
        "WORM rejection must identify the append-only contract: {update_error}"
    );

    ModelVersionEntity::delete_by_id(infra.model_version_id)
        .exec(&db)
        .await
        .expect_err("model artifact deletion must be denied");

    let after = ModelVersionEntity::find_by_id(infra.model_version_id)
        .one(&db)
        .await
        .expect("reload immutable model")
        .expect("model survives rejected writes");
    assert_eq!(after, before);
}

pub async fn route_has_no_lifecycle() {
    let (pool, _container) = setup_pg().await;
    let db = pool.connection().clone();
    let infra = seed_shared_demo_infra(&db).await;

    let legacy_columns = db
        .query_all_raw(Statement::from_string(
            DatabaseBackend::Postgres,
            "SELECT column_name
             FROM information_schema.columns
             WHERE table_schema = 'public'
               AND table_name = 'quant_model_version'
               AND column_name IN (
                   'publication_status',
                   'published_at',
                   'retired_at',
                   'publish_path_set_id',
                   'quality_gate_report'
               )
             ORDER BY column_name",
        ))
        .await
        .expect("inspect immutable model schema");
    assert!(
        legacy_columns.is_empty(),
        "serving lifecycle must not be stored on quant_model_version"
    );

    let legacy_type = db
        .query_one_raw(Statement::from_string(
            DatabaseBackend::Postgres,
            "SELECT count(*)::bigint AS count
             FROM pg_type
             WHERE typname = 'qp_publication_status'",
        ))
        .await
        .expect("inspect legacy enum")
        .expect("count row");
    let count = i64::try_get(&legacy_type, "", "count").expect("legacy enum count");
    assert_eq!(count, 0, "the global publication enum must not exist");

    let model = PgModelRegistryRepository::new(db)
        .find_model_version(&infra.model_version_id)
        .await
        .expect("load route candidate")
        .expect("route candidate exists");
    model
        .verified_serving_contract()
        .expect("route candidate remains a verified immutable artifact");
}

//! Immutable factor-definition revision persistence system contracts.

use quant_pivot_error::storage::StorageError;
use quant_pivot_models::{
    domain::{
        api::FactorDefinitionListQuery,
        quant::{FactorRegistrationOutcome, NewFactorDefinition},
    },
    entities::quant_factor_definition::{
        Column as FactorDefinitionColumn, Entity as FactorDefinitionEntity,
    },
    enums::factor::{FactorDefinitionScope, FactorFamily, FactorNormalization},
    types::{
        ContentHash, FactorDefinitionId, SchemaVersion,
        factor::{
            FactorComputationContract, FactorContextEffect, FactorDefinitionDocument,
            FactorDefinitionRef, FactorOutputSemantics,
        },
        stable_name::FactorName,
    },
};
use quant_pivot_repository::{postgres::PgFactorRepository, traits::FactorRepository};
use quant_pivot_system_tests::postgres::setup_pg;
use sea_orm::{
    ColumnTrait, ConnectionTrait, DatabaseConnection, DbBackend, EntityTrait, IntoActiveModel,
    QueryFilter, Statement, TryGetable, sea_query::Expr,
};

fn content_hash(seed: char) -> ContentHash {
    ContentHash::parse(&format!("blake3:{}", seed.to_string().repeat(64))).expect("hash")
}

fn revision(name: &str, seed: char) -> NewFactorDefinition {
    let feature_contract_hash = content_hash(seed);
    let definition = FactorDefinitionDocument {
        name: FactorName::new(name),
        family: FactorFamily::Momentum,
        input_features: Vec::new(),
        output: FactorOutputSemantics::Context {
            effect: FactorContextEffect::HigherIsSupportive,
        },
        normalization: FactorNormalization::Rank,
        owner: format!("test-revision-{seed}"),
        required: false,
        computation: FactorComputationContract {
            semantic_version: 1,
            semantic_key: format!("quant-pivot/test-factor-{seed}@1"),
        },
    };
    let revision = FactorDefinitionRef::try_seal(
        definition,
        feature_contract_hash,
        SchemaVersion::FIRST,
        SchemaVersion::FIRST,
    )
    .expect("sealed factor definition");
    NewFactorDefinition::from(revision)
}

pub async fn registration_insert_only_revision() {
    let (pool, _container) = setup_pg().await;
    let repo = PgFactorRepository::new(pool.connection().clone());
    let first = revision("momentum", '1');
    let first_id = first.factor_definition_id;

    let first_outcomes = repo
        .register_definitions(vec![first.clone()])
        .await
        .expect("register first revision");
    assert!(matches!(
        first_outcomes.as_slice(),
        [FactorRegistrationOutcome::Inserted(_)]
    ));

    let mut idempotent = repo
        .register_definitions(vec![first])
        .await
        .expect("identical registration");
    let FactorRegistrationOutcome::AlreadyPresent(idempotent) = idempotent
        .pop()
        .expect("one idempotent registration outcome")
    else {
        panic!("exact retry must return AlreadyPresent");
    };
    assert_eq!(idempotent.factor_definition_id, first_id);

    let second = revision("momentum", '2');
    let second_id = second.factor_definition_id;
    let second_outcomes = repo
        .register_definitions(vec![second])
        .await
        .expect("register second revision");
    assert!(matches!(
        second_outcomes.as_slice(),
        [FactorRegistrationOutcome::Inserted(_)]
    ));
    assert!(
        repo.find_definition(&first_id)
            .await
            .expect("find first")
            .is_some()
    );
    assert!(
        repo.find_definition(&second_id)
            .await
            .expect("find second")
            .is_some()
    );
}

struct FactorRegistrationScenarios {
    db: DatabaseConnection,
    repo: PgFactorRepository,
    baseline: NewFactorDefinition,
}

impl FactorRegistrationScenarios {
    fn new(db: DatabaseConnection) -> Self {
        Self {
            repo: PgFactorRepository::new(db.clone()),
            db,
            baseline: revision("left", '3'),
        }
    }

    async fn register_canonical_batch(&self) {
        let right = revision("right", '4');
        let registered = self
            .repo
            .register_definitions(vec![right, self.baseline.clone()])
            .await
            .expect("register canonical batch");
        let registered_names = registered
            .iter()
            .map(|outcome| match outcome {
                FactorRegistrationOutcome::Inserted(row)
                | FactorRegistrationOutcome::AlreadyPresent(row) => row.name.as_str(),
            })
            .collect::<Vec<_>>();
        assert_eq!(registered_names, ["left", "right"]);
    }

    async fn reject_identity_collision(&self) {
        let mut corrupted = revision("collision", '9');
        let requested_collision = corrupted.clone();
        corrupted.feature_contract_hash = content_hash('b');
        let corrupted_id = corrupted.factor_definition_id;
        FactorDefinitionEntity::insert(corrupted.into_active_model())
            .exec(&self.db)
            .await
            .expect("inject identity collision at the raw insert boundary");
        assert!(matches!(
            self.repo.find_definition(&corrupted_id).await,
            Err(StorageError::InvariantViolation { .. })
        ));
        assert!(matches!(
            self.repo.find_definitions_by_ids(&[corrupted_id]).await,
            Err(StorageError::InvariantViolation { .. })
        ));
        assert!(matches!(
            self.repo
                .page_definitions(FactorDefinitionListQuery::default())
                .await,
            Err(StorageError::InvariantViolation { .. })
        ));

        let atomic_new = revision("atomic_new", 'a');
        let atomic_new_id = atomic_new.factor_definition_id;
        assert!(matches!(
            self.repo
                .register_definitions(vec![atomic_new, requested_collision])
                .await,
            Err(StorageError::InvariantViolation { .. })
        ));
        assert!(
            self.repo
                .find_definition(&atomic_new_id)
                .await
                .expect("find rolled-back revision")
                .is_none(),
            "a later identity collision must roll back earlier batch inserts"
        );
    }

    async fn preserve_revision_identity(&self) {
        let baseline_id = self.baseline.factor_definition_id;
        let identity_tamper = FactorDefinitionEntity::update_many()
            .col_expr(
                FactorDefinitionColumn::FeatureContractHash,
                Expr::value(content_hash('c')),
            )
            .filter(FactorDefinitionColumn::FactorDefinitionId.eq(baseline_id))
            .exec(&self.db)
            .await;
        assert!(
            identity_tamper.is_err(),
            "persisted factor revision identity must reject in-place mutation"
        );
        let immutable = self
            .repo
            .find_definition(&baseline_id)
            .await
            .expect("reload immutable revision")
            .expect("immutable revision");
        assert_eq!(
            immutable.feature_contract_hash, self.baseline.feature_contract_hash,
            "rejected mutation must preserve the original revision"
        );
    }

    async fn reject_malformed_document(&self) {
        let poison_hash = content_hash('d');
        let poison_id = FactorDefinitionId::from_definition_hash(&poison_hash);
        let malformed_insert = self
            .db
            .execute_raw(Statement::from_sql_and_values(
                DbBackend::Postgres,
                "INSERT INTO quant_factor_definition (
                    factor_definition_id, definition_hash, feature_contract_hash, name,
                    factor_family, scope, input_schema_version, output_schema_version,
                    definition, created_at
                 )
                 SELECT $1, $2, feature_contract_hash, $3, factor_family, scope,
                        input_schema_version, output_schema_version,
                        jsonb_set(
                            jsonb_set(definition, '{name}', to_jsonb($3::text), false),
                            '{input_features}', '[null]'::jsonb, false
                        ),
                        created_at
                 FROM quant_factor_definition
                 WHERE factor_definition_id = $4",
                [
                    poison_id.as_uuid().into(),
                    poison_hash.to_string().into(),
                    "poison".into(),
                    self.baseline.factor_definition_id.as_uuid().into(),
                ],
            ))
            .await;
        assert!(
            malformed_insert.is_err(),
            "relational contract must reject malformed factor-definition members at INSERT"
        );
    }

    async fn reject_version_overflow(&self) {
        let poison_hash = content_hash('d');
        let poison_id = FactorDefinitionId::from_definition_hash(&poison_hash);
        let semantic_overflow = self
            .db
            .execute_raw(Statement::from_sql_and_values(
                DbBackend::Postgres,
                "INSERT INTO quant_factor_definition (
                    factor_definition_id, definition_hash, feature_contract_hash, name,
                    factor_family, scope, input_schema_version, output_schema_version,
                    definition, created_at
                 )
                 SELECT $1, $2, feature_contract_hash, $3, factor_family, scope,
                        input_schema_version, output_schema_version,
                        jsonb_set(
                            jsonb_set(definition, '{name}', to_jsonb($3::text), false),
                            '{computation,semantic_version}', '4294967296'::jsonb, false
                        ),
                        created_at
                 FROM quant_factor_definition
                 WHERE factor_definition_id = $4",
                [
                    poison_id.as_uuid().into(),
                    poison_hash.to_string().into(),
                    "semantic_overflow".into(),
                    self.baseline.factor_definition_id.as_uuid().into(),
                ],
            ))
            .await;
        assert!(
            semantic_overflow.is_err(),
            "factor computation versions outside u32 must be rejected before persistence"
        );

        let output_plane = self
            .db
            .query_one_raw(Statement::from_sql_and_values(
                DbBackend::Postgres,
                "SELECT public.validate_factor_serving_plane(
                    jsonb_build_object(
                        'format_version', 2,
                        'factor_schema_hash', definition_hash::text,
                        'definitions', jsonb_build_array(jsonb_build_object(
                            'revision_version', 2,
                            'factor_definition_id', factor_definition_id::text,
                            'definition_hash', definition_hash::text,
                            'feature_contract_hash', feature_contract_hash::text,
                            'input_schema_version', input_schema_version,
                            'output_schema_version', 2147483648::numeric,
                            'definition', definition
                        ))
                    ),
                    feature_contract_hash::text,
                    input_schema_version,
                    'weighted_factor'
                 ) AS valid
                 FROM quant_factor_definition
                 WHERE factor_definition_id = $1",
                [self.baseline.factor_definition_id.as_uuid().into()],
            ))
            .await
            .expect("validate oversized output schema plane")
            .expect("factor definition row");
        assert!(
            !bool::try_get(&output_plane, "", "valid").expect("decode factor plane validation"),
            "factor output schema versions outside i32 must be rejected"
        );
    }

    async fn reject_boundary_fields(&self) {
        let poison_hash = content_hash('d');
        let poison_id = FactorDefinitionId::from_definition_hash(&poison_hash);
        let boundary_whitespace = self
            .db
            .execute_raw(Statement::from_sql_and_values(
                DbBackend::Postgres,
                "INSERT INTO quant_factor_definition (
                    factor_definition_id, definition_hash, feature_contract_hash, name,
                    factor_family, scope, input_schema_version, output_schema_version,
                    definition, created_at
                 )
                 SELECT $1, $2, feature_contract_hash, $3, factor_family, scope,
                        input_schema_version, output_schema_version,
                        jsonb_set(
                            jsonb_set(definition, '{name}', to_jsonb($3::text), false),
                            '{owner}', to_jsonb(E'\\towner'::text), false
                        ),
                        created_at
                 FROM quant_factor_definition
                 WHERE factor_definition_id = $4",
                [
                    poison_id.as_uuid().into(),
                    poison_hash.to_string().into(),
                    "owner_whitespace".into(),
                    self.baseline.factor_definition_id.as_uuid().into(),
                ],
            ))
            .await;
        assert!(
            boundary_whitespace.is_err(),
            "factor owners with boundary whitespace must be rejected before persistence"
        );

        let oversized_feature = self
            .db
            .execute_raw(Statement::from_sql_and_values(
                DbBackend::Postgres,
                "INSERT INTO quant_factor_definition (
                    factor_definition_id, definition_hash, feature_contract_hash, name,
                    factor_family, scope, input_schema_version, output_schema_version,
                    definition, created_at
                 )
                 SELECT $1, $2, feature_contract_hash, $3, factor_family, scope,
                        input_schema_version, output_schema_version,
                        jsonb_set(
                            jsonb_set(definition, '{name}', to_jsonb($3::text), false),
                            '{input_features}',
                            jsonb_build_array(to_jsonb(('f.' || repeat('a', 255))::text)),
                            false
                        ),
                        created_at
                 FROM quant_factor_definition
                 WHERE factor_definition_id = $4",
                [
                    poison_id.as_uuid().into(),
                    poison_hash.to_string().into(),
                    "oversized_feature".into(),
                    self.baseline.factor_definition_id.as_uuid().into(),
                ],
            ))
            .await;
        assert!(
            oversized_feature.is_err(),
            "factor input feature names must fit the explanation contract"
        );
    }

    async fn reject_scope_mismatch(&self) {
        let mut scope_mismatch = revision("scope_mismatch", 'e');
        scope_mismatch.scope = FactorDefinitionScope::Structural;
        let scope_insert = FactorDefinitionEntity::insert(scope_mismatch.into_active_model())
            .exec(&self.db)
            .await;
        assert!(
            scope_insert.is_err(),
            "relational contract must reject factor scope/definition-family mismatch"
        );
    }

    async fn reject_batch_prevalidation(&self) {
        let unregistered = revision("unregistered", '6');
        let unregistered_id = unregistered.factor_definition_id;
        let mut invalid = revision("invalid", '7');
        "different-name".clone_into(&mut invalid.name);
        assert!(matches!(
            self.repo
                .register_definitions(vec![unregistered, invalid])
                .await,
            Err(StorageError::InvariantViolation { .. })
        ));
        assert!(
            self.repo
                .find_definition(&unregistered_id)
                .await
                .expect("find unregistered")
                .is_none(),
            "full-batch prevalidation must run before the transaction"
        );
    }
}

pub async fn batch_atomic_rejects_collisions() {
    let (pool, _container) = setup_pg().await;
    let scenarios = FactorRegistrationScenarios::new(pool.connection().clone());
    scenarios.register_canonical_batch().await;
    scenarios.reject_identity_collision().await;
    scenarios.preserve_revision_identity().await;
    scenarios.reject_malformed_document().await;
    scenarios.reject_version_overflow().await;
    scenarios.reject_boundary_fields().await;
    scenarios.reject_scope_mismatch().await;
    scenarios.reject_batch_prevalidation().await;
}

pub async fn concurrent_registration_idempotent() {
    let (pool, _container) = setup_pg().await;
    let definition = revision("concurrent", '8');
    let left_repo = PgFactorRepository::new(pool.connection().clone());
    let right_repo = PgFactorRepository::new(pool.connection().clone());

    let (left, right) = tokio::join!(
        left_repo.register_definitions(vec![definition.clone()]),
        right_repo.register_definitions(vec![definition])
    );
    let outcomes: [_; 2] = (
        left.expect("left registration"),
        right.expect("right registration"),
    )
        .into();
    let inserted = outcomes
        .iter()
        .flatten()
        .filter(|outcome| matches!(outcome, FactorRegistrationOutcome::Inserted(_)))
        .count();
    let present = outcomes
        .iter()
        .flatten()
        .filter(|outcome| matches!(outcome, FactorRegistrationOutcome::AlreadyPresent(_)))
        .count();
    assert_eq!(inserted, 1);
    assert_eq!(present, 1);
}

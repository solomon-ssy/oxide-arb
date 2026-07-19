//! Irreversible production-baseline persistence integration tests.

use chrono::Utc;
use quant_pivot_models::{
    config::ProjectLifecyclePolicy,
    domain::NewProductionBaseline,
    enums::runtime_config::{
        CheckOutcome, LifecycleBaseline, LifecycleCheckKind, PolicyActorKind, ProjectLifecycleState,
    },
    hashing::CanonicalDigest,
    runtime_config::{LifecycleCheckDetail, ProductionSealCheck, ProductionSealEvidence},
    types::{BuildCommitHash, ContentHash, DeploymentEnvironment, ProductionBaselineId},
};
use quant_pivot_repository::{postgres::PgPolicyRepository, traits::PolicyRepository};
use quant_pivot_test_support::pg::setup_pg;

fn hash(seed: u8) -> ContentHash {
    ContentHash::parse(format!("blake3:{}", format!("{seed:02x}").repeat(32)))
        .expect("test content hash")
}

fn baseline() -> NewProductionBaseline {
    let now = Utc::now();
    let frozen_policy = ProjectLifecyclePolicy {
        state: ProjectLifecycleState::ProductionFrozen,
        baseline: LifecycleBaseline::Boot,
    };
    NewProductionBaseline {
        production_baseline_id: ProductionBaselineId::boot(),
        environment: DeploymentEnvironment::parse("production").expect("environment"),
        sealed_at: now,
        sealed_by_kind: PolicyActorKind::Operator,
        sealed_by_user_id: None,
        sealed_by_label: "integration-test".to_owned(),
        build_commit: BuildCommitHash::parse("1".repeat(40)).expect("build commit"),
        postgres_schema_fingerprint: hash(1),
        clickhouse_schema_fingerprint: hash(2),
        policy_bundle_hash: hash(3),
        lifecycle_policy_hash: frozen_policy.content_hash().expect("lifecycle hash"),
        evidence: ProductionSealEvidence {
            checks: vec![ProductionSealCheck {
                kind: LifecycleCheckKind::LifecycleContract,
                outcome: CheckOutcome::Passed,
                checked_at: now,
                detail: LifecycleCheckDetail::ContractMatched,
            }],
            backup_evidence_hash: Some(hash(4)),
            config_e2e_evidence_hash: Some(
                CanonicalDigest::content_hash_json(&"e2e").expect("e2e hash"),
            ),
        },
    }
}

#[tokio::test]
#[ignore = "requires Docker"]
async fn production_baseline_is_a_single_append_only_boot_fact() {
    let (pool, _container) = setup_pg().await;
    let repository = PgPolicyRepository::new(pool.connection().clone());

    let sealed = repository
        .seal_production_baseline(baseline())
        .await
        .expect("first production seal");
    assert_eq!(sealed.production_baseline_id, ProductionBaselineId::boot());

    let repeated = repository.seal_production_baseline(baseline()).await;
    assert!(repeated.is_err(), "a second seal must be rejected");
    assert_eq!(
        repository
            .load_production_baseline()
            .await
            .expect("load baseline")
            .expect("sealed baseline")
            .production_baseline_id,
        ProductionBaselineId::boot()
    );
}

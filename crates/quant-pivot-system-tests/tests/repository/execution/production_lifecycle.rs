//! Irreversible production-baseline persistence system contract.

use chrono::Utc;
use quant_pivot_error::QuantResult;
use quant_pivot_models::{
    config::ProjectLifecyclePolicy,
    domain::{
        governance::{NewProductionBaseline, NewProductionEvidence},
        ports::{
            LifecycleSchemaVerificationPort, ProductionEvidenceArtifactVerificationPort,
            VerifiedSchemaFingerprints,
        },
    },
    enums::runtime_config::{
        CheckOutcome, LifecycleBaseline, LifecycleCheckKind, PolicyActorKind,
        ProductionEvidenceKind, ProjectLifecycleState,
    },
    hashing::CanonicalDigest,
    runtime_config::{
        ActivePolicyBundle, LifecycleCheckDetail, ProductionSealCheck, ProductionSealEvidence,
    },
    types::{
        ArtifactUri, BuildCommitHash, ContentHash, DeploymentEnvironment, ProductionBaselineId,
        ProductionEvidenceId,
    },
};
use quant_pivot_repository::{postgres::PgPolicyRepository, traits::PolicyRepository};
use quant_pivot_system_tests::{
    postgres::setup_pg, support::policy_fixtures::bootstrap_default_policy_bundle,
};

fn hash(seed: u8) -> ContentHash {
    ContentHash::parse(format!("blake3:{}", format!("{seed:02x}").repeat(32)))
        .expect("test content hash")
}

fn backup_evidence_hash() -> ContentHash {
    hash(4)
}

fn config_e2e_evidence_hash() -> ContentHash {
    CanonicalDigest::content_hash_json(&"e2e").expect("e2e hash")
}

fn baseline(bundle: &ActivePolicyBundle) -> NewProductionBaseline {
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
        policy_bundle_generation: bundle.generation,
        decision_policy_snapshot_id: bundle.decision_policy_snapshot_id.clone(),
        policy_bundle_hash: bundle.snapshot_hash.clone(),
        lifecycle_policy_hash: frozen_policy.content_hash().expect("lifecycle hash"),
        evidence: ProductionSealEvidence {
            checks: vec![ProductionSealCheck {
                kind: LifecycleCheckKind::LifecycleContract,
                outcome: CheckOutcome::Passed,
                checked_at: now,
                detail: LifecycleCheckDetail::ContractMatched,
            }],
            backup_evidence_hash: Some(backup_evidence_hash()),
            config_e2e_evidence_hash: Some(config_e2e_evidence_hash()),
        },
    }
}

fn evidence(bundle: &ActivePolicyBundle, kind: ProductionEvidenceKind) -> NewProductionEvidence {
    let (artifact_uri, evidence_hash) = match kind {
        ProductionEvidenceKind::BackupRestore => (
            ArtifactUri::parse("s3://production-evidence/backup-restore.json")
                .expect("backup artifact URI"),
            backup_evidence_hash(),
        ),
        ProductionEvidenceKind::ProtectedConfigEndToEnd => (
            ArtifactUri::parse("s3://production-evidence/config-e2e.json")
                .expect("Config E2E artifact URI"),
            config_e2e_evidence_hash(),
        ),
    };
    NewProductionEvidence {
        production_evidence_id: ProductionEvidenceId::from_v7(),
        kind,
        artifact_uri,
        evidence_hash,
        build_commit: BuildCommitHash::parse("1".repeat(40)).expect("build commit"),
        postgres_schema_fingerprint: hash(1),
        clickhouse_schema_fingerprint: hash(2),
        policy_bundle_generation: bundle.generation,
        decision_policy_snapshot_id: bundle.decision_policy_snapshot_id.clone(),
        policy_bundle_hash: bundle.snapshot_hash.clone(),
        recorded_by_kind: PolicyActorKind::Operator,
        recorded_by_user_id: None,
        recorded_by_label: "integration-test".to_owned(),
        reason: "verify irreversible production seal evidence".to_owned(),
        observed_at: Utc::now(),
    }
}

struct StaticSchemaVerification;

#[async_trait::async_trait]
impl LifecycleSchemaVerificationPort for StaticSchemaVerification {
    async fn verify_live(&self) -> QuantResult<VerifiedSchemaFingerprints> {
        Ok(VerifiedSchemaFingerprints {
            postgres_schema_fingerprint: hash(1),
            clickhouse_schema_fingerprint: hash(2),
        })
    }
}

struct StaticArtifactVerification;

#[async_trait::async_trait]
impl ProductionEvidenceArtifactVerificationPort for StaticArtifactVerification {
    async fn verify_artifact(
        &self,
        _artifact_uri: &ArtifactUri,
        _expected_hash: &ContentHash,
    ) -> QuantResult<()> {
        Ok(())
    }
}

pub async fn production_baseline_is_a_single_append_only_boot_fact() {
    let (pool, _container) = setup_pg().await;
    let db = pool.connection().clone();
    bootstrap_default_policy_bundle(
        &db,
        "production-lifecycle-it",
        "bootstrap exact production seal policy bundle",
    )
    .await;
    let repository = PgPolicyRepository::new(db);
    let bundle = repository
        .load_current_bundle()
        .await
        .expect("load active bundle")
        .expect("active bundle");
    for kind in [
        ProductionEvidenceKind::BackupRestore,
        ProductionEvidenceKind::ProtectedConfigEndToEnd,
    ] {
        repository
            .record_production_evidence(
                evidence(&bundle, kind),
                &StaticSchemaVerification,
                &StaticArtifactVerification,
            )
            .await
            .expect("record production evidence");
    }
    let baseline = baseline(&bundle);

    let sealed = repository
        .seal_production_baseline(
            baseline.clone(),
            &StaticSchemaVerification,
            &StaticArtifactVerification,
        )
        .await
        .expect("first production seal");
    assert_eq!(sealed.production_baseline_id, ProductionBaselineId::boot());

    let repeated = repository
        .seal_production_baseline(
            baseline,
            &StaticSchemaVerification,
            &StaticArtifactVerification,
        )
        .await;
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

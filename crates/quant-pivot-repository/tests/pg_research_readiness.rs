//! Signed readiness-evidence index integration tests (Postgres + testcontainers).

use chrono::{Duration, Utc};
use quant_pivot_models::{
    domain::NewResearchReadinessEvidence,
    enums::quant::ResearchReadinessEvidenceKind,
    hashing::CanonicalDigest,
    types::{
        ArtifactUri, ArtifactVersion, AttestationKeyId, ContentHash,
        ResearchReadinessEvidencePayload, ShadowLatencyProfileV1,
    },
};
use quant_pivot_repository::{
    postgres::PgResearchReadinessEvidenceRepository, traits::ResearchReadinessEvidenceRepository,
};
use quant_pivot_test_support::pg::setup_pg;
use sea_orm::ConnectionTrait;
use uuid::Uuid;

fn hash(seed: char) -> ContentHash {
    ContentHash::parse(format!("blake3:{}", seed.to_string().repeat(64))).expect("valid hash")
}

fn latency_payload(observed_at: chrono::DateTime<Utc>) -> ResearchReadinessEvidencePayload {
    ResearchReadinessEvidencePayload::ShadowLatencyProfile(ShadowLatencyProfileV1 {
        format_version: 1,
        window_start: observed_at - Duration::hours(24),
        window_end: observed_at,
        observed_at,
        book_event_count: 100,
        book_age_p50_ms: 10,
        book_age_p95_ms: 20,
        book_age_p99_ms: 30,
        decision_prepared_count: 100,
        decision_prepared_p95_ms: Some(40),
        endpoint_rtt_count: 100,
        endpoint_rtt_p95_ms: Some(50),
        market_delay_count: 100,
        market_delay_p95_ms: Some(60),
    })
}

fn new_evidence(
    observed_at: chrono::DateTime<Utc>,
    scope_hash: ContentHash,
) -> NewResearchReadinessEvidence {
    let payload_json = latency_payload(observed_at);
    let payload_hash =
        CanonicalDigest::content_hash_json(&payload_json).expect("canonical payload content hash");
    NewResearchReadinessEvidence {
        evidence_id: Uuid::now_v7().into(),
        kind: ResearchReadinessEvidenceKind::ShadowLatencyProfile,
        scope_hash,
        window_start: observed_at - Duration::hours(24),
        window_end: observed_at,
        observed_at,
        expires_at: observed_at + Duration::hours(6),
        payload_json,
        payload_hash,
        artifact_uri: ArtifactUri::parse("s3://evidence/latency.json").expect("artifact URI"),
        artifact_version: ArtifactVersion::parse("version-1").expect("artifact version"),
        attestation_key_id: AttestationKeyId::parse("operator-2026-07")
            .expect("attestation key id"),
        attestation_mac: hash('c'),
    }
}

#[tokio::test]
#[ignore = "requires Docker"]
async fn readiness_evidence_is_scoped_expiring_idempotent_and_append_only() {
    let (pool, _container) = setup_pg().await;
    let db = pool.connection().clone();
    let repo = PgResearchReadinessEvidenceRepository::new(db.clone());
    let observed_at = Utc::now() - Duration::minutes(1);
    let scope_hash = hash('a');
    let evidence = new_evidence(observed_at, scope_hash.clone());

    let inserted = repo
        .append(evidence.clone())
        .await
        .expect("append evidence");
    let mut duplicate = evidence;
    duplicate.evidence_id = Uuid::now_v7().into();
    let deduplicated = repo.append(duplicate).await.expect("deduplicate evidence");
    assert_eq!(deduplicated.evidence_id, inserted.evidence_id);

    let current = repo
        .latest_valid(
            ResearchReadinessEvidenceKind::ShadowLatencyProfile,
            &scope_hash,
            observed_at + Duration::minutes(1),
        )
        .await
        .expect("current lookup")
        .expect("current evidence");
    assert_eq!(current.evidence_id, inserted.evidence_id);
    assert!(
        repo.latest_valid(
            ResearchReadinessEvidenceKind::ShadowLatencyProfile,
            &hash('b'),
            observed_at + Duration::minutes(1),
        )
        .await
        .expect("wrong-scope lookup")
        .is_none()
    );
    assert!(
        repo.latest_valid(
            ResearchReadinessEvidenceKind::ShadowLatencyProfile,
            &scope_hash,
            inserted.expires_at,
        )
        .await
        .expect("expired lookup")
        .is_none()
    );

    let mutation = db
        .execute_unprepared(
            "UPDATE quant_research_readiness_evidence SET artifact_version = 'tampered'",
        )
        .await;
    assert!(
        mutation.is_err(),
        "append-only trigger must reject mutation"
    );
}

#[tokio::test]
#[ignore = "requires Docker"]
async fn readiness_evidence_rejects_payload_hash_or_kind_tampering() {
    let (pool, _container) = setup_pg().await;
    let repo = PgResearchReadinessEvidenceRepository::new(pool.connection().clone());
    let observed_at = Utc::now() - Duration::minutes(1);
    let mut evidence = new_evidence(observed_at, hash('a'));
    evidence.payload_hash = hash('f');
    assert!(repo.append(evidence).await.is_err());

    let mut evidence = new_evidence(observed_at, hash('b'));
    evidence.kind = ResearchReadinessEvidenceKind::RetentionRunway;
    assert!(repo.append(evidence).await.is_err());
}

#[tokio::test]
#[ignore = "requires Docker"]
async fn shadow_latency_observation_returns_missing_dimensions_without_fallbacks() {
    let (pool, _container) = setup_pg().await;
    let repo = PgResearchReadinessEvidenceRepository::new(pool.connection().clone());
    let end = Utc::now();
    let observed = repo
        .observe_shadow_latency(end - Duration::hours(24), end)
        .await
        .expect("empty latency observation");
    assert_eq!(observed.decision_prepared_count, 0);
    assert_eq!(observed.decision_prepared_p95_ms, None);
    assert_eq!(observed.endpoint_rtt_count, 0);
    assert_eq!(observed.endpoint_rtt_p95_ms, None);
    assert_eq!(observed.market_delay_count, 0);
    assert_eq!(observed.market_delay_p95_ms, None);
}

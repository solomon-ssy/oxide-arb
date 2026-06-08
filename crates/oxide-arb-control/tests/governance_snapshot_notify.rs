//! Governance publish/rollback must wake the live snapshot refresher notify handle.

use std::{sync::Arc, time::Duration};

use chrono::{Duration as ChronoDuration, Utc};
use oxide_arb_control::governance::{ControlFactorRegistry, PublicationRequest};
use oxide_arb_models::{
    domain::control_factor::AuditActor,
    types::{ControlFactorId, FactorPublicationId},
};
use oxide_arb_test_support::{
    materialization::runtime_config_repo, mocks::MockGovernanceControlFactorRepository,
};
use tokio::sync::{Notify, oneshot};

fn audit_envelope() -> AuditActor {
    AuditActor {
        actor: "risk_owner".into(),
        actor_role: "risk_owner".into(),
        request_id: "req-governance-notify".into(),
        reason: "test publication".into(),
    }
}

fn shadow_publication_request() -> PublicationRequest {
    let now = Utc::now();
    PublicationRequest {
        factor_ids: vec![ControlFactorId::from_v7()],
        idempotency_key: "idem-shadow-notify".into(),
        effective_from: Some(now),
        expires_at: now + ChronoDuration::days(1),
        manual_risk_expansion_approval: false,
    }
}

fn registry_with_notify(
    repo: Arc<MockGovernanceControlFactorRepository>,
    notify: Arc<Notify>,
) -> ControlFactorRegistry {
    ControlFactorRegistry::new(repo, runtime_config_repo()).with_snapshot_refresh_notify(notify)
}

/// Registers a oneshot waiter **before** the governance call so we observe the
/// registry-owned notify, not an unrelated handle.
fn spawn_refresh_waiter(notify: Arc<Notify>) -> oneshot::Receiver<()> {
    let (tx, rx) = oneshot::channel();
    tokio::spawn(async move {
        notify.notified().await;
        let _ = tx.send(());
    });
    rx
}

#[tokio::test]
async fn promote_to_shadow_wakes_snapshot_refresh_notify() {
    let repo = Arc::new(MockGovernanceControlFactorRepository::default());
    let notify = Arc::new(Notify::new());
    let refresh_wait = spawn_refresh_waiter(Arc::clone(&notify));

    let registry = registry_with_notify(Arc::clone(&repo), notify);
    registry
        .promote_to_shadow(audit_envelope(), shadow_publication_request())
        .await
        .expect("shadow publish should succeed");

    assert_eq!(repo.publish_calls(), 1, "repo publish must run first");
    tokio::time::timeout(Duration::from_secs(1), refresh_wait)
        .await
        .expect("refresher notify should fire within timeout")
        .expect("refresh waiter should complete");
}

#[tokio::test]
async fn rollback_publication_wakes_snapshot_refresh_notify() {
    let repo = Arc::new(MockGovernanceControlFactorRepository::default());
    let notify = Arc::new(Notify::new());
    let refresh_wait = spawn_refresh_waiter(Arc::clone(&notify));

    let registry = registry_with_notify(Arc::clone(&repo), notify);
    let active = FactorPublicationId::from_v7();
    let target = FactorPublicationId::from_v7();
    registry
        .rollback_publication(audit_envelope(), &active, &target)
        .await
        .expect("rollback should succeed");

    assert_eq!(repo.rollback_calls(), 1, "repo rollback must run first");
    tokio::time::timeout(Duration::from_secs(1), refresh_wait)
        .await
        .expect("refresher notify should fire within timeout")
        .expect("refresh waiter should complete");
}

#[tokio::test]
async fn publish_without_notify_handle_does_not_wake_unrelated_waiter() {
    let repo = Arc::new(MockGovernanceControlFactorRepository::default());
    let unrelated_notify = Arc::new(Notify::new());
    let refresh_wait = spawn_refresh_waiter(Arc::clone(&unrelated_notify));

    let registry = ControlFactorRegistry::new(repo, runtime_config_repo());
    registry
        .promote_to_shadow(audit_envelope(), shadow_publication_request())
        .await
        .expect("shadow publish should succeed without notify wiring");

    assert!(
        tokio::time::timeout(Duration::from_millis(50), refresh_wait)
            .await
            .is_err(),
        "unwired notify must not be signalled by governance publish"
    );
}

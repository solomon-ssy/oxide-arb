//! Feedback coordinator orchestration contracts.

use std::time::Duration;

use quant_pivot_core::service::feedback_coordinator::{
    FeedbackCoordinatorBudget, FeedbackCoordinatorConfig, FeedbackCoordinatorWake,
};
use tokio::time::timeout;

#[test]
fn config_rejects_bad_cadence() {
    assert!(
        FeedbackCoordinatorConfig::try_new(FeedbackCoordinatorBudget {
            poll_interval: Duration::ZERO,
            lease_heartbeat: Duration::from_secs(30),
            lease_ttl: Duration::from_secs(90),
            max_inflight: 2,
            stuck_after: Duration::from_mins(2),
            alert_timeout: Duration::from_secs(1),
            alert_dedupe_secs: 60,
            shutdown_drain: Duration::from_secs(2),
        })
        .is_err()
    );
    assert!(
        FeedbackCoordinatorConfig::try_new(FeedbackCoordinatorBudget {
            poll_interval: Duration::from_secs(3),
            lease_heartbeat: Duration::from_secs(90),
            lease_ttl: Duration::from_secs(90),
            max_inflight: 2,
            stuck_after: Duration::from_mins(2),
            alert_timeout: Duration::from_secs(1),
            alert_dedupe_secs: 60,
            shutdown_drain: Duration::from_secs(2),
        })
        .is_err()
    );
}

#[tokio::test]
async fn wake_coalesces_signal() {
    let wake = FeedbackCoordinatorWake::new();
    let mut receiver = wake.subscribe();
    wake.wake();
    wake.wake();

    timeout(Duration::from_millis(50), receiver.wait())
        .await
        .expect("a buffered wake must return immediately");
    assert!(
        timeout(Duration::from_millis(10), receiver.wait())
            .await
            .is_err(),
        "bursty wakes must coalesce into one non-authoritative permit"
    );
}

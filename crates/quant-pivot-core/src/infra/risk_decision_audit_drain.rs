//! Background drain for pre-trade risk decision audit events.

use flume::Receiver;
use oxide_arb_error::OxideError;
use oxide_arb_models::domain::risk::NewRiskAuditEvent;
use oxide_arb_repository::{postgres::PgRiskAuditRepository, traits::RiskAuditRepository};
use oxide_arb_risk::audit::RiskAuditEvent;
use std::{mem::take, sync::Arc, time::Duration};
use tokio_util::sync::CancellationToken;

const DEFAULT_BATCH_SIZE: usize = 64;
const DEFAULT_FLUSH_INTERVAL: Duration = Duration::from_millis(250);

/// Drain pre-trade audit events from the channel and persist in batches.
pub async fn spawn_risk_decision_audit_drain(
    rx: Receiver<RiskAuditEvent>,
    repo: Arc<PgRiskAuditRepository>,
    shutdown: CancellationToken,
) -> Result<(), OxideError> {
    spawn_risk_decision_audit_drain_with_config(
        rx,
        repo,
        DEFAULT_BATCH_SIZE,
        DEFAULT_FLUSH_INTERVAL,
        shutdown,
    )
    .await
}

pub async fn spawn_risk_decision_audit_drain_with_config(
    rx: Receiver<RiskAuditEvent>,
    repo: Arc<PgRiskAuditRepository>,
    batch_size: usize,
    flush_interval: Duration,
    shutdown: CancellationToken,
) -> Result<(), OxideError> {
    let mut batch = Vec::with_capacity(batch_size);
    let mut flush_timer = tokio::time::interval(flush_interval);
    flush_timer.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);

    loop {
        tokio::select! {
            biased;
            () = shutdown.cancelled() => {
                drain_remaining(&rx, &mut batch);
                flush_batch(&repo, &mut batch).await?;
                return Ok(());
            }
            _ = flush_timer.tick(), if !batch.is_empty() => {
                flush_batch(&repo, &mut batch).await?;
            }
            event = rx.recv_async() => {
                if let Ok(event) = event {
                    batch.push(event);
                    if batch.len() >= batch_size {
                        flush_batch(&repo, &mut batch).await?;
                    }
                } else {
                    flush_batch(&repo, &mut batch).await?;
                    return Ok(());
                }
            }
        }
    }
}

fn drain_remaining(rx: &Receiver<RiskAuditEvent>, batch: &mut Vec<RiskAuditEvent>) {
    while let Ok(event) = rx.try_recv() {
        batch.push(event);
    }
}

async fn flush_batch(
    repo: &Arc<PgRiskAuditRepository>,
    batch: &mut Vec<RiskAuditEvent>,
) -> Result<(), OxideError> {
    if batch.is_empty() {
        return Ok(());
    }
    let events: Vec<NewRiskAuditEvent> = take(batch)
        .into_iter()
        .map(NewRiskAuditEvent::from)
        .collect();
    repo.create_batch(events).await.map_err(OxideError::from)?;
    Ok(())
}

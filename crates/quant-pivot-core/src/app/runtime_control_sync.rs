//! Multi-instance convergence for the atomic runtime-control snapshot.

use std::{sync::Arc, time::Duration};

use quant_pivot_models::domain::governance::RuntimeControlSnapshot;
use quant_pivot_repository::{
    postgres::RUNTIME_CONTROL_NOTIFY_CHANNEL, traits::RuntimeControlRepository,
};
use quant_pivot_storage::postgres::PostgresPool;
use tokio_util::sync::CancellationToken;

use super::{AppContext, task_id::TaskId, task_registry::AppRunner};
use crate::{
    governance::{RuntimeControlsHandle, SystemStatusPublisher},
    observability::metrics_hub::MetricsHub,
};

const POLL_INTERVAL: Duration = Duration::from_secs(2);

impl AppContext {
    pub fn register_runtime_control_sync(&self, runner: &mut AppRunner) {
        let repository =
            Arc::clone(&self.infra.repos.runtime_control) as Arc<dyn RuntimeControlRepository>;
        let controls = self.runtime_controls();
        let postgres = Arc::clone(&self.infra.pg);
        let metrics = Arc::clone(&self.infra.metrics);
        let status_publisher = Arc::clone(&self.governance.status_publisher);
        runner.spawn(TaskId::RuntimeControlSync, move |token| async move {
            run_runtime_control_sync(
                repository,
                controls,
                postgres,
                metrics,
                status_publisher,
                token,
            )
            .await;
        });
    }
}

async fn run_runtime_control_sync(
    repository: Arc<dyn RuntimeControlRepository>,
    controls: RuntimeControlsHandle,
    postgres: Arc<PostgresPool>,
    metrics: Arc<MetricsHub>,
    status_publisher: Arc<SystemStatusPublisher>,
    shutdown: CancellationToken,
) {
    let mut listener = match postgres.listen(RUNTIME_CONTROL_NOTIFY_CHANNEL).await {
        Ok(listener) => Some(listener),
        Err(error) => {
            tracing::warn!(%error, "runtime-control listener unavailable; polling fallback active");
            None
        }
    };
    loop {
        tokio::select! {
            biased;
            () = shutdown.cancelled() => return,
            result = async {
                match listener.as_mut() {
                    Some(listener) => listener.recv().await,
                    None => std::future::pending().await,
                }
            } => {
                if let Err(error) = result {
                    tracing::warn!(%error, "runtime-control listener disconnected; polling fallback active");
                    listener = None;
                }
                refresh_runtime_controls(
                    repository.as_ref(),
                    &controls,
                    &metrics,
                    &status_publisher,
                ).await;
            }
            () = tokio::time::sleep(POLL_INTERVAL) => {
                refresh_runtime_controls(
                    repository.as_ref(),
                    &controls,
                    &metrics,
                    &status_publisher,
                ).await;
                if listener.is_none() {
                    listener = match postgres.listen(RUNTIME_CONTROL_NOTIFY_CHANNEL).await {
                        Ok(listener) => Some(listener),
                        Err(error) => {
                            tracing::warn!(%error, "runtime-control listener reconnect failed");
                            None
                        }
                    };
                }
            }
        }
    }
}

async fn refresh_runtime_controls(
    repository: &dyn RuntimeControlRepository,
    controls: &RuntimeControlsHandle,
    metrics: &MetricsHub,
    status_publisher: &SystemStatusPublisher,
) {
    match repository.load().await {
        Ok(info) => {
            let snapshot = RuntimeControlSnapshot::from(info);
            let kill_switch_state = snapshot.kill_switch_state;
            if controls.publish_if_newer(snapshot) {
                metrics.set_policy_automatic_halted(!kill_switch_state.allows_new_entry());
                status_publisher.publish();
            }
        }
        Err(error) => {
            tracing::error!(%error, "runtime-control polling read failed; retaining last verified snapshot");
        }
    }
}

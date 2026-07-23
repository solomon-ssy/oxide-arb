//! Typed lifecycle gates for capability-sensitive background tasks.

use std::{future::Future, sync::Arc};

use quant_pivot_models::{domain::ports::SystemCapabilityPort, enums::system::CapabilityId};
use tokio_util::sync::CancellationToken;

pub async fn wait_for_capability(
    capabilities: Arc<dyn SystemCapabilityPort>,
    capability: CapabilityId,
    shutdown: &CancellationToken,
) -> bool {
    let mut capabilities = capabilities.subscribe_capabilities();
    loop {
        if capabilities.borrow().get(capability).enabled {
            return true;
        }
        tokio::select! {
            biased;
            () = shutdown.cancelled() => return false,
            changed = capabilities.changed() => {
                if changed.is_err() {
                    return false;
                }
            }
        }
    }
}

/// Run a cancellation-aware worker only while one capability remains enabled.
///
/// A revoked capability cancels and drains the current worker instance. The task
/// then waits for a later capability revision before constructing a new worker.
pub async fn run_while_capable<F, Fut>(
    capabilities: Arc<dyn SystemCapabilityPort>,
    capability: CapabilityId,
    shutdown: CancellationToken,
    mut worker_factory: F,
) where
    F: FnMut(CancellationToken) -> Fut,
    Fut: Future<Output = ()>,
{
    loop {
        if !wait_for_capability(Arc::clone(&capabilities), capability, &shutdown).await {
            return;
        }

        let worker_token = shutdown.child_token();
        let worker = worker_factory(worker_token.clone());
        tokio::pin!(worker);
        let mut capability_rx = capabilities.subscribe_capabilities();

        loop {
            tokio::select! {
                biased;
                () = shutdown.cancelled() => {
                    worker_token.cancel();
                    worker.await;
                    return;
                }
                changed = capability_rx.changed() => {
                    if changed.is_err() {
                        worker_token.cancel();
                        worker.await;
                        return;
                    }
                    if !capability_rx.borrow().get(capability).enabled {
                        worker_token.cancel();
                        worker.await;
                        break;
                    }
                }
                () = &mut worker => return,
            }
        }
    }
}

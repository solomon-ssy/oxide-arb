//! Typed lifecycle gates for capability-sensitive background tasks.

use std::{future::Future, sync::Arc};

use quant_pivot_models::{domain::ports::BootstrapPort, enums::system::CapabilityId};
use tokio_util::sync::CancellationToken;

pub async fn wait_for_capability(
    bootstrap: Arc<dyn BootstrapPort>,
    capability: CapabilityId,
    shutdown: &CancellationToken,
) -> bool {
    let mut capabilities = bootstrap.subscribe_capabilities();
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
    bootstrap: Arc<dyn BootstrapPort>,
    capability: CapabilityId,
    shutdown: CancellationToken,
    mut worker_factory: F,
) where
    F: FnMut(CancellationToken) -> Fut,
    Fut: Future<Output = ()>,
{
    loop {
        if !wait_for_capability(Arc::clone(&bootstrap), capability, &shutdown).await {
            return;
        }

        let worker_token = shutdown.child_token();
        let worker = worker_factory(worker_token.clone());
        tokio::pin!(worker);
        let mut capabilities = bootstrap.subscribe_capabilities();

        loop {
            tokio::select! {
                biased;
                () = shutdown.cancelled() => {
                    worker_token.cancel();
                    worker.await;
                    return;
                }
                changed = capabilities.changed() => {
                    if changed.is_err() {
                        worker_token.cancel();
                        worker.await;
                        return;
                    }
                    if !capabilities.borrow().get(capability).enabled {
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

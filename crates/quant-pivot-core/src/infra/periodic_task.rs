use quant_pivot_error::QuantError;
use rand::RngExt;
use std::{future::Future, time::Duration};
use tokio_util::sync::CancellationToken;

pub struct PeriodicTask;

impl PeriodicTask {
    /// Periodic loop with a per-tick resolved interval.
    ///
    /// `interval_fn` is evaluated before every wait, so hot-reloadable
    /// cadences (runtime-config activation) take effect on the next tick —
    /// tasks must never capture an interval in their closure at startup.
    /// Fixed cadences simply pass a constant closure (`|| INTERVAL`).
    /// A zero interval is clamped to one second to avoid busy-looping on a
    /// misconfigured cadence.
    pub async fn run<F, Fut, I>(
        name: &str,
        interval_fn: I,
        jitter_pct: f64,
        skip_first_tick: bool,
        shutdown: CancellationToken,
        task_fn: F,
    ) -> Result<(), QuantError>
    where
        I: Fn() -> Duration,
        F: Fn() -> Fut,
        Fut: Future<Output = Result<(), QuantError>>,
    {
        if !skip_first_tick {
            let result = tokio::select! {
                biased;
                () = shutdown.cancelled() => return Ok(()),
                result = task_fn() => result,
            };
            if let Err(error) = result {
                tracing::warn!(task = name, %error, "periodic task iteration failed");
            }
        }

        loop {
            let interval = interval_fn().max(Duration::from_secs(1));
            tokio::select! {
                biased;
                () = shutdown.cancelled() => return Ok(()),
                () = tokio::time::sleep(interval) => {
                    if jitter_pct > 0.0 {
                        let max_jitter = interval.mul_f64(jitter_pct);
                        let jitter_roll = rand::rng().random_range(0.0..1.0);
                        tokio::select! {
                            biased;
                            () = shutdown.cancelled() => return Ok(()),
                            () = tokio::time::sleep(max_jitter.mul_f64(jitter_roll)) => {}
                        }
                    }
                    let result = tokio::select! {
                        biased;
                        () = shutdown.cancelled() => return Ok(()),
                        result = task_fn() => result,
                    };
                    if let Err(error) = result {
                        tracing::warn!(task = name, %error, "periodic task iteration failed");
                    }
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use std::{future, sync::Arc, time::Duration};

    use tokio::sync::Notify;
    use tokio_util::sync::CancellationToken;

    use super::PeriodicTask;

    #[tokio::test]
    async fn cancellation_interrupts_an_in_flight_iteration() {
        let shutdown = CancellationToken::new();
        let entered = Arc::new(Notify::new());
        let task_entered = Arc::clone(&entered);
        let task_shutdown = shutdown.clone();
        let handle = tokio::spawn(async move {
            PeriodicTask::run(
                "cancellation-test",
                || Duration::from_mins(1),
                0.0,
                false,
                task_shutdown,
                move || {
                    let entered = Arc::clone(&task_entered);
                    async move {
                        entered.notify_one();
                        future::pending().await
                    }
                },
            )
            .await
        });

        entered.notified().await;
        shutdown.cancel();
        let result = tokio::time::timeout(Duration::from_millis(100), handle)
            .await
            .expect("periodic task must honor cancellation")
            .expect("periodic task join");
        assert!(result.is_ok());
    }
}

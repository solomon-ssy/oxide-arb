use quant_pivot_error::QuantError;
use rand::RngExt;
use std::time::Duration;
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
        Fut: std::future::Future<Output = Result<(), QuantError>>,
    {
        if !skip_first_tick {
            if shutdown.is_cancelled() {
                return Ok(());
            }
            if let Err(e) = task_fn().await {
                tracing::warn!(task = name, error = %e, "periodic task iteration failed");
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
                        tokio::time::sleep(max_jitter.mul_f64(jitter_roll)).await;
                    }
                    if let Err(e) = task_fn().await {
                        tracing::warn!(task = name, error = %e, "periodic task iteration failed");
                    }
                }
            }
        }
    }
}

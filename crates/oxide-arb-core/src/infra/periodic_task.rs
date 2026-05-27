use oxide_arb_error::OxideError;
use rand::RngExt;
use std::time::Duration;
use tokio_util::sync::CancellationToken;

pub struct PeriodicTask;

impl PeriodicTask {
    pub async fn run<F, Fut>(
        name: &str,
        interval: Duration,
        jitter_pct: f64,
        shutdown: CancellationToken,
        task_fn: F,
    ) -> Result<(), OxideError>
    where
        F: Fn() -> Fut,
        Fut: std::future::Future<Output = Result<(), OxideError>>,
    {
        let mut timer = tokio::time::interval(interval);
        timer.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);

        loop {
            tokio::select! {
                biased;
                () = shutdown.cancelled() => return Ok(()),
                _ = timer.tick() => {
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

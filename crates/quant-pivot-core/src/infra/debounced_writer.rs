use oxide_arb_error::OxideError;
use parking_lot::Mutex;
use std::{future::Future, pin::Pin, sync::Arc, time::Duration};
use tokio_util::sync::CancellationToken;

pub struct DebouncedWriter<T: Clone + Send + 'static> {
    latest: Arc<Mutex<Option<T>>>,
}

impl<T: Clone + Send + 'static> DebouncedWriter<T> {
    pub fn new<F>(
        name: impl Into<String>,
        interval: Duration,
        write_fn: F,
        shutdown: CancellationToken,
    ) -> (Self, impl Future<Output = Result<(), OxideError>>)
    where
        F: Fn(T) -> Pin<Box<dyn Future<Output = Result<(), OxideError>> + Send>> + Send + 'static,
    {
        let latest: Arc<Mutex<Option<T>>> = Arc::new(Mutex::new(None));
        let writer = Self {
            latest: latest.clone(),
        };
        let name = name.into();

        let worker = async move {
            let mut timer = tokio::time::interval(interval);
            timer.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);

            loop {
                tokio::select! {
                    biased;
                    () = shutdown.cancelled() => {
                        let val = latest.lock().take();
                        if let Some(val) = val {
                            if let Err(e) = write_fn(val).await {
                                tracing::warn!(writer = %name, error = %e, "final debounced flush failed");
                            }
                        }
                        return Ok(());
                    }
                    _ = timer.tick() => {
                        let val = latest.lock().take();
                        if let Some(val) = val {
                            if let Err(e) = write_fn(val).await {
                                tracing::warn!(writer = %name, error = %e, "debounced flush failed");
                            }
                        }
                    }
                }
            }
        };

        (writer, worker)
    }

    pub fn update(&self, value: T) {
        *self.latest.lock() = Some(value);
    }
}

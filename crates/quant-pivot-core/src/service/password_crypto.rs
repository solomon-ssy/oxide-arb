//! Bounded asynchronous boundary for Argon2 password work.

use std::{sync::Arc, time::Duration};

use async_trait::async_trait;
use quant_pivot_compute::ComputeExecutor;
use quant_pivot_error::{QuantError, QuantResult, infra::InfraError};
use quant_pivot_models::{
    config::PasswordCryptoConfig,
    domain::ports::PasswordCryptoPort,
    security::{hash_password as hash_secret, verify_password as verify_secret},
};
use tokio::{sync::Semaphore, time::timeout};

const DUMMY_PASSWORD: &str = "quant-pivot::login-timing-guard";

/// Isolates credential hashing from Actix/Tokio workers and bounds queued
/// plaintext material.
///
/// The compute executor owns the dedicated one-thread CPU
/// pool; this service owns request admission and the end-to-end deadline.
pub struct PasswordCryptoService {
    compute: Arc<ComputeExecutor>,
    admission: Arc<Semaphore>,
    max_in_flight: usize,
    deadline: Duration,
    deadline_ms: u64,
    dummy_hash: String,
}

impl PasswordCryptoService {
    /// Build the service and precompute the unknown-user timing guard before the
    /// web server accepts traffic.
    pub async fn new(
        compute: Arc<ComputeExecutor>,
        config: &PasswordCryptoConfig,
    ) -> QuantResult<Self> {
        if !(1..=64).contains(&config.max_in_flight)
            || !(1_000..=60_000).contains(&config.deadline_ms)
        {
            return Err(InfraError::Misconfigured {
                detail: "web password crypto budget is outside its validated range".to_owned(),
            }
            .into());
        }
        let deadline = Duration::from_millis(config.deadline_ms);
        let dummy_hash = timeout(
            deadline,
            compute.run_security(|| hash_secret(DUMMY_PASSWORD).map_err(QuantError::from)),
        )
        .await
        .map_err(|_| InfraError::Misconfigured {
            detail: "password timing guard exceeded the configured startup deadline".to_owned(),
        })??;
        Ok(Self {
            compute,
            admission: Arc::new(Semaphore::new(config.max_in_flight)),
            max_in_flight: config.max_in_flight,
            deadline,
            deadline_ms: config.deadline_ms,
            dummy_hash,
        })
    }

    async fn run<T, F>(&self, work: F) -> QuantResult<T>
    where
        T: Send + 'static,
        F: FnOnce() -> QuantResult<T> + Send + 'static,
    {
        let admission = Arc::clone(&self.admission)
            .try_acquire_owned()
            .map_err(|_| InfraError::ComputeCapacity {
                subsystem: "password crypto",
                limit: self.max_in_flight,
            })?;
        let execute = self.compute.run_security(move || {
            let _admission = admission;
            work()
        });
        timeout(self.deadline, execute)
            .await
            .map_err(|_| InfraError::ComputeDeadline {
                subsystem: "password crypto",
                deadline_ms: self.deadline_ms,
            })?
    }
}

#[async_trait]
impl PasswordCryptoPort for PasswordCryptoService {
    async fn hash(&self, plaintext: String) -> QuantResult<String> {
        self.run(move || hash_secret(&plaintext).map_err(QuantError::from))
            .await
    }

    async fn verify(&self, plaintext: String, stored_hash: Option<String>) -> QuantResult<bool> {
        let phc = stored_hash.unwrap_or_else(|| self.dummy_hash.clone());
        self.run(move || Ok(verify_secret(&plaintext, &phc))).await
    }
}

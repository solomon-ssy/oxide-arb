//! WebSocket reconnection strategy — delegates to [`crate::infra::retry`].
//!
//! This module provides a thin adapter over [`RetryController`] that is
//! specialized for WebSocket shard lifecycle (unlimited retries by default,
//! shard-aware logging).

use std::time::Duration;

use crate::infra::retry::{RetryController, RetryDecision, RetryPolicy};

/// Reconnection policy for a WebSocket shard.
///
/// Wraps [`RetryPolicy`] with a shard-aware interface.
#[derive(Debug, Clone)]
pub struct ReconnectPolicy {
    inner: RetryPolicy,
}

impl ReconnectPolicy {
    pub const fn new(policy: RetryPolicy) -> Self {
        Self { inner: policy }
    }

    pub const fn policy(&self) -> &RetryPolicy {
        &self.inner
    }
}

impl Default for ReconnectPolicy {
    fn default() -> Self {
        Self {
            inner: RetryPolicy::ws_reconnect(),
        }
    }
}

/// Tracks reconnection state for a single shard.
///
/// Wraps [`RetryController`] — delegates backoff math to the `backoff` crate.
pub struct ReconnectState {
    controller: RetryController,
    shard_id: usize,
}

impl ReconnectState {
    pub fn new(shard_id: usize, policy: &ReconnectPolicy) -> Self {
        Self {
            controller: RetryController::new(policy.policy()),
            shard_id,
        }
    }

    /// Get the next backoff delay, or `None` if exhausted.
    pub fn next_delay(&mut self) -> Option<Duration> {
        match self.controller.on_failure() {
            RetryDecision::RetryAfter(dur) => {
                tracing::debug!(
                    shard_id = self.shard_id,
                    attempt = self.controller.retries_used(),
                    backoff_ms = dur.as_millis(),
                    "WS shard scheduling reconnect"
                );
                Some(dur)
            }
            RetryDecision::Exhausted => {
                tracing::error!(
                    shard_id = self.shard_id,
                    retries_used = self.controller.retries_used(),
                    "WS shard reconnection budget exhausted"
                );
                None
            }
        }
    }

    /// Reset after successful connection.
    pub fn reset(&mut self) {
        self.controller.reset();
    }

    pub const fn retries_used(&self) -> u32 {
        self.controller.retries_used()
    }
}

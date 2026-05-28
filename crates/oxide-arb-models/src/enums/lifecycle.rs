//! Lifecycle enums — shutdown progression tracking.

use serde::{Deserialize, Serialize};
use std::fmt::{self, Display, Formatter};

/// Graceful shutdown stage.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ShutdownStage {
    /// Signal received, draining new work.
    Draining,
    /// Awaiting in-flight operations to complete.
    AwaitingInflight,
    /// Flushing persistence buffers.
    Flushing,
    /// All subsystems stopped.
    Stopped,
}

impl Display for ShutdownStage {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        match self {
            Self::Draining => f.write_str("draining"),
            Self::AwaitingInflight => f.write_str("awaiting_inflight"),
            Self::Flushing => f.write_str("flushing"),
            Self::Stopped => f.write_str("stopped"),
        }
    }
}

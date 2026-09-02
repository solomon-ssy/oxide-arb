//! Strongly-typed `sync` full-state snapshot.

use serde::Serialize;

use crate::domain::api::SystemStatusView;

/// Authorized projection of live system state, returned for a `sync` command.
#[derive(Debug, Clone, Default, Serialize)]
pub struct SyncSnapshot {
    /// Control-plane status / uptime snapshot (requires `System` read).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub system_status: Option<SystemStatusView>,
}

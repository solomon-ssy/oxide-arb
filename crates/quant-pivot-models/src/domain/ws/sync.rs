//! Strongly-typed `sync` full-state snapshot (Phase 0).

use serde::Serialize;

use crate::domain::SystemStatusView;

/// Authorized projection of live system state, returned for a `sync` command.
#[derive(Debug, Clone, Default, Serialize)]
pub struct SyncSnapshot {
    /// Quant runtime mode / uptime snapshot (requires `System` read).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub system_status: Option<SystemStatusView>,
}

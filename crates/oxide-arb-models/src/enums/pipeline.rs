//! Data-pipeline control-plane vocabulary.

use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ShardConnectionStatus {
    Connected,
    Disconnected,
    Reconnecting { attempt: u32 },
}

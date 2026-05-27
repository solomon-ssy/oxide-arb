//! WS output drop hook invoked when the shard output channel is saturated.

use std::sync::Arc;

/// Called with the number of events dropped in one dispatch batch.
pub type WsEventDropHook = Arc<dyn Fn(u64) + Send + Sync>;

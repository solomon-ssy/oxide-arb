//! Hook invoked when bounded enqueue failure invalidates a WS stream session.

use std::sync::Arc;

/// Called once for each stream session invalidated by output backpressure.
pub type WsSessionInvalidationHook = Arc<dyn Fn(u64) + Send + Sync>;

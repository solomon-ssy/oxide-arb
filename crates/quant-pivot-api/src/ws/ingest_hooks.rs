//! Ingest-side hooks for WS normalization metrics.

use std::sync::Arc;

/// Invoked when a book level is rejected during WS ingest.
pub type BookLevelRejectHook = Arc<dyn Fn() + Send + Sync>;

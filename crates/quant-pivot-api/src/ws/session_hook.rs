//! Hook invoked when bounded enqueue failure invalidates a WS stream session.

use std::sync::Arc;

use quant_pivot_models::types::TokenId;

/// Called with the complete token set whenever stream continuity is invalidated.
pub type WsSessionInvalidationHook = Arc<dyn Fn(&[TokenId]) + Send + Sync>;

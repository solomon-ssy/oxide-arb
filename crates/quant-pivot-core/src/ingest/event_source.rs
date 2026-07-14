//! Injectable sources for the data pipeline event loop.

use flume::Receiver;
use quant_pivot_api::ws::ClobWsManager;
use quant_pivot_models::{domain::pipeline::PipelineEvent, types::TokenId};
use std::time::Instant;

/// Unified read side for WS-normalized pipeline events.
pub trait PipelineEventSource: Send + Sync {
    fn events(&self) -> &Receiver<PipelineEvent>;
    fn mark_token_applied(&self, token_id: &TokenId, at: Instant);
    fn invalidate_token(&self, token_id: &TokenId);
}

impl PipelineEventSource for ClobWsManager {
    fn events(&self) -> &Receiver<PipelineEvent> {
        Self::events(self)
    }

    fn mark_token_applied(&self, token_id: &TokenId, at: Instant) {
        Self::mark_token_applied(self, token_id, at);
    }

    fn invalidate_token(&self, token_id: &TokenId) {
        Self::invalidate_token(self, token_id);
    }
}

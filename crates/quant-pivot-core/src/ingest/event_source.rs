//! Injectable sources for the data pipeline event loop.

use flume::Receiver;
use quant_pivot_api::ws::{ClobWsManager, NormalizedIngressBatch};
use quant_pivot_models::types::TokenId;

/// Unified read side for WS-normalized pipeline events.
pub trait PipelineEventSource: Send + Sync {
    fn events(&self) -> &Receiver<NormalizedIngressBatch>;
    fn owns_all_tokens(&self, token_ids: &[TokenId]) -> bool;
    fn invalidate_token(&self, token_id: &TokenId);
    fn invalidate_tokens(&self, token_ids: &[TokenId]);
}

impl PipelineEventSource for ClobWsManager {
    fn events(&self) -> &Receiver<NormalizedIngressBatch> {
        Self::events(self)
    }

    fn owns_all_tokens(&self, token_ids: &[TokenId]) -> bool {
        Self::owns_all_tokens(self, token_ids)
    }

    fn invalidate_token(&self, token_id: &TokenId) {
        Self::invalidate_token(self, token_id);
    }

    fn invalidate_tokens(&self, token_ids: &[TokenId]) {
        Self::invalidate_tokens(self, token_ids);
    }
}

//! Injectable sources for the data pipeline event loop.

use flume::Receiver;
use quant_pivot_api::ws::ClobWsManager;
use quant_pivot_models::domain::pipeline::PipelineEvent;

/// Unified read side for WS-normalized pipeline events.
pub trait PipelineEventSource: Send + Sync {
    fn events(&self) -> &Receiver<PipelineEvent>;
}

impl PipelineEventSource for ClobWsManager {
    fn events(&self) -> &Receiver<PipelineEvent> {
        Self::events(self)
    }
}

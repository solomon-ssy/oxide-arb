//! Injectable sources for the data pipeline event loop.

use flume::Receiver;
use oxide_arb_api::ws::ClobWsManager;
use oxide_arb_models::domain::pipeline::PipelineEvent;

/// Unified read side for WS-normalized pipeline events.
pub trait PipelineEventSource: Send + Sync {
    fn events(&self) -> &Receiver<PipelineEvent>;
}

impl PipelineEventSource for ClobWsManager {
    fn events(&self) -> &Receiver<PipelineEvent> {
        Self::events(self)
    }
}

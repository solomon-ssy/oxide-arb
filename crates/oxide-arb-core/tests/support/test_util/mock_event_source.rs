//! Injectable [`PipelineEvent`] source for `DataPipeline` tests (PR-3 wiring).

use flume::{Receiver, Sender};
use oxide_arb_models::domain::pipeline::PipelineEvent;

use oxide_arb_core::pipeline::event_source::PipelineEventSource;

/// Bounded in-memory pipeline event bus for tests.
///
/// Returns `(source, inject)` where `source.receiver()` feeds the pipeline and
/// `inject` pushes synthetic WS-normalized events.
pub struct MockEventSource {
    rx: Receiver<PipelineEvent>,
}

/// Handle for injecting events into a [`MockEventSource`].
#[derive(Clone)]
pub struct MockEventInject {
    tx: Sender<PipelineEvent>,
}

impl MockEventSource {
    /// Create a paired source/inject with the given channel capacity.
    #[must_use]
    pub fn paired(capacity: usize) -> (Self, MockEventInject) {
        let (tx, rx) = flume::bounded(capacity);
        (Self { rx }, MockEventInject { tx })
    }

    /// Consumer side — attach to `DataPipeline` in PR-3 via `PipelineEventSource`.
    #[must_use]
    pub const fn receiver(&self) -> &Receiver<PipelineEvent> {
        &self.rx
    }
}

impl MockEventInject {
    /// Blocking send for deterministic test setup.
    ///
    /// # Panics
    /// Panics if the receiver is dropped.
    pub fn send(&self, event: PipelineEvent) {
        self.tx.send(event).expect("mock pipeline receiver dropped");
    }
}

impl PipelineEventSource for MockEventSource {
    fn events(&self) -> &Receiver<PipelineEvent> {
        Self::receiver(self)
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::time::Instant;

    use oxide_arb_models::domain::book::BookLevel;
    use oxide_arb_models::domain::pipeline::{
        BookSideData, BookSnapshotCmd, IngressTrace, PipelineEvent,
    };
    use oxide_arb_models::types::{Price, Shares, TokenId};

    use super::{MockEventInject, MockEventSource};

    #[test]
    fn roundtrip_book_snapshot_event() {
        let (source, inject) = MockEventSource::paired(4);
        let level = BookLevel::from_decimal_unchecked(
            Price::new(rust_decimal_macros::dec!(0.5)),
            Shares::new(rust_decimal_macros::dec!(10)),
        );
        let cmd = BookSnapshotCmd {
            asset_id: TokenId::new("t1"),
            bids: BookSideData::from_levels(Arc::from([level])),
            asks: BookSideData::empty(),
            timestamp_ms: 1,
            trace: IngressTrace::new(Instant::now(), 1),
        };
        inject.send(PipelineEvent::BookSnapshot(cmd));
        assert!(matches!(
            source.receiver().try_recv().expect("event"),
            PipelineEvent::BookSnapshot(_)
        ));
    }

    #[test]
    fn inject_clone_shares_sender() {
        let (_source, inject) = MockEventSource::paired(2);
        let inject2 = MockEventInject::clone(&inject);
        drop(inject);
        let cmd = BookSnapshotCmd {
            asset_id: TokenId::new("t2"),
            bids: BookSideData::empty(),
            asks: BookSideData::empty(),
            timestamp_ms: 0,
            trace: IngressTrace::new(Instant::now(), 0),
        };
        inject2.send(PipelineEvent::BookSnapshot(cmd));
    }
}

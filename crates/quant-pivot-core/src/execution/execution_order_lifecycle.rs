//! Central publisher for committed execution-order lifecycle transitions.

use chrono::{DateTime, Utc};
use quant_pivot_models::domain::{
    quant::ExecutionOrderInfo,
    runtime::{
        CoreEvent, CoreEventPublisher, ExecutionOrderEventKind, ExecutionOrderLifecycleEvent,
    },
};

/// Publishes `quant.execution_order` revision hints after durable writes commit.
#[derive(Clone)]
pub struct ExecutionOrderLifecyclePublisher {
    events: CoreEventPublisher,
}

impl ExecutionOrderLifecyclePublisher {
    #[must_use]
    pub const fn new(events: CoreEventPublisher) -> Self {
        Self { events }
    }

    /// Publish the write-ahead creation returned by the owning transaction.
    pub fn created(&self, order: &ExecutionOrderInfo, at: DateTime<Utc>) {
        self.publish(order, ExecutionOrderEventKind::Created, at);
    }

    /// Publish a committed state transition. Unchanged rows are deliberately silent.
    pub fn transition(
        &self,
        prior: &ExecutionOrderInfo,
        committed: &ExecutionOrderInfo,
        at: DateTime<Utc>,
    ) {
        if prior.state == committed.state {
            return;
        }
        self.publish(committed, committed.state.into(), at);
    }

    fn publish(
        &self,
        order: &ExecutionOrderInfo,
        kind: ExecutionOrderEventKind,
        at: DateTime<Utc>,
    ) {
        self.events.publish(CoreEvent::ExecutionOrder(
            ExecutionOrderLifecycleEvent::committed(order, kind, at),
        ));
    }
}

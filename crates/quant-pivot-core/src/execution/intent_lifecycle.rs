//! Central order-intent lifecycle event publisher.
//!
//! The single place that turns a committed `quant_order_intent` transition into
//! a `quant.intent` WebSocket frame. The intent service publishes pre-submission
//! transitions with an explicit [`IntentEventKind`]; the dispatcher and the
//! reconciliation service publish post-submission transitions via
//! [`IntentLifecyclePublisher::publish_transition`], which fans out the
//! venue-settled outcome only when the status actually advanced. Events are
//! always published **after** the owning transaction commits.

use chrono::{DateTime, Utc};
use quant_pivot_models::{
    domain::{CoreEvent, CoreEventPublisher, IntentLifecycleEvent, OrderIntentInfo},
    enums::quant::OrderIntentStatus,
};

use quant_pivot_models::domain::IntentEventKind;

/// Publishes `quant.intent` lifecycle events on the shared core event bus.
#[derive(Clone)]
pub struct IntentLifecyclePublisher {
    events: CoreEventPublisher,
}

impl IntentLifecyclePublisher {
    /// Wrap the shared event publisher (a cheap clone of the bounded sender).
    #[must_use]
    pub const fn new(events: CoreEventPublisher) -> Self {
        Self { events }
    }

    /// A cheap clone of the underlying core event publisher, so sibling
    /// execution services (reconciliation / settlement) can fan out their own
    /// lifecycle channels without threading a second publisher through the boot
    /// wiring.
    #[must_use]
    pub fn publisher(&self) -> CoreEventPublisher {
        self.events.clone()
    }

    /// Publish the explicit lifecycle event for a committed transition.
    pub fn publish(&self, intent: &OrderIntentInfo, kind: IntentEventKind, at: DateTime<Utc>) {
        self.events
            .publish(CoreEvent::Intent(IntentLifecycleEvent::from_intent(
                intent, kind, at,
            )));
    }

    /// Publish the post-submission event for `intent` when its status advanced
    /// from `prior` and maps to an observable venue-settled transition. A no-op
    /// when the status is unchanged (e.g. an exit-order reconciliation that
    /// leaves a filled entry intent terminal) or has no post-submission event.
    pub fn publish_transition(
        &self,
        prior: OrderIntentStatus,
        intent: &OrderIntentInfo,
        at: DateTime<Utc>,
    ) {
        if intent.status == prior {
            return;
        }
        if let Some(kind) = IntentEventKind::for_execution_status(intent.status) {
            self.publish(intent, kind, at);
        }
    }
}

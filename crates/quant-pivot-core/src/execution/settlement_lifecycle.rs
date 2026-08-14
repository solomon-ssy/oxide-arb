//! Central publisher for committed settlement-redeem lifecycle transitions.

use quant_pivot_models::domain::{
    quant::settlement::SettlementRedeemInfo,
    runtime::{CoreEvent, CoreEventPublisher, SettlementRedeemLifecycleEvent},
};

/// Publishes `quant.settlement` revision hints from committed repository rows.
#[derive(Clone)]
pub struct SettlementLifecyclePublisher {
    events: CoreEventPublisher,
}

impl SettlementLifecyclePublisher {
    #[must_use]
    pub const fn new(events: CoreEventPublisher) -> Self {
        Self { events }
    }

    pub fn committed(&self, redeem: &SettlementRedeemInfo) {
        self.events.publish(CoreEvent::Settlement(
            SettlementRedeemLifecycleEvent::committed(redeem),
        ));
    }
}

//! Reconciliation closed loop.
//!
//! Brings the internal ledgers (`quant_execution_order` / `quant_capital_allocation`
//! / `quant_strategy_position_lot`) into agreement with Polymarket venue truth. For every
//! truth-unknown order the engine collects venue evidence in a fixed order,
//! decides a terminal verdict, and applies a single idempotent correction.
//! An `Unresolvable` verdict latches the kill-switch (fail-closed) until an
//! operator resolves it.

mod collector;
mod decide;
mod reader;
mod service;

pub use collector::{CollectedReconciliation, EvidenceCollector, VenueEvidenceCollector};
pub use decide::{
    ReconcileFacts, ReconciliationDecision, ShareSettlementBasis, ShareSettlementProjection,
    VenuePresence,
};
pub use reader::{ClobReconciliationReader, VenueReconciliationReader};
pub use service::{OperatorReconcileResolution, ReconciliationService, ReconciliationServiceDeps};

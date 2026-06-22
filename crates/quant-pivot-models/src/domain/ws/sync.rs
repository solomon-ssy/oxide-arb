//! Strongly-typed `sync` full-state snapshot.
//!
//! Replaces the ad-hoc `serde_json::Map` + string keys with a struct whose field
//! names *are* the wire keys. Every section is optional: the session loop fills
//! only the sections the connection is authorized to read (each gated by the same
//! RBAC `Read` check as its HTTP route), and unauthorized sections are omitted
//! entirely via `skip_serializing_if` rather than serialized as `null`.

use serde::Serialize;

use crate::domain::{
    ControlFactorMaterializationRunView, LivePnlView, MaterializationScheduleStatusView,
    OpportunityView, PositionView, RiskEngineStateView, SystemStatus,
};

/// Authorized projection of live system state, returned for a `sync` command.
///
/// Each section mirrors its HTTP counterpart's outbound view, so a `sync` can
/// never leak internal columns the REST routes strip.
#[derive(Debug, Clone, Default, Serialize)]
pub struct SyncSnapshot {
    /// Execution mode / breaker / uptime snapshot (requires `System` read).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub system_status: Option<SystemStatus>,
    /// Live risk-engine state view (requires `Risk` read).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub risk: Option<RiskEngineStateView>,
    /// Currently open positions (requires `Risk` read).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub open_positions: Option<Vec<PositionView>>,
    /// Live `PnL` snapshot (requires `Pnl` read).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub pnl: Option<LivePnlView>,
    /// Recently detected opportunities (requires `Opportunity` read), projected
    /// through the same [`OpportunityView`] as the `opportunity.detected` push
    /// so the feed consumes one wire shape on both paths.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub recent_opportunities: Option<Vec<OpportunityView>>,
    /// Active materialization runs (`Queued` / `Running`), requires `ControlFactor` read.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub active_materialization_runs: Option<Vec<ControlFactorMaterializationRunView>>,
    /// Mode-aware materialization schedule status, requires `ControlFactor` read.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub materialization_schedules: Option<Vec<MaterializationScheduleStatusView>>,
}

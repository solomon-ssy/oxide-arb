//! Lifecycle phase enums — tracking opportunity and trade progression.

use oxide_arb_macros::IntoActiveValue;
use sea_orm::{DeriveActiveEnum, EnumIter};
use serde::{Deserialize, Serialize};

/// Pipeline phase of an opportunity / trade lifecycle event.
#[derive(
    Debug,
    Clone,
    Copy,
    PartialEq,
    Eq,
    Hash,
    Serialize,
    Deserialize,
    EnumIter,
    DeriveActiveEnum,
    IntoActiveValue,
)]
#[sea_orm(rs_type = "String", db_type = "Text")]
#[serde(rename_all = "snake_case")]
pub enum LifecyclePhase {
    #[sea_orm(string_value = "detected")]
    Detected,
    #[sea_orm(string_value = "scored")]
    Scored,
    #[sea_orm(string_value = "risk_checked")]
    RiskChecked,
    #[sea_orm(string_value = "sized")]
    Sized,
    #[sea_orm(string_value = "validated")]
    Validated,
    #[sea_orm(string_value = "dispatched")]
    Dispatched,
    #[sea_orm(string_value = "filled_partial")]
    FilledPartial,
    #[sea_orm(string_value = "filled_full")]
    FilledFull,
    #[sea_orm(string_value = "rejected")]
    Rejected,
    #[sea_orm(string_value = "settled")]
    Settled,
    #[sea_orm(string_value = "reconciled")]
    Reconciled,
    #[sea_orm(string_value = "exit_planned")]
    ExitPlanned,
    #[sea_orm(string_value = "exit_executed")]
    ExitExecuted,
    #[sea_orm(string_value = "exit_confirmed")]
    ExitConfirmed,
    #[sea_orm(string_value = "kill_switch_triggered")]
    KillSwitchTriggered,
}

/// Subsystem that recorded the lifecycle event.
#[derive(
    Debug,
    Clone,
    Copy,
    PartialEq,
    Eq,
    Hash,
    Serialize,
    Deserialize,
    EnumIter,
    DeriveActiveEnum,
    IntoActiveValue,
)]
#[sea_orm(rs_type = "String", db_type = "Text")]
#[serde(rename_all = "snake_case")]
pub enum LifecycleRecorder {
    #[sea_orm(string_value = "scanner")]
    Scanner,
    #[sea_orm(string_value = "execution")]
    Execution,
    #[sea_orm(string_value = "risk_engine")]
    RiskEngine,
    #[sea_orm(string_value = "oracle_poller")]
    OraclePoller,
    #[sea_orm(string_value = "backfill")]
    Backfill,
    #[sea_orm(string_value = "dry_run")]
    DryRun,
    #[sea_orm(string_value = "system")]
    System,
}

impl std::fmt::Display for LifecycleRecorder {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Scanner => f.write_str("scanner"),
            Self::Execution => f.write_str("execution"),
            Self::RiskEngine => f.write_str("risk_engine"),
            Self::OraclePoller => f.write_str("oracle_poller"),
            Self::Backfill => f.write_str("backfill"),
            Self::DryRun => f.write_str("dry_run"),
            Self::System => f.write_str("system"),
        }
    }
}

impl std::str::FromStr for LifecycleRecorder {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "scanner" => Ok(Self::Scanner),
            "execution" => Ok(Self::Execution),
            "risk_engine" => Ok(Self::RiskEngine),
            "oracle_poller" => Ok(Self::OraclePoller),
            "backfill" => Ok(Self::Backfill),
            "dry_run" => Ok(Self::DryRun),
            "system" => Ok(Self::System),
            other => Err(format!("unknown lifecycle recorder: {other}")),
        }
    }
}

/// Graceful shutdown stage.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ShutdownStage {
    /// Signal received, draining new work.
    Draining,
    /// Awaiting in-flight operations to complete.
    AwaitingInflight,
    /// Flushing persistence buffers.
    Flushing,
    /// All subsystems stopped.
    Stopped,
}

impl std::fmt::Display for ShutdownStage {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Draining => f.write_str("draining"),
            Self::AwaitingInflight => f.write_str("awaiting_inflight"),
            Self::Flushing => f.write_str("flushing"),
            Self::Stopped => f.write_str("stopped"),
        }
    }
}

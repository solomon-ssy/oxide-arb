//! Operator dashboard aggregate HTTP contract.

use std::collections::BTreeMap;

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

use crate::{
    domain::{
        api::{
            DependencyCheck, EquitySnapshotView, LiveAccountView, QuantRecommendationView,
            QuantReportView, SystemStatusView,
        },
        data_plane::DataQualitySnapshot,
    },
    types::ExposureBreakdown,
};

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Deserialize, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum DashboardWindow {
    #[serde(rename = "24h")]
    Hours24,
    #[default]
    #[serde(rename = "7d")]
    Days7,
    #[serde(rename = "30d")]
    Days30,
}

impl DashboardWindow {
    #[must_use]
    pub const fn seconds(self) -> i64 {
        match self {
            Self::Hours24 => 86_400,
            Self::Days7 => 604_800,
            Self::Days30 => 2_592_000,
        }
    }
}

#[derive(Debug, Clone, Copy, Default, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct DashboardOverviewQuery {
    #[serde(default)]
    pub window: DashboardWindow,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum DashboardReasonCode {
    DependencyUnavailable,
    EvidenceMissing,
    NoAccountSnapshot,
    NoReport,
    NoSamples,
    SnapshotTooOld,
    TimedOut,
}

/// Independent dashboard section state. A failed or forbidden section never
/// destroys sibling data from the same overview revision.
#[derive(Debug, Clone, Serialize)]
#[serde(tag = "state", rename_all = "snake_case")]
pub enum DashboardSection<T> {
    Ready {
        observed_at: DateTime<Utc>,
        value: T,
    },
    Stale {
        observed_at: DateTime<Utc>,
        value: T,
        reason_code: DashboardReasonCode,
    },
    Unavailable {
        reason_code: DashboardReasonCode,
    },
    Forbidden,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum DashboardPrimaryAction {
    ResolveReconciliation,
    RunReport,
    ViewBlockers,
}

#[derive(Debug, Clone, Serialize)]
pub struct DashboardAuthorityView {
    pub system: SystemStatusView,
    pub primary_action: DashboardPrimaryAction,
    pub primary_action_enabled: bool,
}

#[derive(Debug, Clone, Serialize)]
pub struct DashboardAccountView {
    pub live: LiveAccountView,
    pub latest_equity: Option<EquitySnapshotView>,
}

#[derive(Debug, Clone, Serialize)]
pub struct DashboardReportView {
    pub report: QuantReportView,
    pub recommendations: Vec<QuantRecommendationView>,
}

#[derive(Debug, Clone, Serialize)]
pub struct DashboardLifecycleView {
    pub counts: BTreeMap<String, u64>,
    pub total: u64,
}

#[derive(Debug, Clone, Serialize)]
pub struct DashboardExposureView {
    pub exposures: ExposureBreakdown,
    pub position_count: u64,
}

#[derive(Debug, Clone, Serialize)]
pub struct DashboardResearchReadinessView {
    pub required_history_days: u32,
    pub observed_history_days: Option<u32>,
    pub factor_gate_ready: bool,
    pub model_gate_ready: bool,
}

#[derive(Debug, Clone, Serialize)]
pub struct DashboardSubsystemHealthView {
    pub ready: bool,
    pub checks: Vec<DependencyCheck>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum DashboardActionSeverity {
    Critical,
    Warning,
    Info,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum DashboardActionReasonCode {
    BasisAlertUnacknowledged,
    KillSwitchNotClosed,
    MarketDataDegraded,
    PolicyRevisionAwaitingActivation,
    ReportRunFailed,
    UnresolvedReconciliation,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum DashboardActionOwner {
    Data,
    Governance,
    Operations,
    Research,
    Risk,
}

#[derive(Debug, Clone, Serialize)]
pub struct DashboardActionItemView {
    pub id: String,
    pub severity: DashboardActionSeverity,
    pub reason_code: DashboardActionReasonCode,
    pub owner: DashboardActionOwner,
    pub observed_at: DateTime<Utc>,
    pub target_route: String,
}

#[derive(Debug, Clone, Serialize)]
pub struct DashboardOverviewView {
    pub revision: String,
    pub generated_at: DateTime<Utc>,
    pub window: DashboardWindow,
    pub authority: DashboardSection<DashboardAuthorityView>,
    pub account: DashboardSection<DashboardAccountView>,
    pub equity_curve: DashboardSection<Vec<EquitySnapshotView>>,
    pub latest_report: DashboardSection<DashboardReportView>,
    pub report_lifecycle: DashboardSection<DashboardLifecycleView>,
    pub exposures: DashboardSection<DashboardExposureView>,
    pub data_quality: DashboardSection<DataQualitySnapshot>,
    pub research_readiness: DashboardSection<DashboardResearchReadinessView>,
    pub subsystem_health: DashboardSection<DashboardSubsystemHealthView>,
    pub action_inbox: DashboardSection<Vec<DashboardActionItemView>>,
}

#[cfg(test)]
mod tests {
    use chrono::Utc;
    use rust_decimal_macros::dec;

    use super::{DashboardOverviewQuery, DashboardReasonCode, DashboardSection, DashboardWindow};
    use crate::types::Usd;

    #[test]
    fn window_contract_closed_days() {
        let default = serde_json::from_str::<DashboardOverviewQuery>("{}").expect("default window");
        assert_eq!(default.window, DashboardWindow::Days7);
        assert!(
            serde_json::from_str::<super::DashboardOverviewQuery>(r#"{"window":"days365"}"#)
                .is_err()
        );
    }

    #[test]
    fn section_serialization_explicitly_tagged() {
        let section = DashboardSection::<u8>::Unavailable {
            reason_code: DashboardReasonCode::TimedOut,
        };
        let json = serde_json::to_value(section).expect("serialize section");
        assert_eq!(json["state"], "unavailable");
        assert_eq!(json["reason_code"], "timed_out");
    }

    #[test]
    fn monetary_sections_preserve_strings() {
        let section = DashboardSection::Ready {
            observed_at: Utc::now(),
            value: Usd::new(dec!(1234567890.12345678)),
        };
        let json = serde_json::to_value(section).expect("serialize monetary section");
        assert_eq!(json["value"], "1234567890.12345678");
    }
}

//! Single-revision operator dashboard aggregate.

use std::{collections::BTreeMap, future::Future, time::Duration};

use actix_web::{
    HttpResponse,
    http::{Method, header::CACHE_CONTROL},
    web::{Data, Query},
};
use chrono::{DateTime, Duration as ChronoDuration, Utc};
use quant_pivot_error::QuantResult;
use quant_pivot_models::{
    domain::{
        api::{
            BasisAlertListQuery, EquitySnapshotView, ExecutionOrderListQuery,
            FactorDefinitionListQuery, LiveAccountView, ModelVersionListQuery,
            OrderIntentListQuery, QuantReportListQuery, QuantReportView, ReconciliationListQuery,
            ReportRunListQuery, SystemStatusView,
            dashboard::{
                DashboardAccountView, DashboardActionItemView, DashboardActionOwner,
                DashboardActionReasonCode, DashboardActionSeverity, DashboardAuthorityView,
                DashboardExposureView, DashboardLifecycleView, DashboardOverviewQuery,
                DashboardOverviewView, DashboardPrimaryAction, DashboardReasonCode,
                DashboardReportView, DashboardResearchReadinessView, DashboardSection,
                DashboardSubsystemHealthView,
            },
        },
        pagination::PageRequest,
        quant::EquitySnapshotQuery,
    },
    enums::{
        execution::KillSwitchState,
        quant::{
            ExecutionOrderState, OrderIntentStatus, PublicationStatus, RecommendationReportStatus,
            ReportRunStatus,
        },
        rbac::{Operation, ResourceType},
    },
};
use uuid::Uuid;

use crate::{
    auth::casbin::Rule,
    error::WebError,
    extractors::AuthedActor,
    response::WebResponse,
    routes::registry::{RouteSpec, spec},
    state::AppState,
};

const SECTION_TIMEOUT: Duration = Duration::from_secs(3);
const EQUITY_STALE_AFTER_SECS: i64 = 86_400;
const RESEARCH_HISTORY_GATE_DAYS: u32 = 200;

#[derive(Clone, Copy)]
struct DashboardPermissions(u16);

#[derive(Clone, Copy)]
#[repr(u16)]
enum DashboardPermission {
    ReadSystem = 1 << 0,
    ReadAccount = 1 << 1,
    ReadEquity = 1 << 2,
    ReadReports = 1 << 3,
    ReadIntents = 1 << 4,
    ReadExecutionOrders = 1 << 5,
    ReadResearch = 1 << 6,
    ReadReconciliation = 1 << 7,
    EnqueueReport = 1 << 8,
    ReadConfig = 1 << 9,
}

impl DashboardPermissions {
    fn from_decisions<const N: usize>(decisions: [(DashboardPermission, bool); N]) -> Self {
        Self(
            decisions
                .into_iter()
                .fold(0, |mask, (permission, allowed)| {
                    if allowed {
                        mask | permission as u16
                    } else {
                        mask
                    }
                }),
        )
    }

    const fn allows(self, permission: DashboardPermission) -> bool {
        self.0 & permission as u16 != 0
    }
}

#[derive(Clone, Copy)]
struct PrimaryActionContext {
    has_unresolvable_reconciliation: bool,
    report_capability: bool,
    permissions: DashboardPermissions,
}

pub(crate) fn route_specs() -> Vec<RouteSpec> {
    vec![spec(
        Method::GET,
        "/dashboard/overview",
        Rule::AuthenticatedOnly,
        overview,
    )]
}

async fn overview(
    state: Data<AppState>,
    actor: AuthedActor,
    query: Query<DashboardOverviewQuery>,
) -> Result<HttpResponse, WebError> {
    let generated_at = Utc::now();
    let query = query.into_inner();
    let subject = actor.claims.sub.as_str();
    let (
        can_read_system,
        can_read_account,
        can_read_equity,
        can_read_reports,
        can_read_intents,
        can_read_execution_orders,
        can_read_research,
        can_read_reconciliation,
        can_enqueue_report,
        can_read_config,
    ) = tokio::try_join!(
        permission(&state, subject, ResourceType::System, Operation::Read),
        permission(
            &state,
            subject,
            ResourceType::AccountSnapshot,
            Operation::Read
        ),
        permission(
            &state,
            subject,
            ResourceType::EquitySnapshot,
            Operation::Read
        ),
        permission(&state, subject, ResourceType::QuantReport, Operation::Read),
        permission(&state, subject, ResourceType::OrderIntent, Operation::Read),
        permission(
            &state,
            subject,
            ResourceType::ExecutionOrder,
            Operation::Read
        ),
        permission(
            &state,
            subject,
            ResourceType::Materialization,
            Operation::Read
        ),
        permission(
            &state,
            subject,
            ResourceType::Reconciliation,
            Operation::Read
        ),
        permission(
            &state,
            subject,
            ResourceType::QuantReport,
            Operation::Enqueue
        ),
        permission(
            &state,
            subject,
            ResourceType::DecisionPolicySnapshot,
            Operation::Read
        ),
    )?;
    let permissions = DashboardPermissions::from_decisions([
        (DashboardPermission::ReadSystem, can_read_system),
        (DashboardPermission::ReadAccount, can_read_account),
        (DashboardPermission::ReadEquity, can_read_equity),
        (DashboardPermission::ReadReports, can_read_reports),
        (DashboardPermission::ReadIntents, can_read_intents),
        (
            DashboardPermission::ReadExecutionOrders,
            can_read_execution_orders,
        ),
        (DashboardPermission::ReadResearch, can_read_research),
        (
            DashboardPermission::ReadReconciliation,
            can_read_reconciliation,
        ),
        (DashboardPermission::EnqueueReport, can_enqueue_report),
        (DashboardPermission::ReadConfig, can_read_config),
    ]);

    let window_start = generated_at - ChronoDuration::seconds(query.window.seconds());
    let (
        authority,
        account,
        equity_curve,
        latest_report,
        report_lifecycle,
        research_readiness,
        subsystem_health,
    ) = tokio::join!(
        load_authority(&state, permissions),
        load_account(&state, permissions.allows(DashboardPermission::ReadAccount)),
        load_equity(
            &state,
            permissions.allows(DashboardPermission::ReadEquity),
            window_start,
            generated_at
        ),
        load_latest_report(&state, permissions.allows(DashboardPermission::ReadReports)),
        load_report_lifecycle(&state, permissions, window_start, generated_at),
        load_research_readiness(
            &state,
            permissions.allows(DashboardPermission::ReadResearch)
        ),
        load_subsystem_health(&state, permissions.allows(DashboardPermission::ReadSystem)),
    );
    let exposures = exposure_section(&account);
    let data_quality = if permissions.allows(DashboardPermission::ReadSystem) {
        let snapshot = state.data_quality.snapshot();
        DashboardSection::Ready {
            observed_at: snapshot.as_of,
            value: snapshot,
        }
    } else {
        DashboardSection::Forbidden
    };
    let action_inbox = load_action_inbox(&state, permissions, generated_at).await;

    let response = DashboardOverviewView {
        revision: Uuid::now_v7().to_string(),
        generated_at,
        window: query.window,
        authority,
        account,
        equity_curve,
        latest_report,
        report_lifecycle,
        exposures,
        data_quality,
        research_readiness,
        subsystem_health,
        action_inbox,
    };
    Ok(HttpResponse::Ok()
        .insert_header((CACHE_CONTROL, "private, no-store"))
        .json(WebResponse::ok(response)))
}

async fn load_research_readiness(
    state: &AppState,
    allowed: bool,
) -> DashboardSection<DashboardResearchReadinessView> {
    guarded(allowed, DashboardReasonCode::EvidenceMissing, None, async {
        let (snapshot, factors, models) = tokio::try_join!(
            state.research_readiness.snapshot(),
            state
                .research_catalog
                .list_factors(FactorDefinitionListQuery {
                    page: PageRequest::new(1, 1),
                    ..FactorDefinitionListQuery::default()
                }),
            state.research_catalog.list_models(ModelVersionListQuery {
                publication_status: Some(PublicationStatus::Published),
                page: PageRequest::new(1, 1),
                ..ModelVersionListQuery::default()
            }),
        )?;
        let Some(snapshot) = snapshot else {
            return Ok(None);
        };
        let required_history_days = snapshot
            .required_history_days
            .max(RESEARCH_HISTORY_GATE_DAYS);
        let history_ready = snapshot.retention_ready
            && snapshot
                .observed_history_days
                .is_some_and(|days| days >= required_history_days);
        let factor_gate_ready = history_ready && !factors.items.is_empty();
        let model_gate_ready =
            factor_gate_ready && snapshot.latency_ready && !models.items.is_empty();
        Ok(Some((
            snapshot.observed_at,
            DashboardResearchReadinessView {
                required_history_days,
                observed_history_days: snapshot.observed_history_days,
                factor_gate_ready,
                model_gate_ready,
            },
        )))
    })
    .await
}

async fn permission(
    state: &AppState,
    subject: &str,
    resource: ResourceType,
    operation: Operation,
) -> Result<bool, WebError> {
    state
        .casbin
        .enforce(subject, resource.as_str(), operation.as_str())
        .await
}

async fn guarded<T, F>(
    allowed: bool,
    missing_reason: DashboardReasonCode,
    stale_after_secs: Option<i64>,
    future: F,
) -> DashboardSection<T>
where
    F: Future<Output = QuantResult<Option<(DateTime<Utc>, T)>>>,
{
    if !allowed {
        return DashboardSection::Forbidden;
    }
    match tokio::time::timeout(SECTION_TIMEOUT, future).await {
        Err(_) => DashboardSection::Unavailable {
            reason_code: DashboardReasonCode::TimedOut,
        },
        Ok(Err(_)) => DashboardSection::Unavailable {
            reason_code: DashboardReasonCode::DependencyUnavailable,
        },
        Ok(Ok(None)) => DashboardSection::Unavailable {
            reason_code: missing_reason,
        },
        Ok(Ok(Some((observed_at, value))))
            if stale_after_secs.is_some_and(|seconds| {
                Utc::now() - observed_at > ChronoDuration::seconds(seconds)
            }) =>
        {
            DashboardSection::Stale {
                observed_at,
                value,
                reason_code: DashboardReasonCode::SnapshotTooOld,
            }
        }
        Ok(Ok(Some((observed_at, value)))) => DashboardSection::Ready { observed_at, value },
    }
}

async fn load_authority(
    state: &AppState,
    permissions: DashboardPermissions,
) -> DashboardSection<DashboardAuthorityView> {
    guarded(
        permissions.allows(DashboardPermission::ReadSystem),
        DashboardReasonCode::DependencyUnavailable,
        None,
        async {
            let runtime = state.control.system_status();
            let capabilities = state.capabilities.capabilities(&runtime).await?;
            let (primary_action, primary_action_enabled) = (PrimaryActionContext {
                has_unresolvable_reconciliation: runtime
                    .execution_recovery
                    .has_unresolvable_reconciliation,
                report_capability: capabilities.report_generation_eligible.enabled,
                permissions,
            })
            .primary_action();
            let observed_at = runtime.checked_at;
            Ok(Some((
                observed_at,
                DashboardAuthorityView {
                    system: SystemStatusView {
                        runtime,
                        capabilities,
                    },
                    primary_action,
                    primary_action_enabled,
                },
            )))
        },
    )
    .await
}

impl PrimaryActionContext {
    const fn primary_action(self) -> (DashboardPrimaryAction, bool) {
        if self.has_unresolvable_reconciliation {
            return (
                DashboardPrimaryAction::ResolveReconciliation,
                self.permissions
                    .allows(DashboardPermission::ReadReconciliation),
            );
        }
        if self.report_capability {
            return (
                DashboardPrimaryAction::RunReport,
                self.permissions.allows(DashboardPermission::EnqueueReport)
                    && self.report_capability,
            );
        }
        (DashboardPrimaryAction::ViewBlockers, true)
    }
}

async fn load_account(state: &AppState, allowed: bool) -> DashboardSection<DashboardAccountView> {
    guarded(
        allowed,
        DashboardReasonCode::NoAccountSnapshot,
        None,
        async {
            let live = state.account_read.live_account().await?;
            let latest = state.account_read.latest_equity_snapshot().await?;
            let observed_at = live.fetched_at;
            Ok(Some((
                observed_at,
                DashboardAccountView {
                    live: LiveAccountView::from_live(
                        live.fetched_at,
                        live.budget_cap_usd,
                        live.snapshot,
                    ),
                    latest_equity: latest.map(EquitySnapshotView::from),
                },
            )))
        },
    )
    .await
}

async fn load_equity(
    state: &AppState,
    allowed: bool,
    from: DateTime<Utc>,
    to: DateTime<Utc>,
) -> DashboardSection<Vec<EquitySnapshotView>> {
    guarded(
        allowed,
        DashboardReasonCode::NoSamples,
        Some(EQUITY_STALE_AFTER_SECS),
        async {
            let mut items = state
                .account_read
                .equity_snapshots(EquitySnapshotQuery {
                    from: Some(from),
                    to: Some(to),
                    page: PageRequest::new(1, PageRequest::MAX_SIZE),
                })
                .await?
                .items
                .into_iter()
                .map(EquitySnapshotView::from)
                .collect::<Vec<_>>();
            items.sort_by_key(|item| item.as_of);
            let observed_at = items.last().map(|last| last.as_of);
            Ok(observed_at.map(|observed_at| (observed_at, items)))
        },
    )
    .await
}

async fn load_latest_report(
    state: &AppState,
    allowed: bool,
) -> DashboardSection<DashboardReportView> {
    guarded(allowed, DashboardReasonCode::NoReport, None, async {
        let page = state
            .quant_reports
            .list_reports(QuantReportListQuery {
                status: Some(RecommendationReportStatus::Published),
                page: PageRequest::new(1, 1),
                ..QuantReportListQuery::default()
            })
            .await?;
        let Some(report) = page.items.into_iter().next() else {
            return Ok(None);
        };
        let observed_at = report.decision_at;
        let recommendations = state
            .quant_reports
            .find_recommendations(&report.recommendation_report_id)
            .await?;
        Ok(Some((
            observed_at,
            DashboardReportView {
                report: QuantReportView::from(report),
                recommendations,
            },
        )))
    })
    .await
}

async fn load_report_lifecycle(
    state: &AppState,
    permissions: DashboardPermissions,
    from: DateTime<Utc>,
    to: DateTime<Utc>,
) -> DashboardSection<DashboardLifecycleView> {
    let allowed = permissions.allows(DashboardPermission::ReadReports)
        || permissions.allows(DashboardPermission::ReadIntents)
        || permissions.allows(DashboardPermission::ReadExecutionOrders)
        || permissions.allows(DashboardPermission::ReadReconciliation);
    guarded(allowed, DashboardReasonCode::NoSamples, None, async {
        let mut counts = BTreeMap::new();
        if permissions.allows(DashboardPermission::ReadReports) {
            let mut page_number = 1;
            loop {
                let page = state
                    .quant_reports
                    .list_reports(QuantReportListQuery {
                        from: Some(from),
                        to: Some(to),
                        page: PageRequest::new(page_number, PageRequest::MAX_SIZE),
                        ..QuantReportListQuery::default()
                    })
                    .await?;
                for report in page.items {
                    *counts.entry(report.status.to_string()).or_insert(0) += 1;
                }
                if !page.has_next {
                    break;
                }
                page_number += 1;
            }
            for status in [
                ReportRunStatus::Queued,
                ReportRunStatus::Running,
                ReportRunStatus::Failed,
                ReportRunStatus::Abandoned,
            ] {
                let page = state
                    .quant_reports
                    .list_report_runs(ReportRunListQuery {
                        status: Some(status),
                        from: Some(from),
                        to: Some(to),
                        page: PageRequest::new(1, 1),
                        ..ReportRunListQuery::default()
                    })
                    .await?;
                counts.insert(format!("report_run_{status}"), page.total);
            }
        }
        if permissions.allows(DashboardPermission::ReadIntents) {
            let page = state
                .order_intents
                .list(OrderIntentListQuery {
                    status: Some(OrderIntentStatus::PendingApproval),
                    from: Some(from),
                    to: Some(to),
                    page: PageRequest::new(1, 1),
                    ..OrderIntentListQuery::default()
                })
                .await?;
            counts.insert("intent_pending_approval".to_owned(), page.total);
        }
        if permissions.allows(DashboardPermission::ReadExecutionOrders) {
            for state_filter in [
                ExecutionOrderState::Submitted,
                ExecutionOrderState::PartiallyFilled,
                ExecutionOrderState::Ambiguous,
            ] {
                let page = state
                    .execution_read
                    .list_execution_orders(ExecutionOrderListQuery {
                        state: Some(state_filter),
                        from: Some(from),
                        to: Some(to),
                        page: PageRequest::new(1, 1),
                        ..ExecutionOrderListQuery::default()
                    })
                    .await?;
                counts.insert(format!("execution_{state_filter}"), page.total);
            }
        }
        if permissions.allows(DashboardPermission::ReadReconciliation) {
            let page = state
                .execution_read
                .list_reconciliations(ReconciliationListQuery {
                    resolved: Some(false),
                    from: Some(from),
                    to: Some(to),
                    page: PageRequest::new(1, 1),
                    ..ReconciliationListQuery::default()
                })
                .await?;
            counts.insert("reconciliation_unresolved".to_owned(), page.total);
        }
        let total = counts.values().sum();
        if total == 0 {
            return Ok(None);
        }
        Ok(Some((to, DashboardLifecycleView { counts, total })))
    })
    .await
}

async fn load_subsystem_health(
    state: &AppState,
    allowed: bool,
) -> DashboardSection<DashboardSubsystemHealthView> {
    guarded(
        allowed,
        DashboardReasonCode::DependencyUnavailable,
        None,
        async {
            let report = state.readiness.check().await;
            Ok(Some((
                Utc::now(),
                DashboardSubsystemHealthView {
                    ready: report.ready,
                    checks: report.checks,
                },
            )))
        },
    )
    .await
}

fn exposure_section(
    account: &DashboardSection<DashboardAccountView>,
) -> DashboardSection<DashboardExposureView> {
    match account {
        DashboardSection::Ready { observed_at, value } => DashboardSection::Ready {
            observed_at: *observed_at,
            value: DashboardExposureView {
                exposures: value.live.exposures.clone(),
                position_count: value.live.positions.len() as u64,
            },
        },
        DashboardSection::Stale {
            observed_at,
            value,
            reason_code,
        } => DashboardSection::Stale {
            observed_at: *observed_at,
            value: DashboardExposureView {
                exposures: value.live.exposures.clone(),
                position_count: value.live.positions.len() as u64,
            },
            reason_code: *reason_code,
        },
        DashboardSection::Unavailable { reason_code } => DashboardSection::Unavailable {
            reason_code: *reason_code,
        },
        DashboardSection::Forbidden => DashboardSection::Forbidden,
    }
}

async fn load_action_inbox(
    state: &AppState,
    permissions: DashboardPermissions,
    observed_at: DateTime<Utc>,
) -> DashboardSection<Vec<DashboardActionItemView>> {
    guarded(
        permissions.allows(DashboardPermission::ReadSystem)
            || permissions.allows(DashboardPermission::ReadReports)
            || permissions.allows(DashboardPermission::ReadResearch)
            || permissions.allows(DashboardPermission::ReadReconciliation)
            || permissions.allows(DashboardPermission::ReadConfig),
        DashboardReasonCode::NoSamples,
        None,
        async {
            let mut items = Vec::new();
            if permissions.allows(DashboardPermission::ReadSystem) {
                let runtime = state.control.system_status();
                let quality = state.data_quality.snapshot();
                if runtime.kill_switch.state != KillSwitchState::Closed {
                    items.push(DashboardActionItemView {
                        id: "kill-switch".to_owned(),
                        severity: DashboardActionSeverity::Warning,
                        reason_code: DashboardActionReasonCode::KillSwitchNotClosed,
                        owner: DashboardActionOwner::Risk,
                        observed_at: runtime.kill_switch.changed_at,
                        target_route: "/dashboard".to_owned(),
                    });
                }
                if quality.stale > 0 || quality.ingest_lag_exceeded {
                    items.push(DashboardActionItemView {
                        id: "data-quality".to_owned(),
                        severity: DashboardActionSeverity::Warning,
                        reason_code: DashboardActionReasonCode::MarketDataDegraded,
                        owner: DashboardActionOwner::Data,
                        observed_at: quality.as_of,
                        target_route: "/markets".to_owned(),
                    });
                }
            }
            if permissions.allows(DashboardPermission::ReadReconciliation) {
                let unresolved = state
                    .execution_read
                    .list_reconciliations(ReconciliationListQuery {
                        resolved: Some(false),
                        page: PageRequest::new(1, 1),
                        ..ReconciliationListQuery::default()
                    })
                    .await?;
                if let Some(reconciliation) = unresolved.items.into_iter().next() {
                    items.push(DashboardActionItemView {
                        id: format!(
                            "unresolved-reconciliation:{}",
                            reconciliation.reconciliation_id
                        ),
                        severity: DashboardActionSeverity::Critical,
                        reason_code: DashboardActionReasonCode::UnresolvedReconciliation,
                        owner: DashboardActionOwner::Operations,
                        observed_at: reconciliation.created_at,
                        target_route: "/quant/reconciliations".to_owned(),
                    });
                }
            }
            if permissions.allows(DashboardPermission::ReadResearch) {
                let alerts = state
                    .basis_alerts
                    .page(BasisAlertListQuery {
                        open_only: true,
                        page: PageRequest::new(1, 1),
                        ..BasisAlertListQuery::default()
                    })
                    .await?;
                if let Some(alert) = alerts.items.into_iter().next() {
                    items.push(DashboardActionItemView {
                        id: format!("basis-alert:{}", alert.alert_id),
                        severity: DashboardActionSeverity::Warning,
                        reason_code: DashboardActionReasonCode::BasisAlertUnacknowledged,
                        owner: DashboardActionOwner::Research,
                        observed_at: alert.as_of,
                        target_route: "/research/basis-alerts".to_owned(),
                    });
                }
            }
            if permissions.allows(DashboardPermission::ReadReports) {
                let failed = state
                    .quant_reports
                    .list_report_runs(ReportRunListQuery {
                        status: Some(ReportRunStatus::Failed),
                        page: PageRequest::new(1, 1),
                        ..ReportRunListQuery::default()
                    })
                    .await?;
                if let Some(run) = failed.items.into_iter().next() {
                    items.push(DashboardActionItemView {
                        id: format!("failed-report-run:{}", run.report_run_id),
                        severity: DashboardActionSeverity::Warning,
                        reason_code: DashboardActionReasonCode::ReportRunFailed,
                        owner: DashboardActionOwner::Research,
                        observed_at: run.finished_at.unwrap_or(run.requested_at),
                        target_route: "/quant/reports".to_owned(),
                    });
                }
            }
            if permissions.allows(DashboardPermission::ReadConfig) {
                let (approvals, activation) = tokio::try_join!(
                    state.runtime_config.list_valid_approvals(None, 10),
                    state.runtime_config.load_current_activation(None),
                )?;
                let active_approval = activation.map(|activation| activation.policy_approval_id);
                if let Some(approval) = approvals
                    .into_iter()
                    .find(|approval| active_approval.as_ref() != Some(&approval.policy_approval_id))
                {
                    items.push(DashboardActionItemView {
                        id: format!("policy-approval:{}", approval.policy_approval_id),
                        severity: DashboardActionSeverity::Info,
                        reason_code: DashboardActionReasonCode::PolicyRevisionAwaitingActivation,
                        owner: DashboardActionOwner::Governance,
                        observed_at: approval.decided_at,
                        target_route: "/system/config".to_owned(),
                    });
                }
            }
            Ok(Some((observed_at, items)))
        },
    )
    .await
}

#[cfg(test)]
mod tests {
    use chrono::{Duration as ChronoDuration, Utc};
    use quant_pivot_error::QuantError;
    use quant_pivot_models::domain::api::dashboard::{
        DashboardPrimaryAction, DashboardReasonCode, DashboardSection,
    };

    use super::{DashboardPermission, DashboardPermissions, PrimaryActionContext, guarded};

    #[test]
    fn primary_action_unique_ordered() {
        let permissions = DashboardPermissions::from_decisions([
            (DashboardPermission::ReadConfig, true),
            (DashboardPermission::ReadReconciliation, true),
            (DashboardPermission::EnqueueReport, true),
        ]);
        assert_eq!(
            (PrimaryActionContext {
                has_unresolvable_reconciliation: true,
                report_capability: true,
                permissions,
            })
            .primary_action(),
            (DashboardPrimaryAction::ResolveReconciliation, true)
        );
        assert_eq!(
            (PrimaryActionContext {
                has_unresolvable_reconciliation: false,
                report_capability: true,
                permissions,
            })
            .primary_action(),
            (DashboardPrimaryAction::RunReport, true)
        );
        assert_eq!(
            (PrimaryActionContext {
                has_unresolvable_reconciliation: false,
                report_capability: false,
                permissions,
            })
            .primary_action(),
            (DashboardPrimaryAction::ViewBlockers, true)
        );
    }

    #[actix_web::test]
    async fn guarded_sections_stale_snapshots() {
        let forbidden = guarded(false, DashboardReasonCode::NoSamples, None, async {
            Ok::<_, QuantError>(Some((Utc::now(), 1_u8)))
        })
        .await;
        assert!(matches!(forbidden, DashboardSection::Forbidden));

        let unavailable = guarded(true, DashboardReasonCode::NoSamples, None, async {
            Ok::<_, QuantError>(None::<(_, u8)>)
        })
        .await;
        assert!(matches!(
            unavailable,
            DashboardSection::Unavailable {
                reason_code: DashboardReasonCode::NoSamples
            }
        ));

        let stale = guarded(true, DashboardReasonCode::NoSamples, Some(60), async {
            Ok::<_, QuantError>(Some((Utc::now() - ChronoDuration::seconds(61), 7_u8)))
        })
        .await;
        assert!(matches!(
            stale,
            DashboardSection::Stale {
                value: 7,
                reason_code: DashboardReasonCode::SnapshotTooOld,
                ..
            }
        ));
    }
}

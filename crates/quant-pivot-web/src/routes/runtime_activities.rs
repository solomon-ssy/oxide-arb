//! Permission-scoped Activity Center over existing durable domain facts.

use actix_web::{
    http::Method,
    web::{Data, Query},
};
use base64::{Engine as _, engine::general_purpose::URL_SAFE_NO_PAD};
use quant_pivot_models::{
    domain::api::{
        RuntimeActivityActionView, RuntimeActivityCursor, RuntimeActivityListQuery,
        RuntimeActivityPageView, RuntimeActivityReadQuery, RuntimeActivityView,
    },
    enums::{
        rbac::{Operation, ResourceType},
        runtime_activity::{
            RuntimeActivityActionKind, RuntimeActivityDomain, RuntimeActivityStatus,
        },
    },
};

use crate::{
    auth::casbin::Rule,
    error::WebError,
    extractors::AuthedActor,
    response::WebResponse,
    routes::registry::{RouteSpec, spec},
    state::AppState,
};

const CURSOR_MAX_BYTES: usize = 512;

/// The route authenticates first, then applies per-domain RBAC inside the
/// handler so one actor can see exactly the union of domains they may read.
pub(crate) fn route_specs() -> Vec<RouteSpec> {
    vec![spec(
        Method::GET,
        "/runtime/activities",
        Rule::AuthenticatedOnly,
        list,
    )]
}

/// `GET /api/runtime/activities` — stable keyset page of visible runtime facts.
pub async fn list(
    state: Data<AppState>,
    actor: AuthedActor,
    query: Query<RuntimeActivityListQuery>,
) -> Result<WebResponse<RuntimeActivityPageView>, WebError> {
    let query = query.into_inner();
    let limit = query.normalized_limit().ok_or_else(|| {
        WebError::BadRequest(format!(
            "limit must be between 1 and {}",
            RuntimeActivityListQuery::MAX_LIMIT
        ))
    })?;
    let subject = actor.claims.sub.as_str();
    let permissions = ActivityPermissions::load(&state, subject).await?;
    let visible_domains = permissions.visible_domains();
    if visible_domains.is_empty()
        || query
            .domain
            .is_some_and(|domain| !visible_domains.contains(&domain))
    {
        return Err(WebError::Forbidden);
    }
    let cursor = query.cursor.as_deref().map(decode_cursor).transpose()?;
    if cursor
        .as_ref()
        .is_some_and(|cursor| !visible_domains.contains(&cursor.domain))
    {
        return Err(WebError::Forbidden);
    }
    let mut page = state
        .runtime_activities
        .page(RuntimeActivityReadQuery {
            visible_domains,
            domain: query.domain,
            status: query.status,
            cursor,
            limit,
        })
        .await?;
    for item in &mut page.items {
        item.available_actions = permissions.actions(item);
    }
    page.next_cursor = if page.has_more {
        page.items.last().map(encode_cursor).transpose()?
    } else {
        None
    };
    Ok(WebResponse::ok(page))
}

#[derive(Debug, Clone, Copy)]
struct ActivityPermissions(u16);

#[derive(Debug, Clone, Copy)]
#[repr(u16)]
enum ActivityPermission {
    ReadResearch = 1 << 0,
    ReadReport = 1 << 1,
    ReadExecution = 1 << 2,
    ReadReconciliation = 1 << 3,
    ReadSettlement = 1 << 4,
    MutateResearch = 1 << 5,
    RetryReport = 1 << 6,
    ResolveReconciliation = 1 << 7,
}

impl ActivityPermissions {
    async fn load(state: &AppState, subject: &str) -> Result<Self, WebError> {
        let (
            read_research,
            read_report,
            read_execution,
            read_reconciliation,
            read_settlement,
            mutate_research,
            retry_report,
            resolve_reconciliation,
        ) = tokio::try_join!(
            permission(
                state,
                subject,
                ResourceType::Materialization,
                Operation::Read
            ),
            permission(state, subject, ResourceType::QuantReport, Operation::Read),
            permission(
                state,
                subject,
                ResourceType::ExecutionOrder,
                Operation::Read
            ),
            permission(
                state,
                subject,
                ResourceType::Reconciliation,
                Operation::Read
            ),
            permission(
                state,
                subject,
                ResourceType::SettlementRedeem,
                Operation::Read
            ),
            permission(
                state,
                subject,
                ResourceType::Materialization,
                Operation::Create
            ),
            permission(
                state,
                subject,
                ResourceType::QuantReport,
                Operation::Enqueue
            ),
            permission(
                state,
                subject,
                ResourceType::Reconciliation,
                Operation::Resolve
            ),
        )?;
        Ok(Self::from_decisions([
            (ActivityPermission::ReadResearch, read_research),
            (ActivityPermission::ReadReport, read_report),
            (ActivityPermission::ReadExecution, read_execution),
            (ActivityPermission::ReadReconciliation, read_reconciliation),
            (ActivityPermission::ReadSettlement, read_settlement),
            (ActivityPermission::MutateResearch, mutate_research),
            (ActivityPermission::RetryReport, retry_report),
            (
                ActivityPermission::ResolveReconciliation,
                resolve_reconciliation,
            ),
        ]))
    }

    fn from_decisions<const N: usize>(decisions: [(ActivityPermission, bool); N]) -> Self {
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

    const fn allows(self, permission: ActivityPermission) -> bool {
        self.0 & permission as u16 != 0
    }

    fn visible_domains(self) -> Vec<RuntimeActivityDomain> {
        [
            (
                RuntimeActivityDomain::Research,
                self.allows(ActivityPermission::ReadResearch),
            ),
            (
                RuntimeActivityDomain::Report,
                self.allows(ActivityPermission::ReadReport),
            ),
            (
                RuntimeActivityDomain::Execution,
                self.allows(ActivityPermission::ReadExecution),
            ),
            (
                RuntimeActivityDomain::Reconciliation,
                self.allows(ActivityPermission::ReadReconciliation),
            ),
            (
                RuntimeActivityDomain::Settlement,
                self.allows(ActivityPermission::ReadSettlement),
            ),
        ]
        .into_iter()
        .filter_map(|(domain, allowed)| allowed.then_some(domain))
        .collect()
    }

    fn actions(self, item: &RuntimeActivityView) -> Vec<RuntimeActivityActionView> {
        if !item.action_eligible {
            return Vec::new();
        }
        let action = match item.domain {
            RuntimeActivityDomain::Research if self.allows(ActivityPermission::MutateResearch) => {
                if matches!(
                    item.status,
                    RuntimeActivityStatus::Pending | RuntimeActivityStatus::Running
                ) {
                    Some((
                        RuntimeActivityActionKind::CancelResearchJob,
                        ResourceType::Materialization,
                        Operation::Create,
                    ))
                } else {
                    Some((
                        RuntimeActivityActionKind::RetryResearchJob,
                        ResourceType::Materialization,
                        Operation::Create,
                    ))
                }
            }
            RuntimeActivityDomain::Report if self.allows(ActivityPermission::RetryReport) => {
                Some((
                    RuntimeActivityActionKind::RetryReportRun,
                    ResourceType::QuantReport,
                    Operation::Enqueue,
                ))
            }
            RuntimeActivityDomain::Reconciliation
                if self.allows(ActivityPermission::ResolveReconciliation) =>
            {
                Some((
                    RuntimeActivityActionKind::ResolveReconciliation,
                    ResourceType::Reconciliation,
                    Operation::Resolve,
                ))
            }
            _ => None,
        };
        action
            .map(|(kind, resource, operation)| RuntimeActivityActionView {
                kind,
                permission_code: format!("{}:{}", resource.as_str(), operation.as_str()),
            })
            .into_iter()
            .collect()
    }
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

fn decode_cursor(raw: &str) -> Result<RuntimeActivityCursor, WebError> {
    if raw.is_empty() || raw.len() > CURSOR_MAX_BYTES {
        return Err(WebError::BadRequest("invalid activity cursor".to_owned()));
    }
    let bytes = URL_SAFE_NO_PAD
        .decode(raw)
        .map_err(|_| WebError::BadRequest("invalid activity cursor".to_owned()))?;
    let cursor: RuntimeActivityCursor = serde_json::from_slice(&bytes)
        .map_err(|_| WebError::BadRequest("invalid activity cursor".to_owned()))?;
    if cursor.activity_id.is_empty() || cursor.activity_id.len() > 128 {
        return Err(WebError::BadRequest("invalid activity cursor".to_owned()));
    }
    Ok(cursor)
}

fn encode_cursor(item: &RuntimeActivityView) -> Result<String, WebError> {
    let bytes = serde_json::to_vec(&RuntimeActivityCursor {
        updated_at: item.updated_at,
        domain: item.domain,
        activity_id: item.activity_id.clone(),
    })
    .map_err(|error| WebError::Internal(format!("activity cursor encoding failed: {error}")))?;
    Ok(URL_SAFE_NO_PAD.encode(bytes))
}

#[cfg(test)]
mod tests {
    use chrono::Utc;

    use super::{decode_cursor, encode_cursor};
    use quant_pivot_models::{
        domain::api::{RuntimeActivityEntityView, RuntimeActivityView},
        enums::runtime_activity::{RuntimeActivityDomain, RuntimeActivityStatus},
    };

    #[test]
    fn cursor_round_trip() {
        let item = RuntimeActivityView {
            activity_id: "01900000-0000-7000-8000-000000000001".to_owned(),
            domain: RuntimeActivityDomain::Research,
            kind: "model_train".to_owned(),
            status: RuntimeActivityStatus::Running,
            source_status: "running".to_owned(),
            entity: RuntimeActivityEntityView {
                kind: "research-job".to_owned(),
                id: "01900000-0000-7000-8000-000000000001".to_owned(),
            },
            related_entity: None,
            progress_pct: Some(0.5),
            detail: None,
            started_at: None,
            finished_at: None,
            updated_at: Utc::now(),
            target_route: "/runtime/activity".to_owned(),
            available_actions: Vec::new(),
            action_eligible: true,
        };
        let encoded = encode_cursor(&item).expect("encode cursor");
        let decoded = decode_cursor(&encoded).expect("decode cursor");
        assert_eq!(decoded.activity_id, item.activity_id);
        assert_eq!(decoded.domain, item.domain);
        assert_eq!(decoded.updated_at, item.updated_at);
    }

    #[test]
    fn cursor_rejects_malformed() {
        assert!(decode_cursor("not-base64!").is_err());
        assert!(decode_cursor("").is_err());
    }
}

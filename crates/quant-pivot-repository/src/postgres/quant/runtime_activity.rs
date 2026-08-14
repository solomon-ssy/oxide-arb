//! Single-statement, keyset-paginated projection over existing runtime facts.

use std::str::FromStr;

use async_trait::async_trait;
use chrono::{DateTime, Utc};
use quant_pivot_error::storage::StorageError;
use quant_pivot_models::{
    domain::api::{
        RuntimeActivityDomainCountView, RuntimeActivityEntityView, RuntimeActivityIndicatorView,
        RuntimeActivityPageView, RuntimeActivityReadQuery, RuntimeActivitySummaryView,
        RuntimeActivityView,
    },
    enums::runtime_activity::{RuntimeActivityDomain, RuntimeActivityStatus},
};
use sea_orm::{DatabaseBackend, DatabaseConnection, FromQueryResult, Statement, Value};

use crate::traits::RuntimeActivityRepository;

const ACTIVITY_SQL: &str = r"
WITH activity AS (
    SELECT
        job_id::text AS activity_id,
        'research'::text AS domain,
        kind::text AS kind,
        CASE status::text
            WHEN 'queued' THEN 'pending'
            WHEN 'awaiting_evidence' THEN 'pending'
            WHEN 'retry_scheduled' THEN 'pending'
            WHEN 'running' THEN 'running'
            WHEN 'succeeded' THEN 'succeeded'
            WHEN 'failed' THEN 'failed'
            WHEN 'cancelled' THEN 'cancelled'
        END AS status,
        status::text AS source_status,
        'research-job'::text AS entity_kind,
        CASE WHEN model_spec_id IS NULL THEN NULL ELSE 'model-spec'::text END AS related_kind,
        model_spec_id::text AS related_id,
        CASE
            WHEN (progress_json->>'total')::double precision > 0 THEN
                LEAST(
                    (progress_json->>'processed')::double precision
                        / (progress_json->>'total')::double precision * 100.0,
                    100.0
                )
            ELSE NULL
        END AS progress_pct,
        error_json->>'message' AS detail,
        (
            (status::text IN ('queued', 'awaiting_evidence', 'retry_scheduled', 'running')
                AND NOT (kind::text = 'feature_parity'
                    AND status::text IN ('queued', 'awaiting_evidence', 'retry_scheduled')))
            OR status::text IN ('failed', 'cancelled')
        ) AS action_eligible,
        started_at,
        finished_at,
        updated_at
    FROM quant_research_job

    UNION ALL

    SELECT
        report_run_id::text,
        'report'::text,
        trigger_kind::text,
        CASE status::text
            WHEN 'queued' THEN 'pending'
            WHEN 'running' THEN 'running'
            WHEN 'succeeded' THEN 'succeeded'
            WHEN 'failed' THEN 'failed'
            WHEN 'abandoned' THEN 'failed'
            WHEN 'skipped' THEN 'skipped'
        END,
        status::text,
        'report-run'::text,
        CASE WHEN output_report_id IS NULL THEN NULL ELSE 'report'::text END,
        output_report_id::text,
        NULL::double precision,
        error_summary,
        (trigger_kind::text = 'ad_hoc'
            AND status::text IN ('failed', 'skipped', 'abandoned')),
        started_at,
        finished_at,
        GREATEST(
            requested_at,
            COALESCE(started_at, requested_at),
            COALESCE(decision_at, requested_at),
            COALESCE(heartbeat_at, requested_at),
            COALESCE(finished_at, requested_at)
        )
    FROM quant_report_run

    UNION ALL

    SELECT
        execution_order_id::text,
        'execution'::text,
        order_phase::text,
        CASE state::text
            WHEN 'planned' THEN 'pending'
            WHEN 'accepted' THEN 'pending'
            WHEN 'submitted' THEN 'running'
            WHEN 'partially_filled' THEN 'running'
            WHEN 'cancel_requested' THEN 'running'
            WHEN 'filled' THEN 'succeeded'
            WHEN 'cancelled' THEN 'cancelled'
            WHEN 'failed' THEN 'failed'
            WHEN 'ambiguous' THEN 'attention'
        END,
        state::text,
        'execution-order'::text,
        'order-intent'::text,
        order_intent_id::text,
        NULL::double precision,
        error_message,
        FALSE,
        submitted_at,
        COALESCE(filled_at, cancelled_at),
        updated_at
    FROM quant_execution_order

    UNION ALL

    SELECT
        reconciliation_id::text,
        'reconciliation'::text,
        'reconciliation'::text,
        CASE result::text
            WHEN 'pending' THEN 'pending'
            WHEN 'filled' THEN 'succeeded'
            WHEN 'not_filled' THEN 'succeeded'
            WHEN 'partially_filled' THEN 'succeeded'
            WHEN 'cancelled' THEN 'cancelled'
            WHEN 'unresolvable' THEN 'attention'
        END,
        result::text,
        'reconciliation'::text,
        'execution-order'::text,
        execution_order_id::text,
        NULL::double precision,
        NULL::text,
        (result::text = 'unresolvable' AND resolved_at IS NULL),
        created_at,
        resolved_at,
        updated_at
    FROM quant_reconciliation

    UNION ALL

    SELECT
        settlement_redeem_id::text,
        'settlement'::text,
        route::text,
        CASE state::text
            WHEN 'discovered' THEN 'pending'
            WHEN 'prepared' THEN 'pending'
            WHEN 'retry_scheduled' THEN 'pending'
            WHEN 'submitted' THEN 'running'
            WHEN 'confirmed' THEN 'succeeded'
            WHEN 'not_required' THEN 'succeeded'
            WHEN 'reconciliation_required' THEN 'attention'
            WHEN 'manual_required' THEN 'attention'
        END,
        state::text,
        'settlement-redeem'::text,
        'market'::text,
        market_id::text,
        NULL::double precision,
        last_error,
        FALSE,
        submitted_at,
        COALESCE(confirmed_at, failed_at),
        updated_at
    FROM quant_settlement_redeem
), permitted AS (
    SELECT *
    FROM activity
    WHERE (($1::boolean AND domain = 'research')
        OR ($2::boolean AND domain = 'report')
        OR ($3::boolean AND domain = 'execution')
        OR ($4::boolean AND domain = 'reconciliation')
        OR ($5::boolean AND domain = 'settlement'))
), indicator AS (
    SELECT
        COUNT(*)::bigint AS indicator_total,
        COUNT(*) FILTER (WHERE status = 'running')::bigint AS indicator_running,
        COUNT(*) FILTER (WHERE status = 'attention')::bigint AS indicator_attention
    FROM permitted
), visible AS (
    SELECT *
    FROM permitted
    WHERE ($6::text IS NULL OR domain = $6)
      AND ($7::text IS NULL OR status = $7)
), counts AS (
    SELECT
        COUNT(*)::bigint AS total_count,
        COUNT(*) FILTER (WHERE domain = 'research')::bigint AS research_count,
        COUNT(*) FILTER (WHERE domain = 'report')::bigint AS report_count,
        COUNT(*) FILTER (WHERE domain = 'execution')::bigint AS execution_count,
        COUNT(*) FILTER (WHERE domain = 'reconciliation')::bigint AS reconciliation_count,
        COUNT(*) FILTER (WHERE domain = 'settlement')::bigint AS settlement_count
    FROM visible
), items AS (
    SELECT *
    FROM visible
    WHERE $8::timestamptz IS NULL
       OR (updated_at, domain, activity_id) < ($8::timestamptz, $9::text, $10::text)
    ORDER BY updated_at DESC, domain DESC, activity_id DESC
    LIMIT $11
)
SELECT
    indicator.indicator_total,
    indicator.indicator_running,
    indicator.indicator_attention,
    counts.total_count,
    counts.research_count,
    counts.report_count,
    counts.execution_count,
    counts.reconciliation_count,
    counts.settlement_count,
    items.activity_id,
    items.domain,
    items.kind,
    items.status,
    items.source_status,
    items.entity_kind,
    items.related_kind,
    items.related_id,
    items.progress_pct,
    items.detail,
    items.action_eligible,
    items.started_at,
    items.finished_at,
    items.updated_at
FROM indicator
CROSS JOIN counts
LEFT JOIN items ON TRUE
ORDER BY items.updated_at DESC NULLS LAST, items.domain DESC, items.activity_id DESC
";

#[derive(Debug, FromQueryResult)]
struct RuntimeActivityRow {
    indicator_total: i64,
    indicator_running: i64,
    indicator_attention: i64,
    total_count: i64,
    research_count: i64,
    report_count: i64,
    execution_count: i64,
    reconciliation_count: i64,
    settlement_count: i64,
    activity_id: Option<String>,
    domain: Option<String>,
    kind: Option<String>,
    status: Option<String>,
    source_status: Option<String>,
    entity_kind: Option<String>,
    related_kind: Option<String>,
    related_id: Option<String>,
    progress_pct: Option<f64>,
    detail: Option<String>,
    action_eligible: Option<bool>,
    started_at: Option<DateTime<Utc>>,
    finished_at: Option<DateTime<Utc>>,
    updated_at: Option<DateTime<Utc>>,
}

pub struct PgRuntimeActivityRepository {
    db: DatabaseConnection,
}

impl PgRuntimeActivityRepository {
    pub const fn new(db: DatabaseConnection) -> Self {
        Self { db }
    }
}

#[async_trait]
impl RuntimeActivityRepository for PgRuntimeActivityRepository {
    async fn page(
        &self,
        query: RuntimeActivityReadQuery,
    ) -> Result<RuntimeActivityPageView, StorageError> {
        let visible = |domain| query.visible_domains.contains(&domain);
        let cursor_at = query.cursor.as_ref().map(|cursor| cursor.updated_at);
        let cursor_domain = query
            .cursor
            .as_ref()
            .map(|cursor| cursor.domain.as_str().to_owned());
        let cursor_id = query
            .cursor
            .as_ref()
            .map(|cursor| cursor.activity_id.clone());
        let sql_limit = i64::try_from(query.limit.saturating_add(1)).map_err(|error| {
            StorageError::invariant_violation(None, format!("activity limit invalid: {error}"))
        })?;
        let values: Vec<Value> = vec![
            visible(RuntimeActivityDomain::Research).into(),
            visible(RuntimeActivityDomain::Report).into(),
            visible(RuntimeActivityDomain::Execution).into(),
            visible(RuntimeActivityDomain::Reconciliation).into(),
            visible(RuntimeActivityDomain::Settlement).into(),
            query.domain.map(|domain| domain.as_str().to_owned()).into(),
            query.status.map(|status| status.as_str().to_owned()).into(),
            cursor_at.into(),
            cursor_domain.into(),
            cursor_id.into(),
            sql_limit.into(),
        ];
        let rows = RuntimeActivityRow::find_by_statement(Statement::from_sql_and_values(
            DatabaseBackend::Postgres,
            ACTIVITY_SQL,
            values,
        ))
        .all(&self.db)
        .await
        .map_err(StorageError::from)?;
        let first = rows.first().ok_or_else(|| {
            StorageError::invariant_violation(None, "activity aggregate returned no count row")
        })?;
        let mut by_domain = Vec::new();
        for (domain, raw) in [
            (RuntimeActivityDomain::Research, first.research_count),
            (RuntimeActivityDomain::Report, first.report_count),
            (RuntimeActivityDomain::Execution, first.execution_count),
            (
                RuntimeActivityDomain::Reconciliation,
                first.reconciliation_count,
            ),
            (RuntimeActivityDomain::Settlement, first.settlement_count),
        ] {
            let count = count(raw)?;
            if count > 0 {
                by_domain.push(RuntimeActivityDomainCountView { domain, count });
            }
        }
        let indicator = RuntimeActivityIndicatorView {
            total: count(first.indicator_total)?,
            running: count(first.indicator_running)?,
            attention: count(first.indicator_attention)?,
        };
        let summary = RuntimeActivitySummaryView {
            total: count(first.total_count)?,
            by_domain,
        };
        let mut items = rows
            .into_iter()
            .filter(|row| row.activity_id.is_some())
            .map(RuntimeActivityView::try_from)
            .collect::<Result<Vec<_>, _>>()?;
        let page_limit = usize::try_from(query.limit).map_err(|error| {
            StorageError::invariant_violation(None, format!("activity page limit invalid: {error}"))
        })?;
        let has_more = items.len() > page_limit;
        if has_more {
            items.truncate(page_limit);
        }
        Ok(RuntimeActivityPageView {
            indicator,
            summary,
            items,
            has_more,
            next_cursor: None,
        })
    }
}

impl TryFrom<RuntimeActivityRow> for RuntimeActivityView {
    type Error = StorageError;

    fn try_from(row: RuntimeActivityRow) -> Result<Self, Self::Error> {
        let activity_id = required(row.activity_id, "activity_id")?;
        let domain = RuntimeActivityDomain::from_str(&required(row.domain, "domain")?)
            .map_err(|error| StorageError::invariant_violation(None, error.to_string()))?;
        let status = RuntimeActivityStatus::from_str(&required(row.status, "status")?)
            .map_err(|error| StorageError::invariant_violation(None, error.to_string()))?;
        let entity_kind = required(row.entity_kind, "entity_kind")?;
        let target_route = target_route(domain, &activity_id);
        let related_entity = match (row.related_kind, row.related_id) {
            (Some(kind), Some(id)) => Some(RuntimeActivityEntityView { kind, id }),
            (None, None) => None,
            _ => {
                return Err(StorageError::invariant_violation(
                    None,
                    "activity related entity is incomplete",
                ));
            }
        };
        Ok(Self {
            activity_id: activity_id.clone(),
            domain,
            kind: required(row.kind, "kind")?,
            status,
            source_status: required(row.source_status, "source_status")?,
            entity: RuntimeActivityEntityView {
                kind: entity_kind,
                id: activity_id,
            },
            related_entity,
            progress_pct: row.progress_pct,
            detail: row.detail,
            started_at: row.started_at,
            finished_at: row.finished_at,
            updated_at: required(row.updated_at, "updated_at")?,
            target_route,
            available_actions: Vec::new(),
            action_eligible: required(row.action_eligible, "action_eligible")?,
        })
    }
}

fn required<T>(value: Option<T>, field: &str) -> Result<T, StorageError> {
    value.ok_or_else(|| {
        StorageError::invariant_violation(None, format!("activity row omitted `{field}`"))
    })
}

fn count(raw: i64) -> Result<u64, StorageError> {
    u64::try_from(raw).map_err(|error| {
        StorageError::invariant_violation(None, format!("activity count invalid: {error}"))
    })
}

fn target_route(domain: RuntimeActivityDomain, id: &str) -> String {
    match domain {
        RuntimeActivityDomain::Research => {
            format!("/runtime/activity?domain=research&entity=research-job&id={id}")
        }
        RuntimeActivityDomain::Report => {
            format!("/trading/recommendations?module=reports&entity=report-run&id={id}")
        }
        RuntimeActivityDomain::Execution => {
            format!("/execution/orders?module=orders&entity=execution-order&id={id}")
        }
        RuntimeActivityDomain::Reconciliation => {
            format!("/execution/post-trade?module=reconciliation&entity=reconciliation&id={id}")
        }
        RuntimeActivityDomain::Settlement => {
            format!("/execution/post-trade?module=settlement&entity=settlement-redeem&id={id}")
        }
    }
}

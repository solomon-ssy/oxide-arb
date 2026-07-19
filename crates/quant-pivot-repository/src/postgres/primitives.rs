//! Audited `PostgreSQL`-only expressions that `SeaORM` cannot represent directly.

use chrono::{DateTime, Utc};
use quant_pivot_error::storage::StorageError;
use sea_orm::{
    ConnectionTrait, DatabaseBackend, ExprTrait, FromQueryResult, Statement,
    entity::ActiveEnum,
    sea_query::{
        Alias, Expr, Func, IntoColumnRef, IntoIden, Query, SimpleExpr, extension::postgres::PgFunc,
    },
};

#[derive(Debug, FromQueryResult)]
struct DatabaseClock {
    now: DateTime<Utc>,
}

#[derive(Debug, FromQueryResult)]
pub(super) struct ShadowLatencyAggregate {
    pub decision_prepared_count: i64,
    pub decision_prepared_p95_ms: Option<i64>,
    pub endpoint_rtt_count: i64,
    pub endpoint_rtt_p95_ms: Option<i64>,
    pub market_delay_count: i64,
    pub market_delay_p95_ms: Option<i64>,
}

/// Read `PostgreSQL`'s transaction-aware statement clock.
pub(super) async fn statement_timestamp(
    db: &impl ConnectionTrait,
) -> Result<DateTime<Utc>, StorageError> {
    let query = Query::select()
        .expr_as(
            Func::cust(Alias::new("STATEMENT_TIMESTAMP")),
            Alias::new("now"),
        )
        .to_owned();
    DatabaseClock::find_by_statement(db.get_database_backend().build(&query))
        .one(db)
        .await
        .map_err(StorageError::from)?
        .map(|clock| clock.now)
        .ok_or_else(|| {
            StorageError::invariant_violation(None, "database did not return statement_timestamp")
        })
}

/// Acquire a transaction-scoped `PostgreSQL` advisory lock.
pub(super) async fn advisory_xact_lock(
    db: &impl ConnectionTrait,
    key: i64,
) -> Result<(), StorageError> {
    let query = Query::select()
        .expr(Func::cust(Alias::new("PG_ADVISORY_XACT_LOCK")).arg(Expr::value(key)))
        .to_owned();
    db.query_one(&query).await.map_err(StorageError::from)?;
    Ok(())
}

/// Acquire a transaction-scoped advisory lock derived by `PostgreSQL` from a
/// stable text scope and namespace seed.
pub(super) async fn advisory_text_xact_lock(
    db: &impl ConnectionTrait,
    scope: &str,
    namespace: i64,
) -> Result<(), StorageError> {
    let lock_key = Func::cust(Alias::new("HASHTEXTEXTENDED"))
        .arg(Expr::value(scope))
        .arg(Expr::value(namespace));
    let query = Query::select()
        .expr(Func::cust(Alias::new("PG_ADVISORY_XACT_LOCK")).arg(lock_key))
        .to_owned();
    db.query_one(&query).await.map_err(StorageError::from)?;
    Ok(())
}

/// Queue a transactional `PostgreSQL` notification. Delivery occurs only after
/// the surrounding transaction commits.
pub(super) async fn notify(
    db: &impl ConnectionTrait,
    channel: &str,
    payload: &str,
) -> Result<(), StorageError> {
    let query = Query::select()
        .expr(
            Func::cust(Alias::new("PG_NOTIFY"))
                .arg(Expr::value(channel))
                .arg(Expr::value(payload)),
        )
        .to_owned();
    db.query_one(&query).await.map_err(StorageError::from)?;
    Ok(())
}

/// Execute the `PostgreSQL` ordered-set aggregate used by research readiness.
/// `SeaQuery` has no typed representation for `percentile_cont ... WITHIN
/// GROUP`, so the complete dialect-specific query is isolated here and its
/// result is decoded into a fixed Rust shape.
pub(super) async fn shadow_latency_aggregate(
    db: &impl ConnectionTrait,
    window_start: DateTime<Utc>,
    window_end: DateTime<Utc>,
) -> Result<ShadowLatencyAggregate, StorageError> {
    ShadowLatencyAggregate::find_by_statement(Statement::from_sql_and_values(
        DatabaseBackend::Postgres,
        r"
WITH decision_prepared AS (
    SELECT
        COUNT(*)::bigint AS sample_count,
        percentile_cont(0.95) WITHIN GROUP (
            ORDER BY EXTRACT(EPOCH FROM (created_at - decision_at)) * 1000
        )::bigint AS p95_ms
    FROM quant_recommendation_report
    WHERE runtime_mode = 'report_only'
      AND created_at >= $1 AND created_at < $2
      AND decision_at <= created_at
), endpoint_rtt AS (
    SELECT
        COUNT(*)::bigint AS sample_count,
        percentile_cont(0.95) WITHIN GROUP (
            ORDER BY EXTRACT(EPOCH FROM (fetched_at - started_at)) * 1000
        )::bigint AS p95_ms
    FROM catalog_sync_batch
    WHERE status = 'committed'
      AND fetched_at >= $1 AND fetched_at < $2
      AND started_at <= fetched_at
), market_delay AS (
    SELECT
        COUNT(*)::bigint AS sample_count,
        percentile_cont(0.95) WITHIN GROUP (
            ORDER BY COALESCE(minimum_order_age_secs, 0)::double precision * 1000
        )::bigint AS p95_ms
    FROM clob_market_info_version
    WHERE available_at >= $1 AND available_at < $2
)
SELECT
    decision_prepared.sample_count AS decision_prepared_count,
    decision_prepared.p95_ms AS decision_prepared_p95_ms,
    endpoint_rtt.sample_count AS endpoint_rtt_count,
    endpoint_rtt.p95_ms AS endpoint_rtt_p95_ms,
    market_delay.sample_count AS market_delay_count,
    market_delay.p95_ms AS market_delay_p95_ms
FROM decision_prepared, endpoint_rtt, market_delay
",
        [window_start.into(), window_end.into()],
    ))
    .one(db)
    .await
    .map_err(StorageError::from)?
    .ok_or_else(|| StorageError::invariant_violation(None, "shadow latency query returned no row"))
}

/// Preserve the existing timestamp on idempotent transitions and otherwise
/// use `PostgreSQL`'s transaction-aware current timestamp.
pub(super) fn timestamp_once(column: impl IntoColumnRef) -> SimpleExpr {
    Expr::col(column).if_null(Expr::current_timestamp())
}

/// Test membership in a `PostgreSQL` native-enum array without embedding a
/// column name or enum value in SQL text.
pub(super) fn enum_array_contains<E: ActiveEnum>(
    column: impl IntoColumnRef,
    value: &E,
) -> SimpleExpr {
    enum_value(value).eq(PgFunc::any(Expr::col(column)))
}

/// Test whether a `PostgreSQL` native-enum array is empty while retaining its
/// concrete element type for the empty bound value.
pub(super) fn enum_array_is_empty<E: ActiveEnum>(column: impl IntoColumnRef) -> SimpleExpr {
    Expr::col(column)
        .eq(Expr::value(Vec::<String>::new()).cast_as(Alias::new(format!("{}[]", E::name()))))
}

/// `PostgreSQL` boolean aggregate not currently exposed as a built-in `SeaQuery`
/// function.
pub(super) fn bool_or(column: impl IntoColumnRef) -> SimpleExpr {
    Func::cust(Alias::new("BOOL_OR"))
        .arg(Expr::col(column))
        .into()
}

/// Cast a bound Rust enum value to its `PostgreSQL` native enum type.
pub(super) fn enum_value<E: ActiveEnum>(value: &E) -> SimpleExpr {
    Expr::value(value.to_value()).cast_as(Alias::new(E::name().to_string()))
}

/// Cast `EXCLUDED.column` to its `PostgreSQL` native enum type.
pub(super) fn excluded_enum<E: ActiveEnum>(column: impl IntoIden) -> SimpleExpr {
    excluded_cast(column, E::name().to_string())
}

/// Cast `EXCLUDED.column` to its `PostgreSQL` native enum-array type.
pub(super) fn excluded_enum_array<E: ActiveEnum>(column: impl IntoIden) -> SimpleExpr {
    excluded_cast(column, format!("{}[]", E::name()))
}

fn excluded_cast(column: impl IntoIden, postgres_type: impl AsRef<str>) -> SimpleExpr {
    Expr::col((Alias::new("excluded"), column.into_iden()))
        .cast_as(Alias::new(postgres_type.as_ref()))
}

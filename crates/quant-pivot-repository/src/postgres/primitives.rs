//! Audited `PostgreSQL`-only expressions that `SeaORM` cannot represent directly.

use chrono::{DateTime, Utc};
use quant_pivot_error::storage::StorageError;
use sea_orm::{
    ConnectionTrait, ExprTrait, FromQueryResult,
    entity::ActiveEnum,
    sea_query::{
        Alias, Expr, Func, IntoColumnRef, IntoIden, Query, SimpleExpr, extension::postgres::PgFunc,
    },
};

#[derive(Debug, FromQueryResult)]
struct DatabaseClock {
    now: DateTime<Utc>,
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

/// Bound an application-clock reconciliation cutoff by the database clock
/// before persisting queue readiness. This preserves `ready_at <= created_at`
/// under host/container clock skew without moving a historical cutoff forward.
pub(super) fn queue_ready_at(
    available_through: DateTime<Utc>,
    created_at: DateTime<Utc>,
) -> DateTime<Utc> {
    Ord::min(available_through, created_at)
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

/// Produce a typed SQL `NULL` for a `PostgreSQL` native enum. This is required
/// when an enum column participates in a `UNION` branch that has no value;
/// leaving the null untyped lets `PostgreSQL` resolve an inner union as `text`.
pub(super) fn enum_null<E: ActiveEnum>() -> SimpleExpr {
    Expr::cust("NULL").cast_as(Alias::new(E::name().to_string()))
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

#[cfg(test)]
mod tests {
    use chrono::{Duration, Utc};

    use super::queue_ready_at;

    #[test]
    fn queue_time_bounds_cutoff() {
        let database_now = Utc::now();
        assert_eq!(
            queue_ready_at(database_now + Duration::seconds(30), database_now),
            database_now
        );
        let historical = database_now - Duration::days(1);
        assert_eq!(queue_ready_at(historical, database_now), historical);
    }
}

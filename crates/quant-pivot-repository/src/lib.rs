//! Data access layer for the quant-pivot platform.
//!
//! Provides async repository traits and implementations for:
//!
//! - **`PostgreSQL`** — CRUD for all domain entities via `SeaORM`
//! - **`ClickHouse`** — timeseries insert and query
//! - **`Cached`** — tiered cache wrappers (L1 Moka + L2 Redis)

use quant_pivot_allocator as _;

pub mod batch;
pub mod cached;
pub mod clickhouse;
pub mod postgres;
pub mod traits;

#[cfg(test)]
mod seaorm_stable_semantics {
    use std::collections::BTreeMap;

    use quant_pivot_models::entities::role::Entity;
    use sea_orm::{
        DbBackend, EntityTrait, MockDatabase, PaginatorTrait, QuerySelect, QueryTrait, Transaction,
        Value, error::DbErr,
    };
    use sea_query::{Expr, Query};

    #[tokio::test]
    async fn stable_count_honors_limit_and_offset() -> Result<(), DbErr> {
        let mut count_row = BTreeMap::new();
        count_row.insert("num_items", Value::BigInt(Some(2)));
        let db = MockDatabase::new(DbBackend::Postgres)
            .append_query_results([[count_row]])
            .into_connection();

        let count = Entity::find().limit(4).offset(8).count(&db).await?;

        let sub_query = Entity::find().limit(4).offset(8).into_query();
        let count_query = Query::select()
            .expr(Expr::cust("COUNT(*) AS num_items"))
            .from_subquery(sub_query, "sub_query")
            .to_owned();
        let expected = Transaction::wrap([db.get_database_backend().build(&count_query)]);

        assert_eq!(count, 2);
        assert_eq!(db.into_transaction_log(), expected);
        Ok(())
    }
}

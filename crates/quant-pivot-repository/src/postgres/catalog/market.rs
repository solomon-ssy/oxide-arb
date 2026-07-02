use crate::{
    postgres::{
        catalog::ingest::{find_existing_str_id_chunks, find_models_by_str_id_chunks},
        error,
        query::{non_empty, paginate_mapped},
        write::upsert_many_chunked,
    },
    traits::MarketRepository,
};
use quant_pivot_error::storage::{StorageError, entity};
use quant_pivot_models::{
    domain::{MarketInfo, MarketPageQuery, Paginated, UpsertMarket},
    entities::market::{
        ActiveModel as MarketActiveModel, Column as MarketColumn, Entity as MarketEntity,
    },
    enums::{
        common::{MarketCategory, TickSize},
        fee::FeeSource,
        market::MarketStatus,
    },
    schema::column,
    types::{MarketId, TokenId},
};
use sea_orm::{
    ActiveValue, ColumnTrait, ConnectionTrait, DatabaseConnection, DatabaseTransaction,
    EntityTrait, IntoActiveModel, QueryFilter, QueryOrder,
    sea_query::{Condition, Expr, OnConflict, extension::postgres::PgExpr},
};
use std::{collections::HashSet, sync::Arc};

pub struct PgMarketRepository {
    db: DatabaseConnection,
}

impl PgMarketRepository {
    pub const fn new(db: DatabaseConnection) -> Self {
        Self { db }
    }

    pub const fn with_txn(txn: &DatabaseTransaction) -> PgMarketRepositoryTxn<'_> {
        PgMarketRepositoryTxn { txn }
    }
}

pub struct PgMarketRepositoryTxn<'a> {
    txn: &'a DatabaseTransaction,
}

async fn do_find_by_id(
    db: &impl ConnectionTrait,
    id: &MarketId,
) -> Result<Option<Arc<MarketInfo>>, StorageError> {
    MarketEntity::find_by_id(id.clone())
        .one(db)
        .await
        .map_err(StorageError::from)
        .map(|opt| opt.map(|model| Arc::new(model.into())))
}

async fn do_find_by_ids(
    db: &impl ConnectionTrait,
    ids: &[MarketId],
) -> Result<Vec<Arc<MarketInfo>>, StorageError> {
    find_models_by_str_id_chunks::<MarketEntity, _, _, _>(
        db,
        ids,
        MarketColumn::MarketId,
        MarketId::as_str,
    )
    .await
    .map(|rows| {
        rows.into_iter()
            .map(|model| Arc::new(model.into()))
            .collect()
    })
}

async fn do_find_active(db: &impl ConnectionTrait) -> Result<Arc<[MarketInfo]>, StorageError> {
    MarketEntity::find()
        .filter(MarketColumn::Status.eq(MarketStatus::Active))
        .all(db)
        .await
        .map_err(StorageError::from)
        .map(|rows| rows.into_iter().map(Into::into).collect::<Vec<_>>().into())
}

async fn do_find_by_event(
    db: &impl ConnectionTrait,
    event_id: &str,
) -> Result<Vec<Arc<MarketInfo>>, StorageError> {
    MarketEntity::find()
        .filter(MarketColumn::EventId.eq(event_id))
        .all(db)
        .await
        .map_err(StorageError::from)
        .map(|rows| {
            rows.into_iter()
                .map(|model| Arc::new(model.into()))
                .collect()
        })
}

async fn do_find_existing_ids(
    db: &impl ConnectionTrait,
    ids: &[MarketId],
) -> Result<HashSet<String>, StorageError> {
    find_existing_str_id_chunks::<MarketEntity, _, _, _>(
        db,
        ids,
        MarketColumn::MarketId,
        MarketId::as_str,
    )
    .await
}

fn market_upsert_on_conflict() -> OnConflict {
    OnConflict::column(MarketColumn::MarketId)
        .update_columns([
            MarketColumn::EventId,
            MarketColumn::Question,
            MarketColumn::Slug,
            MarketColumn::YesTokenId,
            MarketColumn::NoTokenId,
            MarketColumn::NegRisk,
            MarketColumn::Outcome,
            MarketColumn::EndDate,
            MarketColumn::ResolvedAt,
            MarketColumn::FeesEnabled,
            MarketColumn::FeeRate,
            MarketColumn::FeeExponent,
            MarketColumn::FeeTakerOnly,
            MarketColumn::FeeRebateRate,
            MarketColumn::FeeObservedAt,
        ])
        .values([
            (
                MarketColumn::Categories,
                column::pg_enum_array_excluded::<MarketCategory>(MarketColumn::Categories),
            ),
            (
                MarketColumn::Status,
                column::pg_enum_excluded::<MarketStatus>(MarketColumn::Status),
            ),
            (
                MarketColumn::TickSize,
                column::pg_enum_excluded::<TickSize>(MarketColumn::TickSize),
            ),
            (
                MarketColumn::FeeSource,
                column::pg_enum_excluded::<FeeSource>(MarketColumn::FeeSource),
            ),
        ])
        .to_owned()
}

async fn do_upsert(
    db: &impl ConnectionTrait,
    dto: UpsertMarket,
) -> Result<Arc<MarketInfo>, StorageError> {
    let model = MarketEntity::insert(dto.into_active_model())
        .on_conflict(market_upsert_on_conflict())
        .exec_with_returning(db)
        .await
        .map_err(StorageError::from)?;
    Ok(Arc::new(model.into()))
}

async fn do_upsert_batch(
    db: &impl ConnectionTrait,
    dtos: Vec<UpsertMarket>,
) -> Result<u64, StorageError> {
    // Mixed NULL / non-NULL native-enum columns (e.g. `fee_source`) in one batch
    // are homogenised inside `upsert_many_chunked` — see `align_partial_columns`.
    upsert_many_chunked::<MarketEntity, UpsertMarket>(db, dtos, market_upsert_on_conflict()).await
}

async fn do_update_status(
    db: &impl ConnectionTrait,
    id: &MarketId,
    status: &str,
    outcome: Option<&str>,
) -> Result<(), StorageError> {
    let status = match status {
        "discovered" => MarketStatus::Discovered,
        "active" => MarketStatus::Active,
        "filtered" => MarketStatus::Filtered,
        "paused" => MarketStatus::Paused,
        "manually_blocked" => MarketStatus::ManuallyBlocked,
        "settled" => MarketStatus::Settled,
        "delisted" => MarketStatus::Delisted,
        other => {
            return Err(error::invariant_violation(
                Some(entity::MARKET),
                format!("invalid market status `{other}`"),
            ));
        }
    };
    let mut model = MarketActiveModel {
        market_id: ActiveValue::Set(id.clone()),
        status: ActiveValue::Set(status),
        outcome: ActiveValue::Set(outcome.map(str::to_owned)),
        ..Default::default()
    };
    model.updated_at = ActiveValue::NotSet;
    MarketEntity::update(model)
        .exec(db)
        .await
        .map_err(StorageError::from)?;
    Ok(())
}

fn page_condition(query: &MarketPageQuery) -> Condition {
    let mut condition = Condition::all()
        .add_option(query.status.map(|status| MarketColumn::Status.eq(status)))
        .add_option(
            query
                .event_id
                .as_ref()
                .map(|event_id| MarketColumn::EventId.eq(event_id.clone())),
        )
        .add_option(query.category.map(|category| {
            Expr::cust_with_values(
                r#""market"."categories" @> ARRAY[$1]::qp_market_category[]"#,
                [category.as_str()],
            )
        }));

    if let (Some(want_subscribed), Some(tokens)) =
        (query.subscribed, query.resolved_subscribed_tokens.as_ref())
    {
        if want_subscribed {
            if tokens.is_empty() {
                condition = condition.add(Expr::cust("1 = 0"));
            } else {
                let token_ids: Vec<TokenId> = tokens.iter().cloned().collect();
                condition = condition
                    .add(MarketColumn::YesTokenId.is_in(token_ids.clone()))
                    .add(MarketColumn::NoTokenId.is_in(token_ids));
            }
        } else if !tokens.is_empty() {
            let token_ids: Vec<TokenId> = tokens.iter().cloned().collect();
            condition = condition.add(
                Condition::any()
                    .add(MarketColumn::YesTokenId.is_not_in(token_ids.clone()))
                    .add(MarketColumn::NoTokenId.is_not_in(token_ids)),
            );
        }
    }

    if let Some(search) = non_empty(query.keyword.as_deref()) {
        let pattern = format!("%{}%", search.replace('%', "\\%").replace('_', "\\_"));
        condition = condition.add(
            Condition::any()
                .add(Expr::col(MarketColumn::Question).ilike(pattern.clone()))
                .add(Expr::col(MarketColumn::Slug).ilike(pattern)),
        );
    }

    condition
}

async fn do_page(
    db: &impl ConnectionTrait,
    query: MarketPageQuery,
) -> Result<Paginated<MarketInfo>, StorageError> {
    let normalized = query.normalized();
    paginate_mapped(
        MarketEntity::find()
            .filter(page_condition(&normalized))
            .order_by_desc(MarketColumn::UpdatedAt)
            .order_by_asc(MarketColumn::MarketId),
        db,
        &normalized.page,
        Into::into,
    )
    .await
}

#[async_trait::async_trait]
impl MarketRepository for PgMarketRepository {
    async fn find_by_id(&self, id: &MarketId) -> Result<Option<Arc<MarketInfo>>, StorageError> {
        do_find_by_id(&self.db, id).await
    }

    async fn find_by_ids(&self, ids: &[MarketId]) -> Result<Vec<Arc<MarketInfo>>, StorageError> {
        do_find_by_ids(&self.db, ids).await
    }

    async fn page(&self, query: MarketPageQuery) -> Result<Paginated<MarketInfo>, StorageError> {
        do_page(&self.db, query).await
    }

    async fn find_active(&self) -> Result<Arc<[MarketInfo]>, StorageError> {
        do_find_active(&self.db).await
    }

    async fn find_by_event(&self, event_id: &str) -> Result<Vec<Arc<MarketInfo>>, StorageError> {
        do_find_by_event(&self.db, event_id).await
    }

    async fn find_existing_ids(&self, ids: &[MarketId]) -> Result<HashSet<String>, StorageError> {
        do_find_existing_ids(&self.db, ids).await
    }

    async fn upsert(&self, market: UpsertMarket) -> Result<Arc<MarketInfo>, StorageError> {
        do_upsert(&self.db, market).await
    }

    async fn upsert_batch(&self, markets: Vec<UpsertMarket>) -> Result<u64, StorageError> {
        do_upsert_batch(&self.db, markets).await
    }

    async fn update_status(
        &self,
        id: &MarketId,
        status: &str,
        outcome: Option<&str>,
    ) -> Result<(), StorageError> {
        do_update_status(&self.db, id, status, outcome).await
    }
}

#[async_trait::async_trait]
impl MarketRepository for PgMarketRepositoryTxn<'_> {
    async fn find_by_id(&self, id: &MarketId) -> Result<Option<Arc<MarketInfo>>, StorageError> {
        do_find_by_id(self.txn, id).await
    }

    async fn find_by_ids(&self, ids: &[MarketId]) -> Result<Vec<Arc<MarketInfo>>, StorageError> {
        do_find_by_ids(self.txn, ids).await
    }

    async fn page(&self, query: MarketPageQuery) -> Result<Paginated<MarketInfo>, StorageError> {
        do_page(self.txn, query).await
    }

    async fn find_active(&self) -> Result<Arc<[MarketInfo]>, StorageError> {
        do_find_active(self.txn).await
    }

    async fn find_by_event(&self, event_id: &str) -> Result<Vec<Arc<MarketInfo>>, StorageError> {
        do_find_by_event(self.txn, event_id).await
    }

    async fn find_existing_ids(&self, ids: &[MarketId]) -> Result<HashSet<String>, StorageError> {
        do_find_existing_ids(self.txn, ids).await
    }

    async fn upsert(&self, market: UpsertMarket) -> Result<Arc<MarketInfo>, StorageError> {
        do_upsert(self.txn, market).await
    }

    async fn upsert_batch(&self, markets: Vec<UpsertMarket>) -> Result<u64, StorageError> {
        do_upsert_batch(self.txn, markets).await
    }

    async fn update_status(
        &self,
        id: &MarketId,
        status: &str,
        outcome: Option<&str>,
    ) -> Result<(), StorageError> {
        do_update_status(self.txn, id, status, outcome).await
    }
}

#[cfg(test)]
mod tests {
    use super::page_condition;
    use quant_pivot_models::{
        domain::{MarketPageQuery, pagination::PageRequest},
        enums::{common::MarketCategory, market::MarketStatus},
        types::{EventId, TokenId},
    };
    use sea_orm::{DbBackend, EntityTrait, QueryFilter, QueryTrait};
    use std::collections::HashSet;

    #[test]
    fn page_condition_adds_optional_filters_to_sql() {
        let query = MarketPageQuery {
            keyword: Some("election".into()),
            status: Some(MarketStatus::Active),
            category: Some(MarketCategory::Politics),
            event_id: Some(EventId::new("evt-1")),
            page: PageRequest::default(),
            ..Default::default()
        };

        let sql = super::MarketEntity::find()
            .filter(page_condition(&query))
            .build(DbBackend::Postgres)
            .to_string();

        assert!(sql.contains(r#""market"."status" ="#));
        assert!(sql.contains(r#""market"."event_id" ="#));
        assert!(sql.contains(r#""market"."categories" @> ARRAY["#));
        assert!(sql.contains("ILIKE"));
        assert!(sql.contains("election"));
    }

    #[test]
    fn page_condition_empty_matches_all_rows() {
        let query = MarketPageQuery::default();
        let sql = super::MarketEntity::find()
            .filter(page_condition(&query))
            .build(DbBackend::Postgres)
            .to_string();

        assert!(!sql.contains(r#""market"."status" ="#));
        assert!(!sql.contains(r#""market"."event_id" ="#));
        assert!(!sql.contains(r#""market"."categories" @> ARRAY["#));
        assert!(!sql.contains("ILIKE"));
    }

    #[test]
    fn page_condition_ignores_blank_keyword() {
        let query = MarketPageQuery {
            keyword: Some(String::new()),
            ..Default::default()
        };
        let sql = super::MarketEntity::find()
            .filter(page_condition(&query))
            .build(DbBackend::Postgres)
            .to_string();

        assert!(!sql.contains("ILIKE"));
    }

    #[test]
    fn page_condition_subscribed_true_requires_both_tokens_in_union() {
        let query = MarketPageQuery {
            subscribed: Some(true),
            resolved_subscribed_tokens: Some([TokenId::new("tok-yes")].into()),
            ..Default::default()
        };
        let sql = super::MarketEntity::find()
            .filter(page_condition(&query))
            .build(DbBackend::Postgres)
            .to_string();

        assert!(sql.contains(r#""market"."yes_token_id" IN"#));
        assert!(sql.contains(r#""market"."no_token_id" IN"#));
    }

    #[test]
    fn page_condition_subscribed_true_with_empty_union_matches_nothing() {
        let query = MarketPageQuery {
            subscribed: Some(true),
            resolved_subscribed_tokens: Some(HashSet::new()),
            ..Default::default()
        };
        let sql = super::MarketEntity::find()
            .filter(page_condition(&query))
            .build(DbBackend::Postgres)
            .to_string();

        assert!(sql.contains("1 = 0"));
    }

    #[test]
    fn page_condition_subscribed_false_excludes_fully_subscribed_pairs() {
        let query = MarketPageQuery {
            subscribed: Some(false),
            resolved_subscribed_tokens: Some([TokenId::new("tok-a"), TokenId::new("tok-b")].into()),
            ..Default::default()
        };
        let sql = super::MarketEntity::find()
            .filter(page_condition(&query))
            .build(DbBackend::Postgres)
            .to_string();

        assert!(sql.contains(r#""market"."yes_token_id" NOT IN"#));
        assert!(sql.contains(r#""market"."no_token_id" NOT IN"#));
    }
}

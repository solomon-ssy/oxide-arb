use std::{collections::HashSet, sync::Arc};

use quant_pivot_error::storage::{StorageError, entity::MARKET};
use quant_pivot_models::{
    domain::{
        api::MarketPageQuery,
        market::{MarketInfo, UpsertMarket},
        pagination::{PageWindow, Paginated},
    },
    entities::market::{
        ActiveModel as MarketActiveModel, Column as MarketColumn, Entity as MarketEntity,
    },
    enums::{
        catalog::CatalogFilterReason,
        common::{MarketCategory, TickSize},
        market::MarketStatus,
    },
    types::{MarketId, TokenId},
};
use sea_orm::{
    ActiveValue, ColumnTrait, DatabaseConnection, DatabaseTransaction, EntityTrait,
    IntoActiveModel, QueryFilter, QueryOrder,
    sea_query::{Condition, Expr, OnConflict, extension::postgres::PgExpr},
};

use crate::{
    postgres::{
        catalog::ingest::{find_existing_chunks, find_str_id_chunks},
        connection::RepositoryConnection,
        primitives,
        query::{non_empty, paginate_mapped},
        write::upsert_many_chunked,
    },
    traits::MarketRepository,
};

pub struct PgMarketRepository<C = DatabaseConnection> {
    db: C,
}

impl PgMarketRepository {
    pub const fn new(db: DatabaseConnection) -> Self {
        Self { db }
    }

    pub(crate) const fn with_txn(
        txn: &DatabaseTransaction,
    ) -> PgMarketRepository<&'_ DatabaseTransaction> {
        PgMarketRepository { db: txn }
    }
}

impl<C> PgMarketRepository<C> {
    fn upsert_conflict() -> OnConflict {
        OnConflict::column(MarketColumn::MarketId)
            .update_columns([
                MarketColumn::EventId,
                MarketColumn::Question,
                MarketColumn::Slug,
                MarketColumn::Description,
                MarketColumn::YesTokenId,
                MarketColumn::NoTokenId,
                MarketColumn::NegRisk,
                MarketColumn::Outcome,
                MarketColumn::StartDate,
                MarketColumn::EndDate,
                MarketColumn::ResolvedAt,
                MarketColumn::ContentHash,
            ])
            .values([
                (
                    MarketColumn::Categories,
                    primitives::excluded_enum_array::<MarketCategory>(MarketColumn::Categories),
                ),
                (
                    MarketColumn::Status,
                    primitives::excluded_enum::<MarketStatus>(MarketColumn::Status),
                ),
                (
                    MarketColumn::FilterReasons,
                    primitives::excluded_enum_array::<CatalogFilterReason>(
                        MarketColumn::FilterReasons,
                    ),
                ),
                (
                    MarketColumn::TickSize,
                    primitives::excluded_enum::<TickSize>(MarketColumn::TickSize),
                ),
            ])
            .to_owned()
    }

    fn page_condition(query: &MarketPageQuery) -> Condition {
        let mut condition = Condition::all()
            .add_option(query.status.map(|status| MarketColumn::Status.eq(status)))
            .add_option(
                query
                    .event_id
                    .as_ref()
                    .map(|event_id| MarketColumn::EventId.eq(event_id.clone())),
            );

        if query.category_unknown == Some(true) {
            condition = condition.add(primitives::enum_array_is_empty::<MarketCategory>((
                MarketEntity,
                MarketColumn::Categories,
            )));
        } else {
            condition = condition.add_option(query.category.map(|category| {
                primitives::enum_array_contains((MarketEntity, MarketColumn::Categories), &category)
            }));
        }

        if let (Some(want_subscribed), Some(tokens)) =
            (query.subscribed, query.resolved_subscribed_tokens.as_ref())
        {
            if want_subscribed {
                if tokens.is_empty() {
                    condition = condition.add(Expr::value(false));
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
}

#[async_trait::async_trait]
impl<C> MarketRepository for PgMarketRepository<C>
where
    C: RepositoryConnection,
{
    async fn find_by_id(&self, id: &MarketId) -> Result<Option<Arc<MarketInfo>>, StorageError> {
        MarketEntity::find_by_id(id.clone())
            .one(self.db.connection())
            .await
            .map_err(StorageError::from)
            .map(|market| market.map(|model| Arc::new(model.into())))
    }

    async fn find_by_ids(&self, ids: &[MarketId]) -> Result<Vec<Arc<MarketInfo>>, StorageError> {
        find_str_id_chunks::<MarketEntity, _, _, _>(
            self.db.connection(),
            ids,
            MarketColumn::MarketId,
            MarketId::as_str,
        )
        .await
        .map(|markets| {
            markets
                .into_iter()
                .map(|model| Arc::new(model.into()))
                .collect()
        })
    }

    async fn find_by_tokens(
        &self,
        token_ids: &[TokenId],
    ) -> Result<Vec<Arc<MarketInfo>>, StorageError> {
        if token_ids.is_empty() {
            return Ok(Vec::new());
        }
        MarketEntity::find()
            .filter(
                Condition::any()
                    .add(MarketColumn::YesTokenId.is_in(token_ids.to_vec()))
                    .add(MarketColumn::NoTokenId.is_in(token_ids.to_vec())),
            )
            .all(self.db.connection())
            .await
            .map_err(StorageError::from)
            .map(|markets| {
                markets
                    .into_iter()
                    .map(|model| Arc::new(model.into()))
                    .collect()
            })
    }

    async fn page(&self, query: MarketPageQuery) -> Result<Paginated<MarketInfo>, StorageError> {
        paginate_mapped(
            MarketEntity::find()
                .filter(Self::page_condition(&query))
                .order_by_desc(MarketColumn::UpdatedAt)
                .order_by_asc(MarketColumn::MarketId),
            self.db.connection(),
            PageWindow::from_query(&query),
            Into::into,
        )
        .await
    }

    async fn find_active(&self) -> Result<Arc<[MarketInfo]>, StorageError> {
        MarketEntity::find()
            .filter(MarketColumn::Status.eq(MarketStatus::Active))
            .all(self.db.connection())
            .await
            .map_err(StorageError::from)
            .map(|markets| {
                markets
                    .into_iter()
                    .map(Into::into)
                    .collect::<Vec<_>>()
                    .into()
            })
    }

    async fn find_by_event(&self, event_id: &str) -> Result<Vec<Arc<MarketInfo>>, StorageError> {
        MarketEntity::find()
            .filter(MarketColumn::EventId.eq(event_id))
            .all(self.db.connection())
            .await
            .map_err(StorageError::from)
            .map(|markets| {
                markets
                    .into_iter()
                    .map(|model| Arc::new(model.into()))
                    .collect()
            })
    }

    async fn find_existing_ids(&self, ids: &[MarketId]) -> Result<HashSet<String>, StorageError> {
        find_existing_chunks::<MarketEntity, _, _, _>(
            self.db.connection(),
            ids,
            MarketColumn::MarketId,
            MarketId::as_str,
        )
        .await
    }

    async fn upsert(&self, market: UpsertMarket) -> Result<Arc<MarketInfo>, StorageError> {
        let model = MarketEntity::insert(market.into_active_model())
            .on_conflict(Self::upsert_conflict())
            .exec_with_returning(self.db.connection())
            .await
            .map_err(StorageError::from)?;
        Ok(Arc::new(model.into()))
    }

    async fn upsert_batch(&self, markets: Vec<UpsertMarket>) -> Result<u64, StorageError> {
        upsert_many_chunked::<MarketEntity, UpsertMarket>(
            self.db.connection(),
            markets,
            Self::upsert_conflict(),
        )
        .await
    }

    async fn update_status(
        &self,
        id: &MarketId,
        status: MarketStatus,
        outcome: Option<&str>,
    ) -> Result<(), StorageError> {
        let current = MarketEntity::find_by_id(id.clone())
            .one(self.db.connection())
            .await
            .map_err(StorageError::from)?
            .ok_or_else(|| StorageError::not_found(MARKET, id))?;
        if status == MarketStatus::Active && !current.filter_reasons.is_empty() {
            return Err(StorageError::state_conflict(
                MARKET,
                Some(id),
                "an upstream-filtered market cannot be activated until filter reasons clear",
            ));
        }
        let mut model = MarketActiveModel {
            market_id: ActiveValue::Set(id.clone()),
            status: ActiveValue::Set(status),
            outcome: ActiveValue::Set(outcome.map(str::to_owned)),
            ..Default::default()
        };
        model.updated_at = ActiveValue::NotSet;
        MarketEntity::update(model)
            .exec(self.db.connection())
            .await
            .map_err(StorageError::from)?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashSet;

    use quant_pivot_models::{
        domain::{api::MarketPageQuery, pagination::PageRequest},
        enums::{common::MarketCategory, market::MarketStatus},
        types::{EventId, TokenId},
    };
    use sea_orm::{DatabaseConnection, DbBackend, EntityTrait, QueryFilter, QueryTrait};

    use super::{MarketEntity, PgMarketRepository};

    #[test]
    fn page_adds_optional_sql() {
        let query = MarketPageQuery {
            keyword: Some("election".into()),
            status: Some(MarketStatus::Active),
            category: Some(MarketCategory::Politics),
            event_id: Some(EventId::new("evt-1")),
            page: PageRequest::default(),
            ..Default::default()
        };

        let sql = MarketEntity::find()
            .filter(PgMarketRepository::<DatabaseConnection>::page_condition(
                &query,
            ))
            .build(DbBackend::Postgres)
            .to_string();

        assert!(sql.contains(r#""market"."status" ="#));
        assert!(sql.contains(r#""market"."event_id" ="#));
        assert!(sql.contains("qp_market_category"), "{sql}");
        assert!(sql.contains(r#"= ANY("market"."categories")"#), "{sql}");
        assert!(sql.contains("ILIKE"));
        assert!(sql.contains("election"));
    }

    #[test]
    fn page_empty_matches_rows() {
        let query = MarketPageQuery::default();
        let sql = MarketEntity::find()
            .filter(PgMarketRepository::<DatabaseConnection>::page_condition(
                &query,
            ))
            .build(DbBackend::Postgres)
            .to_string();

        assert!(!sql.contains(r#""market"."status" ="#));
        assert!(!sql.contains(r#""market"."event_id" ="#));
        assert!(!sql.contains(r#""market"."categories" @> ARRAY["#));
        assert!(!sql.contains("ILIKE"));
    }

    #[test]
    fn page_condition_ignores_keyword() {
        let query = MarketPageQuery {
            keyword: Some(String::new()),
            ..Default::default()
        };
        let sql = MarketEntity::find()
            .filter(PgMarketRepository::<DatabaseConnection>::page_condition(
                &query,
            ))
            .build(DbBackend::Postgres)
            .to_string();

        assert!(!sql.contains("ILIKE"));
    }

    #[test]
    fn page_condition_requires_union() {
        let query = MarketPageQuery {
            subscribed: Some(true),
            resolved_subscribed_tokens: Some([TokenId::new("tok-yes")].into()),
            ..Default::default()
        };
        let sql = MarketEntity::find()
            .filter(PgMarketRepository::<DatabaseConnection>::page_condition(
                &query,
            ))
            .build(DbBackend::Postgres)
            .to_string();

        assert!(sql.contains(r#""market"."yes_token_id" IN"#));
        assert!(sql.contains(r#""market"."no_token_id" IN"#));
    }

    #[test]
    fn page_unknown_empty_array() {
        let query = MarketPageQuery {
            category_unknown: Some(true),
            ..Default::default()
        };
        let sql = MarketEntity::find()
            .filter(PgMarketRepository::<DatabaseConnection>::page_condition(
                &query,
            ))
            .build(DbBackend::Postgres)
            .to_string();

        assert!(
            sql.contains(r#""market"."categories" = CAST('{}' AS qp_market_category[])"#),
            "{sql}"
        );
    }

    #[test]
    fn page_empty_matches_nothing() {
        let query = MarketPageQuery {
            subscribed: Some(true),
            resolved_subscribed_tokens: Some(HashSet::new()),
            ..Default::default()
        };
        let sql = MarketEntity::find()
            .filter(PgMarketRepository::<DatabaseConnection>::page_condition(
                &query,
            ))
            .build(DbBackend::Postgres)
            .to_string();

        assert!(sql.contains("WHERE FALSE"), "{sql}");
    }

    #[test]
    fn page_condition_excludes_pairs() {
        let query = MarketPageQuery {
            subscribed: Some(false),
            resolved_subscribed_tokens: Some([TokenId::new("tok-a"), TokenId::new("tok-b")].into()),
            ..Default::default()
        };
        let sql = MarketEntity::find()
            .filter(PgMarketRepository::<DatabaseConnection>::page_condition(
                &query,
            ))
            .build(DbBackend::Postgres)
            .to_string();

        assert!(sql.contains(r#""market"."yes_token_id" NOT IN"#));
        assert!(sql.contains(r#""market"."no_token_id" NOT IN"#));
    }
}

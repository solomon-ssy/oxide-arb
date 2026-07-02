use crate::{
    postgres::{
        catalog::ingest::{
            find_existing_str_id_chunks, find_models_by_str_id_chunks, upsert_many_chunked,
        },
        error,
        query::paginate_mapped,
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
    types::MarketId,
};
use sea_orm::{
    ActiveValue, ColumnTrait, ConnectionTrait, DatabaseConnection, DatabaseTransaction,
    EntityTrait, IntoActiveModel, QueryFilter, QueryOrder, QueryTrait, sea_query::OnConflict,
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

async fn do_page(
    db: &impl ConnectionTrait,
    query: MarketPageQuery,
) -> Result<Paginated<MarketInfo>, StorageError> {
    let normalized = query.normalized();
    let select = MarketEntity::find()
        .apply_if(normalized.status, |query, status| {
            query.filter(MarketColumn::Status.eq(status))
        })
        .apply_if(normalized.keyword.as_deref(), |query, search| {
            let pattern = format!("%{}%", search.replace('%', "\\%").replace('_', "\\_"));
            query.filter(
                MarketColumn::Question
                    .contains(&pattern)
                    .or(MarketColumn::Slug.contains(&pattern)),
            )
        })
        .order_by_desc(MarketColumn::UpdatedAt);
    paginate_mapped(select, db, &normalized.page, Into::into).await
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

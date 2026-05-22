use crate::traits::PositionRepository;
use chrono::Utc;
use oxide_arb_error::storage::StorageError;
use oxide_arb_models::domain::{NewPosition, UpdatePosition};
use oxide_arb_models::entities::position::{self, ActiveModel, Column, Entity};
use oxide_arb_models::enums::common::PositionStatus;
use oxide_arb_models::types::{MarketId, PositionId, Usd};
use rust_decimal::Decimal;
#[allow(clippy::wildcard_imports)]
use sea_orm::*;

// ── helpers ──────────────────────────────────────────────────────────

async fn find_open_q(db: &impl ConnectionTrait) -> Result<Vec<position::Model>, StorageError> {
    Entity::find()
        .filter(Column::Status.eq(PositionStatus::Open))
        .all(db)
        .await
        .map_err(StorageError::from)
}

async fn find_by_id_q(
    db: &impl ConnectionTrait,
    position_id: &PositionId,
) -> Result<Option<position::Model>, StorageError> {
    Entity::find_by_id(position_id.clone())
        .one(db)
        .await
        .map_err(StorageError::from)
}

async fn find_by_market_q(
    db: &impl ConnectionTrait,
    market_id: &MarketId,
) -> Result<Vec<position::Model>, StorageError> {
    Entity::find()
        .filter(Column::MarketId.eq(market_id.as_str()))
        .all(db)
        .await
        .map_err(StorageError::from)
}

async fn create_q(
    db: &impl ConnectionTrait,
    new: NewPosition,
) -> Result<position::Model, StorageError> {
    new.into_active_model()
        .insert(db)
        .await
        .map_err(StorageError::from)
}

async fn update_q(
    db: &impl ConnectionTrait,
    position_id: &PositionId,
    update: UpdatePosition,
) -> Result<position::Model, StorageError> {
    let existing = Entity::find_by_id(position_id.clone())
        .one(db)
        .await
        .map_err(StorageError::from)?
        .ok_or_else(|| StorageError::NotFound {
            entity: "position",
            id: position_id.to_string(),
        })?;

    let mut active: ActiveModel = existing.into();
    if let Some(shares) = update.shares {
        active.shares = Set(shares);
    }
    if let Some(price) = update.avg_entry_price {
        active.avg_entry_price = Set(price);
    }
    if let Some(cost) = update.total_cost_usd {
        active.total_cost_usd = Set(cost);
    }
    if let Some(fees) = update.total_fees_usd {
        active.total_fees_usd = Set(fees);
    }
    if let Some(pnl) = update.unrealized_pnl {
        active.unrealized_pnl = Set(pnl);
    }
    if let Some(pnl) = update.realized_pnl {
        active.realized_pnl = Set(pnl);
    }
    if let Some(status) = update.status {
        active.status = Set(status);
    }
    if let Some(closed) = update.closed_at {
        active.closed_at = Set(Some(closed));
    }
    if let Some(settled) = update.settled_at {
        active.settled_at = Set(Some(settled));
    }

    active.update(db).await.map_err(StorageError::from)
}

async fn close_position_q(
    db: &impl ConnectionTrait,
    position_id: &PositionId,
    realized_pnl: Decimal,
) -> Result<(), StorageError> {
    let existing = Entity::find_by_id(position_id.clone())
        .one(db)
        .await
        .map_err(StorageError::from)?
        .ok_or_else(|| StorageError::NotFound {
            entity: "position",
            id: position_id.to_string(),
        })?;

    let mut active: ActiveModel = existing.into();
    active.status = Set(PositionStatus::Closed);
    active.realized_pnl = Set(Usd::new(realized_pnl));
    active.closed_at = Set(Some(Utc::now()));
    active.update(db).await.map_err(StorageError::from)?;
    Ok(())
}

async fn settle_position_q(
    db: &impl ConnectionTrait,
    position_id: &PositionId,
    realized_pnl: Decimal,
) -> Result<(), StorageError> {
    let existing = Entity::find_by_id(position_id.clone())
        .one(db)
        .await
        .map_err(StorageError::from)?
        .ok_or_else(|| StorageError::NotFound {
            entity: "position",
            id: position_id.to_string(),
        })?;

    let mut active: ActiveModel = existing.into();
    active.status = Set(PositionStatus::Settled);
    active.realized_pnl = Set(Usd::new(realized_pnl));
    active.settled_at = Set(Some(Utc::now()));
    active.update(db).await.map_err(StorageError::from)?;
    Ok(())
}

async fn total_exposure_q(db: &impl ConnectionTrait) -> Result<Usd, StorageError> {
    // TODO(cache): cache per-market position summaries with
    // `CacheKey::PositionSummary { market_id }` once the risk service owns
    // invalidation after position, trade, and settlement updates.
    let positions = Entity::find()
        .filter(Column::Status.eq(PositionStatus::Open))
        .all(db)
        .await
        .map_err(StorageError::from)?;

    Ok(positions.iter().map(|p| p.total_cost_usd).sum())
}

async fn count_open_q(db: &impl ConnectionTrait) -> Result<usize, StorageError> {
    let count = Entity::find()
        .filter(Column::Status.eq(PositionStatus::Open))
        .count(db)
        .await
        .map_err(StorageError::from)?;
    #[allow(clippy::cast_possible_truncation)]
    Ok(count as usize)
}

// ── connection-based impl ────────────────────────────────────────────

pub struct PgPositionRepository {
    db: DatabaseConnection,
}

impl PgPositionRepository {
    pub const fn new(db: DatabaseConnection) -> Self {
        Self { db }
    }

    pub const fn with_txn(txn: &DatabaseTransaction) -> PgPositionRepositoryTxn<'_> {
        PgPositionRepositoryTxn { txn }
    }
}

impl PositionRepository for PgPositionRepository {
    async fn find_open(&self) -> Result<Vec<position::Model>, StorageError> {
        find_open_q(&self.db).await
    }

    async fn find_by_id(
        &self,
        position_id: &PositionId,
    ) -> Result<Option<position::Model>, StorageError> {
        find_by_id_q(&self.db, position_id).await
    }

    async fn find_by_market(
        &self,
        market_id: &MarketId,
    ) -> Result<Vec<position::Model>, StorageError> {
        find_by_market_q(&self.db, market_id).await
    }

    async fn create(&self, position: NewPosition) -> Result<position::Model, StorageError> {
        create_q(&self.db, position).await
    }

    async fn update(
        &self,
        position_id: &PositionId,
        update: UpdatePosition,
    ) -> Result<position::Model, StorageError> {
        update_q(&self.db, position_id, update).await
    }

    async fn close_position(
        &self,
        position_id: &PositionId,
        realized_pnl: Decimal,
    ) -> Result<(), StorageError> {
        close_position_q(&self.db, position_id, realized_pnl).await
    }

    async fn settle_position(
        &self,
        position_id: &PositionId,
        realized_pnl: Decimal,
    ) -> Result<(), StorageError> {
        settle_position_q(&self.db, position_id, realized_pnl).await
    }

    async fn total_exposure(&self) -> Result<Usd, StorageError> {
        total_exposure_q(&self.db).await
    }

    async fn count_open(&self) -> Result<usize, StorageError> {
        count_open_q(&self.db).await
    }
}

// ── transaction-based impl ───────────────────────────────────────────

pub struct PgPositionRepositoryTxn<'a> {
    txn: &'a DatabaseTransaction,
}

impl PositionRepository for PgPositionRepositoryTxn<'_> {
    async fn find_open(&self) -> Result<Vec<position::Model>, StorageError> {
        find_open_q(self.txn).await
    }

    async fn find_by_id(
        &self,
        position_id: &PositionId,
    ) -> Result<Option<position::Model>, StorageError> {
        find_by_id_q(self.txn, position_id).await
    }

    async fn find_by_market(
        &self,
        market_id: &MarketId,
    ) -> Result<Vec<position::Model>, StorageError> {
        find_by_market_q(self.txn, market_id).await
    }

    async fn create(&self, position: NewPosition) -> Result<position::Model, StorageError> {
        create_q(self.txn, position).await
    }

    async fn update(
        &self,
        position_id: &PositionId,
        update: UpdatePosition,
    ) -> Result<position::Model, StorageError> {
        update_q(self.txn, position_id, update).await
    }

    async fn close_position(
        &self,
        position_id: &PositionId,
        realized_pnl: Decimal,
    ) -> Result<(), StorageError> {
        close_position_q(self.txn, position_id, realized_pnl).await
    }

    async fn settle_position(
        &self,
        position_id: &PositionId,
        realized_pnl: Decimal,
    ) -> Result<(), StorageError> {
        settle_position_q(self.txn, position_id, realized_pnl).await
    }

    async fn total_exposure(&self) -> Result<Usd, StorageError> {
        total_exposure_q(self.txn).await
    }

    async fn count_open(&self) -> Result<usize, StorageError> {
        count_open_q(self.txn).await
    }
}

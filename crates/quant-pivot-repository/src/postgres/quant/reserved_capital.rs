//! Postgres-backed reserved-capital reader.

use rust_decimal::Decimal;
use sea_orm::{
    ColumnTrait, DatabaseConnection, EntityTrait, FromQueryResult, JoinType, QueryFilter,
    QuerySelect, RelationTrait, sea_query::Expr,
};

use crate::traits::ReservedCapitalRepository;
use quant_pivot_error::storage::StorageError;
use quant_pivot_models::{
    entities::quant_order_intent, enums::quant::OrderIntentStatus, types::Usd,
};

/// Order-intent statuses that lock capital (not yet spent or terminal).
const LOCKED_STATUSES: [OrderIntentStatus; 5] = [
    OrderIntentStatus::PendingApproval,
    OrderIntentStatus::Approved,
    OrderIntentStatus::ApprovedByPolicy,
    OrderIntentStatus::Submitted,
    OrderIntentStatus::PartiallyFilled,
];

/// Postgres-backed reserved-capital reader.
pub struct PgReservedCapitalRepository {
    db: DatabaseConnection,
}

impl PgReservedCapitalRepository {
    pub const fn new(db: DatabaseConnection) -> Self {
        Self { db }
    }
}

#[derive(Debug, FromQueryResult)]
struct LockedCapitalSum {
    total: Option<Decimal>,
}

#[async_trait::async_trait]
impl ReservedCapitalRepository for PgReservedCapitalRepository {
    async fn sum_locked_usd(&self) -> Result<Usd, StorageError> {
        let row = quant_order_intent::Entity::find()
            .join(
                JoinType::InnerJoin,
                quant_order_intent::Relation::Recommendation.def(),
            )
            .filter(quant_order_intent::Column::Status.is_in(LOCKED_STATUSES))
            .select_only()
            .column_as(
                Expr::cust("SUM((quant_recommendation.sizing_plan->>'suggested_usd')::numeric)"),
                "total",
            )
            .into_model::<LockedCapitalSum>()
            .one(&self.db)
            .await
            .map_err(StorageError::from)?;

        let total = row.and_then(|row| row.total).unwrap_or(Decimal::ZERO);
        Ok(Usd::new(total))
    }
}

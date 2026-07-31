//! `quant_execution_attempt_outcome` table entity.

use chrono::{DateTime, Utc};
use sea_orm::entity::prelude::*;

use super::{
    quant_execution_order, quant_order_intent, quant_position, quant_recommendation,
    quant_recommendation_execution_rollup_attempt, quant_reconciliation,
};
use crate::{
    enums::{
        execution::{ExitReason, PositionLedgerState},
        quant::{
            ExecutionAttemptNoFillReason, ExecutionAttemptTerminalState, ExecutionOrderState,
            QuantRuntimeMode,
        },
    },
    types::{
        ContentHash, ExecutionAccountId, ExecutionOrderId, MarketId, OrderIntentId, PositionId,
        Price, RecommendationId, ReconciliationId, SchemaVersion, Shares, TokenId, Usd,
    },
};

#[sea_orm::model]
#[derive(Clone, Debug, PartialEq, Eq, DeriveEntityModel)]
#[sea_orm(table_name = "quant_execution_attempt_outcome")]
pub struct Model {
    pub recommendation_id: RecommendationId,
    #[sea_orm(primary_key, auto_increment = false)]
    pub order_intent_id: OrderIntentId,
    pub entry_execution_order_id: ExecutionOrderId,
    pub entry_reconciliation_id: ReconciliationId,
    pub position_id: Option<PositionId>,
    pub execution_account_id: ExecutionAccountId,
    pub market_id: MarketId,
    pub token_id: TokenId,
    #[sea_orm(column_type = r#"custom("qp_quant_runtime_mode")"#)]
    pub runtime_mode: QuantRuntimeMode,
    pub terminal_state: ExecutionAttemptTerminalState,
    pub no_fill_reason: Option<ExecutionAttemptNoFillReason>,
    pub entry_order_state: ExecutionOrderState,
    pub requested_shares: Shares,
    pub filled_shares: Shares,
    pub entry_avg_price: Option<Price>,
    pub entry_fee_usd: Option<Usd>,
    pub entry_filled_at: Option<DateTime<Utc>>,
    pub position_terminal_state: Option<PositionLedgerState>,
    pub exit_reason: Option<ExitReason>,
    pub exit_filled_shares: Option<Shares>,
    pub exit_avg_price: Option<Price>,
    pub exit_fee_usd: Option<Usd>,
    pub exit_at: Option<DateTime<Utc>>,
    pub settlement_payout_usd: Option<Usd>,
    pub realized_pnl_usd: Option<Usd>,
    pub terminal_at: DateTime<Utc>,
    pub source_observed_at: DateTime<Utc>,
    pub available_at: DateTime<Utc>,
    pub source_checkpoint_hash: ContentHash,
    pub execution_fact_hash: ContentHash,
    pub execution_fact_schema_version: SchemaVersion,
    pub outcome_hash: ContentHash,
    pub created_at: DateTime<Utc>,

    #[sea_orm(
        belongs_to,
        relation_enum = "Recommendation",
        from = "recommendation_id",
        to = "recommendation_id"
    )]
    pub recommendation: BelongsTo<quant_recommendation::Entity>,
    #[sea_orm(
        belongs_to,
        relation_enum = "OrderIntent",
        from = "order_intent_id",
        to = "order_intent_id"
    )]
    pub order_intent: BelongsTo<quant_order_intent::Entity>,
    #[sea_orm(
        belongs_to,
        relation_enum = "EntryExecutionOrder",
        from = "entry_execution_order_id",
        to = "execution_order_id"
    )]
    pub entry_execution_order: BelongsTo<quant_execution_order::Entity>,
    #[sea_orm(
        belongs_to,
        relation_enum = "EntryReconciliation",
        from = "entry_reconciliation_id",
        to = "reconciliation_id"
    )]
    pub entry_reconciliation: BelongsTo<quant_reconciliation::Entity>,
    #[sea_orm(
        belongs_to,
        relation_enum = "Position",
        from = "position_id",
        to = "position_id"
    )]
    pub position: BelongsTo<Option<quant_position::Entity>>,
    #[sea_orm(has_one, relation_enum = "RollupAttempt")]
    pub rollup_attempt: HasOne<quant_recommendation_execution_rollup_attempt::Entity>,
}

impl ActiveModelBehavior for ActiveModel {}

use oxide_arb_macros::oxide_schema;
use sea_orm::{
    Iden,
    sea_query::{
        ColumnDef, ForeignKey, ForeignKeyAction, Index, IndexOrder, Table, TableCreateStatement,
    },
};

use crate::{
    enums::common::TradeState,
    idens::{event::Event, market::Market},
    schema::{
        column,
        dependency::TableDependency,
        index::{IndexBuildMode, IndexSpec},
        seed::SeedSpec,
        timestamp_with_write_default,
    },
};

#[oxide_schema(lifecycle = "ledger")]
pub enum Trade {
    Table,
    TradeId,
    ExecutionId,
    ReservationId,
    OpportunityId,
    MarketId,
    EventId,
    TokenId,
    Side,
    Shares,
    Price,
    CostUsd,
    FeeUsd,
    DetectedEdgeBps,
    DetectedProfitUsd,
    NetProfitUsd,
    OrderId,
    TxHash,
    State,
    BusinessOutcome,
    ScoredSnapshot,
    Category,
    NeedsReconcile,
    ReconcileResolution,
    ReconciledAt,
    ReconcileNote,
    PostTradeClaimOwner,
    PostTradeClaimedAt,
    PostTradeAttempts,
    ExecutionMode,
    LatencyMs,
    ErrorMessage,
    SubmittedAt,
    ConfirmedAt,
    CreatedAt,
    UpdatedAt,
}

pub fn table() -> TableCreateStatement {
    Table::create()
        .table(Trade::Table)
        .if_not_exists()
        .col(column::uuid_pk(Trade::TradeId))
        .col(column::uuid_fk(Trade::ExecutionId))
        .col(column::uuid_fk(Trade::ReservationId))
        .col(column::uuid_fk(Trade::OpportunityId))
        .col(column::market_id(Trade::MarketId))
        .col(column::text_id(Trade::EventId))
        .col(column::token_id(Trade::TokenId))
        .col(ColumnDef::new(Trade::Side).text().not_null())
        .col(column::shares(Trade::Shares))
        .col(column::price(Trade::Price))
        .col(column::usd(Trade::CostUsd))
        .col(column::usd(Trade::FeeUsd))
        .col(column::bps_null(Trade::DetectedEdgeBps))
        .col(column::usd_null(Trade::DetectedProfitUsd))
        .col(column::usd_null(Trade::NetProfitUsd))
        .col(column::text_id_null(Trade::OrderId))
        .col(ColumnDef::new(Trade::TxHash).text().null())
        .col(
            ColumnDef::new(Trade::State)
                .text()
                .not_null()
                .default(TradeState::Intent),
        )
        .col(ColumnDef::new(Trade::BusinessOutcome).text().null())
        .col(
            ColumnDef::new(Trade::ScoredSnapshot)
                .json_binary()
                .not_null(),
        )
        .col(ColumnDef::new(Trade::Category).text().not_null())
        .col(
            ColumnDef::new(Trade::NeedsReconcile)
                .boolean()
                .not_null()
                .default(false),
        )
        .col(ColumnDef::new(Trade::ReconcileResolution).text().null())
        .col(
            ColumnDef::new(Trade::ReconciledAt)
                .timestamp_with_time_zone()
                .null(),
        )
        .col(ColumnDef::new(Trade::ReconcileNote).text().null())
        .col(ColumnDef::new(Trade::PostTradeClaimOwner).text().null())
        .col(
            ColumnDef::new(Trade::PostTradeClaimedAt)
                .timestamp_with_time_zone()
                .null(),
        )
        .col(
            ColumnDef::new(Trade::PostTradeAttempts)
                .integer()
                .not_null()
                .default(0),
        )
        .col(ColumnDef::new(Trade::ExecutionMode).text().not_null())
        .col(ColumnDef::new(Trade::LatencyMs).integer().null())
        .col(ColumnDef::new(Trade::ErrorMessage).text().null())
        .col(
            ColumnDef::new(Trade::SubmittedAt)
                .timestamp_with_time_zone()
                .null(),
        )
        .col(
            ColumnDef::new(Trade::ConfirmedAt)
                .timestamp_with_time_zone()
                .null(),
        )
        .col(timestamp_with_write_default(Trade::CreatedAt))
        .col(timestamp_with_write_default(Trade::UpdatedAt))
        .foreign_key(
            ForeignKey::create()
                .name("fk_trade_market")
                .from(Trade::Table, Trade::MarketId)
                .to(Market::Table, Market::MarketId)
                .on_delete(ForeignKeyAction::Restrict),
        )
        .foreign_key(
            ForeignKey::create()
                .name("fk_trade_event")
                .from(Trade::Table, Trade::EventId)
                .to(Event::Table, Event::EventId)
                .on_delete(ForeignKeyAction::Restrict),
        )
        .to_owned()
}

pub fn indexes() -> Vec<IndexSpec> {
    vec![
        simple_index(
            "idx_trades_execution_id",
            Trade::ExecutionId,
            "execution lookup",
        ),
        simple_index(
            "idx_trades_opportunity_id",
            Trade::OpportunityId,
            "opportunity lookup",
        ),
        simple_index("idx_trades_market_id", Trade::MarketId, "market lookup"),
        simple_index("idx_trades_event_id", Trade::EventId, "event lookup"),
        simple_index(
            "idx_trades_business_outcome",
            Trade::BusinessOutcome,
            "business-outcome report group-by",
        ),
        IndexSpec::raw(
            "idx_trade_post_trade_claim",
            trade_table_name,
            IndexBuildMode::Transactional,
            "CREATE INDEX IF NOT EXISTS idx_trade_post_trade_claim \
             ON trade (created_at, post_trade_claimed_at) \
             WHERE state IN (\
                'fill_observed', 'miss_observed', 'fail_observed', \
                'fill_processing', 'miss_processing', 'fail_processing'\
             )",
            "post-trade relay claim and expired lease scans",
        ),
        IndexSpec::raw(
            "idx_trade_submitted_stale",
            trade_table_name,
            IndexBuildMode::Transactional,
            "CREATE INDEX IF NOT EXISTS idx_trade_submitted_stale \
             ON trade (submitted_at) \
             WHERE state = 'submitted'",
            "orphan scan of stale submitted trades",
        ),
        IndexSpec::raw(
            "idx_trade_needs_reconcile",
            trade_table_name,
            IndexBuildMode::Transactional,
            "CREATE INDEX IF NOT EXISTS idx_trade_needs_reconcile \
             ON trade (created_at, trade_id) \
             WHERE needs_reconcile = TRUE",
            "operator and worker scans for unresolved venue outcomes",
        ),
        IndexSpec::sea_query(
            "idx_trades_created_at",
            trade_table_name,
            IndexBuildMode::Transactional,
            Index::create()
                .name("idx_trades_created_at")
                .table(Trade::Table)
                .col((Trade::CreatedAt, IndexOrder::Desc))
                .to_owned(),
            "recent trade scans",
        ),
        IndexSpec::sea_query(
            "idx_trades_market_id_created",
            trade_table_name,
            IndexBuildMode::Transactional,
            Index::create()
                .name("idx_trades_market_id_created")
                .table(Trade::Table)
                .col(Trade::MarketId)
                .col((Trade::CreatedAt, IndexOrder::Desc))
                .to_owned(),
            "market trade history ordered by recency",
        ),
    ]
}

pub fn dependencies() -> Vec<TableDependency> {
    vec![
        TableDependency::foreign_key(market_table_name),
        TableDependency::foreign_key(event_table_name),
    ]
}

pub const fn seed_units() -> Vec<SeedSpec> {
    Vec::new()
}

fn simple_index(name: &'static str, column: Trade, purpose: &'static str) -> IndexSpec {
    IndexSpec::sea_query(
        name,
        trade_table_name,
        IndexBuildMode::Transactional,
        Index::create()
            .name(name)
            .table(Trade::Table)
            .col(column)
            .to_owned(),
        purpose,
    )
}

fn trade_table_name() -> String {
    Trade::Table.to_string()
}

fn market_table_name() -> String {
    Market::Table.to_string()
}

fn event_table_name() -> String {
    Event::Table.to_string()
}

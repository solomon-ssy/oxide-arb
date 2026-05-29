use oxide_arb_macros::oxide_schema;
use sea_orm::{
    Iden,
    sea_query::{
        ColumnDef, ForeignKey, ForeignKeyAction, Index, IndexOrder, Table, TableCreateStatement,
    },
};

use crate::{
    idens::{event::Event, market::Market},
    schema::{
        dependency::TableDependency,
        index::{IndexBuildMode, IndexSpec},
        seed::SeedSpec,
    },
};

#[oxide_schema]
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
        .col(
            ColumnDef::new(Trade::TradeId)
                .text()
                .not_null()
                .primary_key(),
        )
        .col(ColumnDef::new(Trade::ExecutionId).text().not_null())
        .col(ColumnDef::new(Trade::ReservationId).text().not_null())
        .col(ColumnDef::new(Trade::OpportunityId).text().not_null())
        .col(ColumnDef::new(Trade::MarketId).text().not_null())
        .col(ColumnDef::new(Trade::EventId).text().not_null())
        .col(ColumnDef::new(Trade::TokenId).text().not_null())
        .col(ColumnDef::new(Trade::Side).text().not_null())
        .col(ColumnDef::new(Trade::Shares).text().not_null())
        .col(ColumnDef::new(Trade::Price).text().not_null())
        .col(ColumnDef::new(Trade::CostUsd).text().not_null())
        .col(ColumnDef::new(Trade::FeeUsd).text().not_null())
        .col(ColumnDef::new(Trade::DetectedEdgeBps).text().null())
        .col(ColumnDef::new(Trade::DetectedProfitUsd).text().null())
        .col(ColumnDef::new(Trade::NetProfitUsd).text().null())
        .col(ColumnDef::new(Trade::OrderId).text().null())
        .col(ColumnDef::new(Trade::TxHash).text().null())
        .col(ColumnDef::new(Trade::State).text().not_null())
        .col(ColumnDef::new(Trade::BusinessOutcome).text().extra(
            "GENERATED ALWAYS AS (CASE state \
                 WHEN 'fill_observed' THEN 'success' \
                 WHEN 'fill_processing' THEN 'success' \
                 WHEN 'settled' THEN 'success' \
                 WHEN 'miss_observed' THEN 'miss' \
                 WHEN 'miss_processing' THEN 'miss' \
                 WHEN 'missed' THEN 'miss' \
                 WHEN 'fail_observed' THEN 'failed' \
                 WHEN 'fail_processing' THEN 'failed' \
                 WHEN 'failed' THEN 'failed' \
                 WHEN 'orphaned' THEN 'failed' \
                 ELSE NULL END) STORED",
        ))
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
        .col(crate::schema::timestamp_with_write_default(
            Trade::CreatedAt,
        ))
        .col(crate::schema::timestamp_with_write_default(
            Trade::UpdatedAt,
        ))
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

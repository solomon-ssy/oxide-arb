use sea_orm::DeriveIden;

#[derive(DeriveIden)]
pub enum Trade {
    Table,
    TradeId,
    ExecutionId,
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
    Outcome,
    ExecutionMode,
    LatencyMs,
    ErrorMessage,
    ConfirmedAt,
    CreatedAt,
    UpdatedAt,
}

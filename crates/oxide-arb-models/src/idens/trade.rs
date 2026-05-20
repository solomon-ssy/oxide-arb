use sea_orm::DeriveIden;

#[derive(DeriveIden)]
pub enum Trade {
    Table,
    TradeId,
    CreatedAt,
    MarketId,
    EventId,
    Status,
    DetectedEdgeBps,
    DetectedProfitUsd,
    TotalCostUsd,
    TotalFeesUsd,
    TotalGasUsd,
    NetProfitUsd,
    NetProfitProjectedUsd,
    DetectionToExecMs,
    TxHash,
    ConfirmedAt,
    OpportunitySnapshot,
    ValidationSnapshot,
    ExecutionRecord,
    UpdatedAt,
}

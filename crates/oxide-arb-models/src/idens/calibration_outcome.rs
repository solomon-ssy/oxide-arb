use sea_orm::DeriveIden;

#[derive(DeriveIden)]
pub enum EndgameCalibrationOutcome {
    Table,
    Id,
    MarketId,
    Category,
    PriceZone,
    DurationBucket,
    PredictedYes,
    ActualYes,
    EntryPrice,
    ConfidenceAtEntry,
    ConvergenceSecs,
    ResolvedAt,
    CreatedAt,
}

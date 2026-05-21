use sea_orm::DeriveIden;

#[derive(DeriveIden)]
pub enum EndgameCalibrationBucket {
    Table,
    Id,
    Category,
    PriceZone,
    DurationBucket,
    TotalCount,
    CorrectCount,
    AlphaPrior,
    BetaPrior,
    PosteriorMean,
    UpdatedAt,
}

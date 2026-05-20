use sea_orm::DeriveIden;

#[derive(DeriveIden)]
pub enum EndgameCalibrationBucket {
    Table,
    Id,
    PriceZone,
    DurationBucket,
    ResolutionRate,
    SampleSize,
    ConfidenceAdjust,
    UpdatedAt,
}

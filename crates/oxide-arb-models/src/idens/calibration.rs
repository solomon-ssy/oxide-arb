use oxide_arb_macros::oxide_schema;
use sea_orm::{
    Iden,
    sea_query::{ColumnDef, Index, Table, TableCreateStatement},
};

use crate::{
    schema::{
        dependency::TableDependency,
        index::{IndexBuildMode, IndexSpec},
        seed::SeedSpec,
        timestamp_with_write_default,
    },
    types::Probability,
};

#[oxide_schema]
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

pub fn table() -> TableCreateStatement {
    Table::create()
        .table(EndgameCalibrationBucket::Table)
        .if_not_exists()
        .col(
            ColumnDef::new(EndgameCalibrationBucket::Id)
                .integer()
                .not_null()
                .auto_increment()
                .primary_key(),
        )
        .col(
            ColumnDef::new(EndgameCalibrationBucket::Category)
                .text()
                .not_null(),
        )
        .col(
            ColumnDef::new(EndgameCalibrationBucket::PriceZone)
                .text()
                .not_null(),
        )
        .col(
            ColumnDef::new(EndgameCalibrationBucket::DurationBucket)
                .text()
                .not_null(),
        )
        .col(
            ColumnDef::new(EndgameCalibrationBucket::TotalCount)
                .integer()
                .not_null()
                .default(0),
        )
        .col(
            ColumnDef::new(EndgameCalibrationBucket::CorrectCount)
                .integer()
                .not_null()
                .default(0),
        )
        .col(
            ColumnDef::new(EndgameCalibrationBucket::AlphaPrior)
                .text()
                .not_null()
                .default(Probability::ONE),
        )
        .col(
            ColumnDef::new(EndgameCalibrationBucket::BetaPrior)
                .text()
                .not_null()
                .default(Probability::ONE),
        )
        .col(
            ColumnDef::new(EndgameCalibrationBucket::PosteriorMean)
                .text()
                .null(),
        )
        .col(timestamp_with_write_default(
            EndgameCalibrationBucket::UpdatedAt,
        ))
        .to_owned()
}

pub fn indexes() -> Vec<IndexSpec> {
    vec![IndexSpec::sea_query(
        "idx_cal_buckets_unique",
        calibration_bucket_table_name,
        IndexBuildMode::Transactional,
        Index::create()
            .name("idx_cal_buckets_unique")
            .table(EndgameCalibrationBucket::Table)
            .col(EndgameCalibrationBucket::Category)
            .col(EndgameCalibrationBucket::PriceZone)
            .col(EndgameCalibrationBucket::DurationBucket)
            .unique()
            .to_owned(),
        "unique calibration bucket dimensions",
    )]
}

pub const fn dependencies() -> Vec<TableDependency> {
    Vec::new()
}

pub const fn seed_units() -> Vec<SeedSpec> {
    Vec::new()
}

fn calibration_bucket_table_name() -> String {
    EndgameCalibrationBucket::Table.to_string()
}

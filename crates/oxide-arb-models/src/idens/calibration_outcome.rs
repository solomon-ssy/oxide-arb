use oxide_arb_macros::oxide_schema;
use sea_orm::{
    Iden,
    sea_query::{ColumnDef, ForeignKey, ForeignKeyAction, Index, Table, TableCreateStatement},
};

use crate::{
    idens::market::Market,
    schema::{
        dependency::TableDependency,
        index::{IndexBuildMode, IndexSpec},
        seed::SeedSpec,
        timestamp_with_write_default,
    },
};

#[oxide_schema]
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

pub fn table() -> TableCreateStatement {
    Table::create()
        .table(EndgameCalibrationOutcome::Table)
        .if_not_exists()
        .col(
            ColumnDef::new(EndgameCalibrationOutcome::Id)
                .big_integer()
                .not_null()
                .auto_increment()
                .primary_key(),
        )
        .col(
            ColumnDef::new(EndgameCalibrationOutcome::MarketId)
                .text()
                .not_null(),
        )
        .col(
            ColumnDef::new(EndgameCalibrationOutcome::Category)
                .text()
                .not_null(),
        )
        .col(
            ColumnDef::new(EndgameCalibrationOutcome::PriceZone)
                .text()
                .not_null(),
        )
        .col(
            ColumnDef::new(EndgameCalibrationOutcome::DurationBucket)
                .text()
                .not_null(),
        )
        .col(
            ColumnDef::new(EndgameCalibrationOutcome::PredictedYes)
                .boolean()
                .not_null(),
        )
        .col(
            ColumnDef::new(EndgameCalibrationOutcome::ActualYes)
                .boolean()
                .null(),
        )
        .col(
            ColumnDef::new(EndgameCalibrationOutcome::EntryPrice)
                .text()
                .not_null(),
        )
        .col(
            ColumnDef::new(EndgameCalibrationOutcome::ConfidenceAtEntry)
                .text()
                .not_null(),
        )
        .col(
            ColumnDef::new(EndgameCalibrationOutcome::ConvergenceSecs)
                .integer()
                .not_null(),
        )
        .col(
            ColumnDef::new(EndgameCalibrationOutcome::ResolvedAt)
                .timestamp_with_time_zone()
                .null(),
        )
        .col(timestamp_with_write_default(
            EndgameCalibrationOutcome::CreatedAt,
        ))
        .foreign_key(
            ForeignKey::create()
                .name("fk_cal_outcome_market")
                .from(
                    EndgameCalibrationOutcome::Table,
                    EndgameCalibrationOutcome::MarketId,
                )
                .to(Market::Table, Market::MarketId)
                .on_delete(ForeignKeyAction::Restrict),
        )
        .to_owned()
}

pub fn indexes() -> Vec<IndexSpec> {
    vec![
        IndexSpec::sea_query(
            "idx_cal_outcomes_market",
            calibration_outcome_table_name,
            IndexBuildMode::Transactional,
            Index::create()
                .name("idx_cal_outcomes_market")
                .table(EndgameCalibrationOutcome::Table)
                .col(EndgameCalibrationOutcome::MarketId)
                .to_owned(),
            "calibration outcomes by market",
        ),
        IndexSpec::raw(
            "idx_cal_outcomes_unresolved",
            calibration_outcome_table_name,
            IndexBuildMode::Transactional,
            "CREATE INDEX IF NOT EXISTS idx_cal_outcomes_unresolved \
             ON endgame_calibration_outcome (created_at) \
             WHERE actual_yes IS NULL",
            "unresolved calibration outcomes",
        ),
    ]
}

pub fn dependencies() -> Vec<TableDependency> {
    vec![TableDependency::foreign_key(market_table_name)]
}

pub const fn seed_units() -> Vec<SeedSpec> {
    Vec::new()
}

fn calibration_outcome_table_name() -> String {
    EndgameCalibrationOutcome::Table.to_string()
}

fn market_table_name() -> String {
    Market::Table.to_string()
}

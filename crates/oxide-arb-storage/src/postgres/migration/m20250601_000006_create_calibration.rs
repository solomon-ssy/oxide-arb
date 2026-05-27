use super::{execute_sql, migrate_up};
use oxide_arb_models::{
    idens::{
        calibration::EndgameCalibrationBucket, calibration_outcome::EndgameCalibrationOutcome,
        market::Market,
    },
    types::Probability,
};
use sea_orm_migration::prelude::*;

#[derive(DeriveMigrationName)]
pub struct Migration;

#[async_trait::async_trait]
impl MigrationTrait for Migration {
    async fn up(&self, manager: &SchemaManager) -> Result<(), DbErr> {
        migrate_up(
            manager,
            create_tables(),
            create_indexes(),
            specials(manager),
            seeding_data(manager),
        )
        .await
    }

    async fn down(&self, manager: &SchemaManager) -> Result<(), DbErr> {
        super::drop_tables(manager, drop_tables()).await
    }
}

fn create_tables() -> Vec<TableCreateStatement> {
    vec![calibration_bucket_table(), calibration_outcome_table()]
}

fn calibration_bucket_table() -> TableCreateStatement {
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
        .col(
            ColumnDef::new(EndgameCalibrationBucket::UpdatedAt)
                .timestamp_with_time_zone()
                .not_null()
                .default(Expr::current_timestamp()),
        )
        .to_owned()
}

fn calibration_outcome_table() -> TableCreateStatement {
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
        .col(
            ColumnDef::new(EndgameCalibrationOutcome::CreatedAt)
                .timestamp_with_time_zone()
                .not_null()
                .default(Expr::current_timestamp()),
        )
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

fn create_indexes() -> Vec<IndexCreateStatement> {
    vec![
        Index::create()
            .name("idx_cal_buckets_unique")
            .table(EndgameCalibrationBucket::Table)
            .col(EndgameCalibrationBucket::Category)
            .col(EndgameCalibrationBucket::PriceZone)
            .col(EndgameCalibrationBucket::DurationBucket)
            .unique()
            .to_owned(),
        Index::create()
            .name("idx_cal_outcomes_market")
            .table(EndgameCalibrationOutcome::Table)
            .col(EndgameCalibrationOutcome::MarketId)
            .to_owned(),
    ]
}

async fn specials(manager: &SchemaManager<'_>) -> Result<(), DbErr> {
    execute_sql(
        manager,
        ["CREATE INDEX IF NOT EXISTS idx_cal_outcomes_unresolved \
         ON endgame_calibration_outcome (created_at) \
         WHERE actual_yes IS NULL"],
    )
    .await
}

async fn seeding_data(manager: &SchemaManager<'_>) -> Result<(), DbErr> {
    super::noop(manager).await
}

fn drop_tables() -> Vec<TableDropStatement> {
    vec![
        Table::drop()
            .table(EndgameCalibrationOutcome::Table)
            .to_owned(),
        Table::drop()
            .table(EndgameCalibrationBucket::Table)
            .to_owned(),
    ]
}

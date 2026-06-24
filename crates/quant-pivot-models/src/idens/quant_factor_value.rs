use quant_pivot_macros::quant_schema;
use sea_orm::{
    Iden,
    sea_query::{
        ColumnDef, ForeignKey, ForeignKeyAction, Index, IndexOrder, Table, TableCreateStatement,
    },
};

use crate::{
    idens::{
        market::Market, quant_factor_definition::QuantFactorDefinition,
        quant_feature_vector::QuantFeatureVector, quant_model_run::QuantModelRun,
    },
    schema::{
        column,
        dependency::TableDependency,
        index::{IndexBuildMode, IndexSpec},
        seed::SeedSpec,
        timestamp_with_write_default,
    },
};

#[quant_schema(lifecycle = "ledger")]
pub enum QuantFactorValue {
    Table,
    FactorValueId,
    FactorDefinitionId,
    FeatureVectorId,
    ModelRunId,
    MarketId,
    AsOf,
    RawValue,
    NormalizedScore,
    Direction,
    Confidence,
    Explanation,
    CreatedAt,
}

pub fn table() -> TableCreateStatement {
    Table::create()
        .table(QuantFactorValue::Table)
        .if_not_exists()
        .col(column::uuid_pk(QuantFactorValue::FactorValueId))
        .col(column::uuid_fk(QuantFactorValue::FactorDefinitionId))
        .col(column::uuid_fk(QuantFactorValue::FeatureVectorId))
        // Owning online round. The 3.4 `ModelRunner` creates the `quant_model_run`
        // row (status `Running`) before the factor plane persists any value, so the
        // FK below always resolves. Indexed for `list_values_for_run`.
        .col(column::uuid_fk(QuantFactorValue::ModelRunId))
        .col(column::market_id(QuantFactorValue::MarketId))
        .col(
            ColumnDef::new(QuantFactorValue::AsOf)
                .timestamp_with_time_zone()
                .not_null(),
        )
        .col(
            ColumnDef::new(QuantFactorValue::RawValue)
                .decimal_len(28, 12)
                .null(),
        )
        .col(column::probability(QuantFactorValue::NormalizedScore))
        .col(
            ColumnDef::new(QuantFactorValue::Direction)
                .text()
                .not_null(),
        )
        .col(column::probability(QuantFactorValue::Confidence))
        .col(
            ColumnDef::new(QuantFactorValue::Explanation)
                .json_binary()
                .not_null(),
        )
        .col(timestamp_with_write_default(QuantFactorValue::CreatedAt))
        .foreign_key(
            ForeignKey::create()
                .name("fk_quant_factor_value_definition")
                .from(
                    QuantFactorValue::Table,
                    QuantFactorValue::FactorDefinitionId,
                )
                .to(
                    QuantFactorDefinition::Table,
                    QuantFactorDefinition::FactorDefinitionId,
                )
                .on_delete(ForeignKeyAction::Restrict),
        )
        .foreign_key(
            ForeignKey::create()
                .name("fk_quant_factor_value_feature_vector")
                .from(QuantFactorValue::Table, QuantFactorValue::FeatureVectorId)
                .to(
                    QuantFeatureVector::Table,
                    QuantFeatureVector::FeatureVectorId,
                )
                .on_delete(ForeignKeyAction::Restrict),
        )
        .foreign_key(
            ForeignKey::create()
                .name("fk_quant_factor_value_market")
                .from(QuantFactorValue::Table, QuantFactorValue::MarketId)
                .to(Market::Table, Market::MarketId)
                .on_delete(ForeignKeyAction::Restrict),
        )
        .foreign_key(
            ForeignKey::create()
                .name("fk_quant_factor_value_model_run")
                .from(QuantFactorValue::Table, QuantFactorValue::ModelRunId)
                .to(QuantModelRun::Table, QuantModelRun::ModelRunId)
                .on_delete(ForeignKeyAction::Restrict),
        )
        .to_owned()
}

pub fn indexes() -> Vec<IndexSpec> {
    vec![
        IndexSpec::sea_query(
            "idx_quant_factor_value_definition_as_of",
            quant_factor_value_table_name,
            IndexBuildMode::Transactional,
            Index::create()
                .name("idx_quant_factor_value_definition_as_of")
                .table(QuantFactorValue::Table)
                .col(QuantFactorValue::FactorDefinitionId)
                .col((QuantFactorValue::AsOf, IndexOrder::Desc))
                .to_owned(),
            "factor values by definition and PIT timestamp",
        ),
        IndexSpec::sea_query(
            "idx_quant_factor_value_market_as_of",
            quant_factor_value_table_name,
            IndexBuildMode::Transactional,
            Index::create()
                .name("idx_quant_factor_value_market_as_of")
                .table(QuantFactorValue::Table)
                .col(QuantFactorValue::MarketId)
                .col((QuantFactorValue::AsOf, IndexOrder::Desc))
                .to_owned(),
            "factor values by market and PIT timestamp",
        ),
        IndexSpec::sea_query(
            "idx_quant_factor_value_run",
            quant_factor_value_table_name,
            IndexBuildMode::Transactional,
            Index::create()
                .name("idx_quant_factor_value_run")
                .table(QuantFactorValue::Table)
                .col(QuantFactorValue::ModelRunId)
                .col((QuantFactorValue::AsOf, IndexOrder::Desc))
                .to_owned(),
            "factor values by owning model run and PIT timestamp",
        ),
    ]
}

pub fn dependencies() -> Vec<TableDependency> {
    vec![
        TableDependency::foreign_key(quant_factor_definition_table_name),
        TableDependency::foreign_key(quant_feature_vector_table_name),
        TableDependency::foreign_key(market_table_name),
        TableDependency::foreign_key(quant_model_run_table_name),
    ]
}

pub const fn seed_units() -> Vec<SeedSpec> {
    Vec::new()
}

fn quant_factor_value_table_name() -> String {
    QuantFactorValue::Table.to_string()
}

fn quant_factor_definition_table_name() -> String {
    QuantFactorDefinition::Table.to_string()
}

fn quant_feature_vector_table_name() -> String {
    QuantFeatureVector::Table.to_string()
}

fn market_table_name() -> String {
    Market::Table.to_string()
}

fn quant_model_run_table_name() -> String {
    QuantModelRun::Table.to_string()
}

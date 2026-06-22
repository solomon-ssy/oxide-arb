use quant_pivot_macros::quant_schema;
use sea_orm::{
    Iden,
    sea_query::{
        ColumnDef, ForeignKey, ForeignKeyAction, Index, IndexOrder, Table, TableCreateStatement,
    },
};

use crate::{
    idens::market::Market,
    schema::{
        column,
        dependency::TableDependency,
        index::{IndexBuildMode, IndexSpec},
        seed::SeedSpec,
        timestamp_with_write_default,
    },
};

#[quant_schema(lifecycle = "ledger")]
pub enum QuantFeatureVector {
    Table,
    FeatureVectorId,
    MarketId,
    TokenId,
    AsOf,
    FeatureSchemaVersion,
    FeatureHash,
    DataQuality,
    StalenessMs,
    Payload,
    SourceRefs,
    CreatedAt,
}

pub fn table() -> TableCreateStatement {
    Table::create()
        .table(QuantFeatureVector::Table)
        .if_not_exists()
        .col(column::uuid_pk(QuantFeatureVector::FeatureVectorId))
        .col(column::market_id(QuantFeatureVector::MarketId))
        .col(column::token_id_null(QuantFeatureVector::TokenId))
        .col(
            ColumnDef::new(QuantFeatureVector::AsOf)
                .timestamp_with_time_zone()
                .not_null(),
        )
        .col(
            ColumnDef::new(QuantFeatureVector::FeatureSchemaVersion)
                .integer()
                .not_null(),
        )
        .col(
            ColumnDef::new(QuantFeatureVector::FeatureHash)
                .text()
                .not_null(),
        )
        .col(
            ColumnDef::new(QuantFeatureVector::DataQuality)
                .text()
                .not_null(),
        )
        .col(
            ColumnDef::new(QuantFeatureVector::StalenessMs)
                .big_integer()
                .not_null(),
        )
        .col(
            ColumnDef::new(QuantFeatureVector::Payload)
                .json_binary()
                .not_null(),
        )
        .col(
            ColumnDef::new(QuantFeatureVector::SourceRefs)
                .json_binary()
                .not_null(),
        )
        .col(timestamp_with_write_default(QuantFeatureVector::CreatedAt))
        .foreign_key(
            ForeignKey::create()
                .name("fk_quant_feature_vector_market")
                .from(QuantFeatureVector::Table, QuantFeatureVector::MarketId)
                .to(Market::Table, Market::MarketId)
                .on_delete(ForeignKeyAction::Restrict),
        )
        .to_owned()
}

pub fn indexes() -> Vec<IndexSpec> {
    vec![
        IndexSpec::sea_query(
            "idx_quant_feature_vector_market_as_of",
            quant_feature_vector_table_name,
            IndexBuildMode::Transactional,
            Index::create()
                .name("idx_quant_feature_vector_market_as_of")
                .table(QuantFeatureVector::Table)
                .col(QuantFeatureVector::MarketId)
                .col((QuantFeatureVector::AsOf, IndexOrder::Desc))
                .to_owned(),
            "feature vectors by market and PIT timestamp",
        ),
        IndexSpec::sea_query(
            "idx_quant_feature_vector_hash",
            quant_feature_vector_table_name,
            IndexBuildMode::Transactional,
            Index::create()
                .name("idx_quant_feature_vector_hash")
                .table(QuantFeatureVector::Table)
                .col(QuantFeatureVector::FeatureHash)
                .to_owned(),
            "feature vector canonical hash lookup",
        ),
        IndexSpec::sea_query(
            "idx_quant_feature_vector_schema_as_of",
            quant_feature_vector_table_name,
            IndexBuildMode::Transactional,
            Index::create()
                .name("idx_quant_feature_vector_schema_as_of")
                .table(QuantFeatureVector::Table)
                .col(QuantFeatureVector::FeatureSchemaVersion)
                .col((QuantFeatureVector::AsOf, IndexOrder::Desc))
                .to_owned(),
            "feature vectors by schema version",
        ),
    ]
}

pub fn dependencies() -> Vec<TableDependency> {
    vec![TableDependency::foreign_key(market_table_name)]
}

pub const fn seed_units() -> Vec<SeedSpec> {
    Vec::new()
}

fn quant_feature_vector_table_name() -> String {
    QuantFeatureVector::Table.to_string()
}

fn market_table_name() -> String {
    Market::Table.to_string()
}

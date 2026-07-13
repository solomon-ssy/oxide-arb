use quant_pivot_macros::quant_schema;
use sea_orm::{
    Iden,
    sea_query::{
        ColumnDef, Expr, ForeignKey, ForeignKeyAction, Index, IndexOrder, Table,
        TableCreateStatement,
    },
};

use crate::{
    enums::quant::DataQualityStatus,
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
    DecisionAt,
    DecisionBoundary,
    FeatureSchemaVersion,
    FeatureHash,
    DataQuality,
    StalenessMs,
    Payload,
    SourceRefs,
    DecisionCapture,
    DecisionCaptureHash,
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
            ColumnDef::new(QuantFeatureVector::DecisionAt)
                .timestamp_with_time_zone()
                .not_null(),
        )
        // Nullable only for pre-v10 audit rows. Every v10 application write
        // supplies the full boundary and downstream replay rejects `NULL`.
        .col(
            ColumnDef::new(QuantFeatureVector::DecisionBoundary)
                .json_binary()
                .null(),
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
        .col(column::pg_enum::<DataQualityStatus>(
            QuantFeatureVector::DataQuality,
        ))
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
        .col(
            ColumnDef::new(QuantFeatureVector::DecisionCapture)
                .json_binary()
                .null(),
        )
        .col(
            ColumnDef::new(QuantFeatureVector::DecisionCaptureHash)
                .text()
                .null(),
        )
        .col(timestamp_with_write_default(QuantFeatureVector::CreatedAt))
        .foreign_key(
            ForeignKey::create()
                .name("fk_quant_feature_vector_market")
                .from(QuantFeatureVector::Table, QuantFeatureVector::MarketId)
                .to(Market::Table, Market::MarketId)
                .on_delete(ForeignKeyAction::Restrict),
        )
        .check(Expr::cust(
            "(decision_capture IS NULL) = (decision_capture_hash IS NULL)",
        ))
        .to_owned()
}

pub fn indexes() -> Vec<IndexSpec> {
    vec![
        IndexSpec::sea_query(
            "idx_quant_feature_vector_market_decision_at",
            quant_feature_vector_table_name,
            IndexBuildMode::Transactional,
            Index::create()
                .name("idx_quant_feature_vector_market_decision_at")
                .table(QuantFeatureVector::Table)
                .col(QuantFeatureVector::MarketId)
                .col((QuantFeatureVector::DecisionAt, IndexOrder::Desc))
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
            "idx_quant_feature_vector_schema_decision_at",
            quant_feature_vector_table_name,
            IndexBuildMode::Transactional,
            Index::create()
                .name("idx_quant_feature_vector_schema_decision_at")
                .table(QuantFeatureVector::Table)
                .col(QuantFeatureVector::FeatureSchemaVersion)
                .col((QuantFeatureVector::DecisionAt, IndexOrder::Desc))
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

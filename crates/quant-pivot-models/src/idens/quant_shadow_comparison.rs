use quant_pivot_macros::quant_schema;
use sea_orm::{
    Iden,
    sea_query::{
        ColumnDef, ForeignKey, ForeignKeyAction, Index, IndexOrder, Table, TableCreateStatement,
    },
};

use crate::{
    idens::quant_model_version::QuantModelVersion,
    schema::{
        column,
        dependency::TableDependency,
        index::{IndexBuildMode, IndexSpec},
        seed::SeedSpec,
        timestamp_with_write_default,
    },
};

// Append-only, content-addressed shadow comparison at the signal/rank layer: one
// row per `(active, shadow)` model pair per scored cross-section. Scalar metrics
// (TopN overlap, hard-divergence flag) live in typed columns so the publish
// stability summary can aggregate them directly; the rank / score / matured
// outcome sub-structures are JSONB. Immutable governance evidence ⇒ `audit`
// lifecycle (WORM append-only trigger).
#[quant_schema(lifecycle = "audit")]
pub enum QuantShadowComparison {
    Table,
    ShadowComparisonId,
    ActiveModelVersionId,
    ShadowModelVersionId,
    DecisionAt,
    TopnOverlap,
    RankDeltaJson,
    ScoreDeltaJson,
    MaturedOutcomeJson,
    HardDivergence,
    ComparisonHash,
    CreatedAt,
}

pub fn table() -> TableCreateStatement {
    Table::create()
        .table(QuantShadowComparison::Table)
        .if_not_exists()
        .col(column::uuid_pk(QuantShadowComparison::ShadowComparisonId))
        .col(column::uuid_fk(QuantShadowComparison::ActiveModelVersionId))
        .col(column::uuid_fk(QuantShadowComparison::ShadowModelVersionId))
        .col(
            ColumnDef::new(QuantShadowComparison::DecisionAt)
                .timestamp_with_time_zone()
                .not_null(),
        )
        .col(column::probability(QuantShadowComparison::TopnOverlap))
        .col(
            ColumnDef::new(QuantShadowComparison::RankDeltaJson)
                .json_binary()
                .not_null(),
        )
        .col(
            ColumnDef::new(QuantShadowComparison::ScoreDeltaJson)
                .json_binary()
                .not_null(),
        )
        .col(
            ColumnDef::new(QuantShadowComparison::MaturedOutcomeJson)
                .json_binary()
                .null(),
        )
        .col(
            ColumnDef::new(QuantShadowComparison::HardDivergence)
                .boolean()
                .not_null(),
        )
        .col(
            ColumnDef::new(QuantShadowComparison::ComparisonHash)
                .text()
                .not_null(),
        )
        .col(timestamp_with_write_default(
            QuantShadowComparison::CreatedAt,
        ))
        .foreign_key(
            ForeignKey::create()
                .name("fk_quant_shadow_comparison_active_version")
                .from(
                    QuantShadowComparison::Table,
                    QuantShadowComparison::ActiveModelVersionId,
                )
                .to(QuantModelVersion::Table, QuantModelVersion::ModelVersionId)
                .on_delete(ForeignKeyAction::Restrict),
        )
        .foreign_key(
            ForeignKey::create()
                .name("fk_quant_shadow_comparison_shadow_version")
                .from(
                    QuantShadowComparison::Table,
                    QuantShadowComparison::ShadowModelVersionId,
                )
                .to(QuantModelVersion::Table, QuantModelVersion::ModelVersionId)
                .on_delete(ForeignKeyAction::Restrict),
        )
        .to_owned()
}

pub fn indexes() -> Vec<IndexSpec> {
    vec![
        IndexSpec::sea_query(
            "idx_quant_shadow_comparison_shadow_version_decision_at",
            quant_shadow_comparison_table_name,
            IndexBuildMode::Transactional,
            Index::create()
                .name("idx_quant_shadow_comparison_shadow_version_decision_at")
                .table(QuantShadowComparison::Table)
                .col(QuantShadowComparison::ShadowModelVersionId)
                .col((QuantShadowComparison::DecisionAt, IndexOrder::Desc))
                .to_owned(),
            "shadow comparisons by shadow version and recency (publish stability window)",
        ),
        IndexSpec::sea_query(
            "uq_quant_shadow_comparison_hash",
            quant_shadow_comparison_table_name,
            IndexBuildMode::Transactional,
            Index::create()
                .name("uq_quant_shadow_comparison_hash")
                .table(QuantShadowComparison::Table)
                .col(QuantShadowComparison::ComparisonHash)
                .unique()
                .to_owned(),
            "one row per content-addressed shadow comparison",
        ),
    ]
}

pub fn dependencies() -> Vec<TableDependency> {
    vec![TableDependency::foreign_key(quant_model_version_table_name)]
}

pub const fn seed_units() -> Vec<SeedSpec> {
    Vec::new()
}

fn quant_shadow_comparison_table_name() -> String {
    QuantShadowComparison::Table.to_string()
}

fn quant_model_version_table_name() -> String {
    QuantModelVersion::Table.to_string()
}

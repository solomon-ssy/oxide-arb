use quant_pivot_macros::quant_schema;
use sea_orm::{
    Iden,
    sea_query::{ForeignKey, ForeignKeyAction, Index, Table, TableCreateStatement},
};

use crate::{
    enums::quant::OutcomeSide,
    idens::{
        quant_order_intent::QuantOrderIntent, quant_position::QuantPosition,
        quant_settlement_redeem::QuantSettlementRedeem,
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
pub enum QuantSettlementRedeemLot {
    Table,
    SettlementRedeemLotId,
    SettlementRedeemId,
    PositionId,
    OrderIntentId,
    TokenId,
    Side,
    SharesRedeemed,
    CostBasisUsd,
    PayoutUsd,
    RealizedPnlUsd,
    CreatedAt,
}

pub fn table() -> TableCreateStatement {
    Table::create()
        .table(QuantSettlementRedeemLot::Table)
        .if_not_exists()
        .col(column::uuid_pk(
            QuantSettlementRedeemLot::SettlementRedeemLotId,
        ))
        .col(column::uuid_fk(
            QuantSettlementRedeemLot::SettlementRedeemId,
        ))
        .col(column::uuid_fk(QuantSettlementRedeemLot::PositionId))
        .col(column::uuid_fk(QuantSettlementRedeemLot::OrderIntentId))
        .col(column::token_id(QuantSettlementRedeemLot::TokenId))
        .col(column::pg_enum::<OutcomeSide>(
            QuantSettlementRedeemLot::Side,
        ))
        .col(column::shares(QuantSettlementRedeemLot::SharesRedeemed))
        .col(column::usd(QuantSettlementRedeemLot::CostBasisUsd))
        .col(column::usd(QuantSettlementRedeemLot::PayoutUsd))
        .col(column::usd(QuantSettlementRedeemLot::RealizedPnlUsd))
        .col(timestamp_with_write_default(
            QuantSettlementRedeemLot::CreatedAt,
        ))
        .foreign_key(
            ForeignKey::create()
                .name("fk_quant_settlement_redeem_lot_redeem")
                .from(
                    QuantSettlementRedeemLot::Table,
                    QuantSettlementRedeemLot::SettlementRedeemId,
                )
                .to(
                    QuantSettlementRedeem::Table,
                    QuantSettlementRedeem::SettlementRedeemId,
                )
                .on_delete(ForeignKeyAction::Restrict),
        )
        .foreign_key(
            ForeignKey::create()
                .name("fk_quant_settlement_redeem_lot_position")
                .from(
                    QuantSettlementRedeemLot::Table,
                    QuantSettlementRedeemLot::PositionId,
                )
                .to(QuantPosition::Table, QuantPosition::PositionId)
                .on_delete(ForeignKeyAction::Restrict),
        )
        .foreign_key(
            ForeignKey::create()
                .name("fk_quant_settlement_redeem_lot_intent")
                .from(
                    QuantSettlementRedeemLot::Table,
                    QuantSettlementRedeemLot::OrderIntentId,
                )
                .to(QuantOrderIntent::Table, QuantOrderIntent::OrderIntentId)
                .on_delete(ForeignKeyAction::Restrict),
        )
        .to_owned()
}

pub fn indexes() -> Vec<IndexSpec> {
    vec![
        IndexSpec::sea_query(
            "uq_quant_settlement_redeem_lot_position",
            quant_settlement_redeem_lot_table_name,
            IndexBuildMode::Transactional,
            Index::create()
                .name("uq_quant_settlement_redeem_lot_position")
                .table(QuantSettlementRedeemLot::Table)
                .col(QuantSettlementRedeemLot::PositionId)
                .unique()
                .to_owned(),
            "each strategy position lot can be redeemed once",
        ),
        IndexSpec::sea_query(
            "idx_quant_settlement_redeem_lot_redeem",
            quant_settlement_redeem_lot_table_name,
            IndexBuildMode::Transactional,
            Index::create()
                .name("idx_quant_settlement_redeem_lot_redeem")
                .table(QuantSettlementRedeemLot::Table)
                .col(QuantSettlementRedeemLot::SettlementRedeemId)
                .to_owned(),
            "redeem batch lot allocation lookup",
        ),
    ]
}

pub fn dependencies() -> Vec<TableDependency> {
    vec![
        TableDependency::foreign_key(quant_settlement_redeem_table_name),
        TableDependency::foreign_key(quant_position_table_name),
        TableDependency::foreign_key(quant_order_intent_table_name),
    ]
}

pub const fn seed_units() -> Vec<SeedSpec> {
    Vec::new()
}

fn quant_settlement_redeem_lot_table_name() -> String {
    QuantSettlementRedeemLot::Table.to_string()
}

fn quant_settlement_redeem_table_name() -> String {
    QuantSettlementRedeem::Table.to_string()
}

fn quant_position_table_name() -> String {
    QuantPosition::Table.to_string()
}

fn quant_order_intent_table_name() -> String {
    QuantOrderIntent::Table.to_string()
}

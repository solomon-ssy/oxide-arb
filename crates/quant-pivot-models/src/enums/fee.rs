//! Fee quote enums.

/// Fee liquidity role on the wire (not persisted to Postgres).
#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum FeeLiquidityRole {
    Taker,
    Maker,
}

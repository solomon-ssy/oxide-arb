//! Append-only wire labels for generic factor families.
//!
//! These strings are the only valid entries in
//! [`FactorsConfig::enabled_factor_families`](super::sections::config::FactorsConfig).
//! Vertical (`domain.*`) families are routed by market category, not this list.

/// Stable wire labels for generic factor families (append-only contract).
pub const GENERIC_FACTOR_FAMILY_WIRES: &[&str] = &[
    "liquidity",
    "microstructure",
    "momentum",
    "mean_reversion",
    "volatility",
    "activity",
    "resolution",
    "data_quality",
];

/// Whether `wire` is a known generic factor-family label.
#[must_use]
pub fn is_generic_factor_family_wire(wire: &str) -> bool {
    GENERIC_FACTOR_FAMILY_WIRES.contains(&wire)
}

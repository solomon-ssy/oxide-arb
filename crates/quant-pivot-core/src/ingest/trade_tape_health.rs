//! On-chain trade-tape ingest availability (shared by feature + monitor planes).

use std::{collections::HashMap, hash::BuildHasher};

use quant_pivot_api::exchange::{
    EXCHANGE_CONTRACTS, ExchangeContract,
    constants::{CTF_EXCHANGE_V1, CTF_EXCHANGE_V2, NEG_RISK_EXCHANGE_V1, NEG_RISK_EXCHANGE_V2},
};
use quant_pivot_models::{
    config::TradeTapeOnChainConfig,
    domain::data_plane::{TradeTapeBlockCursorInfo, TradeTapeBlockCursorStatus},
};
#[cfg(test)]
use quant_pivot_models::{domain::data_plane::TradeTapeSourceKind, types::EvmAddress};

const ON_CHAIN_CONTRACT_COUNT: usize = EXCHANGE_CONTRACTS.len();

/// Exchange contracts that may emit fills for a market, keyed by Gamma `neg_risk`.
#[must_use]
pub const fn exchange_route(neg_risk: bool) -> &'static [ExchangeContract] {
    if neg_risk {
        &[NEG_RISK_EXCHANGE_V1, NEG_RISK_EXCHANGE_V2]
    } else {
        &[CTF_EXCHANGE_V1, CTF_EXCHANGE_V2]
    }
}

/// Whether one durable block cursor has made forward progress and is not in error.
#[must_use]
pub fn cursor_is_healthy(cursor: &TradeTapeBlockCursorInfo) -> bool {
    cursor.status != TradeTapeBlockCursorStatus::Faulted && cursor.last_finalized_block > 0
}

/// Index cursors by lowercase `0x`-prefixed contract address.
#[must_use]
pub fn cursors_by_contract_address(
    cursors: &[TradeTapeBlockCursorInfo],
) -> HashMap<String, &TradeTapeBlockCursorInfo> {
    cursors
        .iter()
        .map(|cursor| (cursor.contract_address.to_string(), cursor))
        .collect()
}

/// Whether on-chain ingest is healthy for one market's exchange route.
///
/// A market routes to either the CTF or `NegRisk` exchange family (V1 + V2). At
/// least one contract in that family must have a non-error cursor with durable
/// progress before trade-tape features may score for the market.
#[must_use]
pub fn market_tape_available<S: BuildHasher>(
    config: &TradeTapeOnChainConfig,
    cursors_by_address: &HashMap<String, &TradeTapeBlockCursorInfo, S>,
    neg_risk: bool,
) -> bool {
    if !config.enabled {
        return false;
    }
    exchange_route(neg_risk).iter().any(|contract| {
        let address = format!("{:#x}", contract.address);
        cursors_by_address
            .get(&address)
            .is_some_and(|cursor| cursor_is_healthy(cursor))
    })
}

/// Whether the on-chain trade-tape ingest plane is operable for both exchange
/// families (CTF binary markets and `NegRisk` multi-outcome markets).
///
/// Requires all four contract cursors to exist and each family route to have at
/// least one healthy cursor. Disabled worker or missing cursor rows → false.
#[must_use]
pub fn trade_tape_ingest_available(
    config: &TradeTapeOnChainConfig,
    cursors: &[TradeTapeBlockCursorInfo],
) -> bool {
    if !config.enabled || cursors.len() < ON_CHAIN_CONTRACT_COUNT {
        return false;
    }
    let by_address = cursors_by_contract_address(cursors);
    market_tape_available(config, &by_address, false)
        && market_tape_available(config, &by_address, true)
}

/// Worst `head_lag_blocks` across the contracts in one market's exchange route.
#[must_use]
pub fn tape_route_lag_blocks<S: BuildHasher>(
    neg_risk: bool,
    cursors_by_address: &HashMap<String, &TradeTapeBlockCursorInfo, S>,
) -> Option<i64> {
    exchange_route(neg_risk)
        .iter()
        .filter_map(|contract| {
            let address = format!("{:#x}", contract.address);
            cursors_by_address
                .get(&address)
                .map(|cursor| cursor.head_lag_blocks)
        })
        .max()
}

#[cfg(test)]
mod tests {
    use chrono::Utc;

    use super::*;

    fn cursor(
        contract: ExchangeContract,
        status: TradeTapeBlockCursorStatus,
        block: i64,
    ) -> TradeTapeBlockCursorInfo {
        TradeTapeBlockCursorInfo {
            source: TradeTapeSourceKind::OnChain,
            contract_address: EvmAddress::parse(format!("{:#x}", contract.address))
                .expect("exchange fixture address is canonical"),
            last_finalized_block: block,
            last_log_index: 0,
            head_lag_blocks: 0,
            status,
            created_at: Utc::now(),
            updated_at: Utc::now(),
        }
    }

    fn enabled_config() -> TradeTapeOnChainConfig {
        TradeTapeOnChainConfig {
            enabled: true,
            ..TradeTapeOnChainConfig::default()
        }
    }

    #[test]
    fn binary_market_ctf_cursor() {
        let cursors = vec![
            cursor(CTF_EXCHANGE_V2, TradeTapeBlockCursorStatus::Live, 1),
            cursor(NEG_RISK_EXCHANGE_V1, TradeTapeBlockCursorStatus::Faulted, 0),
            cursor(NEG_RISK_EXCHANGE_V2, TradeTapeBlockCursorStatus::Faulted, 0),
        ];
        let by_address = cursors_by_contract_address(&cursors);
        assert!(market_tape_available(&enabled_config(), &by_address, false));
        assert!(!market_tape_available(&enabled_config(), &by_address, true));
    }

    #[test]
    fn plane_requires_both_healthy() {
        let cursors = vec![
            cursor(CTF_EXCHANGE_V1, TradeTapeBlockCursorStatus::Live, 1),
            cursor(CTF_EXCHANGE_V2, TradeTapeBlockCursorStatus::Live, 1),
            cursor(NEG_RISK_EXCHANGE_V1, TradeTapeBlockCursorStatus::Live, 1),
            cursor(NEG_RISK_EXCHANGE_V2, TradeTapeBlockCursorStatus::Live, 1),
        ];
        assert!(trade_tape_ingest_available(&enabled_config(), &cursors));

        let partial = vec![
            cursor(CTF_EXCHANGE_V1, TradeTapeBlockCursorStatus::Live, 1),
            cursor(CTF_EXCHANGE_V2, TradeTapeBlockCursorStatus::Live, 1),
            cursor(NEG_RISK_EXCHANGE_V1, TradeTapeBlockCursorStatus::Faulted, 0),
            cursor(NEG_RISK_EXCHANGE_V2, TradeTapeBlockCursorStatus::Faulted, 0),
        ];
        assert!(!trade_tape_ingest_available(&enabled_config(), &partial));
    }
}

//! Construct a [`VotingOracle`] from application settings.

use super::VotingOracle;
use super::ctf_source::CtfOracleSource;
use super::gamma_source::GammaOracleSource;
use super::source::OracleSource;
use super::uma_source::UmaOracleSource;
use oxide_arb_models::config::{
    GammaConfig, OnchainConfig, PolymarketConfig, SettlementOracleConfig,
};
use oxide_arb_models::constants::CTF_ADDRESS;
use std::sync::Arc;
use std::time::Duration;

/// Build the production 3-source settlement oracle (Gamma + CTF + UMA).
pub fn build_voting_oracle(
    polymarket: &PolymarketConfig,
    gamma: &GammaConfig,
    oracle_cfg: &SettlementOracleConfig,
) -> Result<VotingOracle, oxide_arb_error::rpc::RpcError> {
    let sources: Vec<Arc<dyn OracleSource>> = vec![
        Arc::new(GammaOracleSource::new(gamma.base_url.clone())),
        Arc::new(CtfOracleSource::new(
            polymarket.onchain.rpc_url.clone(),
            CTF_ADDRESS,
        )?),
        Arc::new(UmaOracleSource::new(oracle_cfg)?),
    ];

    let quorum = usize::from(oracle_cfg.voting_quorum.max(1));
    let cross_check_delay = Duration::from_secs(oracle_cfg.cross_check_delay_secs);
    let all_sources_down = oracle_cfg.all_sources_down_strategy.clone();

    Ok(VotingOracle::new(
        sources,
        quorum,
        cross_check_delay,
        all_sources_down,
    ))
}

/// Convenience when only on-chain + gamma URLs are available (tests).
pub fn build_voting_oracle_from_urls(
    gamma_base_url: String,
    onchain: &OnchainConfig,
    oracle_cfg: &SettlementOracleConfig,
) -> Result<VotingOracle, oxide_arb_error::rpc::RpcError> {
    let polymarket = PolymarketConfig {
        onchain: onchain.clone(),
        ..PolymarketConfig::default()
    };
    let gamma = GammaConfig {
        base_url: gamma_base_url,
        ..GammaConfig::default()
    };
    build_voting_oracle(&polymarket, &gamma, oracle_cfg)
}

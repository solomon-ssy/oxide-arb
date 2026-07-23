//! Credential-gated venue account reads with no money-moving capability.

use std::path::Path;

use anyhow::{Context, Result};
use quant_pivot_api::{
    clob::ClobClient,
    data_api::DataApiClient,
    keystore::Keystore,
    wallet::{WalletOwnershipClient, WalletTopology},
};
use quant_pivot_models::config::DeployConfig;

pub async fn run(config_dir: &Path) -> Result<()> {
    let config_dir = config_dir
        .to_str()
        .context("account-read config directory is not valid UTF-8")?;
    let deploy = DeployConfig::load(config_dir).context("load account-read deploy config")?;
    let keystore =
        Keystore::from_config(&deploy.keys).context("load configured signing identity")?;
    let funder = deploy
        .quant
        .account
        .funder
        .as_deref()
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .context("quant.account.funder is required for account-read smoke")?;
    let ownership = WalletOwnershipClient::connect(&deploy.polymarket)
        .context("build read-only wallet ownership client")?;
    let topology = WalletTopology::resolve_verified(
        deploy.quant.account.wallet_kind,
        keystore.address(),
        funder,
        deploy.polymarket.chain_id,
        &ownership,
    )
    .await
    .context("verify configured signer/funder topology")?;
    let clob = ClobClient::connect(keystore.signer_arc(), &deploy.polymarket, &topology)
        .await
        .context("derive CLOB L2 credentials")?;
    let data_api = DataApiClient::new(deploy.market_data.data_api.clone());

    let (_collateral, _open_orders, _trades, _positions) = tokio::try_join!(
        clob.collateral_balance(),
        clob.get_open_orders(),
        clob.get_trades(None, None, None, None),
        data_api.positions(funder),
    )
    .context("read CLOB collateral/orders/trades and Data API positions")?;
    println!("smoke account-read passed without order or settlement submission");
    Ok(())
}

//! Application composition and bootstrap wiring.

use std::sync::Arc;

use parking_lot::Mutex;
use quant_pivot_api::{
    clob::ClobClient,
    keystore::{Keystore, OrderSigner},
    wallet::{WalletOwnershipClient, WalletTopology},
};
use quant_pivot_compute::ComputeExecutor;
use quant_pivot_error::{QuantError, QuantResult};
use quant_pivot_models::{
    config::DeployConfig,
    domain::{quant::NewExecutionAccount, runtime::CoreEventPublisher},
    types::{EvmAddress, EvmCodeHash},
};
use quant_pivot_repository::traits::ExecutionAccountRepository;
use tokio_util::sync::CancellationToken;

use super::{
    AppContext,
    bundles::{
        AccountBundle, AccountBundleDeps, DataBundle, DataBundleDeps, ExecutionBundle,
        ExecutionBundleDeps, GovernanceBundle, GovernanceBundleDeps, InfraBundle, ReportBundle,
        ReportBundleDeps, ResearchBundle, ResearchBundleDeps, RuntimeSnapshot,
    },
};
use crate::{
    execution::{IntentLifecyclePublisher, settlement_discovery_wake::SettlementDiscoveryWake},
    observability::metrics_hub::MetricsHub,
};

impl AppContext {
    /// Build all subsystems from deploy config.
    pub async fn build(
        deploy: Arc<DeployConfig>,
        shutdown: CancellationToken,
        compute: Arc<ComputeExecutor>,
    ) -> QuantResult<Self> {
        let metrics = Arc::new(MetricsHub::new());
        let infra = InfraBundle::assemble(&deploy, Arc::clone(&metrics)).await?;
        let runtime = RuntimeSnapshot::bootstrap(&infra.repos, &deploy).await?;
        let (events, event_rx) = CoreEventPublisher::bounded(4096);
        let intent_lifecycle = Arc::new(IntentLifecyclePublisher::new(events.clone()));
        let settlement_discovery_wake = SettlementDiscoveryWake::new();
        let data = DataBundle::assemble(&DataBundleDeps {
            deploy: &deploy,
            shutdown: &shutdown,
            metrics: &metrics,
            infra: &infra,
            runtime: &runtime.config,
            events: &events,
            settlement_discovery_wake: &settlement_discovery_wake,
        })?;
        let governance = GovernanceBundle::assemble(GovernanceBundleDeps {
            deploy: &deploy,
            metrics: &metrics,
            infra: &infra,
            data: &data,
            runtime,
            events: events.clone(),
        })?;
        let research = ResearchBundle::assemble(&ResearchBundleDeps {
            deploy: &deploy,
            compute: &compute,
            infra: &infra,
            data: &data,
            governance: &governance,
        })?;
        // One authenticated CLOB client (single L1+L2 identity) shared by the
        // account (collateral reads) and execution (order writes) bundles. Fails
        // closed at boot if the private key is missing or auth fails — report_only
        // is not dry-run, so the real venue must be reachable.
        let keystore = Keystore::from_config(&deploy.keys)?;
        let signer = keystore.signer_arc();
        let wallet = resolve_wallet_topology(&deploy, &signer).await?;
        let execution_account = infra
            .repos
            .execution_account
            .ensure(new_execution_account(&deploy, wallet)?)
            .await?;
        let clob =
            Arc::new(ClobClient::connect(Arc::clone(&signer), &deploy.polymarket, &wallet).await?);
        let account = AccountBundle::assemble(AccountBundleDeps {
            deploy: &deploy,
            infra: &infra,
            market_registry: Arc::clone(&data.market_registry),
            clob: Arc::clone(&clob),
            execution_account,
        })?;
        let execution = ExecutionBundle::assemble(&ExecutionBundleDeps {
            deploy: &deploy,
            infra: &infra,
            data: &data,
            governance: &governance,
            research: &research,
            account: &account,
            intent_lifecycle: Arc::clone(&intent_lifecycle),
            clob,
            signer,
            wallet,
            settlement_discovery_wake,
        })?;
        governance.alerts.attach_event_publisher(events.clone());
        let report = ReportBundle::assemble(ReportBundleDeps {
            workers: &deploy.quant.workers,
            infra: &infra,
            data: &data,
            governance: &governance,
            research: &research,
            account: &account,
            events: events.clone(),
            max_recovery_attempts: deploy.quant.research_jobs.max_recovery_attempts,
        })?;
        // Late-bind the execution breaker so activation hot-swaps its venue /
        // daily-loss thresholds without a restart (the breaker is built with the
        // execution bundle, after governance).
        governance
            .applicator
            .attach_execution_breaker(Arc::clone(&execution.breaker));
        governance.bootstrap_execution_recovery().await?;
        governance.bootstrap_bias_table().await?;

        Ok(Self {
            config: deploy,
            wallet,
            compute,
            shutdown,
            events,
            intent_lifecycle,
            event_rx: Mutex::new(Some(event_rx)),
            infra,
            data,
            governance,
            research,
            account,
            report,
            execution,
        })
    }
}

fn new_execution_account(
    deploy: &DeployConfig,
    wallet: WalletTopology,
) -> QuantResult<NewExecutionAccount> {
    let chain_id = i64::try_from(deploy.polymarket.chain_id)
        .map_err(|_| QuantError::config("polymarket.chain_id exceeds PostgreSQL bigint"))?;
    let funder_address = EvmAddress::parse(format!("{:#x}", wallet.funder))
        .map_err(|error| QuantError::config(error.to_string()))?;
    let owner_address = EvmAddress::parse(format!("{:#x}", wallet.owner))
        .map_err(|error| QuantError::config(error.to_string()))?;
    let controller_address = EvmAddress::parse(format!("{:#x}", wallet.signer))
        .map_err(|error| QuantError::config(error.to_string()))?;
    let contract_identity = wallet
        .contract_identity()
        .map_err(|error| QuantError::config(error.to_string()))?;
    let wallet_factory_address = contract_identity
        .map(|identity| EvmAddress::parse(format!("{:#x}", identity.factory)))
        .transpose()
        .map_err(|error| QuantError::config(error.to_string()))?;
    let wallet_implementation_code_hash = contract_identity
        .map(|identity| EvmCodeHash::parse(identity.implementation_code_hash))
        .transpose()
        .map_err(|error| QuantError::config(error.to_string()))?;
    NewExecutionAccount::build(
        chain_id,
        funder_address,
        wallet.kind,
        owner_address,
        controller_address,
        wallet_factory_address,
        wallet_implementation_code_hash,
    )
    .map_err(|error| QuantError::config(error.to_string()))
}

/// Resolve and validate the venue wallet topology from deploy config.
///
/// The funder is mandatory in every mode (report sizing reads the real venue
/// account). For EOA it must equal the signer; for Proxy / Gnosis Safe it must
/// either match the CREATE2-derived wallet (fast, offline) or — when the pinned
/// SDK cannot reproduce that Polymarket wallet generation — be proven on-chain to
/// be controlled by the signer via [`WalletOwnershipClient`].
async fn resolve_wallet_topology(
    deploy: &DeployConfig,
    signer: &OrderSigner,
) -> QuantResult<WalletTopology> {
    let funder = deploy
        .quant
        .account
        .funder
        .as_deref()
        .map(str::trim)
        .filter(|funder| !funder.is_empty())
        .ok_or_else(|| QuantError::config("quant.account.funder is required to reach the venue"))?;
    let ownership = WalletOwnershipClient::connect(&deploy.polymarket)?;
    WalletTopology::resolve_verified(
        deploy.quant.account.wallet_kind,
        signer.address(),
        funder,
        deploy.polymarket.chain_id,
        &ownership,
    )
    .await
    .map_err(|error| QuantError::config(error.to_string()))
}

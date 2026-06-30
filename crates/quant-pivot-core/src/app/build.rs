//! Application composition and bootstrap wiring (Phase 0).

use super::{
    AppContext,
    bundles::{
        AccountBundle, AccountBundleDeps, DataBundle, DataBundleDeps, ExecutionBundle,
        ExecutionBundleDeps, GovernanceBundle, GovernanceBundleDeps, InfraBundle, ReportBundle,
        ReportBundleDeps, ResearchBundle, ResearchBundleDeps, RuntimeSnapshot,
    },
};
use crate::observability::metrics_hub::MetricsHub;
use parking_lot::Mutex;
use quant_pivot_api::{
    clob::ClobClient,
    keystore::{Keystore, OrderSigner},
    wallet::WalletTopology,
};
use quant_pivot_error::{QuantError, QuantResult};
use quant_pivot_models::{config::DeployConfig, domain::CoreEventPublisher};
use std::sync::Arc;
use tokio_util::sync::CancellationToken;

impl AppContext {
    /// Build all subsystems from deploy config.
    pub async fn build(
        deploy: Arc<DeployConfig>,
        shutdown: CancellationToken,
    ) -> QuantResult<Self> {
        let metrics = Arc::new(MetricsHub::new());
        let infra = InfraBundle::assemble(&deploy, Arc::clone(&metrics)).await?;
        let runtime = RuntimeSnapshot::bootstrap(&infra.repos).await?;
        let (events, event_rx) = CoreEventPublisher::bounded(4096);
        let data = DataBundle::assemble(&DataBundleDeps {
            deploy: &deploy,
            shutdown: &shutdown,
            metrics: &metrics,
            infra: &infra,
            runtime: &runtime.config,
        });
        let governance = GovernanceBundle::assemble(GovernanceBundleDeps {
            deploy: &deploy,
            metrics: &metrics,
            infra: &infra,
            data: &data,
            runtime,
            events: events.clone(),
        });
        let research = ResearchBundle::assemble(&ResearchBundleDeps {
            deploy: &deploy,
            infra: &infra,
            data: &data,
            governance: &governance,
        });
        // One authenticated CLOB client (single L1+L2 identity) shared by the
        // account (collateral reads) and execution (order writes) bundles. Fails
        // closed at boot if the private key is missing or auth fails — report_only
        // is not dry-run, so the real venue must be reachable.
        let keystore = Keystore::from_config(&deploy.keys)?;
        let signer = keystore.signer_arc();
        let wallet = resolve_wallet_topology(&deploy, &signer)?;
        let clob =
            Arc::new(ClobClient::connect(Arc::clone(&signer), &deploy.polymarket, &wallet).await?);
        let account = AccountBundle::assemble(AccountBundleDeps {
            deploy: &deploy,
            infra: &infra,
            market_registry: Arc::clone(&data.market_registry),
            clob: Arc::clone(&clob),
        })?;
        let execution = ExecutionBundle::assemble(&ExecutionBundleDeps {
            deploy: &deploy,
            infra: &infra,
            data: &data,
            governance: &governance,
            research: &research,
            account: &account,
            clob,
            signer,
            wallet,
        })?;
        governance.alerts.attach_event_publisher(events.clone());
        let report = ReportBundle::assemble(ReportBundleDeps {
            infra: &infra,
            data: &data,
            governance: &governance,
            research: &research,
            account: &account,
            events: events.clone(),
        })
        .await?;
        // Late-bind the report scheduler so runtime-config activation rebuilds
        // report jobs without a restart (the runner depends on the report
        // lifecycle, which is built after governance).
        governance
            .applicator
            .attach_report_scheduler(Arc::clone(&report.scheduler));
        governance.bootstrap_execution_recovery().await?;

        Ok(Self {
            config: deploy,
            shutdown,
            events,
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

/// Resolve and validate the venue wallet topology from deploy config.
///
/// The funder is mandatory in every mode (report sizing reads the real venue
/// account); for EOA it must equal the signer, and for Proxy / Gnosis Safe it
/// must equal the CREATE2-derived wallet controlled by the signer.
fn resolve_wallet_topology(
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
    WalletTopology::resolve(
        deploy.quant.account.wallet_kind,
        signer.address(),
        funder,
        deploy.polymarket.chain_id,
    )
    .map_err(|error| QuantError::config(error.to_string()))
}

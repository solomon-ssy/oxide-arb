//! Application composition root — wires subsystems in dependency order.
//!
//! Infrastructure (connections, pools, channels, shards) is wired from the
//! [`DeployConfig`]; every trading parameter is wired from the **runtime
//! config snapshot** seeded out of Postgres (`runtime_config_version`), so the
//! audited activation history — not the TOML — is the single source of truth
//! for money-relevant behaviour.
//!
//! ```mermaid
//! flowchart TD
//!     connect["BuildInfra::connect"] --> wire["wire_risk_and_trading / wire_applicator"]
//!     wire --> finalize["BuildInfra::finalize"]
//!     finalize --> assemble["AppContextAssembly::assemble"]
//!     assemble --> ctx["AppContext"]
//! ```

mod assembly;
mod clients;
mod control;
mod detection;
mod execution;
mod infra;
mod risk;
mod types;

use super::AppContext;
use crate::{
    bridge::execution_mode::ExecutionModeHandle, control::status::SystemStatusNudge,
    service::detection_readiness::DetectionReadiness,
};
use oxide_arb_error::OxideResult;
use oxide_arb_models::{config::DeployConfig, domain::CoreEventPublisher};
use std::sync::Arc;
use tokio_util::sync::CancellationToken;

use assembly::ApplicatorWiring;
use types::{
    AppContextAssembly, AppContextAssemblyParts, CORE_EVENT_CHANNEL_CAPACITY,
    TradingLifecycleWiring, WiringConfig,
};

impl AppContext {
    /// Build all subsystems from the deploy config (PG/CH/Redis + trading loop).
    ///
    /// Trading parameters come from the runtime-config snapshot seeded out of
    /// the `runtime_config_version` table during [`types::BuildInfra::connect`].
    /// Bootstrap-only infra fields are stripped by [`types::BuildInfra::finalize`]
    /// before [`AppContextAssembly::assemble`] projects the runtime bundles.
    pub async fn build(
        deploy: Arc<DeployConfig>,
        shutdown: CancellationToken,
    ) -> OxideResult<Self> {
        let (events, event_rx) = CoreEventPublisher::bounded(CORE_EVENT_CHANNEL_CAPACITY);

        let (infra, persistence_workers) =
            types::BuildInfra::connect(&deploy, shutdown.clone()).await?;
        let events = {
            let dropped = infra.metrics().register_ws_event_dropped();
            events.with_drop_hook(Arc::new(move |kind| {
                dropped.with_label_values(&[kind]).inc();
            }))
        };
        infra.alerts().attach_event_publisher(events.clone());
        let mode = infra.execution_mode();
        deploy.ensure_valid_for_mode(mode)?;
        let runtime_store = Arc::clone(infra.runtime_store());
        let runtime = runtime_store.current();
        let wiring = WiringConfig::new(&deploy, &runtime);
        wiring.ensure_valid_for_mode(mode)?;
        let execution_mode = ExecutionModeHandle::new(mode);

        let clients = types::BuildClients::connect(
            &deploy,
            &runtime,
            shutdown.clone(),
            Arc::clone(infra.metrics()),
        )
        .await?;
        let lifecycle = TradingLifecycleWiring::assembled(
            SystemStatusNudge::default(),
            Arc::new(DetectionReadiness::default()),
        );
        let (risk, trading) = infra
            .wire_risk_and_trading(
                wiring,
                &execution_mode,
                &clients,
                &events,
                shutdown.clone(),
                &lifecycle,
            )
            .await?;

        let (settlement_service, settlement_dedup) =
            infra.wire_settlement_bundle(&runtime, &clients, &risk, &trading, &events);

        let applicator = infra.wire_applicator(ApplicatorWiring::new(
            Arc::clone(&runtime_store),
            execution_mode.clone(),
            &clients,
            &risk,
            &trading,
            Arc::clone(&settlement_service),
            Arc::clone(&settlement_dedup),
        ));

        let (assembled_infra, persistence_handles, pending_tasks) =
            infra.finalize(persistence_workers);

        let trade_integrity = Arc::clone(trading.execution().trade_integrity());
        Ok(AppContextAssembly::assembled(AppContextAssemblyParts {
            config: deploy,
            runtime_store,
            applicator,
            execution_mode,
            events,
            event_rx,
            infra: assembled_infra,
            clients,
            risk,
            trading,
            trade_integrity,
            persistence: persistence_handles,
            settlement_service,
            settlement_dedup,
            shutdown,
            pending_tasks,
            lifecycle,
        })
        .assemble())
    }
}

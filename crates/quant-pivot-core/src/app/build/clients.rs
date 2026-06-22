//! External client connection phase (WS, Gamma, CLOB, CTF, oracle).

use super::types::{BuildClients, BuildClientsParts};
use crate::observability::metrics_hub::MetricsHub;
use oxide_arb_api::{
    build_voting_oracle,
    clob::ClobClient,
    ctf::client::CtfRedeemClient,
    fees::FeeCalculator,
    gamma::GammaClient,
    keystore::Keystore,
    ws::{ClobWsManager, WsEventDropHook},
};
use oxide_arb_error::OxideResult;
use oxide_arb_models::{config::DeployConfig, runtime_config::RuntimeConfig};
use std::sync::Arc;
use tokio_util::sync::CancellationToken;

impl BuildClients {
    pub(super) async fn connect(
        deploy: &DeployConfig,
        runtime: &RuntimeConfig,
        shutdown: CancellationToken,
        metrics: Arc<MetricsHub>,
    ) -> OxideResult<Self> {
        let on_events_dropped: WsEventDropHook = {
            let metrics = Arc::clone(&metrics);
            Arc::new(move |n| metrics.ws_events_dropped.inc_by(n))
        };
        let reject_hook = Self::book_level_reject_hook(Arc::clone(&metrics), "ws");
        let rest_reject_hook = Self::book_level_reject_hook(Arc::clone(&metrics), "rest");

        let ws_manager = Arc::new(ClobWsManager::new(
            &deploy.polymarket,
            &deploy.market_data.websocket,
            shutdown,
            Some(on_events_dropped),
            Some(reject_hook),
        ));
        let gamma_client = Arc::new(GammaClient::new(deploy.market_data.gamma.clone()));
        let fee_calculator = Arc::new(FeeCalculator::from_config(&deploy.polymarket.fees));
        let voting_oracle = Arc::new(build_voting_oracle(
            &deploy.polymarket,
            &deploy.market_data.gamma,
            &runtime.settlement.oracle,
        )?);

        let (clob_client, ctf_redeem, holder_address) = match Keystore::from_config(&deploy.keys) {
            Ok(ks) => {
                let holder_address = ks.address_string();
                let signer = ks.signer_arc();
                let ctf_redeem = match CtfRedeemClient::new(
                    Arc::clone(&signer),
                    deploy.polymarket.onchain.rpc_url.clone(),
                    runtime.settlement.redeem.clone(),
                    deploy.polymarket.chain_id,
                ) {
                    Ok(client) => Some(Arc::new(client)),
                    Err(error) => {
                        tracing::warn!(%error, "CTF redeem client unavailable");
                        None
                    }
                };
                let clob_client = match ClobClient::connect(signer, &deploy.polymarket).await {
                    Ok(client) => Some(Arc::new(
                        client.with_book_level_reject_hook(Some(rest_reject_hook)),
                    )),
                    Err(error) => {
                        tracing::warn!(%error, "ClobClient connect failed — Live/paper CLOB disabled");
                        None
                    }
                };
                (clob_client, ctf_redeem, holder_address)
            }
            Err(error) => {
                tracing::info!(%error, "Keystore unavailable — running without ClobClient");
                (None, None, "unavailable".to_owned())
            }
        };

        Ok(Self::assembled(BuildClientsParts {
            ws_manager,
            gamma_client,
            fee_calculator,
            voting_oracle,
            clob_client,
            ctf_redeem,
            holder_address,
        }))
    }

    fn book_level_reject_hook(
        metrics: Arc<MetricsHub>,
        source: &'static str,
    ) -> Arc<dyn Fn() + Send + Sync> {
        Arc::new(move || {
            metrics
                .book_level_rejected
                .with_label_values(&[source])
                .inc();
        })
    }
}

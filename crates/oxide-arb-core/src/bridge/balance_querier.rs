use std::sync::Arc;

use oxide_arb_api::clob::ClobClient;
use oxide_arb_error::{OxideError, OxideResult};
use oxide_arb_models::types::{MarketId, Usd};
use oxide_arb_repository::postgres::PgPositionRepository;
use oxide_arb_repository::traits::PositionRepository;
use oxide_arb_risk::traits::BalanceQuerier;

pub struct CoreBalanceQuerier {
    clob_client: Arc<ClobClient>,
    position_repo: Arc<PgPositionRepository>,
}

impl CoreBalanceQuerier {
    pub const fn new(
        clob_client: Arc<ClobClient>,
        position_repo: Arc<PgPositionRepository>,
    ) -> Self {
        Self {
            clob_client,
            position_repo,
        }
    }
}

#[async_trait::async_trait]
impl BalanceQuerier for CoreBalanceQuerier {
    async fn query_balance(&self) -> OxideResult<(Usd, Usd)> {
        let balance = self
            .clob_client
            .collateral_balance()
            .await
            .map_err(OxideError::from)?;
        Ok((balance, Usd::ZERO))
    }

    async fn query_positions(&self) -> OxideResult<Vec<(MarketId, Usd)>> {
        let positions = self
            .position_repo
            .find_open()
            .await
            .map_err(OxideError::from)?;

        Ok(positions
            .into_iter()
            .map(|p| (p.market_id, p.total_cost_usd))
            .collect())
    }
}

use crate::{execution::fsm::ExecutionFSM, trade_integrity::TradeIntegrityStore};
use oxide_arb_error::{OxideError, OxideResult, trading::TradingError};
use oxide_arb_risk::engine::RiskEngine;

/// Resume risk after operator ack; clears kill switch when trading is allowed again.
pub async fn resume_trading(
    risk: &RiskEngine,
    fsm: &ExecutionFSM,
    integrity: &TradeIntegrityStore,
    operator_ack: &str,
) -> OxideResult<()> {
    risk.acknowledge_and_resume(operator_ack).await?;
    integrity.refresh_async().await.map_err(OxideError::from)?;
    let blocking = integrity.load().blocking_count;
    if blocking > 0 {
        return Err(OxideError::Trading(
            TradingError::BlockingTradesUnresolved { count: blocking },
        ));
    }
    if risk.allows_trading() && fsm.emergency_class().allows_auto_recover() {
        fsm.clear_emergency();
    }
    Ok(())
}

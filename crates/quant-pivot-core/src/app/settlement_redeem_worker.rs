//! Settlement redemption worker wiring (Phase 05.10).
//!
//! Registers the periodic sweep that redeems resolved standard binary CTF lots
//! whose frozen exit policy opted into `redeem_policy=auto`. The cadence is read
//! from runtime-config (`execution.settlement_redeem.interval_secs`) on every
//! tick, so activation changes take effect without a restart.

use std::{sync::Arc, time::Duration};

use chrono::Utc;

use super::AppContext;
use crate::{
    app::{task_id::TaskId, task_registry::AppRunner},
    infra::periodic_task::PeriodicTask,
};

impl AppContext {
    /// Register the settlement redeem sweep (`TaskId::SettlementRedeemWorker`).
    pub fn register_settlement_redeem_worker(&self, runner: &mut AppRunner) {
        let service = Arc::clone(&self.execution.settlement_redeem);
        let config = self.runtime_config();
        runner.spawn(TaskId::SettlementRedeemWorker, move |token| async move {
            let _ = PeriodicTask::run(
                "settlement-redeem-worker",
                move || {
                    let secs = config
                        .current()
                        .execution
                        .settlement_redeem
                        .interval_secs
                        .max(1);
                    Duration::from_secs(secs)
                },
                0.0,
                true,
                token,
                move || {
                    let service = Arc::clone(&service);
                    async move {
                        service.run_pass(Utc::now()).await?;
                        Ok(())
                    }
                },
            )
            .await;
        });
    }
}

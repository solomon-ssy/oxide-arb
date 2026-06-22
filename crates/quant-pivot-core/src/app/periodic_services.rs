//! Periodic background services (Gamma catalog sync).

use super::AppContext;
use crate::{
    app::{task_id::TaskId, task_registry::AppRunner},
    infra::periodic_task::PeriodicTask,
};
use std::{sync::Arc, time::Duration};

impl AppContext {
    pub fn register_periodic_services(&self, runner: &mut AppRunner) {
        let gamma = Arc::clone(&self.data.gamma_service);
        let interval_secs = self.config.market_data.gamma.full_sync_interval_secs;
        runner.spawn(TaskId::GammaSync, move |token| async move {
            let _ = PeriodicTask::run(
                "gamma-sync",
                move || Duration::from_secs(interval_secs),
                0.0,
                false,
                token,
                move || {
                    let gamma = Arc::clone(&gamma);
                    async move { gamma.sync().await }
                },
            )
            .await;
        });
    }
}

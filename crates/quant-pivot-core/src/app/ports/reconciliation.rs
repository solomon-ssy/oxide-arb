//! Core [`ReconciliationPort`] — operator resolve over the reconciliation service.

use std::sync::Arc;

use async_trait::async_trait;
use chrono::Utc;
use quant_pivot_error::{
    QuantResult,
    execution::ExecutionError,
    storage::{StorageError, entity::QUANT_RECONCILIATION},
};
use quant_pivot_models::{
    domain::{
        api::{ResolveReconciliationCommand, ResolveReconciliationOutcome},
        ports::ReconciliationPort,
    },
    enums::execution::ReconciliationResult,
};
use quant_pivot_repository::traits::ReconciliationRepository;

use crate::{
    execution::reconciliation::{OperatorReconcileResolution, ReconciliationService},
    governance::execution_recovery::ExecutionRecoveryCoordinator,
};

/// Production reconciliation port.
pub struct CoreReconciliationPort {
    service: Arc<ReconciliationService>,
    reconciliation: Arc<dyn ReconciliationRepository>,
    recovery: Arc<ExecutionRecoveryCoordinator>,
}

impl CoreReconciliationPort {
    #[must_use]
    pub fn new(
        service: Arc<ReconciliationService>,
        reconciliation: Arc<dyn ReconciliationRepository>,
        recovery: Arc<ExecutionRecoveryCoordinator>,
    ) -> Self {
        Self {
            service,
            reconciliation,
            recovery,
        }
    }
}

#[async_trait]
impl ReconciliationPort for CoreReconciliationPort {
    async fn resolve_operator(
        &self,
        command: ResolveReconciliationCommand,
    ) -> QuantResult<ResolveReconciliationOutcome> {
        validate_operator_result(&command)?;

        let reconciliation = self
            .reconciliation
            .find_by_id(&command.reconciliation_id)
            .await?
            .ok_or_else(|| {
                StorageError::not_found(QUANT_RECONCILIATION, command.reconciliation_id)
            })?;

        if reconciliation.result != ReconciliationResult::Unresolvable
            || reconciliation.resolved_at.is_some()
        {
            return Err(ExecutionError::ReconciliationNotResolvable {
                reconciliation_id: command.reconciliation_id.to_string(),
                result: reconciliation.result.to_string(),
            }
            .into());
        }

        let execution_order = self
            .service
            .resolve(
                OperatorReconcileResolution {
                    execution_order_id: reconciliation.execution_order_id,
                    result: command.result,
                    filled_shares: command.filled_shares,
                    avg_price: command.avg_price,
                    operator: command.operator,
                    note: command.reason,
                },
                Utc::now(),
            )
            .await?;

        let _ = self.recovery.refresh().await;
        let recovery = self.recovery.handle().current();

        Ok(ResolveReconciliationOutcome {
            execution_order,
            recovery,
        })
    }
}

fn validate_operator_result(command: &ResolveReconciliationCommand) -> QuantResult<()> {
    match command.result {
        ReconciliationResult::Filled | ReconciliationResult::PartiallyFilled => {
            if command.filled_shares.is_none() || command.avg_price.is_none() {
                return Err(ExecutionError::ReconciliationResolveInvalid {
                    detail: "filled_shares and avg_price are required for filled outcomes"
                        .to_owned(),
                }
                .into());
            }
        }
        ReconciliationResult::NotFilled | ReconciliationResult::Cancelled => {}
        ReconciliationResult::Pending | ReconciliationResult::Unresolvable => {
            return Err(ExecutionError::ReconciliationResolveInvalid {
                detail: format!(
                    "operator resolve cannot target result {}",
                    command.result.as_str()
                ),
            }
            .into());
        }
    }
    Ok(())
}

#[cfg(test)]
mod validate_operator_result_tests {
    use quant_pivot_models::{
        domain::api::ResolveReconciliationCommand,
        enums::execution::ReconciliationResult,
        types::{Price, ReconciliationId, Shares},
    };
    use rust_decimal_macros::dec;

    use super::validate_operator_result;

    fn command(result: ReconciliationResult) -> ResolveReconciliationCommand {
        ResolveReconciliationCommand {
            reconciliation_id: ReconciliationId::from_v7(),
            result,
            filled_shares: Some(Shares::new(dec!(10))),
            avg_price: Some(Price::new(dec!(0.5))),
            operator: "op".to_owned(),
            reason: "note".to_owned(),
        }
    }

    #[test]
    fn resolve_rejects_non_terminal_result() {
        for result in [
            ReconciliationResult::Pending,
            ReconciliationResult::Unresolvable,
        ] {
            let err = validate_operator_result(&command(result)).unwrap_err();
            assert!(
                err.to_string().contains("operator resolve cannot target"),
                "unexpected error: {err}"
            );
        }
    }

    #[test]
    fn resolve_requires_fill_fields_for_filled() {
        let mut cmd = command(ReconciliationResult::Filled);
        cmd.filled_shares = None;
        let err = validate_operator_result(&cmd).unwrap_err();
        assert!(err.to_string().contains("filled_shares and avg_price"));
    }
}

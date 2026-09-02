//! Durable settlement-case discovery and pre-submission inventory invalidation.

use std::{future::Future, sync::Arc, time::Duration};

use chrono::{DateTime, Utc};
use quant_pivot_error::{
    QuantError, QuantResult, execution::ExecutionError, storage::StorageError,
};
use quant_pivot_models::{
    domain::quant::{
        settlement::{NewSettlementRedeem, SettlementRedeemInfo},
        settlement_inventory::{
            MarkSettlementInventoryAbsent, RefreshSettlementInventory, SettlementDiscoveryCandidate,
        },
    },
    enums::settlement::{
        SettlementCaseState, SettlementReadinessStatus, SettlementReconciliationState,
    },
    types::{
        SettlementRedeemId,
        settlement_payload::{SettlementPayoutVector, SettlementReadinessEvidence},
    },
};
use quant_pivot_repository::traits::quant::settlement_redeem::SettlementRedeemRepository;
use rand::RngExt;
use tokio::time::sleep;

use crate::execution::SettlementLifecyclePublisher;

const TRANSACTION_ATTEMPTS: usize = 4;
const TRANSACTION_RETRY_BASE_MS: u64 = 10;

/// Bounded durable-poll result used by worker metrics.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct SettlementDiscoverySummary {
    pub discovered: u64,
    pub refreshed: u64,
    pub marked_not_required: u64,
    pub unchanged: u64,
    pub max_discovery_lag_ms: u64,
}

/// PostgreSQL-truth discovery. Venue events are only allowed to wake this poll.
pub struct SettlementDiscoveryService {
    repository: Arc<dyn SettlementRedeemRepository>,
    lifecycle: Arc<SettlementLifecyclePublisher>,
}

impl SettlementDiscoveryService {
    #[must_use]
    pub fn new(
        repository: Arc<dyn SettlementRedeemRepository>,
        lifecycle: Arc<SettlementLifecyclePublisher>,
    ) -> Self {
        Self {
            repository,
            lifecycle,
        }
    }

    /// Revalidate existing cases first, then create missing account-scoped cases.
    pub async fn run_once(
        &self,
        observed_at: DateTime<Utc>,
        limit: u64,
    ) -> QuantResult<SettlementDiscoverySummary> {
        let mut summary = SettlementDiscoverySummary::default();
        let existing = self
            .repository
            .list_refreshable_inventory_cases(limit)
            .await?;
        for redeem in existing {
            self.refresh_existing(&redeem, observed_at, &mut summary)
                .await?;
        }

        let candidates = self.repository.find_discovery_candidates(limit).await?;
        for candidate in candidates {
            self.insert_candidate(candidate, observed_at, &mut summary)
                .await?;
        }
        Ok(summary)
    }

    async fn refresh_existing(
        &self,
        redeem: &SettlementRedeemInfo,
        observed_at: DateTime<Utc>,
        summary: &mut SettlementDiscoverySummary,
    ) -> QuantResult<()> {
        let candidate = self
            .repository
            .load_inventory_candidate(&redeem.market_id, &redeem.execution_account_id)
            .await?;
        let Some(candidate) = candidate else {
            if redeem.state == SettlementCaseState::NotRequired {
                summary.unchanged += 1;
                return Ok(());
            }
            let command = MarkSettlementInventoryAbsent {
                settlement_redeem_id: redeem.settlement_redeem_id,
                expected_inventory_digest: redeem.inventory_digest,
                observed_at,
            };
            let committed =
                match retry_transaction(|| self.repository.mark_inventory_absent(command.clone()))
                    .await
                {
                    Ok(committed) => committed,
                    Err(StorageError::StateConflict { .. }) => {
                        summary.unchanged += 1;
                        return Ok(());
                    }
                    Err(error) => return Err(error.into()),
                };
            self.lifecycle.committed(&committed);
            summary.marked_not_required += 1;
            return Ok(());
        };
        let frozen = candidate.clone().freeze()?;
        let unchanged = redeem.state != SettlementCaseState::NotRequired
            && redeem.route == candidate.route
            && redeem.yes_token_id == candidate.yes_token_id
            && redeem.no_token_id == candidate.no_token_id
            && redeem.resolution_content_hash == candidate.resolution_content_hash
            && redeem.resolution_outcome == candidate.resolution_outcome
            && redeem.resolved_at == candidate.resolved_at
            && redeem.effective_policy == frozen.effective_policy
            && redeem.inventory_digest == frozen.inventory_digest
            && redeem.contributor_lots_digest == frozen.contributor_lots_digest;
        if unchanged {
            summary.unchanged += 1;
            return Ok(());
        }
        let inventory_digest = frozen.inventory_digest;
        let contributor_lots_digest = frozen.contributor_lots_digest;
        let effective_policy = frozen.effective_policy;
        let lots = frozen.into_rows(redeem.settlement_redeem_id);
        let command = RefreshSettlementInventory {
            settlement_redeem_id: redeem.settlement_redeem_id,
            expected_inventory_digest: redeem.inventory_digest,
            yes_token_id: candidate.yes_token_id,
            no_token_id: candidate.no_token_id,
            resolution_content_hash: candidate.resolution_content_hash,
            resolution_outcome: candidate.resolution_outcome,
            resolved_at: candidate.resolved_at,
            effective_policy,
            inventory_digest,
            contributor_lots_digest,
            lots,
            observed_at,
        };
        let committed = match retry_transaction(|| {
            self.repository
                .refresh_discovered_inventory(command.clone())
        })
        .await
        {
            Ok(committed) => committed,
            Err(StorageError::StateConflict { .. }) => {
                summary.unchanged += 1;
                return Ok(());
            }
            Err(error) => return Err(error.into()),
        };
        self.lifecycle.committed(&committed);
        summary.refreshed += 1;
        Ok(())
    }

    async fn insert_candidate(
        &self,
        candidate: SettlementDiscoveryCandidate,
        observed_at: DateTime<Utc>,
        summary: &mut SettlementDiscoverySummary,
    ) -> QuantResult<()> {
        summary.max_discovery_lag_ms = summary.max_discovery_lag_ms.max(
            u64::try_from(
                observed_at
                    .signed_duration_since(candidate.resolved_at)
                    .num_milliseconds()
                    .max(0),
            )
            .unwrap_or(u64::MAX),
        );
        let frozen = candidate.clone().freeze()?;
        let settlement_redeem_id = SettlementRedeemId::from_v7();
        let new_case = NewSettlementRedeem {
            settlement_redeem_id,
            market_id: candidate.market_id.clone(),
            yes_token_id: candidate.yes_token_id.clone(),
            no_token_id: candidate.no_token_id.clone(),
            execution_account_id: candidate.execution_account_id,
            resolution_content_hash: candidate.resolution_content_hash,
            resolution_outcome: candidate.resolution_outcome,
            resolved_at: candidate.resolved_at,
            route: candidate.route,
            effective_policy: frozen.effective_policy,
            inventory_digest: frozen.inventory_digest,
            contributor_lots_digest: frozen.contributor_lots_digest,
            state: SettlementCaseState::Discovered,
            readiness_status: SettlementReadinessStatus::Unchecked,
            readiness_evidence_json: SettlementReadinessEvidence::default(),
            target_adapter: None,
            target_code_hash: None,
            deployment_digest: None,
            deployment_evidence_version: None,
            verified_block_number: None,
            verified_block_hash: None,
            current_authorization_id: None,
            reconciliation_state: SettlementReconciliationState::NotRequired,
            payout_vector_json: SettlementPayoutVector::unresolved(),
            balance_before_json: None,
            balance_after_json: None,
            expected_payout_usd: None,
            actual_payout_usd: None,
            gas_fee_pol: None,
            failure_code: None,
            attempt_count: 0,
            retry_count: 0,
            next_attempt_at: None,
            claim_owner: None,
            lease_expires_at: None,
            last_error: None,
            prepared_at: None,
            submitted_at: None,
            confirmed_at: None,
            failed_at: None,
            created_at: observed_at,
            updated_at: observed_at,
        };
        let rows = frozen.into_rows(settlement_redeem_id);
        match retry_transaction(|| {
            self.repository
                .insert_discovered_case(new_case.clone(), rows.clone())
        })
        .await
        {
            Ok(committed) => {
                self.lifecycle.committed(&committed);
                summary.discovered += 1;
            }
            Err(StorageError::StateConflict { .. }) => {
                summary.unchanged += 1;
            }
            Err(StorageError::Duplicate { .. }) => {
                self.repository
                    .find_by_market_account(&candidate.market_id, &candidate.execution_account_id)
                    .await?
                    .ok_or_else(|| {
                        discovery_invariant("duplicate case disappeared after insert")
                    })?;
                summary.unchanged += 1;
            }
            Err(source) => return Err(QuantError::from(source)),
        }
        Ok(())
    }
}

async fn retry_transaction<T, Operation, OperationFuture>(
    mut operation: Operation,
) -> Result<T, StorageError>
where
    Operation: FnMut() -> OperationFuture,
    OperationFuture: Future<Output = Result<T, StorageError>>,
{
    let mut attempt = 0_usize;
    loop {
        match operation().await {
            Err(error)
                if error.is_retryable_transaction() && attempt + 1 < TRANSACTION_ATTEMPTS =>
            {
                let delay = transaction_retry_delay(attempt);
                tracing::debug!(
                    attempt = attempt + 1,
                    delay_ms = delay.as_millis(),
                    %error,
                    "retrying idempotent settlement discovery transaction"
                );
                sleep(delay).await;
                attempt += 1;
            }
            result => return result,
        }
    }
}

fn transaction_retry_delay(attempt: usize) -> Duration {
    let multiplier = 1_u64 << attempt.min(3);
    let base_ms = TRANSACTION_RETRY_BASE_MS.saturating_mul(multiplier);
    let jitter_ms = rand::rng().random_range(0..=base_ms / 2);
    Duration::from_millis(base_ms.saturating_add(jitter_ms))
}

fn discovery_invariant(reason: &'static str) -> QuantError {
    ExecutionError::SettlementRedeemInvariant {
        reason: reason.to_owned(),
    }
    .into()
}

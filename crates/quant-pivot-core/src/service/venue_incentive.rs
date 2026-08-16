//! Venue maker-award and wallet-credit reconciliation.

use std::sync::Arc;

use chrono::{DateTime, Days, NaiveDate, NaiveTime, Utc};
use quant_pivot_api::{
    clob::{ClobClient, MakerRebateAward},
    data_api::{DataApiClient, VenueIncentiveCredit, VenueIncentiveCreditKind},
};
use quant_pivot_error::{QuantResult, infra::InfraError};
use quant_pivot_models::{
    domain::quant::NewVenueIncentiveEvent,
    enums::fee::{VenueIncentiveKind, VenueIncentiveStage},
    hashing::CanonicalDigest,
    types::{EvmAddress, ExecutionAccountId, VenueIncentiveEventId},
};
use quant_pivot_repository::traits::VenueIncentiveRepository;

/// Reconciles venue incentive stages without feeding wallet credits back into
/// recommendation or execution economics.
pub struct VenueIncentiveReconciliationService {
    clob: Arc<ClobClient>,
    data_api: Arc<DataApiClient>,
    repository: Arc<dyn VenueIncentiveRepository>,
    execution_account_id: ExecutionAccountId,
    funder: EvmAddress,
}

impl VenueIncentiveReconciliationService {
    #[must_use]
    pub const fn new(
        clob: Arc<ClobClient>,
        data_api: Arc<DataApiClient>,
        repository: Arc<dyn VenueIncentiveRepository>,
        execution_account_id: ExecutionAccountId,
        funder: EvmAddress,
    ) -> Self {
        Self {
            clob,
            data_api,
            repository,
            execution_account_id,
            funder,
        }
    }

    /// Reconcile closed UTC days in the configured rolling window. Each day is
    /// committed independently so one later upstream failure cannot discard
    /// already verified earlier days.
    pub async fn reconcile_pass(&self, now: DateTime<Utc>, lookback_days: u32) -> QuantResult<()> {
        if lookback_days == 0 {
            return Err(InfraError::Misconfigured {
                detail: "venue incentive lookback_days must be positive".to_owned(),
            }
            .into());
        }
        let today = now.date_naive();
        for days_ago in 1..=lookback_days {
            let date = today
                .checked_sub_days(Days::new(u64::from(days_ago)))
                .ok_or_else(|| InfraError::Misconfigured {
                    detail: "venue incentive lookback is outside calendar range".to_owned(),
                })?;
            self.reconcile_day(date, now).await?;
        }
        let reconciliation = self
            .repository
            .reconciliation_cumulative(&self.execution_account_id, now)
            .await?;
        tracing::info!(
            execution_account_id = %self.execution_account_id,
            estimated_maker_accrual_usd = %reconciliation.estimated_maker_accrual_usd,
            venue_awarded_maker_usd = %reconciliation.venue_awarded_maker_usd,
            wallet_credited_maker_usd = %reconciliation.wallet_credited_maker_usd,
            wallet_credited_taker_usd = %reconciliation.wallet_credited_taker_usd,
            estimate_to_award_delta_usd = %reconciliation.estimate_to_award_delta(),
            award_to_credit_delta_usd = %reconciliation.award_to_credit_delta(),
            "venue incentive reconciliation updated"
        );
        Ok(())
    }

    async fn reconcile_day(&self, date: NaiveDate, available_at: DateTime<Utc>) -> QuantResult<()> {
        let awards = self.clob.maker_rebate_awards(date, &self.funder).await?;
        self.repository
            .record(
                awards
                    .iter()
                    .map(|award| self.award_event(award, available_at))
                    .collect::<QuantResult<Vec<_>>>()?,
            )
            .await?;

        let start = DateTime::<Utc>::from_naive_utc_and_offset(date.and_time(NaiveTime::MIN), Utc);
        let next_date =
            date.checked_add_days(Days::new(1))
                .ok_or_else(|| InfraError::Misconfigured {
                    detail: "venue incentive activity end is outside calendar range".to_owned(),
                })?;
        let next =
            DateTime::<Utc>::from_naive_utc_and_offset(next_date.and_time(NaiveTime::MIN), Utc);
        let credits = self
            .data_api
            .incentive_credits(&self.funder, start.timestamp(), next.timestamp() - 1)
            .await?;
        self.repository
            .record(
                credits
                    .iter()
                    .map(|credit| self.credit_event(credit, available_at))
                    .collect::<QuantResult<Vec<_>>>()?,
            )
            .await?;
        Ok(())
    }

    fn award_event(
        &self,
        award: &MakerRebateAward,
        available_at: DateTime<Utc>,
    ) -> QuantResult<NewVenueIncentiveEvent> {
        let evidence_hash =
            CanonicalDigest::content_hash_typed("quant-pivot/venue-maker-rebate-award", 1, award)?;
        let source_partition = format!(
            "maker_award:{}:{}:{}:{}",
            award.maker_address, award.program_date, award.market_id, award.asset_address
        );
        Ok(NewVenueIncentiveEvent {
            venue_incentive_event_id: VenueIncentiveEventId::from_v7(),
            execution_account_id: self.execution_account_id,
            execution_fill_id: None,
            market_id: Some(award.market_id.clone()),
            kind: VenueIncentiveKind::MakerRebate,
            stage: VenueIncentiveStage::VenueAwarded,
            program_date: award.program_date,
            amount_usd: award.amount_usd,
            source_schedule_hash: None,
            source_identity: format!("{source_partition}:{evidence_hash}"),
            source_partition,
            transaction_hash: None,
            observed_at: available_at,
            available_at,
            evidence_hash,
        })
    }

    fn credit_event(
        &self,
        credit: &VenueIncentiveCredit,
        available_at: DateTime<Utc>,
    ) -> QuantResult<NewVenueIncentiveEvent> {
        let evidence_hash = CanonicalDigest::content_hash_typed(
            "quant-pivot/venue-wallet-incentive-credit",
            1,
            credit,
        )?;
        let (kind, identity_kind) = match credit.kind {
            VenueIncentiveCreditKind::MakerRebate => {
                (VenueIncentiveKind::MakerRebate, "maker_rebate")
            }
            VenueIncentiveCreditKind::TakerRebate => {
                (VenueIncentiveKind::TakerRebate, "taker_rebate")
            }
        };
        let market_identity = credit
            .market_id
            .as_ref()
            .map_or_else(|| "account".to_owned(), ToString::to_string);
        let source_partition = format!(
            "wallet_credit:{}:{identity_kind}:{}:{market_identity}:{}",
            self.execution_account_id,
            credit.transaction_hash,
            credit.occurred_at.timestamp()
        );
        Ok(NewVenueIncentiveEvent {
            venue_incentive_event_id: VenueIncentiveEventId::from_v7(),
            execution_account_id: self.execution_account_id,
            execution_fill_id: None,
            market_id: credit.market_id.clone(),
            kind,
            stage: VenueIncentiveStage::WalletCredited,
            program_date: credit.occurred_at.date_naive(),
            amount_usd: credit.amount_usd,
            source_schedule_hash: None,
            source_identity: source_partition.clone(),
            source_partition,
            transaction_hash: Some(credit.transaction_hash.clone()),
            observed_at: credit.occurred_at,
            available_at,
            evidence_hash,
        })
    }
}

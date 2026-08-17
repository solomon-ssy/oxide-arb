//! Venue maker-award and wallet-credit reconciliation.

use std::sync::Arc;

use chrono::{DateTime, Days, Duration, NaiveDate, NaiveTime, Utc};
use futures_util::{StreamExt, stream};
use quant_pivot_api::{
    clob::{ClobClient, MakerRebateAward},
    data_api::{DataApiClient, VenueIncentiveCredit, VenueIncentiveCreditKind},
};
use quant_pivot_error::{QuantResult, api::ApiError, infra::InfraError};
use quant_pivot_models::{
    domain::quant::venue_incentive::{
        NewVenueIncentiveAwardSnapshot, NewVenueIncentiveEvent,
        NewVenueIncentiveReconciliationScan, VenueIncentiveScanHealth,
    },
    enums::{
        common::{AlertCategory, AlertLevel, AlertSource},
        fee::{VenueIncentiveKind, VenueIncentiveReconciliationScanStatus, VenueIncentiveStage},
    },
    hashing::CanonicalDigest,
    types::{
        ContentHash, EvmAddress, ExecutionAccountId, Usd, VenueIncentiveEventId,
        ids::VenueIncentiveReconciliationScanId,
    },
};
use quant_pivot_repository::traits::VenueIncentiveRepository;

use crate::observability::{
    alert_dispatcher::{Alert, AlertDispatcher},
    metrics_hub::MetricsHub,
};

/// Runtime dependencies and the cadence required for durable health alerts.
pub struct VenueIncentiveReconciliationDependencies {
    pub award_source: Arc<dyn VenueAwardSource>,
    pub credit_source: Arc<dyn VenueCreditSource>,
    pub repository: Arc<dyn VenueIncentiveRepository>,
    pub metrics: Arc<MetricsHub>,
    pub alerts: Arc<AlertDispatcher>,
    pub execution_account_id: ExecutionAccountId,
    pub funder: EvmAddress,
    pub cadence_secs: u64,
}

/// Reconciles venue incentive stages without feeding wallet credits back into
/// recommendation or execution economics.
pub struct VenueIncentiveReconciliationService {
    award_source: Arc<dyn VenueAwardSource>,
    credit_source: Arc<dyn VenueCreditSource>,
    repository: Arc<dyn VenueIncentiveRepository>,
    metrics: Arc<MetricsHub>,
    alerts: Arc<AlertDispatcher>,
    execution_account_id: ExecutionAccountId,
    funder: EvmAddress,
    cadence_secs: u64,
}

const RECONCILIATION_CONCURRENCY: usize = 4;

#[derive(Clone, Copy)]
struct SuccessfulScanInput {
    kind: VenueIncentiveKind,
    stage: VenueIncentiveStage,
    program_date: NaiveDate,
    started_at: DateTime<Utc>,
    completed_at: DateTime<Utc>,
    response_digest: ContentHash,
    response_count: usize,
}

/// Adapter boundary for the venue's complete maker-award day snapshot.
#[async_trait::async_trait]
pub trait VenueAwardSource: Send + Sync {
    async fn maker_awards(&self, date: NaiveDate) -> Result<Vec<MakerRebateAward>, ApiError>;
}

#[async_trait::async_trait]
impl VenueAwardSource for ClobClient {
    async fn maker_awards(&self, date: NaiveDate) -> Result<Vec<MakerRebateAward>, ApiError> {
        self.maker_rebate_awards(date).await
    }
}

/// Adapter boundary for wallet-confirmed incentive activity.
#[async_trait::async_trait]
pub trait VenueCreditSource: Send + Sync {
    async fn wallet_credits(
        &self,
        funder: &EvmAddress,
        start: i64,
        end: i64,
    ) -> Result<Vec<VenueIncentiveCredit>, ApiError>;
}

#[async_trait::async_trait]
impl VenueCreditSource for DataApiClient {
    async fn wallet_credits(
        &self,
        funder: &EvmAddress,
        start: i64,
        end: i64,
    ) -> Result<Vec<VenueIncentiveCredit>, ApiError> {
        self.incentive_credits(funder, start, end).await
    }
}

impl VenueIncentiveReconciliationService {
    #[must_use]
    pub fn new(dependencies: VenueIncentiveReconciliationDependencies) -> Self {
        Self {
            award_source: dependencies.award_source,
            credit_source: dependencies.credit_source,
            repository: dependencies.repository,
            metrics: dependencies.metrics,
            alerts: dependencies.alerts,
            execution_account_id: dependencies.execution_account_id,
            funder: dependencies.funder,
            cadence_secs: dependencies.cadence_secs,
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
        let mut dates = Vec::with_capacity(lookback_days as usize);
        for days_ago in 1..=lookback_days {
            let date = today
                .checked_sub_days(Days::new(u64::from(days_ago)))
                .ok_or_else(|| InfraError::Misconfigured {
                    detail: "venue incentive lookback is outside calendar range".to_owned(),
                })?;
            dates.push(date);
        }
        let end = dates[0];
        let from = dates[dates.len() - 1];
        let outcomes = stream::iter(dates)
            .map(|date| async move { (date, self.reconcile_day(date).await) })
            .buffer_unordered(RECONCILIATION_CONCURRENCY)
            .collect::<Vec<_>>()
            .await;
        let mut failures = Vec::new();
        for (date, result) in outcomes {
            match result {
                Ok(()) => self
                    .metrics
                    .record_venue_incentive_reconciliation("succeeded"),
                Err(error) => {
                    self.metrics.record_venue_incentive_reconciliation("failed");
                    failures.push(format!("{date}: {error}"));
                }
            }
        }
        if let Err(error) = self.publish_health(now, from, end).await {
            failures.push(format!("health projection: {error}"));
        }
        if !failures.is_empty() {
            return Err(InfraError::VenueIncentiveReconciliation {
                failed_days: failures.len(),
                detail: failures.join("; "),
            }
            .into());
        }
        Ok(())
    }

    async fn publish_health(
        &self,
        now: DateTime<Utc>,
        from: NaiveDate,
        end: NaiveDate,
    ) -> QuantResult<()> {
        let scans = self
            .repository
            .scans(&self.execution_account_id, from, end)
            .await?;
        let health = VenueIncentiveScanHealth::project(&scans, from, end);
        let reconciliation = self
            .repository
            .reconciliation_cumulative(&self.execution_account_id, now)
            .await?;
        let outstanding_since = self
            .repository
            .maker_credit_outstanding_since(&self.execution_account_id, now)
            .await?;
        self.metrics.set_venue_incentive_health(
            health.last_success_at.map(|value| value.timestamp()),
            health.incomplete_day_count,
            reconciliation.estimate_to_award_delta(),
            reconciliation.award_to_credit_delta(),
        );
        self.publish_health_alerts(
            now,
            health,
            reconciliation.venue_awarded_maker_usd - reconciliation.wallet_credited_maker_usd,
            outstanding_since,
        );
        tracing::info!(
            execution_account_id = %self.execution_account_id,
            estimated_maker_accrual_usd = %reconciliation.estimated_maker_accrual_usd,
            venue_awarded_maker_usd = %reconciliation.venue_awarded_maker_usd,
            wallet_credited_maker_usd = %reconciliation.wallet_credited_maker_usd,
            wallet_credited_taker_usd = %reconciliation.wallet_credited_taker_usd,
            estimate_to_award_delta_usd = %reconciliation.estimate_to_award_delta(),
            award_to_credit_delta_usd = %reconciliation.award_to_credit_delta(),
            incomplete_day_count = health.incomplete_day_count,
            "venue incentive reconciliation updated"
        );
        Ok(())
    }

    fn publish_health_alerts(
        &self,
        now: DateTime<Utc>,
        health: VenueIncentiveScanHealth,
        award_outstanding_usd: Usd,
        outstanding_since: Option<DateTime<Utc>>,
    ) {
        let stale_seconds = self.cadence_secs.saturating_mul(2);
        let stale = health.last_success_at.is_none_or(|last| {
            now.signed_duration_since(last).num_seconds()
                >= i64::try_from(stale_seconds).unwrap_or(i64::MAX)
        });
        if stale {
            self.alerts.dispatch_background(
                Alert::new(
                    "venue-incentive-reconciliation:stale",
                    AlertLevel::Warning,
                    AlertCategory::SchedulerHealth,
                    AlertSource::Scheduler,
                    "Venue incentive reconciliation is stale",
                    "No successful upstream incentive scan completed within two reconciliation cadences.",
                    now,
                )
                .with_affects_trading(false)
                .with_visible_toast(false)
                .with_dedupe_secs(self.cadence_secs),
            );
        }
        let closed_day_overdue = health.oldest_incomplete_date.is_some_and(|date| {
            let closed_at =
                DateTime::<Utc>::from_naive_utc_and_offset(date.and_time(NaiveTime::MIN), Utc)
                    + Duration::days(2);
            closed_at <= now
        });
        if closed_day_overdue {
            self.alerts.dispatch_background(
                Alert::new(
                    "venue-incentive-reconciliation:incomplete-day",
                    AlertLevel::Warning,
                    AlertCategory::SchedulerHealth,
                    AlertSource::Scheduler,
                    "Venue incentive reconciliation has incomplete closed days",
                    format!(
                        "{} closed program day(s) are incomplete; oldest is {}.",
                        health.incomplete_day_count,
                        health
                            .oldest_incomplete_date
                            .map_or_else(|| "unknown".to_owned(), |date| date.to_string())
                    ),
                    now,
                )
                .with_affects_trading(false)
                .with_visible_toast(false)
                .with_dedupe_secs(self.cadence_secs),
            );
        }
        let credit_overdue = award_outstanding_usd >= Usd::ONE
            && outstanding_since.is_some_and(|observed_at| {
                now.signed_duration_since(observed_at) >= Duration::hours(48)
            });
        if credit_overdue {
            self.alerts.dispatch_background(
                Alert::new(
                    "venue-incentive-reconciliation:award-credit-overdue",
                    AlertLevel::Warning,
                    AlertCategory::OperatorNotice,
                    AlertSource::Settlement,
                    "Venue maker award has not reached the wallet",
                    format!(
                        "{award_outstanding_usd} of venue-awarded maker incentive remains uncredited for at least 48 hours."
                    ),
                    now,
                )
                .with_affects_trading(false)
                .with_visible_toast(false)
                .with_dedupe_secs(self.cadence_secs),
            );
        }
    }

    async fn reconcile_day(&self, date: NaiveDate) -> QuantResult<()> {
        let award_result = self.reconcile_awards(date).await;
        let start = DateTime::<Utc>::from_naive_utc_and_offset(date.and_time(NaiveTime::MIN), Utc);
        let next_date =
            date.checked_add_days(Days::new(1))
                .ok_or_else(|| InfraError::Misconfigured {
                    detail: "venue incentive activity end is outside calendar range".to_owned(),
                })?;
        let next =
            DateTime::<Utc>::from_naive_utc_and_offset(next_date.and_time(NaiveTime::MIN), Utc);
        let credit_result = self.reconcile_credits(date, start, next).await;
        match (award_result, credit_result) {
            (Ok(()), Ok(())) => Ok(()),
            (Err(award), Ok(())) => Err(award),
            (Ok(()), Err(credit)) => Err(credit),
            (Err(award), Err(credit)) => Err(InfraError::VenueIncentiveReconciliation {
                failed_days: 1,
                detail: format!("award: {award}; credit: {credit}"),
            }
            .into()),
        }
    }

    async fn reconcile_awards(&self, date: NaiveDate) -> QuantResult<()> {
        let started_at = Utc::now();
        let mut awards = match self.award_source.maker_awards(date).await {
            Ok(awards) => awards,
            Err(error) => {
                self.record_failed_scan(
                    VenueIncentiveKind::MakerRebate,
                    VenueIncentiveStage::VenueAwarded,
                    date,
                    started_at,
                    "maker_award_upstream_failed",
                )
                .await?;
                return Err(error.into());
            }
        };
        awards.sort_by(|left, right| {
            (&left.market_id, &left.asset_address).cmp(&(&right.market_id, &right.asset_address))
        });
        let completed_at = Utc::now();
        let response_digest = CanonicalDigest::content_hash_typed(
            "quant-pivot/venue-maker-rebate-award-response",
            1,
            &awards,
        )?;
        let events = awards
            .iter()
            .map(|award| self.award_event(award, completed_at))
            .collect::<QuantResult<Vec<_>>>()?;
        self.repository
            .apply_award_snapshot(NewVenueIncentiveAwardSnapshot {
                scan: self.successful_scan(SuccessfulScanInput {
                    kind: VenueIncentiveKind::MakerRebate,
                    stage: VenueIncentiveStage::VenueAwarded,
                    program_date: date,
                    started_at,
                    completed_at,
                    response_digest,
                    response_count: events.len(),
                })?,
                awards: events,
            })
            .await?;
        Ok(())
    }

    async fn reconcile_credits(
        &self,
        date: NaiveDate,
        start: DateTime<Utc>,
        next: DateTime<Utc>,
    ) -> QuantResult<()> {
        let started_at = Utc::now();
        let mut credits = match self
            .credit_source
            .wallet_credits(&self.funder, start.timestamp(), next.timestamp() - 1)
            .await
        {
            Ok(credits) => credits,
            Err(error) => {
                for kind in [
                    VenueIncentiveKind::MakerRebate,
                    VenueIncentiveKind::TakerRebate,
                ] {
                    self.record_failed_scan(
                        kind,
                        VenueIncentiveStage::WalletCredited,
                        date,
                        started_at,
                        "wallet_credit_upstream_failed",
                    )
                    .await?;
                }
                return Err(error.into());
            }
        };
        credits.sort_by(|left, right| {
            (&left.transaction_hash, left.occurred_at, left.kind).cmp(&(
                &right.transaction_hash,
                right.occurred_at,
                right.kind,
            ))
        });
        let completed_at = Utc::now();
        for kind in [
            VenueIncentiveKind::MakerRebate,
            VenueIncentiveKind::TakerRebate,
        ] {
            let kind_credits = credits
                .iter()
                .filter(|credit| Self::credit_kind(credit) == kind)
                .collect::<Vec<_>>();
            let response_digest = CanonicalDigest::content_hash_typed(
                "quant-pivot/venue-wallet-incentive-credit-response",
                1,
                &kind_credits,
            )?;
            let events = kind_credits
                .into_iter()
                .map(|credit| self.credit_event(credit, completed_at))
                .collect::<QuantResult<Vec<_>>>()?;
            self.repository
                .record_scan(
                    self.successful_scan(SuccessfulScanInput {
                        kind,
                        stage: VenueIncentiveStage::WalletCredited,
                        program_date: date,
                        started_at,
                        completed_at,
                        response_digest,
                        response_count: events.len(),
                    })?,
                    events,
                )
                .await?;
        }
        Ok(())
    }

    fn successful_scan(
        &self,
        input: SuccessfulScanInput,
    ) -> QuantResult<NewVenueIncentiveReconciliationScan> {
        let response_count =
            i32::try_from(input.response_count).map_err(|_| InfraError::Misconfigured {
                detail: "venue incentive response count exceeds i32".to_owned(),
            })?;
        Ok(NewVenueIncentiveReconciliationScan {
            venue_incentive_reconciliation_scan_id: VenueIncentiveReconciliationScanId::from_v7(),
            execution_account_id: self.execution_account_id,
            kind: input.kind,
            stage: input.stage,
            program_date: input.program_date,
            started_at: input.started_at,
            completed_at: input.completed_at,
            status: VenueIncentiveReconciliationScanStatus::Succeeded,
            response_digest: Some(input.response_digest),
            response_count,
            error_code: None,
        })
    }

    async fn record_failed_scan(
        &self,
        kind: VenueIncentiveKind,
        stage: VenueIncentiveStage,
        program_date: NaiveDate,
        started_at: DateTime<Utc>,
        error_code: &str,
    ) -> QuantResult<()> {
        self.repository
            .record_scan(
                NewVenueIncentiveReconciliationScan {
                    venue_incentive_reconciliation_scan_id:
                        VenueIncentiveReconciliationScanId::from_v7(),
                    execution_account_id: self.execution_account_id,
                    kind,
                    stage,
                    program_date,
                    started_at,
                    completed_at: Utc::now(),
                    status: VenueIncentiveReconciliationScanStatus::Failed,
                    response_digest: None,
                    response_count: 0,
                    error_code: Some(error_code.to_owned()),
                },
                Vec::new(),
            )
            .await?;
        Ok(())
    }

    const fn credit_kind(credit: &VenueIncentiveCredit) -> VenueIncentiveKind {
        match credit.kind {
            VenueIncentiveCreditKind::MakerRebate => VenueIncentiveKind::MakerRebate,
            VenueIncentiveCreditKind::TakerRebate => VenueIncentiveKind::TakerRebate,
        }
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

#[cfg(test)]
mod tests {
    use std::sync::{Arc, Mutex};

    use chrono::{DateTime, Duration, NaiveDate, TimeZone, Utc};
    use quant_pivot_api::{clob::MakerRebateAward, data_api::VenueIncentiveCredit};
    use quant_pivot_error::{api::ApiError, storage::StorageError};
    use quant_pivot_models::{
        domain::{
            api::quant_incentive::VenueIncentiveEventListQuery,
            pagination::Paginated,
            quant::venue_incentive::{
                NewVenueIncentiveAwardSnapshot, NewVenueIncentiveEvent,
                NewVenueIncentiveReconciliationScan, VenueIncentiveEventInfo,
                VenueIncentiveReconciliation, VenueIncentiveReconciliationScanInfo,
                VenueIncentiveScanHealth,
            },
        },
        types::{EvmAddress, ExecutionAccountId, Usd},
    };
    use quant_pivot_repository::traits::VenueIncentiveRepository;
    use rust_decimal_macros::dec;

    use super::{
        VenueAwardSource, VenueCreditSource, VenueIncentiveReconciliationDependencies,
        VenueIncentiveReconciliationService,
    };
    use crate::observability::{
        alert_dispatcher::{Alert, AlertDispatcher},
        metrics_hub::MetricsHub,
    };

    struct AwardSourceFixture {
        failed_date: NaiveDate,
        calls: Mutex<Vec<NaiveDate>>,
    }

    #[async_trait::async_trait]
    impl VenueAwardSource for AwardSourceFixture {
        async fn maker_awards(&self, date: NaiveDate) -> Result<Vec<MakerRebateAward>, ApiError> {
            self.calls.lock().expect("award calls lock").push(date);
            if date == self.failed_date {
                return Err(ApiError::Timeout {
                    operation: "maker award fixture".to_owned(),
                    elapsed_ms: 1,
                });
            }
            Ok(Vec::new())
        }
    }

    #[derive(Default)]
    struct CreditSourceFixture {
        calls: Mutex<Vec<NaiveDate>>,
    }

    #[async_trait::async_trait]
    impl VenueCreditSource for CreditSourceFixture {
        async fn wallet_credits(
            &self,
            _funder: &EvmAddress,
            start: i64,
            _end: i64,
        ) -> Result<Vec<VenueIncentiveCredit>, ApiError> {
            let date = DateTime::from_timestamp(start, 0)
                .expect("credit fixture timestamp")
                .date_naive();
            self.calls.lock().expect("credit calls lock").push(date);
            Ok(Vec::new())
        }
    }

    #[derive(Default)]
    struct RepositoryFixture {
        scans: Mutex<Vec<NewVenueIncentiveReconciliationScan>>,
    }

    impl RepositoryFixture {
        fn push_scan(&self, scan: NewVenueIncentiveReconciliationScan) {
            self.scans.lock().expect("scan fixture lock").push(scan);
        }

        fn scan_infos(&self) -> Vec<VenueIncentiveReconciliationScanInfo> {
            self.scans
                .lock()
                .expect("scan fixture lock")
                .iter()
                .cloned()
                .map(|scan| VenueIncentiveReconciliationScanInfo {
                    venue_incentive_reconciliation_scan_id: scan
                        .venue_incentive_reconciliation_scan_id,
                    execution_account_id: scan.execution_account_id,
                    kind: scan.kind,
                    stage: scan.stage,
                    program_date: scan.program_date,
                    started_at: scan.started_at,
                    completed_at: scan.completed_at,
                    status: scan.status,
                    response_digest: scan.response_digest,
                    response_count: scan.response_count,
                    error_code: scan.error_code,
                    created_at: scan.completed_at,
                })
                .collect()
        }
    }

    #[async_trait::async_trait]
    impl VenueIncentiveRepository for RepositoryFixture {
        async fn record(&self, _events: Vec<NewVenueIncentiveEvent>) -> Result<(), StorageError> {
            Ok(())
        }

        async fn record_scan(
            &self,
            scan: NewVenueIncentiveReconciliationScan,
            _events: Vec<NewVenueIncentiveEvent>,
        ) -> Result<(), StorageError> {
            self.push_scan(scan);
            Ok(())
        }

        async fn apply_award_snapshot(
            &self,
            snapshot: NewVenueIncentiveAwardSnapshot,
        ) -> Result<(), StorageError> {
            self.push_scan(snapshot.scan);
            Ok(())
        }

        async fn credited_cumulative(
            &self,
            _execution_account_id: &ExecutionAccountId,
            _as_of: DateTime<Utc>,
        ) -> Result<Usd, StorageError> {
            Ok(Usd::ZERO)
        }

        async fn reconciliation_cumulative(
            &self,
            _execution_account_id: &ExecutionAccountId,
            as_of: DateTime<Utc>,
        ) -> Result<VenueIncentiveReconciliation, StorageError> {
            Ok(VenueIncentiveReconciliation {
                as_of,
                estimated_maker_accrual_usd: Usd::ZERO,
                venue_awarded_maker_usd: Usd::ZERO,
                wallet_credited_maker_usd: Usd::ZERO,
                wallet_credited_taker_usd: Usd::ZERO,
            })
        }

        async fn maker_credit_outstanding_since(
            &self,
            _execution_account_id: &ExecutionAccountId,
            _as_of: DateTime<Utc>,
        ) -> Result<Option<DateTime<Utc>>, StorageError> {
            Ok(None)
        }

        async fn scans(
            &self,
            _execution_account_id: &ExecutionAccountId,
            from: NaiveDate,
            to: NaiveDate,
        ) -> Result<Vec<VenueIncentiveReconciliationScanInfo>, StorageError> {
            Ok(self
                .scan_infos()
                .into_iter()
                .filter(|scan| scan.program_date >= from && scan.program_date <= to)
                .collect())
        }

        async fn page_events(
            &self,
            _execution_account_id: &ExecutionAccountId,
            _query: VenueIncentiveEventListQuery,
        ) -> Result<Paginated<VenueIncentiveEventInfo>, StorageError> {
            Ok(Paginated::new(Vec::new(), 0, 1, 20))
        }
    }

    #[tokio::test]
    async fn single_failure_keeps_dates() {
        let now = Utc
            .with_ymd_and_hms(2026, 8, 17, 12, 0, 0)
            .single()
            .expect("fixture time");
        let failed_date = NaiveDate::from_ymd_opt(2026, 8, 15).expect("fixture date");
        let award_source = Arc::new(AwardSourceFixture {
            failed_date,
            calls: Mutex::new(Vec::new()),
        });
        let credit_source = Arc::new(CreditSourceFixture::default());
        let repository = Arc::new(RepositoryFixture::default());
        let service =
            VenueIncentiveReconciliationService::new(VenueIncentiveReconciliationDependencies {
                award_source: Arc::clone(&award_source) as Arc<dyn VenueAwardSource>,
                credit_source: Arc::clone(&credit_source) as Arc<dyn VenueCreditSource>,
                repository: Arc::clone(&repository) as Arc<dyn VenueIncentiveRepository>,
                metrics: Arc::new(MetricsHub::new()),
                alerts: Arc::new(AlertDispatcher::with_recordings(Arc::new(Mutex::new(
                    Vec::<Alert>::new(),
                )))),
                execution_account_id: ExecutionAccountId::from_v7(),
                funder: EvmAddress::parse(format!("0x{}", "1".repeat(40))).expect("fixture funder"),
                cadence_secs: 3_600,
            });

        let result = service.reconcile_pass(now, 3).await;
        assert!(result.is_err());

        let mut award_dates = award_source.calls.lock().expect("award calls lock").clone();
        award_dates.sort_unstable();
        assert_eq!(
            award_dates,
            [
                NaiveDate::from_ymd_opt(2026, 8, 14).expect("fixture date"),
                failed_date,
                NaiveDate::from_ymd_opt(2026, 8, 16).expect("fixture date"),
            ]
        );
        let mut credit_dates = credit_source
            .calls
            .lock()
            .expect("credit calls lock")
            .clone();
        credit_dates.sort_unstable();
        assert_eq!(credit_dates, award_dates);
        assert_eq!(repository.scan_infos().len(), 9);
    }

    #[tokio::test]
    async fn health_alert_thresholds_fire() {
        let now = Utc
            .with_ymd_and_hms(2026, 8, 17, 12, 0, 0)
            .single()
            .expect("fixture time");
        let recordings = Arc::new(Mutex::new(Vec::<Alert>::new()));
        let service =
            VenueIncentiveReconciliationService::new(VenueIncentiveReconciliationDependencies {
                award_source: Arc::new(AwardSourceFixture {
                    failed_date: now.date_naive(),
                    calls: Mutex::new(Vec::new()),
                }),
                credit_source: Arc::new(CreditSourceFixture::default()),
                repository: Arc::new(RepositoryFixture::default()),
                metrics: Arc::new(MetricsHub::new()),
                alerts: Arc::new(AlertDispatcher::with_recordings(Arc::clone(&recordings))),
                execution_account_id: ExecutionAccountId::from_v7(),
                funder: EvmAddress::parse(format!("0x{}", "1".repeat(40))).expect("fixture funder"),
                cadence_secs: 3_600,
            });

        service.publish_health_alerts(
            now,
            VenueIncentiveScanHealth {
                last_success_at: Some(now - Duration::hours(2)),
                latest_award_success_at: Some(now - Duration::hours(2)),
                oldest_incomplete_date: Some(
                    NaiveDate::from_ymd_opt(2026, 8, 15).expect("fixture date"),
                ),
                incomplete_day_count: 1,
            },
            Usd::new(dec!(1.25)),
            Some(now - Duration::hours(48)),
        );
        for _ in 0..4 {
            tokio::task::yield_now().await;
        }

        let mut keys = recordings
            .lock()
            .expect("alert recordings lock")
            .iter()
            .map(|alert| alert.idempotency_key.clone())
            .collect::<Vec<_>>();
        keys.sort_unstable();
        assert_eq!(
            keys,
            [
                "venue-incentive-reconciliation:award-credit-overdue",
                "venue-incentive-reconciliation:incomplete-day",
                "venue-incentive-reconciliation:stale",
            ]
        );
    }
}

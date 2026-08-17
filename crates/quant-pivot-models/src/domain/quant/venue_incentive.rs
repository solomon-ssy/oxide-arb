//! Append-only venue-incentive lifecycle persistence contracts.

use std::collections::BTreeMap;

use chrono::{DateTime, Days, NaiveDate, Utc};
use schemars::JsonSchema;
use sea_orm::{DeriveIntoActiveModel, DerivePartialModel};
use serde::{Deserialize, Serialize};

use crate::{
    entities::{quant_venue_incentive_event, quant_venue_incentive_reconciliation_scan},
    enums::fee::{VenueIncentiveKind, VenueIncentiveReconciliationScanStatus, VenueIncentiveStage},
    types::{
        ContentHash, EvmTransactionHash, ExecutionAccountId, ExecutionFillId, MarketId, Usd,
        VenueIncentiveEventId, ids::VenueIncentiveReconciliationScanId,
    },
};

#[derive(Debug, Clone, Serialize, Deserialize, DerivePartialModel)]
#[sea_orm(entity = "quant_venue_incentive_event::Entity")]
pub struct VenueIncentiveEventInfo {
    pub venue_incentive_event_id: VenueIncentiveEventId,
    pub execution_account_id: ExecutionAccountId,
    pub execution_fill_id: Option<ExecutionFillId>,
    pub market_id: Option<MarketId>,
    pub kind: VenueIncentiveKind,
    pub stage: VenueIncentiveStage,
    pub program_date: NaiveDate,
    pub amount_usd: Usd,
    pub source_schedule_hash: Option<ContentHash>,
    pub source_partition: String,
    pub source_identity: String,
    pub transaction_hash: Option<EvmTransactionHash>,
    pub observed_at: DateTime<Utc>,
    pub available_at: DateTime<Utc>,
    pub evidence_hash: ContentHash,
    pub created_at: DateTime<Utc>,
}

info_from_model!(
    VenueIncentiveEventInfo,
    quant_venue_incentive_event::Model,
    {
        venue_incentive_event_id,
        execution_account_id,
        execution_fill_id,
        market_id,
        kind,
        stage,
        program_date,
        amount_usd,
        source_schedule_hash,
        source_partition,
        source_identity,
        transaction_hash,
        observed_at,
        available_at,
        evidence_hash,
        created_at,
    }
);

#[derive(Debug, Clone, Serialize, Deserialize, DeriveIntoActiveModel)]
#[sea_orm(active_model = "quant_venue_incentive_event::ActiveModel")]
pub struct NewVenueIncentiveEvent {
    pub venue_incentive_event_id: VenueIncentiveEventId,
    pub execution_account_id: ExecutionAccountId,
    pub execution_fill_id: Option<ExecutionFillId>,
    pub market_id: Option<MarketId>,
    pub kind: VenueIncentiveKind,
    pub stage: VenueIncentiveStage,
    pub program_date: NaiveDate,
    pub amount_usd: Usd,
    pub source_schedule_hash: Option<ContentHash>,
    pub source_partition: String,
    pub source_identity: String,
    pub transaction_hash: Option<EvmTransactionHash>,
    pub observed_at: DateTime<Utc>,
    pub available_at: DateTime<Utc>,
    pub evidence_hash: ContentHash,
}

/// One immutable upstream partition-scan manifest.
#[derive(Debug, Clone, Serialize, Deserialize, DerivePartialModel)]
#[sea_orm(entity = "quant_venue_incentive_reconciliation_scan::Entity")]
pub struct VenueIncentiveReconciliationScanInfo {
    pub venue_incentive_reconciliation_scan_id: VenueIncentiveReconciliationScanId,
    pub execution_account_id: ExecutionAccountId,
    pub kind: VenueIncentiveKind,
    pub stage: VenueIncentiveStage,
    pub program_date: NaiveDate,
    pub started_at: DateTime<Utc>,
    pub completed_at: DateTime<Utc>,
    pub status: VenueIncentiveReconciliationScanStatus,
    pub response_digest: Option<ContentHash>,
    pub response_count: i32,
    pub error_code: Option<String>,
    pub created_at: DateTime<Utc>,
}

info_from_model!(
    VenueIncentiveReconciliationScanInfo,
    quant_venue_incentive_reconciliation_scan::Model,
    {
        venue_incentive_reconciliation_scan_id,
        execution_account_id,
        kind,
        stage,
        program_date,
        started_at,
        completed_at,
        status,
        response_digest,
        response_count,
        error_code,
        created_at,
    }
);

/// Insert payload for one reconciliation scan result.
#[derive(Debug, Clone, Serialize, Deserialize, DeriveIntoActiveModel)]
#[sea_orm(active_model = "quant_venue_incentive_reconciliation_scan::ActiveModel")]
pub struct NewVenueIncentiveReconciliationScan {
    pub venue_incentive_reconciliation_scan_id: VenueIncentiveReconciliationScanId,
    pub execution_account_id: ExecutionAccountId,
    pub kind: VenueIncentiveKind,
    pub stage: VenueIncentiveStage,
    pub program_date: NaiveDate,
    pub started_at: DateTime<Utc>,
    pub completed_at: DateTime<Utc>,
    pub status: VenueIncentiveReconciliationScanStatus,
    pub response_digest: Option<ContentHash>,
    pub response_count: i32,
    pub error_code: Option<String>,
}

/// Complete maker-award response plus its manifest, committed atomically.
#[derive(Debug, Clone)]
pub struct NewVenueIncentiveAwardSnapshot {
    pub scan: NewVenueIncentiveReconciliationScan,
    pub awards: Vec<NewVenueIncentiveEvent>,
}

/// Durable health projection for the required closed-day scan partitions.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct VenueIncentiveScanHealth {
    pub last_success_at: Option<DateTime<Utc>>,
    pub latest_award_success_at: Option<DateTime<Utc>>,
    pub oldest_incomplete_date: Option<NaiveDate>,
    pub incomplete_day_count: u32,
}

impl VenueIncentiveScanHealth {
    /// Project the latest attempt for every required partition in an inclusive
    /// closed-day window. Empty successful responses count as complete scans.
    #[must_use]
    pub fn project(
        scans: &[VenueIncentiveReconciliationScanInfo],
        from: NaiveDate,
        to: NaiveDate,
    ) -> Self {
        let mut latest = BTreeMap::new();
        let mut last_success_at = None;
        let mut latest_award_success_at = None;
        for scan in scans {
            if scan.status == VenueIncentiveReconciliationScanStatus::Succeeded {
                last_success_at = Some(
                    last_success_at.map_or(scan.completed_at, |current: DateTime<Utc>| {
                        current.max(scan.completed_at)
                    }),
                );
                if scan.kind == VenueIncentiveKind::MakerRebate
                    && scan.stage == VenueIncentiveStage::VenueAwarded
                {
                    latest_award_success_at = Some(
                        latest_award_success_at
                            .map_or(scan.completed_at, |current: DateTime<Utc>| {
                                current.max(scan.completed_at)
                            }),
                    );
                }
            }
            let key = (scan.program_date, scan.kind, scan.stage);
            if latest
                .get(&key)
                .is_none_or(|current: &&VenueIncentiveReconciliationScanInfo| {
                    (scan.completed_at, scan.created_at)
                        > (current.completed_at, current.created_at)
                })
            {
                latest.insert(key, scan);
            }
        }
        let mut oldest_incomplete_date = None;
        let mut incomplete_day_count = 0_u32;
        let mut date = from;
        while date <= to {
            let complete = [
                (
                    VenueIncentiveKind::MakerRebate,
                    VenueIncentiveStage::VenueAwarded,
                ),
                (
                    VenueIncentiveKind::MakerRebate,
                    VenueIncentiveStage::WalletCredited,
                ),
                (
                    VenueIncentiveKind::TakerRebate,
                    VenueIncentiveStage::WalletCredited,
                ),
            ]
            .into_iter()
            .all(|(kind, stage)| {
                latest.get(&(date, kind, stage)).is_some_and(|scan| {
                    scan.status == VenueIncentiveReconciliationScanStatus::Succeeded
                })
            });
            if !complete {
                incomplete_day_count = incomplete_day_count.saturating_add(1);
                oldest_incomplete_date = Some(
                    oldest_incomplete_date.map_or(date, |current: NaiveDate| current.min(date)),
                );
            }
            let Some(next) = date.checked_add_days(Days::new(1)) else {
                break;
            };
            date = next;
        }
        Self {
            last_success_at,
            latest_award_success_at,
            oldest_incomplete_date,
            incomplete_day_count,
        }
    }
}

/// Cumulative, point-in-time incentive reconciliation. Estimated accrual and
/// venue award are valuation facts; only wallet credits are account cash.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
pub struct VenueIncentiveReconciliation {
    pub as_of: DateTime<Utc>,
    pub estimated_maker_accrual_usd: Usd,
    pub venue_awarded_maker_usd: Usd,
    pub wallet_credited_maker_usd: Usd,
    pub wallet_credited_taker_usd: Usd,
}

impl VenueIncentiveReconciliation {
    #[must_use]
    pub fn estimate_to_award_delta(self) -> Usd {
        self.venue_awarded_maker_usd - self.estimated_maker_accrual_usd
    }

    #[must_use]
    pub fn award_to_credit_delta(self) -> Usd {
        self.wallet_credited_maker_usd - self.venue_awarded_maker_usd
    }

    #[must_use]
    pub fn wallet_credit_total(self) -> Usd {
        self.wallet_credited_maker_usd + self.wallet_credited_taker_usd
    }
}

#[cfg(test)]
mod tests {
    use chrono::{DateTime, Duration, NaiveDate, TimeZone, Utc};

    use super::{VenueIncentiveReconciliationScanInfo, VenueIncentiveScanHealth};
    use crate::{
        enums::fee::{
            VenueIncentiveKind, VenueIncentiveReconciliationScanStatus, VenueIncentiveStage,
        },
        types::{ExecutionAccountId, ids::VenueIncentiveReconciliationScanId},
    };

    fn scan(
        program_date: NaiveDate,
        kind: VenueIncentiveKind,
        stage: VenueIncentiveStage,
        status: VenueIncentiveReconciliationScanStatus,
        completed_at: DateTime<Utc>,
        created_at: DateTime<Utc>,
    ) -> VenueIncentiveReconciliationScanInfo {
        VenueIncentiveReconciliationScanInfo {
            venue_incentive_reconciliation_scan_id: VenueIncentiveReconciliationScanId::from_v7(),
            execution_account_id: ExecutionAccountId::from_v7(),
            kind,
            stage,
            program_date,
            started_at: completed_at,
            completed_at,
            status,
            response_digest: None,
            response_count: 0,
            error_code: None,
            created_at,
        }
    }

    #[test]
    fn latest_failure_marks_incomplete() {
        let from = NaiveDate::from_ymd_opt(2026, 8, 14).expect("fixture date");
        let to = NaiveDate::from_ymd_opt(2026, 8, 16).expect("fixture date");
        let at = |day, hour| {
            Utc.with_ymd_and_hms(2026, 8, day, hour, 0, 0)
                .single()
                .expect("fixture time")
        };
        let required = |date, completed_at| {
            [
                scan(
                    date,
                    VenueIncentiveKind::MakerRebate,
                    VenueIncentiveStage::VenueAwarded,
                    VenueIncentiveReconciliationScanStatus::Succeeded,
                    completed_at,
                    completed_at,
                ),
                scan(
                    date,
                    VenueIncentiveKind::MakerRebate,
                    VenueIncentiveStage::WalletCredited,
                    VenueIncentiveReconciliationScanStatus::Succeeded,
                    completed_at,
                    completed_at,
                ),
                scan(
                    date,
                    VenueIncentiveKind::TakerRebate,
                    VenueIncentiveStage::WalletCredited,
                    VenueIncentiveReconciliationScanStatus::Succeeded,
                    completed_at,
                    completed_at,
                ),
            ]
        };
        let mut scans = required(from, at(15, 1)).to_vec();
        let middle = NaiveDate::from_ymd_opt(2026, 8, 15).expect("fixture date");
        scans.extend(required(middle, at(16, 1)));
        scans.push(scan(
            middle,
            VenueIncentiveKind::MakerRebate,
            VenueIncentiveStage::VenueAwarded,
            VenueIncentiveReconciliationScanStatus::Failed,
            at(16, 2),
            at(16, 2),
        ));
        scans.extend(required(to, at(17, 1)));

        let health = VenueIncentiveScanHealth::project(&scans, from, to);
        assert_eq!(health.last_success_at, Some(at(17, 1)));
        assert_eq!(health.latest_award_success_at, Some(at(17, 1)));
        assert_eq!(health.oldest_incomplete_date, Some(middle));
        assert_eq!(health.incomplete_day_count, 1);
    }

    #[test]
    fn created_at_breaks_tie() {
        let date = NaiveDate::from_ymd_opt(2026, 8, 16).expect("fixture date");
        let completed_at = Utc
            .with_ymd_and_hms(2026, 8, 17, 1, 0, 0)
            .single()
            .expect("fixture time");
        let mut scans = [
            scan(
                date,
                VenueIncentiveKind::MakerRebate,
                VenueIncentiveStage::VenueAwarded,
                VenueIncentiveReconciliationScanStatus::Succeeded,
                completed_at,
                completed_at,
            ),
            scan(
                date,
                VenueIncentiveKind::MakerRebate,
                VenueIncentiveStage::WalletCredited,
                VenueIncentiveReconciliationScanStatus::Succeeded,
                completed_at,
                completed_at,
            ),
            scan(
                date,
                VenueIncentiveKind::TakerRebate,
                VenueIncentiveStage::WalletCredited,
                VenueIncentiveReconciliationScanStatus::Succeeded,
                completed_at,
                completed_at,
            ),
        ]
        .to_vec();
        scans.push(scan(
            date,
            VenueIncentiveKind::MakerRebate,
            VenueIncentiveStage::VenueAwarded,
            VenueIncentiveReconciliationScanStatus::Failed,
            completed_at,
            completed_at + Duration::microseconds(1),
        ));

        let health = VenueIncentiveScanHealth::project(&scans, date, date);
        assert_eq!(health.oldest_incomplete_date, Some(date));
        assert_eq!(health.incomplete_day_count, 1);
    }
}

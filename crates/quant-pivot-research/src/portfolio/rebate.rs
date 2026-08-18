//! Account-level maker-rebate threshold and payout-lag valuation evidence.

use std::collections::{BTreeMap, VecDeque, btree_map::Entry};

use chrono::{DateTime, Days, NaiveDate, NaiveTime, Utc};
use quant_pivot_error::{QuantResult, report::ReportError};
use quant_pivot_models::{
    domain::quant::{
        VenueIncentiveEventInfo,
        venue_incentive::{VenueIncentiveReconciliationScanInfo, VenueIncentiveScanHealth},
    },
    enums::fee::VenueIncentiveStage,
    hashing::CanonicalDigest,
    runtime_config::MakerRebatePolicy,
    types::{
        MakerRebateDelayBasis, MakerRebateProgramDayBaseline, MakerRebateValuationEvidence,
        MakerRebateValuationHealth, Usd,
    },
};
use rust_decimal::Decimal;

/// Complete point-in-time inputs for one report-wide valuation snapshot.
pub struct MakerRebateValuationInput<'a> {
    pub as_of: DateTime<Utc>,
    pub stale_after_secs: u64,
    pub health_from: NaiveDate,
    pub health_to: NaiveDate,
    pub scans: &'a [VenueIncentiveReconciliationScanInfo],
    pub events: &'a [VenueIncentiveEventInfo],
    pub policy: &'a MakerRebatePolicy,
}

#[derive(Clone, Copy)]
struct ReportedAccrualBucket {
    program_date: NaiveDate,
    program_close: DateTime<Utc>,
    remaining: Decimal,
}

/// Builds one immutable valuation shared by every candidate and cohort.
pub struct MakerRebateValuationFactory;

impl MakerRebateValuationFactory {
    pub fn build(
        input: &MakerRebateValuationInput<'_>,
    ) -> QuantResult<MakerRebateValuationEvidence> {
        if input.health_from > input.health_to
            || input.stale_after_secs == 0
            || input.policy.payout_threshold_usd.value <= Decimal::ZERO
            || input.policy.fallback_lag_from_program_close_secs == 0
            || input.policy.observed_p95_min_samples == 0
        {
            return Err(ReportError::InvariantViolation {
                stage: "maker_rebate_valuation",
                detail: "health window and maker-rebate valuation policy must be valid".to_owned(),
            }
            .into());
        }
        let scan_health =
            VenueIncentiveScanHealth::project(input.scans, input.health_from, input.health_to);
        let health = if scan_health.incomplete_day_count > 0 {
            MakerRebateValuationHealth::Incomplete
        } else if scan_health.last_success_at.is_none_or(|last| {
            input.as_of.signed_duration_since(last).num_seconds()
                >= i64::try_from(input.stale_after_secs).unwrap_or(i64::MAX)
        }) {
            MakerRebateValuationHealth::Stale
        } else {
            MakerRebateValuationHealth::Healthy
        };
        let latest = Self::latest_events(input.events);
        let program_day_baselines = Self::program_day_baselines(&latest);
        let payout_threshold_usd = Usd::new(input.policy.payout_threshold_usd.value);
        let mut delays = Self::complete_bucket_delays(&latest, payout_threshold_usd)?;
        let complete_program_days =
            u32::try_from(delays.len()).map_err(|error| ReportError::NumericOverflow {
                field: "maker_rebate.complete_program_days",
                detail: error.to_string(),
            })?;
        let delay_basis = if complete_program_days < input.policy.observed_p95_min_samples {
            MakerRebateDelayBasis::ConservativeFallback {
                lag_from_program_close_secs: input.policy.fallback_lag_from_program_close_secs,
            }
        } else {
            let required =
                usize::try_from(input.policy.observed_p95_min_samples).map_err(|error| {
                    ReportError::NumericOverflow {
                        field: "maker_rebate.observed_p95_min_samples",
                        detail: error.to_string(),
                    }
                })?;
            let recent_start = delays.len().saturating_sub(required);
            let recent = &mut delays[recent_start..];
            recent.sort_unstable();
            let rank = (recent.len() * 95).div_ceil(100).saturating_sub(1);
            let lag_from_program_close_secs =
                recent
                    .get(rank)
                    .copied()
                    .ok_or_else(|| ReportError::InvariantViolation {
                        stage: "maker_rebate_valuation",
                        detail: "observed payout-delay sample disappeared".to_owned(),
                    })?;
            MakerRebateDelayBasis::ObservedP95 {
                lag_from_program_close_secs,
                complete_program_days,
            }
        };
        let evidence_hash = CanonicalDigest::content_hash_typed(
            "quant-pivot/maker-rebate-valuation",
            2,
            &(
                input.as_of,
                health,
                &program_day_baselines,
                payout_threshold_usd,
                delay_basis,
                scan_health.last_success_at,
                scan_health.oldest_incomplete_date,
            ),
        )?;
        Ok(MakerRebateValuationEvidence {
            as_of: input.as_of,
            health,
            program_day_baselines,
            payout_threshold_usd,
            delay_basis,
            evidence_hash,
        })
    }

    fn latest_events(events: &[VenueIncentiveEventInfo]) -> Vec<&VenueIncentiveEventInfo> {
        let mut latest = BTreeMap::new();
        for event in events {
            match latest.entry(event.source_partition.as_str()) {
                Entry::Vacant(entry) => {
                    entry.insert(event);
                }
                Entry::Occupied(mut entry)
                    if (event.available_at, event.created_at)
                        > (entry.get().available_at, entry.get().created_at) =>
                {
                    entry.insert(event);
                }
                Entry::Occupied(_) => {}
            }
        }
        latest.into_values().collect()
    }

    fn program_day_baselines(
        events: &[&VenueIncentiveEventInfo],
    ) -> Vec<MakerRebateProgramDayBaseline> {
        let mut totals = BTreeMap::<NaiveDate, Decimal>::new();
        for event in events {
            if event.stage == VenueIncentiveStage::EstimatedAccrual
                && event.amount_usd.is_positive()
            {
                *totals.entry(event.program_date).or_default() += event.amount_usd.inner();
            }
        }
        totals
            .into_iter()
            .map(
                |(program_date, confirmed_accrual_usd)| MakerRebateProgramDayBaseline {
                    program_date,
                    confirmed_accrual_usd: Usd::new(confirmed_accrual_usd),
                },
            )
            .collect()
    }

    fn complete_bucket_delays(
        events: &[&VenueIncentiveEventInfo],
        payout_threshold_usd: Usd,
    ) -> QuantResult<Vec<u64>> {
        let mut reported_totals = BTreeMap::<NaiveDate, Decimal>::new();
        let mut credits = Vec::new();
        for event in events {
            if !event.amount_usd.is_positive() {
                continue;
            }
            match event.stage {
                VenueIncentiveStage::VenueReportedAccrual => {
                    *reported_totals.entry(event.program_date).or_default() +=
                        event.amount_usd.inner();
                }
                VenueIncentiveStage::WalletCredited => credits.push(*event),
                VenueIncentiveStage::EstimatedAccrual => {}
            }
        }
        let mut reported = reported_totals
            .into_iter()
            .filter(|(_, total)| *total >= payout_threshold_usd.inner())
            .map(|(program_date, remaining)| {
                let next_date = program_date.checked_add_days(Days::new(1)).ok_or_else(|| {
                    ReportError::InvariantViolation {
                        stage: "maker_rebate_valuation",
                        detail: format!("program day {program_date} has no UTC close"),
                    }
                })?;
                Ok(ReportedAccrualBucket {
                    program_date,
                    program_close: DateTime::from_naive_utc_and_offset(
                        next_date.and_time(NaiveTime::MIN),
                        Utc,
                    ),
                    remaining,
                })
            })
            .collect::<QuantResult<VecDeque<_>>>()?;
        credits.sort_by_key(|event| (event.observed_at, event.available_at, event.created_at));
        let mut completed = Vec::new();
        for credit in credits {
            let mut remaining_credit = credit.amount_usd.inner();
            while remaining_credit > Decimal::ZERO {
                let Some(bucket) = reported.front_mut() else {
                    break;
                };
                if credit.observed_at < bucket.program_close {
                    break;
                }
                let consumed = remaining_credit.min(bucket.remaining);
                remaining_credit -= consumed;
                bucket.remaining -= consumed;
                if bucket.remaining.is_zero() {
                    let delay = credit
                        .observed_at
                        .signed_duration_since(bucket.program_close)
                        .num_seconds();
                    completed.push(u64::try_from(delay).map_err(|error| {
                        ReportError::NumericOverflow {
                            field: "maker_rebate.payout_lag_from_program_close_secs",
                            detail: format!("program day {}: {error}", bucket.program_date),
                        }
                    })?);
                    reported.pop_front();
                }
            }
        }
        Ok(completed)
    }
}

#[cfg(test)]
mod tests {
    use chrono::{DateTime, Days, Duration, NaiveDate, TimeZone, Utc};
    use quant_pivot_models::{
        domain::quant::venue_incentive::{
            VenueIncentiveEventInfo, VenueIncentiveReconciliationScanInfo,
        },
        enums::fee::{
            VenueIncentiveKind, VenueIncentiveReconciliationScanStatus, VenueIncentiveStage,
        },
        runtime_config::{DecimalValue, MakerRebatePolicy},
        types::{
            ExecutionAccountId, MakerRebateDelayBasis, Usd, VenueIncentiveEventId,
            ids::VenueIncentiveReconciliationScanId,
        },
    };
    use rust_decimal_macros::dec;

    use super::{MakerRebateValuationFactory, MakerRebateValuationInput};
    use crate::hashing::ResearchHasher;

    fn event(
        account: ExecutionAccountId,
        date: NaiveDate,
        stage: VenueIncentiveStage,
        amount: Usd,
        observed_at: DateTime<Utc>,
        suffix: &str,
    ) -> VenueIncentiveEventInfo {
        VenueIncentiveEventInfo {
            venue_incentive_event_id: VenueIncentiveEventId::from_v7(),
            execution_account_id: account,
            clob_trade_observation_id: None,
            market_id: None,
            kind: VenueIncentiveKind::MakerRebate,
            stage,
            program_date: date,
            amount_usd: amount,
            source_terms_hash: None,
            source_partition: format!("{date}:{suffix}"),
            source_identity: format!("{date}:{suffix}"),
            transaction_hash: None,
            observed_at,
            available_at: observed_at,
            evidence_hash: ResearchHasher::canonical(&(date, suffix)).expect("event hash"),
            created_at: observed_at,
        }
    }

    fn scans(
        account: ExecutionAccountId,
        from: NaiveDate,
        as_of: DateTime<Utc>,
        days: u32,
    ) -> Vec<VenueIncentiveReconciliationScanInfo> {
        (0..days)
            .flat_map(|offset| {
                let date = from
                    .checked_add_days(Days::new(u64::from(offset)))
                    .expect("scan date");
                [
                    (
                        VenueIncentiveKind::MakerRebate,
                        VenueIncentiveStage::VenueReportedAccrual,
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
                .map(move |(kind, stage)| VenueIncentiveReconciliationScanInfo {
                    venue_incentive_reconciliation_scan_id:
                        VenueIncentiveReconciliationScanId::from_v7(),
                    execution_account_id: account,
                    kind,
                    stage,
                    program_date: date,
                    started_at: as_of - Duration::minutes(2),
                    completed_at: as_of - Duration::minutes(1),
                    status: VenueIncentiveReconciliationScanStatus::Succeeded,
                    response_digest: Some(
                        ResearchHasher::canonical(&(date, kind, stage)).expect("scan hash"),
                    ),
                    response_count: 1,
                    error_code: None,
                    created_at: as_of - Duration::minutes(1),
                })
            })
            .collect()
    }

    #[test]
    fn delay_switches_to_p95() {
        let account = ExecutionAccountId::from_v7();
        let as_of = Utc
            .with_ymd_and_hms(2026, 8, 17, 12, 0, 0)
            .single()
            .expect("as of");
        let health_to = as_of.date_naive().pred_opt().expect("health end");
        let health_from = health_to
            .checked_sub_days(Days::new(29))
            .expect("health start");
        let mut events = Vec::new();
        for offset in 0_u32..30 {
            let date = health_from
                .checked_add_days(Days::new(u64::from(offset)))
                .expect("program date");
            let program_close = date
                .checked_add_days(Days::new(1))
                .expect("next day")
                .and_hms_opt(0, 0, 0)
                .expect("program close")
                .and_utc();
            let credit_at = program_close + Duration::hours(i64::from(offset + 1));
            let scan_at = credit_at + Duration::hours(6);
            events.push(event(
                account,
                date,
                VenueIncentiveStage::VenueReportedAccrual,
                Usd::ONE,
                scan_at,
                "reported",
            ));
            events.push(event(
                account,
                date,
                VenueIncentiveStage::WalletCredited,
                Usd::ONE,
                credit_at,
                "credit",
            ));
        }
        let policy = MakerRebatePolicy {
            payout_threshold_usd: DecimalValue::new(dec!(1)),
            fallback_lag_from_program_close_secs: 172_800,
            observed_p95_min_samples: 30,
        };
        let valuation = MakerRebateValuationFactory::build(&MakerRebateValuationInput {
            as_of,
            stale_after_secs: 3_600,
            health_from,
            health_to,
            scans: &scans(account, health_from, as_of, 30),
            events: &events,
            policy: &policy,
        })
        .expect("valuation");

        assert_eq!(
            valuation.delay_basis,
            MakerRebateDelayBasis::ObservedP95 {
                lag_from_program_close_secs: 29 * 3_600,
                complete_program_days: 30,
            }
        );
    }

    #[test]
    fn fallback_keeps_daily_baseline() {
        let account = ExecutionAccountId::from_v7();
        let as_of = Utc
            .with_ymd_and_hms(2026, 8, 17, 12, 0, 0)
            .single()
            .expect("as of");
        let date = as_of.date_naive().pred_opt().expect("program date");
        let events = vec![event(
            account,
            date,
            VenueIncentiveStage::EstimatedAccrual,
            Usd::new(dec!(0.4)),
            as_of - Duration::minutes(1),
            "estimated",
        )];
        let policy = MakerRebatePolicy::default();
        let valuation = MakerRebateValuationFactory::build(&MakerRebateValuationInput {
            as_of,
            stale_after_secs: 3_600,
            health_from: date,
            health_to: date,
            scans: &scans(account, date, as_of, 1),
            events: &events,
            policy: &policy,
        })
        .expect("valuation");

        assert_eq!(valuation.baseline_for(date), Usd::new(dec!(0.4)));
        assert_eq!(
            valuation.delay_basis,
            MakerRebateDelayBasis::ConservativeFallback {
                lag_from_program_close_secs: 172_800,
            }
        );
    }
}

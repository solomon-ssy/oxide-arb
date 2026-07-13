//! Stateless "next fire times" preview for a report-schedule [`ScheduleCadence`].
//!
//! Uses the same `croner` parser (identical 6-field grammar and wall-clock
//! timezone semantics) that `tokio-cron-scheduler` — and thus the live report
//! scheduler — relies on, so the UI preview matches real scheduling. Pure and
//! side-effect-free: it never touches a running scheduler.

use super::ScheduleCadence;
use chrono::{DateTime, Duration, TimeZone, Utc};
use croner::{Cron, errors::CronError};
use quant_pivot_error::{
    ConfigValidationError, ConfigValidationReport, QuantError, QuantResult, config::ConfigError,
};
use std::str::FromStr;

/// Upper bound on previewed occurrences per request.
pub const MAX_PREVIEW_OCCURRENCES: usize = 20;

/// Compute the next `count` fire times (in UTC) for a cadence, starting after `now`.
///
/// A timezone-bound cron is interpreted as wall-clock time in that IANA zone
/// (matching the runner) and converted back to UTC for the wire. Invalid input
/// (zero interval, malformed cron, unknown timezone) fails as [`QuantError::Config`].
pub fn preview_fire_times(
    cadence: &ScheduleCadence,
    now: DateTime<Utc>,
    count: usize,
) -> QuantResult<Vec<DateTime<Utc>>> {
    let count = count.clamp(1, MAX_PREVIEW_OCCURRENCES);
    match cadence {
        ScheduleCadence::Interval { interval_secs } => {
            if *interval_secs == 0 {
                return Err(reject(
                    "reports.schedules.cadence.interval_secs",
                    "interval must be greater than zero".to_owned(),
                ));
            }
            let step = i64::try_from(*interval_secs).unwrap_or(i64::MAX);
            Ok((1..=i64::try_from(count).unwrap_or(1))
                .map(|k| now + Duration::seconds(step.saturating_mul(k)))
                .collect())
        }
        ScheduleCadence::Cron {
            expr,
            timezone: None,
        } => {
            let cron = parse_cron(expr)?;
            occurrences(&cron, now, count).map_err(|error| reject_cron_eval(&error))
        }
        ScheduleCadence::Cron {
            expr,
            timezone: Some(timezone),
        } => {
            let cron = parse_cron(expr)?;
            let zone: chrono_tz::Tz = timezone.parse().map_err(|_| {
                reject(
                    "reports.schedules.cadence.timezone",
                    format!("invalid IANA timezone {timezone:?}"),
                )
            })?;
            let local = occurrences(&cron, now.with_timezone(&zone), count)
                .map_err(|error| reject_cron_eval(&error))?;
            Ok(local
                .into_iter()
                .map(|fire| fire.with_timezone(&Utc))
                .collect())
        }
    }
}

fn parse_cron(expr: &str) -> QuantResult<Cron> {
    Cron::from_str(expr).map_err(|error| {
        reject(
            "reports.schedules.cadence.expr",
            format!("invalid cron expression {expr:?}: {error}"),
        )
    })
}

fn occurrences<Tz>(
    cron: &Cron,
    start: DateTime<Tz>,
    count: usize,
) -> Result<Vec<DateTime<Tz>>, CronError>
where
    Tz: TimeZone,
{
    let mut out = Vec::with_capacity(count);
    let mut cursor = start;
    for _ in 0..count {
        let next = cron.find_next_occurrence(&cursor, false)?;
        cursor = next.clone();
        out.push(next);
    }
    Ok(out)
}

fn reject(field: &'static str, detail: String) -> QuantError {
    QuantError::Config(
        ConfigValidationReport::single_error(ConfigValidationError::invalid_value(field, detail))
            .into(),
    )
}

fn reject_cron_eval(error: &CronError) -> QuantError {
    reject(
        "reports.schedules.cadence.expr",
        format!("cron evaluation failed: {error}"),
    )
}

/// Validate a schedule cadence with the same parser used by preview and the live scheduler.
///
/// Enabled schedules must pass this before create/activate; keeps validation aligned with
/// `POST /runtime-config/schedule-preview` and `job_for_cadence`.
pub fn validate_schedule_cadence(cadence: &ScheduleCadence, report: &mut ConfigValidationReport) {
    match preview_fire_times(cadence, Utc::now(), 1) {
        Ok(_) => {}
        Err(QuantError::Config(ConfigError::Validation(inner))) => {
            report.errors.extend(inner.errors);
        }
        Err(other) => {
            report.errors.push(ConfigValidationError::invalid_value(
                "reports.schedules.cadence",
                other.to_string(),
            ));
        }
    }
}

#[cfg(test)]
mod tests {
    use super::{ScheduleCadence, preview_fire_times};
    use chrono::{Duration, TimeZone, Utc};

    #[test]
    fn interval_preview_steps_by_interval() {
        let now = Utc.timestamp_opt(1_700_000_000, 0).unwrap();
        let times =
            preview_fire_times(&ScheduleCadence::Interval { interval_secs: 300 }, now, 3).unwrap();
        assert_eq!(times.len(), 3);
        assert_eq!(times[0], now + Duration::seconds(300));
        assert_eq!(times[2], now + Duration::seconds(900));
    }

    #[test]
    fn zero_interval_is_rejected() {
        let now = Utc::now();
        assert!(
            preview_fire_times(&ScheduleCadence::Interval { interval_secs: 0 }, now, 3).is_err()
        );
    }

    #[test]
    fn utc_cron_preview_is_monotonic() {
        let now = Utc.timestamp_opt(1_700_000_000, 0).unwrap();
        let times = preview_fire_times(
            &ScheduleCadence::Cron {
                expr: "0 0 9 * * *".to_owned(),
                timezone: None,
            },
            now,
            5,
        )
        .unwrap();
        assert_eq!(times.len(), 5);
        for pair in times.windows(2) {
            assert!(pair[1] > pair[0]);
        }
    }

    #[test]
    fn timezone_cron_preview_resolves() {
        let now = Utc::now();
        let times = preview_fire_times(
            &ScheduleCadence::Cron {
                expr: "0 0 9 * * *".to_owned(),
                timezone: Some("America/New_York".to_owned()),
            },
            now,
            2,
        )
        .unwrap();
        assert_eq!(times.len(), 2);
        assert!(times[0] > now);
    }

    #[test]
    fn invalid_cron_is_rejected() {
        let now = Utc::now();
        assert!(
            preview_fire_times(
                &ScheduleCadence::Cron {
                    expr: "not-a-cron".to_owned(),
                    timezone: None,
                },
                now,
                3,
            )
            .is_err()
        );
    }
}

//! `ScheduleCadence` → `tokio-cron-scheduler` job mapping.
//!
//! This is the only place that knows how a runtime-config cadence becomes a
//! concrete scheduler job. The fire side-effect is injected as a
//! `FireFutureFactory` so this module stays free of report/lifecycle concerns
//! (parent doc §23.2 layering).

use std::{future::Future, pin::Pin, time::Duration};

use quant_pivot_error::{
    ConfigValidationError, ConfigValidationReport, QuantError, QuantResult,
    scheduler::SchedulerError,
};
use quant_pivot_models::runtime_config::ScheduleCadence;
use tokio_cron_scheduler::Job;

/// Future produced on each fire (the report run, already wrapped).
pub type FireFuture = Pin<Box<dyn Future<Output = ()> + Send>>;

/// Builds a fresh fire future per invocation (jobs fire repeatedly).
pub trait FireFutureFactory: Fn() -> FireFuture + Send + Sync + 'static {}

impl<F> FireFutureFactory for F where F: Fn() -> FireFuture + Send + Sync + 'static {}

fn invalid_job(detail: impl std::fmt::Display) -> QuantError {
    SchedulerError::InvalidJobSpec {
        detail: detail.to_string(),
    }
    .into()
}

/// Build a scheduler job for a cadence, invoking `make_future` on each fire.
///
/// - `Interval { interval_secs }` → repeated job (checked at 500 ms granularity).
/// - `Cron { expr, None }` → UTC cron job.
/// - `Cron { expr, Some(tz) }` → wall-clock cron job in the IANA timezone.
///
/// Operator misconfiguration (zero interval, bad timezone) fails as
/// [`QuantError::Config`]. Semantically valid cadences rejected by the scheduler
/// backend fail as [`QuantError::Scheduler`].
pub fn job_for_cadence<F>(cadence: &ScheduleCadence, make_future: F) -> QuantResult<Job>
where
    F: FireFutureFactory,
{
    match cadence {
        ScheduleCadence::Interval { interval_secs } => {
            if *interval_secs == 0 {
                return Err(QuantError::Config(
                    ConfigValidationReport::single_error(ConfigValidationError::invalid_value(
                        "reports.schedules.interval_secs",
                        "report schedule interval_secs must be > 0",
                    ))
                    .into(),
                ));
            }
            Job::new_repeated_async(Duration::from_secs(*interval_secs), move |_uuid, _sched| {
                make_future()
            })
            .map_err(|error| invalid_job(format!("invalid interval cadence: {error}")))
        }
        ScheduleCadence::Cron {
            expr,
            timezone: None,
        } => Job::new_cron_job_async(expr.as_str(), move |_uuid, _sched| make_future())
            .map_err(|error| invalid_job(format!("invalid cron expression {expr:?}: {error}"))),
        ScheduleCadence::Cron {
            expr,
            timezone: Some(timezone),
        } => {
            let tz = timezone.parse::<chrono_tz::Tz>().map_err(|error| {
                QuantError::Config(
                    ConfigValidationReport::single_error(ConfigValidationError::invalid_value(
                        "reports.schedules.timezone",
                        format!("invalid cron timezone {timezone:?}: {error}"),
                    ))
                    .into(),
                )
            })?;
            Job::new_cron_job_async_tz(expr.as_str(), tz, move |_uuid, _sched| make_future())
                .map_err(|error| invalid_job(format!("invalid cron expression {expr:?}: {error}")))
        }
    }
}

#[cfg(test)]
mod tests {
    use quant_pivot_error::QuantError;
    use quant_pivot_models::runtime_config::ScheduleCadence;

    use super::{FireFuture, job_for_cadence};

    fn noop() -> FireFuture {
        Box::pin(async {})
    }

    #[test]
    fn interval_cadence_builds_a_job() {
        let cadence = ScheduleCadence::Interval { interval_secs: 300 };
        assert!(job_for_cadence(&cadence, noop).is_ok());
    }

    #[test]
    fn zero_interval_fails_closed_as_config() {
        let cadence = ScheduleCadence::Interval { interval_secs: 0 };
        let err = job_for_cadence(&cadence, noop);
        assert!(matches!(err, Err(QuantError::Config(_))));
    }

    #[test]
    fn valid_utc_cron_builds_a_job() {
        let cadence = ScheduleCadence::Cron {
            expr: "0 0 9 * * *".to_owned(),
            timezone: None,
        };
        assert!(job_for_cadence(&cadence, noop).is_ok());
    }

    #[test]
    fn invalid_cron_expression_fails_as_scheduler() {
        let cadence = ScheduleCadence::Cron {
            expr: "not-a-cron".to_owned(),
            timezone: None,
        };
        let err = job_for_cadence(&cadence, noop);
        assert!(matches!(err, Err(QuantError::Scheduler(_))));
    }

    #[test]
    fn valid_tz_cron_builds_a_job() {
        let cadence = ScheduleCadence::Cron {
            expr: "0 0 9 * * *".to_owned(),
            timezone: Some("America/New_York".to_owned()),
        };
        assert!(job_for_cadence(&cadence, noop).is_ok());
    }

    #[test]
    fn invalid_timezone_fails_closed_as_config() {
        let cadence = ScheduleCadence::Cron {
            expr: "0 0 9 * * *".to_owned(),
            timezone: Some("Mars/Phobos".to_owned()),
        };
        let err = job_for_cadence(&cadence, noop);
        assert!(matches!(err, Err(QuantError::Config(_))));
    }
}

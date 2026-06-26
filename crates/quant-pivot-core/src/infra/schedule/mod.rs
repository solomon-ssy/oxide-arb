//! Report-plane scheduling (`tokio-cron-scheduler` facade).
//!
//! Thin isolation layer for report fire scheduling. Business crates and other
//! core modules must reach scheduling through [`ReportScheduleRunner`]; only
//! this module depends on `tokio-cron-scheduler` (boundary-linted).

pub mod job_factory;
pub mod overlap;
pub mod runner;

pub use overlap::ScheduleOverlapGuard;
pub use runner::{
    ReportScheduleRunner, ReportSchedulerDeps, ScheduledReportExecutor, TokioCronScheduleRunner,
};

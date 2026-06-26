//! Phase 04.2 recommendation report generation closed loop.

mod builder;
mod composer;
mod lifecycle;
mod publisher;
mod scheduler;
mod types;

pub use builder::{DefaultReportBuilder, ReportBuilder, ReportBuilderDeps};
pub use composer::{DefaultRecommendationComposer, RecommendationComposer};
pub use lifecycle::{
    AdHocReportRequest, ReportLifecycleDeps, ReportLifecycleService, ScheduledReportRequest,
};
pub use publisher::{ReportPublisher, ReportPublisherDeps};
pub use scheduler::build_report_scheduler;
pub use types::{
    BuildReportRequest, ComposedReport, EmptyReportContext, NotificationRecommendation,
    ReportNotificationPayload, ReportTrigger,
};

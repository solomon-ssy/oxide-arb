//! Phase 04.2 recommendation report generation closed loop.

mod builder;
mod composer;
mod fact_bundle;
mod fact_delivery;
mod funnel;
mod lifecycle;
mod publisher;
mod readiness;
mod scheduler;
mod types;

pub use builder::{DefaultReportBuilder, ReportBuilder, ReportBuilderDeps};
pub use composer::{DefaultRecommendationComposer, RecommendationComposer};
pub use fact_delivery::{ReportFactDeliveryDeps, ReportFactDeliveryWorker};
pub use lifecycle::{
    AdHocReportRequest, ReportLifecycleDeps, ReportLifecycleService, ScheduledReportRequest,
};
pub use publisher::{ReportPublisher, ReportPublisherDeps};
pub use readiness::{DefaultReportReadinessGate, ReportReadinessGate};
pub use scheduler::build_report_scheduler;
pub use types::{
    BuildReportRequest, ComposedReport, EmptyReportContext, NotificationRecommendation,
    ReportNotificationPayload, ReportTrigger,
};

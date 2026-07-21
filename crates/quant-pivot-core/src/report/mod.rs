//! recommendation report generation closed loop.

mod builder;
mod composer;
mod coordinator;
mod fact_bundle;
mod fact_delivery;
mod funnel;
mod lifecycle;
mod publisher;
mod readiness;
mod types;

pub use builder::{DefaultReportBuilder, ReportBuilder, ReportBuilderDeps};
pub use composer::{DefaultRecommendationComposer, RecommendationComposer};
pub use coordinator::{ReportCoordinator, ReportCoordinatorConfig};
pub use fact_delivery::{ReportFactDeliveryDeps, ReportFactDeliveryWorker};
pub use lifecycle::{
    AdHocReportRequest, ReportLifecycleDeps, ReportLifecycleService, RetryAdHocReportRequest,
};
pub use publisher::{ReportPublisher, ReportPublisherDeps};
pub use readiness::{DefaultReportReadinessGate, ReportReadinessGate};
pub use types::{
    BuildReportRequest, ComposedReport, EmptyReportContext, NotificationRecommendation,
    ReportNotificationPayload, ReportTrigger,
};

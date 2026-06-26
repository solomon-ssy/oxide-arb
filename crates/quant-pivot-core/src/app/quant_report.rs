//! Core implementation of [`QuantReportPort`] for the report HTTP surface.
//!
//! This is the service boundary between the web handlers and the report plane:
//! reads go through the report / recommendation repositories, the ad-hoc run goes
//! through the [`ReportScheduleRunner`] (async enqueue), and revoke goes through
//! the [`ReportLifecycleService`] (transactional + post-commit event). Handlers
//! never touch a repository or a venue client directly.

use std::sync::Arc;

use async_trait::async_trait;
use chrono::Utc;
use quant_pivot_error::QuantResult;
use quant_pivot_models::{
    domain::{
        AdHocReportCommand, AdHocReportEnqueued, Paginated, QuantReportListQuery, QuantReportPort,
        RecommendationInfo, RecommendationReportInfo, ReportDiff, compute_report_diff,
    },
    enums::quant::ReportKind,
    types::{RecommendationId, RecommendationReportId},
};
use quant_pivot_repository::traits::{RecommendationReportRepository, RecommendationRepository};

use crate::{
    infra::schedule::ReportScheduleRunner,
    report::{AdHocReportRequest, ReportLifecycleService, ReportTrigger},
};

/// Web-facing report port assembled from the report plane.
pub struct CoreQuantReportPort {
    report_repo: Arc<dyn RecommendationReportRepository>,
    recommendation_repo: Arc<dyn RecommendationRepository>,
    lifecycle: Arc<ReportLifecycleService>,
    scheduler: Arc<dyn ReportScheduleRunner>,
}

impl CoreQuantReportPort {
    /// Assemble the port from the report-plane services and read repositories.
    #[must_use]
    pub fn new(
        report_repo: Arc<dyn RecommendationReportRepository>,
        recommendation_repo: Arc<dyn RecommendationRepository>,
        lifecycle: Arc<ReportLifecycleService>,
        scheduler: Arc<dyn ReportScheduleRunner>,
    ) -> Self {
        Self {
            report_repo,
            recommendation_repo,
            lifecycle,
            scheduler,
        }
    }
}

#[async_trait]
impl QuantReportPort for CoreQuantReportPort {
    async fn list_reports(
        &self,
        query: QuantReportListQuery,
    ) -> QuantResult<Paginated<RecommendationReportInfo>> {
        Ok(self.report_repo.page(query).await?)
    }

    async fn find_report(
        &self,
        report_id: &RecommendationReportId,
    ) -> QuantResult<Option<RecommendationReportInfo>> {
        Ok(self.report_repo.find_by_id(report_id).await?)
    }

    async fn latest_report(
        &self,
        kind: ReportKind,
    ) -> QuantResult<Option<RecommendationReportInfo>> {
        Ok(self.report_repo.latest_published(kind).await?)
    }

    async fn find_recommendations(
        &self,
        report_id: &RecommendationReportId,
    ) -> QuantResult<Vec<RecommendationInfo>> {
        Ok(self.recommendation_repo.find_by_report(report_id).await?)
    }

    async fn find_recommendation(
        &self,
        recommendation_id: &RecommendationId,
    ) -> QuantResult<Option<RecommendationInfo>> {
        Ok(self
            .recommendation_repo
            .find_by_id(recommendation_id)
            .await?)
    }

    async fn diff_reports(
        &self,
        base_report_id: &RecommendationReportId,
        compare_report_id: &RecommendationReportId,
    ) -> QuantResult<Option<ReportDiff>> {
        let (Some(base), Some(compare)) = (
            self.report_repo.find_by_id(base_report_id).await?,
            self.report_repo.find_by_id(compare_report_id).await?,
        ) else {
            return Ok(None);
        };
        let base_recs = self
            .recommendation_repo
            .find_by_report(base_report_id)
            .await?;
        let compare_recs = self
            .recommendation_repo
            .find_by_report(compare_report_id)
            .await?;
        Ok(Some(compute_report_diff(
            &base,
            &base_recs,
            &compare,
            &compare_recs,
        )))
    }

    async fn enqueue_ad_hoc(
        &self,
        command: AdHocReportCommand,
    ) -> QuantResult<AdHocReportEnqueued> {
        let trigger_time = Utc::now();
        let trigger = ReportTrigger::AdHoc {
            request_id: command.request_id.clone(),
        };
        let trigger_key = trigger.key(trigger_time);
        self.scheduler
            .enqueue_ad_hoc(AdHocReportRequest {
                request_id: command.request_id.clone(),
                trigger_time,
                top_n: command.top_n,
                source_delay_secs: command.source_delay_secs,
            })
            .await?;
        Ok(AdHocReportEnqueued {
            request_id: command.request_id,
            trigger_key,
        })
    }

    async fn revoke(
        &self,
        report_id: &RecommendationReportId,
        reason: &str,
    ) -> QuantResult<RecommendationReportInfo> {
        self.lifecycle.revoke(report_id, reason, Utc::now()).await
    }
}

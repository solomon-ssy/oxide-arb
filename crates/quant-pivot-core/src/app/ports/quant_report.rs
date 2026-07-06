//! Core implementation of [`QuantReportPort`] for the report HTTP surface.
//!
//! This is the service boundary between the web handlers and the report plane:
//! reads go through the report / recommendation repositories, the ad-hoc run goes
//! through the [`ReportScheduleRunner`] (async enqueue), and revoke goes through
//! the [`ReportLifecycleService`] (transactional + post-commit event). Handlers
//! never touch a repository or a venue client directly.

use std::{collections::HashMap, sync::Arc};

use async_trait::async_trait;
use chrono::Utc;
use quant_pivot_error::QuantResult;
use quant_pivot_models::{
    domain::{
        AdHocReportCommand, AdHocReportEnqueued, Paginated, QuantEvidenceView,
        QuantRecommendationView, QuantReportListQuery, QuantReportPort, RecommendationInfo,
        RecommendationReportInfo, RecommendationViewContext, ReportDiff, compute_report_diff,
    },
    enums::quant::{RecommendationReportStatus, ReportKind},
    types::{OrderIntentId, RecommendationId, RecommendationReportId},
};
use quant_pivot_repository::traits::{
    OrderIntentRepository, RecommendationReportRepository, RecommendationRepository,
};

use crate::{
    infra::schedule::ReportScheduleRunner,
    report::{AdHocReportRequest, ReportLifecycleService, ReportTrigger},
};

/// Web-facing report port assembled from the report plane.
pub struct CoreQuantReportPort {
    report_repo: Arc<dyn RecommendationReportRepository>,
    recommendation_repo: Arc<dyn RecommendationRepository>,
    order_intent_repo: Arc<dyn OrderIntentRepository>,
    lifecycle: Arc<ReportLifecycleService>,
    scheduler: Arc<dyn ReportScheduleRunner>,
}

impl CoreQuantReportPort {
    /// Assemble the port from the report-plane services and read repositories.
    #[must_use]
    pub fn new(
        report_repo: Arc<dyn RecommendationReportRepository>,
        recommendation_repo: Arc<dyn RecommendationRepository>,
        order_intent_repo: Arc<dyn OrderIntentRepository>,
        lifecycle: Arc<ReportLifecycleService>,
        scheduler: Arc<dyn ReportScheduleRunner>,
    ) -> Self {
        Self {
            report_repo,
            recommendation_repo,
            order_intent_repo,
            lifecycle,
            scheduler,
        }
    }

    /// Project a recommendation into its outbound view, joining the parent
    /// report's lifecycle status and any blocking pre-submission intent.
    fn assemble_view(
        recommendation: RecommendationInfo,
        report_status: RecommendationReportStatus,
        active_order_intent_id: Option<OrderIntentId>,
    ) -> QuantRecommendationView {
        RecommendationViewContext {
            recommendation,
            report_status,
            active_order_intent_id,
        }
        .into()
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
    ) -> QuantResult<Vec<QuantRecommendationView>> {
        let recommendations = self.recommendation_repo.find_by_report(report_id).await?;
        if recommendations.is_empty() {
            return Ok(Vec::new());
        }
        // FK guarantees the report exists when it has recommendations; if it is
        // somehow absent, fall back to the persisted per-recommendation status.
        let Some(report) = self.report_repo.find_by_id(report_id).await? else {
            return Ok(Vec::new());
        };
        let blocking = self
            .order_intent_repo
            .find_blocking_by_report(report_id)
            .await?
            .into_iter()
            .map(|intent| (intent.recommendation_id, intent.order_intent_id))
            .collect::<HashMap<_, _>>();
        Ok(recommendations
            .into_iter()
            .map(|rec| {
                let active = blocking.get(&rec.recommendation_id).cloned();
                Self::assemble_view(rec, report.status, active)
            })
            .collect())
    }

    async fn find_recommendation(
        &self,
        recommendation_id: &RecommendationId,
    ) -> QuantResult<Option<QuantRecommendationView>> {
        let Some(recommendation) = self
            .recommendation_repo
            .find_by_id(recommendation_id)
            .await?
        else {
            return Ok(None);
        };
        let Some(report) = self
            .report_repo
            .find_by_id(&recommendation.recommendation_report_id)
            .await?
        else {
            return Ok(None);
        };
        let active_order_intent_id = self
            .order_intent_repo
            .find_active_by_recommendation(recommendation_id)
            .await?
            .map(|intent| intent.order_intent_id);
        Ok(Some(Self::assemble_view(
            recommendation,
            report.status,
            active_order_intent_id,
        )))
    }

    async fn find_evidence(
        &self,
        recommendation_id: &RecommendationId,
    ) -> QuantResult<Option<QuantEvidenceView>> {
        Ok(self
            .recommendation_repo
            .find_by_id(recommendation_id)
            .await?
            .map(QuantEvidenceView::from))
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

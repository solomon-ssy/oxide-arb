//! Read-only venue incentive reconciliation and event audit API.

use actix_web::{
    http::Method,
    web::{Data, Query},
};
use chrono::{Days, Utc};
use quant_pivot_models::{
    domain::{
        api::quant_incentive::{
            IncentiveReconciliationHealth, IncentiveReconciliationView,
            VenueIncentiveEventListQuery, VenueIncentiveEventView,
        },
        pagination::Paginated,
        quant::venue_incentive::VenueIncentiveScanHealth,
    },
    enums::rbac::{Operation, ResourceType},
    types::Usd,
};

use crate::{
    auth::casbin::Rule,
    error::WebError,
    response::WebResponse,
    routes::registry::{RouteSpec, spec},
    state::AppState,
};

pub(crate) fn route_specs() -> Vec<RouteSpec> {
    vec![
        spec(
            Method::GET,
            "/quant/incentives/reconciliation",
            Rule::ResourceOp(ResourceType::AccountSnapshot, Operation::Read),
            get_reconciliation,
        ),
        spec(
            Method::GET,
            "/quant/incentives/events",
            Rule::ResourceOp(ResourceType::AccountSnapshot, Operation::Read),
            list_events,
        ),
    ]
}

async fn get_reconciliation(
    state: Data<AppState>,
) -> Result<WebResponse<IncentiveReconciliationView>, WebError> {
    let now = Utc::now();
    let lookback_days = state.deploy.quant.workers.venue_incentive_lookback_days;
    let end = now
        .date_naive()
        .checked_sub_days(Days::new(1))
        .ok_or_else(|| WebError::Internal("incentive health date underflow".to_owned()))?;
    let from = end
        .checked_sub_days(Days::new(u64::from(lookback_days.saturating_sub(1))))
        .ok_or_else(|| WebError::Internal("incentive health lookback underflow".to_owned()))?;
    let reconciliation = state
        .venue_incentives
        .reconciliation_cumulative(&state.execution_account_id, now)
        .await?;
    let scans = state
        .venue_incentives
        .scans(&state.execution_account_id, from, end)
        .await?;
    let scan_health = VenueIncentiveScanHealth::project(&scans, from, end);
    let cadence_secs = state
        .deploy
        .quant
        .workers
        .venue_incentive_reconciliation_secs;
    let stale_after_secs =
        i64::try_from(cadence_secs.saturating_mul(2)).map_or(i64::MAX, |value| value);
    let is_stale = scan_health
        .last_success_at
        .is_some_and(|last| now.signed_duration_since(last).num_seconds() >= stale_after_secs);
    let health = if scan_health.last_success_at.is_none() {
        IncentiveReconciliationHealth::Unavailable
    } else if scan_health.incomplete_day_count > 0 {
        IncentiveReconciliationHealth::Incomplete
    } else if is_stale {
        IncentiveReconciliationHealth::Stale
    } else {
        IncentiveReconciliationHealth::Healthy
    };
    let award_outstanding =
        reconciliation.venue_awarded_maker_usd - reconciliation.wallet_credited_maker_usd;
    Ok(WebResponse::ok(IncentiveReconciliationView {
        as_of: reconciliation.as_of,
        estimated_maker_accrual_usd: reconciliation.estimated_maker_accrual_usd,
        venue_awarded_maker_usd: reconciliation.venue_awarded_maker_usd,
        wallet_credited_maker_usd: reconciliation.wallet_credited_maker_usd,
        wallet_credited_taker_usd: reconciliation.wallet_credited_taker_usd,
        estimate_to_award_delta_usd: reconciliation.estimate_to_award_delta(),
        award_to_credit_delta_usd: reconciliation.award_to_credit_delta(),
        last_success_at: scan_health.last_success_at,
        oldest_incomplete_date: scan_health.oldest_incomplete_date,
        incomplete_day_count: scan_health.incomplete_day_count,
        health,
        payout_threshold_usd: Usd::ONE,
        below_payout_threshold: award_outstanding.is_positive() && award_outstanding < Usd::ONE,
    }))
}

async fn list_events(
    state: Data<AppState>,
    query: Query<VenueIncentiveEventListQuery>,
) -> Result<WebResponse<Paginated<VenueIncentiveEventView>>, WebError> {
    let page = state
        .venue_incentives
        .page_events(&state.execution_account_id, query.into_inner())
        .await?
        .map(VenueIncentiveEventView::from);
    Ok(WebResponse::ok(page))
}

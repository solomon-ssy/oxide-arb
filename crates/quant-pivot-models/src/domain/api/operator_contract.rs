//! Schema root for the operator recommendation, execution, and account API.

use schemars::JsonSchema;

use super::{
    AccountRecoveryIncidentView, AccountSnapshotView, CreateIntentRequest, EquitySnapshotView,
    FinalizeAccountRecoveryRequest, LiveAccountView, OrderIntentView, QuantRecommendationView,
    QuantReportDetailView, QuantReportFunnelView, QuantReportView,
    RecommendationEconomicOutcomeView, RecommendationExecutionComparisonView,
    ReconcileAccountRecoveryRequest, ReportDiffView, ReportFunnelMarketView,
    RouteEconomicHealthView, SealAccountRecoveryRequest,
    quant_incentive::{IncentiveReconciliationView, VenueIncentiveEventView},
};

/// Schema-only envelope used to generate frontend wire types and boundary validators.
///
/// HTTP handlers never serialize this envelope. Every field points at the real
/// request or response DTO used by the handler so the SPA cannot maintain a
/// parallel operator contract.
#[derive(JsonSchema)]
#[schemars(deny_unknown_fields)]
pub struct QuantOperatorApiContractSchema {
    pub recommendation_response: QuantRecommendationView,
    pub report_list_row_response: QuantReportView,
    pub report_detail_response: QuantReportDetailView,
    pub report_diff_response: ReportDiffView,
    pub report_funnel_response: QuantReportFunnelView,
    pub report_funnel_market_response: ReportFunnelMarketView,
    pub create_intent_request: CreateIntentRequest,
    pub execution_confirmation_response: OrderIntentView,
    pub account_snapshot_response: AccountSnapshotView,
    pub live_account_response: LiveAccountView,
    pub equity_snapshot_response: EquitySnapshotView,
    pub incentive_reconciliation_response: IncentiveReconciliationView,
    pub incentive_event_response: VenueIncentiveEventView,
    pub recommendation_economic_outcome_response: RecommendationEconomicOutcomeView,
    pub recommendation_execution_comparison_response: RecommendationExecutionComparisonView,
    pub economic_health_response: RouteEconomicHealthView,
    pub account_recovery_incident_response: AccountRecoveryIncidentView,
    pub reconcile_account_recovery_request: ReconcileAccountRecoveryRequest,
    pub seal_account_recovery_request: SealAccountRecoveryRequest,
    pub finalize_account_recovery_request: FinalizeAccountRecoveryRequest,
}

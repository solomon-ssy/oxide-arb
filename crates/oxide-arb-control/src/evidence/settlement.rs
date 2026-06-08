use chrono::{DateTime, TimeZone, Utc};
use oxide_arb_models::{
    clickhouse::OpportunityAuditRow,
    domain::{
        control_factor::{EvidenceSourceBundle, QueryFingerprint},
        evidence::EvidenceMetric,
    },
    enums::clickhouse::{
        ChOpportunityAuditStage, ChSettlementAccountingStatus, ChSettlementOutcome,
    },
    enums::common::{RedeemStatus, SettlementAccountingStatus},
    enums::risk::ReconciliationStatus,
    types::OpportunityId,
};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SettlementReconciliationEvidenceArtifact {
    pub report: SettlementReconciliationEvidenceReport,
    pub missing_joins: Vec<MissingSettlementJoin>,
    pub settled_opportunity_ids: Vec<OpportunityId>,
    pub settled_opportunities: Vec<SettledOpportunityRef>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SettlementReconciliationEvidenceReport {
    pub settled_trade_count: u64,
    pub unsettled_trade_count: EvidenceMetric<u64>,
    pub won_count: u64,
    pub lost_count: u64,
    pub payout_usd_sum: EvidenceMetric<String>,
    pub realized_pnl_usd_sum: EvidenceMetric<String>,
    pub settlement_delay_p50_ms: EvidenceMetric<u64>,
    pub settlement_delay_p95_ms: EvidenceMetric<u64>,
    pub redeem_pending_count: u64,
    pub redeem_failed_count: u64,
    pub cash_drift_usd: EvidenceMetric<String>,
    pub critical_drift_count: EvidenceMetric<u64>,
    pub metrics_stale_secs: EvidenceMetric<u64>,
    pub query_fingerprints: Vec<QueryFingerprint>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct MissingSettlementJoin {
    pub opportunity_id: OpportunityId,
    pub field: String,
    pub reason: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SettledOpportunityRef {
    pub opportunity_id: OpportunityId,
    pub settled_at: DateTime<Utc>,
}

#[must_use]
pub fn build(
    audits: &[OpportunityAuditRow],
    query_fingerprints: Vec<QueryFingerprint>,
    source_bundle: &EvidenceSourceBundle,
    as_of: DateTime<Utc>,
) -> SettlementReconciliationEvidenceArtifact {
    let settlement_rows = audits
        .iter()
        .filter(|row| row.stage == ChOpportunityAuditStage::Settled)
        .collect::<Vec<_>>();
    let mut accumulator = SettlementAccumulator::default();
    for row in &settlement_rows {
        accumulator.observe(row);
        accumulator.record_pg_joins(row, source_bundle);
    }

    SettlementReconciliationEvidenceArtifact {
        report: SettlementReconciliationEvidenceReport {
            settled_trade_count: u64::try_from(settlement_rows.len()).unwrap_or(u64::MAX),
            unsettled_trade_count: EvidenceMetric::Available {
                value: u64::try_from(source_bundle.trades.len())
                    .unwrap_or(u64::MAX)
                    .saturating_sub(u64::try_from(settlement_rows.len()).unwrap_or(u64::MAX)),
            },
            won_count: accumulator.won_count,
            lost_count: accumulator.lost_count,
            payout_usd_sum: complete_decimal_metric(
                accumulator.payout_complete,
                accumulator.payout_sum,
                "settlement.payout_missing",
                "one or more settlement rows are missing payout_usd",
            ),
            realized_pnl_usd_sum: complete_decimal_metric(
                accumulator.realized_pnl_complete,
                accumulator.realized_pnl_sum,
                "settlement.realized_pnl_missing",
                "one or more settlement rows are missing realized_pnl_usd",
            ),
            settlement_delay_p50_ms: percentile_metric(&accumulator.settlement_delays, 50),
            settlement_delay_p95_ms: percentile_metric(&accumulator.settlement_delays, 95),
            redeem_pending_count: accumulator.redeem_pending_count,
            redeem_failed_count: accumulator.redeem_failed_count,
            cash_drift_usd: source_bundle.balance_snapshot.as_ref().map_or_else(
                || EvidenceMetric::Unavailable {
                    code: "reconciliation.cash_drift_source_missing".to_owned(),
                    reason: "cash drift requires balance snapshot inputs".to_owned(),
                },
                |snapshot| EvidenceMetric::Available {
                    value: snapshot.drift_usd.to_string(),
                },
            ),
            critical_drift_count: EvidenceMetric::Available {
                value: source_bundle
                    .reconciliation_reports
                    .iter()
                    .filter(|report| report.status == ReconciliationStatus::Critical)
                    .count()
                    .try_into()
                    .unwrap_or(u64::MAX),
            },
            metrics_stale_secs: source_bundle.reconciliation_reports.last().map_or_else(
                || EvidenceMetric::Unavailable {
                    code: "reconciliation.metrics_freshness_source_missing".to_owned(),
                    reason: "metrics freshness requires reconciliation status inputs".to_owned(),
                },
                |report| {
                    let stale_secs = as_of
                        .signed_duration_since(report.checked_at)
                        .num_seconds()
                        .max(0)
                        .try_into()
                        .unwrap_or(u64::MAX);
                    EvidenceMetric::Available { value: stale_secs }
                },
            ),
            query_fingerprints,
        },
        missing_joins: accumulator.missing_joins,
        settled_opportunity_ids: settlement_rows
            .iter()
            .map(|row| row.opportunity_id.clone())
            .collect(),
        settled_opportunities: settlement_rows
            .iter()
            .filter_map(|row| {
                Utc.timestamp_millis_opt(row.stage_at)
                    .single()
                    .map(|settled_at| SettledOpportunityRef {
                        opportunity_id: row.opportunity_id.clone(),
                        settled_at,
                    })
            })
            .collect(),
    }
}

#[derive(Debug)]
struct SettlementAccumulator {
    missing_joins: Vec<MissingSettlementJoin>,
    won_count: u64,
    lost_count: u64,
    redeem_pending_count: u64,
    redeem_failed_count: u64,
    payout_sum: rust_decimal::Decimal,
    realized_pnl_sum: rust_decimal::Decimal,
    settlement_delays: Vec<u64>,
    payout_complete: bool,
    realized_pnl_complete: bool,
}

impl Default for SettlementAccumulator {
    fn default() -> Self {
        Self {
            missing_joins: Vec::new(),
            won_count: 0,
            lost_count: 0,
            redeem_pending_count: 0,
            redeem_failed_count: 0,
            payout_sum: rust_decimal::Decimal::ZERO,
            realized_pnl_sum: rust_decimal::Decimal::ZERO,
            settlement_delays: Vec::new(),
            payout_complete: true,
            realized_pnl_complete: true,
        }
    }
}

impl SettlementAccumulator {
    fn observe(&mut self, row: &OpportunityAuditRow) {
        self.record_required_joins(row);
        self.record_outcome(row);
        self.record_payout(row);
        self.record_realized_pnl(row);
        self.record_delay(row);
        self.record_accounting(row);
    }

    fn record_required_joins(&mut self, row: &OpportunityAuditRow) {
        if row.trade_id.is_none() {
            self.missing_joins.push(MissingSettlementJoin {
                opportunity_id: row.opportunity_id.clone(),
                field: "trade_id".to_owned(),
                reason: "settlement row cannot join trade".to_owned(),
            });
        }
        if row.winning_token_id.is_none() {
            self.missing_joins.push(MissingSettlementJoin {
                opportunity_id: row.opportunity_id.clone(),
                field: "winning_token_id".to_owned(),
                reason: "settlement row cannot attribute winning side".to_owned(),
            });
        }
        if row.scored_snapshot_json.is_none() {
            self.missing_joins.push(MissingSettlementJoin {
                opportunity_id: row.opportunity_id.clone(),
                field: "scored_snapshot_json".to_owned(),
                reason: "settlement row cannot recover detector snapshot".to_owned(),
            });
        }
    }

    fn record_outcome(&mut self, row: &OpportunityAuditRow) {
        match row.settlement_status {
            Some(ChSettlementOutcome::Won) => self.won_count = self.won_count.saturating_add(1),
            Some(ChSettlementOutcome::Lost) => self.lost_count = self.lost_count.saturating_add(1),
            None => self.missing_joins.push(MissingSettlementJoin {
                opportunity_id: row.opportunity_id.clone(),
                field: "settlement_status".to_owned(),
                reason: "settlement outcome is unavailable".to_owned(),
            }),
        }
    }

    fn record_payout(&mut self, row: &OpportunityAuditRow) {
        if let Some(value) = row.payout_usd {
            self.payout_sum += value.to_usd().inner();
        } else {
            self.payout_complete = false;
            self.missing_joins.push(MissingSettlementJoin {
                opportunity_id: row.opportunity_id.clone(),
                field: "payout_usd".to_owned(),
                reason: "settlement payout is unavailable".to_owned(),
            });
        }
    }

    fn record_realized_pnl(&mut self, row: &OpportunityAuditRow) {
        if let Some(value) = row.realized_pnl_usd {
            self.realized_pnl_sum += value.to_usd().inner();
        } else {
            self.realized_pnl_complete = false;
            self.missing_joins.push(MissingSettlementJoin {
                opportunity_id: row.opportunity_id.clone(),
                field: "realized_pnl_usd".to_owned(),
                reason: "settlement realized PnL is unavailable".to_owned(),
            });
        }
    }

    fn record_delay(&mut self, row: &OpportunityAuditRow) {
        if row.stage_at >= row.detected_at {
            self.settlement_delays
                .push(u64::try_from(row.stage_at - row.detected_at).unwrap_or(u64::MAX));
        } else {
            self.missing_joins.push(MissingSettlementJoin {
                opportunity_id: row.opportunity_id.clone(),
                field: "settlement_delay".to_owned(),
                reason: "settlement timestamp precedes detection timestamp".to_owned(),
            });
        }
    }

    fn record_accounting(&mut self, row: &OpportunityAuditRow) {
        match row.accounting_status {
            Some(ChSettlementAccountingStatus::Pending) => {
                self.redeem_pending_count = self.redeem_pending_count.saturating_add(1);
            }
            Some(ChSettlementAccountingStatus::Failed) => {
                self.redeem_failed_count = self.redeem_failed_count.saturating_add(1);
            }
            Some(
                ChSettlementAccountingStatus::Redeemed | ChSettlementAccountingStatus::Accounted,
            ) => {}
            None => self.missing_joins.push(MissingSettlementJoin {
                opportunity_id: row.opportunity_id.clone(),
                field: "accounting_status".to_owned(),
                reason: "redeem/accounting status is unavailable".to_owned(),
            }),
        }
    }

    fn record_pg_joins(&mut self, row: &OpportunityAuditRow, source_bundle: &EvidenceSourceBundle) {
        if let Some(trade_id) = row.trade_id.as_ref() {
            if let Some(position) = source_bundle
                .positions
                .iter()
                .find(|position| &position.trade_id == trade_id)
            {
                if position.winning_token_id.is_none() {
                    self.missing_joins.push(MissingSettlementJoin {
                        opportunity_id: row.opportunity_id.clone(),
                        field: "position.winning_token_id".to_owned(),
                        reason: "PG position cannot attribute winning token".to_owned(),
                    });
                }
                if matches!(position.redeem_status, RedeemStatus::Pending) {
                    self.missing_joins.push(MissingSettlementJoin {
                        opportunity_id: row.opportunity_id.clone(),
                        field: "position.redeem_status".to_owned(),
                        reason: "PG position redeem lifecycle is still pending".to_owned(),
                    });
                }
                if !matches!(
                    position.settlement_accounting_status,
                    SettlementAccountingStatus::Redeemed | SettlementAccountingStatus::Accounted
                ) {
                    self.missing_joins.push(MissingSettlementJoin {
                        opportunity_id: row.opportunity_id.clone(),
                        field: "position.accounting_status".to_owned(),
                        reason: "PG position redeem/accounting lifecycle is incomplete".to_owned(),
                    });
                }
            } else {
                self.missing_joins.push(MissingSettlementJoin {
                    opportunity_id: row.opportunity_id.clone(),
                    field: "position_id".to_owned(),
                    reason: "settlement row trade_id cannot join a PG position".to_owned(),
                });
            }
        }
        if source_bundle.balance_snapshot.is_none() {
            self.missing_joins.push(MissingSettlementJoin {
                opportunity_id: row.opportunity_id.clone(),
                field: "balance_snapshot".to_owned(),
                reason: "settlement reconciliation requires a PIT balance snapshot".to_owned(),
            });
        }
        if !source_bundle
            .settlement_truth
            .iter()
            .any(|truth| truth.market_id == row.market_id)
        {
            self.missing_joins.push(MissingSettlementJoin {
                opportunity_id: row.opportunity_id.clone(),
                field: "settlement_truth".to_owned(),
                reason: "settlement row market_id cannot join settlement truth".to_owned(),
            });
        }
    }
}

fn complete_decimal_metric(
    complete: bool,
    value: rust_decimal::Decimal,
    code: &str,
    reason: &str,
) -> EvidenceMetric<String> {
    if complete {
        EvidenceMetric::Available {
            value: value.to_string(),
        }
    } else {
        EvidenceMetric::Unavailable {
            code: code.to_owned(),
            reason: reason.to_owned(),
        }
    }
}

fn percentile_metric(values: &[u64], pct: usize) -> EvidenceMetric<u64> {
    if values.is_empty() {
        return EvidenceMetric::Unavailable {
            code: "settlement.delay_unavailable".to_owned(),
            reason: "no valid settlement delays were available".to_owned(),
        };
    }
    let mut sorted = values.to_vec();
    sorted.sort_unstable();
    let idx = sorted
        .len()
        .saturating_sub(1)
        .saturating_mul(pct)
        .saturating_div(100);
    EvidenceMetric::Available { value: sorted[idx] }
}

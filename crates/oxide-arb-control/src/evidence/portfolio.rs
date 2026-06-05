use num_traits::ToPrimitive;
use oxide_arb_models::{
    clickhouse::OpportunityAuditRow,
    domain::{
        control_factor::{EvidenceSourceBundle, QueryFingerprint},
        evidence::EvidenceMetric,
    },
    enums::clickhouse::ChOpportunityAuditStage,
    enums::risk::RiskAuditEventType,
    types::Usd,
};
use serde::{Deserialize, Serialize};

use crate::materialization::ArtifactHasher;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PortfolioRiskEvidenceArtifact {
    pub report: PortfolioRiskEvidenceReport,
    pub sequence_complete: bool,
    pub sequence_hash: Option<String>,
    pub sequence_events: Vec<PortfolioSequenceEvent>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PortfolioRiskEvidenceReport {
    pub peak_reserved_usd: EvidenceMetric<String>,
    pub peak_potential_loss_usd: EvidenceMetric<String>,
    pub peak_total_exposure_usd: EvidenceMetric<String>,
    pub peak_open_positions: EvidenceMetric<u64>,
    pub max_drawdown_pct_bps: EvidenceMetric<u64>,
    pub loss_streak_max: EvidenceMetric<u64>,
    pub risk_denial_count: u64,
    pub sizing_denial_count: u64,
    pub settlement_backlog_max: EvidenceMetric<u64>,
    pub stale_metrics_window_ms: EvidenceMetric<u64>,
    pub insufficient_reasons: Vec<String>,
    pub query_fingerprints: Vec<QueryFingerprint>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PortfolioSequenceEvent {
    pub event_time_ms: i64,
    pub source_priority: u8,
    pub persisted_id: String,
    pub event_type: PortfolioSequenceEventType,
    pub opportunity_id: Option<String>,
    pub trade_id: Option<String>,
    pub binding_constraint: Option<String>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", tag = "kind", content = "value")]
pub enum PortfolioSequenceEventType {
    Audit(ChOpportunityAuditStage),
    RiskAudit(RiskAuditEventType),
    Trade,
    Position,
    PotentialLossBaseline,
    PotentialLossChange,
}

#[must_use]
pub fn build(
    audits: &[OpportunityAuditRow],
    query_fingerprints: Vec<QueryFingerprint>,
    source_bundle: &EvidenceSourceBundle,
) -> PortfolioRiskEvidenceArtifact {
    let sequence_events = portfolio_sequence_events(audits, source_bundle);
    let (risk_denial_count, sizing_denial_count) = portfolio_denial_counts(audits, source_bundle);
    let total_exposure = portfolio_total_exposure(source_bundle);
    let peak_potential_loss = portfolio_peak_potential_loss(source_bundle);
    let (sequence_complete, sequence_hash, insufficient_reasons) =
        portfolio_sequence_metadata(&sequence_events, source_bundle);
    PortfolioRiskEvidenceArtifact {
        report: portfolio_risk_report(
            source_bundle,
            risk_denial_count,
            sizing_denial_count,
            peak_potential_loss,
            total_exposure,
            insufficient_reasons,
            query_fingerprints,
        ),
        sequence_complete,
        sequence_hash,
        sequence_events,
    }
}

fn portfolio_denial_counts(
    audits: &[OpportunityAuditRow],
    source_bundle: &EvidenceSourceBundle,
) -> (u64, u64) {
    let risk_denial_count = ToPrimitive::to_u64(
        &audits
            .iter()
            .filter(|row| row.stage == ChOpportunityAuditStage::RiskRejected)
            .count()
            .saturating_add(
                source_bundle
                    .risk_audit_events
                    .iter()
                    .filter(|event| event.event_type == RiskAuditEventType::TradeDenied)
                    .count(),
            ),
    )
    .unwrap_or(u64::MAX);
    let sizing_denial_count = ToPrimitive::to_u64(
        &audits
            .iter()
            .filter(|row| row.stage == ChOpportunityAuditStage::SizingRejected)
            .count(),
    )
    .unwrap_or(u64::MAX);
    (risk_denial_count, sizing_denial_count)
}

fn portfolio_total_exposure(source_bundle: &EvidenceSourceBundle) -> Usd {
    source_bundle
        .positions
        .iter()
        .map(|position| position.total_cost_usd + position.total_fees_usd)
        .fold(Usd::ZERO, |acc, value| acc + value)
}

fn portfolio_peak_potential_loss(source_bundle: &EvidenceSourceBundle) -> Usd {
    source_bundle
        .potential_loss_baseline
        .iter()
        .chain(source_bundle.potential_loss_changes.iter())
        .map(|entry| entry.max_loss_usd)
        .sum()
}

fn portfolio_sequence_metadata(
    sequence_events: &Vec<PortfolioSequenceEvent>,
    source_bundle: &EvidenceSourceBundle,
) -> (bool, Option<String>, Vec<String>) {
    let binding_constraints_complete = sequence_events.iter().all(|event| {
        !matches!(
            event.event_type,
            PortfolioSequenceEventType::Audit(ChOpportunityAuditStage::SizingRejected)
        ) || event.binding_constraint.is_some()
    });
    let sequence_complete = !sequence_events.is_empty()
        && !source_bundle.trades.is_empty()
        && !source_bundle.positions.is_empty()
        && !source_bundle.risk_audit_events.is_empty()
        && !source_bundle.potential_loss_baseline.is_empty()
        && source_bundle.balance_snapshot.is_some()
        && binding_constraints_complete;
    let insufficient_reasons = insufficient_reasons(
        sequence_complete,
        binding_constraints_complete,
        source_bundle,
    );
    let sequence_hash = if sequence_complete {
        ArtifactHasher::compute(sequence_events)
            .ok()
            .map(|hash| hash.0)
    } else {
        None
    };
    (sequence_complete, sequence_hash, insufficient_reasons)
}

fn portfolio_risk_report(
    source_bundle: &EvidenceSourceBundle,
    risk_denial_count: u64,
    sizing_denial_count: u64,
    peak_potential_loss: Usd,
    total_exposure: Usd,
    insufficient_reasons: Vec<String>,
    query_fingerprints: Vec<QueryFingerprint>,
) -> PortfolioRiskEvidenceReport {
    PortfolioRiskEvidenceReport {
        peak_reserved_usd: source_bundle.balance_snapshot.as_ref().map_or_else(
            || EvidenceMetric::Unavailable {
                code: "risk.balance_snapshot_missing".to_owned(),
                reason: "peak reserved capital requires PIT balance snapshots".to_owned(),
            },
            |snapshot| EvidenceMetric::Available {
                value: snapshot.internal_reserved_usd.to_string(),
            },
        ),
        peak_potential_loss_usd: if source_bundle.potential_loss_baseline.is_empty()
            && source_bundle.potential_loss_changes.is_empty()
        {
            EvidenceMetric::Unavailable {
                code: "risk.potential_loss_timeline_missing".to_owned(),
                reason: "peak potential loss requires historical potential-loss ledger events"
                    .to_owned(),
            }
        } else {
            EvidenceMetric::Available {
                value: peak_potential_loss.to_string(),
            }
        },
        peak_total_exposure_usd: EvidenceMetric::Available {
            value: total_exposure.to_string(),
        },
        peak_open_positions: EvidenceMetric::Available {
            value: ToPrimitive::to_u64(&source_bundle.positions.len()).unwrap_or(u64::MAX),
        },
        max_drawdown_pct_bps: EvidenceMetric::Unavailable {
            code: "risk.equity_timeline_missing".to_owned(),
            reason: "drawdown requires historical equity snapshots".to_owned(),
        },
        loss_streak_max: EvidenceMetric::Unavailable {
            code: "risk.settlement_sequence_missing".to_owned(),
            reason: "loss streak requires complete settlement outcome sequence".to_owned(),
        },
        risk_denial_count,
        sizing_denial_count,
        settlement_backlog_max: EvidenceMetric::Unavailable {
            code: "settlement.backlog_timeline_missing".to_owned(),
            reason: "settlement backlog requires persisted settlement lifecycle events".to_owned(),
        },
        stale_metrics_window_ms: EvidenceMetric::Unavailable {
            code: "risk.metrics_freshness_timeline_missing".to_owned(),
            reason: "stale metrics window requires historical risk metric snapshots".to_owned(),
        },
        insufficient_reasons,
        query_fingerprints,
    }
}

fn insufficient_reasons(
    sequence_complete: bool,
    binding_constraints_complete: bool,
    source_bundle: &EvidenceSourceBundle,
) -> Vec<String> {
    if sequence_complete {
        return Vec::new();
    }
    let mut reasons = Vec::new();
    if source_bundle.trades.is_empty() {
        reasons.push("risk.sequence_incomplete: PG trades are missing".to_owned());
    }
    if source_bundle.positions.is_empty() {
        reasons.push("risk.sequence_incomplete: PG positions are missing".to_owned());
    }
    if source_bundle.risk_audit_events.is_empty() {
        reasons.push("risk.sequence_incomplete: risk audit events are missing".to_owned());
    }
    if source_bundle.potential_loss_baseline.is_empty() {
        reasons.push("risk.sequence_incomplete: potential-loss baseline is missing".to_owned());
    }
    if source_bundle.balance_snapshot.is_none() {
        reasons.push("risk.sequence_incomplete: balance snapshot is missing".to_owned());
    }
    if !binding_constraints_complete {
        reasons.push(
            "risk.sequence_incomplete: sizing decisions require binding constraints".to_owned(),
        );
    }
    reasons
}

fn portfolio_sequence_events(
    audits: &[OpportunityAuditRow],
    source_bundle: &EvidenceSourceBundle,
) -> Vec<PortfolioSequenceEvent> {
    let mut events = Vec::new();
    events.extend(audits.iter().map(|row| PortfolioSequenceEvent {
        event_time_ms: row.stage_at,
        source_priority: audit_source_priority(row.stage),
        persisted_id: format!(
            "{}:{}:{}",
            row.opportunity_id, row.stage_order, row.sequence
        ),
        event_type: PortfolioSequenceEventType::Audit(row.stage),
        opportunity_id: Some(row.opportunity_id.to_string()),
        trade_id: row.trade_id.as_ref().map(ToString::to_string),
        binding_constraint: row.rejection_reason.clone(),
    }));
    events.extend(
        source_bundle
            .trades
            .iter()
            .map(|trade| PortfolioSequenceEvent {
                event_time_ms: trade.created_at.timestamp_millis(),
                source_priority: 70,
                persisted_id: trade.trade_id.to_string(),
                event_type: PortfolioSequenceEventType::Trade,
                opportunity_id: Some(trade.opportunity_id.to_string()),
                trade_id: Some(trade.trade_id.to_string()),
                binding_constraint: None,
            }),
    );
    events.extend(
        source_bundle
            .positions
            .iter()
            .map(|position| PortfolioSequenceEvent {
                event_time_ms: position.opened_at.timestamp_millis(),
                source_priority: 80,
                persisted_id: position.position_id.to_string(),
                event_type: PortfolioSequenceEventType::Position,
                opportunity_id: None,
                trade_id: Some(position.trade_id.to_string()),
                binding_constraint: None,
            }),
    );
    events.extend(source_bundle.risk_audit_events.iter().map(|event| {
        PortfolioSequenceEvent {
            event_time_ms: event.created_at.timestamp_millis(),
            source_priority: 35,
            persisted_id: event.id.to_string(),
            event_type: PortfolioSequenceEventType::RiskAudit(event.event_type),
            opportunity_id: event.opportunity_id.as_ref().map(ToString::to_string),
            trade_id: event.trade_id.as_ref().map(ToString::to_string),
            binding_constraint: event
                .payload
                .get("binding_constraint")
                .and_then(serde_json::Value::as_str)
                .map(str::to_owned),
        }
    }));
    events.extend(source_bundle.potential_loss_baseline.iter().map(|entry| {
        PortfolioSequenceEvent {
            event_time_ms: entry.created_at.timestamp_millis(),
            source_priority: 90,
            persisted_id: entry.ledger_id.to_string(),
            event_type: PortfolioSequenceEventType::PotentialLossBaseline,
            opportunity_id: None,
            trade_id: None,
            binding_constraint: None,
        }
    }));
    events.extend(source_bundle.potential_loss_changes.iter().map(|entry| {
        PortfolioSequenceEvent {
            event_time_ms: entry.created_at.timestamp_millis(),
            source_priority: 91,
            persisted_id: entry.ledger_id.to_string(),
            event_type: PortfolioSequenceEventType::PotentialLossChange,
            opportunity_id: None,
            trade_id: None,
            binding_constraint: None,
        }
    }));
    events.sort_by(|left, right| {
        left.event_time_ms
            .cmp(&right.event_time_ms)
            .then(left.source_priority.cmp(&right.source_priority))
            .then(left.persisted_id.cmp(&right.persisted_id))
    });
    events
}

const fn audit_source_priority(stage: ChOpportunityAuditStage) -> u8 {
    match stage {
        ChOpportunityAuditStage::Detected => 10,
        ChOpportunityAuditStage::ValidationRejected => 20,
        ChOpportunityAuditStage::FactorValidationRejected => 25,
        ChOpportunityAuditStage::RiskRejected => 30,
        ChOpportunityAuditStage::SizingRejected => 40,
        ChOpportunityAuditStage::Filled
        | ChOpportunityAuditStage::Missed
        | ChOpportunityAuditStage::Failed => 70,
        ChOpportunityAuditStage::Settled => 100,
    }
}

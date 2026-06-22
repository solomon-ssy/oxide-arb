//! Evidence ladder for unknown FOK venue outcomes (defer-only when inconclusive).

use chrono::{DateTime, Duration, Utc};
use oxide_arb_api::clob::ClobTrade;
use oxide_arb_models::{
    domain::trade::TradeInfo,
    runtime_config::ReconciliationConfig,
    types::{OrderId, Price, Shares},
};
use rust_decimal::Decimal;

/// Outcome of evaluating reconciliation evidence for one trade.
#[derive(Debug, Clone)]
pub enum EvidenceVerdict {
    /// Proven fill with venue economics.
    Filled {
        shares: Shares,
        price: Price,
        order_id: Option<OrderId>,
        tx_hash: Option<String>,
        clob_trade: Option<Box<ClobTrade>>,
        note: String,
    },
    /// Proven miss — dispositive negative evidence only.
    Miss { note: String },
    /// Insufficient evidence — worker must defer with backoff.
    Defer { note: String },
}

/// Evaluate the evidence ladder L1–L5 for one orphaned trade.
#[must_use]
pub fn evaluate_evidence_ladder(
    trade: &TradeInfo,
    clob_trades: &[ClobTrade],
    ctf_balance_now: Shares,
    competing_pending: bool,
    config: &ReconciliationConfig,
    now: DateTime<Utc>,
) -> EvidenceVerdict {
    // L1: exact order_id match in CLOB history.
    if let Some(order_id) = trade.order_id.as_ref() {
        if let Some(clob_trade) = clob_trades
            .iter()
            .find(|item| &item.order_id == order_id && item.size.is_positive())
        {
            return EvidenceVerdict::Filled {
                shares: clob_trade.size,
                price: clob_trade.price,
                order_id: Some(clob_trade.order_id.clone()),
                tx_hash: Some(clob_trade.tx_hash.clone()),
                clob_trade: Some(Box::new(clob_trade.clone())),
                note: "L1: order_id exact match in CLOB trades".to_owned(),
            };
        }
    }

    // L2: time-window + side + exact size match without competing pending trades.
    if let Some(submitted_at) = trade.submitted_at {
        let window_start = submitted_at - Duration::seconds(config.trade_lookback_secs);
        if !competing_pending {
            if let Some(clob_trade) = clob_trades.iter().find(|item| {
                item.side == trade.side
                    && item.matched_at >= window_start
                    && item.matched_at <= now
                    && shares_equal_lot(item.size, trade.shares)
            }) {
                return EvidenceVerdict::Filled {
                    shares: clob_trade.size,
                    price: clob_trade.price,
                    order_id: Some(clob_trade.order_id.clone()),
                    tx_hash: Some(clob_trade.tx_hash.clone()),
                    clob_trade: Some(Box::new(clob_trade.clone())),
                    note: "L2: CLOB trade window match with exact plan shares".to_owned(),
                };
            }
        }
    }

    // L3: CTF balance delta since pre-submit snapshot.
    if let Some(pre_balance) = trade.pre_submit_ctf_balance {
        let delta = ctf_balance_now - pre_balance;
        let min_shares = trade.shares * config.min_fill_ratio;
        if delta >= min_shares && delta.is_positive() {
            return EvidenceVerdict::Filled {
                shares: delta,
                price: trade.price,
                order_id: trade.order_id.clone(),
                tx_hash: trade.tx_hash.clone(),
                clob_trade: None,
                note: format!("L3: CTF delta {delta} >= min fill {min_shares}"),
            };
        }
    }

    // L4: dispositive negative — no CLOB match, zero delta, min age elapsed.
    if let Some(submitted_at) = trade.submitted_at {
        let min_miss_age_secs = i64::try_from(config.min_miss_age_secs).unwrap_or(i64::MAX);
        let min_miss_age = Duration::seconds(min_miss_age_secs);
        if now >= submitted_at + min_miss_age {
            let zero_delta = trade
                .pre_submit_ctf_balance
                .map_or(Shares::ZERO, |pre| ctf_balance_now - pre)
                .inner()
                == Decimal::ZERO;
            let no_clob_match = !clob_trades.iter().any(|item| {
                item.side == trade.side
                    && item.size.is_positive()
                    && order_matches_trade(item, trade)
            });
            if zero_delta && no_clob_match {
                return EvidenceVerdict::Miss {
                    note: "L4: no CLOB match, zero CTF delta, min miss age elapsed".to_owned(),
                };
            }
        }
    }

    // L5: defer — never blind-miss.
    EvidenceVerdict::Defer {
        note: "L5: insufficient evidence — defer with backoff".to_owned(),
    }
}

fn order_matches_trade(clob_trade: &ClobTrade, trade: &TradeInfo) -> bool {
    trade
        .order_id
        .as_ref()
        .is_some_and(|id| &clob_trade.order_id == id)
        || (clob_trade.side == trade.side && shares_equal_lot(clob_trade.size, trade.shares))
}

fn shares_equal_lot(a: Shares, b: Shares) -> bool {
    a.inner() == b.inner()
}

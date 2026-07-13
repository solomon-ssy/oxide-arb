//! Canonical on-chain maker-side participant concentration metrics (Gini / HHI / CR1).
//!
//! Single source of truth for [`crate::features::structural`] and the structural
//! monitor port — never duplicate formulas elsewhere.

use std::collections::BTreeMap;

use quant_pivot_models::domain::{TradeParticipantRole, TradeTapePrint, TradeTapeSourceKind};
use rust_decimal::Decimal;

/// Gate thresholds before concentration metrics are scored.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ParticipantConcentrationGate {
    pub min_unique_participants: u64,
    pub min_notional_usd: Decimal,
    pub min_coverage_ratio: Decimal,
}

/// Scored concentration snapshot over a trade-tape window.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ParticipantConcentrationSnapshot {
    pub observed_print_count: u64,
    pub eligible_print_count: u64,
    pub unique_participants: u64,
    pub total_notional_usd: Decimal,
    pub coverage_ratio: Decimal,
    pub gini: Decimal,
    pub hhi: Decimal,
    pub cr1_share: Decimal,
}

/// Role-specific Gini when enough unique addresses exist for that role.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ParticipantRoleMetrics {
    pub gini: Decimal,
}

/// Why concentration metrics are unavailable.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ConcentrationMissing {
    TradeTapeUnavailable,
    InsufficientTradeTape,
    InsufficientRoleCoverage,
}

impl ConcentrationMissing {
    #[must_use]
    pub const fn monitor_wire(self) -> &'static str {
        match self {
            Self::TradeTapeUnavailable => "trade_tape_unavailable",
            Self::InsufficientTradeTape => "insufficient_trade_tape",
            Self::InsufficientRoleCoverage => "insufficient_role_coverage",
        }
    }
}

/// Composite weights for participant concentration (Gini / CR1 / HHI).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ConcentrationCompositeWeights {
    pub gini: Decimal,
    pub cr1_share: Decimal,
    pub hhi: Decimal,
}

/// Weighted composite of concentration metrics, normalized by the weight sum.
///
/// Shared by the structural factor plane and the Structural Alpha monitor so
/// ranking and scoring use identical raw values.
#[must_use]
pub fn composite_concentration(
    gini: Decimal,
    cr1_share: Decimal,
    hhi: Decimal,
    weights: &ConcentrationCompositeWeights,
) -> Option<Decimal> {
    let weight_sum = weights.gini + weights.cr1_share + weights.hhi;
    if weight_sum <= Decimal::ZERO {
        return None;
    }
    Some(
        ((gini * weights.gini) + (cr1_share * weights.cr1_share) + (hhi * weights.hhi))
            / weight_sum,
    )
    .map(|value| value.round_dp(12))
}

/// On-chain maker rows with a non-empty address and positive notional.
#[must_use]
pub fn eligible_maker_prints<'a>(prints: &[&'a TradeTapePrint]) -> Vec<&'a TradeTapePrint> {
    prints
        .iter()
        .filter(|&&print| {
            print.source == TradeTapeSourceKind::OnChain
                && print.participant_role == TradeParticipantRole::Maker
                && !print.participant_address.is_empty()
                && print.participant_notional() > Decimal::ZERO
        })
        .copied()
        .collect()
}

/// Notional-weighted Gini coefficient over participant notionals.
#[must_use]
pub fn gini(values: impl IntoIterator<Item = Decimal>) -> Option<Decimal> {
    let mut values: Vec<Decimal> = values
        .into_iter()
        .filter(|value| *value > Decimal::ZERO)
        .collect();
    if values.is_empty() {
        return None;
    }
    if values.len() == 1 {
        return Some(Decimal::ZERO);
    }
    values.sort();
    let total: Decimal = values.iter().copied().sum();
    if total <= Decimal::ZERO {
        return None;
    }
    let n = i64::try_from(values.len()).ok()?;
    let numerator = values
        .iter()
        .enumerate()
        .fold(Decimal::ZERO, |acc, (idx, value)| {
            let rank = i64::try_from(idx + 1).unwrap_or(i64::MAX);
            acc + Decimal::from(2 * rank - n - 1) * *value
        });
    Some((numerator / (Decimal::from(n) * total)).round_dp(12))
}

/// Herfindahl-Hirschman index (sum of squared notional shares).
#[must_use]
pub fn hhi(values: impl IntoIterator<Item = Decimal>) -> Option<Decimal> {
    let values: Vec<Decimal> = values
        .into_iter()
        .filter(|value| *value > Decimal::ZERO)
        .collect();
    let total: Decimal = values.iter().copied().sum();
    if total <= Decimal::ZERO {
        return None;
    }
    Some(
        values
            .into_iter()
            .map(|value| {
                let share = value / total;
                share * share
            })
            .sum::<Decimal>()
            .round_dp(12),
    )
}

/// Largest single-participant notional share (CR1).
#[must_use]
pub fn cr1_share(values: impl IntoIterator<Item = Decimal>) -> Option<Decimal> {
    let mut values: Vec<Decimal> = values
        .into_iter()
        .filter(|value| *value > Decimal::ZERO)
        .collect();
    let total: Decimal = values.iter().copied().sum();
    if total <= Decimal::ZERO {
        return None;
    }
    values.sort_by(|left, right| right.cmp(left));
    Some((values[0] / total).round_dp(12))
}

fn participant_notionals(prints: &[&TradeTapePrint]) -> BTreeMap<String, Decimal> {
    let mut by_participant = BTreeMap::new();
    for print in prints {
        *by_participant
            .entry(print.participant_address.clone())
            .or_insert(Decimal::ZERO) += print.participant_notional();
    }
    by_participant
}

fn on_chain_primary_count(prints: &[TradeTapePrint]) -> usize {
    prints
        .iter()
        .filter(|print| {
            print.source == TradeTapeSourceKind::OnChain
                && print.participant_role == TradeParticipantRole::Maker
        })
        .count()
}

fn coverage_ratio(eligible_count: usize, observed_primary: usize) -> Decimal {
    if observed_primary == 0 {
        Decimal::ZERO
    } else {
        (Decimal::from(eligible_count) / Decimal::from(observed_primary)).round_dp(12)
    }
}

/// Compute concentration metrics or return a structured missing reason.
pub fn compute_concentration(
    prints: &[TradeTapePrint],
    source_available: bool,
    gate: &ParticipantConcentrationGate,
) -> Result<ParticipantConcentrationSnapshot, ConcentrationMissing> {
    if !source_available {
        return Err(ConcentrationMissing::TradeTapeUnavailable);
    }
    if prints.is_empty() {
        return Err(ConcentrationMissing::InsufficientTradeTape);
    }
    let observed_primary = on_chain_primary_count(prints);
    if observed_primary == 0 {
        return Err(ConcentrationMissing::InsufficientTradeTape);
    }
    let refs: Vec<&TradeTapePrint> = prints.iter().collect();
    let eligible = eligible_maker_prints(&refs);
    let coverage = coverage_ratio(eligible.len(), observed_primary);
    let by_participant = participant_notionals(&eligible);
    let unique_participants = u64::try_from(by_participant.len()).unwrap_or(u64::MAX);
    let total_notional: Decimal = by_participant.values().copied().sum();
    if unique_participants < gate.min_unique_participants
        || total_notional < gate.min_notional_usd
        || coverage < gate.min_coverage_ratio
    {
        return Err(ConcentrationMissing::InsufficientTradeTape);
    }
    let weights: Vec<Decimal> = by_participant.values().copied().collect();
    let (Some(gini), Some(hhi), Some(cr1)) = (
        gini(weights.clone()),
        hhi(weights.clone()),
        cr1_share(weights),
    ) else {
        return Err(ConcentrationMissing::InsufficientTradeTape);
    };
    Ok(ParticipantConcentrationSnapshot {
        observed_print_count: u64::try_from(observed_primary).unwrap_or(u64::MAX),
        eligible_print_count: u64::try_from(eligible.len()).unwrap_or(u64::MAX),
        unique_participants,
        total_notional_usd: total_notional.round_dp(8),
        coverage_ratio: coverage,
        gini,
        hhi,
        cr1_share: cr1,
    })
}

/// Role-specific Gini when enough unique addresses trade in that role.
pub fn compute_role_gini(
    prints: &[TradeTapePrint],
    role: TradeParticipantRole,
    gate: &ParticipantConcentrationGate,
) -> Result<ParticipantRoleMetrics, ConcentrationMissing> {
    let refs: Vec<&TradeTapePrint> = prints.iter().collect();
    let role_prints: Vec<&TradeTapePrint> = refs
        .iter()
        .copied()
        .filter(|print| {
            print.source == TradeTapeSourceKind::OnChain && print.participant_role == role
        })
        .filter(|print| {
            !print.participant_address.is_empty() && print.participant_notional() > Decimal::ZERO
        })
        .collect();
    let by_participant = participant_notionals(&role_prints);
    let unique = u64::try_from(by_participant.len()).unwrap_or(u64::MAX);
    if unique < gate.min_unique_participants {
        return Err(ConcentrationMissing::InsufficientRoleCoverage);
    }
    let weights: Vec<Decimal> = by_participant.values().copied().collect();
    let Some(gini) = gini(weights) else {
        return Err(ConcentrationMissing::InsufficientRoleCoverage);
    };
    Ok(ParticipantRoleMetrics { gini })
}

#[cfg(test)]
mod tests {
    use super::{
        ConcentrationCompositeWeights, ParticipantConcentrationGate, composite_concentration,
        compute_concentration, cr1_share, gini, hhi,
    };
    use chrono::Utc;
    use quant_pivot_models::{
        domain::{TradeParticipantRole, TradeTapePrint, TradeTapeSourceKind},
        types::{MarketId, Price, Shares, TokenId, Usd},
    };
    use rust_decimal::Decimal;
    use rust_decimal_macros::dec;

    fn gate() -> ParticipantConcentrationGate {
        ParticipantConcentrationGate {
            min_unique_participants: 2,
            min_notional_usd: dec!(10),
            min_coverage_ratio: dec!(0.5),
        }
    }

    fn print(address: &str, role: TradeParticipantRole, notional: Decimal) -> TradeTapePrint {
        TradeTapePrint {
            market_id: MarketId::new("m1"),
            token_id: TokenId::new("t1"),
            event_time: Utc::now(),
            available_at: None,
            participant_address: address.to_owned(),
            participant_role: role,
            side: None,
            price: Price::new(dec!(0.5)),
            size_shares: Shares::new(notional * dec!(2)),
            notional_usd: Usd::new(notional),
            tx_hash: None,
            trade_id: format!("{address}:{notional}:{role:?}"),
            source: TradeTapeSourceKind::OnChain,
            coverage_flags: 0,
            raw_payload_json: None,
        }
    }

    fn assert_metric(actual: Option<Decimal>, expected: Decimal) {
        assert_eq!(actual.map(|value| value.round_dp(12)), Some(expected));
    }

    #[test]
    fn concentration_metrics_cover_empty_and_single_participant() {
        assert_eq!(gini(Vec::<Decimal>::new()), None);
        assert_eq!(hhi(Vec::<Decimal>::new()), None);
        assert_eq!(cr1_share(Vec::<Decimal>::new()), None);

        let single = vec![dec!(10)];
        assert_metric(gini(single.clone()), dec!(0));
        assert_metric(hhi(single.clone()), dec!(1.000000000000));
        assert_metric(cr1_share(single), dec!(1.000000000000));
    }

    #[test]
    fn composite_concentration_normalizes_weights() {
        let weights = ConcentrationCompositeWeights {
            gini: dec!(0.50),
            cr1_share: dec!(0.30),
            hhi: dec!(0.20),
        };
        let composite =
            composite_concentration(dec!(0.8), dec!(0.6), dec!(0.4), &weights).expect("composite");
        assert_eq!(composite, dec!(0.660000000000));
    }

    #[test]
    fn compute_concentration_scores_maker_whale_window() {
        let prints = vec![
            print("a", TradeParticipantRole::Maker, dec!(90)),
            print("b", TradeParticipantRole::Maker, dec!(10)),
            print("c", TradeParticipantRole::Taker, dec!(50)),
        ];
        let snapshot = compute_concentration(&prints, true, &gate()).expect("scored");
        assert_eq!(snapshot.unique_participants, 2);
        assert_metric(Some(snapshot.cr1_share), dec!(0.900000000000));
    }
}

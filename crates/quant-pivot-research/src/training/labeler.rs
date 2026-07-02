//! Forward-looking label implementations (pure, point-in-time correct).
//!
//! Four labels are horizon-dependent (`return_to_horizon`,
//! `max_favorable_excursion_bps`, `max_adverse_excursion_bps`,
//! `liquidity_exit_possible`); `settlement_outcome` is horizon-independent and
//! keys on the authoritative `winning_token_id`. All forward data is pre-fetched
//! into [`LabelBuildInput::forward`]; a labeler never reads a database.

use chrono::Duration;
use quant_pivot_models::types::Price;
use rust_decimal::Decimal;

use super::{
    LabelBuildInput, LabelBuildOutput, LabelDelayReason, LabelName, Labeler, MissingLabelReason,
    TrainingLabel, TrainingSampleSource,
};

/// `return_to_horizon`: signed mid-price return from entry to the horizon, bps.
pub const RETURN_TO_HORIZON: LabelName = LabelName::from_static("return_to_horizon");
/// `max_favorable_excursion_bps`: best favorable move reached within the horizon.
pub const MAX_FAVORABLE_EXCURSION_BPS: LabelName =
    LabelName::from_static("max_favorable_excursion_bps");
/// `max_adverse_excursion_bps`: worst adverse move reached within the horizon.
pub const MAX_ADVERSE_EXCURSION_BPS: LabelName =
    LabelName::from_static("max_adverse_excursion_bps");
/// `liquidity_exit_possible`: whether sufficient exit depth existed in-horizon.
pub const LIQUIDITY_EXIT_POSSIBLE: LabelName = LabelName::from_static("liquidity_exit_possible");
/// `settlement_outcome`: terminal `settled_yes` (1.0) / `settled_no` (0.0).
pub const SETTLEMENT_OUTCOME: LabelName = LabelName::from_static("settlement_outcome");
/// `hold_vs_exit_alpha_bps`: bps advantage of exiting now (simulated fill @ t)
/// over holding through the lot's terminal outcome (Phase 06.1).
pub const HOLD_VS_EXIT_ALPHA_BPS: LabelName = LabelName::from_static("hold_vs_exit_alpha_bps");
pub const REALIZED_RETURN_BPS: LabelName = LabelName::from_static("realized_return_bps");
pub const REALIZED_PNL_USD: LabelName = LabelName::from_static("realized_pnl_usd");
pub const ENTRY_FILLED: LabelName = LabelName::from_static("entry_filled");
pub const ENTRY_SLIPPAGE_BPS: LabelName = LabelName::from_static("entry_slippage_bps");
pub const MISSED_RETURN_BPS: LabelName = LabelName::from_static("missed_return_bps");
pub const RECOMMENDATION_OUTCOME: LabelName = LabelName::from_static("recommendation_outcome");

/// All label names this plane produces, in a stable order (for schema hashing).
#[must_use]
pub fn label_names() -> Vec<LabelName> {
    vec![
        RETURN_TO_HORIZON,
        MAX_FAVORABLE_EXCURSION_BPS,
        MAX_ADVERSE_EXCURSION_BPS,
        LIQUIDITY_EXIT_POSSIBLE,
        SETTLEMENT_OUTCOME,
    ]
}

/// Stable label schema for selected sample sources.
#[must_use]
pub fn label_names_for_sources(sources: &[TrainingSampleSource]) -> Vec<LabelName> {
    let mut labels = Vec::new();
    if sources.contains(&TrainingSampleSource::HistoricalPit) {
        labels.extend(label_names());
    }
    if sources.contains(&TrainingSampleSource::LiveAttribution) {
        labels.extend([
            REALIZED_RETURN_BPS,
            REALIZED_PNL_USD,
            ENTRY_FILLED,
            ENTRY_SLIPPAGE_BPS,
            MAX_FAVORABLE_EXCURSION_BPS,
            MAX_ADVERSE_EXCURSION_BPS,
            MISSED_RETURN_BPS,
            RECOMMENDATION_OUTCOME,
        ]);
    }
    if sources.contains(&TrainingSampleSource::ExitDecision) {
        labels.extend([HOLD_VS_EXIT_ALPHA_BPS, LIQUIDITY_EXIT_POSSIBLE]);
    }
    labels.dedup();
    labels
}

/// Basis-point denominator (`1 = 10_000 bps`).
fn bps_denominator() -> Decimal {
    Decimal::from(10_000)
}

/// Signed return from `entry` to `value`, in basis points.
fn return_bps(entry: Decimal, value: Decimal) -> Option<Decimal> {
    if entry.is_zero() {
        return None;
    }
    Some((value - entry) / entry * bps_denominator())
}

/// The horizon cutoff for a sample.
fn horizon_end(input: &LabelBuildInput<'_>) -> chrono::DateTime<chrono::Utc> {
    input.as_of + Duration::seconds(i64::try_from(input.horizon_secs).unwrap_or(i64::MAX))
}

/// Whether the available data fully covers the horizon (else the label is not
/// yet mature).
fn horizon_matured(input: &LabelBuildInput<'_>) -> bool {
    input.forward.data_available_until >= horizon_end(input)
}

/// `return_to_horizon` labeler.
pub struct ReturnToHorizonLabeler;

impl Labeler for ReturnToHorizonLabeler {
    fn label_name(&self) -> LabelName {
        RETURN_TO_HORIZON
    }

    fn build_label(&self, input: &LabelBuildInput<'_>) -> LabelBuildOutput {
        let Some(entry) = input.entry_mid else {
            return LabelBuildOutput::Unavailable {
                reason: MissingLabelReason::NoEntryPrice,
            };
        };
        if !horizon_matured(input) {
            return LabelBuildOutput::NotMature {
                available_after: horizon_end(input),
                reason: LabelDelayReason::HorizonNotElapsed,
            };
        }
        let cutoff = horizon_end(input);
        let exit = input
            .forward
            .samples
            .iter()
            .filter(|s| s.at <= cutoff)
            .filter_map(|s| s.mid_close)
            .next_back();
        let Some(exit) = exit else {
            return LabelBuildOutput::Unavailable {
                reason: MissingLabelReason::NoExitPrice,
            };
        };
        return_bps(entry.inner(), exit.inner()).map_or(
            LabelBuildOutput::Unavailable {
                reason: MissingLabelReason::NoEntryPrice,
            },
            |value| {
                LabelBuildOutput::Available(TrainingLabel {
                    label_name: self.label_name(),
                    horizon_secs: input.horizon_secs,
                    value,
                    is_resolved: true,
                })
            },
        )
    }
}

/// `max_favorable_excursion_bps` labeler (best-bid high within the horizon).
pub struct MaxFavorableExcursionLabeler;

impl Labeler for MaxFavorableExcursionLabeler {
    fn label_name(&self) -> LabelName {
        MAX_FAVORABLE_EXCURSION_BPS
    }

    fn build_label(&self, input: &LabelBuildInput<'_>) -> LabelBuildOutput {
        excursion(input, self.label_name(), Extreme::Max)
    }
}

/// `max_adverse_excursion_bps` labeler (best-bid low within the horizon).
pub struct MaxAdverseExcursionLabeler;

impl Labeler for MaxAdverseExcursionLabeler {
    fn label_name(&self) -> LabelName {
        MAX_ADVERSE_EXCURSION_BPS
    }

    fn build_label(&self, input: &LabelBuildInput<'_>) -> LabelBuildOutput {
        excursion(input, self.label_name(), Extreme::Min)
    }
}

/// Which intra-horizon extreme an excursion label tracks.
#[derive(Clone, Copy)]
enum Extreme {
    Max,
    Min,
}

/// Shared excursion computation: the bps move from entry to the best-bid
/// high/low extreme over the horizon window.
fn excursion(input: &LabelBuildInput<'_>, name: LabelName, extreme: Extreme) -> LabelBuildOutput {
    let Some(entry) = input.entry_mid else {
        return LabelBuildOutput::Unavailable {
            reason: MissingLabelReason::NoEntryPrice,
        };
    };
    if !horizon_matured(input) {
        return LabelBuildOutput::NotMature {
            available_after: horizon_end(input),
            reason: LabelDelayReason::HorizonNotElapsed,
        };
    }
    let cutoff = horizon_end(input);
    let mut extreme_price: Option<Decimal> = None;
    for sample in input.forward.samples.iter().filter(|s| s.at <= cutoff) {
        let candidate = match extreme {
            Extreme::Max => sample.best_bid_high,
            Extreme::Min => sample.best_bid_low,
        };
        let Some(candidate) = candidate.map(Price::inner) else {
            continue;
        };
        extreme_price = Some(match (extreme_price, extreme) {
            (None, _) => candidate,
            (Some(current), Extreme::Max) => current.max(candidate),
            (Some(current), Extreme::Min) => current.min(candidate),
        });
    }
    let Some(extreme_price) = extreme_price else {
        return LabelBuildOutput::Unavailable {
            reason: MissingLabelReason::NoForwardData,
        };
    };
    return_bps(entry.inner(), extreme_price).map_or(
        LabelBuildOutput::Unavailable {
            reason: MissingLabelReason::NoEntryPrice,
        },
        |value| {
            LabelBuildOutput::Available(TrainingLabel {
                label_name: name,
                horizon_secs: input.horizon_secs,
                value,
                is_resolved: true,
            })
        },
    )
}

/// `liquidity_exit_possible` labeler.
pub struct LiquidityExitLabeler;

impl Labeler for LiquidityExitLabeler {
    fn label_name(&self) -> LabelName {
        LIQUIDITY_EXIT_POSSIBLE
    }

    fn build_label(&self, input: &LabelBuildInput<'_>) -> LabelBuildOutput {
        if !horizon_matured(input) {
            return LabelBuildOutput::NotMature {
                available_after: horizon_end(input),
                reason: LabelDelayReason::HorizonNotElapsed,
            };
        }
        let cutoff = horizon_end(input);
        let mut saw_forward = false;
        let mut exit_possible = false;
        for sample in input.forward.samples.iter().filter(|s| s.at <= cutoff) {
            saw_forward = true;
            if let Some(depth) = sample.top1_depth_usd
                && depth.inner() >= input.min_exit_depth_usd.inner()
            {
                exit_possible = true;
                break;
            }
        }
        if !saw_forward {
            return LabelBuildOutput::Unavailable {
                reason: MissingLabelReason::NoForwardData,
            };
        }
        LabelBuildOutput::Available(TrainingLabel {
            label_name: self.label_name(),
            horizon_secs: input.horizon_secs,
            value: if exit_possible {
                Decimal::ONE
            } else {
                Decimal::ZERO
            },
            is_resolved: true,
        })
    }
}

/// `hold_vs_exit_alpha_bps` labeler (Phase 06.1 Sell scorer supervision).
///
/// Compares a depth-aware simulated exit at `t` against the net cash from holding
/// `remaining_shares@t` through the lot's actual terminal outcome. Point-in-time
/// correct: the hold oracle reads only pre-fetched terminal ledger facts; the
/// exit side uses the decision book slice (L2 bid-walk or microstructure fallback).
pub struct HoldVsExitProceedsLabeler;

impl Labeler for HoldVsExitProceedsLabeler {
    fn label_name(&self) -> LabelName {
        HOLD_VS_EXIT_ALPHA_BPS
    }

    fn is_horizon_dependent(&self) -> bool {
        false
    }

    fn build_label(&self, input: &LabelBuildInput<'_>) -> LabelBuildOutput {
        let Some(ctx) = input.exit_decision else {
            return LabelBuildOutput::Unavailable {
                reason: MissingLabelReason::NoForwardData,
            };
        };
        if input.as_of >= ctx.terminal.closed_at {
            return LabelBuildOutput::Unavailable {
                reason: MissingLabelReason::NoExitPrice,
            };
        }
        if !ctx.remaining_shares.is_positive() {
            return LabelBuildOutput::Unavailable {
                reason: MissingLabelReason::NoEntryPrice,
            };
        }
        let cost_basis = ctx.avg_price.inner() * ctx.remaining_shares.inner();
        if cost_basis <= Decimal::ZERO {
            return LabelBuildOutput::Unavailable {
                reason: MissingLabelReason::NoEntryPrice,
            };
        }
        let Some(book) = &ctx.decision_book else {
            return LabelBuildOutput::Unavailable {
                reason: MissingLabelReason::NoForwardData,
            };
        };
        let sim = crate::execution_sim::ExitFillSimulator::new(ctx.fee_bps);
        let fill = match book {
            super::DecisionBook::L2 { bids } => sim.simulate_l2(bids, ctx.remaining_shares),
            super::DecisionBook::Microstructure { best_bid, depth } => {
                sim.simulate_fallback(*best_bid, *depth, ctx.remaining_shares)
            }
        };
        if !fill.filled_shares.is_positive() {
            return LabelBuildOutput::Unavailable {
                reason: MissingLabelReason::NoForwardData,
            };
        }
        let exit_proceeds = fill.net_proceeds.inner();
        let hold_terminal = super::hold_terminal_proceeds(&ctx.terminal, input.as_of).inner();
        let alpha = (exit_proceeds - hold_terminal) / cost_basis * bps_denominator();
        LabelBuildOutput::Available(TrainingLabel {
            label_name: self.label_name(),
            horizon_secs: 0,
            value: alpha,
            is_resolved: true,
        })
    }
}

/// `settlement_outcome` labeler (horizon-independent; keys on `winning_token_id`).
pub struct SettlementOutcomeLabeler;

impl Labeler for SettlementOutcomeLabeler {
    fn label_name(&self) -> LabelName {
        SETTLEMENT_OUTCOME
    }

    fn is_horizon_dependent(&self) -> bool {
        false
    }

    fn build_label(&self, input: &LabelBuildInput<'_>) -> LabelBuildOutput {
        let Some(resolution) = input.forward.resolution.as_ref() else {
            return LabelBuildOutput::NotMature {
                available_after: input.forward.data_available_until,
                reason: LabelDelayReason::SettlementPending,
            };
        };
        if resolution.resolved_at <= input.as_of {
            return LabelBuildOutput::NotMature {
                available_after: resolution.resolved_at,
                reason: LabelDelayReason::SettlementPending,
            };
        }
        let settled_yes = resolution.winning_token_id == *input.yes_token_id;
        LabelBuildOutput::Available(TrainingLabel {
            label_name: self.label_name(),
            horizon_secs: 0,
            value: if settled_yes {
                Decimal::ONE
            } else {
                Decimal::ZERO
            },
            is_resolved: true,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::training::{ForwardSample, ForwardWindow, MarketResolution};
    use chrono::{DateTime, TimeZone, Utc};
    use quant_pivot_models::types::{MarketId, Price, TokenId, Usd};
    use rust_decimal::Decimal;
    use rust_decimal_macros::dec;

    fn at(offset: i64) -> DateTime<Utc> {
        Utc.timestamp_opt(1_000_000 + offset, 0)
            .single()
            .expect("ts")
    }

    fn price(value: &str) -> Price {
        Price::new(Decimal::from_str_exact(value).expect("decimal"))
    }

    fn forward(
        samples: Vec<ForwardSample>,
        available_secs: i64,
        resolution: Option<MarketResolution>,
    ) -> ForwardWindow {
        ForwardWindow {
            anchor: at(0),
            data_available_until: at(available_secs),
            samples,
            resolution,
        }
    }

    fn input<'a>(
        market: &'a MarketId,
        token: &'a TokenId,
        entry_mid: Option<Price>,
        horizon_secs: u64,
        forward: &'a ForwardWindow,
    ) -> LabelBuildInput<'a> {
        LabelBuildInput {
            market_id: market,
            token_id: token,
            yes_token_id: token,
            as_of: at(0),
            entry_mid,
            horizon_secs,
            min_exit_depth_usd: Usd::new(Decimal::from(100)),
            forward,
            exit_decision: None,
        }
    }

    fn sample(offset: i64, mid: &str, bid_high: &str, bid_low: &str) -> ForwardSample {
        ForwardSample {
            at: at(offset),
            mid_close: Some(price(mid)),
            best_bid_high: Some(price(bid_high)),
            best_bid_low: Some(price(bid_low)),
            top1_depth_usd: Some(Usd::new(Decimal::from(250))),
        }
    }

    #[test]
    fn return_to_horizon_resolves_when_mature() {
        let market = MarketId::new("m");
        let token = TokenId::new("t");
        let window = forward(vec![sample(60, "0.55", "0.56", "0.54")], 120, None);
        let out = ReturnToHorizonLabeler.build_label(&input(
            &market,
            &token,
            Some(price("0.50")),
            60,
            &window,
        ));
        match out {
            LabelBuildOutput::Available(label) => {
                // (0.55 - 0.50) / 0.50 * 10_000 = 1000 bps.
                assert_eq!(label.value, Decimal::from(1000));
                assert!(label.is_resolved);
            }
            other => panic!("expected available, got {other:?}"),
        }
    }

    #[test]
    fn labeler_waits_for_maturity() {
        let market = MarketId::new("m");
        let token = TokenId::new("t");
        // Data only reaches +30s but the horizon needs +60s.
        let window = forward(vec![sample(30, "0.55", "0.56", "0.54")], 30, None);
        let out = ReturnToHorizonLabeler.build_label(&input(
            &market,
            &token,
            Some(price("0.50")),
            60,
            &window,
        ));
        assert!(matches!(
            out,
            LabelBuildOutput::NotMature {
                reason: LabelDelayReason::HorizonNotElapsed,
                ..
            }
        ));
    }

    #[test]
    fn excursions_track_bid_extremes() {
        let market = MarketId::new("m");
        let token = TokenId::new("t");
        let window = forward(
            vec![
                sample(20, "0.52", "0.60", "0.45"),
                sample(40, "0.50", "0.58", "0.40"),
            ],
            120,
            None,
        );
        let mfe = MaxFavorableExcursionLabeler.build_label(&input(
            &market,
            &token,
            Some(price("0.50")),
            60,
            &window,
        ));
        let mae = MaxAdverseExcursionLabeler.build_label(&input(
            &market,
            &token,
            Some(price("0.50")),
            60,
            &window,
        ));
        // MFE keys on the max best-bid high (0.60): (0.60-0.50)/0.50*10000 = 2000.
        assert!(matches!(mfe, LabelBuildOutput::Available(l) if l.value == Decimal::from(2000)));
        // MAE keys on the min best-bid low (0.40): (0.40-0.50)/0.50*10000 = -2000.
        assert!(matches!(mae, LabelBuildOutput::Available(l) if l.value == Decimal::from(-2000)));
    }

    #[test]
    fn settlement_keys_on_winning_token() {
        let market = MarketId::new("m");
        let yes = TokenId::new("yes");
        let resolved = forward(
            Vec::new(),
            120,
            Some(MarketResolution {
                winning_token_id: yes.clone(),
                resolved_at: at(30),
                observed_at: at(30),
            }),
        );
        let out = SettlementOutcomeLabeler.build_label(&input(&market, &yes, None, 60, &resolved));
        assert!(
            matches!(out, LabelBuildOutput::Available(l) if l.value == Decimal::ONE && l.horizon_secs == 0)
        );

        let pre_as_of = forward(
            Vec::new(),
            120,
            Some(MarketResolution {
                winning_token_id: yes.clone(),
                resolved_at: at(-10),
                observed_at: at(-5),
            }),
        );
        let out = SettlementOutcomeLabeler.build_label(&input(&market, &yes, None, 60, &pre_as_of));
        assert!(matches!(
            out,
            LabelBuildOutput::NotMature {
                reason: LabelDelayReason::SettlementPending,
                ..
            }
        ));

        let pending = forward(Vec::new(), 0, None);
        let out = SettlementOutcomeLabeler.build_label(&input(&market, &yes, None, 60, &pending));
        assert!(matches!(
            out,
            LabelBuildOutput::NotMature {
                reason: LabelDelayReason::SettlementPending,
                ..
            }
        ));
    }

    #[test]
    fn hold_vs_exit_proceeds_prefers_exiting_when_terminal_is_worse() {
        use super::super::{
            DecisionBook, ExitDecisionLabelContext, LotExitEvent, LotTerminalSnapshot,
        };
        use quant_pivot_models::types::{Bps, Shares, Usd};
        use std::sync::Arc;

        let market = MarketId::new("m");
        let token = TokenId::new("t");
        let terminal = LotTerminalSnapshot {
            entry_shares: Shares::new(dec!(100)),
            opened_at: at(0),
            closed_at: at(1000),
            total_net_proceeds: Usd::new(dec!(45)),
            exit_events: vec![LotExitEvent {
                at: at(800),
                shares: Shares::new(dec!(100)),
                net_proceeds: Usd::new(dec!(45)),
            }],
        };
        let ctx = ExitDecisionLabelContext {
            remaining_shares: Shares::new(dec!(100)),
            avg_price: price("0.50"),
            fee_bps: Bps::ZERO,
            terminal,
            decision_book: Some(DecisionBook::L2 {
                bids: Arc::new([
                    quant_pivot_models::domain::market::book::BookLevel::from_decimal_unchecked(
                        price("0.55"),
                        Shares::new(dec!(100)),
                    ),
                ]),
            }),
        };
        let window = forward(Vec::new(), 0, None);
        let mut input = input(&market, &token, Some(price("0.55")), 0, &window);
        input.exit_decision = Some(&ctx);
        let out = HoldVsExitProceedsLabeler.build_label(&input);
        assert!(matches!(out, LabelBuildOutput::Available(l) if l.value > Decimal::ZERO));
    }

    #[test]
    fn hold_vs_exit_proceeds_requires_exit_decision_context() {
        let market = MarketId::new("m");
        let token = TokenId::new("t");
        let window = forward(Vec::new(), 0, None);
        let out = HoldVsExitProceedsLabeler.build_label(&input(
            &market,
            &token,
            Some(price("0.50")),
            0,
            &window,
        ));
        assert!(matches!(
            out,
            LabelBuildOutput::Unavailable {
                reason: MissingLabelReason::NoForwardData,
            }
        ));
    }

    #[test]
    fn liquidity_exit_requires_depth_threshold() {
        let market = MarketId::new("m");
        let token = TokenId::new("t");
        let shallow = ForwardSample {
            at: at(30),
            mid_close: Some(price("0.50")),
            best_bid_high: Some(price("0.50")),
            best_bid_low: Some(price("0.49")),
            top1_depth_usd: Some(Usd::new(Decimal::from(50))),
        };
        let deep = ForwardSample {
            top1_depth_usd: Some(Usd::new(Decimal::from(250))),
            ..shallow
        };
        let window = forward(vec![shallow], 120, None);
        let out = LiquidityExitLabeler.build_label(&input(
            &market,
            &token,
            Some(price("0.50")),
            60,
            &window,
        ));
        assert!(matches!(
            out,
            LabelBuildOutput::Available(l) if l.value == Decimal::ZERO
        ));

        let window = forward(vec![deep], 120, None);
        let out = LiquidityExitLabeler.build_label(&input(
            &market,
            &token,
            Some(price("0.50")),
            60,
            &window,
        ));
        assert!(matches!(
            out,
            LabelBuildOutput::Available(l) if l.value == Decimal::ONE
        ));
    }
}

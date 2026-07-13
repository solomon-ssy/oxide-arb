//! Structural Alpha monitor port (Phase 11.2.1+).

use std::{
    cmp::Reverse,
    collections::{BTreeMap, HashMap},
    str::FromStr,
    sync::Arc,
    time::Duration,
};

use async_trait::async_trait;
use chrono::{DateTime, Utc};
use quant_pivot_error::{QuantError, QuantResult};
use quant_pivot_models::{
    config::TradeTapeOnChainConfig,
    domain::{
        DecisionBoundary, DecisionClock, DecisionSource, MissingReasonCountView,
        NegRiskEventDriftView, NegRiskLegView, ParticipantConcentrationDetailView,
        ParticipantConcentrationMarketView, ParticipantConcentrationParticipantView,
        ParticipantConcentrationSummaryView, RuntimeConfigPort, StructuralMonitorPort,
        TradeParticipantRole, TradeTapeCoverageView, TradeTapePrint, TradeTapeSourceHealthView,
        TradeTapeSourceKind,
        market::{MarketRegistryInfo, registry::NegRiskLeg},
    },
    types::{EventId, MarketId, Price},
};
use quant_pivot_repository::traits::TradeTapeBlockCursorRepository;
use quant_pivot_research::{
    selection::SelectedMarket,
    trade_tape::participant_concentration::{
        ConcentrationCompositeWeights, ConcentrationMissing, ParticipantConcentrationGate,
        composite_concentration, compute_concentration,
    },
};
use rust_decimal::Decimal;

use crate::{
    ingest::{
        book_store::BookStore,
        market_registry::MarketRegistry,
        trade_tape_health::{
            cursors_by_contract_address, trade_tape_market_ingest_available,
            trade_tape_route_lag_blocks,
        },
    },
    prefetch::feature_window::FeatureWindowProvider,
};
use std::iter;

const TOP_MARKETS_LIMIT: usize = 50;
const TOP_PARTICIPANTS_LIMIT: usize = 25;

/// Structural monitor backed by live registry/books and unified PIT trade tape.
pub struct CoreStructuralMonitor {
    market_registry: Arc<MarketRegistry>,
    book_store: Arc<BookStore>,
    feature_windows: Arc<FeatureWindowProvider>,
    block_cursor_repo: Arc<dyn TradeTapeBlockCursorRepository>,
    runtime_config: Arc<dyn RuntimeConfigPort>,
    trade_tape_on_chain: TradeTapeOnChainConfig,
}

impl CoreStructuralMonitor {
    #[must_use]
    pub fn new(
        market_registry: Arc<MarketRegistry>,
        book_store: Arc<BookStore>,
        feature_windows: Arc<FeatureWindowProvider>,
        block_cursor_repo: Arc<dyn TradeTapeBlockCursorRepository>,
        runtime_config: Arc<dyn RuntimeConfigPort>,
        trade_tape_on_chain: TradeTapeOnChainConfig,
    ) -> Self {
        Self {
            market_registry,
            book_store,
            feature_windows,
            block_cursor_repo,
            runtime_config,
            trade_tape_on_chain,
        }
    }

    fn leg_view(&self, leg: &NegRiskLeg) -> NegRiskLegView {
        let best_ask = self
            .book_store
            .load(&leg.yes_token_id)
            .and_then(|book| book.best_ask())
            .map(Price::inner);
        let question = self
            .market_registry
            .get_market(&leg.market_id)
            .map(|market| market.question.clone())
            .unwrap_or_default();
        NegRiskLegView {
            market_id: leg.market_id.clone(),
            yes_token_id: leg.yes_token_id.clone(),
            question,
            best_ask,
        }
    }

    fn active_markets(&self) -> Vec<MarketRegistryInfo> {
        let mut markets: Vec<MarketRegistryInfo> = self
            .market_registry
            .active_markets()
            .iter()
            .filter_map(|market_id| self.market_registry.get_market(market_id))
            .map(|market| (*market).clone())
            .collect();
        markets.sort_by(|left, right| left.market_id.as_str().cmp(right.market_id.as_str()));
        markets
    }

    fn pit_boundary(
        &self,
        trigger_time: DateTime<Utc>,
    ) -> QuantResult<(DecisionBoundary, Duration)> {
        let runtime = self.runtime_config.current();
        let knowledge_lag_secs = runtime.pit_knowledge_lag_secs().ok_or_else(|| {
            QuantError::config("runtime report schedules do not define one canonical knowledge lag")
        })?;
        let boundary = DecisionClock::new(knowledge_lag_secs)
            .boundary(trigger_time)?
            .with_source_cutoff(DecisionSource::TradeTape, 0)?;
        Ok((
            boundary,
            Duration::from_secs(runtime.features.structural.trade_tape_window_secs),
        ))
    }

    async fn participant_markets(
        &self,
        markets: Vec<MarketRegistryInfo>,
    ) -> QuantResult<(
        DecisionBoundary,
        Vec<ParticipantConcentrationMarketView>,
        HashMap<MarketId, Vec<TradeTapePrint>>,
    )> {
        let runtime = self.runtime_config.current();
        let gate = ParticipantConcentrationGate {
            min_unique_participants: runtime
                .features
                .structural
                .trade_tape_min_unique_participants,
            min_notional_usd: parse_decimal(
                "features.structural.trade_tape_min_notional_usd",
                &runtime
                    .features
                    .structural
                    .trade_tape_min_notional_usd
                    .value,
            )?,
            min_coverage_ratio: parse_decimal(
                "features.structural.trade_tape_min_coverage_ratio",
                &runtime
                    .features
                    .structural
                    .trade_tape_min_coverage_ratio
                    .value,
            )?,
        };
        let factor = &runtime.factors.structural.participant_concentration;
        let weights = ConcentrationCompositeWeights {
            gini: parse_decimal(
                "factors.structural.participant_concentration.gini_weight",
                &factor.gini_weight.value,
            )?,
            cr1_share: parse_decimal(
                "factors.structural.participant_concentration.cr1_share_weight",
                &factor.cr1_share_weight.value,
            )?,
            hhi: parse_decimal(
                "factors.structural.participant_concentration.hhi_weight",
                &factor.hhi_weight.value,
            )?,
        };
        let trigger_time = Utc::now();
        let (boundary, lookback) = self.pit_boundary(trigger_time)?;
        let selected = markets
            .iter()
            .map(|market| SelectedMarket {
                market_id: market.market_id.clone(),
                event_id: market.event_id.clone(),
                category: market.fee_category(),
                primary_token_id: market.token_yes.clone(),
                secondary_token_id: Some(market.token_no.clone()),
                liquidity_usd: market.liquidity_usd,
                volume_24h_usd: market.volume_24h,
                source_refs: Vec::new(),
            })
            .collect::<Vec<_>>();
        let windows = self
            .feature_windows
            .load_trade_tape_windows(&selected, &boundary, lookback)
            .await?;
        let cursors = self
            .block_cursor_repo
            .list_by_source(TradeTapeSourceKind::OnChain.as_str())
            .await?;
        let cursors_by_address = cursors_by_contract_address(&cursors);

        let mut prints_by_market = HashMap::new();
        let views = markets
            .iter()
            .map(|market| {
                let prints = windows
                    .get(&market.market_id)
                    .map(|window| window.prints.clone())
                    .unwrap_or_default();
                prints_by_market.insert(market.market_id.clone(), prints.clone());
                let source_available = trade_tape_market_ingest_available(
                    &self.trade_tape_on_chain,
                    &cursors_by_address,
                    market.neg_risk,
                );
                let lag_blocks = trade_tape_route_lag_blocks(market.neg_risk, &cursors_by_address);
                concentration_market_view(
                    market,
                    &prints,
                    lag_blocks,
                    &boundary,
                    gate,
                    weights,
                    source_available,
                )
            })
            .collect();
        Ok((boundary, views, prints_by_market))
    }
}

#[async_trait]
impl StructuralMonitorPort for CoreStructuralMonitor {
    async fn negrisk_events(&self) -> QuantResult<Vec<NegRiskEventDriftView>> {
        let as_of = Utc::now();
        let mut event_ids: Vec<EventId> = self
            .market_registry
            .active_markets()
            .iter()
            .filter_map(|market_id| self.market_registry.get_market(market_id))
            .filter(|market| market.neg_risk)
            .map(|market| market.event_id.clone())
            .collect();
        event_ids.sort();
        event_ids.dedup();

        let mut events = Vec::new();
        for event_id in event_ids {
            let Some(event) = self.market_registry.get_event(&event_id) else {
                continue;
            };
            let leg_set = self.market_registry.neg_risk_leg_set(&event_id);
            if leg_set.expected_legs == 0 {
                continue;
            }
            let leg_views: Vec<NegRiskLegView> =
                leg_set.legs.iter().map(|leg| self.leg_view(leg)).collect();
            let (ask_sum, drift) = leg_sum(&leg_views);
            events.push(NegRiskEventDriftView {
                event_id: event.event_id,
                title: event.title,
                leg_count: u32::try_from(leg_set.expected_legs).unwrap_or(u32::MAX),
                ask_sum,
                drift,
                legs: leg_views,
                computed_at: as_of,
            });
        }
        events.sort_by_key(|event| Reverse(drift_magnitude(event.drift)));
        Ok(events)
    }

    async fn trade_tape_coverage(&self) -> QuantResult<TradeTapeCoverageView> {
        let trigger_time = Utc::now();
        let (boundary, _) = self.pit_boundary(trigger_time)?;
        let runtime = self.runtime_config.current();
        let active_markets = self.active_markets();
        let active_market_count = u64::try_from(active_markets.len()).unwrap_or(u64::MAX);
        let cursors = self
            .block_cursor_repo
            .list_by_source(TradeTapeSourceKind::OnChain.as_str())
            .await?;
        let mut bootstrap_count = 0_u64;
        let mut catching_up_count = 0_u64;
        let mut live_count = 0_u64;
        let mut error_count = 0_u64;
        let mut worst_lag_blocks: Option<i64> = None;
        let mut last_updated_at: Option<DateTime<Utc>> = None;
        for cursor in &cursors {
            match cursor.status.as_str() {
                "bootstrap" => bootstrap_count += 1,
                "catching_up" => catching_up_count += 1,
                "live" => live_count += 1,
                "error" => error_count += 1,
                _ => {}
            }
            worst_lag_blocks = Some(worst_lag_blocks.map_or(cursor.head_lag_blocks, |lag| {
                lag.max(cursor.head_lag_blocks)
            }));
            last_updated_at = Some(
                last_updated_at.map_or(cursor.updated_at, |updated| updated.max(cursor.updated_at)),
            );
        }
        let contract_cursor_count = u64::try_from(cursors.len()).unwrap_or(u64::MAX);
        let live_contract_count = live_count;
        let ingest_enabled = self.trade_tape_on_chain.enabled;
        let cursors_by_address = cursors_by_contract_address(&cursors);
        let covered_market_ratio = if active_market_count == 0 {
            Decimal::ZERO
        } else {
            let covered = active_markets
                .iter()
                .filter(|market| {
                    trade_tape_market_ingest_available(
                        &self.trade_tape_on_chain,
                        &cursors_by_address,
                        market.neg_risk,
                    )
                })
                .count();
            Decimal::from(u64::try_from(covered).unwrap_or(u64::MAX))
                / Decimal::from(active_market_count)
        };
        let missing_markets = active_markets
            .iter()
            .filter(|market| {
                !trade_tape_market_ingest_available(
                    &self.trade_tape_on_chain,
                    &cursors_by_address,
                    market.neg_risk,
                )
            })
            .count();
        let missing_markets = u64::try_from(missing_markets).unwrap_or(u64::MAX);
        Ok(TradeTapeCoverageView {
            decision_at: trigger_time,
            knowledge_cutoff: boundary.cutoff_for(DecisionSource::TradeTape),
            window_secs: runtime.features.structural.trade_tape_window_secs,
            knowledge_lag_secs: boundary.knowledge_lag_secs(),
            active_market_count,
            token_cursor_count: contract_cursor_count,
            market_cursor_count: live_contract_count,
            covered_market_ratio,
            source_health: vec![TradeTapeSourceHealthView {
                source: TradeTapeSourceKind::OnChain.as_str().to_owned(),
                enabled: ingest_enabled,
                token_cursor_count: contract_cursor_count,
                bootstrap_count,
                catching_up_count,
                live_count,
                empty_count: 0,
                error_count,
                worst_lag_blocks,
                last_updated_at,
            }],
            missing_reason_breakdown: reason_counts(
                iter::once((
                    ConcentrationMissing::TradeTapeUnavailable
                        .monitor_wire()
                        .to_owned(),
                    missing_markets,
                ))
                .filter(|(_, count)| *count > 0),
            ),
        })
    }

    async fn participant_concentration(&self) -> QuantResult<ParticipantConcentrationSummaryView> {
        let runtime = self.runtime_config.current();
        let (boundary, mut markets, _) = self.participant_markets(self.active_markets()).await?;
        let missing_reason_breakdown = missing_reason_breakdown(&markets);
        markets.sort_by(|left, right| {
            right
                .composite_raw
                .cmp(&left.composite_raw)
                .then_with(|| left.market_id.as_str().cmp(right.market_id.as_str()))
        });
        markets.truncate(TOP_MARKETS_LIMIT);
        Ok(ParticipantConcentrationSummaryView {
            decision_at: boundary.decision_at(),
            knowledge_cutoff: boundary.cutoff_for(DecisionSource::TradeTape),
            window_secs: runtime.features.structural.trade_tape_window_secs,
            knowledge_lag_secs: boundary.knowledge_lag_secs(),
            min_unique_participants: runtime
                .features
                .structural
                .trade_tape_min_unique_participants,
            min_notional_usd: parse_decimal(
                "features.structural.trade_tape_min_notional_usd",
                &runtime
                    .features
                    .structural
                    .trade_tape_min_notional_usd
                    .value,
            )?,
            min_coverage_ratio: parse_decimal(
                "features.structural.trade_tape_min_coverage_ratio",
                &runtime
                    .features
                    .structural
                    .trade_tape_min_coverage_ratio
                    .value,
            )?,
            markets,
            missing_reason_breakdown,
        })
    }

    async fn participant_concentration_market(
        &self,
        market_id: &MarketId,
    ) -> QuantResult<Option<ParticipantConcentrationDetailView>> {
        let Some(market) = self.market_registry.get_market(market_id) else {
            return Ok(None);
        };
        let (boundary, mut markets, prints_by_market) =
            self.participant_markets(vec![(*market).clone()]).await?;
        let Some(view) = markets.pop() else {
            return Ok(None);
        };
        let top_participants = participant_breakdown(
            prints_by_market
                .get(market_id)
                .map_or(&[][..], Vec::as_slice),
            TOP_PARTICIPANTS_LIMIT,
        );
        Ok(Some(ParticipantConcentrationDetailView {
            decision_at: boundary.decision_at(),
            knowledge_cutoff: boundary.cutoff_for(DecisionSource::TradeTape),
            market: view,
            top_participants,
        }))
    }
}

fn parse_decimal(field: &'static str, value: &str) -> QuantResult<Decimal> {
    Decimal::from_str(value).map_err(|error| {
        QuantError::config(format!(
            "runtime config field {field} is not a valid decimal: {error}"
        ))
    })
}

fn leg_sum(legs: &[NegRiskLegView]) -> (Option<Decimal>, Option<Decimal>) {
    let mut sum = Decimal::ZERO;
    for leg in legs {
        let Some(ask) = leg.best_ask else {
            return (None, None);
        };
        sum += ask;
    }
    (Some(sum), Some(sum - Decimal::ONE))
}

fn drift_magnitude(drift: Option<Decimal>) -> Decimal {
    drift.map_or(Decimal::NEGATIVE_ONE, |value| value.abs())
}

fn concentration_market_view(
    market: &MarketRegistryInfo,
    prints: &[TradeTapePrint],
    lag_blocks: Option<i64>,
    boundary: &DecisionBoundary,
    gate: ParticipantConcentrationGate,
    weights: ConcentrationCompositeWeights,
    source_available: bool,
) -> ParticipantConcentrationMarketView {
    let concentration = compute_concentration(prints, source_available, &gate);
    let missing_reason = concentration
        .as_ref()
        .err()
        .map(|reason| reason.monitor_wire().to_owned());

    let (
        trade_count,
        participant_count,
        notional_usd,
        coverage_ratio,
        gini,
        hhi,
        cr1_share,
        composite_raw,
    ) = concentration.map_or(
        (None, None, None, None, None, None, None, None),
        |snapshot| {
            let composite =
                composite_concentration(snapshot.gini, snapshot.cr1_share, snapshot.hhi, &weights);
            (
                Some(snapshot.observed_print_count),
                Some(snapshot.unique_participants),
                Some(snapshot.total_notional_usd),
                Some(snapshot.coverage_ratio),
                Some(snapshot.gini),
                Some(snapshot.hhi),
                Some(snapshot.cr1_share),
                composite,
            )
        },
    );

    ParticipantConcentrationMarketView {
        market_id: market.market_id.clone(),
        token_id: market.token_yes.clone(),
        question: market.question.clone(),
        knowledge_cutoff: boundary.cutoff_for(DecisionSource::TradeTape),
        trade_count,
        participant_count,
        notional_usd,
        coverage_ratio,
        gini,
        hhi,
        cr1_share,
        composite_raw,
        lag_blocks,
        missing_reason,
    }
}

fn participant_breakdown(
    prints: &[TradeTapePrint],
    limit: usize,
) -> Vec<ParticipantConcentrationParticipantView> {
    #[derive(Default)]
    struct ParticipantAccumulator {
        trade_count: u64,
        notional_usd: Decimal,
    }

    let mut participants = BTreeMap::<(String, String), ParticipantAccumulator>::new();
    for print in prints.iter().filter(|print| {
        print.source == TradeTapeSourceKind::OnChain
            && print.participant_role == TradeParticipantRole::Maker
    }) {
        let notional = print.participant_notional();
        if print.participant_address.is_empty() || notional <= Decimal::ZERO {
            continue;
        }
        let entry = participants
            .entry((
                print.participant_address.clone(),
                participant_role(print.participant_role).to_owned(),
            ))
            .or_default();
        entry.trade_count = entry.trade_count.saturating_add(1);
        entry.notional_usd += notional;
    }
    let total = participants
        .values()
        .map(|participant| participant.notional_usd)
        .sum();
    let mut views = participants
        .into_iter()
        .map(
            |((participant_address, role), participant)| ParticipantConcentrationParticipantView {
                participant_address,
                participant_role: role,
                trade_count: participant.trade_count,
                notional_usd: participant.notional_usd,
                share: decimal_ratio(participant.notional_usd, total),
            },
        )
        .collect::<Vec<_>>();
    views.sort_by(|left, right| {
        right
            .notional_usd
            .cmp(&left.notional_usd)
            .then_with(|| left.participant_address.cmp(&right.participant_address))
    });
    views.truncate(limit);
    views
}

const fn participant_role(role: TradeParticipantRole) -> &'static str {
    match role {
        TradeParticipantRole::Maker => "maker",
        TradeParticipantRole::Taker => "taker",
        TradeParticipantRole::Unknown => "unknown",
    }
}

fn missing_reason_breakdown(
    markets: &[ParticipantConcentrationMarketView],
) -> Vec<MissingReasonCountView> {
    let mut counts = BTreeMap::<String, u64>::new();
    for market in markets {
        if let Some(reason) = &market.missing_reason {
            *counts.entry(reason.clone()).or_default() += 1;
        }
    }
    reason_counts(counts)
}

fn reason_counts(counts: impl IntoIterator<Item = (String, u64)>) -> Vec<MissingReasonCountView> {
    counts
        .into_iter()
        .map(|(reason, count)| MissingReasonCountView { reason, count })
        .collect()
}

fn decimal_ratio(numerator: Decimal, denominator: Decimal) -> Decimal {
    if denominator <= Decimal::ZERO {
        return Decimal::ZERO;
    }
    numerator / denominator
}

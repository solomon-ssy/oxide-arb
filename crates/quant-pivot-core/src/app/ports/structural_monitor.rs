//! Structural Alpha monitor application port.

use std::{
    cmp::Reverse,
    collections::{BTreeMap, HashMap},
    iter,
    sync::Arc,
    time::Duration,
};

use async_trait::async_trait;
use chrono::{DateTime, Utc};
use quant_pivot_error::{QuantError, QuantResult};
use quant_pivot_models::{
    config::FinalizedExchangeHistoryConfig,
    domain::{
        api::{
            ExchangeHistorySourceView, ExecutionHistoryCoverageView, MissingReasonCountView,
            NegRiskEventDriftView, NegRiskLegView, ParticipantConcentrationDetailView,
            ParticipantConcentrationMarketView, ParticipantConcentrationParticipantView,
            ParticipantConcentrationSummaryView,
        },
        data_plane::{
            DecisionBoundary, DecisionClock, DecisionSource, ExchangeHistoryFrontier,
            ExecutionParticipantPrint, ExecutionParticipantRole,
        },
        market::{MarketRegistryInfo, registry::NegRiskLeg},
        ports::{PolicySnapshotPort, StructuralMonitorPort},
    },
    types::{EventId, MarketId, Price},
};
use quant_pivot_repository::traits::ExchangeHistoryRepository;
use quant_pivot_research::{
    execution_history::participant_concentration::{
        ConcentrationCompositeWeights, ConcentrationMissing, ParticipantConcentrationGate,
        composite_concentration, compute_concentration,
    },
    selection::SelectedMarket,
};
use rust_decimal::Decimal;

use crate::{
    ingest::{book_store::BookStore, market_registry::MarketRegistry},
    prefetch::feature_window::FeatureWindowProvider,
};

const TOP_MARKETS_LIMIT: usize = 50;
const TOP_PARTICIPANTS_LIMIT: usize = 25;

/// Structural monitor backed by live registry/books and attested PIT executions.
pub struct CoreStructuralMonitor {
    market_registry: Arc<MarketRegistry>,
    book_store: Arc<BookStore>,
    feature_windows: Arc<FeatureWindowProvider>,
    exchange_history_repo: Arc<dyn ExchangeHistoryRepository>,
    runtime_config: Arc<dyn PolicySnapshotPort>,
    finalized_exchange_history: FinalizedExchangeHistoryConfig,
}

impl CoreStructuralMonitor {
    #[must_use]
    pub fn new(
        market_registry: Arc<MarketRegistry>,
        book_store: Arc<BookStore>,
        feature_windows: Arc<FeatureWindowProvider>,
        exchange_history_repo: Arc<dyn ExchangeHistoryRepository>,
        runtime_config: Arc<dyn PolicySnapshotPort>,
        finalized_exchange_history: FinalizedExchangeHistoryConfig,
    ) -> Self {
        Self {
            market_registry,
            book_store,
            feature_windows,
            exchange_history_repo,
            runtime_config,
            finalized_exchange_history,
        }
    }

    fn leg_view(&self, leg: &NegRiskLeg) -> NegRiskLegView {
        let best_ask = self
            .book_store
            .load_fresh_by_id(&leg.yes_token_id)
            .ok()
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
            .with_source_cutoff(DecisionSource::FinalizedExecution, 0)?;
        Ok((
            boundary,
            Duration::from_secs(
                runtime
                    .profile_artifacts
                    .features
                    .definition
                    .structural
                    .execution_window_secs,
            ),
        ))
    }

    async fn participant_markets(
        &self,
        markets: Vec<MarketRegistryInfo>,
    ) -> QuantResult<(
        DecisionBoundary,
        Vec<ParticipantConcentrationMarketView>,
        HashMap<MarketId, Vec<ExecutionParticipantPrint>>,
    )> {
        let runtime = self.runtime_config.current();
        let gate = ParticipantConcentrationGate {
            min_unique_participants: runtime
                .profile_artifacts
                .features
                .definition
                .structural
                .execution_min_unique_participants,
            min_notional_usd: parse_decimal(
                "features.structural.execution_min_notional_usd",
                &runtime
                    .profile_artifacts
                    .features
                    .definition
                    .structural
                    .execution_min_notional_usd
                    .value,
            ),
            min_coverage_ratio: parse_decimal(
                "features.structural.execution_min_coverage_ratio",
                &runtime
                    .profile_artifacts
                    .features
                    .definition
                    .structural
                    .execution_min_coverage_ratio
                    .value,
            ),
        };
        let factor = &runtime
            .profile_artifacts
            .scoring
            .definition
            .structural
            .participant_concentration;
        let weights = ConcentrationCompositeWeights {
            gini: parse_decimal(
                "factors.structural.participant_concentration.gini_weight",
                &factor.gini_weight.value,
            ),
            cr1_share: parse_decimal(
                "factors.structural.participant_concentration.cr1_share_weight",
                &factor.cr1_share_weight.value,
            ),
            hhi: parse_decimal(
                "factors.structural.participant_concentration.hhi_weight",
                &factor.hhi_weight.value,
            ),
        };
        let trigger_time = Utc::now();
        let (boundary, lookback) = self.pit_boundary(trigger_time)?;
        let selected = markets
            .iter()
            .map(|market| SelectedMarket {
                market_id: market.market_id.clone(),
                event_id: market.event_id.clone(),
                category: market.primary_category(),
                primary_token_id: market.token_yes.clone(),
                secondary_token_id: Some(market.token_no.clone()),
                liquidity_usd: market.liquidity_usd,
                volume_24h_usd: market.volume_24h,
                source_refs: Vec::new(),
            })
            .collect::<Vec<_>>();
        let windows = self
            .feature_windows
            .load_execution_windows(&selected, &boundary, lookback)
            .await?;
        let accepted = self
            .exchange_history_repo
            .latest_accepted(ExchangeHistoryFrontier::Activation)
            .await?;
        let accepted_through_at = accepted.as_ref().and_then(|row| row.effective_through_at);

        let mut prints_by_market = HashMap::new();
        let views = markets
            .iter()
            .map(|market| {
                let prints = windows
                    .get(&market.market_id)
                    .map(|window| window.prints.clone())
                    .unwrap_or_default();
                prints_by_market.insert(market.market_id.clone(), prints.clone());
                let source_available = self.finalized_exchange_history.enabled
                    && accepted_through_at.is_some_and(|through| {
                        through >= boundary.cutoff_for(DecisionSource::FinalizedExecution)
                    });
                concentration_market_view(
                    market,
                    &prints,
                    None,
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

    async fn execution_history_coverage(&self) -> QuantResult<ExecutionHistoryCoverageView> {
        let trigger_time = Utc::now();
        let (boundary, _) = self.pit_boundary(trigger_time)?;
        let runtime = self.runtime_config.current();
        let active_markets = self.active_markets();
        let active_market_count = u64::try_from(active_markets.len()).unwrap_or(u64::MAX);
        let accepted = self
            .exchange_history_repo
            .latest_accepted(ExchangeHistoryFrontier::Activation)
            .await?;
        let quarantine_count = u64::try_from(
            self.exchange_history_repo
                .list_quarantine(ExchangeHistoryFrontier::Activation, 10_000)
                .await?
                .len(),
        )
        .unwrap_or(u64::MAX);
        let ingest_enabled = self.finalized_exchange_history.enabled;
        let accepted_through_at = accepted.as_ref().and_then(|row| row.effective_through_at);
        let covered = ingest_enabled
            && quarantine_count == 0
            && accepted_through_at.is_some_and(|through| {
                through >= boundary.cutoff_for(DecisionSource::FinalizedExecution)
            });
        let covered_market_ratio = if active_market_count == 0 {
            Decimal::ZERO
        } else if covered {
            Decimal::ONE
        } else {
            Decimal::ZERO
        };
        let missing_markets = if covered { 0 } else { active_market_count };
        let accepted_block = accepted
            .as_ref()
            .and_then(|row| u64::try_from(row.to_block).ok());
        let state = if !ingest_enabled {
            "disabled"
        } else if quarantine_count > 0 {
            "quarantined"
        } else if covered {
            "ready"
        } else {
            "backfilling"
        };
        Ok(ExecutionHistoryCoverageView {
            decision_at: trigger_time,
            knowledge_cutoff: boundary.cutoff_for(DecisionSource::FinalizedExecution),
            window_secs: runtime
                .profile_artifacts
                .features
                .definition
                .structural
                .execution_window_secs,
            knowledge_lag_secs: boundary.knowledge_lag_secs(),
            active_market_count,
            covered_market_ratio,
            source_health: vec![ExchangeHistorySourceView {
                extractor: "hypersync".to_owned(),
                attestor: "independent_polygon_archive_rpc".to_owned(),
                enabled: ingest_enabled,
                state: state.to_owned(),
                accepted_through_block: accepted_block,
                effective_through_at: accepted_through_at,
                quarantine_count,
                last_updated_at: accepted.as_ref().map(|row| row.updated_at),
            }],
            missing_reason_breakdown: reason_counts(
                iter::once((
                    ConcentrationMissing::FinalizedExecutionUnavailable
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
            knowledge_cutoff: boundary.cutoff_for(DecisionSource::FinalizedExecution),
            window_secs: runtime
                .profile_artifacts
                .features
                .definition
                .structural
                .execution_window_secs,
            knowledge_lag_secs: boundary.knowledge_lag_secs(),
            min_unique_participants: runtime
                .profile_artifacts
                .features
                .definition
                .structural
                .execution_min_unique_participants,
            min_notional_usd: parse_decimal(
                "features.structural.execution_min_notional_usd",
                &runtime
                    .profile_artifacts
                    .features
                    .definition
                    .structural
                    .execution_min_notional_usd
                    .value,
            ),
            min_coverage_ratio: parse_decimal(
                "features.structural.execution_min_coverage_ratio",
                &runtime
                    .profile_artifacts
                    .features
                    .definition
                    .structural
                    .execution_min_coverage_ratio
                    .value,
            ),
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
            knowledge_cutoff: boundary.cutoff_for(DecisionSource::FinalizedExecution),
            market: view,
            top_participants,
        }))
    }
}

const fn parse_decimal(_field: &'static str, value: &Decimal) -> Decimal {
    *value
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
    prints: &[ExecutionParticipantPrint],
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
        knowledge_cutoff: boundary.cutoff_for(DecisionSource::FinalizedExecution),
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
    prints: &[ExecutionParticipantPrint],
    limit: usize,
) -> Vec<ParticipantConcentrationParticipantView> {
    #[derive(Default)]
    struct ParticipantAccumulator {
        trade_count: u64,
        notional_usd: Decimal,
    }

    let mut participants = BTreeMap::<(String, String), ParticipantAccumulator>::new();
    for print in prints
        .iter()
        .filter(|print| print.participant_role == ExecutionParticipantRole::Maker)
    {
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

const fn participant_role(role: ExecutionParticipantRole) -> &'static str {
    match role {
        ExecutionParticipantRole::Maker => "maker",
        ExecutionParticipantRole::Taker => "taker",
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

//! Adapter from the canonical policy replay kernel into WORM economic outcome facts.

use std::{
    collections::{BTreeMap, BTreeSet},
    fmt::{Display, Formatter, Result as FmtResult},
    slice,
    sync::Arc,
};

use chrono::{DateTime, Duration, Utc};
use quant_pivot_compute::{ComputeExecutor, OfflineMemory};
use quant_pivot_error::{QuantError, QuantResult, report::ReportError, research::ResearchError};
use quant_pivot_models::{
    clickhouse::{
        BookL2LedgerRow, BookStreamSessionRow, MarketResolutionRow, QuantSignalCandidateEventRow,
    },
    domain::{
        data_plane::{DecisionBoundary, DecisionSource},
        quant::{
            EconomicExitEvidenceKind, EconomicOutcomeCensorReason, EconomicOutcomeDeferredReason,
            EconomicOutcomeReplayContext, EconomicOutcomeTaskClaim,
            NewRecommendationEconomicOutcome, RecommendationEconomicAmounts,
            RecommendationEconomicEvidence, RecommendationEconomicOutcomeInput,
            RecommendationEconomicOutcomePayload, RecommendationEconomicStateDetail,
        },
    },
    enums::{
        clickhouse::{ChCanonicalBookEventType, ChOutcomeSide},
        execution::ExitReason,
        quant::{OutcomeSide, RecommendationEconomicOutcomeState},
    },
    hashing::CanonicalDigest,
    types::{
        Bps, ClobMarketInfoVersion, ConditionTruth, ContentHash, DecisionPolicySnapshotId,
        EconomicTierId, EntryMakerRebateTerms, ModelVersionId, PayoutRatio, Probability,
        RecommendationId, RecommendationPolicyProvenance, RecommendationReportId, ReportRouteRunId,
        ResearchProfileArtifactId, SchemaVersion, Shares, TokenId, TradePolicyArtifactId,
        TradePolicyCandidateSpec, TradePolicyReplayGap, Usd,
    },
};
use quant_pivot_repository::traits::{
    ClobMarketInfoRepository, QuantFactReadRepository, RecommendationEconomicOutcomeRepository,
};
use quant_pivot_research::{
    execution_semantics::{
        PitFeeSchedule, PitMakerRebateEvidence, PitMakerRebateSchedule,
        PitMakerRebateUnavailableReason,
    },
    policy_replay::{
        POLICY_REPLAY_KERNEL_VERSION, PolicyReplayLatency, PolicyReplayObservation,
        PolicyReplayOutcome, PolicyReplaySignal, replay_policy_candidate, replay_policy_horizon,
    },
};
use rust_decimal::Decimal;
use serde::Serialize;
use tokio_util::sync::CancellationToken;
use uuid::Uuid;

use crate::{
    prefetch::replay_page::ReplayPage,
    service::trade_policy_replay::{passive_trades, policy_book, resolution_at},
};

const OUTPUT_DOMAIN: &str = "quant-pivot/recommendation-policy-replay-output";
const OUTPUT_VERSION: u32 = 1;
const SOURCE_CENSOR_DOMAIN: &str = "quant-pivot/recommendation-economic-source-censor";
const SOURCE_CENSOR_VERSION: u32 = 1;
// One immutable replay shares the process-wide offline job/CPU owner. The 4 GiB
// lease covers bounded CH result buffers, typed facts, hashing, and at most two
// million reconstructed ladder cells; no per-recommendation pool is created.
const ECONOMIC_REPLAY_MEMORY_GIB: u32 = 4;
const MAX_ECONOMIC_REPLAY_POINTS: usize = 10_000;
const MAX_ECONOMIC_REPLAY_LEVELS: usize = 2_000_000;

#[derive(Debug, Clone)]
pub struct RecommendationEconomicReplayBinding {
    pub recommendation_id: RecommendationId,
    pub recommendation_report_id: RecommendationReportId,
    pub report_route_run_id: ReportRouteRunId,
    pub decision_policy_snapshot_id: DecisionPolicySnapshotId,
    pub economic_tier_id: EconomicTierId,
    pub model_version_id: ModelVersionId,
    pub trade_policy_artifact_id: TradePolicyArtifactId,
    pub research_profile_artifact_id: ResearchProfileArtifactId,
    pub decision_at: DateTime<Utc>,
    pub horizon_at: DateTime<Utc>,
    pub replay_until: DateTime<Utc>,
    pub resolution_outcome_hash: Option<ContentHash>,
    pub source_cutoff_at: DateTime<Utc>,
    pub source_available_until: DateTime<Utc>,
    pub replay_input_hash: ContentHash,
    pub available_at: DateTime<Utc>,
}

pub struct RecommendationEconomicReplayAdapter;

/// Why a source replay remains incomplete before its immutable cutoff.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum EconomicReplayDeferCause {
    ResolutionFactUnavailable,
    BookReconstruction { detail: String },
    PolicyGap(TradePolicyReplayGap),
}

impl Display for EconomicReplayDeferCause {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> FmtResult {
        match self {
            Self::ResolutionFactUnavailable => formatter.write_str("ResolutionFactUnavailable"),
            Self::BookReconstruction { detail } => {
                write!(formatter, "BookReconstruction: {detail}")
            }
            Self::PolicyGap(gap) => write!(formatter, "PolicyGap: {gap:?}"),
        }
    }
}

impl EconomicReplayDeferCause {
    /// Retain identity, frozen clocks and cause within the existing task-error limit.
    pub(crate) fn task_detail(
        &self,
        binding: &RecommendationEconomicReplayBinding,
        token_id: &TokenId,
        attempt_count: i32,
    ) -> String {
        // The repository's existing retry-task contract allows at most 4096
        // characters. Truncate only the diagnostic, never a replay boundary.
        const MAX_CHARACTERS: usize = 4_096;
        const TRUNCATED: &str = "...[truncated]";
        let detail = format!(
            "{:?} recommendation_id={} token_id={} attempt_count={} decision_at={} horizon_at={} replay_until={} resolution_outcome_hash={:?} source_cutoff_at={} source_available_until={} available_at={} cause={self}",
            EconomicOutcomeDeferredReason::SourceIncompleteBeforeCutoff,
            binding.recommendation_id,
            token_id,
            attempt_count,
            binding.decision_at,
            binding.horizon_at,
            binding.replay_until,
            binding.resolution_outcome_hash,
            binding.source_cutoff_at,
            binding.source_available_until,
            binding.available_at,
        );
        if detail.chars().count() <= MAX_CHARACTERS {
            return detail;
        }
        let mut bounded = detail
            .chars()
            .take(MAX_CHARACTERS - TRUNCATED.len())
            .collect::<String>();
        bounded.push_str(TRUNCATED);
        bounded
    }
}

#[derive(Debug, Clone)]
pub enum RecommendationEconomicReplayAttempt {
    /// Shared offline capacity is busy; no source read or replay was attempted.
    CapacityDeferred,
    Ready {
        binding: RecommendationEconomicReplayBinding,
        replay: Box<PolicyReplayOutcome>,
    },
    Deferred {
        binding: RecommendationEconomicReplayBinding,
        token_id: TokenId,
        cause: EconomicReplayDeferCause,
    },
}

impl RecommendationEconomicReplayAttempt {
    fn resolve(
        binding: RecommendationEconomicReplayBinding,
        token_id: TokenId,
        replay: Result<PolicyReplayOutcome, EconomicReplayDeferCause>,
    ) -> Self {
        let cause = match replay {
            Ok(replay) => {
                if let Some(gap) = replay.gap
                    && CanonicalRecommendationEconomicReplaySource::transient_gap(gap)
                    && binding.source_available_until < binding.source_cutoff_at
                {
                    EconomicReplayDeferCause::PolicyGap(gap)
                } else {
                    return Self::Ready {
                        binding,
                        replay: Box::new(replay),
                    };
                }
            }
            Err(cause) => cause,
        };
        Self::Deferred {
            binding,
            token_id,
            cause,
        }
    }
}

#[async_trait::async_trait]
pub trait RecommendationEconomicReplaySource: Send + Sync {
    async fn replay(
        &self,
        claim: EconomicOutcomeTaskClaim,
        available_through: DateTime<Utc>,
    ) -> QuantResult<RecommendationEconomicReplayAttempt>;
}

#[derive(Serialize)]
struct SourceCensorPreimage {
    recommendation_id: RecommendationId,
    horizon_at: DateTime<Utc>,
    replay_until: DateTime<Utc>,
    resolution_outcome_hash: Option<ContentHash>,
    source_cutoff_at: DateTime<Utc>,
    source_available_until: DateTime<Utc>,
}

#[derive(Serialize)]
struct EconomicReplaySourcePreimage<'a> {
    recommendation_id: RecommendationId,
    horizon_at: DateTime<Utc>,
    replay_until: DateTime<Utc>,
    resolution_outcome_hash: Option<ContentHash>,
    source_cutoff_at: DateTime<Utc>,
    source_available_until: DateTime<Utc>,
    l2_ledger: &'a [BookL2LedgerRow],
    sessions: &'a [BookStreamSessionRow],
    market_info: &'a [ClobMarketInfoVersion],
    signals: &'a [QuantSignalCandidateEventRow],
    resolution: Option<&'a MarketResolutionRow>,
}

struct LoadedEconomicReplay {
    page: ReplayPage,
    signals: Vec<QuantSignalCandidateEventRow>,
}

impl LoadedEconomicReplay {
    fn prepare(&mut self) {
        self.page.l2_ledger.sort_by(|left, right| {
            (
                left.venue_event_time,
                left.stream_session_id,
                left.token_sequence,
                left.event_hash,
            )
                .cmp(&(
                    right.venue_event_time,
                    right.stream_session_id,
                    right.token_sequence,
                    right.event_hash,
                ))
        });
        self.page.snapshots = self
            .page
            .l2_ledger
            .iter()
            .filter(|row| row.event_type == ChCanonicalBookEventType::Snapshot)
            .cloned()
            .collect();
    }
}

/// Production source adapter over immutable `PostgreSQL` lineage and PIT `ClickHouse` facts.
pub struct CanonicalRecommendationEconomicReplaySource {
    outcomes: Arc<dyn RecommendationEconomicOutcomeRepository>,
    facts: Arc<dyn QuantFactReadRepository>,
    market_info: Arc<dyn ClobMarketInfoRepository>,
    compute: Arc<ComputeExecutor>,
}

impl CanonicalRecommendationEconomicReplaySource {
    #[must_use]
    pub const fn new(
        outcomes: Arc<dyn RecommendationEconomicOutcomeRepository>,
        facts: Arc<dyn QuantFactReadRepository>,
        market_info: Arc<dyn ClobMarketInfoRepository>,
        compute: Arc<ComputeExecutor>,
    ) -> Self {
        Self {
            outcomes,
            facts,
            market_info,
            compute,
        }
    }

    fn validate_window(
        claim: EconomicOutcomeTaskClaim,
        decision_at: DateTime<Utc>,
        available_through: DateTime<Utc>,
    ) -> QuantResult<()> {
        let trigger_matches = if claim.resolution_outcome_hash.is_some() {
            claim.replay_until < claim.horizon_at
        } else {
            claim.replay_until == claim.horizon_at
        };
        if decision_at >= claim.replay_until
            || claim.replay_until > available_through
            || available_through > claim.source_cutoff_at
            || available_through != claim.source_available_until
            || claim.source_cutoff_at <= claim.replay_until
            || claim.attempt_count <= 0
            || !trigger_matches
        {
            return Err(
                contract_error("economic claim has an invalid frozen replay window").into(),
            );
        }
        Ok(())
    }

    fn validate_claim(
        context: &EconomicOutcomeReplayContext,
        claim: EconomicOutcomeTaskClaim,
        available_through: DateTime<Utc>,
    ) -> QuantResult<()> {
        Self::validate_window(claim, context.report.decision_at, available_through)?;
        let Some(expected_hash) = claim.resolution_outcome_hash else {
            return Ok(());
        };
        let resolution = context
            .resolution_outcome
            .as_ref()
            .ok_or_else(|| contract_error("early economic claim lost its resolution projection"))?;
        resolution
            .validate()
            .map_err(|error| contract_error(&error.to_string()))?;
        let knowledge_lag =
            context.decision_boundary.decision_at() - context.decision_boundary.knowledge_cutoff();
        let visible_at = resolution
            .resolved_at
            .checked_add_signed(knowledge_lag)
            .ok_or_else(|| contract_error("resolution visibility overflows chrono"))?
            .max(resolution.source_observed_at);
        let forward_observation = resolution.source_observed_at > context.report.decision_at;
        let visible_at_cutoff =
            resolution.available_at <= available_through && visible_at == claim.replay_until;
        if resolution.recommendation_id != claim.recommendation_id
            || resolution.market_id != context.recommendation.market_id
            || resolution.token_id != context.recommendation.token_id
            || resolution.outcome_hash != expected_hash
            || !forward_observation
            || !visible_at_cutoff
        {
            return Err(
                contract_error("early economic claim differs from its frozen resolution").into(),
            );
        }
        Ok(())
    }

    fn verify_resolution(
        context: &EconomicOutcomeReplayContext,
        claim: EconomicOutcomeTaskClaim,
        fact: Option<&MarketResolutionRow>,
    ) -> QuantResult<()> {
        let Some(fact) = fact else {
            return Ok(());
        };
        fact.validate()
            .map_err(|error| contract_error(&error.to_string()))?;
        if claim.resolution_outcome_hash.is_none() {
            return Ok(());
        }
        let resolution = context
            .resolution_outcome
            .as_ref()
            .ok_or_else(|| contract_error("early replay lost its resolution commitment"))?;
        let payout = fact
            .payout_for(&context.recommendation.token_id)
            .map_err(|error| contract_error(&error.to_string()))?;
        let log_index = i64::try_from(fact.source_log_index)
            .map_err(|error| contract_error(&error.to_string()))?;
        if fact.market_id != resolution.market_id
            || fact.resolution_fact_hash != resolution.resolution_fact_hash
            || fact.source_checkpoint_hash != resolution.source_checkpoint_hash
            || log_index != resolution.resolution_fact_log_index
            || resolution.resolution_fact_schema_version != SchemaVersion::FIRST
            || payout != resolution.token_payout_ratio
            || DateTime::from_timestamp_millis(fact.resolved_at) != Some(resolution.resolved_at)
            || DateTime::from_timestamp_millis(fact.observed_at)
                != Some(resolution.source_observed_at)
        {
            return Err(contract_error(
                "canonical resolution source differs from the claimed projection",
            )
            .into());
        }
        Ok(())
    }

    async fn load(
        &self,
        context: &EconomicOutcomeReplayContext,
        claim: EconomicOutcomeTaskClaim,
        available_through: DateTime<Utc>,
    ) -> QuantResult<LoadedEconomicReplay> {
        let token_id = &context.recommendation.token_id;
        let market_id = &context.recommendation.market_id;
        let baseline = self
            .facts
            .book_ledger_snapshot_at(
                token_id,
                context
                    .decision_boundary
                    .cutoff_for(DecisionSource::Book)
                    .timestamp_millis(),
                context.decision_boundary.decision_at().timestamp_millis(),
            )
            .await?;
        let from_ms = baseline.as_ref().map_or_else(
            || {
                context
                    .decision_boundary
                    .knowledge_cutoff()
                    .timestamp_millis()
            },
            |row| row.venue_event_time,
        );
        let to_ms = claim
            .replay_until
            .timestamp_millis()
            .checked_add(1)
            .ok_or_else(|| QuantError::config("economic replay horizon millisecond overflow"))?;
        let mut l2_ledger = self
            .facts
            .book_l2_ledger_between(
                vec![token_id.clone()],
                from_ms,
                to_ms,
                available_through.timestamp_millis(),
            )
            .await?;
        if let Some(baseline) = baseline
            && !l2_ledger.iter().any(|row| {
                row.stream_session_id == baseline.stream_session_id
                    && row.token_sequence == baseline.token_sequence
                    && row.event_hash == baseline.event_hash
            })
        {
            l2_ledger.push(baseline);
        }
        let session_ids = l2_ledger
            .iter()
            .map(|row| row.stream_session_id)
            .collect::<BTreeSet<Uuid>>()
            .into_iter()
            .collect::<Vec<_>>();
        let sessions = self
            .facts
            .book_stream_sessions(session_ids, available_through.timestamp_millis())
            .await?;
        let effective_to = claim
            .replay_until
            .checked_add_signed(Duration::milliseconds(1))
            .ok_or_else(|| QuantError::config("economic market-info horizon overflow"))?;
        let market_info = self
            .market_info
            .window(
                slice::from_ref(market_id),
                context.decision_boundary.knowledge_cutoff(),
                effective_to,
                available_through,
            )
            .await?;
        let signals = self
            .facts
            .signal_candidates_between(
                token_id,
                &context.recommendation.evidence_refs.model_version_id,
                context.report.decision_at.timestamp_millis(),
                claim.replay_until.timestamp_millis(),
                available_through.timestamp_millis(),
            )
            .await?;
        let resolution = self
            .facts
            .resolution_at(
                market_id,
                claim.replay_until.timestamp_millis(),
                available_through.timestamp_millis(),
            )
            .await?;
        Self::verify_resolution(context, claim, resolution.as_ref())?;
        Ok(LoadedEconomicReplay {
            page: ReplayPage {
                market_ids: vec![market_id.clone()],
                token_ids: vec![token_id.clone()],
                catalog_markets: Vec::new(),
                catalog_events: Vec::new(),
                clob_market_info: market_info,
                snapshots: Vec::new(),
                sessions,
                gaps: Vec::new(),
                l2_ledger,
                microstructure: Vec::new(),
                finalized_executions: Vec::new(),
                resolutions: resolution.into_iter().collect(),
                linkages: Vec::new(),
                domain_observations: Vec::new(),
                crypto_reports: Vec::new(),
                weather_observations: Vec::new(),
                weather_forecasts: Vec::new(),
            },
            signals,
        })
    }

    fn binding(
        context: &EconomicOutcomeReplayContext,
        claim: EconomicOutcomeTaskClaim,
        available_through: DateTime<Utc>,
        replay_input_hash: ContentHash,
    ) -> QuantResult<RecommendationEconomicReplayBinding> {
        let model_version_id = context
            .route_run
            .model_version_id
            .ok_or_else(|| contract_error("economic replay Route has no frozen model version"))?;
        let trade_policy_artifact_id = context
            .route_run
            .trade_policy_artifact_id
            .ok_or_else(|| contract_error("economic replay Route has no frozen trade policy"))?;
        let research_profile_artifact_id =
            context
                .route_run
                .research_profile_artifact_id
                .clone()
                .ok_or_else(|| contract_error("economic replay Route has no frozen profile"))?;
        if context.recommendation.recommendation_id != claim.recommendation_id
            || context.recommendation.recommendation_report_id
                != context.report.recommendation_report_id
            || context.recommendation.report_route_run_id != context.route_run.report_route_run_id
            || context.recommendation.evidence_refs.model_version_id != model_version_id
            || context.report.decision_at != context.decision_boundary.decision_at()
        {
            return Err(
                contract_error("economic replay PostgreSQL lineage is inconsistent").into(),
            );
        }
        Ok(RecommendationEconomicReplayBinding {
            recommendation_id: claim.recommendation_id,
            recommendation_report_id: context.report.recommendation_report_id,
            report_route_run_id: context.route_run.report_route_run_id,
            decision_policy_snapshot_id: context.report.decision_policy_snapshot_id,
            economic_tier_id: context.recommendation.economic_tier_id,
            model_version_id,
            trade_policy_artifact_id,
            research_profile_artifact_id,
            decision_at: context.report.decision_at,
            horizon_at: claim.horizon_at,
            replay_until: claim.replay_until,
            resolution_outcome_hash: claim.resolution_outcome_hash,
            source_cutoff_at: claim.source_cutoff_at,
            source_available_until: available_through,
            replay_input_hash,
            available_at: available_through,
        })
    }

    fn selected_candidate(
        context: &EconomicOutcomeReplayContext,
    ) -> QuantResult<(&TradePolicyCandidateSpec, Usd)> {
        let RecommendationPolicyProvenance::TradePolicy {
            artifact_id,
            artifact_hash,
            cohort_index,
            cohort_key,
        } = context.recommendation.trade_plan.policy.as_ref()
        else {
            return Err(contract_error(
                "economic replay requires executable Trade Policy provenance",
            )
            .into());
        };
        if *artifact_id != context.trade_policy.artifact_id
            || *artifact_hash != context.trade_policy.content_hash
        {
            return Err(contract_error("recommendation Trade Policy identity drifted").into());
        }
        let index = usize::try_from(*cohort_index)
            .map_err(|error| contract_error(&format!("cohort index overflow: {error}")))?;
        let cohort = context
            .trade_policy
            .payload_json
            .cohorts
            .get(index)
            .ok_or_else(|| contract_error("recommendation Trade Policy cohort is absent"))?;
        if cohort.key != **cohort_key {
            return Err(contract_error("recommendation Trade Policy cohort key drifted").into());
        }
        let candidate = context
            .trade_policy
            .payload_json
            .candidates
            .iter()
            .find(|candidate| candidate.candidate_id == cohort.selected_candidate_id)
            .ok_or_else(|| contract_error("selected Trade Policy candidate is absent"))?;
        Ok((candidate, cohort.key.cash_budget_tier))
    }

    fn latency_ms(context: &EconomicOutcomeReplayContext) -> QuantResult<u64> {
        [
            Some(context.latency_profile.book_age_p95_ms),
            context.latency_profile.decision_prepared_p95_ms,
            context.latency_profile.endpoint_rtt_p95_ms,
            context.latency_profile.market_delay_p95_ms,
        ]
        .into_iter()
        .try_fold(0_u64, |total, value| {
            total
                .checked_add(value.ok_or_else(|| {
                    contract_error("economic replay latency profile is incomplete")
                })?)
                .ok_or_else(|| contract_error("economic replay latency sum overflow").into())
        })
    }

    fn timeline(
        boundary: &DecisionBoundary,
        claim: EconomicOutcomeTaskClaim,
        heartbeat_secs: u64,
        latency_ms: u64,
        page: &ReplayPage,
    ) -> QuantResult<Vec<(DateTime<Utc>, bool)>> {
        let heartbeat = Duration::seconds(
            i64::try_from(heartbeat_secs)
                .map_err(|error| contract_error(&format!("heartbeat overflow: {error}")))?,
        );
        if heartbeat <= Duration::zero() {
            return Err(contract_error("economic replay heartbeat must be positive").into());
        }
        let action_delay = Duration::milliseconds(
            i64::try_from(latency_ms)
                .map_err(|error| contract_error(&format!("latency overflow: {error}")))?,
        );
        let mut timeline = BTreeMap::new();
        let mut tick = boundary.decision_at();
        while tick <= claim.replay_until {
            if timeline.len() >= MAX_ECONOMIC_REPLAY_POINTS {
                return Err(
                    contract_error("economic replay exceeds its decision-point budget").into(),
                );
            }
            timeline.insert(tick, true);
            if let Some(action_at) = tick.checked_add_signed(action_delay)
                && action_at <= claim.replay_until
            {
                timeline.entry(action_at).or_insert(false);
            }
            tick = tick
                .checked_add_signed(heartbeat)
                .ok_or_else(|| contract_error("economic replay heartbeat overflowed UTC"))?;
        }
        timeline.insert(claim.replay_until, true);
        for resolution in &page.resolutions {
            let resolved_at = DateTime::from_timestamp_millis(resolution.resolved_at)
                .ok_or_else(|| contract_error("resolution timestamp is outside chrono range"))?;
            let observed_at = DateTime::from_timestamp_millis(resolution.observed_at)
                .ok_or_else(|| contract_error("resolution observation is outside chrono range"))?;
            let knowledge_lag = boundary.decision_at() - boundary.knowledge_cutoff();
            let visible_at = resolved_at
                .checked_add_signed(knowledge_lag)
                .ok_or_else(|| contract_error("resolution visibility overflows chrono"))?
                .max(observed_at);
            if visible_at >= boundary.decision_at() && visible_at <= claim.replay_until {
                timeline.entry(visible_at).or_insert(false);
            }
        }
        Ok(timeline.into_iter().collect())
    }

    fn signal_at(
        signals: &[QuantSignalCandidateEventRow],
        context: &EconomicOutcomeReplayContext,
        at: DateTime<Utc>,
    ) -> Option<PolicyReplaySignal> {
        let valid_from = at.checked_sub_signed(Duration::seconds(
            i64::try_from(context.profile_spec.decision_cadence_secs).ok()?,
        ))?;
        let row = signals
            .iter()
            .filter(|row| {
                row.rejection_reason.is_empty()
                    && row.event_time >= valid_from.timestamp_millis()
                    && row.event_time <= at.timestamp_millis()
                    && row.ingestion_time <= at.timestamp_millis()
            })
            .max_by(|left, right| {
                (left.event_time, left.ingestion_time)
                    .cmp(&(right.event_time, right.ingestion_time))
                    .then_with(|| {
                        left.signal_candidate_id
                            .to_string()
                            .cmp(&right.signal_candidate_id.to_string())
                    })
            })?;
        Some(PolicyReplaySignal {
            token_id: row.token_id.clone(),
            outcome_side: match row.side {
                ChOutcomeSide::Yes => OutcomeSide::Yes,
                ChOutcomeSide::No => OutcomeSide::No,
            },
            composite_score: Probability::from(row.score).inner(),
            expected_return_bps: Bps::from(row.expected_return_bps).inner(),
            route_gate_eligible: true,
            opportunistic_confidence: None,
            opportunistic_expected_alpha_bps: None,
            opportunistic_p_exit_better: None,
        })
    }

    fn maker_rebate_evidence(
        context: &EconomicOutcomeReplayContext,
        at: DateTime<Utc>,
    ) -> QuantResult<PitMakerRebateEvidence> {
        Ok(
            match context.recommendation.trade_plan.sizing.maker_rebate_terms {
                EntryMakerRebateTerms::AggressiveNotApplicable => {
                    PitMakerRebateEvidence::NoProgram {
                        terms_hash: CanonicalDigest::content_hash_typed(
                            "quant-pivot/aggressive-maker-rebate-not-applicable",
                            1,
                            &context.recommendation.recommendation_id,
                        )?,
                        available_at: context.report.decision_at,
                    }
                }
                EntryMakerRebateTerms::PassiveNoProgram {
                    terms_hash,
                    available_at,
                } if available_at <= at => PitMakerRebateEvidence::NoProgram {
                    terms_hash,
                    available_at,
                },
                EntryMakerRebateTerms::PassiveProgram { schedule }
                    if schedule.available_at <= at =>
                {
                    PitMakerRebateEvidence::Available {
                        schedule: PitMakerRebateSchedule {
                            terms_hash: schedule.terms_hash,
                            available_at: schedule.available_at,
                            platform_rate: schedule.platform_rate,
                            exponent: schedule.exponent,
                            taker_only: schedule.taker_only,
                            rebate_rate: schedule.rebate_rate,
                        },
                    }
                }
                terms => PitMakerRebateEvidence::Unavailable {
                    reason: PitMakerRebateUnavailableReason::NotPointInTime,
                    terms_hash: terms
                        .passive_terms_hash()
                        .unwrap_or_else(|| ContentHash::from_bytes([0; 32])),
                    available_at: at,
                },
            },
        )
    }

    fn replay_loaded(
        context: &EconomicOutcomeReplayContext,
        claim: EconomicOutcomeTaskClaim,
        loaded: &LoadedEconomicReplay,
        cancel: &CancellationToken,
    ) -> QuantResult<Result<PolicyReplayOutcome, EconomicReplayDeferCause>> {
        let (candidate, cash_budget) = Self::selected_candidate(context)?;
        let latency_ms = Self::latency_ms(context)?;
        let timeline = Self::timeline(
            &context.decision_boundary,
            claim,
            context.profile_spec.exit_heartbeat_secs,
            latency_ms,
            &loaded.page,
        )?;
        Self::validate_compute(loaded, timeline.len())?;
        if cancel.is_cancelled() {
            return Err(ResearchError::Cancelled {
                detail: "economic replay cancelled before book reconstruction".to_owned(),
            }
            .into());
        }
        let boundaries = timeline
            .iter()
            .map(|(at, _)| context.decision_boundary.rebased(*at))
            .collect::<QuantResult<Vec<_>>>()?;
        let books = match loaded
            .page
            .books_at_boundaries(&context.recommendation.token_id, &boundaries)
        {
            Ok(books) => books,
            Err(error) => {
                return Ok(Err(EconomicReplayDeferCause::BookReconstruction {
                    detail: error.to_string(),
                }));
            }
        };
        let mut previous_at = context.report.decision_at - Duration::milliseconds(1);
        let mut previous_session_id = None;
        let mut observations = Vec::with_capacity(timeline.len());
        for (((at, decision_tick), boundary), book) in timeline.iter().zip(&boundaries).zip(books) {
            if cancel.is_cancelled() {
                return Err(ResearchError::Cancelled {
                    detail: "economic replay cancelled while preparing observations".to_owned(),
                }
                .into());
            }
            let book = book.and_then(policy_book);
            let fee_schedule = loaded
                .page
                .market_info_at(
                    &context.recommendation.market_id,
                    &context.recommendation.token_id,
                    boundary,
                )
                .map(ClobMarketInfoVersion::fee_schedule)
                .as_ref()
                .map(PitFeeSchedule::from_market_fee_schedule)
                .transpose()
                .map_err(|error| contract_error(&format!("invalid PIT fee schedule: {error:?}")))?;
            let signal = Self::signal_at(&loaded.signals, context, *at);
            let (trades, trade_coverage) = passive_trades(
                &loaded.page,
                &context.recommendation.token_id,
                previous_at,
                *at,
                boundary,
                book.as_ref().map(|book| book.stream_session_id),
                previous_session_id,
            );
            previous_at = *at;
            previous_session_id = book.as_ref().map(|book| book.stream_session_id);
            observations.push(PolicyReplayObservation {
                at: *at,
                decision_tick: *decision_tick,
                condition_truth: ConditionTruth::Satisfied,
                book,
                fee_schedule,
                maker_rebate_evidence: Self::maker_rebate_evidence(context, *at)?,
                signal,
                passive_trade_coverage: trade_coverage,
                passive_trades: trades,
                resolution: resolution_at(
                    &loaded.page,
                    &context.recommendation.market_id,
                    &context.recommendation.token_id,
                    boundary,
                )?,
            });
        }
        if claim.resolution_outcome_hash.is_some() {
            return replay_policy_candidate(
                candidate,
                context.recommendation.outcome_side,
                cash_budget,
                context.recommendation.market_context.tick_size,
                PolicyReplayLatency {
                    base_delay_ms: latency_ms,
                    stress_multiplier: Decimal::ONE,
                },
                &observations,
            )
            .map(Ok);
        }
        replay_policy_horizon(
            candidate,
            context.recommendation.outcome_side,
            cash_budget,
            context.recommendation.market_context.tick_size,
            PolicyReplayLatency {
                base_delay_ms: latency_ms,
                stress_multiplier: Decimal::ONE,
            },
            &observations,
            claim.horizon_at,
        )
        .map(Ok)
    }

    fn validate_compute(loaded: &LoadedEconomicReplay, point_count: usize) -> QuantResult<()> {
        if point_count > MAX_ECONOMIC_REPLAY_POINTS {
            return Err(contract_error("economic replay exceeds its decision-point budget").into());
        }
        let mut levels = BTreeSet::new();
        for row in &loaded.page.l2_ledger {
            for (ask, prices) in [(false, &row.bid_prices), (true, &row.ask_prices)] {
                for price in prices {
                    levels.insert((ask, price.scaled_i128()));
                    if levels.len().saturating_mul(point_count) > MAX_ECONOMIC_REPLAY_LEVELS {
                        return Err(contract_error(
                            "economic replay exceeds its ladder-memory budget",
                        )
                        .into());
                    }
                }
            }
        }
        Ok(())
    }

    fn replay_prepared(
        context: &EconomicOutcomeReplayContext,
        claim: EconomicOutcomeTaskClaim,
        loaded: &LoadedEconomicReplay,
        cancel: &CancellationToken,
    ) -> QuantResult<RecommendationEconomicReplayAttempt> {
        if cancel.is_cancelled() {
            return Err(ResearchError::Cancelled {
                detail: "economic replay cancelled before hashing".to_owned(),
            }
            .into());
        }
        let replay_input_hash = CanonicalDigest::content_hash_typed(
            "quant-pivot/recommendation-economic-replay-source",
            1,
            &EconomicReplaySourcePreimage {
                recommendation_id: claim.recommendation_id,
                horizon_at: claim.horizon_at,
                replay_until: claim.replay_until,
                resolution_outcome_hash: claim.resolution_outcome_hash,
                source_cutoff_at: claim.source_cutoff_at,
                source_available_until: claim.source_available_until,
                l2_ledger: &loaded.page.l2_ledger,
                sessions: &loaded.page.sessions,
                market_info: &loaded.page.clob_market_info,
                signals: &loaded.signals,
                resolution: loaded.page.resolutions.first(),
            },
        )?;
        let binding = Self::binding(
            context,
            claim,
            claim.source_available_until,
            replay_input_hash,
        )?;
        if claim.resolution_outcome_hash.is_some() && loaded.page.resolutions.is_empty() {
            return Ok(RecommendationEconomicReplayAttempt::Deferred {
                binding,
                token_id: context.recommendation.token_id.clone(),
                cause: EconomicReplayDeferCause::ResolutionFactUnavailable,
            });
        }
        let replay = Self::replay_loaded(context, claim, loaded, cancel)?;
        if cancel.is_cancelled() {
            return Err(ResearchError::Cancelled {
                detail: "economic replay cancelled before returning its result".to_owned(),
            }
            .into());
        }
        Ok(RecommendationEconomicReplayAttempt::resolve(
            binding,
            context.recommendation.token_id.clone(),
            replay,
        ))
    }

    const fn transient_gap(gap: TradePolicyReplayGap) -> bool {
        matches!(
            gap,
            TradePolicyReplayGap::EmptyTimeline
                | TradePolicyReplayGap::EntryConditionUnavailable
                | TradePolicyReplayGap::SignalReinferenceUnavailable
                | TradePolicyReplayGap::EntryBookUnavailable
                | TradePolicyReplayGap::EntryBookStale
                | TradePolicyReplayGap::ExitBookUnavailable
                | TradePolicyReplayGap::ExitBookStale
                | TradePolicyReplayGap::PitFeeScheduleUnavailable
                | TradePolicyReplayGap::PitMakerRebateUnavailable
                | TradePolicyReplayGap::PassiveTradeCoverageUnavailable
                | TradePolicyReplayGap::ResolutionEvidenceUnavailable
                | TradePolicyReplayGap::ResidualPositionUnresolved
        )
    }
}

#[async_trait::async_trait]
impl RecommendationEconomicReplaySource for CanonicalRecommendationEconomicReplaySource {
    async fn replay(
        &self,
        claim: EconomicOutcomeTaskClaim,
        available_through: DateTime<Utc>,
    ) -> QuantResult<RecommendationEconomicReplayAttempt> {
        let Some(admission) = self
            .compute
            .try_acquire_offline(OfflineMemory::try_gib(ECONOMIC_REPLAY_MEMORY_GIB)?)?
        else {
            return Ok(RecommendationEconomicReplayAttempt::CapacityDeferred);
        };
        let cancel = CancellationToken::new();
        let _cancel_on_drop = cancel.clone().drop_guard();
        let context = self
            .outcomes
            .replay_context(&claim.recommendation_id)
            .await?;
        Self::validate_claim(&context, claim, available_through)?;
        let mut loaded = self.load(&context, claim, available_through).await?;
        let compute_cancel = cancel.clone();
        Box::pin(self.compute.run_admitted(admission, move || {
            if compute_cancel.is_cancelled() {
                return Err(ResearchError::Cancelled {
                    detail: "economic replay cancelled before source preparation".to_owned(),
                }
                .into());
            }
            loaded.prepare();
            Self::replay_prepared(&context, claim, &loaded, &compute_cancel)
        }))
        .await
    }
}

impl RecommendationEconomicReplayAdapter {
    pub fn adapt(
        binding: RecommendationEconomicReplayBinding,
        replay: &PolicyReplayOutcome,
    ) -> QuantResult<NewRecommendationEconomicOutcome> {
        binding.validate()?;
        if replay
            .gap
            .is_some_and(CanonicalRecommendationEconomicReplaySource::transient_gap)
            && binding.source_available_until < binding.source_cutoff_at
        {
            return Err(contract_error(
                "incomplete economic replay must remain deferred before its deadline",
            )
            .into());
        }
        if replay
            .entered_at
            .is_some_and(|at| at < binding.decision_at || at > binding.replay_until)
            || replay
                .terminal_at
                .is_some_and(|at| at > binding.replay_until)
            || replay
                .entered_at
                .zip(replay.terminal_at)
                .is_some_and(|(entered, terminal)| terminal < entered)
            || replay
                .fills
                .iter()
                .any(|fill| fill.filled_at > binding.replay_until)
        {
            return Err(contract_error(
                "economic replay produced a fill outside its frozen window",
            )
            .into());
        }
        let (state, detail, exit_evidence_kind) = binding.classify(replay)?;
        let replay_output_hash =
            CanonicalDigest::content_hash_typed(OUTPUT_DOMAIN, OUTPUT_VERSION, replay)
                .map_err(|error| contract_error(&error.to_string()))?;
        NewRecommendationEconomicOutcome::try_seal(RecommendationEconomicOutcomeInput {
            recommendation_id: binding.recommendation_id,
            recommendation_report_id: binding.recommendation_report_id,
            report_route_run_id: binding.report_route_run_id,
            decision_policy_snapshot_id: binding.decision_policy_snapshot_id,
            economic_tier_id: binding.economic_tier_id,
            model_version_id: binding.model_version_id,
            trade_policy_artifact_id: binding.trade_policy_artifact_id,
            research_profile_artifact_id: binding.research_profile_artifact_id,
            state,
            decision_at: binding.decision_at,
            horizon_at: binding.horizon_at,
            source_available_until: binding.source_available_until,
            replay_kernel_version: POLICY_REPLAY_KERNEL_VERSION.to_owned(),
            payload: RecommendationEconomicOutcomePayload {
                detail,
                amounts: Self::amounts(replay, state),
                evidence: RecommendationEconomicEvidence {
                    exit_evidence_kind,
                    full_l2_covered: replay.full_l2_coverage.is_covered(),
                    fee_covered: replay.fee_covered,
                    passive_trade_covered: replay.passive_reconciled_trade_covered,
                    replay_input_hash: binding.replay_input_hash,
                    replay_output_hash,
                },
            },
            available_at: binding.available_at,
        })
        .map_err(|error| contract_error(&error.to_string()).into())
    }

    pub fn censor_unavailable(
        binding: RecommendationEconomicReplayBinding,
    ) -> QuantResult<NewRecommendationEconomicOutcome> {
        binding.validate()?;
        if binding.source_available_until < binding.source_cutoff_at {
            return Err(contract_error(
                "economic source cannot be censored before its frozen deadline",
            )
            .into());
        }
        let replay_output_hash = CanonicalDigest::content_hash_typed(
            SOURCE_CENSOR_DOMAIN,
            SOURCE_CENSOR_VERSION,
            &SourceCensorPreimage {
                recommendation_id: binding.recommendation_id,
                horizon_at: binding.horizon_at,
                replay_until: binding.replay_until,
                resolution_outcome_hash: binding.resolution_outcome_hash,
                source_cutoff_at: binding.source_cutoff_at,
                source_available_until: binding.source_available_until,
            },
        )
        .map_err(|error| contract_error(&error.to_string()))?;
        NewRecommendationEconomicOutcome::try_seal(RecommendationEconomicOutcomeInput {
            recommendation_id: binding.recommendation_id,
            recommendation_report_id: binding.recommendation_report_id,
            report_route_run_id: binding.report_route_run_id,
            decision_policy_snapshot_id: binding.decision_policy_snapshot_id,
            economic_tier_id: binding.economic_tier_id,
            model_version_id: binding.model_version_id,
            trade_policy_artifact_id: binding.trade_policy_artifact_id,
            research_profile_artifact_id: binding.research_profile_artifact_id,
            state: RecommendationEconomicOutcomeState::Censored,
            decision_at: binding.decision_at,
            horizon_at: binding.horizon_at,
            source_available_until: binding.source_available_until,
            replay_kernel_version: POLICY_REPLAY_KERNEL_VERSION.to_owned(),
            payload: RecommendationEconomicOutcomePayload {
                detail: RecommendationEconomicStateDetail::Censored {
                    censored_at: binding.replay_until,
                    reason: EconomicOutcomeCensorReason::SourceUnavailable,
                },
                amounts: RecommendationEconomicAmounts {
                    entry_filled_shares: Shares::ZERO,
                    exited_shares: Shares::ZERO,
                    entry_cost_usd: Usd::ZERO,
                    exit_proceeds_usd: Usd::ZERO,
                    resolution_payout_usd: Usd::ZERO,
                    execution_fee_usd: Usd::ZERO,
                    expected_maker_rebate_usd: Usd::ZERO,
                    net_pnl_usd: None,
                    net_return_bps: None,
                },
                evidence: RecommendationEconomicEvidence {
                    exit_evidence_kind: EconomicExitEvidenceKind::None,
                    full_l2_covered: false,
                    fee_covered: false,
                    passive_trade_covered: None,
                    replay_input_hash: binding.replay_input_hash,
                    replay_output_hash,
                },
            },
            available_at: binding.available_at,
        })
        .map_err(|error| contract_error(&error.to_string()).into())
    }
}

impl RecommendationEconomicReplayBinding {
    fn validate(&self) -> QuantResult<()> {
        let trigger_matches = if self.resolution_outcome_hash.is_some() {
            self.replay_until < self.horizon_at
        } else {
            self.replay_until == self.horizon_at
        };
        if self.decision_at >= self.replay_until
            || self.replay_until > self.source_available_until
            || self.source_available_until > self.source_cutoff_at
            || self.source_cutoff_at <= self.replay_until
            || self.available_at < self.source_available_until
            || !trigger_matches
        {
            return Err(
                contract_error("economic binding has an invalid frozen replay window").into(),
            );
        }
        Ok(())
    }

    fn classify(
        &self,
        replay: &PolicyReplayOutcome,
    ) -> QuantResult<(
        RecommendationEconomicOutcomeState,
        RecommendationEconomicStateDetail,
        EconomicExitEvidenceKind,
    )> {
        if let Some(gap) = replay.gap {
            if gap == TradePolicyReplayGap::EntryNotTriggered {
                return Ok((
                    RecommendationEconomicOutcomeState::EntryNotTriggered,
                    RecommendationEconomicStateDetail::EntryNotTriggered,
                    EconomicExitEvidenceKind::None,
                ));
            }
            return Ok((
                RecommendationEconomicOutcomeState::Censored,
                RecommendationEconomicStateDetail::Censored {
                    censored_at: replay
                        .terminal_at
                        .unwrap_or_else(|| self.source_available_until.min(self.replay_until)),
                    reason: RecommendationEconomicReplayAdapter::censor_reason(gap),
                },
                EconomicExitEvidenceKind::None,
            ));
        }
        let Some(entered_at) = replay.entered_at else {
            let triggered_at = replay
                .entry_triggered_at
                .ok_or_else(|| contract_error("entry-unfilled replay has no trigger time"))?;
            return Ok((
                RecommendationEconomicOutcomeState::EntryUnfilled,
                RecommendationEconomicStateDetail::EntryUnfilled { triggered_at },
                EconomicExitEvidenceKind::None,
            ));
        };
        let terminal_at = replay
            .terminal_at
            .ok_or_else(|| contract_error("uncensored replay has no terminal time"))?;
        let terminal_reason = replay
            .terminal_reason
            .ok_or_else(|| contract_error("uncensored replay has no terminal reason"))?;
        if terminal_reason == ExitReason::ResolutionRedeem {
            let payout_ratio = replay
                .fills
                .iter()
                .rev()
                .find(|fill| fill.exit_reason == Some(ExitReason::ResolutionRedeem))
                .and_then(|fill| fill.vwap)
                .map(|price| PayoutRatio::try_new(price.inner()))
                .transpose()
                .map_err(|error| contract_error(&error.to_string()))?
                .ok_or_else(|| contract_error("resolution replay has no payout ratio"))?;
            return Ok((
                RecommendationEconomicOutcomeState::ResolvedBeforeHorizon,
                RecommendationEconomicStateDetail::ResolvedBeforeHorizon {
                    entered_at: Some(entered_at),
                    resolved_at: terminal_at,
                    payout_ratio,
                },
                EconomicExitEvidenceKind::ResolutionPayout,
            ));
        }
        if terminal_at == self.horizon_at {
            return Ok((
                RecommendationEconomicOutcomeState::HorizonLiquidated,
                RecommendationEconomicStateDetail::HorizonLiquidated {
                    entered_at,
                    liquidated_at: terminal_at,
                },
                EconomicExitEvidenceKind::FullBidLadder,
            ));
        }
        Ok((
            RecommendationEconomicOutcomeState::PolicyExited,
            RecommendationEconomicStateDetail::PolicyExited {
                entered_at,
                exited_at: terminal_at,
                exit_reason: terminal_reason,
            },
            EconomicExitEvidenceKind::PolicyFill,
        ))
    }
}

impl RecommendationEconomicReplayAdapter {
    fn amounts(
        replay: &PolicyReplayOutcome,
        state: RecommendationEconomicOutcomeState,
    ) -> RecommendationEconomicAmounts {
        let entry_cost_usd = replay
            .fills
            .iter()
            .filter(|fill| fill.exit_reason.is_none())
            .map(|fill| fill.gross_amount)
            .sum();
        let resolution_payout_usd = replay
            .fills
            .iter()
            .filter(|fill| fill.exit_reason == Some(ExitReason::ResolutionRedeem))
            .map(|fill| fill.gross_amount)
            .sum();
        let exit_proceeds_usd = replay
            .fills
            .iter()
            .filter(|fill| {
                fill.exit_reason.is_some() && fill.exit_reason != Some(ExitReason::ResolutionRedeem)
            })
            .map(|fill| fill.gross_amount)
            .sum();
        let net_pnl_usd = (state != RecommendationEconomicOutcomeState::Censored).then(|| {
            exit_proceeds_usd + resolution_payout_usd + replay.expected_maker_rebate_accrual_usd
                - entry_cost_usd
                - replay.execution_fee_usd
        });
        RecommendationEconomicAmounts {
            entry_filled_shares: replay.entry_filled_shares,
            exited_shares: replay.exited_shares,
            entry_cost_usd,
            exit_proceeds_usd,
            resolution_payout_usd,
            execution_fee_usd: replay.execution_fee_usd,
            expected_maker_rebate_usd: replay.expected_maker_rebate_accrual_usd,
            net_pnl_usd,
            net_return_bps: replay.expected_net_return_bps,
        }
    }
    const fn censor_reason(gap: TradePolicyReplayGap) -> EconomicOutcomeCensorReason {
        match gap {
            TradePolicyReplayGap::EmptyTimeline
            | TradePolicyReplayGap::EntryConditionUnavailable
            | TradePolicyReplayGap::SignalReinferenceUnavailable => {
                EconomicOutcomeCensorReason::SourceUnavailable
            }
            TradePolicyReplayGap::NonMonotonicTimeline => {
                EconomicOutcomeCensorReason::ContractMismatch
            }
            TradePolicyReplayGap::EntryBookUnavailable
            | TradePolicyReplayGap::ExitBookUnavailable => {
                EconomicOutcomeCensorReason::BookUnavailable
            }
            TradePolicyReplayGap::EntryBookStale | TradePolicyReplayGap::ExitBookStale => {
                EconomicOutcomeCensorReason::BookStale
            }
            TradePolicyReplayGap::PitFeeScheduleUnavailable
            | TradePolicyReplayGap::PitMakerRebateUnavailable => {
                EconomicOutcomeCensorReason::FeeUnavailable
            }
            TradePolicyReplayGap::PassiveTradeCoverageUnavailable => {
                EconomicOutcomeCensorReason::PassiveTradeCoverageUnavailable
            }
            TradePolicyReplayGap::EntryNotTriggered
            | TradePolicyReplayGap::EntryDepthInsufficient
            | TradePolicyReplayGap::PassiveTermsDrift
            | TradePolicyReplayGap::PassiveCancelFillRace
            | TradePolicyReplayGap::ExitDepthInsufficient
            | TradePolicyReplayGap::ResolutionEvidenceUnavailable
            | TradePolicyReplayGap::ResidualPositionUnresolved => {
                EconomicOutcomeCensorReason::ReplayGap
            }
        }
    }
}

fn contract_error(detail: &str) -> ReportError {
    ReportError::ContractViolation {
        detail: format!("recommendation economic replay: {detail}"),
    }
}

#[cfg(test)]
mod tests {
    use chrono::{Duration, TimeZone, Utc};
    use quant_pivot_models::{
        clickhouse::{MarketResolutionFactInput, MarketResolutionRow},
        domain::{
            data_plane::{DecisionBoundary, DecisionClock},
            quant::EconomicOutcomeTaskClaim,
        },
        enums::quant::{OutcomeSide, RecommendationEconomicOutcomeState},
        types::{
            Bps, ContentHash, DecisionPolicySnapshotId, EconomicTierId, EvmBlockHash,
            EvmTransactionHash, MarketId, ModelVersionId, PayoutRatio, RecommendationId,
            RecommendationReportId, ReportRouteRunId, ResearchProfileId, ResearchProfileRef,
            Shares, TokenId, TradePolicyArtifactId, TradePolicyReplayGap, Usd,
            trade_policy_evidence::TradePolicyEvidenceCoverage,
        },
    };
    use quant_pivot_research::policy_replay::{PolicyReplayLatency, PolicyReplayOutcome};
    use rust_decimal::Decimal;

    use super::{
        CanonicalRecommendationEconomicReplaySource, EconomicReplayDeferCause,
        LoadedEconomicReplay, MAX_ECONOMIC_REPLAY_POINTS, RecommendationEconomicReplayAdapter,
        RecommendationEconomicReplayAttempt, RecommendationEconomicReplayBinding,
    };
    use crate::prefetch::replay_page::ReplayPage;

    struct ReplayWindowFixture {
        boundary: DecisionBoundary,
        claim: EconomicOutcomeTaskClaim,
        page: ReplayPage,
    }

    impl ReplayWindowFixture {
        fn new() -> Self {
            let decision_at = Utc.timestamp_opt(1_000, 0).single().expect("decision time");
            let replay_until = decision_at + Duration::seconds(12);
            let claim = EconomicOutcomeTaskClaim {
                recommendation_id: RecommendationId::from_v7(),
                horizon_at: decision_at + Duration::hours(1),
                replay_until,
                resolution_outcome_hash: Some(ContentHash::from_bytes([7; 32])),
                source_cutoff_at: replay_until + Duration::minutes(1),
                source_available_until: replay_until,
                attempt_count: 1,
            };
            let resolution = MarketResolutionRow::seal(MarketResolutionFactInput {
                market_id: MarketId::new("economic-window"),
                token_ids: [TokenId::new("101"), TokenId::new("202")],
                payout_ratios: [PayoutRatio::ONE, PayoutRatio::ZERO],
                resolved_at: (decision_at + Duration::seconds(2)).timestamp_millis(),
                observed_at: (decision_at + Duration::seconds(3)).timestamp_millis(),
                source_block_number: 42,
                source_block_hash: EvmBlockHash::parse(format!("0x{}", "11".repeat(32)))
                    .expect("block hash"),
                source_transaction_hash: EvmTransactionHash::parse(format!(
                    "0x{}",
                    "22".repeat(32)
                ))
                .expect("transaction hash"),
                source_log_index: 3,
                source_checkpoint_hash: ContentHash::from_bytes([4; 32]),
            })
            .expect("canonical resolution");
            Self {
                boundary: DecisionClock::new(10)
                    .boundary(decision_at)
                    .expect("decision boundary"),
                claim,
                page: ReplayPage {
                    market_ids: vec![resolution.market_id.clone()],
                    token_ids: resolution.token_ids.clone(),
                    catalog_markets: Vec::new(),
                    catalog_events: Vec::new(),
                    clob_market_info: Vec::new(),
                    snapshots: Vec::new(),
                    sessions: Vec::new(),
                    gaps: Vec::new(),
                    l2_ledger: Vec::new(),
                    microstructure: Vec::new(),
                    finalized_executions: Vec::new(),
                    resolutions: vec![resolution],
                    linkages: Vec::new(),
                    domain_observations: Vec::new(),
                    crypto_reports: Vec::new(),
                    weather_observations: Vec::new(),
                    weather_forecasts: Vec::new(),
                },
            }
        }

        fn binding(&self) -> RecommendationEconomicReplayBinding {
            RecommendationEconomicReplayBinding {
                recommendation_id: self.claim.recommendation_id,
                recommendation_report_id: RecommendationReportId::from_v7(),
                report_route_run_id: ReportRouteRunId::from_v7(),
                decision_policy_snapshot_id: DecisionPolicySnapshotId::from_v7(),
                economic_tier_id: EconomicTierId::from_v7(),
                model_version_id: ModelVersionId::from_v7(),
                trade_policy_artifact_id: TradePolicyArtifactId::from_v7(),
                research_profile_artifact_id: ResearchProfileRef {
                    id: ResearchProfileId::new("economic-window"),
                    version: 1,
                    content_hash: ContentHash::from_bytes([8; 32]),
                }
                .artifact_id(),
                decision_at: self.boundary.decision_at(),
                horizon_at: self.claim.horizon_at,
                replay_until: self.claim.replay_until,
                resolution_outcome_hash: self.claim.resolution_outcome_hash,
                source_cutoff_at: self.claim.source_cutoff_at,
                source_available_until: self.claim.source_available_until,
                replay_input_hash: ContentHash::from_bytes([9; 32]),
                available_at: self.claim.source_available_until,
            }
        }

        fn replay_gap(&self) -> PolicyReplayOutcome {
            PolicyReplayOutcome {
                candidate_id: self.claim.recommendation_id.to_string(),
                outcome_side: OutcomeSide::Yes,
                cash_budget: Usd::new(Decimal::TEN),
                latency: PolicyReplayLatency {
                    base_delay_ms: 10,
                    stress_multiplier: Decimal::ONE,
                },
                entry_triggered_at: None,
                entered_at: None,
                terminal_at: None,
                terminal_reason: None,
                entry_fill_ratio: Decimal::ZERO,
                entry_fill_latency_ms: None,
                post_fill_markout_bps: None::<Bps>,
                exit_fill_ratio: Decimal::ZERO,
                entry_filled_shares: Shares::ZERO,
                exited_shares: Shares::ZERO,
                execution_fee_usd: Usd::ZERO,
                expected_maker_rebate_accrual_usd: Usd::ZERO,
                expected_net_return_bps: None,
                risk_net_return_bps: None,
                ambiguous_touch: false,
                full_l2_coverage: TradePolicyEvidenceCoverage::Covered,
                fee_covered: true,
                passive_rebate_evidence_coverage: TradePolicyEvidenceCoverage::NotRequired,
                passive_reconciled_trade_covered: None,
                gap: Some(TradePolicyReplayGap::EntryNotTriggered),
                fills: Vec::new(),
            }
        }
    }

    #[test]
    fn terminal_window_is_bounded() {
        let fixture = ReplayWindowFixture::new();
        CanonicalRecommendationEconomicReplaySource::validate_window(
            fixture.claim,
            fixture.boundary.decision_at(),
            fixture.claim.source_available_until,
        )
        .expect("visible terminal window");
        let timeline = CanonicalRecommendationEconomicReplaySource::timeline(
            &fixture.boundary,
            fixture.claim,
            5,
            500,
            &fixture.page,
        )
        .expect("terminal timeline");
        assert_eq!(
            timeline.last().map(|point| point.0),
            Some(fixture.claim.replay_until)
        );
        assert!(
            timeline
                .iter()
                .all(|point| point.0 <= fixture.claim.source_available_until)
        );
        assert!(
            !timeline
                .iter()
                .any(|point| point.0 == fixture.claim.horizon_at)
        );
        assert!(
            !timeline
                .iter()
                .any(|point| point.0 == fixture.boundary.decision_at() + Duration::seconds(3))
        );
    }

    #[test]
    fn claim_rejects_future_window() {
        let fixture = ReplayWindowFixture::new();
        let claim = fixture.claim;
        for invalid in [
            EconomicOutcomeTaskClaim {
                source_available_until: claim.replay_until - Duration::seconds(1),
                ..claim
            },
            EconomicOutcomeTaskClaim {
                resolution_outcome_hash: None,
                ..claim
            },
            EconomicOutcomeTaskClaim {
                replay_until: claim.horizon_at,
                ..claim
            },
            EconomicOutcomeTaskClaim {
                source_cutoff_at: claim.replay_until,
                ..claim
            },
            EconomicOutcomeTaskClaim {
                attempt_count: 0,
                ..claim
            },
        ] {
            assert!(
                CanonicalRecommendationEconomicReplaySource::validate_window(
                    invalid,
                    fixture.boundary.decision_at(),
                    invalid.source_available_until,
                )
                .is_err()
            );
        }
    }

    #[test]
    fn source_censor_waits_deadline() {
        let fixture = ReplayWindowFixture::new();
        assert!(
            RecommendationEconomicReplayAdapter::censor_unavailable(fixture.binding()).is_err()
        );
        let mut binding = fixture.binding();
        binding.source_available_until = binding.source_cutoff_at;
        binding.available_at = binding.source_cutoff_at;
        let outcome = RecommendationEconomicReplayAdapter::censor_unavailable(binding)
            .expect("source deadline creates truthful censor");
        assert_eq!(
            outcome.payload_json.detail.terminal_at(),
            Some(fixture.claim.replay_until)
        );
        assert!(
            outcome
                .payload_json
                .detail
                .terminal_at()
                .is_some_and(|at| at <= outcome.available_at)
        );
        assert!(
            outcome.horizon_at > outcome.available_at,
            "profile horizon remains unchanged"
        );
    }

    #[test]
    fn compute_points_are_bounded() {
        let fixture = ReplayWindowFixture::new();
        let loaded = LoadedEconomicReplay {
            page: fixture.page,
            signals: Vec::new(),
        };
        assert!(
            CanonicalRecommendationEconomicReplaySource::validate_compute(
                &loaded,
                MAX_ECONOMIC_REPLAY_POINTS + 1,
            )
            .is_err()
        );
        let mut long_claim = fixture.claim;
        long_claim.horizon_at = fixture.boundary.decision_at() + Duration::days(1);
        long_claim.replay_until = long_claim.horizon_at;
        long_claim.resolution_outcome_hash = None;
        assert!(
            CanonicalRecommendationEconomicReplaySource::timeline(
                &fixture.boundary,
                long_claim,
                1,
                500,
                &loaded.page,
            )
            .is_err()
        );
    }

    #[test]
    fn binding_rejects_clock_drift() {
        let fixture = ReplayWindowFixture::new();
        let mut binding = fixture.binding();
        binding.available_at = binding.source_available_until - Duration::milliseconds(1);
        assert!(binding.validate().is_err());
        let mut binding = fixture.binding();
        binding.source_available_until = binding.source_cutoff_at + Duration::milliseconds(1);
        binding.available_at = binding.source_available_until;
        assert!(binding.validate().is_err());
    }

    #[test]
    fn entry_gap_maps_directly() {
        let fixture = ReplayWindowFixture::new();
        let outcome =
            RecommendationEconomicReplayAdapter::adapt(fixture.binding(), &fixture.replay_gap());
        assert!(outcome.is_ok_and(|outcome| {
            outcome.state == RecommendationEconomicOutcomeState::EntryNotTriggered
        }));
    }

    #[test]
    fn transient_gap_stays_deferred() {
        let fixture = ReplayWindowFixture::new();
        let mut replay = fixture.replay_gap();
        replay.gap = Some(TradePolicyReplayGap::EntryBookUnavailable);
        assert!(RecommendationEconomicReplayAdapter::adapt(fixture.binding(), &replay).is_err());
        let mut binding = fixture.binding();
        binding.source_available_until = binding.source_cutoff_at;
        binding.available_at = binding.source_cutoff_at;
        let outcome = RecommendationEconomicReplayAdapter::adapt(binding, &replay)
            .expect("transient source gap can be censored only at its frozen deadline");
        assert_eq!(outcome.state, RecommendationEconomicOutcomeState::Censored);
        assert_eq!(
            outcome.payload_json.detail.terminal_at(),
            Some(fixture.claim.replay_until)
        );
    }

    #[test]
    fn deferred_causes_retain_semantics() {
        let fixture = ReplayWindowFixture::new();
        let token_id = TokenId::new("101");
        for cause in [
            EconomicReplayDeferCause::ResolutionFactUnavailable,
            EconomicReplayDeferCause::BookReconstruction {
                detail: "Source Slice session original failure".to_owned(),
            },
        ] {
            for at_cutoff in [false, true] {
                let mut binding = fixture.binding();
                if at_cutoff {
                    binding.source_available_until = binding.source_cutoff_at;
                    binding.available_at = binding.source_cutoff_at;
                }
                let expected_cutoff = binding.source_cutoff_at;
                let attempt = RecommendationEconomicReplayAttempt::resolve(
                    binding,
                    token_id.clone(),
                    Err(cause.clone()),
                );
                let RecommendationEconomicReplayAttempt::Deferred {
                    binding,
                    token_id: actual_token,
                    cause: actual_cause,
                } = attempt
                else {
                    panic!("missing source remains deferred for worker cutoff handling")
                };
                assert_eq!(actual_cause, cause);
                assert_eq!(actual_token, token_id);
                assert_eq!(binding.source_cutoff_at, expected_cutoff);
                assert_eq!(binding.replay_until, fixture.claim.replay_until);
                let censor = RecommendationEconomicReplayAdapter::censor_unavailable(binding);
                assert_eq!(censor.is_ok(), at_cutoff);
            }
        }
    }

    #[test]
    fn policy_gap_preserves_cutoff() {
        let fixture = ReplayWindowFixture::new();
        for gap in [
            TradePolicyReplayGap::EntryBookStale,
            TradePolicyReplayGap::ExitBookUnavailable,
        ] {
            let mut replay = fixture.replay_gap();
            replay.gap = Some(gap);
            let attempt = RecommendationEconomicReplayAttempt::resolve(
                fixture.binding(),
                TokenId::new("101"),
                Ok(replay.clone()),
            );
            assert!(
                matches!(attempt, RecommendationEconomicReplayAttempt::Deferred {
                cause: EconomicReplayDeferCause::PolicyGap(actual), ..
            } if actual == gap)
            );
            let mut binding = fixture.binding();
            binding.source_available_until = binding.source_cutoff_at;
            binding.available_at = binding.source_cutoff_at;
            let RecommendationEconomicReplayAttempt::Ready { binding, replay } =
                RecommendationEconomicReplayAttempt::resolve(
                    binding,
                    TokenId::new("101"),
                    Ok(replay),
                )
            else {
                panic!("source deadline retains normal replay-to-censor adaptation")
            };
            let outcome = RecommendationEconomicReplayAdapter::adapt(binding, &replay)
                .expect("cutoff permits the original censor path");
            assert_eq!(outcome.state, RecommendationEconomicOutcomeState::Censored);
        }
        let attempt = RecommendationEconomicReplayAttempt::resolve(
            fixture.binding(),
            TokenId::new("101"),
            Ok(fixture.replay_gap()),
        );
        let RecommendationEconomicReplayAttempt::Ready { binding, replay } = attempt else {
            panic!("non-transient entry gap remains ready without waiting")
        };
        assert_eq!(
            RecommendationEconomicReplayAdapter::adapt(binding, &replay)
                .expect("non-transient outcome")
                .state,
            RecommendationEconomicOutcomeState::EntryNotTriggered
        );
    }

    #[test]
    fn deferred_detail_is_bounded() {
        let fixture = ReplayWindowFixture::new();
        let binding = fixture.binding();
        let token_id = TokenId::new("101");
        let cause = EconomicReplayDeferCause::PolicyGap(TradePolicyReplayGap::EntryBookStale);
        let detail = cause.task_detail(&binding, &token_id, 3);
        for expected in [
            "SourceIncompleteBeforeCutoff".to_owned(),
            format!("recommendation_id={}", binding.recommendation_id),
            "token_id=101".to_owned(),
            "attempt_count=3".to_owned(),
            format!("decision_at={}", binding.decision_at),
            format!("horizon_at={}", binding.horizon_at),
            format!("replay_until={}", binding.replay_until),
            format!("source_cutoff_at={}", binding.source_cutoff_at),
            format!("source_available_until={}", binding.source_available_until),
            "cause=PolicyGap: EntryBookStale".to_owned(),
        ] {
            assert!(detail.contains(&expected), "missing {expected}: {detail}");
        }
        let original = "Source Slice original non-monotonic book sequence";
        let cause = EconomicReplayDeferCause::BookReconstruction {
            detail: original.to_owned(),
        };
        assert!(
            cause
                .task_detail(&binding, &token_id, 1)
                .ends_with(original)
        );
        let cause = EconomicReplayDeferCause::BookReconstruction {
            detail: "错".repeat(5_000),
        };
        let bounded = cause.task_detail(&binding, &token_id, 1);
        assert_eq!(bounded.chars().count(), 4_096);
        assert!(bounded.ends_with("...[truncated]"));
        assert!(bounded.contains("cause=BookReconstruction: "));
    }
}

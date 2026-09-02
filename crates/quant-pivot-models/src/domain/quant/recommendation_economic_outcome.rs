//! Immutable recommendation-level executable economic outcome contract.

use chrono::{DateTime, Utc};
use rust_decimal::Decimal;
use schemars::JsonSchema;
use sea_orm::{DeriveIntoActiveModel, DerivePartialModel, FromJsonQueryResult};
use serde::{Deserialize, Serialize};
use thiserror::Error;

use crate::{
    entities::quant_recommendation_economic_outcome,
    enums::{execution::ExitReason, quant::RecommendationEconomicOutcomeState},
    hashing::CanonicalDigest,
    types::{
        ContentHash, DecisionPolicySnapshotId, EconomicTierId, ModelVersionId, PayoutRatio,
        RecommendationId, RecommendationReportId, ReportRouteRunId, ResearchProfileArtifactId,
        Shares, TradePolicyArtifactId, Usd,
    },
};

const OUTCOME_HASH_DOMAIN: &str = "quant-pivot/recommendation-economic-outcome";
const OUTCOME_HASH_VERSION: u32 = 1;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum EconomicExitEvidenceKind {
    None,
    PolicyFill,
    FullBidLadder,
    ResolutionPayout,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum EconomicOutcomeCensorReason {
    SourceUnavailable,
    SourceLate,
    BookUnavailable,
    BookStale,
    FeeUnavailable,
    PassiveTradeCoverageUnavailable,
    ReplayGap,
    ContractMismatch,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum RecommendationEconomicStateDetail {
    EntryNotTriggered,
    EntryUnfilled {
        triggered_at: DateTime<Utc>,
    },
    PolicyExited {
        entered_at: DateTime<Utc>,
        exited_at: DateTime<Utc>,
        exit_reason: ExitReason,
    },
    HorizonLiquidated {
        entered_at: DateTime<Utc>,
        liquidated_at: DateTime<Utc>,
    },
    ResolvedBeforeHorizon {
        entered_at: Option<DateTime<Utc>>,
        resolved_at: DateTime<Utc>,
        payout_ratio: PayoutRatio,
    },
    Censored {
        censored_at: DateTime<Utc>,
        reason: EconomicOutcomeCensorReason,
    },
}

impl RecommendationEconomicStateDetail {
    #[must_use]
    pub const fn state(&self) -> RecommendationEconomicOutcomeState {
        match self {
            Self::EntryNotTriggered => RecommendationEconomicOutcomeState::EntryNotTriggered,
            Self::EntryUnfilled { .. } => RecommendationEconomicOutcomeState::EntryUnfilled,
            Self::PolicyExited { .. } => RecommendationEconomicOutcomeState::PolicyExited,
            Self::HorizonLiquidated { .. } => RecommendationEconomicOutcomeState::HorizonLiquidated,
            Self::ResolvedBeforeHorizon { .. } => {
                RecommendationEconomicOutcomeState::ResolvedBeforeHorizon
            }
            Self::Censored { .. } => RecommendationEconomicOutcomeState::Censored,
        }
    }

    #[must_use]
    pub const fn terminal_at(&self) -> Option<DateTime<Utc>> {
        match self {
            Self::EntryNotTriggered => None,
            Self::EntryUnfilled { triggered_at } => Some(*triggered_at),
            Self::PolicyExited { exited_at, .. } => Some(*exited_at),
            Self::HorizonLiquidated { liquidated_at, .. } => Some(*liquidated_at),
            Self::ResolvedBeforeHorizon { resolved_at, .. } => Some(*resolved_at),
            Self::Censored { censored_at, .. } => Some(*censored_at),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct RecommendationEconomicAmounts {
    pub entry_filled_shares: Shares,
    pub exited_shares: Shares,
    pub entry_cost_usd: Usd,
    pub exit_proceeds_usd: Usd,
    pub resolution_payout_usd: Usd,
    pub execution_fee_usd: Usd,
    pub expected_maker_rebate_usd: Usd,
    pub net_pnl_usd: Option<Usd>,
    #[serde(with = "rust_decimal::serde::str_option")]
    #[schemars(with = "Option<String>")]
    pub net_return_bps: Option<Decimal>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct RecommendationEconomicEvidence {
    pub exit_evidence_kind: EconomicExitEvidenceKind,
    pub full_l2_covered: bool,
    pub fee_covered: bool,
    pub passive_trade_covered: Option<bool>,
    pub replay_input_hash: ContentHash,
    pub replay_output_hash: ContentHash,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, FromJsonQueryResult, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct RecommendationEconomicOutcomePayload {
    pub detail: RecommendationEconomicStateDetail,
    pub amounts: RecommendationEconomicAmounts,
    pub evidence: RecommendationEconomicEvidence,
}

#[derive(Debug, Clone)]
pub struct RecommendationEconomicOutcomeInput {
    pub recommendation_id: RecommendationId,
    pub recommendation_report_id: RecommendationReportId,
    pub report_route_run_id: ReportRouteRunId,
    pub decision_policy_snapshot_id: DecisionPolicySnapshotId,
    pub economic_tier_id: EconomicTierId,
    pub model_version_id: ModelVersionId,
    pub trade_policy_artifact_id: TradePolicyArtifactId,
    pub research_profile_artifact_id: ResearchProfileArtifactId,
    pub state: RecommendationEconomicOutcomeState,
    pub decision_at: DateTime<Utc>,
    pub horizon_at: DateTime<Utc>,
    pub source_available_until: DateTime<Utc>,
    pub replay_kernel_version: String,
    pub payload: RecommendationEconomicOutcomePayload,
    pub available_at: DateTime<Utc>,
}

#[derive(Debug, Error, Clone, PartialEq, Eq)]
pub enum RecommendationEconomicOutcomeError {
    #[error("economic outcome state differs from payload detail")]
    StateMismatch,
    #[error("economic outcome timeline is invalid")]
    InvalidTimeline,
    #[error("economic outcome evidence kind is invalid for state")]
    InvalidEvidence,
    #[error("economic outcome amounts do not reconcile")]
    InvalidAmounts,
    #[error("economic outcome replay kernel version is empty")]
    EmptyKernelVersion,
    #[error("economic outcome hash failed: {0}")]
    Hash(String),
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, DerivePartialModel, JsonSchema)]
#[sea_orm(entity = "quant_recommendation_economic_outcome::Entity")]
pub struct RecommendationEconomicOutcomeInfo {
    pub recommendation_id: RecommendationId,
    pub recommendation_report_id: RecommendationReportId,
    pub report_route_run_id: ReportRouteRunId,
    pub decision_policy_snapshot_id: DecisionPolicySnapshotId,
    pub economic_tier_id: EconomicTierId,
    pub model_version_id: ModelVersionId,
    pub trade_policy_artifact_id: TradePolicyArtifactId,
    pub research_profile_artifact_id: ResearchProfileArtifactId,
    pub state: RecommendationEconomicOutcomeState,
    pub decision_at: DateTime<Utc>,
    pub horizon_at: DateTime<Utc>,
    pub source_available_until: DateTime<Utc>,
    pub replay_kernel_version: String,
    pub payload_json: RecommendationEconomicOutcomePayload,
    pub evidence_hash: ContentHash,
    pub available_at: DateTime<Utc>,
    pub created_at: DateTime<Utc>,
}

impl RecommendationEconomicOutcomeInfo {
    pub fn verify(&self) -> Result<(), RecommendationEconomicOutcomeError> {
        NewRecommendationEconomicOutcome {
            recommendation_id: self.recommendation_id,
            recommendation_report_id: self.recommendation_report_id,
            report_route_run_id: self.report_route_run_id,
            decision_policy_snapshot_id: self.decision_policy_snapshot_id,
            economic_tier_id: self.economic_tier_id,
            model_version_id: self.model_version_id,
            trade_policy_artifact_id: self.trade_policy_artifact_id,
            research_profile_artifact_id: self.research_profile_artifact_id.clone(),
            state: self.state,
            decision_at: self.decision_at,
            horizon_at: self.horizon_at,
            source_available_until: self.source_available_until,
            replay_kernel_version: self.replay_kernel_version.clone(),
            payload_json: self.payload_json.clone(),
            evidence_hash: self.evidence_hash,
            available_at: self.available_at,
        }
        .verify()
    }
}

info_from_model!(
    RecommendationEconomicOutcomeInfo,
    quant_recommendation_economic_outcome::Model,
    {
        recommendation_id,
        recommendation_report_id,
        report_route_run_id,
        decision_policy_snapshot_id,
        economic_tier_id,
        model_version_id,
        trade_policy_artifact_id,
        research_profile_artifact_id,
        state,
        decision_at,
        horizon_at,
        source_available_until,
        replay_kernel_version,
        payload_json,
        evidence_hash,
        available_at,
        created_at,
    }
);

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, DeriveIntoActiveModel)]
#[sea_orm(active_model = "quant_recommendation_economic_outcome::ActiveModel")]
pub struct NewRecommendationEconomicOutcome {
    pub recommendation_id: RecommendationId,
    pub recommendation_report_id: RecommendationReportId,
    pub report_route_run_id: ReportRouteRunId,
    pub decision_policy_snapshot_id: DecisionPolicySnapshotId,
    pub economic_tier_id: EconomicTierId,
    pub model_version_id: ModelVersionId,
    pub trade_policy_artifact_id: TradePolicyArtifactId,
    pub research_profile_artifact_id: ResearchProfileArtifactId,
    pub state: RecommendationEconomicOutcomeState,
    pub decision_at: DateTime<Utc>,
    pub horizon_at: DateTime<Utc>,
    pub source_available_until: DateTime<Utc>,
    pub replay_kernel_version: String,
    pub payload_json: RecommendationEconomicOutcomePayload,
    pub evidence_hash: ContentHash,
    pub available_at: DateTime<Utc>,
}

impl From<RecommendationEconomicOutcomeInfo> for NewRecommendationEconomicOutcome {
    fn from(info: RecommendationEconomicOutcomeInfo) -> Self {
        Self {
            recommendation_id: info.recommendation_id,
            recommendation_report_id: info.recommendation_report_id,
            report_route_run_id: info.report_route_run_id,
            decision_policy_snapshot_id: info.decision_policy_snapshot_id,
            economic_tier_id: info.economic_tier_id,
            model_version_id: info.model_version_id,
            trade_policy_artifact_id: info.trade_policy_artifact_id,
            research_profile_artifact_id: info.research_profile_artifact_id,
            state: info.state,
            decision_at: info.decision_at,
            horizon_at: info.horizon_at,
            source_available_until: info.source_available_until,
            replay_kernel_version: info.replay_kernel_version,
            payload_json: info.payload_json,
            evidence_hash: info.evidence_hash,
            available_at: info.available_at,
        }
    }
}

impl From<NewRecommendationEconomicOutcome> for RecommendationEconomicOutcomeInput {
    fn from(outcome: NewRecommendationEconomicOutcome) -> Self {
        Self {
            recommendation_id: outcome.recommendation_id,
            recommendation_report_id: outcome.recommendation_report_id,
            report_route_run_id: outcome.report_route_run_id,
            decision_policy_snapshot_id: outcome.decision_policy_snapshot_id,
            economic_tier_id: outcome.economic_tier_id,
            model_version_id: outcome.model_version_id,
            trade_policy_artifact_id: outcome.trade_policy_artifact_id,
            research_profile_artifact_id: outcome.research_profile_artifact_id,
            state: outcome.state,
            decision_at: outcome.decision_at,
            horizon_at: outcome.horizon_at,
            source_available_until: outcome.source_available_until,
            replay_kernel_version: outcome.replay_kernel_version,
            payload: outcome.payload_json,
            available_at: outcome.available_at,
        }
    }
}

impl NewRecommendationEconomicOutcome {
    /// Bind repository-assigned availability and reseal the exact immutable payload.
    pub fn with_available_at(
        self,
        available_at: DateTime<Utc>,
    ) -> Result<Self, RecommendationEconomicOutcomeError> {
        self.verify()?;
        let mut input = RecommendationEconomicOutcomeInput::from(self);
        input.available_at = available_at;
        Self::try_seal(input)
    }

    pub fn try_seal(
        input: RecommendationEconomicOutcomeInput,
    ) -> Result<Self, RecommendationEconomicOutcomeError> {
        input.validate()?;
        let evidence_hash = input.evidence_hash()?;
        Ok(Self {
            recommendation_id: input.recommendation_id,
            recommendation_report_id: input.recommendation_report_id,
            report_route_run_id: input.report_route_run_id,
            decision_policy_snapshot_id: input.decision_policy_snapshot_id,
            economic_tier_id: input.economic_tier_id,
            model_version_id: input.model_version_id,
            trade_policy_artifact_id: input.trade_policy_artifact_id,
            research_profile_artifact_id: input.research_profile_artifact_id,
            state: input.state,
            decision_at: input.decision_at,
            horizon_at: input.horizon_at,
            source_available_until: input.source_available_until,
            replay_kernel_version: input.replay_kernel_version,
            payload_json: input.payload,
            evidence_hash,
            available_at: input.available_at,
        })
    }

    pub fn verify(&self) -> Result<(), RecommendationEconomicOutcomeError> {
        let input = RecommendationEconomicOutcomeInput {
            recommendation_id: self.recommendation_id,
            recommendation_report_id: self.recommendation_report_id,
            report_route_run_id: self.report_route_run_id,
            decision_policy_snapshot_id: self.decision_policy_snapshot_id,
            economic_tier_id: self.economic_tier_id,
            model_version_id: self.model_version_id,
            trade_policy_artifact_id: self.trade_policy_artifact_id,
            research_profile_artifact_id: self.research_profile_artifact_id.clone(),
            state: self.state,
            decision_at: self.decision_at,
            horizon_at: self.horizon_at,
            source_available_until: self.source_available_until,
            replay_kernel_version: self.replay_kernel_version.clone(),
            payload: self.payload_json.clone(),
            available_at: self.available_at,
        };
        input.validate()?;
        if input.evidence_hash()? == self.evidence_hash {
            Ok(())
        } else {
            Err(RecommendationEconomicOutcomeError::Hash(
                "stored evidence hash differs from canonical preimage".to_owned(),
            ))
        }
    }
}

impl RecommendationEconomicOutcomeInput {
    fn evidence_hash(&self) -> Result<ContentHash, RecommendationEconomicOutcomeError> {
        CanonicalDigest::content_hash_typed(
            OUTCOME_HASH_DOMAIN,
            OUTCOME_HASH_VERSION,
            &(
                self.recommendation_id,
                self.recommendation_report_id,
                self.report_route_run_id,
                self.decision_policy_snapshot_id,
                self.economic_tier_id,
                self.model_version_id,
                self.trade_policy_artifact_id,
                self.research_profile_artifact_id.clone(),
                self.state,
                self.decision_at,
                self.horizon_at,
                self.source_available_until,
                &self.replay_kernel_version,
                &self.payload,
                self.available_at,
            ),
        )
        .map_err(|error| RecommendationEconomicOutcomeError::Hash(error.to_string()))
    }

    fn validate(&self) -> Result<(), RecommendationEconomicOutcomeError> {
        if self.state != self.payload.detail.state() {
            return Err(RecommendationEconomicOutcomeError::StateMismatch);
        }
        if self.replay_kernel_version.trim().is_empty() {
            return Err(RecommendationEconomicOutcomeError::EmptyKernelVersion);
        }
        if self.decision_at >= self.horizon_at
            || self.available_at < self.source_available_until
            || self.payload.detail.terminal_at().is_some_and(|terminal| {
                terminal < self.decision_at
                    || terminal > self.horizon_at
                    || terminal > self.source_available_until
            })
        {
            return Err(RecommendationEconomicOutcomeError::InvalidTimeline);
        }
        self.validate_evidence()?;
        self.validate_amounts()
    }

    fn validate_evidence(&self) -> Result<(), RecommendationEconomicOutcomeError> {
        let valid = match self.state {
            RecommendationEconomicOutcomeState::EntryNotTriggered
            | RecommendationEconomicOutcomeState::EntryUnfilled => {
                self.payload.evidence.exit_evidence_kind == EconomicExitEvidenceKind::None
            }
            RecommendationEconomicOutcomeState::PolicyExited => {
                self.payload.evidence.exit_evidence_kind == EconomicExitEvidenceKind::PolicyFill
            }
            RecommendationEconomicOutcomeState::HorizonLiquidated => {
                self.payload.evidence.exit_evidence_kind == EconomicExitEvidenceKind::FullBidLadder
                    && self.payload.evidence.full_l2_covered
            }
            RecommendationEconomicOutcomeState::ResolvedBeforeHorizon => {
                self.payload.evidence.exit_evidence_kind
                    == EconomicExitEvidenceKind::ResolutionPayout
            }
            RecommendationEconomicOutcomeState::Censored => true,
        };
        if valid {
            Ok(())
        } else {
            Err(RecommendationEconomicOutcomeError::InvalidEvidence)
        }
    }

    fn validate_amounts(&self) -> Result<(), RecommendationEconomicOutcomeError> {
        let amounts = &self.payload.amounts;
        if amounts.exited_shares > amounts.entry_filled_shares
            || amounts.entry_cost_usd.is_negative()
            || amounts.exit_proceeds_usd.is_negative()
            || amounts.resolution_payout_usd.is_negative()
            || amounts.execution_fee_usd.is_negative()
            || amounts.expected_maker_rebate_usd.is_negative()
        {
            return Err(RecommendationEconomicOutcomeError::InvalidAmounts);
        }
        let expected = amounts.exit_proceeds_usd
            + amounts.resolution_payout_usd
            + amounts.expected_maker_rebate_usd
            - amounts.entry_cost_usd
            - amounts.execution_fee_usd;
        if amounts
            .net_pnl_usd
            .is_some_and(|net_pnl| net_pnl != expected)
            || (self.state != RecommendationEconomicOutcomeState::Censored
                && amounts.net_pnl_usd.is_none())
        {
            return Err(RecommendationEconomicOutcomeError::InvalidAmounts);
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use chrono::{Duration, TimeZone, Utc};
    use rust_decimal_macros::dec;

    use super::{
        EconomicExitEvidenceKind, NewRecommendationEconomicOutcome, RecommendationEconomicAmounts,
        RecommendationEconomicEvidence, RecommendationEconomicOutcomeError,
        RecommendationEconomicOutcomeInput, RecommendationEconomicOutcomePayload,
        RecommendationEconomicStateDetail,
    };
    use crate::{
        enums::quant::RecommendationEconomicOutcomeState,
        types::{
            ContentHash, DecisionPolicySnapshotId, EconomicTierId, ModelVersionId,
            RecommendationId, RecommendationReportId, ReportRouteRunId, ResearchProfileId,
            ResearchProfileRef, Shares, TradePolicyArtifactId, Usd,
        },
    };

    impl RecommendationEconomicOutcomeInput {
        fn fixture() -> Self {
            let decision_at = Utc.timestamp_opt(100, 0).single().unwrap_or_default();
            let horizon_at = decision_at + Duration::hours(1);
            Self {
                recommendation_id: RecommendationId::from_v7(),
                recommendation_report_id: RecommendationReportId::from_v7(),
                report_route_run_id: ReportRouteRunId::from_v7(),
                decision_policy_snapshot_id: DecisionPolicySnapshotId::from_v7(),
                economic_tier_id: EconomicTierId::from_v7(),
                model_version_id: ModelVersionId::from_v7(),
                trade_policy_artifact_id: TradePolicyArtifactId::from_v7(),
                research_profile_artifact_id: ResearchProfileRef {
                    id: ResearchProfileId::new("test"),
                    version: 1,
                    content_hash: ContentHash::from_bytes([9; 32]),
                }
                .artifact_id(),
                state: RecommendationEconomicOutcomeState::HorizonLiquidated,
                decision_at,
                horizon_at,
                source_available_until: horizon_at,
                replay_kernel_version: "policy_replay_v1".to_owned(),
                payload: RecommendationEconomicOutcomePayload {
                    detail: RecommendationEconomicStateDetail::HorizonLiquidated {
                        entered_at: decision_at + Duration::minutes(1),
                        liquidated_at: horizon_at,
                    },
                    amounts: RecommendationEconomicAmounts {
                        entry_filled_shares: Shares::new(dec!(10)),
                        exited_shares: Shares::new(dec!(10)),
                        entry_cost_usd: Usd::new(dec!(10)),
                        exit_proceeds_usd: Usd::new(dec!(12)),
                        resolution_payout_usd: Usd::ZERO,
                        execution_fee_usd: Usd::new(dec!(1)),
                        expected_maker_rebate_usd: Usd::ZERO,
                        net_pnl_usd: Some(Usd::new(dec!(1))),
                        net_return_bps: Some(dec!(1000)),
                    },
                    evidence: RecommendationEconomicEvidence {
                        exit_evidence_kind: EconomicExitEvidenceKind::FullBidLadder,
                        full_l2_covered: true,
                        fee_covered: true,
                        passive_trade_covered: None,
                        replay_input_hash: ContentHash::from_bytes([1; 32]),
                        replay_output_hash: ContentHash::from_bytes([2; 32]),
                    },
                },
                available_at: horizon_at,
            }
        }
    }

    #[test]
    fn horizon_outcome_is_stable() {
        let input = RecommendationEconomicOutcomeInput::fixture();
        let first = NewRecommendationEconomicOutcome::try_seal(input.clone());
        let second = NewRecommendationEconomicOutcome::try_seal(input);
        assert_eq!(first, second);
        assert!(first.is_ok_and(|outcome| outcome.verify().is_ok()));
    }

    #[test]
    fn horizon_requires_ladder() {
        let mut input = RecommendationEconomicOutcomeInput::fixture();
        input.payload.evidence.exit_evidence_kind = EconomicExitEvidenceKind::PolicyFill;
        assert_eq!(
            NewRecommendationEconomicOutcome::try_seal(input),
            Err(RecommendationEconomicOutcomeError::InvalidEvidence),
        );
    }

    #[test]
    fn availability_reseals_without_rewriting() {
        let outcome = NewRecommendationEconomicOutcome::try_seal(
            RecommendationEconomicOutcomeInput::fixture(),
        )
        .expect("sealed outcome");
        let available_at = outcome.available_at + Duration::seconds(1);
        let resealed = outcome
            .clone()
            .with_available_at(available_at)
            .expect("database availability");
        assert_eq!(resealed.payload_json, outcome.payload_json);
        assert_eq!(
            resealed.source_available_until,
            outcome.source_available_until
        );
        assert_eq!(resealed.available_at, available_at);
        assert_ne!(resealed.evidence_hash, outcome.evidence_hash);
        resealed.verify().expect("resealed hash verifies");
        let mut corrupt = outcome;
        corrupt.evidence_hash = ContentHash::from_bytes([99; 32]);
        assert!(matches!(
            corrupt.with_available_at(available_at),
            Err(RecommendationEconomicOutcomeError::Hash(_))
        ));
    }

    #[test]
    fn terminal_requires_visible_sources() {
        let mut input = RecommendationEconomicOutcomeInput::fixture();
        input.source_available_until = input.horizon_at - Duration::seconds(1);
        assert_eq!(
            NewRecommendationEconomicOutcome::try_seal(input),
            Err(RecommendationEconomicOutcomeError::InvalidTimeline)
        );
    }
}

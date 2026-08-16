//! Immutable research-profile contracts for policy fitting and serving lineage.

use std::{
    borrow::Cow,
    fmt::{Display, Formatter, Result as FmtResult},
    str::FromStr,
    sync::Arc,
};

use chrono::{DateTime, TimeZone, Utc};
use quant_pivot_error::hashing::CanonicalDigestError;
use rust_decimal::Decimal;
use schemars::{JsonSchema, Schema, SchemaGenerator};
use sea_orm::{
    ActiveValue, ColIdx, DbErr, FromJsonQueryResult, FromQueryResult, IntoActiveValue, QueryResult,
    TryFromU64, TryGetError, TryGetable,
    sea_query::{ArrayType, ColumnType, Nullable, Value, ValueType, ValueTypeErr},
};
use serde::{Deserialize, Deserializer, Serialize, Serializer, de::Error};

use crate::{
    enums::common::MarketCategory,
    hashing::CanonicalDigest,
    types::{Bps, ContentHash, ResearchProfileId, TradePolicyQualityGate, Usd},
};

pub const POOLED_1H_CONTROL_PROFILE_ID: &str = "pooled_1h_control";
pub const CRYPTO_PRICE_15M_PROFILE_ID: &str = "crypto_price_15m";
pub const WEATHER_FORECAST_24H_PROFILE_ID: &str = "weather_forecast_24h";
pub const POOLED_BINARY_1H_BOOTSTRAP_PROFILE_ID: &str = "pooled_binary_1h_bootstrap_trade";
pub const CRYPTO_PRICE_15M_BOOTSTRAP_PROFILE_ID: &str = "crypto_price_15m_bootstrap_trade";
pub const WEATHER_FORECAST_24H_BOOTSTRAP_PROFILE_ID: &str = "weather_forecast_24h_bootstrap_trade";
pub const POOLED_1H_HORIZON_SECS: u64 = 3_600;
pub const CRYPTO_PRICE_15M_HORIZON_SECS: u64 = 900;
pub const WEATHER_FORECAST_24H_HORIZON_SECS: u64 = 86_400;
const SECONDS_PER_DAY: u64 = 86_400;
// A feedback schedule beyond one year is operationally indistinguishable from
// disabling it and cannot be represented honestly as an active cadence.
const MAX_FEEDBACK_EVALUATION_WINDOW_DAYS: u32 = 365;
const MAX_FEEDBACK_DURATION_SECS: u64 = 365 * SECONDS_PER_DAY;
// PostgreSQL cohort and scheduler counters use signed BIGINT.
const MAX_FEEDBACK_OBSERVATION_COUNT: u64 = 9_223_372_036_854_775_807;
const MIN_FEEDBACK_BOOTSTRAP_REPETITIONS: u32 = 1_000;
const MAX_FEEDBACK_BOOTSTRAP_REPETITIONS: u32 = 100_000;
/// Breaking hash-schema version for [`ResearchFeedbackPolicy`].
pub const RESEARCH_FEEDBACK_POLICY_HASH_VERSION: u32 = 2;
const RESEARCH_FEEDBACK_POLICY_HASH_DOMAIN: &str = "quant-pivot/research-feedback-policy";
const RESEARCH_AVAILABILITY_POLICY_HASH_DOMAIN: &str = "quant-pivot/research-availability-policy";
const RESEARCH_AVAILABILITY_POLICY_HASH_VERSION: u32 = 1;

/// Stable immutable profile identity carried by every downstream artifact.
#[derive(
    Debug,
    Clone,
    PartialEq,
    Eq,
    PartialOrd,
    Ord,
    Hash,
    Serialize,
    Deserialize,
    JsonSchema,
    FromJsonQueryResult,
    FromQueryResult,
)]
#[serde(deny_unknown_fields)]
pub struct ResearchProfileRef {
    pub id: ResearchProfileId,
    pub version: u32,
    pub content_hash: ContentHash,
}

/// Canonical, reversible, content-addressed FK for one immutable profile.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct ResearchProfileArtifactId(Arc<ResearchProfileRef>);

#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[error("invalid research profile artifact id: {detail}")]
pub struct ResearchProfileArtifactIdParseError {
    detail: String,
}

impl ResearchProfileArtifactId {
    #[must_use]
    pub fn from_profile_ref(profile_ref: &ResearchProfileRef) -> Self {
        Self(Arc::new(profile_ref.clone()))
    }

    #[must_use]
    pub fn profile_ref(&self) -> ResearchProfileRef {
        self.0.as_ref().clone()
    }

    fn parse_parts(value: &str) -> Result<ResearchProfileRef, ResearchProfileArtifactIdParseError> {
        let mut parts = value.splitn(4, ':');
        if parts.next() != Some("rpa") {
            return Err(ResearchProfileArtifactIdParseError {
                detail: "missing rpa prefix".to_owned(),
            });
        }
        let id = parts
            .next()
            .ok_or_else(|| ResearchProfileArtifactIdParseError {
                detail: "missing profile id".to_owned(),
            })?;
        if id.is_empty()
            || !id
                .bytes()
                .all(|byte| byte.is_ascii_lowercase() || byte.is_ascii_digit() || byte == b'_')
        {
            return Err(ResearchProfileArtifactIdParseError {
                detail: "profile id must contain lowercase ASCII, digits, or underscore".to_owned(),
            });
        }
        let version = parts
            .next()
            .ok_or_else(|| ResearchProfileArtifactIdParseError {
                detail: "missing profile version".to_owned(),
            })?
            .parse::<u32>()
            .map_err(|error| ResearchProfileArtifactIdParseError {
                detail: format!("invalid profile version: {error}"),
            })?;
        if version == 0 {
            return Err(ResearchProfileArtifactIdParseError {
                detail: "profile version must be positive".to_owned(),
            });
        }
        let content_hash = ContentHash::parse(parts.next().ok_or_else(|| {
            ResearchProfileArtifactIdParseError {
                detail: "missing profile content hash".to_owned(),
            }
        })?)
        .map_err(|error| ResearchProfileArtifactIdParseError {
            detail: error.to_string(),
        })?;
        Ok(ResearchProfileRef {
            id: ResearchProfileId::new(id),
            version,
            content_hash,
        })
    }
}

impl Display for ResearchProfileArtifactId {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> FmtResult {
        write!(
            formatter,
            "rpa:{}:{}:{}",
            self.0.id, self.0.version, self.0.content_hash
        )
    }
}

impl FromStr for ResearchProfileArtifactId {
    type Err = ResearchProfileArtifactIdParseError;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        Self::parse_parts(value).map(|profile_ref| Self(Arc::new(profile_ref)))
    }
}

impl Serialize for ResearchProfileArtifactId {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        serializer.serialize_str(&self.to_string())
    }
}

impl<'de> Deserialize<'de> for ResearchProfileArtifactId {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        String::deserialize(deserializer)?
            .parse()
            .map_err(Error::custom)
    }
}

impl JsonSchema for ResearchProfileArtifactId {
    fn inline_schema() -> bool {
        true
    }

    fn schema_name() -> Cow<'static, str> {
        Cow::Borrowed("ResearchProfileArtifactId")
    }

    fn json_schema(_generator: &mut SchemaGenerator) -> Schema {
        schemars::json_schema!({
            "type": "string",
            "pattern": "^rpa:[a-z0-9_]+:[1-9][0-9]*:blake3:[0-9a-f]{64}$"
        })
    }
}

impl From<ResearchProfileArtifactId> for Value {
    fn from(id: ResearchProfileArtifactId) -> Self {
        Self::String(Some(id.to_string()))
    }
}

impl From<&ResearchProfileArtifactId> for Value {
    fn from(id: &ResearchProfileArtifactId) -> Self {
        Self::String(Some(id.to_string()))
    }
}

impl ValueType for ResearchProfileArtifactId {
    fn try_from(value: Value) -> Result<Self, ValueTypeErr> {
        match value {
            Value::String(Some(value)) => value.parse().map_err(|_| ValueTypeErr),
            _ => Err(ValueTypeErr),
        }
    }

    fn type_name() -> String {
        "ResearchProfileArtifactId".to_owned()
    }

    fn array_type() -> ArrayType {
        ArrayType::String
    }

    fn column_type() -> ColumnType {
        ColumnType::Text
    }
}

impl Nullable for ResearchProfileArtifactId {
    fn null() -> Value {
        Value::String(None)
    }
}

impl IntoActiveValue<Self> for ResearchProfileArtifactId {
    fn into_active_value(self) -> ActiveValue<Self> {
        ActiveValue::Set(self)
    }
}

impl TryFromU64 for ResearchProfileArtifactId {
    fn try_from_u64(_value: u64) -> Result<Self, DbErr> {
        Err(DbErr::ConvertFromU64("ResearchProfileArtifactId"))
    }
}

impl TryGetable for ResearchProfileArtifactId {
    fn try_get_by<I: ColIdx>(result: &QueryResult, index: I) -> Result<Self, TryGetError> {
        let value = String::try_get_by(result, index)?;
        value
            .parse()
            .map_err(|error: ResearchProfileArtifactIdParseError| {
                TryGetError::DbErr(DbErr::Type(error.to_string()))
            })
    }
}

impl ResearchProfileRef {
    #[must_use]
    pub fn artifact_id(&self) -> ResearchProfileArtifactId {
        ResearchProfileArtifactId::from_profile_ref(self)
    }

    /// Validate that the reference has the exact canonical artifact-id form.
    pub fn validate(&self) -> Result<(), ResearchProfileArtifactIdParseError> {
        let canonical = self
            .artifact_id()
            .to_string()
            .parse::<ResearchProfileArtifactId>()?
            .profile_ref();
        if canonical == *self {
            Ok(())
        } else {
            Err(ResearchProfileArtifactIdParseError {
                detail: "research profile reference is not canonical".to_owned(),
            })
        }
    }
}

/// Information set under which policy decisions are evaluated.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum ResearchInformationRegime {
    PooledBinaryMarket,
    CryptoPrice,
    WeatherForecast,
}

/// Deterministic decision-clock contract.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ResearchDecisionTrigger {
    EveryFiveMinutes,
    Hourly,
    HourlyLatestCompleteGefsCycle,
}

/// Closed set of implemented policy fitters. A profile without a fitter may
/// still govern datasets and model research, but it cannot enter the policy-fit
/// job graph.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ResearchPolicyFitter {
    WeatherForecast,
}

crate::pg_enum! {
    type_name = "qp_research_evaluation_track",
    /// Evaluation track requested by a fit. It never activates execution by itself.
    pub enum ResearchEvaluationTrack {
        ResearchOnly => "research_only",
        SemiAutoCandidate => "semi_auto_candidate",
    }
}

/// Strongly typed source requirements sealed into the source-slice manifest.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ResearchProfileDataSource {
    CatalogLedger,
    GammaMarketIdentity,
    ClobMarketInfo,
    ClobL2,
    MarketExecution,
    ExecutionParticipant,
    BinanceMarketData,
    PolymarketRtds,
    AviationWeather,
    GhcnhCalibration,
    GefsEnsemble,
    PolymarketResolution,
}

/// Closed feature contracts prevent a profile from silently changing its
/// historical information regime when a source is unavailable.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum ResearchFeatureContract {
    FullL2,
    FullL2Crypto,
    FullL2Weather,
    TradeBootstrap,
    TradeBootstrapCrypto,
    TradeBootstrapWeather,
}

/// Resolution truth used by every built-in binary-market profile.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum ResearchLabelContract {
    FinalTokenPayoutRatio,
}

/// Candidate-cohort semantics frozen into source-slice identity.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ResearchCohortContract {
    AllEligible,
    CryptoResolved,
    WeatherResolved,
}

/// Point-in-time availability basis for predictive facts.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "basis", rename_all = "snake_case")]
pub enum ResearchAvailabilityPolicy {
    IngestionObserved,
    FinalizedBlockConfirmation { confirmation_blocks: u64 },
}

impl ResearchAvailabilityPolicy {
    /// Domain-separated commitment sealed into source-coverage evidence.
    pub fn content_hash(&self) -> Result<ContentHash, CanonicalDigestError> {
        CanonicalDigest::content_hash_typed(
            RESEARCH_AVAILABILITY_POLICY_HASH_DOMAIN,
            RESEARCH_AVAILABILITY_POLICY_HASH_VERSION,
            self,
        )
    }
}

/// Maximum serving permission carried through model and route lineage.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum ServingAuthority {
    ReportOnlyWithLiveL2,
    ExecutionEligible,
}

impl ResearchFeatureContract {
    #[must_use]
    pub fn required_sources(self) -> Vec<ResearchProfileDataSource> {
        let mut sources = match self {
            Self::FullL2 | Self::FullL2Crypto | Self::FullL2Weather => vec![
                ResearchProfileDataSource::ClobMarketInfo,
                ResearchProfileDataSource::ClobL2,
                ResearchProfileDataSource::MarketExecution,
                ResearchProfileDataSource::ExecutionParticipant,
            ],
            Self::TradeBootstrap => vec![
                ResearchProfileDataSource::MarketExecution,
                ResearchProfileDataSource::ExecutionParticipant,
            ],
            Self::TradeBootstrapCrypto => vec![
                ResearchProfileDataSource::MarketExecution,
                ResearchProfileDataSource::ExecutionParticipant,
                ResearchProfileDataSource::BinanceMarketData,
            ],
            Self::TradeBootstrapWeather => vec![
                ResearchProfileDataSource::MarketExecution,
                ResearchProfileDataSource::ExecutionParticipant,
                ResearchProfileDataSource::AviationWeather,
                ResearchProfileDataSource::GhcnhCalibration,
                ResearchProfileDataSource::GefsEnsemble,
            ],
        };
        match self {
            Self::FullL2Crypto => sources.extend([
                ResearchProfileDataSource::BinanceMarketData,
                ResearchProfileDataSource::PolymarketRtds,
            ]),
            Self::FullL2Weather => sources.extend([
                ResearchProfileDataSource::AviationWeather,
                ResearchProfileDataSource::GhcnhCalibration,
                ResearchProfileDataSource::GefsEnsemble,
            ]),
            Self::FullL2
            | Self::TradeBootstrap
            | Self::TradeBootstrapCrypto
            | Self::TradeBootstrapWeather => {}
        }
        sources.sort_unstable();
        sources
    }

    #[must_use]
    pub const fn requires_l2(self) -> bool {
        matches!(
            self,
            Self::FullL2 | Self::FullL2Crypto | Self::FullL2Weather
        )
    }
}

impl ResearchLabelContract {
    #[must_use]
    pub const fn required_source(self) -> ResearchProfileDataSource {
        match self {
            Self::FinalTokenPayoutRatio => ResearchProfileDataSource::PolymarketResolution,
        }
    }
}

impl ResearchCohortContract {
    #[must_use]
    pub const fn category(self) -> Option<MarketCategory> {
        match self {
            Self::AllEligible => None,
            Self::CryptoResolved => Some(MarketCategory::Crypto),
            Self::WeatherResolved => Some(MarketCategory::Weather),
        }
    }

    #[must_use]
    pub fn required_sources(self) -> Vec<ResearchProfileDataSource> {
        match self {
            Self::AllEligible => vec![ResearchProfileDataSource::CatalogLedger],
            Self::CryptoResolved => vec![
                ResearchProfileDataSource::CatalogLedger,
                ResearchProfileDataSource::GammaMarketIdentity,
                ResearchProfileDataSource::BinanceMarketData,
                ResearchProfileDataSource::PolymarketResolution,
            ],
            Self::WeatherResolved => vec![
                ResearchProfileDataSource::CatalogLedger,
                ResearchProfileDataSource::GammaMarketIdentity,
                ResearchProfileDataSource::AviationWeather,
                ResearchProfileDataSource::GhcnhCalibration,
                ResearchProfileDataSource::GefsEnsemble,
                ResearchProfileDataSource::PolymarketResolution,
            ],
        }
    }
}

/// Immutable feedback methodology and promotion thresholds owned by a profile.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, FromJsonQueryResult)]
#[serde(deny_unknown_fields)]
pub struct ResearchFeedbackPolicy {
    pub evaluation_window_days: u32,
    /// Maximum challenger templates selected by one immutable recipe plan.
    /// Capacity increases require an explicit profile revision.
    pub max_challengers: u32,
    /// How often the coordinator may evaluate whether a new feedback cycle is
    /// warranted. This schedules evaluation only; it grants no promotion
    /// authority.
    pub feedback_cadence_secs: u64,
    pub minimum_mature_labels: u64,
    /// Incremental labels matured after the champion/cycle cutoff. This is
    /// distinct from the total mature-label floor above.
    pub minimum_new_mature_labels: u64,
    /// Minimum interval between successful retraining starts for this profile.
    pub retraining_cooldown_secs: u64,
    pub minimum_coverage: Decimal,
    pub data_drift_psi_threshold: Decimal,
    pub data_drift_ks_p_value: Decimal,
    pub concept_target_rank_ic_drop: Decimal,
    pub label_js_divergence: Decimal,
    pub minimum_effect_bps: Bps,
    pub effect_confidence: Decimal,
    /// Minimum number of same-decision-tick portfolio-return observations
    /// required before the comparison stage may emit numeric evidence.
    pub comparison_minimum_observations: u64,
    /// Fixed number of shared circular-block bootstrap repetitions.
    pub comparison_bootstrap_repetitions: u32,
    /// Fixed circular block length measured in decision-tick observations.
    pub comparison_block_length: u32,
    /// Explicit deterministic seed for the domain-separated counter generator.
    pub comparison_bootstrap_seed: u64,
    pub shadow_minimum_observations: u64,
}

/// Stable validation failures for immutable feedback scheduling and gates.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum ResearchFeedbackPolicyError {
    #[error("feedback policy duration `{field}` must be positive")]
    ZeroDuration { field: &'static str },
    #[error(
        "feedback policy duration `{field}` is {actual}, exceeding the maximum {maximum} seconds"
    )]
    DurationExceeds {
        field: &'static str,
        actual: u64,
        maximum: u64,
    },
    #[error(
        "feedback policy `evaluation_window_days` is {actual}, exceeding the maximum {maximum} days"
    )]
    EvaluationWindowExceeds { actual: u32, maximum: u32 },
    #[error("feedback policy `evaluation_window_days` cannot be represented in seconds")]
    EvaluationWindowOverflow,
    #[error(
        "feedback policy `feedback_cadence_secs` ({cadence_secs}) exceeds its evaluation window ({evaluation_window_secs} seconds)"
    )]
    CadenceExceedsWindow {
        cadence_secs: u64,
        evaluation_window_secs: u64,
    },
    #[error(
        "feedback policy `retraining_cooldown_secs` ({cooldown_secs}) cannot be shorter than `feedback_cadence_secs` ({cadence_secs})"
    )]
    CooldownShorterThanCadence {
        cadence_secs: u64,
        cooldown_secs: u64,
    },
    #[error("feedback policy count `{field}` must be positive")]
    ZeroCount { field: &'static str },
    #[error(
        "feedback policy count `{field}` is {actual}, exceeding the durable count maximum {maximum}"
    )]
    CountExceeds {
        field: &'static str,
        actual: u64,
        maximum: u64,
    },
    #[error(
        "feedback policy `minimum_new_mature_labels` ({minimum_new}) cannot exceed `minimum_mature_labels` ({minimum_total})"
    )]
    NewLabelsExceedTotal {
        minimum_new: u64,
        minimum_total: u64,
    },
    #[error("feedback policy `{field}` must be within (0, 1]")]
    UnitInterval { field: &'static str },
    #[error("feedback policy `data_drift_psi_threshold` must be positive")]
    NonPositivePsi,
    #[error("feedback policy `minimum_effect_bps` must be positive")]
    NonPositiveEffect,
    #[error(
        "feedback policy comparison bootstrap repetitions must be within [{minimum}, {maximum}], got {actual}"
    )]
    BootstrapRepetitions {
        minimum: u32,
        maximum: u32,
        actual: u32,
    },
    #[error(
        "feedback policy comparison block length must be positive and no greater than minimum observations ({minimum_observations}), got {actual}"
    )]
    ComparisonBlockLength {
        minimum_observations: u64,
        actual: u32,
    },
    #[error(transparent)]
    Hash(#[from] CanonicalDigestError),
}

impl ResearchFeedbackPolicy {
    /// Validate scheduling bounds, incremental-label semantics, and statistical
    /// thresholds without relying on any runtime defaults.
    pub fn validate(&self) -> Result<(), ResearchFeedbackPolicyError> {
        if self.evaluation_window_days == 0 {
            return Err(ResearchFeedbackPolicyError::ZeroDuration {
                field: "evaluation_window_days",
            });
        }
        if self.evaluation_window_days > MAX_FEEDBACK_EVALUATION_WINDOW_DAYS {
            return Err(ResearchFeedbackPolicyError::EvaluationWindowExceeds {
                actual: self.evaluation_window_days,
                maximum: MAX_FEEDBACK_EVALUATION_WINDOW_DAYS,
            });
        }
        let evaluation_window_secs = u64::from(self.evaluation_window_days)
            .checked_mul(SECONDS_PER_DAY)
            .ok_or(ResearchFeedbackPolicyError::EvaluationWindowOverflow)?;
        for (field, value) in [
            ("feedback_cadence_secs", self.feedback_cadence_secs),
            ("retraining_cooldown_secs", self.retraining_cooldown_secs),
        ] {
            if value == 0 {
                return Err(ResearchFeedbackPolicyError::ZeroDuration { field });
            }
            if value > MAX_FEEDBACK_DURATION_SECS {
                return Err(ResearchFeedbackPolicyError::DurationExceeds {
                    field,
                    actual: value,
                    maximum: MAX_FEEDBACK_DURATION_SECS,
                });
            }
        }
        if self.feedback_cadence_secs > evaluation_window_secs {
            return Err(ResearchFeedbackPolicyError::CadenceExceedsWindow {
                cadence_secs: self.feedback_cadence_secs,
                evaluation_window_secs,
            });
        }
        if self.retraining_cooldown_secs < self.feedback_cadence_secs {
            return Err(ResearchFeedbackPolicyError::CooldownShorterThanCadence {
                cadence_secs: self.feedback_cadence_secs,
                cooldown_secs: self.retraining_cooldown_secs,
            });
        }
        for (field, value) in [
            ("max_challengers", u64::from(self.max_challengers)),
            ("minimum_mature_labels", self.minimum_mature_labels),
            ("minimum_new_mature_labels", self.minimum_new_mature_labels),
            (
                "comparison_minimum_observations",
                self.comparison_minimum_observations,
            ),
            (
                "shadow_minimum_observations",
                self.shadow_minimum_observations,
            ),
        ] {
            if value == 0 {
                return Err(ResearchFeedbackPolicyError::ZeroCount { field });
            }
            if value > MAX_FEEDBACK_OBSERVATION_COUNT {
                return Err(ResearchFeedbackPolicyError::CountExceeds {
                    field,
                    actual: value,
                    maximum: MAX_FEEDBACK_OBSERVATION_COUNT,
                });
            }
        }
        if self.minimum_new_mature_labels > self.minimum_mature_labels {
            return Err(ResearchFeedbackPolicyError::NewLabelsExceedTotal {
                minimum_new: self.minimum_new_mature_labels,
                minimum_total: self.minimum_mature_labels,
            });
        }
        for (name, value) in [
            ("minimum_coverage", self.minimum_coverage),
            ("data_drift_ks_p_value", self.data_drift_ks_p_value),
            (
                "concept_target_rank_ic_drop",
                self.concept_target_rank_ic_drop,
            ),
            ("label_js_divergence", self.label_js_divergence),
            ("effect_confidence", self.effect_confidence),
        ] {
            if value <= Decimal::ZERO || value > Decimal::ONE {
                return Err(ResearchFeedbackPolicyError::UnitInterval { field: name });
            }
        }
        if self.data_drift_psi_threshold <= Decimal::ZERO {
            return Err(ResearchFeedbackPolicyError::NonPositivePsi);
        }
        if self.minimum_effect_bps.inner() <= Decimal::ZERO {
            return Err(ResearchFeedbackPolicyError::NonPositiveEffect);
        }
        if !(MIN_FEEDBACK_BOOTSTRAP_REPETITIONS..=MAX_FEEDBACK_BOOTSTRAP_REPETITIONS)
            .contains(&self.comparison_bootstrap_repetitions)
        {
            return Err(ResearchFeedbackPolicyError::BootstrapRepetitions {
                minimum: MIN_FEEDBACK_BOOTSTRAP_REPETITIONS,
                maximum: MAX_FEEDBACK_BOOTSTRAP_REPETITIONS,
                actual: self.comparison_bootstrap_repetitions,
            });
        }
        if self.comparison_block_length == 0
            || u64::from(self.comparison_block_length) > self.comparison_minimum_observations
        {
            return Err(ResearchFeedbackPolicyError::ComparisonBlockLength {
                minimum_observations: self.comparison_minimum_observations,
                actual: self.comparison_block_length,
            });
        }
        Ok(())
    }

    /// Domain-separated commitment frozen into each feedback-cycle identity.
    pub fn content_hash(&self) -> Result<ContentHash, ResearchFeedbackPolicyError> {
        self.validate()?;
        CanonicalDigest::content_hash_typed(
            RESEARCH_FEEDBACK_POLICY_HASH_DOMAIN,
            RESEARCH_FEEDBACK_POLICY_HASH_VERSION,
            self,
        )
        .map_err(Into::into)
    }
}

/// Profile content. The content hash excludes publication metadata.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, FromJsonQueryResult)]
#[serde(deny_unknown_fields)]
pub struct ResearchProfileSpec {
    pub information_regime: ResearchInformationRegime,
    pub policy_fitter: Option<ResearchPolicyFitter>,
    pub category: Option<MarketCategory>,
    pub decision_trigger: ResearchDecisionTrigger,
    pub decision_cadence_secs: u64,
    pub target_horizon_secs: u64,
    pub max_holding_secs: u64,
    pub exit_heartbeat_secs: u64,
    pub fit_span_days: u32,
    pub max_feature_lookback_secs: u64,
    pub purge_embargo_secs: u64,
    pub feature_contract: ResearchFeatureContract,
    pub label_contract: ResearchLabelContract,
    pub cohort_contract: ResearchCohortContract,
    pub availability_policy: ResearchAvailabilityPolicy,
    pub serving_authority: ServingAuthority,
    pub allowed_cash_budget_tiers: Vec<Usd>,
    pub activation_eligibility: ResearchEvaluationTrack,
    pub quality_gate: TradePolicyQualityGate,
    pub feedback_policy: ResearchFeedbackPolicy,
}

impl ResearchProfileSpec {
    pub fn validate(&self) -> Result<(), String> {
        if self.decision_cadence_secs == 0
            || self.target_horizon_secs == 0
            || self.max_holding_secs == 0
            || self.exit_heartbeat_secs == 0
            || self.fit_span_days == 0
        {
            return Err("research profile durations must be positive".to_owned());
        }
        if self.max_holding_secs > self.target_horizon_secs {
            return Err("research profile max holding cannot exceed target horizon".to_owned());
        }
        match (self.information_regime, self.policy_fitter) {
            (
                ResearchInformationRegime::PooledBinaryMarket
                | ResearchInformationRegime::CryptoPrice
                | ResearchInformationRegime::WeatherForecast,
                None,
            )
            | (
                ResearchInformationRegime::WeatherForecast,
                Some(ResearchPolicyFitter::WeatherForecast),
            ) => {}
            _ => {
                return Err(
                    "research profile policy fitter does not match its information regime"
                        .to_owned(),
                );
            }
        }
        match self.serving_authority {
            ServingAuthority::ReportOnlyWithLiveL2 if self.policy_fitter.is_some() => {
                return Err("bootstrap profiles cannot carry a policy fitter".to_owned());
            }
            ServingAuthority::ExecutionEligible
                if self.information_regime == ResearchInformationRegime::WeatherForecast
                    && self.policy_fitter != Some(ResearchPolicyFitter::WeatherForecast) =>
            {
                return Err(
                    "execution-eligible Weather profile requires the WeatherForecast fitter"
                        .to_owned(),
                );
            }
            ServingAuthority::ExecutionEligible
                if self.information_regime != ResearchInformationRegime::WeatherForecast
                    && self.policy_fitter.is_some() =>
            {
                return Err("non-Weather execution profile cannot carry a policy fitter".to_owned());
            }
            ServingAuthority::ReportOnlyWithLiveL2 | ServingAuthority::ExecutionEligible => {}
        }
        if self.category != self.cohort_contract.category() {
            return Err("research profile category does not match its cohort contract".to_owned());
        }
        if let ResearchAvailabilityPolicy::FinalizedBlockConfirmation {
            confirmation_blocks,
        } = self.availability_policy
            && confirmation_blocks != 12
        {
            return Err("bootstrap availability policy requires 12 confirmations".to_owned());
        }
        if matches!(
            self.serving_authority,
            ServingAuthority::ReportOnlyWithLiveL2
        ) && self.feature_contract.requires_l2()
        {
            return Err("report-only bootstrap authority requires an L2-free contract".to_owned());
        }
        if matches!(
            self.serving_authority,
            ServingAuthority::ReportOnlyWithLiveL2
        ) && !matches!(
            self.availability_policy,
            ResearchAvailabilityPolicy::FinalizedBlockConfirmation { .. }
        ) {
            return Err(
                "report-only bootstrap authority requires finalized block availability".to_owned(),
            );
        }
        if self.allowed_cash_budget_tiers.is_empty()
            || self
                .allowed_cash_budget_tiers
                .iter()
                .any(|tier| !tier.is_positive())
            || self
                .allowed_cash_budget_tiers
                .windows(2)
                .any(|tiers| tiers[0] >= tiers[1])
        {
            return Err(
                "research profile cash-budget tiers must be positive, unique, and sorted"
                    .to_owned(),
            );
        }
        self.quality_gate.validate()?;
        self.feedback_policy
            .validate()
            .map_err(|error| error.to_string())
    }

    #[must_use]
    pub const fn permits(&self, track: ResearchEvaluationTrack) -> bool {
        match (self.activation_eligibility, track) {
            (ResearchEvaluationTrack::ResearchOnly, ResearchEvaluationTrack::ResearchOnly)
            | (ResearchEvaluationTrack::SemiAutoCandidate, _) => true,
            (ResearchEvaluationTrack::ResearchOnly, ResearchEvaluationTrack::SemiAutoCandidate) => {
                false
            }
        }
    }

    /// Complete raw-history runway required to reproduce one fit, rounded up
    /// to natural days for retention planning.
    pub fn required_days(&self) -> Result<u32, String> {
        let ceil_days = |seconds: u64| {
            seconds
                .checked_add(SECONDS_PER_DAY - 1)
                .map(|value| value / SECONDS_PER_DAY)
                .ok_or_else(|| "research profile duration overflow".to_owned())
        };
        let variable_days = ceil_days(self.max_feature_lookback_secs)?
            .checked_add(ceil_days(self.target_horizon_secs)?)
            .and_then(|value| value.checked_add(ceil_days(self.purge_embargo_secs).ok()?))
            .ok_or_else(|| "research profile required-days overflow".to_owned())?;
        u64::from(self.fit_span_days)
            .checked_add(variable_days)
            .and_then(|days| <u32 as TryFrom<u64>>::try_from(days).ok())
            .ok_or_else(|| "research profile required-days exceed u32".to_owned())
    }

    #[must_use]
    pub fn required_sources(&self) -> Vec<ResearchProfileDataSource> {
        let mut sources = self.feature_contract.required_sources();
        sources.push(ResearchProfileDataSource::GammaMarketIdentity);
        sources.push(self.label_contract.required_source());
        sources.extend(self.cohort_contract.required_sources());
        sources.sort_unstable();
        sources.dedup();
        sources
    }
}

/// Immutable governed profile artifact exposed by the research API.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ResearchProfileArtifact {
    pub profile_ref: ResearchProfileRef,
    pub spec: ResearchProfileSpec,
    pub published_by: String,
    pub published_at: DateTime<Utc>,
    pub governance_reason: String,
}

impl ResearchProfileArtifact {
    #[must_use]
    pub fn required_sources_contains(&self, source: ResearchProfileDataSource) -> bool {
        self.spec.required_sources().binary_search(&source).is_ok()
    }

    fn try_new(
        id: &str,
        version: u32,
        spec: ResearchProfileSpec,
        published_at: DateTime<Utc>,
    ) -> Result<Self, String> {
        if id.is_empty()
            || !id
                .bytes()
                .all(|byte| byte.is_ascii_lowercase() || byte.is_ascii_digit() || byte == b'_')
            || version == 0
        {
            return Err("research profile id/version is invalid".to_owned());
        }
        spec.validate()?;
        let content_hash = CanonicalDigest::content_hash_json(&spec)
            .map_err(|error| format!("research profile hash failed: {error}"))?;
        Ok(Self {
            profile_ref: ResearchProfileRef {
                id: ResearchProfileId::new(id),
                version,
                content_hash,
            },
            spec,
            published_by: "research-governance".to_owned(),
            published_at,
            governance_reason: "immutable vertical and feedback-policy freeze".to_owned(),
        })
    }

    fn pooled_full_l2(published_at: DateTime<Utc>) -> Result<Self, String> {
        Self::try_new(
            POOLED_1H_CONTROL_PROFILE_ID,
            5,
            ResearchProfileSpec {
                information_regime: ResearchInformationRegime::PooledBinaryMarket,
                policy_fitter: None,
                category: None,
                decision_trigger: ResearchDecisionTrigger::Hourly,
                decision_cadence_secs: 3_600,
                target_horizon_secs: POOLED_1H_HORIZON_SECS,
                max_holding_secs: POOLED_1H_HORIZON_SECS,
                exit_heartbeat_secs: 60,
                fit_span_days: 30,
                max_feature_lookback_secs: 86_400,
                purge_embargo_secs: 86_400,
                feature_contract: ResearchFeatureContract::FullL2,
                label_contract: ResearchLabelContract::FinalTokenPayoutRatio,
                cohort_contract: ResearchCohortContract::AllEligible,
                availability_policy: ResearchAvailabilityPolicy::IngestionObserved,
                serving_authority: ServingAuthority::ExecutionEligible,
                allowed_cash_budget_tiers: vec![Usd::new(Decimal::new(25, 0))],
                activation_eligibility: ResearchEvaluationTrack::ResearchOnly,
                quality_gate: TradePolicyQualityGate::production(),
                feedback_policy: production_feedback_policy(
                    SECONDS_PER_DAY,
                    50,
                    7 * SECONDS_PER_DAY,
                ),
            },
            published_at,
        )
    }

    fn crypto_full_l2(published_at: DateTime<Utc>) -> Result<Self, String> {
        Self::try_new(
            CRYPTO_PRICE_15M_PROFILE_ID,
            4,
            ResearchProfileSpec {
                information_regime: ResearchInformationRegime::CryptoPrice,
                policy_fitter: None,
                category: Some(MarketCategory::Crypto),
                decision_trigger: ResearchDecisionTrigger::EveryFiveMinutes,
                decision_cadence_secs: 300,
                target_horizon_secs: CRYPTO_PRICE_15M_HORIZON_SECS,
                max_holding_secs: CRYPTO_PRICE_15M_HORIZON_SECS,
                exit_heartbeat_secs: 30,
                fit_span_days: 90,
                max_feature_lookback_secs: 86_400,
                purge_embargo_secs: 3_600,
                feature_contract: ResearchFeatureContract::FullL2Crypto,
                label_contract: ResearchLabelContract::FinalTokenPayoutRatio,
                cohort_contract: ResearchCohortContract::CryptoResolved,
                availability_policy: ResearchAvailabilityPolicy::IngestionObserved,
                serving_authority: ServingAuthority::ExecutionEligible,
                allowed_cash_budget_tiers: vec![Usd::new(Decimal::new(25, 0))],
                activation_eligibility: ResearchEvaluationTrack::SemiAutoCandidate,
                quality_gate: TradePolicyQualityGate::production(),
                feedback_policy: production_feedback_policy(6 * 3_600, 50, 3 * SECONDS_PER_DAY),
            },
            published_at,
        )
    }

    fn weather_full_l2(published_at: DateTime<Utc>) -> Result<Self, String> {
        Self::try_new(
            WEATHER_FORECAST_24H_PROFILE_ID,
            5,
            ResearchProfileSpec {
                information_regime: ResearchInformationRegime::WeatherForecast,
                policy_fitter: Some(ResearchPolicyFitter::WeatherForecast),
                category: Some(MarketCategory::Weather),
                decision_trigger: ResearchDecisionTrigger::HourlyLatestCompleteGefsCycle,
                decision_cadence_secs: 3_600,
                target_horizon_secs: WEATHER_FORECAST_24H_HORIZON_SECS,
                max_holding_secs: WEATHER_FORECAST_24H_HORIZON_SECS,
                exit_heartbeat_secs: 60,
                fit_span_days: 90,
                max_feature_lookback_secs: 604_800,
                purge_embargo_secs: 172_800,
                feature_contract: ResearchFeatureContract::FullL2Weather,
                label_contract: ResearchLabelContract::FinalTokenPayoutRatio,
                cohort_contract: ResearchCohortContract::WeatherResolved,
                availability_policy: ResearchAvailabilityPolicy::IngestionObserved,
                serving_authority: ServingAuthority::ExecutionEligible,
                allowed_cash_budget_tiers: vec![Usd::new(Decimal::new(25, 0))],
                activation_eligibility: ResearchEvaluationTrack::SemiAutoCandidate,
                quality_gate: TradePolicyQualityGate::production(),
                feedback_policy: production_feedback_policy(
                    SECONDS_PER_DAY,
                    50,
                    14 * SECONDS_PER_DAY,
                ),
            },
            published_at,
        )
    }

    fn pooled_trade_bootstrap(published_at: DateTime<Utc>) -> Result<Self, String> {
        Self::try_new(
            POOLED_BINARY_1H_BOOTSTRAP_PROFILE_ID,
            2,
            ResearchProfileSpec {
                information_regime: ResearchInformationRegime::PooledBinaryMarket,
                policy_fitter: None,
                category: None,
                decision_trigger: ResearchDecisionTrigger::Hourly,
                decision_cadence_secs: 3_600,
                target_horizon_secs: POOLED_1H_HORIZON_SECS,
                max_holding_secs: POOLED_1H_HORIZON_SECS,
                exit_heartbeat_secs: 60,
                fit_span_days: 30,
                max_feature_lookback_secs: 86_400,
                purge_embargo_secs: 86_400,
                feature_contract: ResearchFeatureContract::TradeBootstrap,
                label_contract: ResearchLabelContract::FinalTokenPayoutRatio,
                cohort_contract: ResearchCohortContract::AllEligible,
                availability_policy: ResearchAvailabilityPolicy::FinalizedBlockConfirmation {
                    confirmation_blocks: 12,
                },
                serving_authority: ServingAuthority::ReportOnlyWithLiveL2,
                allowed_cash_budget_tiers: vec![Usd::new(Decimal::new(25, 0))],
                activation_eligibility: ResearchEvaluationTrack::ResearchOnly,
                quality_gate: TradePolicyQualityGate::production(),
                feedback_policy: production_feedback_policy(
                    SECONDS_PER_DAY,
                    50,
                    7 * SECONDS_PER_DAY,
                ),
            },
            published_at,
        )
    }

    fn crypto_trade_bootstrap(published_at: DateTime<Utc>) -> Result<Self, String> {
        Self::try_new(
            CRYPTO_PRICE_15M_BOOTSTRAP_PROFILE_ID,
            2,
            ResearchProfileSpec {
                information_regime: ResearchInformationRegime::CryptoPrice,
                policy_fitter: None,
                category: Some(MarketCategory::Crypto),
                decision_trigger: ResearchDecisionTrigger::EveryFiveMinutes,
                decision_cadence_secs: 300,
                target_horizon_secs: CRYPTO_PRICE_15M_HORIZON_SECS,
                max_holding_secs: CRYPTO_PRICE_15M_HORIZON_SECS,
                exit_heartbeat_secs: 30,
                fit_span_days: 90,
                max_feature_lookback_secs: 86_400,
                purge_embargo_secs: 3_600,
                feature_contract: ResearchFeatureContract::TradeBootstrapCrypto,
                label_contract: ResearchLabelContract::FinalTokenPayoutRatio,
                cohort_contract: ResearchCohortContract::CryptoResolved,
                availability_policy: ResearchAvailabilityPolicy::FinalizedBlockConfirmation {
                    confirmation_blocks: 12,
                },
                serving_authority: ServingAuthority::ReportOnlyWithLiveL2,
                allowed_cash_budget_tiers: vec![Usd::new(Decimal::new(25, 0))],
                activation_eligibility: ResearchEvaluationTrack::ResearchOnly,
                quality_gate: TradePolicyQualityGate::production(),
                feedback_policy: production_feedback_policy(6 * 3_600, 50, 3 * SECONDS_PER_DAY),
            },
            published_at,
        )
    }

    fn weather_trade_bootstrap(published_at: DateTime<Utc>) -> Result<Self, String> {
        Self::try_new(
            WEATHER_FORECAST_24H_BOOTSTRAP_PROFILE_ID,
            2,
            ResearchProfileSpec {
                information_regime: ResearchInformationRegime::WeatherForecast,
                policy_fitter: None,
                category: Some(MarketCategory::Weather),
                decision_trigger: ResearchDecisionTrigger::HourlyLatestCompleteGefsCycle,
                decision_cadence_secs: 3_600,
                target_horizon_secs: WEATHER_FORECAST_24H_HORIZON_SECS,
                max_holding_secs: WEATHER_FORECAST_24H_HORIZON_SECS,
                exit_heartbeat_secs: 60,
                fit_span_days: 90,
                max_feature_lookback_secs: 604_800,
                purge_embargo_secs: 172_800,
                feature_contract: ResearchFeatureContract::TradeBootstrapWeather,
                label_contract: ResearchLabelContract::FinalTokenPayoutRatio,
                cohort_contract: ResearchCohortContract::WeatherResolved,
                availability_policy: ResearchAvailabilityPolicy::FinalizedBlockConfirmation {
                    confirmation_blocks: 12,
                },
                serving_authority: ServingAuthority::ReportOnlyWithLiveL2,
                allowed_cash_budget_tiers: vec![Usd::new(Decimal::new(25, 0))],
                activation_eligibility: ResearchEvaluationTrack::ResearchOnly,
                quality_gate: TradePolicyQualityGate::production(),
                feedback_policy: production_feedback_policy(
                    SECONDS_PER_DAY,
                    50,
                    14 * SECONDS_PER_DAY,
                ),
            },
            published_at,
        )
    }
}

/// Return the complete closed registry for Runtime v1.
pub fn builtin_research_profiles() -> Result<Vec<ResearchProfileArtifact>, String> {
    let published_at = Utc
        .with_ymd_and_hms(2026, 7, 25, 0, 0, 0)
        .single()
        .ok_or_else(|| "research profile publication timestamp is invalid".to_owned())?;
    Ok(vec![
        ResearchProfileArtifact::pooled_full_l2(published_at)?,
        ResearchProfileArtifact::crypto_full_l2(published_at)?,
        ResearchProfileArtifact::weather_full_l2(published_at)?,
        ResearchProfileArtifact::pooled_trade_bootstrap(published_at)?,
        ResearchProfileArtifact::crypto_trade_bootstrap(published_at)?,
        ResearchProfileArtifact::weather_trade_bootstrap(published_at)?,
    ])
}

/// Runtime v1 raw-retention floor:
/// `max(180, 2 × max(required_days(profile)))`.
pub fn minimum_raw_retention_days() -> Result<u32, String> {
    let max_required = builtin_research_profiles()?
        .iter()
        .map(|profile| profile.spec.required_days())
        .collect::<Result<Vec<_>, _>>()?
        .into_iter()
        .max()
        .ok_or_else(|| "research profile registry is empty".to_owned())?;
    max_required
        .checked_mul(2)
        .map(|days| days.max(180))
        .ok_or_else(|| "raw-retention floor overflow".to_owned())
}

impl ResearchProfileRef {
    /// Resolve and verify an immutable built-in profile reference.
    pub fn resolve_builtin_research_profile(&self) -> Result<ResearchProfileArtifact, String> {
        builtin_research_profiles()?
            .into_iter()
            .find(|profile| profile.profile_ref == *self)
            .ok_or_else(|| {
                format!(
                    "unknown or content-mismatched research profile {}@{}",
                    self.id, self.version
                )
            })
    }
}

fn production_feedback_policy(
    feedback_cadence_secs: u64,
    minimum_new_mature_labels: u64,
    retraining_cooldown_secs: u64,
) -> ResearchFeedbackPolicy {
    ResearchFeedbackPolicy {
        evaluation_window_days: 30,
        max_challengers: 1,
        feedback_cadence_secs,
        minimum_mature_labels: 500,
        minimum_new_mature_labels,
        retraining_cooldown_secs,
        minimum_coverage: Decimal::new(95, 2),
        data_drift_psi_threshold: Decimal::new(2, 1),
        data_drift_ks_p_value: Decimal::new(5, 2),
        concept_target_rank_ic_drop: Decimal::new(3, 1),
        label_js_divergence: Decimal::new(1, 1),
        minimum_effect_bps: Bps::new(Decimal::from(25)),
        effect_confidence: Decimal::new(95, 2),
        comparison_minimum_observations: 500,
        comparison_bootstrap_repetitions: 10_000,
        comparison_block_length: 32,
        comparison_bootstrap_seed: 11_900_909,
        shadow_minimum_observations: 1_000,
    }
}

#[cfg(test)]
mod tests {
    use serde_json::{Value, from_value, to_value};

    use super::{
        Bps, CRYPTO_PRICE_15M_BOOTSTRAP_PROFILE_ID, CRYPTO_PRICE_15M_PROFILE_ID,
        MAX_FEEDBACK_DURATION_SECS, MAX_FEEDBACK_EVALUATION_WINDOW_DAYS,
        MAX_FEEDBACK_OBSERVATION_COUNT, POOLED_1H_CONTROL_PROFILE_ID,
        POOLED_BINARY_1H_BOOTSTRAP_PROFILE_ID, RESEARCH_FEEDBACK_POLICY_HASH_VERSION,
        ResearchEvaluationTrack, ResearchFeedbackPolicy, ResearchFeedbackPolicyError,
        ResearchProfileArtifact, ResearchProfileDataSource, SECONDS_PER_DAY, ServingAuthority,
        WEATHER_FORECAST_24H_BOOTSTRAP_PROFILE_ID, WEATHER_FORECAST_24H_HORIZON_SECS,
        WEATHER_FORECAST_24H_PROFILE_ID, builtin_research_profiles, minimum_raw_retention_days,
    };
    use crate::hashing::CanonicalDigest;

    type PolicyMutation = fn(&mut ResearchFeedbackPolicy);

    impl ResearchFeedbackPolicy {
        fn fixture() -> Self {
            builtin_research_profiles()
                .expect("profiles")
                .into_iter()
                .next()
                .expect("pooled profile")
                .spec
                .feedback_policy
        }
    }

    #[test]
    fn builtins_are_content_addressed() {
        let profiles = builtin_research_profiles().expect("profiles");
        assert_eq!(profiles.len(), 6);
        let pooled = profiles
            .iter()
            .find(|profile| profile.profile_ref.id.as_str() == POOLED_1H_CONTROL_PROFILE_ID)
            .expect("pooled profile");
        let crypto = profiles
            .iter()
            .find(|profile| profile.profile_ref.id.as_str() == CRYPTO_PRICE_15M_PROFILE_ID)
            .expect("crypto profile");
        let weather = profiles
            .iter()
            .find(|profile| profile.profile_ref.id.as_str() == WEATHER_FORECAST_24H_PROFILE_ID)
            .expect("weather profile");
        assert_eq!(pooled.profile_ref.version, 5);
        assert_eq!(crypto.profile_ref.version, 4);
        assert_eq!(weather.profile_ref.version, 5);
        assert_eq!(
            pooled.profile_ref.content_hash.to_string(),
            "blake3:c758bfc3d6307f58aea8db2b153f7cf120cb1e29584c9d0c3465241e248be2f3"
        );
        assert_eq!(
            crypto.profile_ref.content_hash.to_string(),
            "blake3:61657584a6794a28447389ba9f75ab5c2cf2be580cc8d081aadde67da4bdd49a"
        );
        assert_eq!(
            weather.profile_ref.content_hash.to_string(),
            "blake3:3b2eacbcf3ab5015afea2fb997a47ed9b5a8da0a8cff477d80ca43e27e55315e"
        );
        assert_eq!(pooled.spec.feedback_policy.feedback_cadence_secs, 86_400);
        assert_eq!(
            pooled.spec.feedback_policy.retraining_cooldown_secs,
            604_800
        );
        assert_eq!(crypto.spec.feedback_policy.feedback_cadence_secs, 21_600);
        assert_eq!(
            crypto.spec.feedback_policy.retraining_cooldown_secs,
            259_200
        );
        assert_eq!(weather.spec.feedback_policy.feedback_cadence_secs, 86_400);
        assert_eq!(
            weather.spec.feedback_policy.retraining_cooldown_secs,
            1_209_600
        );
        for profile in &profiles {
            assert_eq!(profile.spec.feedback_policy.minimum_new_mature_labels, 50);
            profile.spec.validate().expect("valid built-in profile");
            assert_eq!(
                CanonicalDigest::content_hash_json(&profile.spec).expect("profile hash"),
                profile.profile_ref.content_hash
            );
            let restored = from_value::<ResearchProfileArtifact>(
                to_value(profile).expect("serialize built-in profile"),
            )
            .expect("deserialize built-in profile");
            assert_eq!(&restored, profile);
            restored.spec.validate().expect("valid restored profile");

            let mut legacy_ref = profile.profile_ref.clone();
            legacy_ref.version = legacy_ref.version.checked_sub(1).expect("bumped version");
            assert!(legacy_ref.resolve_builtin_research_profile().is_err());
        }
        assert_eq!(weather.spec.fit_span_days, 90);
        assert_eq!(
            weather.spec.target_horizon_secs,
            WEATHER_FORECAST_24H_HORIZON_SECS
        );
        assert_eq!(weather.spec.allowed_cash_budget_tiers.len(), 1);
        assert_eq!(
            weather.spec.quality_gate.min_lower_confidence_utility_bps,
            Bps::ZERO
        );
        assert!(
            weather
                .spec
                .permits(ResearchEvaluationTrack::SemiAutoCandidate)
        );
        assert!(
            crypto
                .spec
                .permits(ResearchEvaluationTrack::SemiAutoCandidate)
        );
        assert_eq!(
            weather
                .profile_ref
                .resolve_builtin_research_profile()
                .expect("resolve")
                .profile_ref,
            weather.profile_ref
        );
        assert!(
            !pooled
                .spec
                .permits(ResearchEvaluationTrack::SemiAutoCandidate)
        );
        assert_eq!(weather.spec.required_days().expect("required days"), 100);
        assert_eq!(minimum_raw_retention_days().expect("retention"), 200);
        let bootstrap_ids = [
            POOLED_BINARY_1H_BOOTSTRAP_PROFILE_ID,
            CRYPTO_PRICE_15M_BOOTSTRAP_PROFILE_ID,
            WEATHER_FORECAST_24H_BOOTSTRAP_PROFILE_ID,
        ];
        for bootstrap_id in bootstrap_ids {
            let bootstrap = profiles
                .iter()
                .find(|profile| profile.profile_ref.id.as_str() == bootstrap_id)
                .expect("bootstrap profile");
            assert_eq!(
                bootstrap.spec.serving_authority,
                ServingAuthority::ReportOnlyWithLiveL2
            );
            assert!(!bootstrap.required_sources_contains(ResearchProfileDataSource::ClobL2));
            assert!(
                bootstrap.required_sources_contains(ResearchProfileDataSource::MarketExecution)
            );
            assert!(
                bootstrap
                    .required_sources_contains(ResearchProfileDataSource::ExecutionParticipant)
            );
            let expected_hash = match bootstrap_id {
                POOLED_BINARY_1H_BOOTSTRAP_PROFILE_ID => {
                    "blake3:1f047f98bc5e190699807adc17a97b3423cebd642d99be6d45c09b00783b893c"
                }
                CRYPTO_PRICE_15M_BOOTSTRAP_PROFILE_ID => {
                    "blake3:2781adf5aca31683001926b6b8300fdb6fb4a41bb36fe88aee1a9735a4a27ea5"
                }
                WEATHER_FORECAST_24H_BOOTSTRAP_PROFILE_ID => {
                    "blake3:1d020798301ffef16d89408155de6a8bd4fad8b54b3f49ef9b3cfe09213ac49e"
                }
                _ => unreachable!("closed bootstrap profile registry"),
            };
            assert_eq!(
                bootstrap.profile_ref.content_hash.to_string(),
                expected_hash
            );
        }
    }

    #[test]
    fn unknown_fields_are_rejected() {
        let mut value =
            to_value(ResearchFeedbackPolicy::fixture()).expect("serialize feedback policy");
        let removed_permission = ["auto", "publish", "eligible"].join("_");
        value
            .as_object_mut()
            .expect("policy object")
            .insert(removed_permission, Value::Bool(true));
        assert!(from_value::<ResearchFeedbackPolicy>(value).is_err());

        let mut unknown =
            to_value(ResearchFeedbackPolicy::fixture()).expect("serialize feedback policy");
        unknown
            .as_object_mut()
            .expect("policy object")
            .insert("unknown_schedule_knob".to_owned(), Value::from(1));
        assert!(from_value::<ResearchFeedbackPolicy>(unknown).is_err());
    }

    #[test]
    fn zero_values_are_rejected() {
        let mut policy = ResearchFeedbackPolicy::fixture();
        policy.evaluation_window_days = 0;
        assert_eq!(
            policy.validate(),
            Err(ResearchFeedbackPolicyError::ZeroDuration {
                field: "evaluation_window_days"
            })
        );

        let mut policy = ResearchFeedbackPolicy::fixture();
        policy.feedback_cadence_secs = 0;
        assert_eq!(
            policy.validate(),
            Err(ResearchFeedbackPolicyError::ZeroDuration {
                field: "feedback_cadence_secs"
            })
        );

        let mut policy = ResearchFeedbackPolicy::fixture();
        policy.retraining_cooldown_secs = 0;
        assert_eq!(
            policy.validate(),
            Err(ResearchFeedbackPolicyError::ZeroDuration {
                field: "retraining_cooldown_secs"
            })
        );

        let zero_counts: [(&str, PolicyMutation); 4] = [
            (
                "minimum_mature_labels",
                |policy: &mut ResearchFeedbackPolicy| policy.minimum_mature_labels = 0,
            ),
            (
                "minimum_new_mature_labels",
                |policy: &mut ResearchFeedbackPolicy| {
                    policy.minimum_new_mature_labels = 0;
                },
            ),
            (
                "comparison_minimum_observations",
                |policy: &mut ResearchFeedbackPolicy| {
                    policy.comparison_minimum_observations = 0;
                },
            ),
            (
                "shadow_minimum_observations",
                |policy: &mut ResearchFeedbackPolicy| {
                    policy.shadow_minimum_observations = 0;
                },
            ),
        ];
        for (field, mutate) in zero_counts {
            let mut policy = ResearchFeedbackPolicy::fixture();
            mutate(&mut policy);
            assert_eq!(
                policy.validate(),
                Err(ResearchFeedbackPolicyError::ZeroCount { field })
            );
        }
    }

    #[test]
    fn bounds_are_enforced() {
        let mut policy = ResearchFeedbackPolicy::fixture();
        policy.evaluation_window_days = MAX_FEEDBACK_EVALUATION_WINDOW_DAYS;
        policy.feedback_cadence_secs = MAX_FEEDBACK_DURATION_SECS;
        policy.retraining_cooldown_secs = MAX_FEEDBACK_DURATION_SECS;
        policy.minimum_mature_labels = MAX_FEEDBACK_OBSERVATION_COUNT;
        policy.minimum_new_mature_labels = MAX_FEEDBACK_OBSERVATION_COUNT;
        policy.comparison_minimum_observations = MAX_FEEDBACK_OBSERVATION_COUNT;
        policy.comparison_block_length = 1;
        policy.shadow_minimum_observations = MAX_FEEDBACK_OBSERVATION_COUNT;
        policy.validate().expect("inclusive boundaries");

        let mut policy = ResearchFeedbackPolicy::fixture();
        policy.evaluation_window_days = MAX_FEEDBACK_EVALUATION_WINDOW_DAYS + 1;
        assert_eq!(
            policy.validate(),
            Err(ResearchFeedbackPolicyError::EvaluationWindowExceeds {
                actual: MAX_FEEDBACK_EVALUATION_WINDOW_DAYS + 1,
                maximum: MAX_FEEDBACK_EVALUATION_WINDOW_DAYS,
            })
        );

        let excessive_durations: [(&str, PolicyMutation); 2] = [
            (
                "feedback_cadence_secs",
                |policy: &mut ResearchFeedbackPolicy| {
                    policy.feedback_cadence_secs = MAX_FEEDBACK_DURATION_SECS + 1;
                },
            ),
            (
                "retraining_cooldown_secs",
                |policy: &mut ResearchFeedbackPolicy| {
                    policy.retraining_cooldown_secs = MAX_FEEDBACK_DURATION_SECS + 1;
                },
            ),
        ];
        for (field, mutate) in excessive_durations {
            let mut policy = ResearchFeedbackPolicy::fixture();
            mutate(&mut policy);
            assert_eq!(
                policy.validate(),
                Err(ResearchFeedbackPolicyError::DurationExceeds {
                    field,
                    actual: MAX_FEEDBACK_DURATION_SECS + 1,
                    maximum: MAX_FEEDBACK_DURATION_SECS,
                })
            );
        }

        let excessive_counts: [(&str, PolicyMutation); 4] = [
            (
                "minimum_mature_labels",
                |policy: &mut ResearchFeedbackPolicy| {
                    policy.minimum_mature_labels = MAX_FEEDBACK_OBSERVATION_COUNT + 1;
                },
            ),
            (
                "minimum_new_mature_labels",
                |policy: &mut ResearchFeedbackPolicy| {
                    policy.minimum_mature_labels = MAX_FEEDBACK_OBSERVATION_COUNT;
                    policy.minimum_new_mature_labels = MAX_FEEDBACK_OBSERVATION_COUNT + 1;
                },
            ),
            (
                "comparison_minimum_observations",
                |policy: &mut ResearchFeedbackPolicy| {
                    policy.comparison_minimum_observations = MAX_FEEDBACK_OBSERVATION_COUNT + 1;
                },
            ),
            (
                "shadow_minimum_observations",
                |policy: &mut ResearchFeedbackPolicy| {
                    policy.shadow_minimum_observations = MAX_FEEDBACK_OBSERVATION_COUNT + 1;
                },
            ),
        ];
        for (field, mutate) in excessive_counts {
            let mut policy = ResearchFeedbackPolicy::fixture();
            mutate(&mut policy);
            assert_eq!(
                policy.validate(),
                Err(ResearchFeedbackPolicyError::CountExceeds {
                    field,
                    actual: MAX_FEEDBACK_OBSERVATION_COUNT + 1,
                    maximum: MAX_FEEDBACK_OBSERVATION_COUNT,
                })
            );
        }

        let mut policy = ResearchFeedbackPolicy::fixture();
        policy.feedback_cadence_secs = u64::MAX;
        assert!(matches!(
            policy.validate(),
            Err(ResearchFeedbackPolicyError::DurationExceeds {
                field: "feedback_cadence_secs",
                actual: u64::MAX,
                ..
            })
        ));
    }

    #[test]
    fn cross_field_invariants_hold() {
        let mut policy = ResearchFeedbackPolicy::fixture();
        policy.minimum_new_mature_labels = policy.minimum_mature_labels + 1;
        assert_eq!(
            policy.validate(),
            Err(ResearchFeedbackPolicyError::NewLabelsExceedTotal {
                minimum_new: 501,
                minimum_total: 500,
            })
        );

        let mut policy = ResearchFeedbackPolicy::fixture();
        policy.feedback_cadence_secs = 2 * SECONDS_PER_DAY;
        policy.retraining_cooldown_secs = SECONDS_PER_DAY;
        assert_eq!(
            policy.validate(),
            Err(ResearchFeedbackPolicyError::CooldownShorterThanCadence {
                cadence_secs: 2 * SECONDS_PER_DAY,
                cooldown_secs: SECONDS_PER_DAY,
            })
        );

        let mut policy = ResearchFeedbackPolicy::fixture();
        policy.evaluation_window_days = 1;
        policy.feedback_cadence_secs = SECONDS_PER_DAY + 1;
        policy.retraining_cooldown_secs = SECONDS_PER_DAY + 1;
        assert_eq!(
            policy.validate(),
            Err(ResearchFeedbackPolicyError::CadenceExceedsWindow {
                cadence_secs: SECONDS_PER_DAY + 1,
                evaluation_window_secs: SECONDS_PER_DAY,
            })
        );
    }

    #[test]
    fn schedule_fields_change_hash() {
        let profile = builtin_research_profiles()
            .expect("profiles")
            .into_iter()
            .next()
            .expect("pooled profile");
        let baseline = profile.profile_ref.content_hash;

        let mut cadence = profile.spec.clone();
        cadence.feedback_policy.feedback_cadence_secs += 1;
        cadence.validate().expect("cadence variant");
        assert_ne!(
            CanonicalDigest::content_hash_json(&cadence).expect("cadence hash"),
            baseline
        );

        let mut labels = profile.spec.clone();
        labels.feedback_policy.minimum_new_mature_labels += 1;
        labels.validate().expect("label variant");
        assert_ne!(
            CanonicalDigest::content_hash_json(&labels).expect("label hash"),
            baseline
        );

        let mut cooldown = profile.spec;
        cooldown.feedback_policy.retraining_cooldown_secs += 1;
        cooldown.validate().expect("cooldown variant");
        assert_ne!(
            CanonicalDigest::content_hash_json(&cooldown).expect("cooldown hash"),
            baseline
        );
    }

    #[test]
    fn policy_hash_is_deterministic() {
        assert_eq!(RESEARCH_FEEDBACK_POLICY_HASH_VERSION, 2);
        let profiles = builtin_research_profiles().expect("profiles");
        for profile in &profiles {
            let policy = &profile.spec.feedback_policy;
            let baseline = policy.content_hash().expect("policy hash");
            assert_eq!(
                policy.content_hash().expect("repeated policy hash"),
                baseline
            );
            let expected = match profile.profile_ref.id.as_str() {
                POOLED_1H_CONTROL_PROFILE_ID | POOLED_BINARY_1H_BOOTSTRAP_PROFILE_ID => {
                    "blake3:b90bf30d5f4e377784104540413a2101ee8ccf71401eb4cd0d47abef4c649934"
                }
                CRYPTO_PRICE_15M_PROFILE_ID | CRYPTO_PRICE_15M_BOOTSTRAP_PROFILE_ID => {
                    "blake3:2bf7e831cc4969a2f9a624696747cb79741928596319322859afade8e90d77f9"
                }
                WEATHER_FORECAST_24H_PROFILE_ID | WEATHER_FORECAST_24H_BOOTSTRAP_PROFILE_ID => {
                    "blake3:a37d8ce342764b69520df70f663a197040a9866804bc9a028718bb9bddf96810"
                }
                other => panic!("unexpected built-in profile {other}"),
            };
            assert_eq!(baseline.to_string(), expected);

            let mut cadence = policy.clone();
            cadence.feedback_cadence_secs += 1;
            assert_ne!(
                cadence.content_hash().expect("cadence policy hash"),
                baseline
            );

            let mut labels = policy.clone();
            labels.minimum_new_mature_labels += 1;
            assert_ne!(labels.content_hash().expect("label policy hash"), baseline);

            let mut cooldown = policy.clone();
            cooldown.retraining_cooldown_secs += 1;
            assert_ne!(
                cooldown.content_hash().expect("cooldown policy hash"),
                baseline
            );
        }
    }
}

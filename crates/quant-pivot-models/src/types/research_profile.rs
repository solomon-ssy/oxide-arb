//! Immutable research-profile contracts for policy fitting and serving lineage.

use std::{
    borrow::Cow,
    fmt::{Display, Formatter, Result as FmtResult},
    str::FromStr,
    sync::Arc,
};

use chrono::{DateTime, TimeZone, Utc};
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
pub const POOLED_1H_HORIZON_SECS: u64 = 3_600;
pub const CRYPTO_PRICE_15M_HORIZON_SECS: u64 = 900;
pub const WEATHER_FORECAST_24H_HORIZON_SECS: u64 = 86_400;

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
}

/// Information set under which policy decisions are evaluated.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ResearchInformationRegime {
    PooledBinaryMarket,
    CryptoPrice,
    WeatherForecast,
}

/// Closed market selection supported by the profile implementation.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ResearchMarketSelector {
    AllEligible,
    CryptoPriceContract,
    WeatherContract,
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
    ClobMarketInfo,
    ClobL2,
    TradeTape,
    BinanceMarketData,
    PolymarketRtds,
    AviationWeather,
    GhcnhCalibration,
    GefsEnsemble,
    PolymarketResolution,
}

/// Immutable feedback methodology and promotion thresholds owned by a profile.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, FromJsonQueryResult)]
#[serde(deny_unknown_fields)]
pub struct ResearchFeedbackPolicy {
    pub evaluation_window_days: u32,
    pub minimum_mature_labels: u64,
    pub minimum_coverage: Decimal,
    pub data_drift_psi_threshold: Decimal,
    pub data_drift_ks_p_value: Decimal,
    pub concept_rank_ic_drop: Decimal,
    pub label_js_divergence: Decimal,
    pub minimum_effect_bps: Bps,
    pub effect_confidence: Decimal,
    pub shadow_minimum_observations: u64,
    pub auto_publish_eligible: bool,
}

impl ResearchFeedbackPolicy {
    fn validate(&self) -> Result<(), String> {
        if self.evaluation_window_days == 0
            || self.minimum_mature_labels == 0
            || self.shadow_minimum_observations == 0
            || self.minimum_effect_bps.inner() <= Decimal::ZERO
        {
            return Err("feedback policy counts/windows/effect must be positive".to_owned());
        }
        for (name, value) in [
            ("minimum_coverage", self.minimum_coverage),
            ("data_drift_ks_p_value", self.data_drift_ks_p_value),
            ("concept_rank_ic_drop", self.concept_rank_ic_drop),
            ("label_js_divergence", self.label_js_divergence),
            ("effect_confidence", self.effect_confidence),
        ] {
            if value <= Decimal::ZERO || value > Decimal::ONE {
                return Err(format!("feedback policy {name} must be within (0, 1]"));
            }
        }
        if self.data_drift_psi_threshold <= Decimal::ZERO {
            return Err("feedback policy PSI threshold must be positive".to_owned());
        }
        Ok(())
    }
}

/// Profile content. The content hash excludes publication metadata.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, FromJsonQueryResult)]
#[serde(deny_unknown_fields)]
pub struct ResearchProfileSpec {
    pub information_regime: ResearchInformationRegime,
    pub policy_fitter: Option<ResearchPolicyFitter>,
    pub market_selector: ResearchMarketSelector,
    pub category: Option<MarketCategory>,
    pub decision_trigger: ResearchDecisionTrigger,
    pub decision_cadence_secs: u64,
    pub target_horizon_secs: u64,
    pub max_holding_secs: u64,
    pub exit_heartbeat_secs: u64,
    pub fit_span_days: u32,
    pub max_feature_lookback_secs: u64,
    pub purge_embargo_secs: u64,
    pub required_sources: Vec<ResearchProfileDataSource>,
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
                | ResearchInformationRegime::CryptoPrice,
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
        if self.required_sources.is_empty()
            || self
                .required_sources
                .windows(2)
                .any(|sources| sources[0] >= sources[1])
        {
            return Err(
                "research profile sources must be unique and canonically sorted".to_owned(),
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
        self.feedback_policy.validate()
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
        const SECONDS_PER_DAY: u64 = 86_400;

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
        self.spec.required_sources.binary_search(&source).is_ok()
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
}

/// Return the complete closed registry for Runtime v1.
pub fn builtin_research_profiles() -> Result<Vec<ResearchProfileArtifact>, String> {
    let published_at = Utc
        .with_ymd_and_hms(2026, 7, 14, 0, 0, 0)
        .single()
        .ok_or_else(|| "research profile publication timestamp is invalid".to_owned())?;
    let cash_budget = vec![Usd::new(Decimal::new(25, 0))];
    let pooled = ResearchProfileArtifact::try_new(
        POOLED_1H_CONTROL_PROFILE_ID,
        2,
        ResearchProfileSpec {
            information_regime: ResearchInformationRegime::PooledBinaryMarket,
            policy_fitter: None,
            market_selector: ResearchMarketSelector::AllEligible,
            category: None,
            decision_trigger: ResearchDecisionTrigger::Hourly,
            decision_cadence_secs: 3_600,
            target_horizon_secs: POOLED_1H_HORIZON_SECS,
            max_holding_secs: POOLED_1H_HORIZON_SECS,
            exit_heartbeat_secs: 60,
            fit_span_days: 30,
            max_feature_lookback_secs: 86_400,
            purge_embargo_secs: 86_400,
            required_sources: vec![
                ResearchProfileDataSource::CatalogLedger,
                ResearchProfileDataSource::ClobMarketInfo,
                ResearchProfileDataSource::ClobL2,
                ResearchProfileDataSource::TradeTape,
            ],
            allowed_cash_budget_tiers: cash_budget.clone(),
            activation_eligibility: ResearchEvaluationTrack::ResearchOnly,
            quality_gate: production_quality_gate(),
            feedback_policy: production_feedback_policy(false),
        },
        published_at,
    )?;
    let crypto = ResearchProfileArtifact::try_new(
        CRYPTO_PRICE_15M_PROFILE_ID,
        1,
        ResearchProfileSpec {
            information_regime: ResearchInformationRegime::CryptoPrice,
            policy_fitter: None,
            market_selector: ResearchMarketSelector::CryptoPriceContract,
            category: Some(MarketCategory::Crypto),
            decision_trigger: ResearchDecisionTrigger::EveryFiveMinutes,
            decision_cadence_secs: 300,
            target_horizon_secs: CRYPTO_PRICE_15M_HORIZON_SECS,
            max_holding_secs: CRYPTO_PRICE_15M_HORIZON_SECS,
            exit_heartbeat_secs: 30,
            fit_span_days: 90,
            max_feature_lookback_secs: 86_400,
            purge_embargo_secs: 3_600,
            required_sources: vec![
                ResearchProfileDataSource::CatalogLedger,
                ResearchProfileDataSource::ClobMarketInfo,
                ResearchProfileDataSource::ClobL2,
                ResearchProfileDataSource::TradeTape,
                ResearchProfileDataSource::BinanceMarketData,
                ResearchProfileDataSource::PolymarketRtds,
                ResearchProfileDataSource::PolymarketResolution,
            ],
            allowed_cash_budget_tiers: cash_budget.clone(),
            activation_eligibility: ResearchEvaluationTrack::ResearchOnly,
            quality_gate: production_quality_gate(),
            feedback_policy: production_feedback_policy(true),
        },
        published_at,
    )?;
    let weather = ResearchProfileArtifact::try_new(
        WEATHER_FORECAST_24H_PROFILE_ID,
        2,
        ResearchProfileSpec {
            information_regime: ResearchInformationRegime::WeatherForecast,
            policy_fitter: Some(ResearchPolicyFitter::WeatherForecast),
            market_selector: ResearchMarketSelector::WeatherContract,
            category: Some(MarketCategory::Weather),
            decision_trigger: ResearchDecisionTrigger::HourlyLatestCompleteGefsCycle,
            decision_cadence_secs: 3_600,
            target_horizon_secs: WEATHER_FORECAST_24H_HORIZON_SECS,
            max_holding_secs: WEATHER_FORECAST_24H_HORIZON_SECS,
            exit_heartbeat_secs: 60,
            fit_span_days: 90,
            max_feature_lookback_secs: 604_800,
            purge_embargo_secs: 172_800,
            required_sources: vec![
                ResearchProfileDataSource::CatalogLedger,
                ResearchProfileDataSource::ClobMarketInfo,
                ResearchProfileDataSource::ClobL2,
                ResearchProfileDataSource::TradeTape,
                ResearchProfileDataSource::AviationWeather,
                ResearchProfileDataSource::GhcnhCalibration,
                ResearchProfileDataSource::GefsEnsemble,
                ResearchProfileDataSource::PolymarketResolution,
            ],
            allowed_cash_budget_tiers: cash_budget,
            activation_eligibility: ResearchEvaluationTrack::SemiAutoCandidate,
            quality_gate: production_quality_gate(),
            feedback_policy: production_feedback_policy(true),
        },
        published_at,
    )?;
    Ok(vec![pooled, crypto, weather])
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

/// Resolve and verify an immutable built-in profile reference.
pub fn resolve_builtin_research_profile(
    profile_ref: &ResearchProfileRef,
) -> Result<ResearchProfileArtifact, String> {
    builtin_research_profiles()?
        .into_iter()
        .find(|profile| profile.profile_ref == *profile_ref)
        .ok_or_else(|| {
            format!(
                "unknown or content-mismatched research profile {}@{}",
                profile_ref.id, profile_ref.version
            )
        })
}

fn production_quality_gate() -> TradePolicyQualityGate {
    TradePolicyQualityGate {
        min_effective_sample_size: 100,
        min_full_l2_coverage: Decimal::new(95, 2),
        min_common_candidate_support: Decimal::new(95, 2),
        min_passive_reconciled_trade_coverage: Decimal::new(95, 2),
        min_fee_catalog_coverage: Decimal::ONE,
        min_eligible_market_coverage: Decimal::new(95, 2),
        min_cpcv_paths: 21,
        min_deflated_sharpe_ratio: Decimal::new(95, 2),
        max_probability_of_backtest_overfitting: Decimal::new(5, 1),
        max_ambiguous_touch_rate: Decimal::new(5, 2),
        max_depth_failure_rate: Decimal::new(5, 2),
        min_lower_confidence_utility_bps: Bps::ZERO,
    }
}

fn production_feedback_policy(auto_publish_eligible: bool) -> ResearchFeedbackPolicy {
    ResearchFeedbackPolicy {
        evaluation_window_days: 30,
        minimum_mature_labels: 500,
        minimum_coverage: Decimal::new(95, 2),
        data_drift_psi_threshold: Decimal::new(2, 1),
        data_drift_ks_p_value: Decimal::new(5, 2),
        concept_rank_ic_drop: Decimal::new(3, 1),
        label_js_divergence: Decimal::new(1, 1),
        minimum_effect_bps: Bps::new(Decimal::from(25)),
        effect_confidence: Decimal::new(95, 2),
        shadow_minimum_observations: 1_000,
        auto_publish_eligible,
    }
}

#[cfg(test)]
mod tests {
    use super::{
        Bps, ResearchEvaluationTrack, WEATHER_FORECAST_24H_HORIZON_SECS, builtin_research_profiles,
        minimum_raw_retention_days, resolve_builtin_research_profile,
    };

    #[test]
    fn builtins_are_hash_stable_and_weather_is_the_only_semi_auto_candidate() {
        let profiles = builtin_research_profiles().expect("profiles");
        assert_eq!(profiles.len(), 3);
        let weather = profiles
            .iter()
            .find(|profile| profile.spec.target_horizon_secs == WEATHER_FORECAST_24H_HORIZON_SECS)
            .expect("weather profile");
        assert_eq!(weather.spec.fit_span_days, 90);
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
        assert_eq!(
            resolve_builtin_research_profile(&weather.profile_ref)
                .expect("resolve")
                .profile_ref,
            weather.profile_ref
        );
        let control = profiles
            .iter()
            .find(|profile| profile.profile_ref != weather.profile_ref)
            .expect("control profile");
        assert!(
            !control
                .spec
                .permits(ResearchEvaluationTrack::SemiAutoCandidate)
        );
        assert_eq!(weather.spec.required_days().expect("required days"), 100);
        assert_eq!(minimum_raw_retention_days().expect("retention"), 200);
    }
}

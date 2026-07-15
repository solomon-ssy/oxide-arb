//! Canonical decision clock and source-visibility boundary.

use std::{collections::BTreeMap, time::Duration as StdDuration};

use chrono::{DateTime, Duration, Utc};
use quant_pivot_error::{QuantError, QuantResult};
use sea_orm::FromJsonQueryResult;
use serde::{Deserialize, Serialize};

/// Governed source whose effective-time visibility is frozen in a decision.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum DecisionSource {
    Catalog,
    Book,
    Microstructure,
    TradeTape,
    Linkage,
    DomainCrypto,
    DomainWeather,
}

impl DecisionSource {
    /// Stable wire key used by persisted and operator-facing cutoff maps.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Catalog => "catalog",
            Self::Book => "book",
            Self::Microstructure => "microstructure",
            Self::TradeTape => "trade_tape",
            Self::Linkage => "linkage",
            Self::DomainCrypto => "domain_crypto",
            Self::DomainWeather => "domain_weather",
        }
    }
}

/// The immutable time boundary governing one research or serving decision.
///
/// `decision_at` is when the decision is made. `knowledge_cutoff` is the latest
/// globally visible source instant. Keeping both in one value prevents callers
/// from subtracting the configured lag a second time.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, FromJsonQueryResult)]
pub struct DecisionBoundary {
    decision_at: DateTime<Utc>,
    knowledge_cutoff: DateTime<Utc>,
    knowledge_lag_secs: u64,
    per_source_cutoffs: BTreeMap<DecisionSource, DateTime<Utc>>,
}

crate::jsonb_active!(DecisionBoundary);

impl DecisionBoundary {
    /// Validate a boundary loaded from an artifact or persistence layer.
    ///
    /// Constructors preserve these invariants by construction, but serialized
    /// values cross an untrusted boundary and must be checked before replay.
    pub fn validate(&self) -> QuantResult<()> {
        let lag = checked_duration(self.knowledge_lag_secs, "knowledge_lag_secs")?;
        let expected = self
            .decision_at
            .checked_sub_signed(lag)
            .ok_or_else(|| QuantError::config("knowledge cutoff is outside chrono range"))?;
        if self.knowledge_cutoff != expected {
            return Err(QuantError::config(format!(
                "knowledge cutoff {} does not equal decision time {} minus {} seconds",
                self.knowledge_cutoff, self.decision_at, self.knowledge_lag_secs
            )));
        }
        if let Some((source, cutoff)) = self
            .per_source_cutoffs
            .iter()
            .find(|(_, cutoff)| **cutoff > self.knowledge_cutoff)
        {
            return Err(QuantError::config(format!(
                "source {source:?} cutoff {cutoff} is after global knowledge cutoff {}",
                self.knowledge_cutoff
            )));
        }
        Ok(())
    }

    /// Decision/trigger instant for the round.
    #[must_use]
    pub const fn decision_at(&self) -> DateTime<Utc> {
        self.decision_at
    }

    /// Latest globally visible source instant for the round.
    #[must_use]
    pub const fn knowledge_cutoff(&self) -> DateTime<Utc> {
        self.knowledge_cutoff
    }

    /// Governed global knowledge lag in seconds.
    #[must_use]
    pub const fn knowledge_lag_secs(&self) -> u64 {
        self.knowledge_lag_secs
    }

    /// Governed global knowledge lag as a standard duration.
    #[must_use]
    pub const fn knowledge_lag(&self) -> StdDuration {
        StdDuration::from_secs(self.knowledge_lag_secs)
    }

    /// Freeze one source cutoff. A source can be registered exactly once;
    /// downstream readers only look it up and never subtract lag again.
    pub fn with_source_cutoff(
        mut self,
        source: DecisionSource,
        availability_lag_secs: u64,
    ) -> QuantResult<Self> {
        if self.per_source_cutoffs.contains_key(&source) {
            return Err(QuantError::config(format!(
                "decision source {source:?} cutoff was derived more than once"
            )));
        }
        let source_lag = checked_duration(availability_lag_secs, "availability_lag_secs")?;
        let source_cutoff = self
            .decision_at
            .checked_sub_signed(source_lag)
            .ok_or_else(|| {
                QuantError::config("source visibility cutoff is outside chrono range")
            })?;
        self.per_source_cutoffs
            .insert(source, self.knowledge_cutoff.min(source_cutoff));
        Ok(self)
    }

    /// Frozen cutoff for `source`; sources without an additional lag use the
    /// global knowledge cutoff.
    #[must_use]
    pub fn cutoff_for(&self, source: DecisionSource) -> DateTime<Utc> {
        self.per_source_cutoffs
            .get(&source)
            .copied()
            .unwrap_or(self.knowledge_cutoff)
    }

    /// Every explicitly frozen source cutoff, for Parquet/report evidence.
    #[must_use]
    pub const fn per_source_cutoffs(&self) -> &BTreeMap<DecisionSource, DateTime<Utc>> {
        &self.per_source_cutoffs
    }

    /// Move a persisted boundary to another decision instant while preserving
    /// every governed global/source availability lag exactly once.
    pub fn rebased(&self, decision_at: DateTime<Utc>) -> QuantResult<Self> {
        let mut boundary = DecisionClock::new(self.knowledge_lag_secs).boundary(decision_at)?;
        for (source, cutoff) in &self.per_source_cutoffs {
            let source_lag = self
                .decision_at
                .signed_duration_since(*cutoff)
                .num_seconds();
            if source_lag < 0 {
                return Err(QuantError::config(format!(
                    "source {source:?} cutoff is after the original decision"
                )));
            }
            boundary = boundary.with_source_cutoff(
                *source,
                u64::try_from(source_lag).map_err(|error| {
                    QuantError::config(format!("source {source:?} lag is invalid: {error}"))
                })?,
            )?;
        }
        boundary.validate()?;
        Ok(boundary)
    }
}

/// Constructs [`DecisionBoundary`] values from one governed knowledge lag.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct DecisionClock {
    knowledge_lag_secs: u64,
}

impl DecisionClock {
    /// Build a decision clock from the governed lag in seconds.
    #[must_use]
    pub const fn new(knowledge_lag_secs: u64) -> Self {
        Self { knowledge_lag_secs }
    }

    /// Build the sole time boundary for `decision_at`.
    pub fn boundary(self, decision_at: DateTime<Utc>) -> QuantResult<DecisionBoundary> {
        let knowledge_lag = checked_duration(self.knowledge_lag_secs, "knowledge_lag_secs")?;
        let knowledge_cutoff = decision_at
            .checked_sub_signed(knowledge_lag)
            .ok_or_else(|| QuantError::config("knowledge cutoff is outside chrono range"))?;
        let boundary = DecisionBoundary {
            decision_at,
            knowledge_cutoff,
            knowledge_lag_secs: self.knowledge_lag_secs,
            per_source_cutoffs: BTreeMap::new(),
        };
        boundary.validate()?;
        Ok(boundary)
    }

    /// Build the canonical serving/replay boundary with every governed source
    /// cutoff frozen once from `decision_at`.
    ///
    /// Report generation, exit re-inference, parity replay, and offline replay
    /// must all use this constructor so persisted `per_source_cutoffs` agree.
    pub fn serving_boundary(
        self,
        decision_at: DateTime<Utc>,
        domain_crypto_lag_secs: u64,
        domain_weather_lag_secs: u64,
    ) -> QuantResult<DecisionBoundary> {
        self.boundary(decision_at)?
            .with_source_cutoff(DecisionSource::Catalog, 0)?
            .with_source_cutoff(DecisionSource::Book, 0)?
            .with_source_cutoff(DecisionSource::Microstructure, 0)?
            .with_source_cutoff(DecisionSource::TradeTape, 0)?
            .with_source_cutoff(DecisionSource::Linkage, 0)?
            .with_source_cutoff(DecisionSource::DomainCrypto, domain_crypto_lag_secs)?
            .with_source_cutoff(DecisionSource::DomainWeather, domain_weather_lag_secs)
    }
}

fn checked_duration(seconds: u64, field: &'static str) -> QuantResult<Duration> {
    let seconds = i64::try_from(seconds)
        .map_err(|error| QuantError::config(format!("{field} too large: {error}")))?;
    Ok(Duration::seconds(seconds))
}

#[cfg(test)]
mod tests {
    use chrono::{Duration, TimeZone, Utc};

    use super::{DecisionClock, DecisionSource};
    use crate::domain::DecisionBoundary;

    #[test]
    fn derives_global_cutoff_once() {
        let decision_at = Utc.with_ymd_and_hms(2026, 7, 10, 12, 0, 0).unwrap();
        let boundary = DecisionClock::new(120)
            .boundary(decision_at)
            .expect("valid boundary");

        assert_eq!(boundary.decision_at(), decision_at);
        assert_eq!(
            boundary.knowledge_cutoff(),
            decision_at - Duration::seconds(120)
        );
        assert_eq!(boundary.knowledge_lag_secs(), 120);
    }

    #[test]
    fn source_cutoff_uses_the_stricter_lag() {
        let decision_at = Utc.with_ymd_and_hms(2026, 7, 10, 12, 0, 0).unwrap();
        let boundary = DecisionClock::new(120)
            .boundary(decision_at)
            .expect("valid boundary");

        let boundary = boundary
            .with_source_cutoff(DecisionSource::Book, 5)
            .expect("book cutoff")
            .with_source_cutoff(DecisionSource::DomainCrypto, 300)
            .expect("domain cutoff");
        assert_eq!(
            boundary.cutoff_for(DecisionSource::Book),
            decision_at - Duration::seconds(120)
        );
        assert_eq!(
            boundary.cutoff_for(DecisionSource::DomainCrypto),
            decision_at - Duration::seconds(300)
        );
    }

    #[test]
    fn rejects_deriving_one_source_twice() {
        let decision_at = Utc.with_ymd_and_hms(2026, 7, 10, 12, 0, 0).unwrap();
        let boundary = DecisionClock::new(120)
            .boundary(decision_at)
            .expect("boundary")
            .with_source_cutoff(DecisionSource::Catalog, 0)
            .expect("catalog cutoff");
        assert!(
            boundary
                .with_source_cutoff(DecisionSource::Catalog, 0)
                .is_err()
        );
    }

    #[test]
    fn rejects_unrepresentable_lag() {
        let decision_at = Utc.with_ymd_and_hms(2026, 7, 10, 12, 0, 0).unwrap();
        assert!(DecisionClock::new(u64::MAX).boundary(decision_at).is_err());
    }

    #[test]
    fn serving_boundary_registers_every_governed_source() {
        let decision_at = Utc.with_ymd_and_hms(2026, 7, 10, 12, 0, 0).unwrap();
        let boundary = DecisionClock::new(120)
            .serving_boundary(decision_at, 300, 600)
            .expect("serving boundary");

        assert_eq!(
            boundary.cutoff_for(DecisionSource::Catalog),
            decision_at - Duration::seconds(120)
        );
        assert_eq!(
            boundary.cutoff_for(DecisionSource::Book),
            decision_at - Duration::seconds(120)
        );
        assert_eq!(
            boundary.cutoff_for(DecisionSource::Microstructure),
            decision_at - Duration::seconds(120)
        );
        assert_eq!(
            boundary.cutoff_for(DecisionSource::TradeTape),
            decision_at - Duration::seconds(120)
        );
        assert_eq!(
            boundary.cutoff_for(DecisionSource::Linkage),
            decision_at - Duration::seconds(120)
        );
        assert_eq!(
            boundary.cutoff_for(DecisionSource::DomainCrypto),
            decision_at - Duration::seconds(300)
        );
        assert_eq!(
            boundary.cutoff_for(DecisionSource::DomainWeather),
            decision_at - Duration::seconds(600)
        );
        assert_eq!(boundary.per_source_cutoffs().len(), 7);
    }

    #[test]
    fn rejects_tampered_serialized_cutoff() {
        let decision_at = Utc.with_ymd_and_hms(2026, 7, 10, 12, 0, 0).unwrap();
        let boundary = DecisionClock::new(120)
            .boundary(decision_at)
            .expect("boundary");
        let mut payload = serde_json::to_value(boundary).expect("serialize");
        payload["knowledge_cutoff"] =
            serde_json::to_value(decision_at).expect("serialize decision time");
        let tampered: DecisionBoundary =
            serde_json::from_value(payload).expect("deserialize shape");

        assert!(tampered.validate().is_err());
    }

    #[test]
    fn rebase_preserves_every_effective_source_lag() {
        let original_at = Utc.with_ymd_and_hms(2026, 7, 10, 12, 0, 0).unwrap();
        let rebased_at = original_at + Duration::hours(6);
        let original = DecisionClock::new(120)
            .serving_boundary(original_at, 300, 600)
            .expect("boundary");
        let rebased = original.rebased(rebased_at).expect("rebased");

        for source in [
            DecisionSource::Catalog,
            DecisionSource::Book,
            DecisionSource::Microstructure,
            DecisionSource::TradeTape,
            DecisionSource::Linkage,
            DecisionSource::DomainCrypto,
            DecisionSource::DomainWeather,
        ] {
            assert_eq!(
                original_at - original.cutoff_for(source),
                rebased_at - rebased.cutoff_for(source)
            );
        }
    }
}

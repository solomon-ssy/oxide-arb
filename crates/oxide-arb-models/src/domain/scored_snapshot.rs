use crate::{
    domain::{
        calibration::{BucketKey, CalibrationSnapshot},
        opportunity::Opportunity,
    },
    enums::{
        calibration::{DurationBucket, PriceZone},
        common::{MarketCategory, Side, StalenessLevel},
        evidence::MissingEvidenceField,
    },
    types::{
        Bps, EventId, FactorPublicationId, MarketId, MicroProb, MicroScore, OpportunityId, Price,
        Shares, TokenId, Usd,
    },
};
use chrono::{DateTime, Utc};
use num_traits::ToPrimitive;
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};

/// Frozen evidence captured when an opportunity is scored.
///
/// This snapshot is persisted with the trade and mirrored into `ClickHouse`
/// audit rows so terminal/settlement facts never have to reconstruct scoring
/// attribution from current state. Legacy scalar fields remain available for
/// execution paths that need cheap numeric access, but the typed evidence
/// fields are the canonical materialization input.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ScoredOpportunitySnapshot {
    pub opportunity_id: OpportunityId,
    pub market_id: MarketId,
    pub event_id: EventId,
    pub token_id: TokenId,
    pub token_yes: Option<TokenId>,
    pub token_no: Option<TokenId>,
    pub side: Side,
    pub category: MarketCategory,
    pub entry_price: Price,
    pub edge_bps: Bps,
    pub expected_net_profit: Usd,
    pub net_profit_if_correct: Usd,
    pub shares: Shares,
    pub total_cost: Usd,
    pub total_fees: Usd,
    pub resolution_prob: f64,
    pub resolution_prob_decimal: Decimal,
    pub confidence: f64,
    pub confidence_decimal: Decimal,
    pub fill_probability: Option<MicroProb>,
    pub score: Option<MicroScore>,
    pub urgency_factor: Option<MicroProb>,
    pub category_weight: Option<MicroProb>,
    pub staleness_discount: Option<MicroProb>,
    pub convergence_secs: u32,
    pub price_zone: PriceZone,
    pub duration_bucket: DurationBucket,
    pub depth_used_pct: f64,
    pub depth_used_pct_decimal: Decimal,
    pub staleness: StalenessLevel,
    pub calibration: CalibrationEvidenceSnapshot,
    pub book: Option<BookEvidenceSnapshot>,
    pub factors: Option<AppliedFactorTrace>,
    pub missing_fields: Vec<MissingEvidenceField>,
    pub detected_at: DateTime<Utc>,
    pub schema_version: u32,
}

impl ScoredOpportunitySnapshot {
    pub const SCHEMA_VERSION: u32 = 2;

    #[must_use]
    pub fn from_opportunity(opp: &Opportunity) -> Self {
        Self {
            opportunity_id: opp.opportunity_id.clone(),
            market_id: opp.market_id.clone(),
            event_id: opp.event_id.clone(),
            token_id: opp.token_id.clone(),
            token_yes: None,
            token_no: None,
            side: opp.side,
            category: opp.category,
            entry_price: opp.entry_price,
            edge_bps: opp.edge_bps,
            expected_net_profit: opp.expected_net_profit,
            net_profit_if_correct: opp.net_profit,
            shares: opp.shares,
            total_cost: opp.total_cost,
            total_fees: opp.total_fees,
            resolution_prob: opp.resolution_adjust.to_f64().unwrap_or(0.0),
            resolution_prob_decimal: opp.resolution_adjust,
            confidence: opp.meta.confidence.to_f64().unwrap_or(0.0),
            confidence_decimal: opp.meta.confidence,
            fill_probability: None,
            score: None,
            urgency_factor: None,
            category_weight: None,
            staleness_discount: None,
            convergence_secs: u32::try_from(
                opp.meta.convergence_duration_secs.min(u64::from(u32::MAX)),
            )
            .unwrap_or(u32::MAX),
            price_zone: opp.meta.price_zone,
            duration_bucket: opp.meta.duration_bucket,
            depth_used_pct: opp.depth_used_pct.to_f64().unwrap_or(0.0),
            depth_used_pct_decimal: opp.depth_used_pct,
            staleness: opp.staleness,
            calibration: CalibrationEvidenceSnapshot::from(&opp.calibration),
            book: None,
            factors: None,
            missing_fields: vec![
                MissingEvidenceField::TokenYes,
                MissingEvidenceField::TokenNo,
                MissingEvidenceField::FillProbability,
                MissingEvidenceField::Score,
                MissingEvidenceField::BookContext,
                MissingEvidenceField::AppliedFactors,
            ],
            detected_at: opp.detected_at,
            schema_version: Self::SCHEMA_VERSION,
        }
    }

    #[must_use]
    pub fn with_score_components(
        mut self,
        fill_probability: MicroProb,
        score: MicroScore,
        urgency_factor: MicroProb,
        category_weight: MicroProb,
        staleness_discount: MicroProb,
    ) -> Self {
        self.fill_probability = Some(fill_probability);
        self.score = Some(score);
        self.urgency_factor = Some(urgency_factor);
        self.category_weight = Some(category_weight);
        self.staleness_discount = Some(staleness_discount);
        self.remove_missing(MissingEvidenceField::FillProbability);
        self.remove_missing(MissingEvidenceField::Score);
        self
    }

    #[must_use]
    pub fn with_book_context(
        mut self,
        token_yes: TokenId,
        token_no: TokenId,
        yes_book_version: u64,
        no_book_version: u64,
    ) -> Self {
        self.token_yes = Some(token_yes);
        self.token_no = Some(token_no);
        self.book = Some(BookEvidenceSnapshot {
            yes_book_version: Some(yes_book_version),
            no_book_version: Some(no_book_version),
            book_age_ms: None,
            context_json: None,
        });
        self.remove_missing(MissingEvidenceField::TokenYes);
        self.remove_missing(MissingEvidenceField::TokenNo);
        self.remove_missing(MissingEvidenceField::BookContext);
        self
    }

    #[must_use]
    pub fn with_publication(mut self, publication_id: FactorPublicationId) -> Self {
        let mut factors = self.factors.unwrap_or_else(AppliedFactorTrace::known_empty);
        factors.control_publication_id = Some(publication_id);
        self.factors = Some(factors);
        self.remove_missing(MissingEvidenceField::AppliedFactors);
        self
    }

    #[must_use]
    pub fn with_known_empty_factor_trace(mut self) -> Self {
        self.factors = Some(AppliedFactorTrace::known_empty());
        self
    }

    /// Record the control factors actually applied to this decision.
    ///
    /// Replaces the empty placeholder so terminal/settlement audit and detection
    /// rows preserve the publication id, factor ids, and per-factor input/output
    /// effects rather than hiding them in logs.
    #[must_use]
    pub fn with_applied_control_factors(
        mut self,
        publication_id: Option<FactorPublicationId>,
        applied: &[crate::domain::control_factor::AppliedControlFactor],
    ) -> Self {
        let factor_ids = applied
            .iter()
            .map(|factor| factor.factor_id.to_string())
            .collect();
        let effects_json = if applied.is_empty() {
            None
        } else {
            serde_json::to_value(applied).ok()
        };
        self.factors = Some(AppliedFactorTrace {
            control_publication_id: publication_id,
            factor_ids,
            effects_json,
        });
        self.remove_missing(MissingEvidenceField::AppliedFactors);
        self
    }

    #[must_use]
    pub const fn calibration_bucket_key(&self) -> BucketKey {
        BucketKey {
            category: self.category,
            price_zone: self.price_zone,
            duration_bucket: self.duration_bucket,
        }
    }

    fn remove_missing(&mut self, field: MissingEvidenceField) {
        self.missing_fields.retain(|item| *item != field);
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CalibrationEvidenceSnapshot {
    pub sample_size: u32,
    pub alpha_prior: Decimal,
    pub beta_prior: Decimal,
    pub posterior_mean: Decimal,
    pub fallback_tier: u8,
    pub snapshot_hash: Option<String>,
}

impl From<&CalibrationSnapshot> for CalibrationEvidenceSnapshot {
    fn from(snapshot: &CalibrationSnapshot) -> Self {
        Self {
            sample_size: snapshot.sample_size,
            alpha_prior: snapshot.alpha_prior,
            beta_prior: snapshot.beta_prior,
            posterior_mean: snapshot.posterior_mean,
            fallback_tier: snapshot.fallback_tier,
            snapshot_hash: None,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BookEvidenceSnapshot {
    pub yes_book_version: Option<u64>,
    pub no_book_version: Option<u64>,
    pub book_age_ms: Option<u64>,
    pub context_json: Option<serde_json::Value>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AppliedFactorTrace {
    pub control_publication_id: Option<FactorPublicationId>,
    pub factor_ids: Vec<String>,
    pub effects_json: Option<serde_json::Value>,
}

impl AppliedFactorTrace {
    #[must_use]
    pub const fn known_empty() -> Self {
        Self {
            control_publication_id: None,
            factor_ids: Vec::new(),
            effects_json: None,
        }
    }
}

//! Durable feedback invalidation wire contract.

use chrono::{DateTime, Utc};
use quant_pivot_error::feedback::FeedbackError;
use serde::{Deserialize, Serialize};

use crate::{
    domain::quant::FeedbackOutboxEntry,
    types::{FeedbackCycleId, ResearchProfileId},
};

/// Closed REST subject taxonomy for `research.feedback` invalidations.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ResearchFeedbackSubjectKind {
    FeedbackCycle,
}

/// One durable revision hint. REST remains the authoritative snapshot.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ResearchFeedbackEvent {
    pub revision: i64,
    pub subject_kind: ResearchFeedbackSubjectKind,
    pub subject_id: FeedbackCycleId,
    pub profile_id: ResearchProfileId,
    pub occurred_at: DateTime<Utc>,
}

impl TryFrom<&FeedbackOutboxEntry> for ResearchFeedbackEvent {
    type Error = FeedbackError;

    fn try_from(entry: &FeedbackOutboxEntry) -> Result<Self, Self::Error> {
        entry.validate()?;
        Ok(Self {
            revision: entry.revision,
            subject_kind: ResearchFeedbackSubjectKind::FeedbackCycle,
            subject_id: entry.source.feedback_cycle_id(),
            profile_id: entry.profile_id.clone(),
            occurred_at: entry.source.occurred_at(),
        })
    }
}

#[cfg(test)]
mod tests {
    use chrono::{TimeZone, Utc};
    use serde_json::to_value;

    use super::{ResearchFeedbackEvent, ResearchFeedbackSubjectKind};
    use crate::types::{FeedbackCycleId, ResearchProfileId};

    #[test]
    fn payload_has_exact_fields() {
        let event = ResearchFeedbackEvent {
            revision: 42,
            subject_kind: ResearchFeedbackSubjectKind::FeedbackCycle,
            subject_id: FeedbackCycleId::from_v7(),
            profile_id: ResearchProfileId::new("crypto_price_15m"),
            occurred_at: Utc
                .with_ymd_and_hms(2026, 7, 29, 0, 0, 0)
                .single()
                .expect("valid timestamp"),
        };
        let value = to_value(&event).expect("serialize feedback event");
        let object = value.as_object().expect("feedback object");
        assert_eq!(object.len(), 5);
        assert_eq!(value["revision"], 42);
        assert_eq!(value["subject_kind"], "feedback_cycle");
        assert_eq!(value["profile_id"], "crypto_price_15m");
        assert!(value["subject_id"].is_string());
        assert_eq!(value["occurred_at"], "2026-07-29T00:00:00Z");
    }
}

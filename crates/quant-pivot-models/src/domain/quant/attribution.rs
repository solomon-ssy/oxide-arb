//! Immutable attribution artifact index contracts.

use chrono::{DateTime, Utc};
use sea_orm::{DeriveIntoActiveModel, DerivePartialModel};
use serde::{Deserialize, Serialize};
use thiserror::Error;

use crate::{
    entities::quant_attribution_artifact,
    enums::quant::{AttributionArtifactKind, AttributionCohort},
    types::{
        ArtifactUri, AttributionArtifactId, ContentHash, FeedbackCycleId, ModelVersionId,
        OrderIntentId, RecommendationId,
    },
};

/// Insert contract for one immutable, content-addressed attribution artifact.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, DeriveIntoActiveModel)]
#[sea_orm(active_model = "crate::entities::quant_attribution_artifact::ActiveModel")]
pub struct NewAttributionArtifact {
    pub attribution_artifact_id: AttributionArtifactId,
    pub artifact_kind: AttributionArtifactKind,
    pub source_cohort: AttributionCohort,
    pub source_feedback_cycle_id: FeedbackCycleId,
    pub model_version_id: Option<ModelVersionId>,
    pub recommendation_id: Option<RecommendationId>,
    pub order_intent_id: Option<OrderIntentId>,
    pub artifact_uri: ArtifactUri,
    pub artifact_hash: ContentHash,
    pub source_cutoff: DateTime<Utc>,
}

impl NewAttributionArtifact {
    /// Build a relational index whose identity is the exact artifact bytes hash.
    pub fn try_new(
        source_cohort: AttributionCohort,
        source_feedback_cycle_id: FeedbackCycleId,
        subject: AttributionSubject,
        artifact_uri: ArtifactUri,
        artifact_hash: ContentHash,
        source_cutoff: DateTime<Utc>,
    ) -> Result<Self, AttributionArtifactContractError> {
        let artifact_kind = subject.kind();
        let (model_version_id, recommendation_id, order_intent_id) = subject.into_parts();
        let artifact = Self {
            attribution_artifact_id: AttributionArtifactId::from_content_hash(&artifact_hash),
            artifact_kind,
            source_cohort,
            source_feedback_cycle_id,
            model_version_id,
            recommendation_id,
            order_intent_id,
            artifact_uri,
            artifact_hash,
            source_cutoff,
        };
        artifact.validate()?;
        Ok(artifact)
    }

    pub fn validate(&self) -> Result<(), AttributionArtifactContractError> {
        validate_identity(
            self.attribution_artifact_id,
            self.artifact_kind,
            self.model_version_id,
            self.recommendation_id,
            self.order_intent_id,
            self.artifact_hash,
            self.source_cutoff,
        )
    }
}

/// Closed subject vocabulary prevents semantically incompatible bindings.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AttributionSubject {
    Prediction {
        model_version_id: ModelVersionId,
        recommendation_id: RecommendationId,
    },
    Decision {
        model_version_id: ModelVersionId,
        recommendation_id: RecommendationId,
    },
    ResolutionOutcome {
        model_version_id: ModelVersionId,
    },
    ExecutionOutcome {
        model_version_id: ModelVersionId,
    },
    Execution {
        recommendation_id: RecommendationId,
        order_intent_id: OrderIntentId,
    },
    PolicyCounterfactual {
        recommendation_id: RecommendationId,
        order_intent_id: OrderIntentId,
    },
}

impl AttributionSubject {
    #[must_use]
    pub const fn kind(self) -> AttributionArtifactKind {
        match self {
            Self::Prediction { .. } => AttributionArtifactKind::PredictionExplanation,
            Self::Decision { .. } => AttributionArtifactKind::DecisionInterventionReplay,
            Self::ResolutionOutcome { .. } => AttributionArtifactKind::ResolutionOutcomeAssociation,
            Self::ExecutionOutcome { .. } => AttributionArtifactKind::ExecutionOutcomeAssociation,
            Self::Execution { .. } => AttributionArtifactKind::ExecutionTrajectory,
            Self::PolicyCounterfactual { .. } => {
                AttributionArtifactKind::PolicyCounterfactualOutcome
            }
        }
    }

    const fn into_parts(
        self,
    ) -> (
        Option<ModelVersionId>,
        Option<RecommendationId>,
        Option<OrderIntentId>,
    ) {
        match self {
            Self::Prediction {
                model_version_id,
                recommendation_id,
            }
            | Self::Decision {
                model_version_id,
                recommendation_id,
            } => (Some(model_version_id), Some(recommendation_id), None),
            Self::ResolutionOutcome { model_version_id }
            | Self::ExecutionOutcome { model_version_id } => (Some(model_version_id), None, None),
            Self::Execution {
                recommendation_id,
                order_intent_id,
            }
            | Self::PolicyCounterfactual {
                recommendation_id,
                order_intent_id,
            } => (None, Some(recommendation_id), Some(order_intent_id)),
        }
    }
}

/// Persisted immutable attribution artifact index.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, DerivePartialModel)]
#[sea_orm(entity = "crate::entities::quant_attribution_artifact::Entity")]
pub struct AttributionArtifactInfo {
    pub attribution_artifact_id: AttributionArtifactId,
    pub artifact_kind: AttributionArtifactKind,
    pub source_cohort: AttributionCohort,
    pub source_feedback_cycle_id: FeedbackCycleId,
    pub model_version_id: Option<ModelVersionId>,
    pub recommendation_id: Option<RecommendationId>,
    pub order_intent_id: Option<OrderIntentId>,
    pub artifact_uri: ArtifactUri,
    pub artifact_hash: ContentHash,
    pub source_cutoff: DateTime<Utc>,
    pub available_at: DateTime<Utc>,
    pub created_at: DateTime<Utc>,
}

info_from_model!(
    AttributionArtifactInfo,
    quant_attribution_artifact::Model,
    {
        attribution_artifact_id,
        artifact_kind,
        source_cohort,
        source_feedback_cycle_id,
        model_version_id,
        recommendation_id,
        order_intent_id,
        artifact_uri,
        artifact_hash,
        source_cutoff,
        available_at,
        created_at,
    }
);

impl AttributionArtifactInfo {
    pub fn validate(&self) -> Result<(), AttributionArtifactContractError> {
        validate_identity(
            self.attribution_artifact_id,
            self.artifact_kind,
            self.model_version_id,
            self.recommendation_id,
            self.order_intent_id,
            self.artifact_hash,
            self.source_cutoff,
        )?;
        if self.available_at < self.source_cutoff || self.created_at != self.available_at {
            return Err(AttributionArtifactContractError::InvalidTimeline);
        }
        Ok(())
    }

    #[must_use]
    pub fn matches(&self, candidate: &NewAttributionArtifact) -> bool {
        self.attribution_artifact_id == candidate.attribution_artifact_id
            && self.artifact_kind == candidate.artifact_kind
            && self.source_cohort == candidate.source_cohort
            && self.source_feedback_cycle_id == candidate.source_feedback_cycle_id
            && self.model_version_id == candidate.model_version_id
            && self.recommendation_id == candidate.recommendation_id
            && self.order_intent_id == candidate.order_intent_id
            && self.artifact_uri == candidate.artifact_uri
            && self.artifact_hash == candidate.artifact_hash
            && self.source_cutoff == candidate.source_cutoff
    }
}

fn validate_identity(
    artifact_id: AttributionArtifactId,
    kind: AttributionArtifactKind,
    model_version_id: Option<ModelVersionId>,
    recommendation_id: Option<RecommendationId>,
    order_intent_id: Option<OrderIntentId>,
    artifact_hash: ContentHash,
    source_cutoff: DateTime<Utc>,
) -> Result<(), AttributionArtifactContractError> {
    if artifact_id != AttributionArtifactId::from_content_hash(&artifact_hash) {
        return Err(AttributionArtifactContractError::IdentityMismatch);
    }
    if source_cutoff.timestamp_millis() <= 0 {
        return Err(AttributionArtifactContractError::InvalidTimeline);
    }
    let bindings_valid = match kind {
        AttributionArtifactKind::PredictionExplanation
        | AttributionArtifactKind::DecisionInterventionReplay => {
            model_version_id.is_some() && recommendation_id.is_some() && order_intent_id.is_none()
        }
        AttributionArtifactKind::ResolutionOutcomeAssociation
        | AttributionArtifactKind::ExecutionOutcomeAssociation => {
            model_version_id.is_some() && recommendation_id.is_none() && order_intent_id.is_none()
        }
        AttributionArtifactKind::ExecutionTrajectory
        | AttributionArtifactKind::PolicyCounterfactualOutcome => {
            model_version_id.is_none() && recommendation_id.is_some() && order_intent_id.is_some()
        }
    };
    if !bindings_valid {
        return Err(AttributionArtifactContractError::InvalidSubject);
    }
    Ok(())
}

#[derive(Debug, Error, Clone, PartialEq, Eq)]
pub enum AttributionArtifactContractError {
    #[error("attribution artifact identity differs from its content hash")]
    IdentityMismatch,
    #[error("attribution artifact kind and subject bindings are incompatible")]
    InvalidSubject,
    #[error("attribution artifact timeline is invalid")]
    InvalidTimeline,
}

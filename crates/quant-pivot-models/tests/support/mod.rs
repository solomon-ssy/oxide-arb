//! Fixtures local to report wire-contract snapshots.

pub mod report_fixtures;
pub mod report_snapshots;

use quant_pivot_models::types::{
    ResearchEvaluationTrack, ResearchProfileRef, builtin_research_profiles,
};
use uuid::Uuid;

#[must_use]
fn fixture_profile_ref() -> ResearchProfileRef {
    builtin_research_profiles()
        .expect("research profiles")
        .into_iter()
        .find(|profile| {
            profile.spec.activation_eligibility == ResearchEvaluationTrack::SemiAutoCandidate
        })
        .expect("weather profile")
        .profile_ref
}

#[must_use]
fn seeded_uuid(name: &str) -> Uuid {
    Uuid::new_v5(&Uuid::NAMESPACE_OID, name.as_bytes())
}

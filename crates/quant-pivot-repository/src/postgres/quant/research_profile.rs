//! Typed immutable research-profile persistence helpers.

use quant_pivot_error::storage::StorageError;
use quant_pivot_models::{
    entities::research_profile_artifact,
    hashing::CanonicalDigest,
    types::{
        ResearchProfileArtifact, ResearchProfileArtifactId, ResearchProfileRef,
        builtin_research_profiles, resolve_builtin_research_profile,
    },
};
use sea_orm::{ActiveValue::Set, ConnectionTrait, EntityTrait, TryInsertResult};

fn validate_row(
    row: &research_profile_artifact::Model,
) -> Result<ResearchProfileRef, StorageError> {
    let version = u32::try_from(row.version).map_err(|error| {
        StorageError::invariant_violation(
            Some("research_profile_artifact"),
            format!("profile version is not a positive u32: {error}"),
        )
    })?;
    let profile_ref = ResearchProfileRef {
        id: row.research_profile_id.clone(),
        version,
        content_hash: row.content_hash.clone(),
    };
    let computed_hash = CanonicalDigest::content_hash_json(&row.spec).map_err(|error| {
        StorageError::invariant_violation(
            Some("research_profile_artifact"),
            format!("failed to hash typed profile spec: {error}"),
        )
    })?;
    if computed_hash != row.content_hash
        || profile_ref.artifact_id() != row.research_profile_artifact_id
    {
        return Err(StorageError::invariant_violation(
            Some("research_profile_artifact"),
            format!(
                "research profile artifact {} has inconsistent id/hash/spec",
                row.research_profile_artifact_id
            ),
        ));
    }
    Ok(profile_ref)
}

pub async fn ensure(
    db: &impl ConnectionTrait,
    profile_ref: &ResearchProfileRef,
) -> Result<ResearchProfileArtifactId, StorageError> {
    let profile = resolve_builtin_research_profile(profile_ref).map_err(|detail| {
        StorageError::invariant_violation(Some("research_profile_artifact"), detail)
    })?;
    insert_or_verify(db, &profile).await
}

pub async fn ensure_builtins(
    db: &impl ConnectionTrait,
) -> Result<Vec<ResearchProfileArtifactId>, StorageError> {
    let profiles = builtin_research_profiles().map_err(|detail| {
        StorageError::invariant_violation(Some("research_profile_artifact"), detail)
    })?;
    let mut ids = Vec::with_capacity(profiles.len());
    for profile in &profiles {
        ids.push(insert_or_verify(db, profile).await?);
    }
    Ok(ids)
}

async fn insert_or_verify(
    db: &impl ConnectionTrait,
    profile: &ResearchProfileArtifact,
) -> Result<ResearchProfileArtifactId, StorageError> {
    let artifact_id = profile.profile_ref.artifact_id();
    let version = i32::try_from(profile.profile_ref.version).map_err(|error| {
        StorageError::invariant_violation(
            Some("research_profile_artifact"),
            format!("profile version exceeds i32: {error}"),
        )
    })?;
    let outcome =
        research_profile_artifact::Entity::insert(research_profile_artifact::ActiveModel {
            research_profile_artifact_id: Set(artifact_id.clone()),
            research_profile_id: Set(profile.profile_ref.id.clone()),
            version: Set(version),
            content_hash: Set(profile.profile_ref.content_hash.clone()),
            spec: Set(profile.spec.clone()),
            published_by: Set(profile.published_by.clone()),
            published_at: Set(profile.published_at),
            governance_reason: Set(profile.governance_reason.clone()),
            created_at: sea_orm::ActiveValue::NotSet,
        })
        .on_conflict_do_nothing_on([research_profile_artifact::Column::ResearchProfileArtifactId])
        .exec_without_returning(db)
        .await
        .map_err(StorageError::from)?;
    if !matches!(
        outcome,
        TryInsertResult::Inserted(1 | 0) | TryInsertResult::Conflicted
    ) {
        return Err(StorageError::invariant_violation(
            Some("research_profile_artifact"),
            "single research profile insert returned an invalid row count",
        ));
    }
    let row = research_profile_artifact::Entity::find_by_id(artifact_id.clone())
        .one(db)
        .await
        .map_err(StorageError::from)?
        .ok_or_else(|| {
            StorageError::invariant_violation(
                Some("research_profile_artifact"),
                "research profile disappeared after insert",
            )
        })?;
    let stored_ref = validate_row(&row)?;
    if stored_ref != profile.profile_ref
        || row.spec != profile.spec
        || row.published_by != profile.published_by
        || row.published_at != profile.published_at
        || row.governance_reason != profile.governance_reason
    {
        return Err(StorageError::state_conflict(
            "research_profile_artifact",
            Some(&artifact_id),
            "immutable profile address exists with different content or governance metadata",
        ));
    }
    Ok(artifact_id)
}

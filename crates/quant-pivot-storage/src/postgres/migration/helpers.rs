//! Catalog-driven Postgres migration helpers.

use chrono::Utc;
use quant_pivot_error::seed::SeedError;
use quant_pivot_models::{
    entities::seed_application,
    seed::{
        SeedContext, SeedDependency, SeedSpec, rbac::BOOTSTRAP_ADMIN_PASSWORD_HASH_INPUT,
        spec::all_specs,
    },
};
use sea_orm::{
    ActiveValue::Set, ColumnTrait, ConnectionTrait, DatabaseConnection, DbErr, EntityTrait,
    QueryFilter, TransactionTrait,
};
use std::collections::{BTreeMap, BTreeSet};
use tracing::info;

/// Apply versioned catalog seeds after the immutable SQL schema is verified.
pub async fn run_catalog_seeds(
    db: &DatabaseConnection,
    bootstrap_admin_password_hash: &str,
) -> Result<(), sea_orm::DbErr> {
    let mut ctx = SeedContext::new();
    ctx.put(
        BOOTSTRAP_ADMIN_PASSWORD_HASH_INPUT,
        bootstrap_admin_password_hash.to_owned(),
    );
    for seed in ordered_seeds()? {
        let transaction = db.begin().await?;
        if seed_already_applied(&transaction, &seed).await? {
            (seed.hydrate)(&transaction, &mut ctx).await?;
            transaction.commit().await?;
            info!(
                seed_id = seed.id,
                seed_version = seed.version,
                "catalog seed already applied"
            );
            continue;
        }
        let rows = (seed.apply)(&transaction, &mut ctx).await?;
        (seed.hydrate)(&transaction, &mut ctx).await?;
        record_seed_application(&transaction, &seed, rows).await?;
        transaction.commit().await?;
        info!(
            seed_id = seed.id,
            seed_version = seed.version,
            rows,
            "catalog seed applied"
        );
    }
    Ok(())
}

/// Verify every compiled catalog seed has an applied row with the same checksum.
pub async fn verify_catalog_seeds(db: &DatabaseConnection) -> Result<(), DbErr> {
    let mut ctx = SeedContext::new();
    for seed in ordered_seeds()? {
        let transaction = db.begin().await?;
        if !seed_already_applied(&transaction, &seed).await? {
            return Err(DbErr::Custom(format!(
                "catalog seed `{}` v{} is pending",
                seed.id, seed.version
            )));
        }
        (seed.hydrate)(&transaction, &mut ctx).await?;
        transaction.commit().await?;
    }
    Ok(())
}

fn ordered_seeds() -> Result<Vec<SeedSpec>, DbErr> {
    let seeds = all_specs();
    let by_key = seeds
        .iter()
        .map(|seed| (format!("{}#{}", seed.id, seed.version), *seed))
        .collect::<BTreeMap<_, _>>();
    let mut artifact_producers = BTreeMap::new();

    for seed in &seeds {
        for artifact in seed.produces {
            let previous =
                artifact_producers.insert(artifact.key.0, format!("{}#{}", seed.id, seed.version));
            if previous.is_some() {
                return Err(DbErr::Custom(
                    SeedError::DuplicateArtifactProducer {
                        key: artifact.key.0,
                    }
                    .to_string(),
                ));
            }
        }
    }

    let mut incoming = by_key
        .keys()
        .map(|key| (key.clone(), BTreeSet::<String>::new()))
        .collect::<BTreeMap<_, _>>();
    let mut outgoing = by_key
        .keys()
        .map(|key| (key.clone(), BTreeSet::<String>::new()))
        .collect::<BTreeMap<_, _>>();

    for seed in &seeds {
        let key = format!("{}#{}", seed.id, seed.version);
        for dep in seed.depends_on {
            let parent = match dep {
                SeedDependency::Seed { id, version } => format!("{id}#{version}"),
                SeedDependency::Artifact(artifact) => {
                    artifact_producers.get(artifact.0).cloned().ok_or_else(|| {
                        DbErr::Custom(
                            SeedError::MissingDependency {
                                dependency: artifact.0,
                            }
                            .to_string(),
                        )
                    })?
                }
            };
            if !by_key.contains_key(&parent) {
                return Err(DbErr::Custom(
                    SeedError::MissingDependency {
                        dependency: seed.id,
                    }
                    .to_string(),
                ));
            }
            incoming
                .entry(key.clone())
                .or_default()
                .insert(parent.clone());
            outgoing.entry(parent).or_default().insert(key.clone());
        }
    }

    let mut ready = incoming
        .iter()
        .filter_map(|(key, deps)| deps.is_empty().then_some(key.clone()))
        .collect::<BTreeSet<_>>();
    let mut ordered = Vec::with_capacity(seeds.len());

    while let Some(key) = ready.pop_first() {
        ordered.push(key.clone());
        let children = outgoing.remove(&key).unwrap_or_default();
        for child in children {
            let deps = incoming.get_mut(&child).ok_or_else(|| {
                DbErr::Custom(format!("seed graph references unknown child `{child}`"))
            })?;
            deps.remove(&key);
            if deps.is_empty() {
                ready.insert(child);
            }
        }
    }

    if ordered.len() != seeds.len() {
        return Err(DbErr::Custom(SeedError::Cycle.to_string()));
    }

    Ok(ordered.into_iter().map(|key| by_key[&key]).collect())
}

#[inline]
async fn seed_already_applied<C>(db: &C, seed: &SeedSpec) -> Result<bool, DbErr>
where
    C: ConnectionTrait,
{
    let Some(row) = seed_application::Entity::find()
        .filter(seed_application::Column::SeedId.eq(seed.id))
        .filter(seed_application::Column::SeedVersion.eq(seed_version_i32(seed)?))
        .one(db)
        .await?
    else {
        return Ok(false);
    };
    if row.checksum == seed.checksum {
        Ok(true)
    } else {
        Err(DbErr::Custom(format!(
            "seed `{}` v{} checksum mismatch: ledger has `{}`, code has `{}`",
            seed.id, seed.version, row.checksum, seed.checksum
        )))
    }
}

#[inline]
async fn record_seed_application(
    db: &impl ConnectionTrait,
    seed: &SeedSpec,
    rows: u64,
) -> Result<(), DbErr> {
    let rows = <i64 as TryFrom<u64>>::try_from(rows)
        .map_err(|_| DbErr::Custom(format!("seed `{}` affected too many rows", seed.id)))?;
    seed_application::Entity::insert(seed_application::ActiveModel {
        seed_id: Set(seed.id.to_owned()),
        seed_version: Set(seed_version_i32(seed)?),
        checksum: Set(seed.checksum.to_owned()),
        applied_at: Set(Utc::now()),
        rows_affected: Set(rows),
    })
    .exec_without_returning(db)
    .await?;
    Ok(())
}

#[inline]
fn seed_version_i32(seed: &SeedSpec) -> Result<i32, DbErr> {
    <i32 as TryFrom<u32>>::try_from(seed.version)
        .map_err(|_| DbErr::Custom(format!("seed `{}` version exceeds i32", seed.id)))
}

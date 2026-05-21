//! `SeedRunner` — executes a `SeedPlan` with `ON CONFLICT`-based idempotency.
//!
//! Idempotency is handled at the SQL level by each seed's `ON CONFLICT` clause,
//! not by a separate tracking table. `SeaORM`'s migration table already records
//! whether the bootstrap migration has been applied.

use oxide_arb_models::seed::{SeedContext, SeedPlan};
use sea_orm::ConnectionTrait;
use tracing::info;

/// Execute all seeds in the plan sequentially, sharing a `SeedContext`.
///
/// Each seed runs within the migration's transaction. `ON CONFLICT` clauses
/// in each seed's SQL ensure data-level idempotency.
pub async fn run_plan(db: &dyn ConnectionTrait, plan: &SeedPlan) -> Result<(), sea_orm::DbErr> {
    let mut ctx = SeedContext::new();

    for seed in plan.seeds() {
        let seed_id = seed.id();
        let rows = seed.execute(db, &mut ctx).await?;
        info!(seed_id, rows, "seed applied");
    }

    Ok(())
}

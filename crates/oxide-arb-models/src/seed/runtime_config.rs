//! Runtime configuration defaults seed — inserts one row per `RuntimeConfigKey`.

use crate::{
    entities::runtime_config,
    enums::runtime_config::RuntimeConfigKey,
    idens::runtime_config::RuntimeConfig,
    schema::seed::{SeedArtifact, SeedDependency, SeedSpec},
    seed::{SeedConflictPolicy, SeedContext},
};
use sea_orm::{
    DeriveIntoActiveModel, EntityTrait, IntoActiveModel, Iterable, QueryTrait,
    sea_query::OnConflict,
};

const DEPENDS_ON: &[SeedDependency] = &[];
const PRODUCES: &[SeedArtifact] = &[];

pub const RUNTIME_CONFIG_SEED: SeedSpec = SeedSpec {
    id: "trading.runtime_config.defaults",
    version: 1,
    target_table: runtime_config_table_name,
    depends_on: DEPENDS_ON,
    produces: PRODUCES,
    conflict_policy: SeedConflictPolicy::InsertKeyIfAbsent,
    checksum: "runtime_config.defaults.v1",
    loader: load_boxed,
};

/// DTO for inserting a single runtime configuration row.
///
/// `updated_at` is omitted so the DB column default (`statement_timestamp()`) applies.
#[derive(Debug, Clone, DeriveIntoActiveModel)]
#[sea_orm(active_model = "crate::entities::runtime_config::ActiveModel")]
pub struct NewRuntimeConfig {
    pub key: RuntimeConfigKey,
    pub value: serde_json::Value,
    pub description: Option<String>,
    pub updated_by: String,
}

/// Insert a default row for every `RuntimeConfigKey` variant.
///
/// Uses `ON CONFLICT (key) DO NOTHING` so operator-modified values
/// survive re-migration.
pub async fn load(
    db: &dyn sea_orm::ConnectionTrait,
    _ctx: &mut SeedContext,
) -> Result<u64, sea_orm::DbErr> {
    let models: Vec<runtime_config::ActiveModel> = RuntimeConfigKey::iter()
        .map(|key| {
            let (value, description) = default_for(key);
            NewRuntimeConfig {
                key,
                value,
                description: Some(description.to_owned()),
                updated_by: "system:bootstrap".to_owned(),
            }
            .into_active_model()
        })
        .collect();

    if models.is_empty() {
        return Ok(0);
    }

    let backend = db.get_database_backend();
    let stmt = runtime_config::Entity::insert_many(models)
        .on_conflict(
            OnConflict::column(runtime_config::Column::Key)
                .do_nothing()
                .to_owned(),
        )
        .build(backend);
    let result = db.execute(stmt).await?;
    Ok(result.rows_affected())
}

/// Conservative defaults aligned with the application's TOML configuration.
fn default_for(key: RuntimeConfigKey) -> (serde_json::Value, &'static str) {
    match key {
        RuntimeConfigKey::MaxPortfolioExposureUsd => (
            serde_json::json!(1000.0),
            "Maximum total portfolio exposure in USD",
        ),
        RuntimeConfigKey::MaxSinglePositionUsd => (
            serde_json::json!(200.0),
            "Maximum single position size in USD",
        ),
        RuntimeConfigKey::MaxDailyLossUsd => (
            serde_json::json!(500.0),
            "Maximum daily loss before circuit breaker triggers (USD)",
        ),
        RuntimeConfigKey::CircuitBreakerThreshold => (
            serde_json::json!(3),
            "Consecutive losses to trigger circuit breaker",
        ),
        RuntimeConfigKey::MinProfitThresholdUsd => (
            serde_json::json!(0.50),
            "Minimum expected profit for opportunity detection (USD)",
        ),
        RuntimeConfigKey::EndgameHoursBeforeClose => (
            serde_json::json!(24),
            "Hours before market close to enter endgame mode",
        ),
        RuntimeConfigKey::ConvergenceThreshold => (
            serde_json::json!(0.02),
            "Price convergence threshold for exit signals",
        ),
        RuntimeConfigKey::MaxSlippageBps => (
            serde_json::json!(50),
            "Maximum acceptable slippage in basis points",
        ),
        RuntimeConfigKey::OrderTimeoutSecs => (serde_json::json!(30), "Order timeout in seconds"),
        RuntimeConfigKey::CooldownAfterTradeSecs => (
            serde_json::json!(60),
            "Cooldown period after trade execution (seconds)",
        ),
        RuntimeConfigKey::KellyFraction => (
            serde_json::json!(0.25),
            "Fraction of Kelly criterion for position sizing",
        ),
        RuntimeConfigKey::MaxPositionFractionOfBook => (
            serde_json::json!(0.05),
            "Maximum position as fraction of order book depth",
        ),
        RuntimeConfigKey::MaintenanceMode => (
            serde_json::json!(false),
            "Enable maintenance mode (halts all trading)",
        ),
        RuntimeConfigKey::DryRunMode => (
            serde_json::json!(true),
            "Enable dry-run mode (simulate without execution)",
        ),
    }
}

fn load_boxed<'a>(
    db: &'a dyn sea_orm::ConnectionTrait,
    ctx: &'a mut SeedContext,
) -> std::pin::Pin<Box<dyn std::future::Future<Output = Result<u64, sea_orm::DbErr>> + Send + 'a>> {
    Box::pin(load(db, ctx))
}

fn runtime_config_table_name() -> String {
    use sea_orm::Iden;
    RuntimeConfig::Table.to_string()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn all_keys_have_defaults() {
        for key in RuntimeConfigKey::iter() {
            let (value, desc) = default_for(key);
            assert!(!desc.is_empty(), "key {key} has empty description");
            assert_ne!(value, serde_json::Value::Null, "key {key} has null default");
        }
    }
}

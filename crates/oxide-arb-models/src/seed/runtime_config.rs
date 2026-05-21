//! Runtime configuration defaults seed — inserts one row per `RuntimeConfigKey`.

use crate::entities::runtime_config::{self, RuntimeConfigKey};
use crate::seed::SeedContext;
use oxide_arb_macros::SeedUnit;
use sea_orm::{
    DeriveIntoActiveModel, EntityTrait, IntoActiveModel, QueryTrait, sea_query::OnConflict,
};
use strum::IntoEnumIterator;

#[derive(SeedUnit)]
#[seed_unit(
    id = "trading.runtime_config",
    order = 20,
    policy = InsertKeyIfAbsent,
    loader = crate::seed::runtime_config::load,
)]
pub struct RuntimeConfigSeed;

/// DTO for inserting a single runtime configuration row.
///
/// `updated_at` is omitted — the DB column default (`CURRENT_TIMESTAMP`) applies.
#[derive(Debug, Clone, DeriveIntoActiveModel)]
#[sea_orm(active_model = "crate::entities::runtime_config::ActiveModel")]
pub struct NewRuntimeConfig {
    pub key: String,
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
                key: key.as_str().to_owned(),
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

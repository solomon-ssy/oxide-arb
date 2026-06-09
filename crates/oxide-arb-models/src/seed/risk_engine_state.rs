//! Risk engine singleton seed — inserts the canonical id=1 row.

use crate::{
    entities::risk_state,
    enums::risk::BreakerStateName,
    idens::risk_state::RiskEngineState,
    schema::seed::{SeedArtifact, SeedDependency, SeedSpec},
    seed::{SeedConflictPolicy, SeedContext},
    types::Usd,
};
use chrono::{DateTime, NaiveDate, Utc};
use sea_orm::{
    ConnectionTrait, DbErr, DeriveIntoActiveModel, EntityTrait, IntoActiveModel, QueryTrait,
    sea_query::OnConflict,
};
use std::pin::Pin;

const DEPENDS_ON: &[SeedDependency] = &[];
const PRODUCES: &[SeedArtifact] = &[];

pub const RISK_ENGINE_STATE_SEED: SeedSpec = SeedSpec {
    id: "trading.risk_engine_state.bootstrap",
    version: 1,
    target_table: risk_engine_state_table_name,
    depends_on: DEPENDS_ON,
    produces: PRODUCES,
    conflict_policy: SeedConflictPolicy::InsertIfAbsent,
    checksum: "risk_engine_state.bootstrap.v1",
    loader: load_boxed,
};

/// All fields required to bootstrap the risk engine singleton.
#[derive(Debug, Clone, DeriveIntoActiveModel)]
#[sea_orm(active_model = "crate::entities::risk_state::ActiveModel")]
pub struct NewRiskEngineState {
    pub id: i32,
    pub breaker_state: BreakerStateName,
    pub is_halted: bool,
    pub consecutive_misses: i32,
    pub cooldown_multiplier: i32,
    pub total_exposure: Usd,
    pub hourly_loss_usd: Usd,
    pub hourly_fee_usd: Usd,
    pub hourly_trade_count: i32,
    pub hourly_success_count: i32,
    pub hourly_miss_count: i32,
    pub hourly_window_start: DateTime<Utc>,
    pub daily_loss_usd: Usd,
    pub daily_fee_usd: Usd,
    pub daily_pnl: Usd,
    pub daily_budget_spent: Usd,
    pub daily_trade_count: i32,
    pub daily_success_count: i32,
    pub daily_miss_count: i32,
    pub daily_window_start: NaiveDate,
    pub weekly_loss_usd: Usd,
    pub weekly_trade_count: i32,
    pub weekly_window_start: NaiveDate,
    pub hwm_equity: Usd,
    pub total_realized_pnl: Usd,
}

impl Default for NewRiskEngineState {
    fn default() -> Self {
        let now = Utc::now();
        Self {
            id: 1,
            breaker_state: BreakerStateName::Closed,
            is_halted: false,
            consecutive_misses: 0,
            cooldown_multiplier: 1,
            total_exposure: Usd::ZERO,
            hourly_loss_usd: Usd::ZERO,
            hourly_fee_usd: Usd::ZERO,
            hourly_trade_count: 0,
            hourly_success_count: 0,
            hourly_miss_count: 0,
            hourly_window_start: now,
            daily_loss_usd: Usd::ZERO,
            daily_fee_usd: Usd::ZERO,
            daily_pnl: Usd::ZERO,
            daily_budget_spent: Usd::ZERO,
            daily_trade_count: 0,
            daily_success_count: 0,
            daily_miss_count: 0,
            daily_window_start: now.date_naive(),
            weekly_loss_usd: Usd::ZERO,
            weekly_trade_count: 0,
            weekly_window_start: now.date_naive(),
            hwm_equity: Usd::ZERO,
            total_realized_pnl: Usd::ZERO,
        }
    }
}

/// Insert the singleton risk engine row.
///
/// Uses `ON CONFLICT DO NOTHING` to protect production state from being
/// overwritten during re-migration.
pub async fn load(
    db: &dyn sea_orm::ConnectionTrait,
    _ctx: &mut SeedContext,
) -> Result<u64, sea_orm::DbErr> {
    let am = NewRiskEngineState::default().into_active_model();
    let backend = db.get_database_backend();
    let stmt = risk_state::Entity::insert(am)
        .on_conflict(
            OnConflict::column(risk_state::Column::Id)
                .do_nothing()
                .to_owned(),
        )
        .build(backend);
    let result = db.execute(stmt).await?;
    Ok(result.rows_affected())
}

fn load_boxed<'a>(
    db: &'a dyn ConnectionTrait,
    ctx: &'a mut SeedContext,
) -> Pin<Box<dyn Future<Output = Result<u64, DbErr>> + Send + 'a>> {
    Box::pin(load(db, ctx))
}

fn risk_engine_state_table_name() -> String {
    use sea_orm::Iden;
    RiskEngineState::Table.to_string()
}

//! `PostgreSQL` repository implementations.

pub mod accounting;
pub mod blacklist_persistence;
pub mod calibration;
pub mod emergency;
pub mod event;
pub mod market;
mod orm;
pub mod position;
pub mod potential_loss;
pub mod reconciliation;
pub mod report;
pub mod resolution_event;
pub mod risk_audit;
pub mod risk_fill;
pub mod risk_state;
pub mod runtime_config;
pub mod trade;

pub use accounting::{PgAccountingRepository, PgAccountingRepositoryTxn};
pub use blacklist_persistence::{
    PgBlacklistPersistenceRepository, PgBlacklistPersistenceRepositoryTxn,
};
pub use calibration::{PgCalibrationRepository, PgCalibrationRepositoryTxn};
pub use emergency::PgEmergencyRepository;
pub use event::{PgEventRepository, PgEventRepositoryTxn};
pub use market::{PgMarketRepository, PgMarketRepositoryTxn};
pub use position::{PgPositionRepository, PgPositionRepositoryTxn};
pub use potential_loss::{PgPotentialLossRepository, PgPotentialLossRepositoryTxn};
pub use reconciliation::PgReconciliationRepository;
pub use report::PgReportRepository;
pub use resolution_event::{PgResolutionEventRepository, PgResolutionEventRepositoryTxn};
pub use risk_audit::PgRiskAuditRepository;
pub use risk_state::{PgRiskStateRepository, PgRiskStateRepositoryTxn};
pub use runtime_config::{PgRuntimeConfigRepository, PgRuntimeConfigRepositoryTxn};
pub use trade::{PgTradeRepository, PgTradeRepositoryTxn};

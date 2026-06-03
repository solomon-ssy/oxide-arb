//! Repository trait definitions.

pub mod accounting;
pub mod blacklist_persistence;
pub mod calibration;
pub mod control_factor;
pub mod emergency;
pub mod event;
pub mod fact_data;
pub mod market;
pub mod position;
pub mod potential_loss;
pub mod reconciliation;
pub mod report;
pub mod resolution_event;
pub mod risk_audit;
pub mod risk_state;
pub mod runtime_config;
pub mod timeseries;
pub mod trade;

pub use timeseries::{
    EvidenceTimeseriesRepository, MarketFilter, TimeWindow, TimeseriesFactWriter,
};

pub use accounting::AccountingRepository;
pub use blacklist_persistence::BlacklistPersistenceRepository;
pub use calibration::CalibrationRepository;
pub use control_factor::ControlFactorRepository;
pub use emergency::EmergencyRepository;
pub use event::EventRepository;
pub use fact_data::{
    BalanceSnapshotRepository, ControlFactorDatasetRepository,
    ControlFactorShadowDecisionRepository, PositionExitRepository,
};
pub use market::MarketRepository;
pub use position::PositionRepository;
pub use potential_loss::PotentialLossRepository;
pub use reconciliation::ReconciliationRepository;
pub use report::ReportRepository;
pub use resolution_event::ResolutionEventRepository;
pub use risk_audit::RiskAuditRepository;
pub use risk_state::RiskStateRepository;
pub use runtime_config::RuntimeConfigVersionRepository;
pub use trade::TradeRepository;

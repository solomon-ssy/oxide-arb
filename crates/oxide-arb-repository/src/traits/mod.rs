//! Repository trait definitions.

pub mod accounting;
pub mod blacklist_persistence;
pub mod calibration;
pub mod emergency;
pub mod event;
pub mod market;
pub mod outbox;
pub mod position;
pub mod potential_loss;
pub mod reconciliation;
pub mod report;
pub mod risk_audit;
pub mod risk_state;
pub mod runtime_config;
pub mod timeseries;
pub mod trade;

pub use accounting::AccountingRepository;
pub use blacklist_persistence::BlacklistPersistenceRepository;
pub use calibration::CalibrationRepository;
pub use emergency::EmergencyRepository;
pub use event::EventRepository;
pub use market::MarketRepository;
pub use outbox::OutboxRepository;
pub use position::PositionRepository;
pub use potential_loss::PotentialLossRepository;
pub use reconciliation::ReconciliationRepository;
pub use report::ReportRepository;
pub use risk_audit::RiskAuditRepository;
pub use risk_state::RiskStateRepository;
pub use runtime_config::RuntimeConfigRepository;
pub use timeseries::TimeseriesRepository;
pub use trade::TradeRepository;

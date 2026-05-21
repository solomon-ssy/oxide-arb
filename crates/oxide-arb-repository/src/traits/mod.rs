//! Repository trait definitions.

pub mod accounting;
pub mod calibration;
pub mod event;
pub mod lifecycle;
pub mod market;
pub mod position;
pub mod potential_loss;
pub mod risk_state;
pub mod runtime_config;
pub mod timeseries;
pub mod trade;

pub use accounting::AccountingRepository;
pub use calibration::CalibrationRepository;
pub use event::EventRepository;
pub use lifecycle::LifecycleRepository;
pub use market::MarketRepository;
pub use position::PositionRepository;
pub use potential_loss::PotentialLossRepository;
pub use risk_state::RiskStateRepository;
pub use runtime_config::RuntimeConfigRepository;
pub use timeseries::TimeseriesRepository;
pub use trade::TradeRepository;

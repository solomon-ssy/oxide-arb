//! Trade-tape concentration estimators shared by the feature plane and monitor.

pub mod participant_concentration;

pub use participant_concentration::{
    ConcentrationCompositeWeights, ConcentrationMissing, ParticipantConcentrationGate,
    ParticipantConcentrationSnapshot, ParticipantRoleMetrics, composite_concentration,
    compute_concentration, compute_role_gini, cr1_share, eligible_maker_prints, gini, hhi,
};

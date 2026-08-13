//! Global portfolio plane over venue-executable discounted USD scenario cash flows.

pub mod account;
mod capital_bucket_contract;
mod economic;
mod global;
mod scenario;
mod scenario_model;
mod solver_boundary;

pub use account::{AccountDrawdown, AccountSnapshot};
pub use capital_bucket_contract::{CapitalTimeBucketContract, CapitalTimeBucketContractError};
pub use economic::{
    EconomicTierFactory, ExecutableCashTierSeedFactory, ExecutableCashTierSeedInput,
    ExecutableTierLadderSeedFactory, ExecutableTierLadderSeedInput, ExecutableTierSeed,
    ExistingPortfolioFactory,
};
pub use global::{
    GlobalPortfolioInput, GlobalPortfolioPlanner, GlobalPortfolioResult, PlannedEconomicTier,
    TierAdmissionRejection, TierAdmissionRejectionCode,
};
pub use scenario::{
    PortfolioScenarioGenerationInput, PortfolioScenarioGenerator, PortfolioScenarioLegInput,
    SealedPortfolioScenarioArtifact, VerifiedPortfolioScenarioModel,
};
pub(crate) use scenario_model::scenario_economic_function_hash;
pub use scenario_model::{
    FittedPortfolioScenarioModel, PortfolioScenarioFoldFitInput, PortfolioScenarioMethodology,
    PortfolioScenarioModelFitInput, PortfolioScenarioModelFitter,
    PortfolioScenarioResidualObservation, PortfolioScenarioRouteFitInput,
};

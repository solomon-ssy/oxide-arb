//! Schema root for the research model authoring and CPCV API surface.

use schemars::JsonSchema;

use super::{
    CreateModelSpecRequest, FeatureContractView, QuantModelSpecView, RunCpcvBacktestRequest,
};

/// Schema-only envelope used to generate frontend types and runtime decoders.
///
/// HTTP handlers never serialize this envelope. Its only purpose is to keep
/// model authoring, frozen model-spec reads, and CPCV submission reachable from
/// one Rust-owned schema root so the SPA cannot maintain parallel wire types.
#[derive(JsonSchema)]
#[schemars(deny_unknown_fields)]
pub struct ResearchModelApiContractSchema {
    pub create_model_spec_request: CreateModelSpecRequest,
    pub feature_contract_response: FeatureContractView,
    pub model_spec_response: QuantModelSpecView,
    pub run_cpcv_backtest_request: RunCpcvBacktestRequest,
}

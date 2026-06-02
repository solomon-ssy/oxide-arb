//! Governance gates for typed control-factor artifacts.

use oxide_arb_error::control::FactorValueError;
use oxide_arb_models::domain::control_factor::{ControlFactorValue, FactorPayload};
use oxide_arb_models::enums::control_factor::FactorStatus;

pub struct ControlFactorGate;

impl ControlFactorGate {
    pub fn validate_transition(
        factor: &ControlFactorValue,
        target: FactorStatus,
        previous_payload: Option<&FactorPayload>,
    ) -> Result<(), FactorValueError> {
        factor.validate_for_transition(target, previous_payload)
    }
}

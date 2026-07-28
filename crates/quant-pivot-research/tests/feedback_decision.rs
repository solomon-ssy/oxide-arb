use std::mem::size_of;

use quant_pivot_models::{
    domain::ports::{FeedbackDecisionExecutionPort, FeedbackDecisionJobParams},
    types::FeedbackDecisionArtifactId,
};
use quant_pivot_research::feedback_decision::{
    FeedbackDecisionArtifact, FeedbackDecisionCodec, FeedbackDecisionEvaluator,
    FeedbackDecisionOutcome,
};

type DecisionExecutionPort = dyn FeedbackDecisionExecutionPort;

#[test]
fn f11_surface_is_linked() {
    assert!(size_of::<FeedbackDecisionArtifactId>() > 0);
    assert!(size_of::<FeedbackDecisionJobParams>() > 0);
    assert!(size_of::<FeedbackDecisionOutcome>() > 0);
    assert!(size_of::<FeedbackDecisionArtifact>() > 0);
    let _ = FeedbackDecisionEvaluator;
    let _ = FeedbackDecisionCodec;
    assert!(size_of::<&DecisionExecutionPort>() > 0);
}

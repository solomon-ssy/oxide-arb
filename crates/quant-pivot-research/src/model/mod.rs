//! Model plane: unified runtime contract, strongly-typed signal candidate,
//! serialized artifacts, and the training contract.
//!
//! Online closure terminus (3.4): `FactorValue → QuantModelRuntime →
//! SignalCandidate`. The runtime/factory traits and the artifact shell are
//! fixed here; 3.4 implements the weighted-factor runtime and 3.6 the
//! classical trainer/runtime.

pub mod artifact;
pub mod runtime;
pub mod signal;
pub mod trainer;

pub use artifact::{
    ClassicalModelArtifact, FactorWeight, ModelArtifact, ModelArtifactHeader,
    WeightedFactorModelArtifact,
};
pub use runtime::{
    ClassicalKind, FactorInferenceRow, FactorInferenceTable, InferenceMatrix, InferenceMatrixRow,
    ModelFamily, ModelRuntimeFactory, ModelRuntimeInput, ModelRuntimeMetrics, ModelRuntimeOutput,
    ModelRuntimeWarning, ParseModelFamilyError, QuantModelRuntime,
};
pub use signal::{FactorContribution, ModelExplanation, SignalCandidate, SignalWarning};
pub use trainer::{ModelTrainer, TrainModelRequest, TrainedModelArtifact};

use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", tag = "status")]
pub enum EvidenceMetric<T> {
    Available { value: T },
    Unavailable { code: String, reason: String },
}

impl<T> EvidenceMetric<T> {
    #[must_use]
    pub const fn is_available(&self) -> bool {
        matches!(self, Self::Available { .. })
    }
}

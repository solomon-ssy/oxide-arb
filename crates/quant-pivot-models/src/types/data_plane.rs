//! Compact identifiers used only inside the live data plane.

use serde::{Deserialize, Serialize};

/// Dense process-local token index assigned by the catalog data plane.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(transparent)]
pub struct TokenKey(u32);

impl TokenKey {
    #[must_use]
    pub const fn new(value: u32) -> Self {
        Self(value)
    }

    #[must_use]
    pub const fn get(self) -> u32 {
        self.0
    }

    #[must_use]
    pub const fn index(self) -> usize {
        self.0 as usize
    }
}

/// Fixed partition actor index.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(transparent)]
pub struct PartitionId(u8);

impl PartitionId {
    #[must_use]
    pub const fn new(value: u8) -> Self {
        Self(value)
    }

    #[must_use]
    pub const fn get(self) -> u8 {
        self.0
    }
}

/// Monotonic batch sequence local to one partition actor.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(transparent)]
pub struct PartitionBatchId(u64);

impl PartitionBatchId {
    #[must_use]
    pub const fn new(value: u64) -> Self {
        Self(value)
    }

    #[must_use]
    pub const fn get(self) -> u64 {
        self.0
    }
}

#[cfg(test)]
mod tests {
    use std::mem::size_of;

    use super::{PartitionBatchId, PartitionId, TokenKey};

    #[test]
    fn plane_identifiers_compact_values() {
        assert_eq!(size_of::<TokenKey>(), 4);
        assert_eq!(size_of::<PartitionId>(), 1);
        assert_eq!(size_of::<PartitionBatchId>(), 8);

        let key = TokenKey::new(7);
        let copied = key;
        assert_eq!(key, copied);
    }
}

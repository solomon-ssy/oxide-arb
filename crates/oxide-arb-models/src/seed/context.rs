//! Typed context for passing data between dependent seed units.

use std::any::Any;
use std::collections::HashMap;

/// Shared state passed through a seed plan's execution.
///
/// Graph-ordered seeds (e.g. RBAC) store created entity IDs here so that
/// downstream seeds can reference them without hard-coding values.
/// Trading bootstrap seeds currently ignore the context.
pub struct SeedContext {
    data: HashMap<&'static str, Box<dyn Any + Send + Sync>>,
}

impl SeedContext {
    #[must_use]
    pub fn new() -> Self {
        Self {
            data: HashMap::new(),
        }
    }

    /// Store a typed value for downstream seeds to read.
    pub fn put<T: Any + Send + Sync>(&mut self, key: &'static str, value: T) {
        self.data.insert(key, Box::new(value));
    }

    /// Retrieve a typed value stored by an upstream seed.
    pub fn get<T: Any + Send + Sync + 'static>(&self, key: &'static str) -> Option<&T> {
        self.data.get(key).and_then(|v| v.downcast_ref())
    }
}

impl Default for SeedContext {
    fn default() -> Self {
        Self::new()
    }
}

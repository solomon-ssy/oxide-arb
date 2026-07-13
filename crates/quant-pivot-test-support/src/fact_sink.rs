//! Test-only synchronous fact sinks for durable writer contracts.

use std::{
    marker::PhantomData,
    sync::{Arc, Mutex},
};

use async_trait::async_trait;
use quant_pivot_error::storage::StorageError;
use quant_pivot_repository::traits::FactWriter;

/// Acknowledges and discards every batch.
pub struct DiscardFactWriter<T> {
    _row: PhantomData<fn(T)>,
}

impl<T> DiscardFactWriter<T> {
    #[must_use]
    pub const fn new() -> Self {
        Self { _row: PhantomData }
    }
}

impl<T> Default for DiscardFactWriter<T> {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait]
impl<T: Send + Sync + 'static> FactWriter<T> for DiscardFactWriter<T> {
    async fn write_batch(&self, _rows: Vec<T>) -> Result<(), StorageError> {
        Ok(())
    }
}

/// Captures every durably acknowledged batch in insertion order.
pub struct RecordingFactWriter<T> {
    rows: Arc<Mutex<Vec<T>>>,
}

impl<T> RecordingFactWriter<T> {
    #[must_use]
    pub const fn new(rows: Arc<Mutex<Vec<T>>>) -> Self {
        Self { rows }
    }
}

#[async_trait]
impl<T: Send + Sync + 'static> FactWriter<T> for RecordingFactWriter<T> {
    async fn write_batch(&self, rows: Vec<T>) -> Result<(), StorageError> {
        self.rows
            .lock()
            .expect("recording fact sink mutex poisoned")
            .extend(rows);
        Ok(())
    }
}

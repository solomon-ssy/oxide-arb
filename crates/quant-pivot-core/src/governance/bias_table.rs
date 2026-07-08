//! [`BiasTableApplicator`]: the hot-reloadable favorite-longshot bias-table
//! snapshot bound to the factor plane (Phase 11.2.1).
//!
//! On each runtime-config activation the applicator resolves
//! `factors.structural.favorite_longshot.bias_table_ref`: `None` clears the
//! snapshot (the `struct.favorite_longshot` factor stays inert), and a set ref
//! is loaded from the content-addressed ledger, rehydrated, and **content-hash
//! verified** before it binds. A ref that cannot be loaded or whose recomputed
//! hash does not match is a hard activation failure — the factor plane never
//! silently falls back to a stale or absent table.
//!
//! The online [`FactorPipelineService`](crate::service::factor_pipeline::FactorPipelineService)
//! reads [`BiasTableApplicator::current`] each round; the offline plane resolves
//! its own frozen ref (training / backtest) so both sides bind the *same* table
//! bytes — no training-serving skew.

use std::sync::Arc;

use arc_swap::ArcSwap;
use quant_pivot_error::control::ControlError;
use quant_pivot_models::{
    runtime_config::FavoriteLongshotConfig,
    types::{CalibrationArtifactId, ContentHash},
};
use quant_pivot_repository::traits::CalibrationArtifactRepository;
use quant_pivot_research::model::FavoriteLongshotBiasTable;

/// Lock-free, hot-reloadable holder for the active favorite-longshot bias table.
pub struct BiasTableApplicator {
    repo: Arc<dyn CalibrationArtifactRepository>,
    snapshot: ArcSwap<Option<Arc<FavoriteLongshotBiasTable>>>,
}

impl BiasTableApplicator {
    /// An applicator with no table bound (the inert default before activation).
    #[must_use]
    pub fn new(repo: Arc<dyn CalibrationArtifactRepository>) -> Self {
        Self {
            repo,
            snapshot: ArcSwap::from_pointee(None),
        }
    }

    /// Rebuild the snapshot from a freshly activated favorite-longshot config.
    ///
    /// `bias_table_ref = None` clears the snapshot. A set ref is loaded and
    /// content-hash verified; any failure is returned as a
    /// [`ControlError::Precondition`] so the activation fails closed (never a
    /// silent downgrade to the inert / previous table).
    ///
    /// # Errors
    ///
    /// Returns [`ControlError::Precondition`] when the ref is malformed, the
    /// table is absent, or its recomputed content hash does not match.
    pub async fn reload(&self, config: &FavoriteLongshotConfig) -> Result<(), ControlError> {
        let Some(raw) = config.bias_table_ref.as_ref() else {
            self.snapshot.store(Arc::new(None));
            return Ok(());
        };
        let id: CalibrationArtifactId = raw.trim().parse().map_err(|error| {
            ControlError::Precondition(format!(
                "favorite_longshot.bias_table_ref `{raw}` is not a valid table id: {error}"
            ))
        })?;
        let info = self
            .repo
            .find_by_id(&id)
            .await
            .map_err(|error| {
                ControlError::Precondition(format!("bias-table `{id}` load failed: {error}"))
            })?
            .ok_or_else(|| {
                ControlError::Precondition(format!(
                    "favorite_longshot.bias_table_ref `{id}` not found — cannot activate a config \
                     pinning a missing bias table"
                ))
            })?;
        let table = FavoriteLongshotBiasTable::from_persisted(&info).map_err(|error| {
            ControlError::Precondition(format!("bias-table `{id}` failed to rehydrate: {error}"))
        })?;
        self.snapshot.store(Arc::new(Some(Arc::new(table))));
        Ok(())
    }

    /// The bias table under the current snapshot, if one is bound.
    #[must_use]
    pub fn current(&self) -> Option<Arc<FavoriteLongshotBiasTable>> {
        self.snapshot.load().as_ref().clone()
    }

    /// The content hash of the currently bound table, if any.
    ///
    /// Threaded into model-run input hashes and dataset coverage so a serve-time
    /// table skew against the table used to build the training dataset is
    /// auditable without polluting `factor_schema_hash`.
    #[must_use]
    pub fn current_content_hash(&self) -> Option<ContentHash> {
        self.current().map(|table| table.content_hash.clone())
    }
}

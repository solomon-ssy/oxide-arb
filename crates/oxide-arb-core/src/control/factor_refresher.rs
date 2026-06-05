//! Live control-factor refresher: startup load, periodic poll, notify-driven
//! refresh, and fail-closed policy.
//!
//! The refresher is the single writer of [`FactorSnapshotStore`]. It loads the
//! active Published/Shadow publications from Postgres, resolves their member
//! factors, and compiles them into immutable snapshots. The trading hot path
//! never performs this I/O; it only reads the published `ArcSwap`.

use crate::{
    control::factor_snapshot::FactorSnapshotStore, observability::metrics_hub::MetricsHub,
};
use chrono::Utc;
use oxide_arb_error::{OxideError, OxideResult, control::SnapshotBuildError};
use oxide_arb_models::{
    domain::control_factor::{
        ControlFactorPublicationInfo, ControlFactorSnapshot, ControlFactorValue,
    },
    enums::control_factor::PublicationMode,
};
use oxide_arb_repository::traits::ControlFactorRepository;
use parking_lot::Mutex;
use rand::RngExt;
use std::{sync::Arc, time::Duration};
use tokio::sync::Notify;
use tokio_util::sync::CancellationToken;

/// Default poll interval for the periodic refresh fallback (seconds).
const DEFAULT_REFRESH_INTERVAL_SECS: u64 = 60;
/// Jitter applied to the refresh interval to desync replicas.
const REFRESH_JITTER_PCT: f64 = 0.10;

/// Refresher tunables. `fail_closed` makes an expired critical safety factor
/// (or a failed initial Published load) refuse Live startup.
#[derive(Debug, Clone)]
pub struct FactorRefreshConfig {
    pub interval: Duration,
    pub jitter_pct: f64,
    pub fail_closed: bool,
}

impl FactorRefreshConfig {
    /// Build from execution mode: Live fails closed on safety-factor loss.
    #[must_use]
    pub const fn for_live(fail_closed: bool) -> Self {
        Self {
            interval: Duration::from_secs(DEFAULT_REFRESH_INTERVAL_SECS),
            jitter_pct: REFRESH_JITTER_PCT,
            fail_closed,
        }
    }
}

/// A compiled snapshot plus its member count (for metrics).
struct Compiled {
    snapshot: ControlFactorSnapshot,
    factor_count: usize,
}

/// Single-writer refresher for the live control-factor snapshots.
pub struct FactorRefresher {
    repo: Arc<dyn ControlFactorRepository>,
    store: Arc<FactorSnapshotStore>,
    metrics: Arc<MetricsHub>,
    config: FactorRefreshConfig,
    notify: Arc<Notify>,
    published_version: Mutex<Option<String>>,
    shadow_version: Mutex<Option<String>>,
}

impl FactorRefresher {
    #[must_use]
    pub fn new(
        repo: Arc<dyn ControlFactorRepository>,
        store: Arc<FactorSnapshotStore>,
        metrics: Arc<MetricsHub>,
        config: FactorRefreshConfig,
    ) -> Self {
        Self {
            repo,
            store,
            metrics,
            config,
            notify: Arc::new(Notify::new()),
            published_version: Mutex::new(None),
            shadow_version: Mutex::new(None),
        }
    }

    /// Handle used to wake the refresher immediately (e.g. on publication change).
    #[must_use]
    pub fn notify_handle(&self) -> Arc<Notify> {
        Arc::clone(&self.notify)
    }

    /// Startup load. Published failure under `fail_closed` refuses to start;
    /// shadow failures are always best-effort and never block boot.
    pub async fn startup(&self) -> OxideResult<()> {
        match self.refresh_mode(PublicationMode::Published).await {
            Ok(()) => {}
            Err(error) => {
                self.metrics.control_factor_fail_closed_events.inc();
                if self.config.fail_closed {
                    tracing::error!(%error, "control-factor Published load failed — refusing to start (fail closed)");
                    return Err(error);
                }
                tracing::warn!(%error, "control-factor Published load failed — continuing with neutral snapshot");
            }
        }
        if let Err(error) = self.refresh_mode(PublicationMode::Shadow).await {
            tracing::warn!(%error, "control-factor Shadow load failed — shadow disabled until next refresh");
        }
        Ok(())
    }

    /// Run the periodic + notify-driven refresh loop until shutdown.
    pub async fn run(self: Arc<Self>, shutdown: CancellationToken) {
        let mut timer = tokio::time::interval(self.config.interval);
        timer.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        timer.tick().await; // skip immediate first tick (startup already loaded)
        let notify = Arc::clone(&self.notify);
        loop {
            tokio::select! {
                biased;
                () = shutdown.cancelled() => return,
                () = notify.notified() => {}
                _ = timer.tick() => {
                    if self.config.jitter_pct > 0.0 {
                        let max_jitter = self.config.interval.mul_f64(self.config.jitter_pct);
                        tokio::time::sleep(max_jitter.mul_f64(rand_unit())).await;
                    }
                }
            }
            self.refresh_tick().await;
        }
    }

    /// One refresh iteration over both modes; never propagates errors.
    pub async fn refresh_tick(&self) {
        if let Err(error) = self.refresh_mode(PublicationMode::Published).await {
            self.metrics.control_factor_refresh_failures.inc();
            tracing::warn!(%error, "control-factor Published refresh failed — retaining prior snapshot");
        }
        if let Err(error) = self.refresh_mode(PublicationMode::Shadow).await {
            self.metrics.control_factor_refresh_failures.inc();
            tracing::warn!(%error, "control-factor Shadow refresh failed — retaining prior snapshot");
        }
        self.update_age_gauge();
    }

    /// Refresh a single mode: no-op when the active publication is unchanged.
    async fn refresh_mode(&self, mode: PublicationMode) -> OxideResult<()> {
        self.metrics.control_factor_refresh_total.inc();

        let Some(publication_info) = self.repo.load_active_publication(mode).await? else {
            // No active publication: reset slot to neutral once.
            self.apply_neutral(mode);
            return Ok(());
        };

        if !self.version_changed(mode, &publication_info.publication_hash) {
            return Ok(());
        }

        let fail_closed = matches!(mode, PublicationMode::Published) && self.config.fail_closed;
        let compiled = self.compile(&publication_info, fail_closed).await?;
        self.store(mode, compiled, &publication_info.publication_hash);
        self.metrics.control_factor_version_changes.inc();
        Ok(())
    }

    /// Resolve member factors and compile the immutable snapshot.
    async fn compile(
        &self,
        publication_info: &ControlFactorPublicationInfo,
        fail_closed: bool,
    ) -> OxideResult<Compiled> {
        let publication = publication_info.to_publication();
        let infos = self
            .repo
            .load_factors_by_ids(&publication.factor_ids)
            .await?;

        let mut factors: Vec<ControlFactorValue> = Vec::with_capacity(infos.len());
        for info in &infos {
            let typed = info.to_typed().map_err(|error| {
                OxideError::SnapshotBuild(SnapshotBuildError::DimensionPayloadMismatch {
                    factor_id: format!("{}: {error}", info.factor_id.as_str()),
                })
            })?;
            factors.push(typed);
        }

        for member in &publication.factor_ids {
            if !factors.iter().any(|factor| &factor.factor_id == member) {
                return Err(OxideError::SnapshotBuild(
                    SnapshotBuildError::MissingMember {
                        factor_id: member.as_str().to_owned(),
                    },
                ));
            }
        }

        let snapshot =
            ControlFactorSnapshot::compile(&publication, &factors, Utc::now(), fail_closed)?;
        Ok(Compiled {
            snapshot,
            factor_count: factors.len(),
        })
    }

    fn store(&self, mode: PublicationMode, compiled: Compiled, version: &str) {
        let snapshot = Arc::new(compiled.snapshot);
        match mode {
            PublicationMode::Published => {
                self.store.store_published(snapshot);
                *self.published_version.lock() = Some(version.to_owned());
                self.metrics
                    .control_factor_active_count
                    .set(i64::try_from(compiled.factor_count).unwrap_or(i64::MAX));
                self.metrics.control_factor_snapshot_load_age_seconds.set(0);
            }
            PublicationMode::Shadow => {
                self.store.store_shadow(snapshot);
                *self.shadow_version.lock() = Some(version.to_owned());
            }
        }
    }

    fn apply_neutral(&self, mode: PublicationMode) {
        let mut guard = match mode {
            PublicationMode::Published => self.published_version.lock(),
            PublicationMode::Shadow => self.shadow_version.lock(),
        };
        if guard.is_none() {
            return; // already neutral
        }
        *guard = None;
        drop(guard);
        let neutral = Arc::new(ControlFactorSnapshot::neutral(Utc::now()));
        match mode {
            PublicationMode::Published => {
                self.store.store_published(neutral);
                self.metrics.control_factor_active_count.set(0);
            }
            PublicationMode::Shadow => self.store.store_shadow(neutral),
        }
    }

    fn version_changed(&self, mode: PublicationMode, version: &str) -> bool {
        let guard = match mode {
            PublicationMode::Published => self.published_version.lock(),
            PublicationMode::Shadow => self.shadow_version.lock(),
        };
        guard.as_deref() != Some(version)
    }

    fn update_age_gauge(&self) {
        let loaded_at = self.store.published().loaded_at;
        let age = (Utc::now() - loaded_at).num_seconds().max(0);
        self.metrics
            .control_factor_snapshot_load_age_seconds
            .set(age);
    }
}

/// Uniform random in `[0, 1)` for jitter, without pulling rand into the type.
fn rand_unit() -> f64 {
    rand::rng().random_range(0.0..1.0)
}

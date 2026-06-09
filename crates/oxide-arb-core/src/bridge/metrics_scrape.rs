//! Prometheus scrape port wired to the process [`MetricsHub`].

use std::sync::Arc;

use oxide_arb_models::domain::{MetricsScrapePort, PrometheusTextPayload};

use crate::observability::metrics_hub::MetricsHub;

/// [`MetricsScrapePort`] backed by the live process metrics registry.
#[derive(Clone)]
pub struct CoreMetricsScrape {
    hub: Arc<MetricsHub>,
}

impl CoreMetricsScrape {
    /// Wrap the shared metrics hub used by the running process.
    #[must_use]
    pub const fn new(hub: Arc<MetricsHub>) -> Self {
        Self { hub }
    }
}

impl MetricsScrapePort for CoreMetricsScrape {
    fn scrape_prometheus(&self) -> Result<PrometheusTextPayload, String> {
        let (content_type, body) = self.hub.gather_prometheus_text()?;
        Ok(PrometheusTextPayload { content_type, body })
    }
}

//! Prometheus metrics scrape port for the Admin API.

use prometheus::{Encoder, Registry, TextEncoder};
use quant_pivot_models::domain::ports::MetricsScrapePort;

pub struct CoreMetricsScrape {
    registry: Registry,
}

impl CoreMetricsScrape {
    #[must_use]
    pub const fn new(registry: Registry) -> Self {
        Self { registry }
    }
}

impl MetricsScrapePort for CoreMetricsScrape {
    fn gather_prometheus(&self) -> String {
        let metric_families = self.registry.gather();
        let mut buffer = Vec::new();
        let encoder = TextEncoder::new();
        let _ = encoder.encode(&metric_families, &mut buffer);
        String::from_utf8_lossy(&buffer).into_owned()
    }
}

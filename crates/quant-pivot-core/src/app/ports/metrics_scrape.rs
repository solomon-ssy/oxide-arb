//! Prometheus metrics scrape port for the Admin API.

use quant_pivot_models::domain::MetricsScrapePort;

pub struct CoreMetricsScrape {
    registry: prometheus::Registry,
}

impl CoreMetricsScrape {
    #[must_use]
    pub const fn new(registry: prometheus::Registry) -> Self {
        Self { registry }
    }
}

impl MetricsScrapePort for CoreMetricsScrape {
    fn gather_prometheus(&self) -> String {
        use prometheus::Encoder;
        let metric_families = self.registry.gather();
        let mut buffer = Vec::new();
        let encoder = prometheus::TextEncoder::new();
        let _ = encoder.encode(&metric_families, &mut buffer);
        String::from_utf8_lossy(&buffer).into_owned()
    }
}

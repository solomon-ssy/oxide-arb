//! Prometheus counters for cache hit/miss tracking.

use prometheus::{IntCounterVec, Opts, Registry};

pub struct CacheMetrics {
    hits: IntCounterVec,
    misses: IntCounterVec,
}

impl CacheMetrics {
    pub fn new() -> Self {
        let hits = IntCounterVec::new(
            Opts::new("quant_pivot_cache_hits_total", "Total cache hits"),
            &["level", "domain"],
        )
        .expect("quant_pivot_cache_hits_total metric");

        let misses = IntCounterVec::new(
            Opts::new("quant_pivot_cache_misses_total", "Total cache misses"),
            &["domain"],
        )
        .expect("quant_pivot_cache_misses_total metric");

        Self { hits, misses }
    }

    pub fn register(&self, registry: &Registry) -> Result<(), prometheus::Error> {
        registry.register(Box::new(self.hits.clone()))?;
        registry.register(Box::new(self.misses.clone()))?;
        Ok(())
    }

    pub fn record_hit(&self, level: &str, domain: &str) {
        self.hits.with_label_values(&[level, domain]).inc();
    }

    pub fn record_miss(&self, domain: &str) {
        self.misses.with_label_values(&[domain]).inc();
    }
}

impl Default for CacheMetrics {
    fn default() -> Self {
        Self::new()
    }
}

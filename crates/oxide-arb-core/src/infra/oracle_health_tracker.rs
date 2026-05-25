use std::collections::VecDeque;
use std::time::{Duration, Instant};

use dashmap::DashMap;
use oxide_arb_models::enums::system::SourceHealth;

pub struct OracleHealthTracker {
    sources: DashMap<String, SourceHealthWindow>,
}

struct SourceHealthWindow {
    samples: VecDeque<(Instant, bool)>,
    window: Duration,
    consecutive_failures: u32,
}

impl OracleHealthTracker {
    pub fn new() -> Self {
        Self {
            sources: DashMap::new(),
        }
    }

    pub fn record(&self, source_id: &str, success: bool) {
        self.sources
            .entry(source_id.to_owned())
            .or_insert_with(|| SourceHealthWindow::new(Duration::from_secs(300)))
            .record(success);
    }

    pub fn health(&self, source_id: &str) -> SourceHealth {
        self.sources
            .get(source_id)
            .map_or(SourceHealth::Healthy, |w| w.evaluate())
    }

    pub fn all_healthy_or_degraded(&self) -> bool {
        self.sources
            .iter()
            .all(|e| e.evaluate() != SourceHealth::Down)
    }
}

impl SourceHealthWindow {
    const fn new(window: Duration) -> Self {
        Self {
            samples: VecDeque::new(),
            window,
            consecutive_failures: 0,
        }
    }

    fn record(&mut self, success: bool) {
        let now = Instant::now();
        self.samples.push_back((now, success));
        self.prune(now);
        if success {
            self.consecutive_failures = 0;
        } else {
            self.consecutive_failures += 1;
        }
    }

    fn prune(&mut self, now: Instant) {
        while let Some(&(ts, _)) = self.samples.front() {
            if now.duration_since(ts) > self.window {
                self.samples.pop_front();
            } else {
                break;
            }
        }
    }

    fn evaluate(&self) -> SourceHealth {
        if self.consecutive_failures >= 5 {
            return SourceHealth::Down;
        }
        if self.samples.is_empty() {
            return SourceHealth::Healthy;
        }
        let success_count = self.samples.iter().filter(|(_, s)| *s).count();
        let total = self.samples.len();
        if success_count * 10 > total * 9 {
            SourceHealth::Healthy
        } else if success_count * 2 >= total {
            SourceHealth::Degraded
        } else {
            SourceHealth::Down
        }
    }
}

impl Default for OracleHealthTracker {
    fn default() -> Self {
        Self::new()
    }
}

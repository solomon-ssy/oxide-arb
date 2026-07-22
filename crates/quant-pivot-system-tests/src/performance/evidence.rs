use std::{
    fs,
    path::{Path, PathBuf},
    process::Command,
};

use anyhow::{Context, Result, bail};
use chrono::{DateTime, Utc};
use hdrhistogram::Histogram;
use quant_pivot_allocator::NAME as ALLOCATOR_NAME;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use tikv_jemalloc_ctl::{epoch, stats::allocated};

pub const PERFORMANCE_EVIDENCE_SCHEMA_VERSION: u16 = 1;
pub const HDR_LOWEST_US: u64 = 1;
pub const HDR_HIGHEST_US: u64 = 60_000_000;
pub const HDR_SIGNIFICANT_FIGURES: u8 = 3;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PerformanceEnvironmentV1 {
    pub git_sha: String,
    pub git_dirty: bool,
    pub rustc: String,
    pub kernel: String,
    pub cpu_model: String,
    pub cpu_governor: String,
    pub allocator: String,
    pub clickhouse_version: String,
    pub clickhouse_settings: Vec<String>,
    pub network_rtt_p50_us: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PerformanceWorkloadV1 {
    pub active_tokens: usize,
    pub warmup_seconds: u64,
    pub sustained_seconds: u64,
    pub sustained_events_per_second: u64,
    pub burst_seconds: u64,
    pub burst_events_per_second: u64,
    pub recovery_seconds: u64,
    pub source_events: u64,
    pub durable_publications: u64,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct PerformanceCorrectnessV1 {
    pub source_errors: u64,
    pub dropped: u64,
    pub gaps: u64,
    pub duplicates: u64,
    pub out_of_order: u64,
    pub invalid_fresh_reads: u64,
    pub ws_session_invalidations: u64,
    pub book_apply_invalidations: u64,
    pub writer_drops: u64,
    pub writer_flush_failures: u64,
}

impl PerformanceCorrectnessV1 {
    #[must_use]
    pub const fn total(&self) -> u64 {
        self.source_errors
            .saturating_add(self.dropped)
            .saturating_add(self.gaps)
            .saturating_add(self.duplicates)
            .saturating_add(self.out_of_order)
            .saturating_add(self.invalid_fresh_reads)
            .saturating_add(self.ws_session_invalidations)
            .saturating_add(self.book_apply_invalidations)
            .saturating_add(self.writer_drops)
            .saturating_add(self.writer_flush_failures)
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HistogramSummaryV1 {
    pub samples: u64,
    pub p50_us: u64,
    pub p95_us: u64,
    pub p99_us: u64,
    pub max_us: u64,
}

impl HistogramSummaryV1 {
    #[must_use]
    pub fn from_histogram(histogram: &Histogram<u64>) -> Self {
        Self {
            samples: histogram.len(),
            p50_us: histogram.value_at_quantile(0.50),
            p95_us: histogram.value_at_quantile(0.95),
            p99_us: histogram.value_at_quantile(0.99),
            max_us: histogram.max(),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PerformanceMeasurementsV1 {
    pub enqueue_latency: HistogramSummaryV1,
    pub ingress_to_durable_publish_latency: HistogramSummaryV1,
    pub throughput_events_per_second: f64,
    pub cpu_ns_per_event: Option<f64>,
    pub encoded_bytes_per_event: f64,
    pub net_allocated_bytes_per_event: Option<f64>,
    pub online_rss_bytes: Option<u64>,
    pub peak_rss_bytes: Option<u64>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PerformanceArtifactV1 {
    pub kind: String,
    pub path: PathBuf,
    pub sha256: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PerformanceEvidenceV1 {
    pub schema_version: u16,
    pub profile: String,
    pub run_index: u16,
    pub started_at: DateTime<Utc>,
    pub finished_at: DateTime<Utc>,
    pub fixture_seed: u64,
    pub fixture_hash: String,
    pub environment: PerformanceEnvironmentV1,
    pub workload: PerformanceWorkloadV1,
    pub correctness: PerformanceCorrectnessV1,
    pub measurements: PerformanceMeasurementsV1,
    pub artifacts: Vec<PerformanceArtifactV1>,
    pub passed: bool,
}

#[derive(Debug, Serialize)]
struct RawHistogramV1 {
    schema_version: u16,
    unit: &'static str,
    coordinated_omission_expected_interval_us: u64,
    lowest_discernible_value: u64,
    highest_trackable_value: u64,
    significant_figures: u8,
    total_count: u64,
    recorded: Vec<RawHistogramBucketV1>,
}

#[derive(Debug, Serialize)]
struct RawHistogramBucketV1 {
    value_iterated_to: u64,
    count_at_value: u64,
}

pub fn collect_environment(
    clickhouse_version: String,
    clickhouse_settings: Vec<String>,
    network_rtt_p50_us: u64,
) -> Result<PerformanceEnvironmentV1> {
    let status = command_output("git", &["status", "--porcelain"])?;
    Ok(PerformanceEnvironmentV1 {
        git_sha: command_output("git", &["rev-parse", "HEAD"])?,
        git_dirty: !status.is_empty(),
        rustc: command_output("rustc", &["-Vv"])?,
        kernel: command_output("uname", &["-srvmo"])?,
        cpu_model: cpu_model()?,
        cpu_governor: cpu_governor(),
        allocator: ALLOCATOR_NAME.to_owned(),
        clickhouse_version,
        clickhouse_settings,
        network_rtt_p50_us,
    })
}

pub fn write_histogram_artifact(
    output_dir: &Path,
    name: &str,
    histogram: &Histogram<u64>,
    expected_interval_us: u64,
) -> Result<PerformanceArtifactV1> {
    let recorded = histogram
        .iter_recorded()
        .map(|entry| RawHistogramBucketV1 {
            value_iterated_to: entry.value_iterated_to(),
            count_at_value: entry.count_at_value(),
        })
        .collect();
    let raw = RawHistogramV1 {
        schema_version: PERFORMANCE_EVIDENCE_SCHEMA_VERSION,
        unit: "microseconds",
        coordinated_omission_expected_interval_us: expected_interval_us,
        lowest_discernible_value: HDR_LOWEST_US,
        highest_trackable_value: HDR_HIGHEST_US,
        significant_figures: HDR_SIGNIFICANT_FIGURES,
        total_count: histogram.len(),
        recorded,
    };
    fs::create_dir_all(output_dir).with_context(|| {
        format!(
            "create performance artifact directory {}",
            output_dir.display()
        )
    })?;
    let path = output_dir.join(format!("{name}.hdr.json"));
    let bytes = serde_json::to_vec_pretty(&raw).context("serialize raw HDR histogram")?;
    fs::write(&path, &bytes)
        .with_context(|| format!("write raw HDR histogram {}", path.display()))?;
    Ok(PerformanceArtifactV1 {
        kind: "hdr_histogram_v1".to_owned(),
        path,
        sha256: sha256_hex(&bytes),
    })
}

pub fn write_evidence(output_dir: &Path, evidence: &PerformanceEvidenceV1) -> Result<PathBuf> {
    fs::create_dir_all(output_dir).with_context(|| {
        format!(
            "create performance evidence directory {}",
            output_dir.display()
        )
    })?;
    let path = output_dir.join(format!(
        "performance-evidence-v1-{}-run-{}.json",
        evidence.profile, evidence.run_index
    ));
    let bytes = serde_json::to_vec_pretty(evidence).context("serialize PerformanceEvidenceV1")?;
    fs::write(&path, bytes)
        .with_context(|| format!("write performance evidence {}", path.display()))?;
    Ok(path)
}

pub fn process_cpu_ns() -> Result<Option<u64>> {
    let Ok(stat) = fs::read_to_string("/proc/self/stat") else {
        return Ok(None);
    };
    let close = stat
        .rfind(')')
        .context("/proc/self/stat is missing command terminator")?;
    let fields = stat[close + 1..].split_whitespace().collect::<Vec<_>>();
    let user_ticks = fields
        .get(11)
        .context("/proc/self/stat is missing user ticks")?
        .parse::<u64>()
        .context("parse process user ticks")?;
    let system_ticks = fields
        .get(12)
        .context("/proc/self/stat is missing system ticks")?
        .parse::<u64>()
        .context("parse process system ticks")?;
    let ticks_per_second = command_output("getconf", &["CLK_TCK"])?
        .parse::<u64>()
        .context("parse CLK_TCK")?;
    if ticks_per_second == 0 {
        bail!("CLK_TCK must be non-zero");
    }
    Ok(Some(
        user_ticks
            .saturating_add(system_ticks)
            .saturating_mul(1_000_000_000)
            / ticks_per_second,
    ))
}

pub fn resident_memory_bytes() -> Result<Option<u64>> {
    linux_status_bytes("VmRSS:")
}

pub fn peak_resident_memory_bytes() -> Result<Option<u64>> {
    linux_status_bytes("VmHWM:")
}

pub fn jemalloc_allocated_bytes() -> Result<usize> {
    epoch::advance().context("advance jemalloc statistics epoch")?;
    allocated::read().context("read jemalloc allocated bytes")
}

fn linux_status_bytes(label: &str) -> Result<Option<u64>> {
    let Ok(status) = fs::read_to_string("/proc/self/status") else {
        return Ok(None);
    };
    let Some(line) = status.lines().find(|line| line.starts_with(label)) else {
        return Ok(None);
    };
    let kib = line
        .split_whitespace()
        .nth(1)
        .context("Linux memory status line is missing its value")?
        .parse::<u64>()
        .context("parse Linux memory status")?;
    Ok(Some(kib.saturating_mul(1_024)))
}

fn command_output(program: &str, args: &[&str]) -> Result<String> {
    let output = Command::new(program)
        .args(args)
        .output()
        .with_context(|| format!("run {program}"))?;
    if !output.status.success() {
        bail!("{program} failed with {}", output.status);
    }
    String::from_utf8(output.stdout)
        .with_context(|| format!("decode {program} output"))
        .map(|output| output.trim().to_owned())
}

fn cpu_model() -> Result<String> {
    if let Ok(cpuinfo) = fs::read_to_string("/proc/cpuinfo")
        && let Some(model) = cpuinfo
            .lines()
            .find_map(|line| line.strip_prefix("model name\t: "))
    {
        return Ok(model.to_owned());
    }
    command_output("sysctl", &["-n", "machdep.cpu.brand_string"])
}

fn cpu_governor() -> String {
    fs::read_to_string("/sys/devices/system/cpu/cpu0/cpufreq/scaling_governor").map_or_else(
        |_| "unavailable".to_owned(),
        |value| value.trim().to_owned(),
    )
}

fn sha256_hex(bytes: &[u8]) -> String {
    hex::encode(Sha256::digest(bytes))
}

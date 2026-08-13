use std::{
    env, fs,
    path::{Path, PathBuf},
    process::{Command, Output},
};

use anyhow::{Context, Result, bail};
use chrono::{DateTime, Utc};
use quant_pivot_system_tests::performance::PerformanceProfile;
use serde::Serialize;
use sha2::{Digest, Sha256};

const KERNEL_EVIDENCE_SCHEMA_VERSION: u16 = 1;
const FULL_KERNEL_REPETITIONS: u16 = 10;

#[derive(Serialize)]
struct KernelEvidenceV1 {
    schema_version: u16,
    profile: String,
    started_at: DateTime<Utc>,
    finished_at: DateTime<Utc>,
    runs: Vec<KernelRunEvidenceV1>,
}

#[derive(Serialize)]
struct KernelRunEvidenceV1 {
    gate: String,
    iteration: u16,
    command: Vec<String>,
    stdout: String,
    stderr: String,
    output_sha256: String,
    peak_rss_bytes: Option<u64>,
}

struct KernelGate {
    name: &'static str,
    cargo_args: Vec<String>,
    program_args: Vec<String>,
    repetitions: u16,
}

pub fn run(profile: PerformanceProfile, output_dir: &Path) -> Result<()> {
    fs::create_dir_all(output_dir).with_context(|| {
        format!(
            "create performance orchestration output {}",
            output_dir.display()
        )
    })?;
    let started_at = Utc::now();
    let mut runs = Vec::new();
    for gate in kernel_gates(profile) {
        for iteration in 1..=gate.repetitions {
            let output = run_cargo(&gate.cargo_args, &gate.program_args)?;
            let evidence = kernel_run_evidence(&gate, iteration, &output);
            let succeeded = output.status.success();
            runs.push(evidence);
            if !succeeded {
                write_kernel_evidence(output_dir, profile, started_at, runs)?;
                bail!(
                    "performance kernel {} iteration {iteration} failed with {}",
                    gate.name,
                    output.status
                );
            }
        }
    }
    let kernel_path = write_kernel_evidence(output_dir, profile, started_at, runs)?;
    println!("kernel performance evidence: {}", kernel_path.display());

    let cargo = env::var_os("CARGO").unwrap_or_else(|| "cargo".into());
    let status = Command::new(cargo)
        .args([
            "run",
            "--release",
            "-p",
            "quant-pivot-system-tests",
            "--bin",
            "performance_load",
            "--",
            "--profile",
            profile.name(),
            "--output",
        ])
        .arg(output_dir)
        .status()
        .context("run production-stack performance load")?;
    if !status.success() {
        bail!("production-stack performance load failed with {status}");
    }
    Ok(())
}

fn kernel_gates(profile: PerformanceProfile) -> Vec<KernelGate> {
    let repetitions = if profile == PerformanceProfile::Smoke {
        1
    } else {
        FULL_KERNEL_REPETITIONS
    };
    let model_rows = if profile == PerformanceProfile::Smoke {
        "100000"
    } else {
        "1000000"
    };
    let portfolio_args = if profile == PerformanceProfile::Smoke {
        ["1000", "100", "20", "30"]
    } else {
        ["10000", "400", "20", "180"]
    };
    vec![
        KernelGate {
            name: "training_matrix_gate",
            cargo_args: release_bench_args("training_matrix_gate", None, true),
            program_args: vec!["1000000".to_owned()],
            repetitions,
        },
        KernelGate {
            name: "cpcv_orchestration_gate",
            cargo_args: release_bench_args("cpcv_orchestration_gate", None, true),
            program_args: vec!["1000000".to_owned()],
            repetitions,
        },
        KernelGate {
            name: "portfolio_compute_gate",
            cargo_args: release_bench_args("portfolio_compute_gate", None, true),
            program_args: portfolio_args.map(str::to_owned).to_vec(),
            repetitions,
        },
        KernelGate {
            name: "report_funnel_gate",
            cargo_args: release_bench_args("report_funnel_gate", None, false),
            program_args: Vec::new(),
            repetitions,
        },
        KernelGate {
            name: "model_train_replay_gate",
            cargo_args: release_bench_args(
                "model_train_replay_gate",
                Some("model-train-gate"),
                false,
            ),
            program_args: vec![model_rows.to_owned()],
            repetitions: 1,
        },
    ]
}

fn release_bench_args(
    binary: &str,
    features: Option<&str>,
    no_default_features: bool,
) -> Vec<String> {
    let mut args = vec![
        "run".to_owned(),
        "--quiet".to_owned(),
        "--release".to_owned(),
        "-p".to_owned(),
        "quant-pivot-bench".to_owned(),
    ];
    if no_default_features {
        args.push("--no-default-features".to_owned());
    }
    if let Some(features) = features {
        args.extend(["--features".to_owned(), features.to_owned()]);
    }
    args.extend(["--bin".to_owned(), binary.to_owned()]);
    args
}

fn run_cargo(cargo_args: &[String], program_args: &[String]) -> Result<Output> {
    let cargo = env::var_os("CARGO").unwrap_or_else(|| "cargo".into());
    let mut command = Command::new(cargo);
    command.args(cargo_args);
    if !program_args.is_empty() {
        command.arg("--").args(program_args);
    }
    command.output().context("run performance kernel")
}

fn kernel_run_evidence(gate: &KernelGate, iteration: u16, output: &Output) -> KernelRunEvidenceV1 {
    let stdout = String::from_utf8_lossy(&output.stdout).into_owned();
    let stderr = String::from_utf8_lossy(&output.stderr).into_owned();
    let mut bytes = output.stdout.clone();
    bytes.extend_from_slice(&output.stderr);
    let mut command = gate.cargo_args.clone();
    if !gate.program_args.is_empty() {
        command.push("--".to_owned());
        command.extend(gate.program_args.clone());
    }
    let peak_rss_bytes = parse_u64_metric(&stdout, "peak_rss_bytes");
    KernelRunEvidenceV1 {
        gate: gate.name.to_owned(),
        iteration,
        command,
        stdout,
        stderr,
        output_sha256: sha256_hex(&bytes),
        peak_rss_bytes,
    }
}

fn parse_u64_metric(output: &str, name: &str) -> Option<u64> {
    output
        .split_ascii_whitespace()
        .find_map(|field| field.strip_prefix(name)?.strip_prefix('='))
        .and_then(|value| value.parse().ok())
}

fn write_kernel_evidence(
    output_dir: &Path,
    profile: PerformanceProfile,
    started_at: DateTime<Utc>,
    runs: Vec<KernelRunEvidenceV1>,
) -> Result<PathBuf> {
    let evidence = KernelEvidenceV1 {
        schema_version: KERNEL_EVIDENCE_SCHEMA_VERSION,
        profile: profile.name().to_owned(),
        started_at,
        finished_at: Utc::now(),
        runs,
    };
    let path = output_dir.join("kernel-evidence-v1.json");
    let bytes = serde_json::to_vec_pretty(&evidence).context("serialize kernel evidence")?;
    fs::write(&path, bytes).with_context(|| format!("write kernel evidence {}", path.display()))?;
    Ok(path)
}

fn sha256_hex(bytes: &[u8]) -> String {
    hex::encode(Sha256::digest(bytes))
}

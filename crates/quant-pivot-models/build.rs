use std::{
    env,
    path::{Path, PathBuf},
    process::Command,
};

const BUILD_GIT_SHA_ENV: &str = "QUANT_PIVOT_BUILD_GIT_SHA";
const BUILD_GIT_STATE_ENV: &str = "QUANT_PIVOT_BUILD_GIT_STATE";

fn main() {
    let manifest_dir = env::var_os("CARGO_MANIFEST_DIR")
        .map(PathBuf::from)
        .unwrap_or_default();
    let workspace_root = manifest_dir.join("../..");
    let git_dir = workspace_root.join(".git");

    println!("cargo:rerun-if-changed={}", git_dir.join("HEAD").display());
    println!("cargo:rerun-if-changed={}", git_dir.join("index").display());

    let git_sha = git_output(&workspace_root, &["rev-parse", "HEAD"])
        .filter(|value| !value.is_empty())
        .unwrap_or_else(|| "unavailable".to_owned());
    let git_state = git_output(
        &workspace_root,
        &["status", "--porcelain=v1", "--untracked-files=normal"],
    )
    .map_or("unavailable", |status| {
        if status.is_empty() { "clean" } else { "dirty" }
    });

    println!("cargo:rustc-env={BUILD_GIT_SHA_ENV}={git_sha}");
    println!("cargo:rustc-env={BUILD_GIT_STATE_ENV}={git_state}");
}

fn git_output(workspace_root: &Path, args: &[&str]) -> Option<String> {
    let output = Command::new("git")
        .args(args)
        .current_dir(workspace_root)
        .output()
        .ok()?;
    if !output.status.success() {
        return None;
    }
    String::from_utf8(output.stdout)
        .ok()
        .map(|value| value.trim().to_owned())
}

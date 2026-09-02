//! Environment ownership at nested Cargo process boundaries.

use std::{collections::BTreeSet, env, ffi::OsString, process::Command};

/// Prevent a parent Cargo invocation's package identity from invalidating child builds.
///
/// Cargo sets these values for `cargo run` and `cargo test`. A dependency build
/// script may track the environment received by the nested Cargo process before
/// Cargo replaces it with that dependency's own package metadata. User-controlled
/// build, toolchain, registry, proxy, and jobserver settings remain inherited.
pub trait CargoCommandExt {
    /// Remove caller package metadata from this command without changing the parent environment.
    fn clear_caller_metadata(&mut self) -> &mut Self;
}

impl CargoCommandExt for Command {
    fn clear_caller_metadata(&mut self) -> &mut Self {
        let metadata_keys: BTreeSet<OsString> = env::vars_os()
            .map(|(key, _)| key)
            .chain(self.get_envs().map(|(key, _)| key.to_owned()))
            .filter(|key| {
                let key = key.as_encoded_bytes();
                key.starts_with(b"CARGO_PKG_")
                    || key.starts_with(b"CARGO_BIN_EXE_")
                    || matches!(
                        key,
                        b"CARGO_MANIFEST_DIR"
                            | b"CARGO_MANIFEST_PATH"
                            | b"CARGO_MANIFEST_LINKS"
                            | b"CARGO_CRATE_NAME"
                            | b"CARGO_BIN_NAME"
                            | b"CARGO_PRIMARY_PACKAGE"
                            | b"CARGO_TARGET_TMPDIR"
                    )
            })
            .collect();
        for key in metadata_keys {
            self.env_remove(key);
        }
        self
    }
}

#[cfg(test)]
mod tests {
    use std::{collections::BTreeMap, env, ffi::OsStr, fs, process::Command, time::Duration};

    use anyhow::{Context, Result, ensure};
    use tempfile::TempDir;
    use tokio::{process::Command as TokioCommand, time::timeout};

    use super::CargoCommandExt;

    struct CargoFixture {
        directory: TempDir,
    }

    impl CargoFixture {
        fn new() -> Result<Self> {
            let directory = TempDir::with_prefix("quant-pivot-cargo-env-")?;
            fs::create_dir(directory.path().join("src"))?;
            fs::write(
                directory.path().join("Cargo.toml"),
                "[package]\nname = \"cargo-env-fixture\"\nversion = \"0.1.0\"\nedition = \"2024\"\n\n[workspace]\n",
            )?;
            fs::write(directory.path().join("src/lib.rs"), "pub struct Fixture;\n")?;
            fs::write(
                directory.path().join("build.rs"),
                r#"use std::{env, fs::OpenOptions, io::Write, path::PathBuf};

fn main() {
    println!("cargo:rerun-if-changed=build.rs");
    println!("cargo:rerun-if-env-changed=CARGO_MANIFEST_DIR");
    println!("cargo:rerun-if-env-changed=CARGO_PKG_NAME");
    let output = PathBuf::from(env::var_os("OUT_DIR").expect("Cargo build output"));
    let mut executions = OpenOptions::new()
        .create(true)
        .append(true)
        .open(output.join("executions.txt"))
        .expect("open isolated execution counter");
    writeln!(executions, "run").expect("record build script execution");
}
"#,
            )?;
            Ok(Self { directory })
        }

        async fn build(&self, caller: &str, clear_metadata: bool) -> Result<()> {
            let mut command = Command::new(env!("CARGO"));
            command
                .current_dir(self.directory.path())
                .args([
                    "build",
                    "--offline",
                    "--quiet",
                    "--jobs",
                    "1",
                    "--target-dir",
                ])
                .arg(self.directory.path().join("target"))
                .env("CARGO_MANIFEST_DIR", self.directory.path().join(caller))
                .env("CARGO_PKG_NAME", caller);
            if clear_metadata {
                command.clear_caller_metadata();
            }
            let output = timeout(
                Duration::from_secs(30),
                TokioCommand::from(command).kill_on_drop(true).output(),
            )
            .await
            .context("isolated Cargo build exceeded its 30-second deadline")??;
            ensure!(
                output.status.success(),
                "isolated Cargo build failed: {}",
                String::from_utf8_lossy(&output.stderr)
            );
            Ok(())
        }

        fn executions(&self) -> Result<usize> {
            let mut pending = vec![self.directory.path().join("target")];
            let mut executions = 0;
            while let Some(directory) = pending.pop() {
                for entry in fs::read_dir(directory)? {
                    let entry = entry?;
                    if entry.file_type()?.is_dir() {
                        pending.push(entry.path());
                    } else if entry.file_name() == "executions.txt" {
                        executions += fs::read_to_string(entry.path())?.lines().count();
                    }
                }
            }
            Ok(executions)
        }
    }

    #[test]
    fn child_environment_is_scoped() {
        let metadata = [
            "CARGO_MANIFEST_DIR",
            "CARGO_MANIFEST_PATH",
            "CARGO_MANIFEST_LINKS",
            "CARGO_PKG_NAME",
            "CARGO_PKG_VERSION",
            "CARGO_PKG_EXTRA_METADATA",
            "CARGO_CRATE_NAME",
            "CARGO_BIN_NAME",
            "CARGO_BIN_EXE_fixture",
            "CARGO_PRIMARY_PACKAGE",
            "CARGO_TARGET_TMPDIR",
        ];
        let parent_before: Vec<_> = metadata.iter().map(env::var_os).collect();
        let build_settings = [
            ("CARGO", "/fixture/bin/cargo"),
            ("CARGO_BUILD_JOBS", "2"),
            ("CARGO_HOME", "/fixture/cargo"),
            ("CARGO_TARGET_DIR", "/fixture/target"),
            ("CARGO_BUILD_TARGET", "aarch64-apple-darwin"),
            ("CARGO_MAKEFLAGS", "--jobserver-auth=3,4 -j"),
            ("RUSTFLAGS", "-C target-cpu=native"),
            ("CARGO_ENCODED_RUSTFLAGS", "-C\u{1f}target-cpu=native"),
            ("RUSTUP_TOOLCHAIN", "stable"),
            (
                "CARGO_REGISTRIES_FIXTURE_INDEX",
                "https://example.invalid/index",
            ),
            ("HTTPS_PROXY", "http://127.0.0.1:1"),
            ("NO_PROXY", "localhost,127.0.0.1"),
        ];
        let mut command = Command::new("fixture-cargo");
        command
            .envs(metadata.iter().map(|key| (key, "caller-metadata")))
            .envs(build_settings)
            .clear_caller_metadata();
        let child_environment: BTreeMap<_, _> = command.get_envs().collect();

        for key in metadata {
            assert_eq!(child_environment.get(OsStr::new(key)), Some(&None), "{key}");
        }
        for (key, value) in build_settings {
            assert_eq!(
                child_environment.get(OsStr::new(key)),
                Some(&Some(OsStr::new(value))),
                "{key}"
            );
        }
        let parent_after: Vec<_> = metadata.iter().map(env::var_os).collect();
        assert_eq!(parent_before, parent_after);
    }

    #[tokio::test]
    async fn nested_build_reuses_cache() -> Result<()> {
        let fixture = CargoFixture::new()?;

        fixture.build("caller-a", false).await?;
        assert_eq!(fixture.executions()?, 1);
        fixture.build("caller-a", false).await?;
        assert_eq!(fixture.executions()?, 1);
        fixture.build("caller-b", false).await?;
        assert_eq!(fixture.executions()?, 2);

        fixture.build("caller-a", true).await?;
        assert_eq!(fixture.executions()?, 3);
        fixture.build("caller-b", true).await?;
        assert_eq!(fixture.executions()?, 3);
        fixture.build("caller-a", true).await?;
        assert_eq!(fixture.executions()?, 3);
        Ok(())
    }
}

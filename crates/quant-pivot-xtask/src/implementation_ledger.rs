use std::{
    collections::{BTreeMap, BTreeSet},
    fs,
    path::Path,
    process::Command,
};

use anyhow::{Context, Result, bail};

const DESIGN_PATH: &str =
    "docs/plans/quant-pivot/phase-12/12.0-execution-authority-account-recovery-fast-feedback.md";
const LEDGER_PATH: &str = "docs/plans/quant-pivot/phase-12/12.1-implementation-ledger.md";
const STATES: &[&str] = &["TODO", "IN_PROGRESS", "BLOCKED", "DONE"];
const CHECKPOINT_FIELDS: &[&str] = &[
    "design_contract_hash",
    "baseline_branch",
    "baseline_head",
    "baseline_git_status",
    "current_task_id",
    "last_completed_task_id",
    "last_verified_command",
    "next_recovery_command",
    "implementation_status",
    "operational_activation_claimed",
];

#[derive(Debug)]
struct Task {
    status: String,
    dependencies: Vec<String>,
    detail: String,
}

pub fn validate(workspace_root: &Path) -> Result<Vec<String>> {
    let design_path = workspace_root.join(DESIGN_PATH);
    let ledger_path = workspace_root.join(LEDGER_PATH);
    let design = fs::read_to_string(&design_path)
        .with_context(|| format!("read {}", design_path.display()))?;
    let ledger = fs::read_to_string(&ledger_path)
        .with_context(|| format!("read {}", ledger_path.display()))?;
    let mut violations = validate_sources(&design, &ledger);
    let documentation_complete = ["W0-01", "W0-02", "W0-03", "W0-04"]
        .into_iter()
        .all(|id| task_status(&ledger, id) == Some("DONE"));
    if !documentation_complete && workspace_root.join(".git").exists() {
        violations.extend(validate_scope(workspace_root)?);
    }
    Ok(violations
        .into_iter()
        .map(|violation| format!("{}: {violation}", ledger_path.display()))
        .collect())
}

fn validate_sources(design: &str, ledger: &str) -> Vec<String> {
    let mut violations = Vec::new();
    let Some((checkpoint_source, implementation_and_after)) =
        split_heading(ledger, "## 2. Implementation Ledger")
    else {
        return vec!["missing Implementation Ledger section".to_owned()];
    };
    let Some((implementation, evidence_and_after)) =
        split_heading(implementation_and_after, "## 3. Evidence Ledger")
    else {
        return vec!["missing Evidence Ledger section".to_owned()];
    };
    let Some((evidence, decision_and_after)) =
        split_heading(evidence_and_after, "## 4. Decision Ledger")
    else {
        return vec!["missing Decision Ledger section".to_owned()];
    };
    let Some((decisions, _blockers)) = split_heading(decision_and_after, "## 5. Blocker Ledger")
    else {
        return vec!["missing Blocker Ledger section".to_owned()];
    };

    let mut checkpoint = BTreeMap::new();
    for field in CHECKPOINT_FIELDS {
        let marker = format!("- `{field}`:");
        let value = checkpoint_source
            .lines()
            .find_map(|line| {
                line.trim()
                    .strip_prefix(&marker)
                    .map(|value| value.trim().trim_matches('`').to_owned())
            })
            .unwrap_or_default();
        if value.is_empty() {
            violations.push(format!("checkpoint field `{field}` is missing or empty"));
        }
        checkpoint.insert(*field, value);
    }

    let expected_hash = format!("blake3:{}", blake3::hash(design.as_bytes()).to_hex());
    if checkpoint.get("design_contract_hash") != Some(&expected_hash) {
        violations.push(format!(
            "design_contract_hash differs from current design bytes: expected `{expected_hash}`"
        ));
    }
    if checkpoint
        .get("operational_activation_claimed")
        .is_none_or(|value| value != "false")
    {
        violations
            .push("operational activation cannot be claimed during implementation".to_owned());
    }
    if !decisions.contains("design") && !decisions.contains("设计") {
        violations.push("Decision Ledger does not record the design contract".to_owned());
    }

    let mut tasks = BTreeMap::<String, Task>::new();
    for line in implementation.lines() {
        let Some(row) = cells(line) else {
            continue;
        };
        let Some(id) = row.first() else {
            continue;
        };
        if !valid_id(id) || row.len() < 5 {
            continue;
        }
        if tasks.contains_key(*id) {
            violations.push(format!("task ID `{id}` is duplicated"));
            continue;
        }
        let status = row[1].to_owned();
        if !STATES.contains(&status.as_str()) {
            violations.push(format!("task `{id}` uses unsupported status `{status}`"));
        }
        let dependencies = parse_dependencies(row[2], id, &mut violations);
        tasks.insert(
            (*id).to_owned(),
            Task {
                status,
                dependencies,
                detail: row[3..].join(" | "),
            },
        );
    }
    if tasks.is_empty() {
        violations.push("implementation ledger contains no task rows".to_owned());
        return violations;
    }

    let active = tasks
        .iter()
        .filter_map(|(id, task)| (task.status == "IN_PROGRESS").then_some(id.as_str()))
        .collect::<Vec<_>>();
    let checkpoint_task = checkpoint
        .get("current_task_id")
        .map(String::as_str)
        .unwrap_or_default();
    match (checkpoint_task, active.as_slice()) {
        ("none", []) => {}
        (_, [id]) if checkpoint_task == *id => {}
        _ => violations.push(format!(
            "current_task_id `{checkpoint_task}` does not match the single active task [{}]",
            active.join(", ")
        )),
    }

    let passed = evidence
        .lines()
        .filter_map(cells)
        .filter(|row| row.len() >= 4 && row[2].starts_with("PASS"))
        .flat_map(|row| extract_ids(row[1]))
        .collect::<BTreeSet<_>>();
    for (id, task) in &tasks {
        if task.status == "DONE" && !passed.contains(id) {
            violations.push(format!("DONE task `{id}` has no PASS evidence"));
        }
        if task.status == "BLOCKED"
            && (!task.detail.contains("blocker=")
                || !task.detail.contains("unblock=")
                || !task.detail.contains("resume="))
        {
            violations.push(format!(
                "BLOCKED task `{id}` must record blocker=, unblock=, and resume="
            ));
        }
        validate_dependencies(id, task, &tasks, &mut violations);
    }
    violations
}

fn task_status<'a>(ledger: &'a str, task_id: &str) -> Option<&'a str> {
    ledger.lines().find_map(|line| {
        let row = cells(line)?;
        (row.len() >= 2 && row[0] == task_id).then_some(row[1])
    })
}

fn validate_scope(workspace_root: &Path) -> Result<Vec<String>> {
    let output = Command::new("git")
        .args(["status", "--porcelain", "--untracked-files=all"])
        .current_dir(workspace_root)
        .output()
        .context("inspect documentation-stage changed paths")?;
    if !output.status.success() {
        bail!(
            "git status failed while checking documentation scope: {}",
            String::from_utf8_lossy(&output.stderr).trim()
        );
    }
    let allowed = [
        "docs/plans/quant-pivot/phase-12/",
        "docs/plans/quant-pivot/README.md",
        "docs/audit/2026-08-13-full-system-deep-audit.md",
        "crates/quant-pivot-xtask/src/architecture.rs",
        "crates/quant-pivot-xtask/src/implementation_ledger.rs",
        "crates/quant-pivot-xtask/src/main.rs",
        "ui",
    ];
    Ok(String::from_utf8_lossy(&output.stdout)
        .lines()
        .filter_map(|line| line.get(3..).map(str::trim))
        .filter(|path| !allowed.iter().any(|prefix| path.starts_with(prefix)))
        .map(|path| format!("business-code change `{path}` is not allowed before docs close"))
        .collect())
}

fn split_heading<'a>(source: &'a str, heading: &str) -> Option<(&'a str, &'a str)> {
    source.split_once(&format!("\n{heading}\n"))
}

fn cells(line: &str) -> Option<Vec<&str>> {
    let line = line.trim();
    if !line.starts_with('|') || !line.ends_with('|') {
        return None;
    }
    Some(line.trim_matches('|').split('|').map(str::trim).collect())
}

fn valid_id(value: &str) -> bool {
    let Some(value) = value.strip_prefix('W') else {
        return false;
    };
    let Some((wave, task)) = value.split_once('-') else {
        return false;
    };
    !wave.is_empty()
        && wave.chars().all(|character| character.is_ascii_digit())
        && !task.is_empty()
        && task
            .chars()
            .all(|character| character.is_ascii_alphanumeric() || character == '-')
}

fn extract_ids(value: &str) -> Vec<String> {
    value
        .split(|character: char| !(character.is_ascii_alphanumeric() || character == '-'))
        .filter(|token| valid_id(token))
        .map(str::to_owned)
        .collect()
}

fn parse_dependencies(value: &str, task_id: &str, violations: &mut Vec<String>) -> Vec<String> {
    if matches!(value, "" | "无") {
        return Vec::new();
    }
    value
        .split(',')
        .map(str::trim)
        .filter(|dependency| !dependency.is_empty())
        .filter_map(|dependency| {
            let reference = dependency.strip_suffix('*').unwrap_or(dependency);
            if valid_id(reference) || (dependency.ends_with('*') && valid_prefix(reference)) {
                Some(dependency.to_owned())
            } else {
                violations.push(format!(
                    "task `{task_id}` has invalid dependency `{dependency}`"
                ));
                None
            }
        })
        .collect()
}

fn valid_prefix(value: &str) -> bool {
    let Some(value) = value.strip_prefix('W') else {
        return false;
    };
    let Some((wave, prefix)) = value.split_once('-') else {
        return false;
    };
    !wave.is_empty()
        && wave.chars().all(|character| character.is_ascii_digit())
        && !prefix.is_empty()
        && prefix
            .chars()
            .all(|character| character.is_ascii_alphanumeric() || character == '-')
}

fn validate_dependencies(
    task_id: &str,
    task: &Task,
    tasks: &BTreeMap<String, Task>,
    violations: &mut Vec<String>,
) {
    for dependency in &task.dependencies {
        let dependency_ids = dependency.strip_suffix('*').map_or_else(
            || vec![dependency.as_str()],
            |prefix| {
                tasks
                    .keys()
                    .filter(|id| id.starts_with(prefix))
                    .map(String::as_str)
                    .collect::<Vec<_>>()
            },
        );
        if dependency_ids.is_empty() {
            violations.push(format!(
                "task `{task_id}` dependency `{dependency}` matches no task"
            ));
            continue;
        }
        for dependency_id in dependency_ids {
            let Some(dependency_task) = tasks.get(dependency_id) else {
                violations.push(format!(
                    "task `{task_id}` references unknown dependency `{dependency_id}`"
                ));
                continue;
            };
            if dependency_id == task_id {
                violations.push(format!("task `{task_id}` depends on itself"));
            } else if matches!(task.status.as_str(), "IN_PROGRESS" | "DONE")
                && dependency_task.status != "DONE"
            {
                violations.push(format!(
                    "task `{task_id}` is {} while dependency `{dependency_id}` is {}",
                    task.status, dependency_task.status
                ));
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use std::{fs, path::Path};

    use super::{DESIGN_PATH, LEDGER_PATH, validate_sources};

    fn fixture(design: &str, implementation: &str, evidence: &str) -> String {
        let design_hash = blake3::hash(design.as_bytes()).to_hex();
        format!(
            r"
## 0. Current checkpoint

- `design_contract_hash`: `blake3:{design_hash}`
- `baseline_branch`: `main`
- `baseline_head`: `abc`
- `baseline_git_status`: `clean`
- `current_task_id`: `W0-02`
- `last_completed_task_id`: `W0-01`
- `last_verified_command`: `test`
- `next_recovery_command`: `test`
- `implementation_status`: `active`
- `operational_activation_claimed`: `false`

## 2. Implementation Ledger

{implementation}

## 3. Evidence Ledger

| Date | Item | Result | Evidence |
|---|---|---|---|
{evidence}

## 4. Decision Ledger

| Date | Decision | Reason | Impact |
|---|---|---|---|
| now | frozen design | drift | hash |

## 5. Blocker Ledger
"
        )
    }

    #[test]
    fn validates_current_ledger() {
        let root = Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("../..")
            .canonicalize()
            .expect("workspace root");
        let design = fs::read_to_string(root.join(DESIGN_PATH)).expect("design");
        let ledger = fs::read_to_string(root.join(LEDGER_PATH)).expect("ledger");
        let violations = validate_sources(&design, &ledger);
        assert!(violations.is_empty(), "{violations:#?}");
    }

    #[test]
    fn accepts_valid_ledger() {
        let design = "design";
        let ledger = fixture(
            design,
            r"
| ID | Status | Dependencies | Task | Evidence |
|---|---|---|---|---|
| W0-01 | DONE | 无 | docs | test |
| W0-02 | IN_PROGRESS | W0-01 | code | test |
",
            "| now | W0-01 | PASS | ok |",
        );
        assert!(validate_sources(design, &ledger).is_empty());
    }

    #[test]
    fn rejects_invalid_ledger() {
        let design = "design";
        let mut ledger = fixture(
            design,
            r"
| ID | Status | Dependencies | Task | Evidence |
|---|---|---|---|---|
| W0-01 | DONE | 无 | docs | test |
| W0-02 | IN_PROGRESS | W0-01 | code | test |
| W1-01 | DONE | W0-02 | early | test |
",
            "| now | W0-01 | PASS | ok |",
        );
        ledger = ledger.replace("blake3:", "blake3:00");
        let violations = validate_sources(design, &ledger);
        assert!(
            violations
                .iter()
                .any(|item| item.contains("design_contract_hash"))
        );
        assert!(
            violations
                .iter()
                .any(|item| item.contains("W1-01") && item.contains("PASS"))
        );
        assert!(
            violations
                .iter()
                .any(|item| item.contains("W1-01") && item.contains("dependency"))
        );
    }
}

#!/usr/bin/env bash
# Import-style lint — enforce file/module-preamble `use` discipline from
# `.cursor/rules/quant-pivot-rust-style.mdc`:
#
#   1. No deep paths in item bodies (at most one `::` after imports).
#      Forbidden:  crate::execution::ExitSignalVerdict
#                  quant_pivot_models::quant::test()
#                  std::collections::HashMap
#      Allowed:    ExitSignalVerdict / quant::test() / HashMap::new()
#   2. No `use` inside functions / block items — only module preambles
#      (file header or nested `mod tests { ... }` / other `mod` headers).
#
# Scope: crates/*/src/**/*.rs (excludes quant-pivot-macros — proc-macro
# codegen may keep fully-qualified `::std::...` paths).
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"

export LINT_IMPORT_STYLE_ROOT="$ROOT"
python3 - <<'PY'
from __future__ import annotations

import os
import re
import sys
from pathlib import Path

ROOT = Path(os.environ["LINT_IMPORT_STYLE_ROOT"])
SRC_ROOTS = sorted(ROOT.glob("crates/*/src"))
EXCLUDE_CRATES = {"quant-pivot-macros"}

# Deep paths: 2+ `::` under roots called out by the style guide.
# (External crates like `chrono::` / SeaORM `entity::Entity::find` are
# left to review — this gate matches the documented mechanical rule.)
DEEP_PATH = re.compile(
    r"""
    (?<![\w/$])
    (?P<path>
        (?:crate|super|self|std|core|alloc|quant_pivot_[a-z0-9_]+)
        (?: :: [A-Za-z_][A-Za-z0-9_]*){2,}
    )
    """,
    re.VERBOSE,
)

# Style exception: trait bounds may keep `impl std::future::Future<...>`.
FUTURE_BOUND = re.compile(r"\bimpl\s+std::future::Future\b")
# Declared macros may keep fully-qualified `std::` / `$crate::` paths.
MACRO_RULES = re.compile(r"\bmacro_rules\s*!")

USE_ITEM = re.compile(r"^\s*(?:pub\s+)?use\b")
MOD_ITEM = re.compile(
    r"^(?P<indent>\s*)(?:pub(?:\([^)]*\))?\s+)?mod\s+(?P<name>\w+)\b"
)
FN_ITEM = re.compile(
    r"\b(?:pub\s+)?(?:async\s+)?(?:unsafe\s+)?(?:const\s+)?fn\s+(?P<name>\w+)\b"
)
IMPL_OR_TRAIT = re.compile(r"\b(?:pub\s+)?(?:unsafe\s+)?(?:impl|trait)\b")
CONST_STATIC = re.compile(r"\b(?:pub\s+)?(?:const|static)\b")
TYPE_ALIAS = re.compile(r"\b(?:pub\s+)?(?:type)\b")
ATTR_LINE = re.compile(r"^\s*#!?\[")
DOC_LINE = re.compile(r"^\s*//!|^\s*///")


def iter_rs_files() -> list[Path]:
    files: list[Path] = []
    for src in SRC_ROOTS:
        crate = src.parent.name
        if crate in EXCLUDE_CRATES:
            continue
        files.extend(sorted(src.rglob("*.rs")))
    return files


def strip_strings_and_comments(line: str, in_block_comment: bool) -> tuple[str, bool]:
    """Return code-only text for brace/`use` detection; track /* */ state."""
    if in_block_comment:
        end = line.find("*/")
        if end < 0:
            return "", True
        line = line[end + 2 :]
        in_block_comment = False

    out: list[str] = []
    i = 0
    n = len(line)
    in_string = False
    in_char = False
    while i < n:
        ch = line[i]
        nxt = line[i + 1] if i + 1 < n else ""

        if not in_string and not in_char:
            if ch == "/" and nxt == "/":
                break
            if ch == "/" and nxt == "*":
                end = line.find("*/", i + 2)
                if end < 0:
                    return "".join(out), True
                i = end + 2
                continue
            if ch == '"':
                in_string = True
                out.append(" ")
                i += 1
                continue
            if ch == "'":
                # lifetime ('a) vs char ('x') — lifetimes have letter after '
                if nxt.isalpha() or nxt == "_":
                    out.append(ch)
                    i += 1
                    continue
                in_char = True
                out.append(" ")
                i += 1
                continue
            out.append(ch)
            i += 1
            continue

        if in_string:
            if ch == "\\" and i + 1 < n:
                i += 2
                continue
            if ch == '"':
                in_string = False
            i += 1
            continue

        # in_char
        if ch == "\\" and i + 1 < n:
            i += 2
            continue
        if ch == "'":
            in_char = False
        i += 1

    return "".join(out), in_block_comment


def strip_for_path_scan(line: str) -> str:
    """Remove strings, line comments, and rustdoc link targets for path matching."""
    s = re.sub(r"\[[^\]]*\]\([^)]*\)", " ", line)
    s = re.sub(
        r"\[(?:crate|super|self|std|core|alloc|quant_pivot_[a-z0-9_]+)(?:::[^\]]+)?\]",
        " ",
        s,
    )
    code, _ = strip_strings_and_comments(s, False)
    return code


class Scope:
    __slots__ = ("kind", "brace_depth", "name")

    def __init__(self, kind: str, brace_depth: int, name: str = "") -> None:
        self.kind = kind  # mod | fn | other
        self.brace_depth = brace_depth
        self.name = name


def lint_file(path: Path) -> list[str]:
    errors: list[str] = []
    lines = path.read_text(encoding="utf-8", errors="replace").splitlines()
    rel = path.relative_to(ROOT)

    brace = 0
    stack: list[Scope] = [Scope("mod", 0, "<file>")]
    pending: str | None = None  # mod | fn | other | macro
    pending_name = ""
    in_block_comment = False
    in_use = False
    use_brace_delta = 0
    use_line_start = 0

    for lineno, raw in enumerate(lines, 1):
        code, in_block_comment = strip_strings_and_comments(raw, in_block_comment)
        stripped = raw.strip()

        # Finish multi-line use tree tracking (skip deep-path scan inside use).
        if in_use:
            use_brace_delta += code.count("{") - code.count("}")
            for ch in code:
                if ch == "{":
                    brace += 1
                    kind = pending or "other"
                    stack.append(Scope(kind, brace, pending_name))
                    pending = None
                    pending_name = ""
                elif ch == "}":
                    while stack and stack[-1].brace_depth == brace:
                        stack.pop()
                    brace = max(0, brace - 1)
            if ";" in code and use_brace_delta <= 0:
                in_use = False
                use_brace_delta = 0
            continue

        if not code.strip():
            continue

        if DOC_LINE.match(raw) or ATTR_LINE.match(raw):
            continue

        # --- use items ---
        if USE_ITEM.match(raw):
            innermost = stack[-1].kind if stack else "mod"
            if innermost != "mod":
                scope = stack[-1].name or innermost
                kind = stack[-1].kind
                if kind == "fn":
                    where = f"function `{scope}`"
                else:
                    where = f"`{kind}` scope" + (f" `{scope}`" if scope else "")
                errors.append(
                    f"{rel}:{lineno}: `use` must be in a module preamble "
                    f"(file header or `mod` block), not inside {where}: "
                    f"{stripped}"
                )
            if ";" not in code:
                in_use = True
                use_brace_delta = code.count("{") - code.count("}")
                use_line_start = lineno
            continue

        # --- pending scope kind from item keywords ---
        if MACRO_RULES.search(code):
            pending = "macro"
            pending_name = "macro_rules"
        else:
            mod_m = MOD_ITEM.match(raw)
            if mod_m and ";" not in code:
                pending = "mod"
                pending_name = mod_m.group("name")
            elif FN_ITEM.search(code):
                pending = "fn"
                m = FN_ITEM.search(code)
                pending_name = m.group("name") if m else ""
            elif IMPL_OR_TRAIT.search(code) or CONST_STATIC.search(code) or TYPE_ALIAS.search(code):
                if pending is None:
                    pending = "other"
                    pending_name = ""

        # Trait / extern method stubs (`fn foo(...);`) must not leak pending.
        if pending == "fn" and ";" in code and "{" not in code:
            pending = None
            pending_name = ""

        # --- deep paths in bodies (not use / docs / attrs / macro_rules) ---
        # Declared macros keep fully-qualified paths (`std::…`, `::std::…`, `$crate::…`).
        # SeaORM entity relation impls idiomatically use `super::other::Entity`.
        in_macro = any(s.kind == "macro" for s in stack) or pending == "macro"
        in_entities = "/entities/" in str(rel).replace("\\", "/")
        looks_like_macro_body = (
            "$" in code
            or "::std::" in code
            or "::core::" in code
            or "info_from_model!" in code
        )
        if not in_macro and not in_entities and not looks_like_macro_body:
            path_scan = strip_for_path_scan(raw)
            path_scan_for_match = FUTURE_BOUND.sub(" impl Future ", path_scan)
            for m in DEEP_PATH.finditer(path_scan_for_match):
                path = m.group("path")
                errors.append(
                    f"{rel}:{lineno}: deep path `{path}` — import at module "
                    f"preamble and use ≤1 `::` in bodies "
                    f"(type short name or `module::item`): {stripped}"
                )

        # --- braces / scope stack ---
        for ch in code:
            if ch == "{":
                brace += 1
                kind = pending or "other"
                stack.append(Scope(kind, brace, pending_name))
                pending = None
                pending_name = ""
            elif ch == "}":
                while stack and stack[-1].brace_depth == brace:
                    stack.pop()
                brace = max(0, brace - 1)

    if in_use:
        errors.append(
            f"{rel}:{use_line_start}: unfinished `use` tree (missing `;`?) — lint aborted mid-file"
        )

    return errors


def main() -> int:
    all_errors: list[str] = []
    files = iter_rs_files()
    for path in files:
        all_errors.extend(lint_file(path))

    use_errs = [e for e in all_errors if "`use` must be" in e]
    path_errs = [e for e in all_errors if "deep path" in e]
    other = [e for e in all_errors if e not in use_errs and e not in path_errs]

    print("=== Checking no `use` outside module preambles ===")
    if use_errs:
        print("\n".join(use_errs))
        print(f"ERROR: {len(use_errs)} nested `use` violation(s)")
    else:
        print("ok")

    print("=== Checking no deep paths in item bodies (≤1 `::`) ===")
    if path_errs:
        print("\n".join(path_errs))
        print(f"ERROR: {len(path_errs)} deep-path violation(s)")
    else:
        print("ok")

    if other:
        print("=== Other ===")
        print("\n".join(other))

    total = len(all_errors)
    if total:
        print(f"{total} import-style violation(s) in {len(files)} files")
        return 1

    print(f"Import-style checks passed ({len(files)} files).")
    return 0


sys.exit(main())
PY

#!/usr/bin/env python3
"""Merge duplicate preamble `use` lines per root (std, oxide_arb_*, crate, etc.)."""

from __future__ import annotations

import re
import sys
from collections import defaultdict
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
CRATES = ROOT / "crates"

MERGE_ROOTS = frozenset(
    {
        "crate",
        "std",
        "core",
        "alloc",
        "tokio",
        "serde",
        "chrono",
        "flume",
        "arc_swap",
        "dashmap",
        "parking_lot",
        "num_traits",
        "futures_util",
        "tokio_util",
        "sea_orm",
        "thiserror",
        "tracing",
        "rust_decimal",
        "rust_decimal_macros",
        "wiremock",
        "proptest",
        "pretty_assertions",
        "async_trait",
        "reqwest",
        "alloy",
        "polymarket",
        "ahash",
        "rand",
        "backoff",
        "parking_lot",
        "teloxide",
        "moka",
        "hex",
        "uuid",
        "zeroize",
        "criterion",
    }
)

USE_LINE = re.compile(
    r"^(?P<indent>\s*)(?P<pub>pub(?:\s+\(crate\))?\s+)?use\s+(?P<body>.+);\s*$"
)
CODE_START = re.compile(
    r"^\s*(?:pub\s+)?(?:struct|enum|union|trait|type|const|static|async\s+fn|fn|impl|macro_rules!|extern\s+crate)\b"
)


def is_blank(line: str) -> bool:
    return not line.strip()


def is_attr(line: str) -> bool:
    return line.strip().startswith("#[") and not line.strip().startswith("#!")


def is_mod_line(line: str) -> bool:
    s = line.strip()
    return (s.startswith("mod ") or s.startswith("pub mod ")) and not s.rstrip().endswith("{")


def is_pub_use_line(line: str) -> bool:
    s = line.strip()
    return s.startswith("pub use ") or s.startswith("pub(crate) use ") or s.startswith("pub(super) use ")


def is_use_line(line: str) -> bool:
    s = line.strip()
    return s.startswith("use ") and not is_pub_use_line(line)


def collect_use_block(lines: list[str], start: int) -> tuple[list[str], int]:
    block = []
    i = start
    while i < len(lines) and is_attr(lines[i]):
        block.append(lines[i])
        i += 1
    if i >= len(lines):
        return block, i
    block.append(lines[i])
    depth = lines[i].count("{") - lines[i].count("}")
    if ";" not in lines[i] or depth > 0:
        i += 1
        while i < len(lines):
            block.append(lines[i])
            depth += lines[i].count("{") - lines[i].count("}")
            if ";" in lines[i] and depth <= 0:
                i += 1
                break
            i += 1
    else:
        i += 1
    return block, i


def use_block_to_tuple(block: list[str]) -> tuple[str, str, str] | None:
    text = "\n".join(block)
    m = re.search(r"^(\s*)(pub(?:\s+\(crate\))?\s+)?use\s+(.+);\s*$", text, re.M | re.S)
    if not m:
        return None
    body = re.sub(r"\s+", " ", m.group(3).strip())
    return m.group(1), m.group(2) or "", body


def split_top_level_commas(s: str) -> list[str]:
    parts: list[str] = []
    depth = 0
    cur: list[str] = []
    for ch in s:
        if ch == "{":
            depth += 1
        elif ch == "}":
            depth -= 1
        elif ch == "," and depth == 0:
            part = "".join(cur).strip()
            if part:
                parts.append(part)
            cur = []
            continue
        cur.append(ch)
    tail = "".join(cur).strip()
    if tail:
        parts.append(tail)
    return parts


def parse_use_body(body: str) -> tuple[str, list[str], str | None]:
    body = body.strip()
    root, _, rest = body.partition("::")
    if not rest:
        return root, [], root
    depth = 0
    brace_at = -1
    for i, ch in enumerate(rest):
        if ch == "{":
            if depth == 0:
                brace_at = i
                break
            depth += 1
        elif ch == "}":
            depth -= 1
    if brace_at >= 0:
        prefix = rest[:brace_at].rstrip(":")
        inner = rest[brace_at:]
        segs = prefix.split("::") if prefix else []
        return root, segs, inner
    segs = rest.split("::")
    if len(segs) == 1:
        return root, [], segs[0]
    return root, segs[:-1], segs[-1]


def parse_use_item(item: str) -> tuple[list[str], str | None, str | None]:
    item = item.strip().rstrip(",")
    if "{" in item:
        idx = item.index("{")
        prefix = item[:idx].rstrip(":")
        inner = item[idx:]
        segs = prefix.split("::") if prefix else []
        return segs, None, inner
    if "::" in item:
        parts = item.split("::")
        return parts[:-1], parts[-1], None
    return [], item, None


def trie_insert(trie: dict, segs: list[str], leaf: str) -> None:
    node = trie
    for s in segs:
        node = node.setdefault("children", {}).setdefault(s, {"children": {}, "leaves": set()})
    node.setdefault("leaves", set()).add(leaf)


def trie_insert_brace(trie: dict, segs: list[str], inner: str) -> None:
    for item in split_top_level_commas(inner[1:-1]):
        sub_segs, leaf, braced = parse_use_item(item)
        full = segs + sub_segs
        if braced:
            trie_insert_brace(trie, full, braced)
        elif leaf:
            trie_insert(trie, full, leaf)


def format_trie(trie: dict, indent: str) -> list[str]:
    lines: list[str] = []
    for name, child in sorted(trie.get("children", {}).items()):
        sub = format_trie(child, indent + "    ")
        if not child.get("children") and len(child.get("leaves", set())) == 1:
            leaf = next(iter(child["leaves"]))
            lines.append(f"{indent}{name}::{leaf},")
        else:
            lines.append(f"{indent}{name}::{{")
            lines.extend(sub)
            lines.append(f"{indent}}},")
    for leaf in sorted(trie.get("leaves", set())):
        lines.append(f"{indent}{leaf},")
    return lines


def format_merged_group(indent: str, pub: str, root: str, trie: dict) -> list[str]:
    pub = f"{pub} " if pub else ""
    children = trie.get("children", {})
    leaves = trie.get("leaves", set())
    if not children and len(leaves) == 1:
        leaf = next(iter(leaves))
        return [f"{indent}{pub}use {root}::{leaf};"]
    body = format_trie(trie, indent + "    ")
    return [f"{indent}{pub}use {root}::{{", *body, f"{indent}}};"]


def merge_uses(block: list[tuple[str, str, str]]) -> list[str]:
    out: list[str] = []
    groups: dict[tuple[str, str, str], dict] = defaultdict(
        lambda: {"children": {}, "leaves": set()}
    )

    for indent, pub, body in block:
        if " as " in body:
            out.append(f"{indent}{pub}use {body};")
            continue
        root, segs, leaf = parse_use_body(body)
        if root == "super":
            key = (indent, pub.strip(), root)
            trie = groups[key]
            if leaf and leaf.startswith("{"):
                trie_insert_brace(trie, segs, leaf)
            elif leaf:
                trie_insert(trie, segs, leaf)
            else:
                trie_insert(trie, segs, "")
            continue
        if root not in MERGE_ROOTS and not root.startswith("oxide_arb_"):
            out.append(f"{indent}{pub}use {body};")
            continue
        key = (indent, pub.strip(), root)
        trie = groups[key]
        if leaf and leaf.startswith("{"):
            trie_insert_brace(trie, segs, leaf)
        elif leaf:
            trie_insert(trie, segs, leaf)
        else:
            trie_insert(trie, segs, "")

    for (indent, pub, root), trie in sorted(groups.items()):
        out.extend(format_merged_group(indent, pub, root, trie))
    return out


def split_preamble(lines: list[str]) -> tuple[list[str], list[str], list[str], list[tuple[str, str, str]], list[str]]:
    header: list[str] = []
    mods: list[str] = []
    pub_uses: list[str] = []
    uses: list[tuple[str, str, str]] = []
    i = 0
    while i < len(lines) and (is_blank(lines[i]) or lines[i].strip().startswith("//") or lines[i].strip().startswith("#!")):
        header.append(lines[i])
        i += 1
    while header and is_blank(header[-1]):
        header.pop()

    while i < len(lines):
        if is_blank(lines[i]):
            i += 1
            continue
        if is_mod_line(lines[i]) or (is_attr(lines[i]) and i + 1 < len(lines) and is_mod_line(lines[i + 1])):
            block, i = collect_use_block(lines, i)
            mods.extend(block)
            continue
        if is_pub_use_line(lines[i]) or (is_attr(lines[i]) and i + 1 < len(lines) and is_pub_use_line(lines[i + 1])):
            block, i = collect_use_block(lines, i)
            pub_uses.extend(block)
            continue
        if is_use_line(lines[i]) or (is_attr(lines[i]) and i + 1 < len(lines) and is_use_line(lines[i + 1])):
            block, i = collect_use_block(lines, i)
            tup = use_block_to_tuple(block)
            if tup:
                uses.append(tup)
            continue
        if CODE_START.match(lines[i]) or is_attr(lines[i]):
            break
        break

    return header, mods, pub_uses, uses, lines[i:]


def rebuild_preamble(header, mods, pub_uses, use_lines) -> list[str]:
    sections: list[list[str]] = []
    if header:
        h = [ln for ln in header if not is_blank(ln)]
        if h:
            sections.append(h)
    if mods:
        sections.append([ln for ln in mods if not is_blank(ln)])
    if pub_uses:
        sections.append([ln for ln in pub_uses if not is_blank(ln)])
    if use_lines:
        sections.append([ln for ln in use_lines if not is_blank(ln)])

    out: list[str] = []
    for sec in sections:
        if out and not is_blank(out[-1]):
            out.append("")
        out.extend(sec)
    return out


def process_file(path: Path) -> bool:
    original = path.read_text()
    lines = original.splitlines()
    header, mods, pub_uses, use_tuples, rest = split_preamble(lines)
    if not use_tuples:
        return False
    merged = merge_uses(use_tuples)
    old_lines = [f"{i}{' ' if p else ''}use {b};" for i, p, b in use_tuples]
    if merged == old_lines:
        return False
    preamble = rebuild_preamble(header, mods, pub_uses, merged)
    if preamble and rest:
        if not is_blank(preamble[-1]):
            preamble.append("")
    while rest and is_blank(rest[0]):
        rest.pop(0)
    result = preamble + rest
    text = "\n".join(result)
    if original.endswith("\n") and not text.endswith("\n"):
        text += "\n"
    if text != original:
        path.write_text(text)
        return True
    return False


def main() -> int:
    target = (ROOT / sys.argv[1]).resolve() if len(sys.argv) > 1 else CRATES.resolve()
    paths = sorted(target.rglob("*.rs")) if target.is_dir() else [target.resolve()]
    n = 0
    for path in paths:
        if process_file(path):
            n += 1
            try:
                print(path.relative_to(ROOT))
            except ValueError:
                print(path)
    print(f"merge_preamble_uses: {n} files updated")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

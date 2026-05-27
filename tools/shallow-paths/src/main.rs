//! Shallow `std::` and `oxide_arb_*` paths via span-preserving source edits.

use proc_macro2::LineColumn;
use std::{collections::BTreeSet, fs, path::PathBuf};
use quote::ToTokens;
use syn::{
    parse_file,
    spanned::Spanned,
    visit::{self, Visit},
    Block, Item, ItemUse, Path, PathSegment, Stmt,
};

fn main() {
    let mut args: Vec<String> = std::env::args().skip(1).collect();
    let hoist = args.first().is_some_and(|a| a == "hoist");
    if hoist {
        args.remove(0);
    }
    let root = PathBuf::from(args.first().cloned().unwrap_or_else(|| "crates".to_string()));
    let mut n = 0usize;
    for entry in walkdir::WalkDir::new(&root)
        .into_iter()
        .filter_map(Result::ok)
        .filter(|e| e.path().extension().is_some_and(|x| x == "rs"))
    {
        let updated = if hoist {
            hoist_file(entry.path())
        } else {
            process_file(entry.path())
        };
        if updated {
            n += 1;
            println!("{}", entry.path().display());
        }
    }
    eprintln!(
        "shallow-paths: {n} files updated ({})",
        if hoist { "hoist" } else { "shallow" }
    );
}

fn is_shallow_root(root: &str) -> bool {
    root == "std" || root.starts_with("oxide_arb_")
}

/// Avoid importing nested `Result` (e.g. `std::fmt::Result`) which shadows `std::result::Result`.
fn should_skip_shallow(path: &Path) -> bool {
    path.segments.last().is_some_and(|s| s.ident == "Result")
}

fn process_file(path: &std::path::Path) -> bool {
    let src = fs::read_to_string(path).unwrap_or_else(|e| panic!("{}: {e}", path.display()));
    let file = parse_file(&src).unwrap_or_else(|e| panic!("{}: {e}", path.display()));

    let mut collector = ImportCollector::default();
    collector.visit_file(&file);

    let mut visitor = ShallowVisitor {
        scope: collector.idents,
        imports: BTreeSet::new(),
        replacements: Vec::new(),
        in_use: false,
        source: src.clone(),
    };
    visitor.visit_file(&file);

    if visitor.replacements.is_empty() && visitor.imports.is_empty() {
        return false;
    }

    // Drop strict-prefix overlaps (e.g. keep `std::thread::sleep(...)` not `std::thread` + `sleep`).
    let mut reps: Vec<Replacement> = visitor.replacements;
    reps.sort_by_key(|r| r.start);
    let mut filtered: Vec<Replacement> = Vec::new();
    for r in reps {
        if filtered
            .iter()
            .any(|prev| r.start >= prev.start && r.end <= prev.end)
        {
            continue;
        }
        filtered.retain(|prev| !(prev.start >= r.start && prev.end <= r.end));
        filtered.push(r);
    }

    let mut out = src.clone();
    filtered.sort_by(|a, b| b.start.cmp(&a.start));
    for r in &filtered {
        if r.start < out.len() && r.end <= out.len() && r.start < r.end {
            let slice = &out[r.start..r.end];
            if slice
                .chars()
                .all(|c| c.is_ascii_alphanumeric() || c == ':' || c == '_')
            {
                out.replace_range(r.start..r.end, &r.text);
            }
        }
    }

    if !visitor.imports.is_empty() {
        let insert_byte = find_use_insert_byte(&out);
        let mut use_lines = String::new();
        for imp in &visitor.imports {
            if !has_toplevel_use(&out, imp) {
                use_lines.push_str(&format!("use {imp};\n"));
            }
        }
        if !use_lines.is_empty() {
            out.insert_str(insert_byte, &use_lines);
        }
    }

    if out != src {
        fs::write(path, out).unwrap();
        true
    } else {
        false
    }
}

fn has_toplevel_use(src: &str, imp: &str) -> bool {
    let want = format!("use {imp};");
    for line in src.lines() {
        let t = line.trim();
        if t.starts_with("mod ")
            || t.starts_with("pub mod ")
            || t.starts_with("#[")
            || (t.starts_with("pub ")
                && (t.contains(" fn ")
                    || t.contains(" struct ")
                    || t.contains(" enum ")
                    || t.contains(" const ")
                    || t.contains(" type ")
                    || t.contains(" trait ")))
        {
            break;
        }
        if t == want || t.starts_with(&format!("use {imp} as ")) {
            return true;
        }
    }
    false
}

fn find_use_insert_byte(src: &str) -> usize {
    let mut last_use_end = 0usize;
    for (i, line) in src.lines().enumerate() {
        let trimmed = line.trim();
        if trimmed.starts_with("use ") || (trimmed == "};" && i > 0) {
            if let Some(pos) = line.find(';') {
                let line_start: usize = src.lines().take(i).map(|l| l.len() + 1).sum();
                last_use_end = line_start + pos + 1;
                if trimmed.ends_with("};") {
                    last_use_end = line_start + line.len();
                }
            }
        } else if trimmed.starts_with("pub ")
            || trimmed.starts_with("mod ")
            || trimmed.starts_with("//")
            || trimmed.starts_with("#!")
            || trimmed.starts_with("#[")
        {
            continue;
        } else if last_use_end > 0 {
            break;
        }
    }
    if last_use_end > 0 {
        return last_use_end
            + if src.as_bytes().get(last_use_end) == Some(&b'\n') {
                1
            } else {
                0
            };
    }
    // After leading comments/attrs
    for (idx, line) in src.lines().enumerate() {
        let t = line.trim();
        if !t.starts_with("//") && !t.starts_with("#!") && !t.starts_with("#[") {
            return src.lines().take(idx).map(|l| l.len() + 1).sum();
        }
    }
    0
}

struct Replacement {
    start: usize,
    end: usize,
    text: String,
}

#[derive(Default)]
struct ImportCollector {
    idents: BTreeSet<String>,
    item_depth: u32,
}

impl<'ast> Visit<'ast> for ImportCollector {
    fn visit_item(&mut self, i: &'ast syn::Item) {
        self.item_depth += 1;
        visit::visit_item(self, i);
        self.item_depth -= 1;
    }

    fn visit_item_use(&mut self, i: &'ast ItemUse) {
        if self.item_depth == 1 {
            collect_use_idents(&i.tree, &mut self.idents);
        }
        visit::visit_item_use(self, i);
    }
}

fn collect_use_idents(tree: &syn::UseTree, out: &mut BTreeSet<String>) {
    match tree {
        syn::UseTree::Path(p) => {
            if let syn::UseTree::Name(n) = &*p.tree {
                out.insert(n.ident.to_string());
            }
            collect_use_idents(&p.tree, out);
        }
        syn::UseTree::Name(n) => {
            out.insert(n.ident.to_string());
        }
        syn::UseTree::Rename(r) => {
            out.insert(r.rename.to_string());
        }
        syn::UseTree::Glob(_) => {}
        syn::UseTree::Group(g) => {
            for t in &g.items {
                collect_use_idents(t, out);
            }
        }
    }
}

#[derive(Default)]
struct ShallowVisitor {
    scope: BTreeSet<String>,
    imports: BTreeSet<String>,
    in_use: bool,
    replacements: Vec<Replacement>,
    source: String,
}

impl<'ast> Visit<'ast> for ShallowVisitor {
    fn visit_item_use(&mut self, i: &'ast ItemUse) {
        self.in_use = true;
        visit::visit_item_use(self, i);
        self.in_use = false;
    }

    fn visit_path(&mut self, path: &'ast Path) {
        if self.in_use || path.leading_colon.is_some() {
            visit::visit_path(self, path);
            return;
        }
        let Some(first) = path.segments.first() else {
            return;
        };
        let root = first.ident.to_string();
        if !is_shallow_root(&root) || path.segments.len() < 3 || should_skip_shallow(path) {
            visit::visit_path(self, path);
            return;
        }

        if let Some((import, short)) = split_for_import(path) {
            let short_s = path_to_string(&short);
            let imported_type = import.rsplit("::").next().unwrap_or(&import).to_string();
            let short_type = short.segments.first().map(|s| s.ident.to_string());
            let text = if self.scope.contains(&imported_type)
                && short_type.as_ref() == Some(&imported_type)
                && short.segments.len() == 1
            {
                imported_type.clone()
            } else if self.scope.contains(&imported_type) {
                let alias = conflict_alias(&import, &imported_type);
                self.imports.insert(format!("{import} as {alias}"));
                short_s.replacen(&format!("{imported_type}::"), &format!("{alias}::"), 1)
            } else {
                self.imports.insert(import);
                short_s
            };
            let span = path.span();
            self.replacements.push(Replacement {
                start: line_column_to_byte(&self.source, span.start()),
                end: line_column_to_byte(&self.source, span.end()),
                text,
            });
            return;
        }
        visit::visit_path(self, path);
    }
}

fn conflict_alias(import: &str, tail: &str) -> String {
    if let Some(rest) = import.strip_prefix("std::") {
        let hint = rest.split("::").next().unwrap_or("std");
        let mut h = hint.chars();
        let cap = match h.next() {
            None => String::new(),
            Some(c) => c.to_uppercase().chain(h).collect(),
        };
        return format!("Std{cap}{tail}");
    }
    if let Some(rest) = import.strip_prefix("oxide_arb_") {
        let crate_hint = rest.split("::").next().unwrap_or("ext");
        let mut h = crate_hint.chars();
        let cap = match h.next() {
            None => String::new(),
            Some(c) => c.to_uppercase().chain(h).collect(),
        };
        return format!("{cap}{tail}");
    }
    format!("Imported{tail}")
}

fn path_to_string(path: &Path) -> String {
    path.segments
        .iter()
        .map(|s| s.ident.to_string())
        .collect::<Vec<_>>()
        .join("::")
}

fn split_for_import(path: &Path) -> Option<(String, Path)> {
    let segs = &path.segments;
    if segs.len() < 3 {
        return None;
    }
    let root = segs.first()?.ident.to_string();
    if !is_shallow_root(&root) {
        return None;
    }

    if segs.len() == 3 {
        let import = segments_to_string_iter(segs.iter());
        let short = Path::from(segs.last()?.ident.clone());
        return Some((import, short));
    }

    let import = segments_to_string_iter(segs.iter().take(segs.len() - 1));
    let short = path_from_segments(segs.iter().skip(segs.len().saturating_sub(2)));
    Some((import, short))
}

fn segments_to_string_iter<'a, I>(segs: I) -> String
where
    I: Iterator<Item = &'a PathSegment>,
{
    segs.map(|s| s.ident.to_string())
        .collect::<Vec<_>>()
        .join("::")
}

fn line_column_to_byte(src: &str, lc: LineColumn) -> usize {
    let mut offset = 0usize;
    for (lineno, line) in src.lines().enumerate() {
        if lineno + 1 == lc.line {
            return offset + lc.column;
        }
        offset += line.len() + 1;
    }
    offset
}

fn path_from_segments<'a, I>(segs: I) -> Path
where
    I: Iterator<Item = &'a PathSegment>,
{
    let mut iter = segs.peekable();
    let first = iter.next().expect("non-empty short path");
    let mut out = Path::from(first.ident.clone());
    for s in iter {
        out.segments.push(s.clone());
    }
    out
}

// ── Hoist inner `use` to module scope ─────────────────────────────────────

fn format_use_item(u: &ItemUse) -> String {
    let mut s = u.to_token_stream().to_string().replace(" :: ", "::");
    if !s.ends_with(';') {
        s.push(';');
    }
    s
}

fn peel_leading_uses(block: &Block, source: &str, remove: &mut Vec<(usize, usize)>, hoisted: &mut BTreeSet<String>) {
    for stmt in &block.stmts {
        let Stmt::Item(Item::Use(u)) = stmt else {
            break;
        };
        let start = line_column_to_byte(source, stmt.span().start());
        let end = line_column_to_byte(source, stmt.span().end());
        if start < end {
            remove.push((start, end));
            hoisted.insert(format_use_item(u));
        }
    }
}

struct HoistFinder<'a> {
    source: &'a str,
    remove: Vec<(usize, usize)>,
    hoisted: BTreeSet<String>,
}

impl<'ast> Visit<'ast> for HoistFinder<'_> {
    fn visit_item_fn(&mut self, i: &'ast syn::ItemFn) {
        peel_leading_uses(&i.block, self.source, &mut self.remove, &mut self.hoisted);
        visit::visit_item_fn(self, i);
    }

    fn visit_impl_item_fn(&mut self, i: &'ast syn::ImplItemFn) {
        peel_leading_uses(&i.block, self.source, &mut self.remove, &mut self.hoisted);
        visit::visit_impl_item_fn(self, i);
    }
}

fn hoist_file(path: &std::path::Path) -> bool {
    let src = fs::read_to_string(path).unwrap_or_else(|e| panic!("{}: {e}", path.display()));
    let file = parse_file(&src).unwrap_or_else(|e| panic!("{}: {e}", path.display()));

    let mut finder = HoistFinder {
        source: &src,
        remove: Vec::new(),
        hoisted: BTreeSet::new(),
    };
    finder.visit_file(&file);

    if finder.hoisted.is_empty() {
        return false;
    }

    let mut out = src.clone();
    finder.remove.sort_by_key(|(s, _)| *s);
    for (start, end) in finder.remove.into_iter().rev() {
        if end <= out.len() && start < end {
            let mut cut = end;
            while cut < out.len() && matches!(out.as_bytes().get(cut), Some(b'\n' | b'\r')) {
                cut += 1;
            }
            out.replace_range(start..cut, "");
        }
    }

    let mut insert_lines = String::new();
    for imp in &finder.hoisted {
        let body = imp.strip_prefix("use ").unwrap_or(imp).trim_end_matches(';');
        if !has_toplevel_use(&out, body) {
            insert_lines.push_str(imp);
            insert_lines.push('\n');
        }
    }
    if !insert_lines.is_empty() {
        let insert_byte = find_use_insert_byte(&out);
        out.insert_str(insert_byte, &insert_lines);
    }

    if out != src {
        fs::write(path, out).unwrap();
        true
    } else {
        false
    }
}

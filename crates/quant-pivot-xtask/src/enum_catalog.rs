use std::{
    collections::BTreeMap,
    fs,
    io::Result as IoResult,
    mem,
    path::{Path, PathBuf},
};

use anyhow::{Context, Error as AnyhowError, Result, bail};
use proc_macro2::{Delimiter, TokenStream, TokenTree};
use serde::Serialize;
use syn::{
    LitStr, Macro, parse_file, parse_str,
    visit::{self, Visit},
};

const SCHEMA_VERSION: u32 = 1;

#[derive(Debug, Serialize)]
struct EnumCatalogSchema {
    schema_version: u32,
    source: &'static str,
    enums: Vec<EnumDefinition>,
}

#[derive(Debug, Serialize)]
struct EnumDefinition {
    name: String,
    module: String,
    kind: EnumKind,
    members: Vec<EnumMember>,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "snake_case")]
enum EnumKind {
    Postgres,
    Wire,
}

#[derive(Debug, Serialize)]
struct EnumMember {
    rust_variant: String,
    wire_value: String,
}

struct MacroCollector<'a> {
    module: &'a str,
    definitions: Vec<EnumDefinition>,
    error: Option<AnyhowError>,
}

impl<'ast> Visit<'ast> for MacroCollector<'_> {
    fn visit_macro(&mut self, node: &'ast Macro) {
        if self.error.is_some() {
            return;
        }
        let Some(name) = node
            .path
            .segments
            .last()
            .map(|segment| segment.ident.to_string())
        else {
            return;
        };
        let kind = match name.as_str() {
            "pg_enum" => EnumKind::Postgres,
            "wire_enum" => EnumKind::Wire,
            _ => {
                visit::visit_macro(self, node);
                return;
            }
        };
        match parse_enum_macro(self.module, kind, node.tokens.clone()) {
            Ok(definition) => self.definitions.push(definition),
            Err(error) => self.error = Some(error),
        }
    }
}

pub fn write_schema(output: &PathBuf) -> Result<()> {
    let enum_dir = Path::new(env!("CARGO_MANIFEST_DIR")).join("../quant-pivot-models/src/enums");
    let mut definitions = collect_definitions(&enum_dir)?;
    definitions.sort_by(|left, right| left.name.cmp(&right.name));

    let mut names = BTreeMap::new();
    for definition in &definitions {
        if let Some(existing) = names.insert(&definition.name, &definition.module) {
            bail!(
                "duplicate enum {} in modules {existing} and {}",
                definition.name,
                definition.module
            );
        }
    }

    let schema = EnumCatalogSchema {
        schema_version: SCHEMA_VERSION,
        source: "crates/quant-pivot-models/src/enums",
        enums: definitions,
    };
    let mut rendered =
        serde_json::to_string_pretty(&schema).context("render enum catalog schema")?;
    rendered.push('\n');
    fs::create_dir_all(
        output
            .parent()
            .context("enum catalog schema path has no parent")?,
    )
    .with_context(|| format!("create {}", output.display()))?;
    fs::write(output, rendered).with_context(|| format!("write {}", output.display()))?;
    println!("generated {}", output.display());
    Ok(())
}

fn collect_definitions(enum_dir: &Path) -> Result<Vec<EnumDefinition>> {
    let mut paths = fs::read_dir(enum_dir)
        .with_context(|| format!("read {}", enum_dir.display()))?
        .map(|entry| entry.map(|value| value.path()))
        .collect::<IoResult<Vec<_>>>()?;
    paths.sort();

    let mut definitions = Vec::new();
    for path in paths {
        if path.extension().and_then(|value| value.to_str()) != Some("rs")
            || path.file_name().and_then(|value| value.to_str()) == Some("mod.rs")
        {
            continue;
        }
        let module = path
            .file_stem()
            .and_then(|value| value.to_str())
            .context("enum module path is not UTF-8")?;
        let source = fs::read_to_string(&path)
            .with_context(|| format!("read enum module {}", path.display()))?;
        let syntax =
            parse_file(&source).with_context(|| format!("parse enum module {}", path.display()))?;
        let mut collector = MacroCollector {
            module,
            definitions: Vec::new(),
            error: None,
        };
        collector.visit_file(&syntax);
        if let Some(error) = collector.error {
            return Err(error).with_context(|| format!("inspect {}", path.display()));
        }
        definitions.extend(collector.definitions);
    }
    Ok(definitions)
}

fn parse_enum_macro(module: &str, kind: EnumKind, tokens: TokenStream) -> Result<EnumDefinition> {
    let tokens = tokens.into_iter().collect::<Vec<_>>();
    let enum_index = tokens
        .iter()
        .position(|token| matches!(token, TokenTree::Ident(value) if value == "enum"))
        .context("enum macro has no enum declaration")?;
    let name = match tokens.get(enum_index + 1) {
        Some(TokenTree::Ident(value)) => value.to_string(),
        _ => bail!("enum macro has no enum name"),
    };
    let body = tokens
        .iter()
        .skip(enum_index + 2)
        .find_map(|token| match token {
            TokenTree::Group(group) if group.delimiter() == Delimiter::Brace => {
                Some(group.stream())
            }
            _ => None,
        })
        .with_context(|| format!("enum {name} has no body"))?;
    let members = parse_members(&name, body)?;
    if members.is_empty() {
        bail!("enum {name} has no members");
    }
    Ok(EnumDefinition {
        name,
        module: module.to_owned(),
        kind,
        members,
    })
}

fn parse_members(enum_name: &str, body: TokenStream) -> Result<Vec<EnumMember>> {
    let mut segments = Vec::new();
    let mut current = Vec::new();
    for token in body {
        if matches!(&token, TokenTree::Punct(value) if value.as_char() == ',') {
            if !current.is_empty() {
                segments.push(mem::take(&mut current));
            }
        } else {
            current.push(token);
        }
    }
    if !current.is_empty() {
        segments.push(current);
    }

    segments
        .into_iter()
        .map(|segment| parse_member(enum_name, &segment))
        .collect()
}

fn parse_member(enum_name: &str, tokens: &[TokenTree]) -> Result<EnumMember> {
    let arrow = tokens
        .windows(2)
        .position(|pair| {
            matches!(&pair[0], TokenTree::Punct(value) if value.as_char() == '=')
                && matches!(&pair[1], TokenTree::Punct(value) if value.as_char() == '>')
        })
        .with_context(|| format!("enum {enum_name} member has no wire mapping"))?;

    let mut index = 0;
    let rust_variant = loop {
        match tokens.get(index) {
            Some(TokenTree::Punct(value)) if value.as_char() == '#' => index += 2,
            Some(TokenTree::Ident(value)) => break value.to_string(),
            Some(_) => index += 1,
            None => bail!("enum {enum_name} member has no Rust variant"),
        }
    };
    let wire_literal = match tokens.get(arrow + 2) {
        Some(TokenTree::Literal(value)) => value.to_string(),
        _ => bail!("enum {enum_name}::{rust_variant} has no string wire value"),
    };
    let wire_value = parse_str::<LitStr>(&wire_literal)
        .with_context(|| format!("parse {enum_name}::{rust_variant} wire value"))?
        .value();
    Ok(EnumMember {
        rust_variant,
        wire_value,
    })
}

#[cfg(test)]
mod tests {
    use anyhow::{Result, anyhow};
    use proc_macro2::TokenStream;

    use super::{EnumKind, parse_enum_macro};

    #[test]
    fn parses_attributes_and_discriminants() -> Result<()> {
        let tokens = r#"
            type_name = "qp_state",
            @derive(Default)
            pub enum ExampleState {
                #[default]
                Ready => "ready",
                Running = 2 => "running",
            }
        "#
        .parse::<TokenStream>()
        .map_err(|error| anyhow!(error.to_string()))?;
        let definition = parse_enum_macro("example", EnumKind::Postgres, tokens)?;
        assert_eq!(definition.name, "ExampleState");
        assert_eq!(definition.members.len(), 2);
        assert_eq!(definition.members[0].rust_variant, "Ready");
        assert_eq!(definition.members[0].wire_value, "ready");
        assert_eq!(definition.members[1].wire_value, "running");
        Ok(())
    }
}

//! Pulls every Rust sample out of the README and the user guide and writes it into a module the
//! crate compiles. A sample's top-level pieces (separated by blank lines) that parse as items stay
//! items; the rest become the body of an async function with the usual variables in scope.
//! Samples that contain "…" are templates of syntax, not code, and are skipped.

use std::fmt::Write as _;
use std::path::Path;

fn main() {
    let root = Path::new(env!("CARGO_MANIFEST_DIR")).join("../..");
    let mut files = vec![root.join("README.md")];
    let mut guide: Vec<_> =
        std::fs::read_dir(root.join("docs/guide")).unwrap().map(|entry| entry.unwrap().path()).collect();
    guide.sort();
    files.extend(guide);

    let mut out = String::new();
    for file in &files {
        println!("cargo::rerun-if-changed={}", file.display());
        let text = std::fs::read_to_string(file).unwrap();
        let stem = file.file_stem().unwrap().to_string_lossy().replace('-', "_").to_lowercase();
        // One module per document, so a later sample can use what an earlier one defined.
        writeln!(out, "/// The samples of {}.", file.file_name().unwrap().to_string_lossy()).unwrap();
        writeln!(out, "#[allow(unused, dead_code, unreachable_code, clippy::all, clippy::pedantic)]").unwrap();
        writeln!(out, "mod doc_{stem} {{\n    use crate::scaffold::*;\n").unwrap();
        for (index, sample) in samples(&text).into_iter().enumerate() {
            if sample.contains('…') {
                continue;
            }
            let (items, statements) = split(&sample);
            writeln!(out, "// sample {}\n{items}", index + 1).unwrap();
            if !statements.trim().is_empty() {
                // `definition` is a variable when a sample calls methods on it, and otherwise the
                // product's describing function.
                let definition = if statements.contains("definition.") { "mut definition: Definition, " } else { "" };
                writeln!(
                    out,
                    "async fn sample_{}(pipeline: &mut Pipeline, {definition}scheduler: downbeat_client::Client, database_url: String, date: NaiveDate) -> anyhow::Result<()> {{\n    let _ = {{\n{statements}\n    }};\n    Ok(())\n}}",
                    index + 1
                )
                .unwrap();
            }
        }
        writeln!(out, "}}\n").unwrap();
    }
    std::fs::write(Path::new(&std::env::var("OUT_DIR").unwrap()).join("samples.rs"), out).unwrap();
}

/// The bodies of the ```rust blocks of a Markdown text.
fn samples(text: &str) -> Vec<String> {
    let mut found = Vec::new();
    let mut current: Option<String> = None;
    for line in text.lines() {
        let trimmed = line.trim_start();
        match &mut current {
            None if trimmed == "```rust" => current = Some(String::new()),
            Some(_) if trimmed == "```" => found.push(current.take().unwrap()),
            Some(sample) => {
                // Samples inside list items are indented; the indentation is the list's, not Rust's.
                let indent = line.len() - trimmed.len();
                sample.push_str(if indent >= 2 && line.starts_with("  ") { &line[2..] } else { line });
                sample.push('\n');
            }
            None => {}
        }
    }
    found
}

/// Splits a sample into the pieces that are items and the pieces that are statements.
fn split(sample: &str) -> (String, String) {
    let mut items = String::new();
    let mut statements = String::new();
    for piece in sample.split("\n\n") {
        if syn::parse_file(piece).is_ok_and(|file| !file.items.is_empty()) {
            items.push_str(piece);
            items.push('\n');
        } else {
            statements.push_str(piece);
            statements.push_str(";\n");
        }
    }
    (items, statements)
}

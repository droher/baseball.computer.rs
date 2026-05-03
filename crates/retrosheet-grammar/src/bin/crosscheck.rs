//! Phase 1j cross-check binary.
//!
//! Walks a directory of Retrosheet event files (`.EV*`/`.EB*`), extracts every
//! play description (the 7th comma-separated field of each `play,` record),
//! normalizes it the way the Rust parser does (strip `#`/`!`/space, replace
//! `?` and `999`+ runs with `0`), then runs the Pest grammar against it.
//!
//! Output: total plays scanned, unique play strings, pass/fail counts, and a
//! sample of the first N distinct failures so a human can audit divergences
//! and decide whether to broaden the grammar or document a `@quirk`.
//!
//! Usage: `cargo run -p retrosheet-grammar --bin crosscheck-plays --release -- <dir>`

#![allow(clippy::expect_used)]

use std::collections::{BTreeMap, HashSet};
use std::fs;
use std::path::{Path, PathBuf};
use std::process::ExitCode;

use pest::Parser;
use retrosheet_grammar::{RetrosheetParser, Rule};

const SAMPLE_LIMIT: usize = 25;

fn main() -> ExitCode {
    let mut args = std::env::args().skip(1);
    let Some(root) = args.next() else {
        eprintln!("usage: crosscheck-plays <retrosheet-dir>");
        return ExitCode::from(2);
    };
    let root = PathBuf::from(root);
    let mut event_files = Vec::new();
    if let Err(e) = collect_event_files(&root, &mut event_files) {
        eprintln!("walk failed: {e}");
        return ExitCode::from(2);
    }
    event_files.sort();
    eprintln!(
        "scanning {} event files under {}",
        event_files.len(),
        root.display()
    );

    let mut total_plays: u64 = 0;
    let mut unique_plays: HashSet<String> = HashSet::new();
    let mut failures: BTreeMap<String, (u64, PathBuf)> = BTreeMap::new();

    for path in &event_files {
        let Ok(text) = fs::read_to_string(path) else {
            continue;
        };
        for line in text.lines() {
            let Some(play) = play_field(line) else {
                continue;
            };
            let normalized = normalize(play);
            if normalized.is_empty() {
                continue;
            }
            total_plays += 1;
            if !unique_plays.insert(normalized.clone()) {
                if let Some(entry) = failures.get_mut(&normalized) {
                    entry.0 += 1;
                }
                continue;
            }
            if RetrosheetParser::parse(Rule::play, &normalized).is_err() {
                failures.insert(normalized, (1, path.clone()));
            }
        }
    }

    let unique_count = unique_plays.len();
    let failure_count = failures.len();
    let failure_occurrences: u64 = failures.values().map(|(n, _)| n).sum();
    println!(
        "plays: total={total_plays} unique={unique_count} failures_unique={failure_count} failures_total={failure_occurrences}",
    );

    if !failures.is_empty() {
        println!("\nfirst {SAMPLE_LIMIT} distinct failures (lex order):");
        for (play, (count, path)) in failures.iter().take(SAMPLE_LIMIT) {
            println!("  {play:?} (x{count}, first seen in {})", path.display());
        }
    }

    if failures.is_empty() {
        ExitCode::SUCCESS
    } else {
        ExitCode::from(1)
    }
}

fn collect_event_files(dir: &Path, out: &mut Vec<PathBuf>) -> std::io::Result<()> {
    for entry in fs::read_dir(dir)? {
        let entry = entry?;
        let path = entry.path();
        let file_type = entry.file_type()?;
        if file_type.is_dir() {
            collect_event_files(&path, out)?;
        } else if file_type.is_file() && is_event_file(&path) {
            out.push(path);
        }
    }
    Ok(())
}

fn is_event_file(path: &Path) -> bool {
    path.extension()
        .and_then(|s| s.to_str())
        .is_some_and(|ext| {
            matches!(
                ext,
                "EVA" | "EVN" | "EVE" | "EBA" | "EBN" | "EBE" | "EDA" | "EDN" | "EDE" | "EBR"
            )
        })
}

fn play_field(line: &str) -> Option<&str> {
    let mut it = line.split(',');
    if it.next()? != "play" {
        return None;
    }
    // skip inning, team, batter, count, pitches
    for _ in 0..5 {
        it.next()?;
    }
    let rest = it.next()?;
    Some(rest.trim_end_matches(['\r', '\n']))
}

/// Mirrors the normalization in `ParsedPlay::try_from` (`src/event_file/play.rs:2258`):
/// drop `#`, `!`, and space, then collapse `?` and any run of two-or-more `9`s
/// into `0` (the parser's `UNKNOWN_FIELDER_REGEX = r"999*|\?"`).
fn normalize(raw: &str) -> String {
    let stripped: String = raw
        .chars()
        .filter(|c| !matches!(c, '#' | '!' | ' '))
        .collect();
    let mut out = String::with_capacity(stripped.len());
    let bytes = stripped.as_bytes();
    let mut i = 0;
    while i < bytes.len() {
        let c = bytes[i] as char;
        if c == '?' {
            out.push('0');
            i += 1;
            continue;
        }
        if c == '9' && i + 1 < bytes.len() && bytes[i + 1] == b'9' {
            out.push('0');
            while i < bytes.len() && bytes[i] == b'9' {
                i += 1;
            }
            continue;
        }
        out.push(c);
        i += 1;
    }
    out
}

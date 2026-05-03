//! Phase 3a corpus harness.
//!
//! Walks a directory of Retrosheet event files, splits each into per-game
//! chunks (a chunk starts with an `id,` record and runs to the next `id,`
//! or EOF), then runs `Rule::file` on each chunk.
//!
//! Output: total games, pass/fail counts, and a sample of distinct failure
//! shapes. Failure shape is `(record_tag, first_field)` derived from the
//! line that the parser pointed at — coarse but enough to cluster.

#![allow(clippy::expect_used)]

use std::collections::BTreeMap;
use std::fs;
use std::io::Write;
use std::path::{Path, PathBuf};
use std::process::ExitCode;

use pest::Parser;
use retrosheet_grammar::{QuirkKind, RetrosheetParser, Rule, scan_quirks};

const SAMPLE_LIMIT: usize = 40;

fn main() -> ExitCode {
    let mut args = std::env::args().skip(1);
    let Some(root) = args.next() else {
        eprintln!("usage: crosscheck-files <retrosheet-dir> [log-path]");
        return ExitCode::from(2);
    };
    let log_path = args.next().map(PathBuf::from);
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

    let mut total_games: u64 = 0;
    let mut total_failed: u64 = 0;
    // shape -> (count, first sample (file, game_id, line_number, line))
    let mut failures: BTreeMap<String, FailureBucket> = BTreeMap::new();
    let mut quirk_counts: BTreeMap<QuirkKind, u64> = BTreeMap::new();
    let mut total_quirks: u64 = 0;

    for path in &event_files {
        let Ok(text) = fs::read_to_string(path) else {
            continue;
        };
        for game in split_games(&text) {
            total_games += 1;
            let chunk = normalize_chunk(game.text);
            match RetrosheetParser::parse(Rule::file, &chunk) {
                Err(e) => {
                    total_failed += 1;
                    let (line_no, line) = locate_failure(&chunk, &e);
                    let shape = shape_of(&line);
                    let bucket = failures.entry(shape).or_insert_with(|| FailureBucket {
                        count: 0,
                        sample_path: path.clone(),
                        sample_game_id: game.id.to_string(),
                        sample_line_no: line_no,
                        sample_line: line.clone(),
                        sample_error: e.to_string(),
                    });
                    bucket.count += 1;
                }
                Ok(pairs) => {
                    for hit in scan_quirks(pairs) {
                        *quirk_counts.entry(hit.kind).or_insert(0) += 1;
                        total_quirks += 1;
                    }
                }
            }
        }
    }

    let pass = total_games - total_failed;
    println!(
        "games: total={total_games} pass={pass} fail={total_failed} distinct_shapes={}",
        failures.len()
    );
    println!(
        "quirks: total={total_quirks} distinct_kinds={}",
        quirk_counts.len()
    );
    for (kind, count) in &quirk_counts {
        println!("  [{count:>10}x] {}", kind.as_str());
    }

    let mut shapes_sorted: Vec<(&String, &FailureBucket)> = failures.iter().collect();
    shapes_sorted.sort_by(|a, b| b.1.count.cmp(&a.1.count));

    println!(
        "\ntop {} failure shapes by count:",
        SAMPLE_LIMIT.min(shapes_sorted.len())
    );
    for (shape, bucket) in shapes_sorted.iter().take(SAMPLE_LIMIT) {
        println!(
            "  [{:>7}x] shape={shape}  game={} file={} line {}: {:?}",
            bucket.count,
            bucket.sample_game_id,
            bucket.sample_path.display(),
            bucket.sample_line_no,
            bucket.sample_line
        );
    }

    if let Some(log_path) = log_path {
        if let Err(e) = write_log(&log_path, &shapes_sorted, total_games, pass, total_failed) {
            eprintln!("log write failed: {e}");
        } else {
            eprintln!("wrote full report to {}", log_path.display());
        }
    }

    if total_failed == 0 {
        ExitCode::SUCCESS
    } else {
        ExitCode::from(1)
    }
}

/// Append a trailing newline if missing so `Rule::file`'s final
/// `line_terminator` matches `EOI` cleanly. The grammar handles `[#! ]`
/// strips and `?`/`999+` unknown-fielder rewrites natively (see
/// `play_strip` and the `fielder` rule), so the harness no longer pre-
/// processes the play body.
fn normalize_chunk(text: &str) -> String {
    if text.ends_with('\n') {
        text.to_string()
    } else {
        let mut out = String::with_capacity(text.len() + 1);
        out.push_str(text);
        out.push('\n');
        out
    }
}

struct FailureBucket {
    count: u64,
    sample_path: PathBuf,
    sample_game_id: String,
    sample_line_no: usize,
    sample_line: String,
    sample_error: String,
}

struct GameChunk<'a> {
    id: &'a str,
    text: &'a str,
}

fn split_games(text: &str) -> Vec<GameChunk<'_>> {
    let mut out = Vec::new();
    let bytes = text.as_bytes();
    let mut starts: Vec<usize> = Vec::new();
    let mut i = 0;
    while i < bytes.len() {
        let at_line_start = i == 0 || bytes[i - 1] == b'\n';
        if at_line_start && bytes[i..].starts_with(b"id,") {
            starts.push(i);
        }
        i += 1;
    }
    for w in 0..starts.len() {
        let start = starts[w];
        let end = if w + 1 < starts.len() {
            starts[w + 1]
        } else {
            text.len()
        };
        let chunk = &text[start..end];
        let first_line = chunk.lines().next().unwrap_or("");
        let id = first_line
            .strip_prefix("id,")
            .unwrap_or("")
            .trim_end_matches(['\r', '\n']);
        out.push(GameChunk { id, text: chunk });
    }
    out
}

fn locate_failure(chunk: &str, err: &pest::error::Error<Rule>) -> (usize, String) {
    use pest::error::LineColLocation;
    let (line_no, _col) = match err.line_col {
        LineColLocation::Pos((l, c)) | LineColLocation::Span((l, c), _) => (l, c),
    };
    let line = chunk
        .lines()
        .nth(line_no.saturating_sub(1))
        .unwrap_or("")
        .to_string();
    (line_no, line)
}

/// Coarse shape: record tag + first body atom (or first 16 chars of body).
/// `play,9,0,foo,??,...,K23` -> `play|9|0|foo|??`. Good enough to cluster
/// distinct grammar gaps without leaking PII (player ids).
fn shape_of(line: &str) -> String {
    let trimmed = line.trim_end_matches(['\r', '\n']);
    let mut parts = trimmed.split(',');
    let tag = parts.next().unwrap_or("");
    let mut shape = tag.to_string();
    if tag == "play" {
        // play,INNING,SIDE,BATTER,COUNT,PITCHES,PLAY -> show structure: tag/inning/side/count/play_head
        let _inning = parts.next().unwrap_or("");
        let _side = parts.next().unwrap_or("");
        let _batter = parts.next().unwrap_or("");
        let _count = parts.next().unwrap_or("");
        let _pitches = parts.next().unwrap_or("");
        let play = parts.next().unwrap_or("");
        let head: String = play.chars().take(20).collect();
        shape.push_str("|play_head=");
        shape.push_str(&head);
    } else {
        for atom in parts.take(2) {
            shape.push('|');
            shape.push_str(atom);
        }
    }
    shape
}

fn write_log(
    path: &Path,
    shapes: &[(&String, &FailureBucket)],
    total: u64,
    pass: u64,
    fail: u64,
) -> std::io::Result<()> {
    let mut f = fs::File::create(path)?;
    writeln!(
        f,
        "games: total={total} pass={pass} fail={fail} distinct_shapes={}",
        shapes.len()
    )?;
    writeln!(f)?;
    for (shape, bucket) in shapes {
        writeln!(
            f,
            "[{:>7}x] shape={shape}\n  game={} file={} line {}\n  line: {}\n  error: {}\n",
            bucket.count,
            bucket.sample_game_id,
            bucket.sample_path.display(),
            bucket.sample_line_no,
            bucket.sample_line,
            bucket.sample_error.lines().next().unwrap_or(""),
        )?;
    }
    Ok(())
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

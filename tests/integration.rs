//! End-to-end snapshot tests over curated Retrosheet fixtures.
//!
//! Approach: invoke the compiled binary against `tests/fixtures/`, then assert
//! schema-level invariants on the resulting CSV / JSONL output. We don't pin
//! exact byte-for-byte snapshots because the parser is config-driven (the set
//! of `EventFileSchema` variants drives output filenames); instead each test
//! checks structural relationships that must hold for any valid run.
//!
//! Fixtures cover six account-type / game-type combinations:
//!   - 2024AS.EVE      — modern All-Star Game (PlayByPlay)
//!   - 2014ALWC.EVE    — Wild Card postseason (PlayByPlay)
//!   - 1959.EDA        — deduced play-by-play
//!   - 1948_DET.EBA    — single box-score game
//!   - 1914_BSN.EVN    — BSN191409102 PBP, exercises bogus `presadj` skip path
//!   - 1948_BIR.EBR    — BIR194806210 box, exercises trailing-space `info` value
//!   - 2023ALW1.EVE    — MIN202310030 Wild Card (deep substitution chain, 17 subs)
//!   - 2023NLW1.EVE    — Wild Card with 14 subs and 2 `com,...` records interleaved
//!     between subs and plays
//!
//! `csv_snapshots_are_stable` canonicalizes every output file (sort data rows
//! lexicographically; preserve header) and compares against committed snapshots
//! in `tests/snapshots/all_fixtures/`. Run with `BLESS=1 cargo test` to regenerate.

use std::collections::HashMap;
use std::fs;
use std::path::{Path, PathBuf};
use std::process::Command;

use tempfile::TempDir;

fn fixtures_dir() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures")
}

fn binary_path() -> PathBuf {
    PathBuf::from(env!("CARGO_BIN_EXE_baseball-computer"))
}

fn run_parser(json: bool) -> TempDir {
    let out = TempDir::new().expect("create tempdir");
    let mut cmd = Command::new(binary_path());
    cmd.arg("-i").arg(fixtures_dir());
    cmd.arg("-o").arg(out.path());
    if json {
        cmd.arg("--json");
    }
    let status = cmd.status().expect("spawn binary");
    assert!(status.success(), "binary exited with {:?}", status);
    out
}

fn read_csv_rows(path: &Path) -> Vec<csv::StringRecord> {
    let mut reader = csv::ReaderBuilder::new()
        .has_headers(true)
        .flexible(false)
        .from_path(path)
        .unwrap_or_else(|e| panic!("failed to open {}: {e}", path.display()));
    reader
        .records()
        .collect::<Result<Vec<_>, _>>()
        .unwrap_or_else(|e| panic!("malformed CSV row in {}: {e}", path.display()))
}

fn count_lines(path: &Path) -> usize {
    fs::read_to_string(path).unwrap().lines().count()
}

#[test]
fn parser_runs_clean_against_fixtures() {
    let _ = run_parser(false);
}

#[test]
fn json_mode_writes_one_line_per_play_by_play_game() {
    let out = run_parser(true);
    let path = out.path().join("games.jsonl");
    assert!(path.exists(), "games.jsonl missing");
    // JSON mode bypasses CSV and writes every game — PBP and box-score alike —
    // to games.jsonl. 6 fixtures × 1 game each = 6 lines.
    let lines = count_lines(&path);
    assert_eq!(
        lines, 8,
        "expected one JSONL line per fixture game, got {lines}"
    );
}

#[test]
fn json_lines_are_valid_json_with_required_keys() {
    let out = run_parser(true);
    let body = fs::read_to_string(out.path().join("games.jsonl")).unwrap();
    for line in body.lines() {
        let v: serde_json::Value = serde_json::from_str(line).expect("invalid JSON line");
        assert!(v.get("id").is_some(), "missing id in {line}");
        assert!(v.get("teams").is_some(), "missing teams in {line}");
        assert!(
            v.get("events").is_some() || v.get("box_score_data").is_some(),
            "expected events or box_score_data in {line}"
        );
    }
}

#[test]
fn games_csv_has_one_row_per_pbp_fixture() {
    let out = run_parser(false);
    let games = read_csv_rows(&out.path().join("games.csv"));
    // PBP fixtures: allstar, postseason 2014, deduced 1959, 1914_BSN, plus
    // 2023ALW1 + 2023NLW1 (sub-chain & comment fixtures). Box score fixtures
    // (1948_DET, 1948_BIR) go to box_score_games.csv.
    assert_eq!(
        games.len(),
        6,
        "games.csv should have 6 rows, got {}",
        games.len()
    );
    let box_games = read_csv_rows(&out.path().join("box_score_games.csv"));
    assert_eq!(box_games.len(), 2);
}

#[test]
fn dead_ball_fixtures_parse_resiliently() {
    // 1914_BSN.EVN exercises a bogus `presadj` referencing an empty base —
    // parser must skip with a warn instead of bailing the whole game.
    // 1948_BIR.EBR has `info,hometeam,BIR ` (trailing space) — info parser
    // must trim and accept the value.
    let out = run_parser(false);
    let pbp_games = read_csv_rows(&out.path().join("games.csv"));
    let pbp_idx = pbp_games
        .iter()
        .position(|r| r.iter().any(|v| v == "BSN191409102"));
    assert!(
        pbp_idx.is_some(),
        "BSN191409102 missing from games.csv — parser likely bailed on bogus presadj"
    );
    let box_games = read_csv_rows(&out.path().join("box_score_games.csv"));
    let bir_idx = box_games
        .iter()
        .position(|r| r.iter().any(|v| v == "BIR194806210"));
    assert!(
        bir_idx.is_some(),
        "BIR194806210 missing from box_score_games.csv — info-value trim regression"
    );
    // Confirm the trimmed home team really stuck (i.e. the row carries `BIR`,
    // not whitespace-padded garbage).
    let bir = &box_games[bir_idx.expect("bir row index")];
    assert!(
        bir.iter().any(|v| v == "BIR"),
        "BIR194806210 row missing the trimmed `BIR` home team value: {:?}",
        bir
    );
}

#[test]
fn every_event_belongs_to_one_of_the_three_pbp_games() {
    let out = run_parser(false);
    let games = read_csv_rows(&out.path().join("games.csv"));
    let game_id_idx = {
        let mut r = csv::ReaderBuilder::new()
            .from_path(out.path().join("games.csv"))
            .unwrap();
        r.headers()
            .unwrap()
            .iter()
            .position(|h| h == "game_id")
            .expect("game_id column")
    };
    let game_ids: std::collections::HashSet<String> =
        games.iter().map(|r| r[game_id_idx].to_string()).collect();

    let events = read_csv_rows(&out.path().join("events.csv"));
    let event_game_id_idx = {
        let mut r = csv::ReaderBuilder::new()
            .from_path(out.path().join("events.csv"))
            .unwrap();
        r.headers()
            .unwrap()
            .iter()
            .position(|h| h == "game_id")
            .expect("game_id column")
    };
    for e in &events {
        assert!(
            game_ids.contains(&e[event_game_id_idx]),
            "event references unknown game {}",
            &e[event_game_id_idx]
        );
    }
    assert!(!events.is_empty(), "events.csv unexpectedly empty");
}

#[test]
fn lineup_appearances_have_nine_or_more_rows_per_game() {
    let out = run_parser(false);
    let lineup = read_csv_rows(&out.path().join("game_lineup_appearances.csv"));
    let game_id_idx = {
        let mut r = csv::ReaderBuilder::new()
            .from_path(out.path().join("game_lineup_appearances.csv"))
            .unwrap();
        r.headers()
            .unwrap()
            .iter()
            .position(|h| h == "game_id")
            .expect("game_id column")
    };
    let mut per_game: HashMap<String, usize> = HashMap::new();
    for row in &lineup {
        *per_game.entry(row[game_id_idx].to_string()).or_insert(0) += 1;
    }
    assert!(!per_game.is_empty());
    for (gid, n) in per_game {
        // Two teams × at least nine batters = 18 rows minimum, more with subs.
        assert!(n >= 18, "game {gid} only has {n} lineup appearances");
    }
}

#[test]
fn event_keys_are_unique_across_runs() {
    let out = run_parser(false);
    let events = read_csv_rows(&out.path().join("events.csv"));
    let event_key_idx = {
        let mut r = csv::ReaderBuilder::new()
            .from_path(out.path().join("events.csv"))
            .unwrap();
        r.headers()
            .unwrap()
            .iter()
            .position(|h| h == "event_key")
            .expect("event_key column")
    };
    let mut seen = std::collections::HashSet::new();
    for row in &events {
        let key = &row[event_key_idx];
        assert!(seen.insert(key.to_string()), "duplicate event_key {key}");
    }
}

#[test]
fn box_score_fixture_produces_expected_files() {
    let out = run_parser(false);
    // Box-score fixture is a single 1948 game, so we expect at least the games row,
    // batting lines, fielding lines, and pitching lines.
    for f in &[
        "box_score_games.csv",
        "box_score_batting_lines.csv",
        "box_score_pitching_lines.csv",
        "box_score_fielding_lines.csv",
    ] {
        let path = out.path().join(f);
        assert!(path.exists(), "{f} missing");
        let n = count_lines(&path);
        assert!(n >= 2, "{f} has only {n} line(s) (header counts as 1)");
    }
}

/// Canonicalize output: sort data rows lexicographically, keep the header line
/// in place. JSON-mode `games.jsonl` lines are sorted directly. Returns the
/// canonical string, or `None` if the file is empty (no header / no body).
fn canonicalize(path: &Path) -> String {
    let body = fs::read_to_string(path).unwrap_or_else(|e| panic!("read {}: {e}", path.display()));
    let mut lines: Vec<&str> = body.lines().collect();
    let is_jsonl = path.extension().and_then(|s| s.to_str()) == Some("jsonl");
    if is_jsonl || lines.is_empty() {
        lines.sort_unstable();
    } else {
        // Preserve header (line 0); sort the rest.
        let (head, tail) = lines.split_at_mut(1);
        tail.sort_unstable();
        lines = head.iter().chain(tail.iter()).copied().collect();
    }
    let mut out = lines.join("\n");
    if !out.is_empty() {
        out.push('\n');
    }
    out
}

fn snapshot_dir() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/snapshots/all_fixtures")
}

fn collect_output_files(dir: &Path) -> Vec<PathBuf> {
    let mut paths: Vec<PathBuf> = fs::read_dir(dir)
        .unwrap()
        .filter_map(|e| e.ok().map(|e| e.path()))
        .filter(|p| {
            matches!(
                p.extension().and_then(|s| s.to_str()),
                Some("csv") | Some("jsonl")
            )
        })
        .collect();
    paths.sort();
    paths
}

#[test]
fn csv_snapshots_are_stable() {
    let out = run_parser(false);
    let snap_dir = snapshot_dir();
    let bless = std::env::var_os("BLESS").is_some();
    if bless {
        // Wipe-and-rewrite so removed schemas don't leave stale snapshots.
        if snap_dir.exists() {
            fs::remove_dir_all(&snap_dir).unwrap();
        }
        fs::create_dir_all(&snap_dir).unwrap();
    }
    let mut missing = vec![];
    let mut diffs = vec![];
    for path in collect_output_files(out.path()) {
        let name = path.file_name().unwrap().to_str().unwrap();
        let canonical = canonicalize(&path);
        let snap_path = snap_dir.join(format!("{name}.sorted"));
        if bless {
            fs::write(&snap_path, &canonical).unwrap();
            continue;
        }
        if !snap_path.exists() {
            missing.push(name.to_string());
            continue;
        }
        let expected = fs::read_to_string(&snap_path).unwrap();
        if expected != canonical {
            diffs.push(name.to_string());
        }
    }
    assert!(
        missing.is_empty(),
        "missing snapshots (run `BLESS=1 cargo test csv_snapshots_are_stable` to create): {missing:?}"
    );
    assert!(
        diffs.is_empty(),
        "snapshot diff in {diffs:?}; run `BLESS=1 cargo test csv_snapshots_are_stable` to update if intentional"
    );
}

// `games.jsonl` is intentionally not snapshotted: it preserves vec field
// ordering (e.g. `out_on_play: ["Second","Batter"]`) which currently varies
// across runs because some upstream HashMap-backed iterations are non-stable.
// The CSV path doesn't surface those vecs (only their lengths), so CSV
// snapshots stay stable. `json_lines_are_valid_json_with_required_keys` still
// guards the JSONL structure.

#[test]
fn rerun_is_deterministic_for_top_level_row_counts() {
    // Two consecutive runs over the same fixture set must produce the same number
    // of rows in each schema. Ordering may differ due to rayon, so we don't compare
    // bytes — just row counts.
    let a = run_parser(false);
    let b = run_parser(false);

    for f in [
        "games.csv",
        "events.csv",
        "game_lineup_appearances.csv",
        "game_fielding_appearances.csv",
        "box_score_games.csv",
    ] {
        let pa = a.path().join(f);
        let pb = b.path().join(f);
        assert!(pa.exists(), "{f} missing in run A");
        assert!(pb.exists(), "{f} missing in run B");
        assert_eq!(
            count_lines(&pa),
            count_lines(&pb),
            "{f} row count is non-deterministic"
        );
    }
}

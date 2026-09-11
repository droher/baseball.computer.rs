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
use std::process::ExitStatus;
use std::thread;
use std::time::{Duration, Instant};

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

fn run_scorer_fixture(records: &str, json: bool) -> TempDir {
    let input = TempDir::new().unwrap();
    let original = include_str!("fixtures/events/2024AS.EVE");
    let replacement = original.replace("info,oscorer,wells701", records);
    assert_ne!(replacement, original);
    fs::write(input.path().join("2024AS.EVE"), replacement).unwrap();
    let output = TempDir::new().unwrap();
    let mut command = Command::new(binary_path());
    command
        .arg("-i")
        .arg(input.path())
        .arg("-o")
        .arg(output.path());
    if json {
        command.arg("--json");
    }
    assert!(command.status().unwrap().success());
    output
}

fn run_duplicate_fixture(files: &[(&str, String)], threads: &str) -> TempDir {
    let input = TempDir::new().unwrap();
    for (name, contents) in files {
        fs::write(input.path().join(name), contents).unwrap();
    }
    let output = TempDir::new().unwrap();
    let mut command = Command::new(binary_path());
    command
        .env("RAYON_NUM_THREADS", threads)
        .arg("-i")
        .arg(input.path())
        .arg("-o")
        .arg(output.path());
    let status = command_status_with_timeout(&mut command);
    assert!(status.success());
    output
}

fn command_status_with_timeout(command: &mut Command) -> ExitStatus {
    let mut child = command.spawn().unwrap();
    let deadline = Instant::now() + Duration::from_secs(5);
    loop {
        if let Some(status) = child.try_wait().unwrap() {
            return status;
        }
        if Instant::now() >= deadline {
            child.kill().unwrap();
            panic!("parser exceeded duplicate fixture timeout");
        }
        thread::sleep(Duration::from_millis(10));
    }
}

fn official_scorer(output: &Path) -> String {
    let mut reader = csv::Reader::from_path(output.join("games.csv")).unwrap();
    let rows = reader
        .deserialize::<HashMap<String, String>>()
        .collect::<Result<Vec<_>, _>>()
        .unwrap();
    assert_eq!(rows.len(), 1);
    rows[0]["official_scorer"].clone()
}

#[test]
fn duplicate_selection_uses_lexical_file_then_in_file_order_for_all_thread_counts() {
    let original = include_str!("fixtures/events/2024AS.EVE");
    let lexical_first = original.replace("info,oscorer,wells701", "info,oscorer,first001");
    let scheduled_first = format!(
        "{}{}",
        original.replace("info,oscorer,wells701", "info,oscorer,later001"),
        "com,padding\r\n".repeat(100)
    );
    let within_file_later = original.replace("info,oscorer,wells701", "info,oscorer,later002");
    let within_file = format!("{lexical_first}\r\n{within_file_later}");

    for threads in ["1", "2", "4"] {
        let cross_file = run_duplicate_fixture(
            &[
                ("2024AAA.EVE", lexical_first.clone()),
                ("2024ZZZ.EVE", scheduled_first.clone()),
            ],
            threads,
        );
        assert_eq!(official_scorer(cross_file.path()), "first001");

        let same_file = run_duplicate_fixture(&[("2024DUP.EVE", within_file.clone())], threads);
        assert_eq!(official_scorer(same_file.path()), "first001");
    }
}

#[test]
fn duplicate_selection_fails_closed_without_overriding_account_precedence() {
    let original = include_str!("fixtures/events/2024AS.EVE");
    let malformed = original.replace("info,date,2024/07/16", "info,date,invalid");
    assert_ne!(malformed, original);
    let duplicate_input = TempDir::new().unwrap();
    fs::write(
        duplicate_input.path().join("2024DUP.EVE"),
        format!("{malformed}\r\n{original}"),
    )
    .unwrap();
    let duplicate_output = TempDir::new().unwrap();
    let mut duplicate_command = Command::new(binary_path());
    duplicate_command
        .arg("-i")
        .arg(duplicate_input.path())
        .arg("-o")
        .arg(duplicate_output.path());
    assert!(!command_status_with_timeout(&mut duplicate_command).success());

    let invalid_context = original.replace("info,visteam,NLS\r\n", "");
    assert_ne!(invalid_context, original);
    let context_input = TempDir::new().unwrap();
    fs::write(
        context_input.path().join("2024DUP.EVE"),
        format!("{invalid_context}\r\n{original}"),
    )
    .unwrap();
    let context_output = TempDir::new().unwrap();
    let mut context_command = Command::new(binary_path());
    context_command
        .arg("-i")
        .arg(context_input.path())
        .arg("-o")
        .arg(context_output.path());
    assert!(!command_status_with_timeout(&mut context_command).success());

    let precedence_output = run_duplicate_fixture(
        &[
            ("2024AAA.EVE", original.to_owned()),
            ("2024.EDA", format!("{malformed}\r\n{original}")),
        ],
        "4",
    );
    assert_eq!(official_scorer(precedence_output.path()), "wells701");

    let context_precedence_output = run_duplicate_fixture(
        &[
            ("2024AAA.EVE", original.to_owned()),
            ("2024.EDA", format!("{invalid_context}\r\n{original}")),
        ],
        "4",
    );
    assert_eq!(
        official_scorer(context_precedence_output.path()),
        "wells701"
    );

    let box_score = include_str!("fixtures/events/1948_DET.EBA");
    let shared_id_pbp = original.replace("ALS202407160", "DET194806220");
    let namespace_output = run_duplicate_fixture(
        &[
            ("2024AAA.EVE", shared_id_pbp),
            ("1948_DET.EBA", box_score.to_owned()),
        ],
        "4",
    );
    assert_eq!(
        read_csv_rows(&namespace_output.path().join("games.csv")).len(),
        1
    );
    assert_eq!(
        read_csv_rows(&namespace_output.path().join("box_score_games.csv")).len(),
        1
    );
}

#[test]
fn scorer_provenance_is_key_specific_in_csv_and_json() {
    let source = "Administrative scorer label, retained beyond sixteen bytes";
    for records in [
        format!("info,scorer,\"{source}\"\r\ninfo,oscorer,officl01"),
        format!("info,oscorer,officl01\r\ninfo,scorer,\"{source}\""),
    ] {
        let csv_output = run_scorer_fixture(&records, false);
        let mut reader = csv::Reader::from_path(csv_output.path().join("games.csv")).unwrap();
        let headers = reader.headers().unwrap().clone();
        let scorer_index = headers.iter().position(|value| value == "scorer").unwrap();
        assert_eq!(headers.get(scorer_index + 1), Some("official_scorer"));
        assert_eq!(headers.get(scorer_index + 2), Some("source_scorer"));
        let row = reader
            .deserialize::<HashMap<String, String>>()
            .next()
            .unwrap()
            .unwrap();
        assert_eq!(
            row.get("official_scorer").map(String::as_str),
            Some("officl01")
        );
        assert_eq!(row.get("source_scorer").map(String::as_str), Some(source));

        let json_output = run_scorer_fixture(&records, true);
        let text = fs::read_to_string(json_output.path().join("games.jsonl")).unwrap();
        let game: serde_json::Value = serde_json::from_str(text.trim()).unwrap();
        assert_eq!(game["metadata"]["official_scorer"], "officl01");
        assert_eq!(game["metadata"]["source_scorer"], source);
    }
}

#[test]
fn real_fixtures_do_not_cross_fill_scorer_origins() {
    let csv_output = run_parser(false);
    let mut reader = csv::Reader::from_path(csv_output.path().join("games.csv")).unwrap();
    let rows = reader
        .deserialize::<HashMap<String, String>>()
        .collect::<Result<Vec<_>, _>>()
        .unwrap();
    let official_only = rows
        .iter()
        .find(|row| row["game_id"] == "ALS202407160")
        .unwrap();
    assert_eq!(official_only["official_scorer"], "wells701");
    assert_eq!(official_only["source_scorer"], "");
    let source_only = rows
        .iter()
        .find(|row| row["game_id"] == "KC1195906030")
        .unwrap();
    assert_eq!(source_only["official_scorer"], "");
    assert_eq!(source_only["source_scorer"], "71,215");
}

#[test]
fn original_retrosheet_games_preserve_missing_prefixes_and_deduplicate_runner_plays() {
    let out = TempDir::new().unwrap();
    let status = Command::new(binary_path())
        .arg("-i")
        .arg(Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/pitch_history"))
        .arg("-o")
        .arg(out.path())
        .status()
        .unwrap();
    assert!(status.success());
    let mut reader = csv::Reader::from_path(out.path().join("event_pitch_sequences.csv")).unwrap();
    let headers = reader.headers().unwrap().clone();
    let column = |name| headers.iter().position(|header| header == name).unwrap();
    let rows = reader.records().collect::<Result<Vec<_>, _>>().unwrap();
    for (game, event, expected) in [
        (
            "ANA202304070",
            "22",
            vec![
                "Ball",
                "CalledStrike",
                "Ball",
                "SwingingStrike",
                "Ball",
                "FoulTip",
            ],
        ),
        (
            "ANA202504040",
            "12",
            vec!["Foul", "SwingingStrike", "SwingingStrike"],
        ),
        (
            "ANA202304070",
            "34",
            vec!["CalledStrike", "Foul", "Ball", "Foul", "Ball", "Ball"],
        ),
        ("ANA202304070", "35", vec!["Ball"]),
    ] {
        let pitches = rows
            .iter()
            .filter(|row| &row[column("game_id")] == game && &row[column("event_id")] == event)
            .collect::<Vec<_>>();
        assert_eq!(
            pitches
                .iter()
                .map(|row| &row[column("sequence_item")])
                .collect::<Vec<_>>(),
            expected,
            "{game} event {event}"
        );
        for (index, row) in pitches.iter().enumerate() {
            assert_eq!(
                row[column("sequence_id")].parse::<usize>().unwrap(),
                index + 1
            );
        }
    }
}

#[test]
fn unreviewed_pitch_history_conflicts_require_audit_mode_and_preserve_game_evidence() {
    let input = TempDir::new().unwrap();
    let original = include_str!("pitch_history/2023ANA.EVA");
    let conflicted = original.replace("CFBF*B>B.B,W", "CFBS*B>B.B,W");
    assert_ne!(conflicted, original);
    fs::write(input.path().join("2023ANA.EVA"), conflicted).unwrap();

    for json in [false, true] {
        let out = TempDir::new().unwrap();
        let mut command = Command::new(binary_path());
        command
            .arg("-i")
            .arg(input.path())
            .arg("-o")
            .arg(out.path());
        if json {
            command.arg("--json");
        }
        let result = command.output().unwrap();
        assert!(!result.status.success());
        let message = String::from_utf8_lossy(&result.stderr);
        assert!(message.contains("ANA202304070"), "{message}");
        assert!(message.contains("event 35"), "{message}");
        assert!(
            message.contains("Unreviewed pitch sequence conflict"),
            "{message}"
        );
    }

    let csv_out = TempDir::new().unwrap();
    let csv_status = Command::new(binary_path())
        .arg("-i")
        .arg(input.path())
        .arg("-o")
        .arg(csv_out.path())
        .arg("--audit-pitch-conflicts")
        .status()
        .unwrap();
    assert!(csv_status.success());

    let games = read_csv_rows(&csv_out.path().join("games.csv"));
    assert_eq!(games.len(), 1);
    let statuses = read_csv_rows(&csv_out.path().join("event_pitch_sequence_status.csv"));
    let mut status_reader =
        csv::Reader::from_path(csv_out.path().join("event_pitch_sequence_status.csv")).unwrap();
    let status_headers = status_reader.headers().unwrap().clone();
    let status_column = |name| {
        status_headers
            .iter()
            .position(|header| header == name)
            .unwrap()
    };
    let conflicting = statuses
        .iter()
        .filter(|row| {
            matches!(
                &row[status_column("raw_pitch_sequence")],
                "CFBF*B>B" | "CFBS*B>B.B"
            )
        })
        .collect::<Vec<_>>();
    assert_eq!(conflicting.len(), 2);
    assert!(
        conflicting
            .iter()
            .all(|row| &row[status_column("status")] == "Unresolved")
    );
    assert_eq!(
        conflicting[0][status_column("appearance_start_event_id")],
        conflicting[1][status_column("appearance_start_event_id")]
    );

    let sequences = read_csv_rows(&csv_out.path().join("event_pitch_sequences.csv"));
    let mut sequence_reader =
        csv::Reader::from_path(csv_out.path().join("event_pitch_sequences.csv")).unwrap();
    let sequence_headers = sequence_reader.headers().unwrap().clone();
    let sequence_column = |name| {
        sequence_headers
            .iter()
            .position(|header| header == name)
            .unwrap()
    };
    for status in &conflicting {
        assert!(sequences.iter().all(|sequence| {
            sequence[sequence_column("event_id")] != status[status_column("event_id")]
        }));
    }

    let issues = read_csv_rows(&csv_out.path().join("event_pitch_sequence_issues.csv"));
    let mut issue_reader =
        csv::Reader::from_path(csv_out.path().join("event_pitch_sequence_issues.csv")).unwrap();
    let issue_headers = issue_reader.headers().unwrap().clone();
    let issue_column = |name| {
        issue_headers
            .iter()
            .position(|header| header == name)
            .unwrap()
    };
    assert_eq!(issues.len(), 1);
    assert_eq!(issues[0].get(issue_column("reason")), Some("TokenMismatch"));
    assert_eq!(
        issues[0].get(issue_column("prior_raw_pitch_sequence")),
        Some("CFBF*B>B")
    );
    assert_eq!(
        issues[0].get(issue_column("current_raw_pitch_sequence")),
        Some("CFBS*B>B.B")
    );

    let next_appearance = statuses
        .iter()
        .find(|row| row.get(status_column("raw_pitch_sequence")) == Some("F1X"))
        .unwrap();
    assert_eq!(
        next_appearance.get(status_column("status")),
        Some("Resolved")
    );
    assert!(sequences.iter().any(|sequence| {
        sequence[sequence_column("event_id")] == next_appearance[status_column("event_id")]
    }));

    let json_out = TempDir::new().unwrap();
    let json_status = Command::new(binary_path())
        .arg("-i")
        .arg(input.path())
        .arg("-o")
        .arg(json_out.path())
        .arg("--audit-pitch-conflicts")
        .arg("--json")
        .status()
        .unwrap();
    assert!(json_status.success());
    let json = fs::read_to_string(json_out.path().join("games.jsonl")).unwrap();
    let game: serde_json::Value = serde_json::from_str(json.trim()).unwrap();
    let events = game["events"].as_array().unwrap();
    let conflicting_events = events
        .iter()
        .filter(|event| {
            matches!(
                event["raw_pitch_sequence"].as_str(),
                Some("CFBF*B>B") | Some("CFBS*B>B.B")
            )
        })
        .collect::<Vec<_>>();
    assert_eq!(conflicting_events.len(), 2);
    assert!(conflicting_events.iter().all(|event| {
        event["results"]["pitch_sequence_status"] == "Unresolved"
            && event["results"]["pitch_sequence"] == serde_json::json!([])
    }));
    let next_event = events
        .iter()
        .find(|event| event["raw_pitch_sequence"] == "F1X")
        .unwrap();
    assert_eq!(next_event["results"]["pitch_sequence_status"], "Resolved");
    assert!(
        !next_event["results"]["pitch_sequence"]
            .as_array()
            .unwrap()
            .is_empty()
    );
}

#[test]
fn reviewed_historical_pickoff_conflict_exports_quarantined_appearance() {
    let input = Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/pitch_conflicts");
    let csv_out = TempDir::new().unwrap();
    let csv_status = Command::new(binary_path())
        .arg("-i")
        .arg(&input)
        .arg("-o")
        .arg(csv_out.path())
        .status()
        .unwrap();
    assert!(csv_status.success());

    let games = read_csv_rows(&csv_out.path().join("games.csv"));
    assert_eq!(games.len(), 1);
    assert!(games[0].iter().any(|value| value == "ATL199708270"));

    let statuses = read_csv_rows(&csv_out.path().join("event_pitch_sequence_status.csv"));
    let mut status_reader =
        csv::Reader::from_path(csv_out.path().join("event_pitch_sequence_status.csv")).unwrap();
    let status_headers = status_reader.headers().unwrap().clone();
    let status_column = |name| {
        status_headers
            .iter()
            .position(|header| header == name)
            .unwrap()
    };
    let quarantined = statuses
        .iter()
        .filter(|row| matches!(row.get(status_column("event_id")), Some("46" | "47")))
        .collect::<Vec<_>>();
    assert_eq!(quarantined.len(), 2);
    assert!(quarantined.iter().all(|row| {
        row.get(status_column("status")) == Some("Unresolved")
            && row.get(status_column("appearance_start_event_id")) == Some("46")
    }));
    assert_eq!(
        quarantined[0].get(status_column("raw_pitch_sequence")),
        Some("1SPBS+1")
    );
    assert_eq!(
        quarantined[1].get(status_column("raw_pitch_sequence")),
        Some("1SPBS+2.X")
    );

    let sequences = read_csv_rows(&csv_out.path().join("event_pitch_sequences.csv"));
    let mut sequence_reader =
        csv::Reader::from_path(csv_out.path().join("event_pitch_sequences.csv")).unwrap();
    let sequence_headers = sequence_reader.headers().unwrap().clone();
    let sequence_column = |name| {
        sequence_headers
            .iter()
            .position(|header| header == name)
            .unwrap()
    };
    assert!(
        sequences
            .iter()
            .all(|row| { !matches!(row.get(sequence_column("event_id")), Some("46" | "47")) })
    );
    assert!(
        sequences
            .iter()
            .any(|row| row.get(sequence_column("event_id")) == Some("48"))
    );

    let issues = read_csv_rows(&csv_out.path().join("event_pitch_sequence_issues.csv"));
    assert_eq!(issues.len(), 1);
    let mut issue_reader =
        csv::Reader::from_path(csv_out.path().join("event_pitch_sequence_issues.csv")).unwrap();
    let issue_headers = issue_reader.headers().unwrap().clone();
    let issue_column = |name| {
        issue_headers
            .iter()
            .position(|header| header == name)
            .unwrap()
    };
    assert_eq!(issues[0].get(issue_column("event_id")), Some("47"));
    assert_eq!(
        issues[0].get(issue_column("reason")),
        Some("CatcherPickoffConflict")
    );
    assert_eq!(
        issues[0].get(issue_column("prior_raw_pitch_sequence")),
        Some("1SPBS+1")
    );
    assert_eq!(
        issues[0].get(issue_column("current_raw_pitch_sequence")),
        Some("1SPBS+2.X")
    );

    let json_out = TempDir::new().unwrap();
    let json_status = Command::new(binary_path())
        .arg("-i")
        .arg(&input)
        .arg("-o")
        .arg(json_out.path())
        .arg("--json")
        .status()
        .unwrap();
    assert!(json_status.success());
    let json = fs::read_to_string(json_out.path().join("games.jsonl")).unwrap();
    let game: serde_json::Value = serde_json::from_str(json.trim()).unwrap();
    assert_eq!(game["id"], "ATL199708270");
    let events = game["events"].as_array().unwrap();
    for event_id in [46, 47] {
        let event = events
            .iter()
            .find(|event| event["event_id"] == event_id)
            .unwrap();
        assert_eq!(event["results"]["pitch_sequence_status"], "Unresolved");
        assert_eq!(event["results"]["pitch_sequence"], serde_json::json!([]));
    }
    let next_event = events.iter().find(|event| event["event_id"] == 48).unwrap();
    assert_eq!(next_event["results"]["pitch_sequence_status"], "Resolved");
    assert!(
        !next_event["results"]["pitch_sequence"]
            .as_array()
            .unwrap()
            .is_empty()
    );
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

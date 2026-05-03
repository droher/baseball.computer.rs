#![allow(clippy::unwrap_used, clippy::expect_used, clippy::match_same_arms)]

use pest::Parser;
use retrosheet_grammar::{RetrosheetParser, Rule};
use std::fs;
use std::path::{Path, PathBuf};

fn corpus_dir(category: &str) -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("corpus")
        .join("plays")
        .join(category)
}

fn read_files(dir: &Path) -> Vec<(String, String)> {
    let mut entries: Vec<_> = fs::read_dir(dir)
        .unwrap_or_else(|e| panic!("read_dir {} failed: {e}", dir.display()))
        .filter_map(Result::ok)
        .filter(|e| e.path().extension().and_then(|s| s.to_str()) == Some("txt"))
        .collect();
    entries.sort_by_key(std::fs::DirEntry::file_name);
    entries
        .into_iter()
        .map(|e| {
            let name = e.file_name().into_string().expect("utf8 filename");
            let body = fs::read_to_string(e.path())
                .unwrap_or_else(|err| panic!("read {name} failed: {err}"));
            (name, body.trim_end_matches(['\n', '\r']).to_string())
        })
        .collect()
}

#[test]
fn corpus_valid_plays_parse() {
    let dir = corpus_dir("valid");
    let cases = read_files(&dir);
    assert!(
        !cases.is_empty(),
        "no valid corpus files found in {}",
        dir.display()
    );
    for (name, input) in cases {
        let result = RetrosheetParser::parse(Rule::play, &input);
        assert!(
            result.is_ok(),
            "expected {name} ({input:?}) to parse, got error: {}",
            result.err().map_or_else(String::new, |e| e.to_string()),
        );
    }
}

#[test]
fn corpus_invalid_plays_reject() {
    let dir = corpus_dir("invalid");
    let cases = read_files(&dir);
    assert!(
        !cases.is_empty(),
        "no invalid corpus files found in {}",
        dir.display()
    );
    for (name, input) in cases {
        let result = RetrosheetParser::parse(Rule::play, &input);
        let err = result.err().unwrap_or_else(|| {
            panic!("expected {name} ({input:?}) to fail, but it parsed");
        });
        let hint = error_hint_from_filename(&name);
        let msg = err.to_string();
        assert!(
            msg.contains(hint),
            "invalid case {name}: error message {msg:?} did not contain hint {hint:?}",
        );
    }
}

fn error_hint_from_filename(name: &str) -> &'static str {
    match name {
        "001-empty.txt" => "expected",
        "003-lowercase.txt" => "expected",
        "004-trailing-junk.txt" => "EOI",
        "005-error-no-fielder.txt" => "expected",
        "006-error-two-digit-putout.txt" => "EOI",
        "007-multi-out-empty-parens.txt" => "runner",
        "008-multi-out-bad-runner.txt" => "runner",
        "009-leading-plus.txt" => "expected",
        other => panic!("unknown invalid corpus file: {other}"),
    }
}

#[test]
fn home_run_beats_h_in_ordered_choice() {
    let pairs = RetrosheetParser::parse(Rule::play, "HR").expect("HR parses");
    let play = pairs.peek().expect("one play pair");
    let main = play.into_inner().next().expect("main_play");
    let inner = main.into_inner().next().expect("inner main alternative");
    assert_eq!(
        inner.as_rule(),
        Rule::home_run,
        "HR landed in {:?}",
        inner.as_rule()
    );
}

#[test]
fn intentional_walk_beats_walk_in_ordered_choice() {
    let pairs = RetrosheetParser::parse(Rule::play, "IW").expect("IW parses");
    let main = pairs.peek().unwrap().into_inner().next().unwrap();
    let inner = main.into_inner().next().unwrap();
    assert_eq!(inner.as_rule(), Rule::intentional_walk);
}

#[test]
fn error_play_beats_fielded_out_for_e_form() {
    let pairs = RetrosheetParser::parse(Rule::play, "E5").expect("E5 parses");
    let main = pairs.peek().unwrap().into_inner().next().unwrap();
    let inner = main.into_inner().next().unwrap();
    assert_eq!(inner.as_rule(), Rule::error_play);
}

#[test]
fn wild_pitch_beats_walk_in_ordered_choice() {
    let pairs = RetrosheetParser::parse(Rule::play, "WP").expect("WP parses");
    let main = pairs.peek().unwrap().into_inner().next().unwrap();
    let inner = main.into_inner().next().unwrap();
    assert_eq!(inner.as_rule(), Rule::wild_pitch);
}

#[test]
fn foul_fly_error_takes_optional_fielder() {
    let with_fielder = RetrosheetParser::parse(Rule::play, "FLE5").expect("FLE5 parses");
    let main = with_fielder.peek().unwrap().into_inner().next().unwrap();
    assert_eq!(
        main.into_inner().next().unwrap().as_rule(),
        Rule::foul_fly_error
    );
    let bare = RetrosheetParser::parse(Rule::play, "FLE").expect("bare FLE parses");
    let main = bare.peek().unwrap().into_inner().next().unwrap();
    assert_eq!(
        main.into_inner().next().unwrap().as_rule(),
        Rule::foul_fly_error
    );
}

#[test]
fn trailing_questionable_marker_is_silently_consumed() {
    // STRIP_CHARS_REGEX strips `#` (questionable play) and `!` (exceptional)
    // before the play body reaches the grammar.
    let pairs = RetrosheetParser::parse(Rule::play, "K#").expect("K# parses");
    let main = pairs.peek().unwrap().into_inner().next().unwrap();
    let inner = main.into_inner().next().unwrap();
    assert_eq!(inner.as_rule(), Rule::strikeout);

    RetrosheetParser::parse(Rule::play, "S8.2-H;1-3;BX2(86)#")
        .expect("trailing # after advance parses");
    RetrosheetParser::parse(Rule::play, "FC.2X3(5)!").expect("trailing ! parses");
}

#[test]
fn internal_strip_chars_are_silently_consumed() {
    // play_strip is threaded between every concatenation in play body rules,
    // so `[#! ]` chars between tokens are accepted (mirrors the legacy
    // global STRIP_CHARS_REGEX).
    RetrosheetParser::parse(Rule::play, "S#8/L").expect("strip between hit letter and fielder");
    RetrosheetParser::parse(Rule::play, "K23 +WP").expect("strip before chain separator");
    RetrosheetParser::parse(Rule::play, "46(1)!3/GDP").expect("strip between putout groups");
    RetrosheetParser::parse(Rule::play, "S8 .2-H").expect("strip before advance section");
    RetrosheetParser::parse(Rule::play, "S8/L#/F").expect("strip between modifiers");
}

#[test]
fn unknown_fielder_question_mark_accepted() {
    // fielder rule accepts `?` directly (legacy UNKNOWN_FIELDER_REGEX rewrote
    // it to `0` before parsing). `single` is atomic so children are
    // suppressed; we walk fielded_out (compound atomic) for a structural
    // check.
    let pairs = RetrosheetParser::parse(Rule::play, "S?").expect("S? parses");
    let main = pairs.peek().unwrap().into_inner().next().unwrap();
    let inner = main.into_inner().next().unwrap();
    assert_eq!(inner.as_rule(), Rule::single);
    assert_eq!(inner.as_str(), "S?");

    let pairs = RetrosheetParser::parse(Rule::play, "?(B)").expect("?(B) parses");
    let main = pairs.peek().unwrap().into_inner().next().unwrap();
    let inner = main.into_inner().next().unwrap();
    assert_eq!(inner.as_rule(), Rule::fielded_out);
    let group = inner.into_inner().next().unwrap();
    let fielder = group
        .into_inner()
        .find(|p| p.as_rule() == Rule::fielder)
        .expect("fielder present");
    assert_eq!(fielder.as_str(), "?");
}

#[test]
fn unknown_fielder_double_nine_collapses_to_one_fielder() {
    // `999*` (two or more 9s) is the legacy unknown-fielder shorthand. The
    // grammar matches a `9{2,}` run as a single fielder. Walk via
    // `fielded_out` since `single` is atomic and suppresses child pairs.
    let pairs = RetrosheetParser::parse(Rule::play, "99(B)").expect("99(B) parses");
    let main = pairs.peek().unwrap().into_inner().next().unwrap();
    let inner = main.into_inner().next().unwrap();
    assert_eq!(inner.as_rule(), Rule::fielded_out);
    let group = inner.into_inner().next().unwrap();
    let fielders: Vec<_> = group
        .into_inner()
        .filter(|p| p.as_rule() == Rule::fielder)
        .map(|p| p.as_str().to_string())
        .collect();
    assert_eq!(fielders, vec!["99"], "two 9s collapse to one fielder");

    let pairs = RetrosheetParser::parse(Rule::play, "9999(B)").expect("9999(B) parses");
    let main = pairs.peek().unwrap().into_inner().next().unwrap();
    let inner = main.into_inner().next().unwrap();
    let group = inner.into_inner().next().unwrap();
    let fielders: Vec<_> = group
        .into_inner()
        .filter(|p| p.as_rule() == Rule::fielder)
        .map(|p| p.as_str().to_string())
        .collect();
    assert_eq!(fielders, vec!["9999"], "any 9-run collapses to one fielder");
}

#[test]
fn single_nine_is_still_a_normal_fielder() {
    let pairs = RetrosheetParser::parse(Rule::play, "9(B)").expect("9(B) parses");
    let main = pairs.peek().unwrap().into_inner().next().unwrap();
    let inner = main.into_inner().next().unwrap();
    let group = inner.into_inner().next().unwrap();
    let fielder = group
        .into_inner()
        .find(|p| p.as_rule() == Rule::fielder)
        .expect("fielder present");
    assert_eq!(fielder.as_str(), "9");
}

#[test]
fn pocs_beats_pickoff_in_ordered_choice() {
    let pairs = RetrosheetParser::parse(Rule::play, "POCS2(1361)").expect("POCS parses");
    let main = pairs.peek().unwrap().into_inner().next().unwrap();
    let inner = main.into_inner().next().unwrap();
    assert_eq!(inner.as_rule(), Rule::pickoff_caught_stealing);
}

#[test]
fn caught_stealing_beats_catcher_interference() {
    let pairs = RetrosheetParser::parse(Rule::play, "CS2(24)").expect("CS parses");
    let main = pairs.peek().unwrap().into_inner().next().unwrap();
    let inner = main.into_inner().next().unwrap();
    assert_eq!(inner.as_rule(), Rule::caught_stealing);
}

#[test]
fn chained_play_splits_primary_and_followup() {
    let pairs = RetrosheetParser::parse(Rule::play, "K23+WP").expect("K23+WP parses");
    let play = pairs.peek().unwrap();
    let main_count = play
        .clone()
        .into_inner()
        .filter(|p| p.as_rule() == Rule::main_play)
        .count();
    let chained_count = play
        .into_inner()
        .filter(|p| p.as_rule() == Rule::chained_play)
        .count();
    assert_eq!(main_count, 1, "exactly one top-level main_play");
    assert_eq!(chained_count, 1, "one chained_play for the +WP segment");
}

#[test]
fn chained_play_supports_multiple_followups() {
    let pairs = RetrosheetParser::parse(Rule::play, "W+SB2+SB3").expect("W+SB2+SB3 parses");
    let play = pairs.peek().unwrap();
    let chained_count = play
        .into_inner()
        .filter(|p| p.as_rule() == Rule::chained_play)
        .count();
    assert_eq!(chained_count, 2);
}

fn modifier_leaf_rule(input: &str) -> Rule {
    let pairs = RetrosheetParser::parse(Rule::play, input)
        .unwrap_or_else(|e| panic!("{input} parse failed: {e}"));
    let modifier = pairs
        .peek()
        .unwrap()
        .into_inner()
        .find(|p| p.as_rule() == Rule::modifier)
        .expect("modifier present");
    let body = modifier.into_inner().next().unwrap();
    assert_eq!(body.as_rule(), Rule::modifier_body);
    let inner = body.into_inner().next().unwrap();
    if inner.as_rule() == Rule::recognized_modifier {
        inner.into_inner().next().unwrap().as_rule()
    } else {
        inner.as_rule()
    }
}

#[test]
fn semicolon_top_level_chains_distinct_from_advance_section() {
    let pairs = RetrosheetParser::parse(Rule::play, "SB3;SB2.3-H").expect("SB3;SB2.3-H parses");
    let play = pairs.peek().unwrap();
    let chained: Vec<_> = play
        .clone()
        .into_inner()
        .filter(|p| p.as_rule() == Rule::chained_play)
        .collect();
    assert_eq!(chained.len(), 1, "one top-level ; chain");
    let advances: Vec<_> = play
        .into_inner()
        .find(|p| p.as_rule() == Rule::advance_section)
        .unwrap()
        .into_inner()
        .filter(|p| p.as_rule() == Rule::advance)
        .collect();
    assert_eq!(advances.len(), 1, "one advance after .");
}

#[test]
fn x_must_not_be_a_destination_base() {
    let result = RetrosheetParser::parse(Rule::play, "S8.2-X");
    assert!(
        result.is_err(),
        "S8.2-X must NOT parse — X is the separator"
    );
}

#[test]
fn advance_putout_with_interference_form_parses() {
    let pairs = RetrosheetParser::parse(Rule::play, "S.2X3(5/INT)").expect("(5/INT) parses");
    let advance = pairs
        .peek()
        .unwrap()
        .into_inner()
        .find(|p| p.as_rule() == Rule::advance_section)
        .unwrap()
        .into_inner()
        .find(|p| p.as_rule() == Rule::advance)
        .unwrap();
    let modifier_body = advance
        .into_inner()
        .find(|p| p.as_rule() == Rule::advance_modifier)
        .unwrap()
        .into_inner()
        .find(|p| p.as_rule() == Rule::advance_modifier_body)
        .unwrap();
    let inner = modifier_body.into_inner().next().unwrap();
    let leaf = if inner.as_rule() == Rule::recognized_advance_modifier {
        inner.into_inner().next().unwrap().as_rule()
    } else {
        inner.as_rule()
    };
    assert_eq!(leaf, Rule::advance_putout_with_interference);
}

#[test]
fn advance_section_supports_multiple_runners() {
    let pairs = RetrosheetParser::parse(Rule::play, "HR.3-H;2-H;1-H")
        .expect("HR with three runners parses");
    let advances: Vec<_> = pairs
        .peek()
        .unwrap()
        .into_inner()
        .find(|p| p.as_rule() == Rule::advance_section)
        .unwrap()
        .into_inner()
        .filter(|p| p.as_rule() == Rule::advance)
        .collect();
    assert_eq!(advances.len(), 3);
}

#[test]
fn modifier_keyword_beats_trajectory_letter() {
    assert_eq!(modifier_leaf_rule("8/GDP"), Rule::ground_double_play);
}

#[test]
fn contact_description_falls_back_when_no_keyword() {
    assert_eq!(modifier_leaf_rule("8/F78XD"), Rule::contact_description);
}

#[test]
fn throw_to_home_beats_throw_to_base() {
    assert_eq!(modifier_leaf_rule("8/THH"), Rule::throw_to_home);
}

#[test]
fn multi_out_groups_are_walkable() {
    let pairs = RetrosheetParser::parse(Rule::play, "64(1)3").expect("64(1)3 parses");
    let main = pairs.peek().unwrap().into_inner().next().unwrap();
    let inner = main.into_inner().next().unwrap();
    assert_eq!(inner.as_rule(), Rule::fielded_out);
    let groups: Vec<_> = inner
        .into_inner()
        .filter(|p| p.as_rule() == Rule::putout_group)
        .collect();
    assert_eq!(
        groups.len(),
        2,
        "expected two putout groups, got {groups:?}"
    );
}

#[test]
fn multi_out_triple_play_has_three_groups() {
    let pairs = RetrosheetParser::parse(Rule::play, "1(B)16(2)63(1)").expect("triple play parses");
    let main = pairs.peek().unwrap().into_inner().next().unwrap();
    let inner = main.into_inner().next().unwrap();
    assert_eq!(inner.as_rule(), Rule::fielded_out);
    let groups: Vec<_> = inner
        .into_inner()
        .filter(|p| p.as_rule() == Rule::putout_group)
        .collect();
    assert_eq!(groups.len(), 3);
}

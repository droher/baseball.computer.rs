#![allow(clippy::unwrap_used, clippy::expect_used, clippy::match_same_arms)]

use pest::Parser;
use retrosheet_grammar::{RetrosheetParser, Rule};
use std::fs;
use std::path::{Path, PathBuf};

fn corpus_dir(category: &str) -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("corpus")
        .join("records")
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
            (name, body)
        })
        .collect()
}

#[test]
fn corpus_valid_records_parse() {
    let dir = corpus_dir("valid");
    let cases = read_files(&dir);
    assert!(
        !cases.is_empty(),
        "no valid corpus files found in {}",
        dir.display()
    );
    for (name, input) in cases {
        let result = RetrosheetParser::parse(Rule::file, &input);
        assert!(
            result.is_ok(),
            "expected {name} to parse, got error: {}",
            result.err().map_or_else(String::new, |e| e.to_string()),
        );
    }
}

#[test]
fn corpus_invalid_records_reject() {
    let dir = corpus_dir("invalid");
    let cases = read_files(&dir);
    assert!(
        !cases.is_empty(),
        "no invalid corpus files found in {}",
        dir.display()
    );
    for (name, input) in cases {
        let result = RetrosheetParser::parse(Rule::file, &input);
        let err = result.err().unwrap_or_else(|| {
            panic!("expected {name} to fail, but it parsed");
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
        "002-no-id.txt" => "expected",
        "003-id-not-first.txt" => "expected",
        "004-unknown-tag.txt" => "EOI",
        "005-id-no-comma.txt" => "expected",
        "006-bare-tag-no-fields.txt" => "expected",
        "007-info-unknown-key.txt" => "expected",
        "008-info-bad-date.txt" => "expected",
        "009-info-bad-doubleheader.txt" => "expected",
        "010-info-bad-gametype.txt" => "expected",
        "011-info-bad-bool.txt" => "expected",
        "012-start-bad-side.txt" => "expected",
        "013-start-bad-lineup.txt" => "expected",
        "014-start-bad-fielding-position.txt" => "expected",
        "015-play-bad-inning.txt" => "expected",
        "016-play-bad-play-body.txt" => "expected",
        other => panic!("unknown invalid corpus file: {other}"),
    }
}

#[test]
fn id_must_be_first_record() {
    let input = "id,KCA201409300\nversion,2\n";
    let pairs = RetrosheetParser::parse(Rule::file, input).expect("parses");
    let file = pairs.peek().expect("file pair");
    let mut inner = file.into_inner();
    let first = inner.next().expect("first child");
    assert_eq!(first.as_rule(), Rule::id_record);
}

#[test]
fn record_tag_dispatch_walkable() {
    let input = "id,X\ninfo,visteam,BOS\nstart,p,\"name\",1,1,1\nplay,1,0,p,??,,S8\ncom,c\ndata,er,p,1\nstat,bline,p,0\nline,0,1\nevent,hpline,0\npresadj,p,1\n";
    let pairs = RetrosheetParser::parse(Rule::file, input).expect("parses");
    let file = pairs.peek().unwrap();
    let mut found_rules = Vec::new();
    for child in file.into_inner() {
        if child.as_rule() == Rule::record_line {
            let inner_record = child
                .into_inner()
                .find(|p| p.as_rule() == Rule::record)
                .expect("record");
            let leaf = inner_record.into_inner().next().expect("leaf");
            found_rules.push(leaf.as_rule());
        }
    }
    assert_eq!(
        found_rules,
        vec![
            Rule::info_record,
            Rule::start_record,
            Rule::play_record,
            Rule::com_record,
            Rule::data_record,
            Rule::stat_record,
            Rule::line_record,
            Rule::event_record,
            Rule::adjustment_record,
        ]
    );
}

#[test]
fn quoted_field_preserves_internal_commas() {
    let input = "id,X\nstart,doej101,\"Last, First\",1,1,1\n";
    let body = appearance_body(input);
    let name = body
        .into_inner()
        .find(|p| p.as_rule() == Rule::player_name)
        .expect("player_name");
    assert_eq!(name.as_str(), "\"Last, First\"");
    let inner = name.into_inner().next().expect("quoted or unquoted");
    assert_eq!(inner.as_rule(), Rule::quoted_field);
}

fn appearance_body(input: &str) -> pest::iterators::Pair<'_, Rule> {
    let pairs = RetrosheetParser::parse(Rule::file, input)
        .unwrap_or_else(|e| panic!("{input:?} parse failed: {e}"));
    let file = pairs.peek().unwrap();
    file.into_inner()
        .filter(|p| p.as_rule() == Rule::record_line)
        .find_map(|line| {
            line.into_inner()
                .find(|p| p.as_rule() == Rule::record)
                .and_then(|r| {
                    r.into_inner()
                        .find(|p| matches!(p.as_rule(), Rule::start_record | Rule::sub_record))
                })
                .and_then(|rec| {
                    rec.into_inner()
                        .find(|p| p.as_rule() == Rule::appearance_body)
                })
        })
        .expect("appearance_body present")
}

#[test]
fn empty_player_field_accepted() {
    // Empty `wp`/`lp`/`save`/`gwrbi` values (`info,save,`) are tolerated.
    let input = "id,X\ninfo,save,\n";
    let pairs = RetrosheetParser::parse(Rule::file, input).expect("parses");
    let file = pairs.peek().unwrap();
    let body = info_body(file);
    let leaf = body.into_inner().next().expect("info leaf");
    assert_eq!(leaf.as_rule(), Rule::info_player_field);
    let value = leaf
        .into_inner()
        .find(|p| p.as_rule() == Rule::info_player_value)
        .expect("info_player_value");
    assert_eq!(value.as_str(), "");
}

fn info_body(file: pest::iterators::Pair<Rule>) -> pest::iterators::Pair<Rule> {
    file.into_inner()
        .filter(|p| p.as_rule() == Rule::record_line)
        .find_map(|line| {
            line.into_inner()
                .find(|p| p.as_rule() == Rule::record)
                .and_then(|r| r.into_inner().find(|p| p.as_rule() == Rule::info_record))
                .and_then(|info| info.into_inner().find(|p| p.as_rule() == Rule::info_body))
        })
        .expect("info_body present")
}

fn adjustment_leaf(input: &str) -> pest::iterators::Pair<'_, Rule> {
    let pairs = RetrosheetParser::parse(Rule::file, input)
        .unwrap_or_else(|e| panic!("{input:?} parse failed: {e}"));
    let file = pairs.peek().unwrap();
    file.into_inner()
        .filter(|p| p.as_rule() == Rule::record_line)
        .find_map(|line| {
            line.into_inner()
                .find(|p| p.as_rule() == Rule::record)
                .and_then(|r| {
                    r.into_inner()
                        .find(|p| p.as_rule() == Rule::adjustment_record)
                })
                .and_then(|adj| adj.into_inner().next())
        })
        .expect("adjustment leaf")
}

#[test]
fn presadj_dispatches_to_presadj_record() {
    let leaf = adjustment_leaf("id,X\npresadj,oescj101,2\n");
    assert_eq!(leaf.as_rule(), Rule::presadj_record);
    let runner = leaf
        .into_inner()
        .find(|p| p.as_rule() == Rule::baserunner_value)
        .expect("baserunner_value");
    assert_eq!(runner.as_str(), "2");
}

#[test]
fn radj_accepts_h_for_home() {
    let leaf = adjustment_leaf("id,X\nradj,abcdef01,H\n");
    assert_eq!(leaf.as_rule(), Rule::radj_record);
    let base = leaf
        .into_inner()
        .find(|p| p.as_rule() == Rule::base_value)
        .expect("base_value");
    assert_eq!(base.as_str(), "H");
}

#[test]
fn badj_and_padj_accept_l_or_r() {
    let leaf = adjustment_leaf("id,X\nbadj,batt0001,L\n");
    assert_eq!(leaf.as_rule(), Rule::badj_record);
    let leaf2 = adjustment_leaf("id,X\npadj,pitc0001,R\n");
    assert_eq!(leaf2.as_rule(), Rule::padj_record);
}

#[test]
fn ladj_uses_side_and_lineup_position() {
    let leaf = adjustment_leaf("id,X\nladj,1,4\n");
    assert_eq!(leaf.as_rule(), Rule::ladj_record);
    let rules: Vec<_> = leaf.into_inner().map(|p| p.as_rule()).collect();
    assert_eq!(rules, vec![Rule::side_value, Rule::lineup_position_value]);
}

#[test]
fn presadj_rejects_invalid_baserunner() {
    assert!(RetrosheetParser::parse(Rule::file, "id,X\npresadj,p,4\n").is_err());
}

#[test]
fn radj_rejects_invalid_base() {
    assert!(RetrosheetParser::parse(Rule::file, "id,X\nradj,p,5\n").is_err());
}

#[test]
fn badj_rejects_invalid_hand() {
    assert!(RetrosheetParser::parse(Rule::file, "id,X\nbadj,p,S\n").is_err());
}

#[test]
fn no_trailing_newline_parses() {
    let input = "id,X\nversion,2";
    let result = RetrosheetParser::parse(Rule::file, input);
    assert!(
        result.is_ok(),
        "no-trailing-newline should parse: {}",
        result.err().map_or_else(String::new, |e| e.to_string()),
    );
}

fn parse_single_info(input: &str) -> Rule {
    let pairs = RetrosheetParser::parse(Rule::file, input)
        .unwrap_or_else(|e| panic!("{input:?} parse failed: {e}"));
    let body = info_body(pairs.peek().unwrap());
    body.into_inner().next().expect("info leaf").as_rule()
}

#[test]
fn info_team_dispatches_to_team_rule() {
    assert_eq!(
        parse_single_info("id,X\ninfo,visteam,BOS\n"),
        Rule::info_team
    );
    assert_eq!(
        parse_single_info("id,X\ninfo,hometeam,NYA\n"),
        Rule::info_team
    );
}

#[test]
fn info_date_strict_format() {
    assert_eq!(
        parse_single_info("id,X\ninfo,date,2024/04/01\n"),
        Rule::info_date
    );
    assert!(RetrosheetParser::parse(Rule::file, "id,X\ninfo,date,04/01/2024\n").is_err());
    assert!(RetrosheetParser::parse(Rule::file, "id,X\ninfo,date,2024-04-01\n").is_err());
}

#[test]
fn info_doubleheader_range() {
    for n in ["0", "1", "2", "3", "4"] {
        let input = format!("id,X\ninfo,number,{n}\n");
        assert_eq!(parse_single_info(&input), Rule::info_doubleheader, "{n}");
    }
    assert!(RetrosheetParser::parse(Rule::file, "id,X\ninfo,number,5\n").is_err());
}

#[test]
fn info_unknown_key_rejected() {
    assert!(RetrosheetParser::parse(Rule::file, "id,X\ninfo,foobar,baz\n").is_err());
}

#[test]
fn info_int_accepts_negative_one() {
    assert_eq!(
        parse_single_info("id,X\ninfo,windspeed,-1\n"),
        Rule::info_int
    );
    assert_eq!(parse_single_info("id,X\ninfo,temp,-1\n"), Rule::info_int);
}

#[test]
fn info_bool_case_insensitive() {
    assert_eq!(
        parse_single_info("id,X\ninfo,usedh,TRUE\n"),
        Rule::info_usedh
    );
    assert_eq!(
        parse_single_info("id,X\ninfo,usedh,false\n"),
        Rule::info_usedh
    );
    assert_eq!(
        parse_single_info("id,X\ninfo,htbf,FALSE\n"),
        Rule::info_htbf
    );
    assert!(RetrosheetParser::parse(Rule::file, "id,X\ninfo,htbf,yes\n").is_err());
}

#[test]
fn info_daynight_empty_accepted() {
    assert_eq!(
        parse_single_info("id,X\ninfo,daynight,\n"),
        Rule::info_daynight
    );
    assert_eq!(
        parse_single_info("id,X\ninfo,daynight,night\n"),
        Rule::info_daynight
    );
}

#[test]
fn info_fieldcond_alias_dispatches() {
    assert_eq!(
        parse_single_info("id,X\ninfo,fieldcon,wet\n"),
        Rule::info_fieldcond
    );
    assert_eq!(
        parse_single_info("id,X\ninfo,fieldcond,dry\n"),
        Rule::info_fieldcond
    );
}

#[test]
fn info_gametype_known_values() {
    for v in [
        "regular",
        "wildcard",
        "allstar",
        "lcs",
        "worldseries",
        "negroleagues",
    ] {
        let input = format!("id,X\ninfo,gametype,{v}\n");
        assert_eq!(parse_single_info(&input), Rule::info_gametype, "{v}");
    }
    assert!(RetrosheetParser::parse(Rule::file, "id,X\ninfo,gametype,playoffs\n").is_err());
}

#[test]
fn info_umpire_position_walkable() {
    let pairs =
        RetrosheetParser::parse(Rule::file, "id,X\ninfo,umphome,doej101\n").expect("parses");
    let body = info_body(pairs.peek().unwrap());
    let leaf = body.into_inner().next().expect("info leaf");
    assert_eq!(leaf.as_rule(), Rule::info_umpire);
    let pos = leaf
        .into_inner()
        .find(|p| p.as_rule() == Rule::info_umpire_position)
        .expect("info_umpire_position");
    assert_eq!(pos.as_str(), "umphome");
}

#[test]
fn start_record_fields_walkable() {
    let body = appearance_body("id,X\nstart,doej101,\"Doe, John\",0,1,7\n");
    let rules: Vec<_> = body.into_inner().map(|p| p.as_rule()).collect();
    assert_eq!(
        rules,
        vec![
            Rule::player_id,
            Rule::player_name,
            Rule::side_value,
            Rule::lineup_position_value,
            Rule::fielding_position_value,
        ]
    );
}

#[test]
fn sub_record_uses_same_body() {
    let body = appearance_body("id,X\nstart,a,\"a\",0,1,1\nsub,smit002,\"Smith\",1,3,4\n");
    let id = body
        .into_inner()
        .find(|p| p.as_rule() == Rule::player_id)
        .expect("player_id");
    assert_eq!(id.as_str(), "a");
}

#[test]
fn lineup_position_zero_is_pitcher_with_dh() {
    let body = appearance_body("id,X\nstart,p001,\"P\",0,0,1\n");
    let lineup = body
        .into_inner()
        .find(|p| p.as_rule() == Rule::lineup_position_value)
        .expect("lineup_position_value");
    assert_eq!(lineup.as_str(), "0");
}

#[test]
fn fielding_position_two_digit_forms_parse() {
    for n in ["10", "11", "12"] {
        let input = format!("id,X\nstart,p,\"P\",0,1,{n}\n");
        let body = appearance_body(&input);
        let pos = body
            .into_inner()
            .find(|p| p.as_rule() == Rule::fielding_position_value)
            .expect("fielding_position_value");
        let digits = pos
            .into_inner()
            .find(|p| p.as_rule() == Rule::fielding_position_digits)
            .expect("fielding_position_digits");
        assert_eq!(digits.as_str(), n, "{n}");
    }
}

#[test]
fn fielding_position_trailing_space_tolerated() {
    let body = appearance_body("id,X\nstart,p,\"P\",0,1,7 \n");
    let pos = body
        .into_inner()
        .find(|p| p.as_rule() == Rule::fielding_position_value)
        .expect("fielding_position_value");
    let digits = pos
        .into_inner()
        .find(|p| p.as_rule() == Rule::fielding_position_digits)
        .expect("fielding_position_digits");
    assert_eq!(digits.as_str(), "7");
}

#[test]
fn start_rejects_bad_side() {
    assert!(RetrosheetParser::parse(Rule::file, "id,X\nstart,p,\"P\",2,1,7\n").is_err());
}

#[test]
fn start_rejects_lineup_out_of_range() {
    assert!(RetrosheetParser::parse(Rule::file, "id,X\nstart,p,\"P\",0,10,7\n").is_err());
}

#[test]
fn start_rejects_fielding_position_thirteen() {
    assert!(RetrosheetParser::parse(Rule::file, "id,X\nstart,p,\"P\",0,1,13\n").is_err());
}

fn play_record(input: &str) -> pest::iterators::Pair<'_, Rule> {
    let pairs = RetrosheetParser::parse(Rule::file, input)
        .unwrap_or_else(|e| panic!("{input:?} parse failed: {e}"));
    let file = pairs.peek().unwrap();
    file.into_inner()
        .filter(|p| p.as_rule() == Rule::record_line)
        .find_map(|line| {
            line.into_inner()
                .find(|p| p.as_rule() == Rule::record)
                .and_then(|r| r.into_inner().find(|p| p.as_rule() == Rule::play_record))
        })
        .expect("play_record present")
}

#[test]
fn play_record_fields_walkable() {
    let pr = play_record("id,X\nplay,1,0,crisc001,32,CBBBFX,S7/L\n");
    let rules: Vec<_> = pr.into_inner().map(|p| p.as_rule()).collect();
    assert_eq!(
        rules,
        vec![
            Rule::play_inning,
            Rule::side_value,
            Rule::play_batter,
            Rule::play_count,
            Rule::play_pitches,
            Rule::main_play,
            Rule::modifier,
        ]
    );
}

#[test]
fn play_record_embeds_play_grammar() {
    let pr = play_record("id,X\nplay,3,1,abcd0001,01,CX,HR/9/L.1-H\n");
    let main = pr
        .into_inner()
        .find(|p| p.as_rule() == Rule::main_play)
        .expect("main_play");
    let leaf = main.into_inner().next().expect("main alternative");
    assert_eq!(leaf.as_rule(), Rule::home_run);
}

#[test]
fn play_count_question_marks_accepted() {
    let pr = play_record("id,X\nplay,1,0,a,??,,S8\n");
    let count = pr
        .into_inner()
        .find(|p| p.as_rule() == Rule::play_count)
        .expect("play_count");
    assert_eq!(count.as_str(), "??");
}

#[test]
fn play_pitches_can_be_empty() {
    let pr = play_record("id,X\nplay,1,0,a,??,,S8\n");
    let pitches = pr
        .into_inner()
        .find(|p| p.as_rule() == Rule::play_pitches)
        .expect("play_pitches");
    assert_eq!(pitches.as_str(), "");
}

#[test]
fn play_record_rejects_invalid_play_body() {
    assert!(RetrosheetParser::parse(Rule::file, "id,X\nplay,1,0,a,00,,zzz\n").is_err());
}

#[test]
fn play_record_rejects_bad_count_length() {
    assert!(RetrosheetParser::parse(Rule::file, "id,X\nplay,1,0,a,123,,S8\n").is_err());
}

#[test]
fn play_record_rejects_non_numeric_inning() {
    assert!(RetrosheetParser::parse(Rule::file, "id,X\nplay,top,0,a,00,,S8\n").is_err());
}

fn pitch_tokens(input: &str) -> Vec<(Rule, String)> {
    let pr = play_record(input);
    let pitches = pr
        .into_inner()
        .find(|p| p.as_rule() == Rule::play_pitches)
        .expect("play_pitches");
    pitches
        .into_inner()
        .map(|tok| {
            let leaf = tok.into_inner().next().expect("pitch_token leaf");
            (leaf.as_rule(), leaf.as_str().to_string())
        })
        .collect()
}

#[test]
fn pitch_sequence_basic_chars_walkable() {
    let toks = pitch_tokens("id,X\nplay,1,0,a,32,BCSFX,S8\n");
    assert_eq!(
        toks.iter().map(|(r, _)| *r).collect::<Vec<_>>(),
        vec![Rule::pitch_char_token; 5]
    );
    assert_eq!(
        toks.iter().map(|(_, s)| s.clone()).collect::<Vec<_>>(),
        vec!["B", "C", "S", "F", "X"]
    );
}

#[test]
fn pitch_sequence_flags_distinct_from_chars() {
    let toks = pitch_tokens("id,X\nplay,1,0,a,11,*B>CX,S8\n");
    let rules: Vec<Rule> = toks.iter().map(|(r, _)| *r).collect();
    assert_eq!(
        rules,
        vec![
            Rule::pitch_flag_token,
            Rule::pitch_char_token,
            Rule::pitch_flag_token,
            Rule::pitch_char_token,
            Rule::pitch_char_token,
        ]
    );
}

#[test]
fn pitch_sequence_pickoff_consumes_base() {
    let toks = pitch_tokens("id,X\nplay,1,0,a,01,B+2C,S8\n");
    assert_eq!(toks[1].0, Rule::pickoff_token);
    assert_eq!(toks[1].1, "+2");
    assert_eq!(toks[2].0, Rule::pitch_char_token);
}

#[test]
fn pitch_sequence_pa_resume_pickoff_uses_dot_then_plus() {
    let toks = pitch_tokens("id,X\nplay,1,0,a,02,BC.+3FX,S8\n");
    let rules: Vec<Rule> = toks.iter().map(|(r, _)| *r).collect();
    assert_eq!(
        rules,
        vec![
            Rule::pitch_char_token,
            Rule::pitch_char_token,
            Rule::pa_break_token,
            Rule::pickoff_token,
            Rule::pitch_char_token,
            Rule::pitch_char_token,
        ]
    );
}

#[test]
fn pitch_sequence_bare_plus_falls_through_to_pitch_char() {
    // `+` not followed by a valid base is not a pickoff; grammar accepts it
    // as a pitch_char_token (PitchType::from_str → Unrecognized).
    let toks = pitch_tokens("id,X\nplay,1,0,a,00,B+ZX,S8\n");
    assert_eq!(toks[1].0, Rule::pitch_char_token);
    assert_eq!(toks[1].1, "+");
}

#[test]
fn pitch_sequence_unknown_pitch_char_accepted() {
    let toks = pitch_tokens("id,X\nplay,1,0,a,00,Z,S8\n");
    assert_eq!(toks[0].0, Rule::pitch_char_token);
    assert_eq!(toks[0].1, "Z");
}

#[test]
fn crlf_line_endings_parse() {
    let input = "id,X\r\nversion,2\r\ninfo,visteam,BOS\r\n";
    let result = RetrosheetParser::parse(Rule::file, input);
    assert!(
        result.is_ok(),
        "CRLF should parse: {}",
        result.err().map_or_else(String::new, |e| e.to_string()),
    );
}

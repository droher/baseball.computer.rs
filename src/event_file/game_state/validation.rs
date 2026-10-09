//! Game-id reconciliation and info-record presence checks.

use std::collections::{BTreeSet, HashMap};

use anyhow::{Context, Result, bail};
use chrono::Datelike;
use tracing::warn;

use crate::event_file::box_score::BoxScoreLine;
use crate::event_file::info::{DoubleheaderStatus, InfoRecord, Team};
use crate::event_file::misc::GameId;
use crate::event_file::parser::{MappedRecord, RecordSlice};
use crate::event_file::traits::{LineupPosition, Matchup, Player, Side};

use super::GameSetting;

pub(super) fn get_game_id(rv: &RecordSlice) -> Result<GameId> {
    rv.iter()
        .find_map(|mr| {
            if let MappedRecord::GameId(g) = *mr {
                Some(g)
            } else {
                None
            }
        })
        .context("No Game ID found in records")
}

#[derive(Debug, Copy, Clone, Eq, PartialEq, Ord, PartialOrd, Hash)]
enum LineupSource {
    Start,
    BattingLine,
}

pub(super) fn ensure_unique_lineup_slots(rv: &RecordSlice) -> Result<()> {
    let mut slots: HashMap<(LineupSource, Side, Player), BTreeSet<LineupPosition>> = HashMap::new();
    for record in rv {
        let (source, side, player, position) = match record {
            MappedRecord::Start(start) => (
                LineupSource::Start,
                start.side,
                start.player,
                start.lineup_position,
            ),
            MappedRecord::BoxScoreLine(BoxScoreLine::BattingLine(line)) => (
                LineupSource::BattingLine,
                line.side,
                line.batter_id,
                line.lineup_position,
            ),
            _ => continue,
        };
        if position != LineupPosition::PitcherWithDh {
            let _ = slots
                .entry((source, side, player))
                .or_default()
                .insert(position);
        }
    }
    let mut duplicates: Vec<_> = slots
        .into_iter()
        .filter(|(_, positions)| positions.len() > 1)
        .collect();
    if duplicates.is_empty() {
        return Ok(());
    }
    duplicates.sort_unstable();
    let described = duplicates
        .iter()
        .map(|((source, side, player), positions)| {
            let positions: Vec<u8> = positions.iter().map(|p| u8::from(*p)).collect();
            format!("{player} ({side}) in {source:?} lineup positions {positions:?}")
        })
        .collect::<Vec<_>>()
        .join("; ");
    bail!("Player listed in more than one lineup position: {described}")
}

pub(super) const fn doubleheader_status_from_suffix(suffix: u8) -> Option<DoubleheaderStatus> {
    match suffix {
        b'0' => Some(DoubleheaderStatus::SingleGame),
        b'1' => Some(DoubleheaderStatus::DoubleHeaderGame1),
        b'2' => Some(DoubleheaderStatus::DoubleHeaderGame2),
        b'3' => Some(DoubleheaderStatus::DoubleHeaderGame3),
        b'4' => Some(DoubleheaderStatus::DoubleHeaderGame4),
        _ => None,
    }
}

#[derive(Debug, Default, Copy, Clone, Eq, PartialEq)]
pub(super) struct InfoPresence {
    pub date: bool,
    pub home_team: bool,
    pub doubleheader: bool,
}

pub(super) fn info_presence(slice: &RecordSlice) -> InfoPresence {
    let mut p = InfoPresence::default();
    for r in slice {
        if let MappedRecord::Info(info) = r {
            match info {
                InfoRecord::GameDate(_) => p.date = true,
                InfoRecord::HomeTeam(_) => p.home_team = true,
                InfoRecord::DoubleheaderStatus(_) => p.doubleheader = true,
                _ => {}
            }
        }
    }
    p
}

// Checks the `id,...` record against `info,date` and `info,hometeam`, and
// uses the game ID suffix to fill in `doubleheader_status` when no `info,number`
// record is present. Disagreements log a warning; the parser does not abort.
pub(super) fn reconcile_with_game_id(
    game_id: GameId,
    setting: &mut GameSetting,
    teams: &Matchup<Team>,
    presence: InfoPresence,
) {
    let id_str = game_id.id.as_str();
    let bytes = id_str.as_bytes();
    if bytes.len() != 12 {
        warn!(
            "game_id {} is {} chars long, expected 12; skipping the rest of the checks",
            id_str,
            bytes.len()
        );
        return;
    }

    let home_from_id = &id_str[0..3];
    if !presence.home_team {
        warn!(
            "game_id {} has no info,hometeam record; skipping the home team check",
            id_str
        );
    } else if teams.home.as_str() != home_from_id {
        warn!(
            "game_id {} starts with home team {}, but info,hometeam is {}",
            id_str, home_from_id, teams.home
        );
    }

    if presence.date {
        let year_res = id_str[3..7].parse::<i32>();
        let month_res = id_str[7..9].parse::<u32>();
        let day_res = id_str[9..11].parse::<u32>();
        if let (Ok(y), Ok(m), Ok(d)) = (year_res, month_res, day_res) {
            if setting.date.year() != y || setting.date.month() != m || setting.date.day() != d {
                warn!(
                    "game_id {} encodes the date {:04}-{:02}-{:02}, but info,date is {}",
                    id_str, y, m, d, setting.date
                );
            }
        } else {
            warn!(
                "game_id {} has a date that is not numeric; skipping the date check",
                id_str
            );
        }
    } else {
        warn!(
            "game_id {} has no info,date record; skipping the date check",
            id_str
        );
    }

    let suffix = bytes[11];
    let Some(dh_from_id) = doubleheader_status_from_suffix(suffix) else {
        warn!(
            "game_id {} ends in '{}', which is not a known doubleheader number",
            id_str, suffix as char
        );
        return;
    };

    if presence.doubleheader {
        if setting.doubleheader_status != dh_from_id {
            warn!(
                "game_id {} ends in {:?} but info,number says {:?}; keeping info,number",
                id_str, dh_from_id, setting.doubleheader_status
            );
        }
    } else if dh_from_id != DoubleheaderStatus::SingleGame {
        warn!(
            "game_id {} has no info,number record; using {:?} from the game ID suffix",
            id_str, dh_from_id
        );
        setting.doubleheader_status = dh_from_id;
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::*;
    use crate::event_file::misc::str_to_tinystr;
    use chrono::NaiveDate;

    fn make_setting(date: NaiveDate, dh: DoubleheaderStatus) -> GameSetting {
        GameSetting {
            date,
            doubleheader_status: dh,
            ..GameSetting::default()
        }
    }

    fn make_teams(home: &str, away: &str) -> Matchup<Team> {
        Matchup {
            away: str_to_tinystr(away).unwrap(),
            home: str_to_tinystr(home).unwrap(),
        }
    }

    fn make_game_id(s: &str) -> GameId {
        GameId {
            id: str_to_tinystr(s).unwrap(),
        }
    }

    fn full_presence() -> InfoPresence {
        InfoPresence {
            date: true,
            home_team: true,
            doubleheader: true,
        }
    }

    #[test]
    fn doubleheader_status_from_suffix_covers_known_digits() {
        assert_eq!(
            doubleheader_status_from_suffix(b'0'),
            Some(DoubleheaderStatus::SingleGame)
        );
        assert_eq!(
            doubleheader_status_from_suffix(b'1'),
            Some(DoubleheaderStatus::DoubleHeaderGame1)
        );
        assert_eq!(
            doubleheader_status_from_suffix(b'2'),
            Some(DoubleheaderStatus::DoubleHeaderGame2)
        );
        assert_eq!(
            doubleheader_status_from_suffix(b'3'),
            Some(DoubleheaderStatus::DoubleHeaderGame3)
        );
        assert_eq!(
            doubleheader_status_from_suffix(b'4'),
            Some(DoubleheaderStatus::DoubleHeaderGame4)
        );
        assert_eq!(doubleheader_status_from_suffix(b'5'), None);
        assert_eq!(doubleheader_status_from_suffix(b'9'), None);
        assert_eq!(doubleheader_status_from_suffix(b'x'), None);
    }

    #[test]
    fn reconcile_uses_game_id_suffix_when_info_number_missing() {
        let mut setting = make_setting(
            NaiveDate::from_ymd_opt(1904, 5, 30).unwrap(),
            DoubleheaderStatus::SingleGame,
        );
        let teams = make_teams("BRO", "BSN");
        let presence = InfoPresence {
            doubleheader: false,
            ..full_presence()
        };
        reconcile_with_game_id(make_game_id("BRO190405302"), &mut setting, &teams, presence);
        assert_eq!(
            setting.doubleheader_status,
            DoubleheaderStatus::DoubleHeaderGame2
        );
    }

    #[test]
    fn reconcile_keeps_info_number_when_present_and_disagrees_with_suffix() {
        // Mirrors HOM194509200: ID suffix 0, info,number says 1.
        let mut setting = make_setting(
            NaiveDate::from_ymd_opt(1945, 9, 20).unwrap(),
            DoubleheaderStatus::DoubleHeaderGame1,
        );
        let teams = make_teams("HOM", "AWY");
        reconcile_with_game_id(
            make_game_id("HOM194509200"),
            &mut setting,
            &teams,
            full_presence(),
        );
        assert_eq!(
            setting.doubleheader_status,
            DoubleheaderStatus::DoubleHeaderGame1
        );
    }

    #[test]
    fn reconcile_skips_all_checks_for_short_game_id() {
        let mut setting = make_setting(
            NaiveDate::from_ymd_opt(1948, 6, 22).unwrap(),
            DoubleheaderStatus::SingleGame,
        );
        let teams = make_teams("BLG", "AWY");
        reconcile_with_game_id(
            make_game_id("BLG4806220"),
            &mut setting,
            &teams,
            full_presence(),
        );
        assert_eq!(setting.doubleheader_status, DoubleheaderStatus::SingleGame);
    }

    #[test]
    fn reconcile_leaves_default_when_info_number_missing_and_suffix_zero() {
        let mut setting = make_setting(
            NaiveDate::from_ymd_opt(1990, 4, 1).unwrap(),
            DoubleheaderStatus::SingleGame,
        );
        let teams = make_teams("ABC", "XYZ");
        let presence = InfoPresence {
            doubleheader: false,
            ..full_presence()
        };
        reconcile_with_game_id(make_game_id("ABC199004010"), &mut setting, &teams, presence);
        assert_eq!(setting.doubleheader_status, DoubleheaderStatus::SingleGame);
    }

    #[test]
    fn reconcile_does_not_override_setting_on_home_team_mismatch() {
        // Home-team disagreement is a warn-only signal: setting should not change.
        let mut setting = make_setting(
            NaiveDate::from_ymd_opt(1990, 4, 1).unwrap(),
            DoubleheaderStatus::DoubleHeaderGame1,
        );
        let teams = make_teams("XYZ", "AWY"); // info,hometeam = XYZ, but ID prefix = ABC
        reconcile_with_game_id(
            make_game_id("ABC199004011"),
            &mut setting,
            &teams,
            full_presence(),
        );
        assert_eq!(
            setting.doubleheader_status,
            DoubleheaderStatus::DoubleHeaderGame1
        );
    }

    #[test]
    fn info_presence_detects_each_record_independently() {
        let only_date = vec![MappedRecord::Info(InfoRecord::GameDate(
            NaiveDate::from_ymd_opt(2024, 4, 1).unwrap(),
        ))];
        let p = info_presence(&only_date);
        assert!(p.date);
        assert!(!p.home_team);
        assert!(!p.doubleheader);

        let all = vec![
            MappedRecord::Info(InfoRecord::GameDate(
                NaiveDate::from_ymd_opt(2024, 4, 1).unwrap(),
            )),
            MappedRecord::Info(InfoRecord::HomeTeam(str_to_tinystr("BOS").unwrap())),
            MappedRecord::Info(InfoRecord::DoubleheaderStatus(
                DoubleheaderStatus::DoubleHeaderGame2,
            )),
        ];
        let p = info_presence(&all);
        assert!(p.date && p.home_team && p.doubleheader);
    }

    fn records(lines: &[&[&str]]) -> Vec<MappedRecord> {
        lines
            .iter()
            .map(|fields| {
                MappedRecord::try_from(&csv::StringRecord::from(fields.to_vec())).unwrap()
            })
            .collect()
    }

    const BLINE_STATS: [&str; 17] = [
        "4", "1", "1", "0", "0", "0", "0", "0", "0", "0", "0", "0", "0", "0", "0", "0", "0",
    ];

    fn bline<'a>(player: &'a str, side: &'a str, slot: &'a str, nth: &'a str) -> Vec<&'a str> {
        let mut fields = vec!["stat", "bline", player, side, slot, nth];
        fields.extend(BLINE_STATS);
        fields
    }

    #[test]
    fn rejects_batter_in_two_lineup_positions() {
        let rv = records(&[
            &bline("johnb111", "1", "1", "1"),
            &bline("johnb111", "1", "4", "1"),
        ]);
        let err = ensure_unique_lineup_slots(&rv).unwrap_err().to_string();
        assert!(err.contains("johnb111"), "{err}");
    }

    #[test]
    fn rejects_starter_in_two_lineup_positions() {
        let rv = records(&[
            &["start", "byrdb101", "Bill Byrd", "1", "8", "8"],
            &["start", "byrdb101", "Bill Byrd", "1", "9", "8"],
        ]);
        assert!(ensure_unique_lineup_slots(&rv).is_err());
    }

    #[test]
    fn accepts_substitutes_sharing_a_lineup_position() {
        let rv = records(&[
            &bline("starter1", "0", "9", "1"),
            &bline("pinchh01", "0", "9", "2"),
            &bline("starter1", "1", "4", "1"),
        ]);
        assert!(ensure_unique_lineup_slots(&rv).is_ok());
    }

    #[test]
    fn accepts_start_record_disagreeing_with_batting_line() {
        let rv = records(&[
            &["start", "barkm101", "Marvin Barker", "1", "8", "9"],
            &bline("barkm101", "1", "7", "1"),
        ]);
        assert!(ensure_unique_lineup_slots(&rv).is_ok());
    }

    #[test]
    fn accepts_two_way_player_batting_and_pitching_with_dh() {
        let rv = records(&[
            &["start", "ohtas001", "Shohei Ohtani", "0", "1", "10"],
            &["start", "ohtas001", "Shohei Ohtani", "0", "0", "1"],
        ]);
        assert!(ensure_unique_lineup_slots(&rv).is_ok());
    }
}

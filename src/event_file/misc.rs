use std::convert::TryFrom;
use std::fmt::Debug;
use std::str::FromStr;

use anyhow::{Error, Result, anyhow};
use bimap::BiMap;
use num_traits::PrimInt;
use regex::{Match, Regex};
use serde::{Deserialize, Serialize};
use strum_macros::{AsRefStr, EnumString};
use tracing::warn;

use crate::event_file::play::Base;
use crate::event_file::traits::{
    Batter, Fielder, FieldingPosition, LineupPosition, Pitcher, Player, RetrosheetEventRecord, Side,
};

use super::play::BaseRunner;
use super::schemas::GameIdString;

pub type Comment = String;

/// Indicates the hands that the batter/pitcher are using. For the most part, this is not given
/// explicitly, but occasionally the batter bats from a different side than his roster data
/// indicates, and under very rare circumstances the pitcher can switch.
#[derive(
    Debug, Default, Eq, PartialEq, EnumString, Copy, Clone, Serialize, Deserialize, AsRefStr,
)]
pub enum Hand {
    #[strum(serialize = "L")]
    Left,
    #[strum(serialize = "R")]
    Right,
    #[default]
    Default,
}

#[derive(Ord, PartialOrd, Debug, Copy, Clone, Eq, PartialEq, Serialize, Deserialize, Hash)]
pub struct GameId {
    pub id: GameIdString,
}
impl TryFrom<&RetrosheetEventRecord> for GameId {
    type Error = Error;

    fn try_from(record: &RetrosheetEventRecord) -> Result<Self> {
        let record = record.deserialize::<[&str; 2]>(None)?;
        Ok(Self {
            id: str_to_tinystr(record[1])?,
        })
    }
}

#[derive(Debug, Eq, PartialEq, Copy, Clone)]
pub struct HandAdjustment {
    pub player_id: Player,
    pub hand: Hand,
}
pub type BatHandAdjustment = HandAdjustment;
pub type PitchHandAdjustment = HandAdjustment;

impl TryFrom<&RetrosheetEventRecord> for HandAdjustment {
    type Error = Error;

    fn try_from(record: &RetrosheetEventRecord) -> Result<Self> {
        let record = record.deserialize::<[&str; 3]>(None)?;

        Ok(Self {
            player_id: str_to_tinystr(record[1])?,
            hand: Hand::from_str(record[2])?,
        })
    }
}

#[derive(Debug, Copy, Clone, Eq, PartialEq)]
pub struct LineupAdjustment {
    side: Side,
    lineup_position: LineupPosition,
}

impl TryFrom<&RetrosheetEventRecord> for LineupAdjustment {
    type Error = Error;

    fn try_from(record: &RetrosheetEventRecord) -> Result<Self> {
        let record = record.deserialize::<[&str; 3]>(None)?;

        Ok(Self {
            side: Side::from_str(record[1])?,
            lineup_position: LineupPosition::try_from(record[2])?,
        })
    }
}

#[derive(Debug, PartialEq, Eq, Clone, Serialize)]
pub struct AppearanceRecord {
    pub player: Player,
    pub player_name: String,
    pub side: Side,
    pub lineup_position: LineupPosition,
    pub fielding_position: FieldingPosition,
}

impl TryFrom<&RetrosheetEventRecord> for AppearanceRecord {
    type Error = Error;

    fn try_from(record: &RetrosheetEventRecord) -> Result<Self> {
        let record = record.deserialize::<[&str; 6]>(None)?;
        Ok(Self {
            player: str_to_tinystr(record[1])?,
            player_name: record[2].to_string(),
            side: Side::from_str(record[3])?,
            lineup_position: LineupPosition::try_from(record[4])?,
            fielding_position: FieldingPosition::try_from(record[5].trim_end())?,
        })
    }
}

pub type StartRecord = AppearanceRecord;
pub type SubstitutionRecord = AppearanceRecord;

#[derive(Debug, Eq, PartialEq, Copy, Clone, Serialize)]
pub struct EarnedRunRecord {
    pub pitcher_id: Pitcher,
    pub earned_runs: u8,
}

impl TryFrom<&RetrosheetEventRecord> for EarnedRunRecord {
    type Error = Error;

    fn try_from(record: &RetrosheetEventRecord) -> Result<Self> {
        let arr = record.deserialize::<[&str; 4]>(None)?;
        match arr[1] {
            "er" => Ok(Self {
                pitcher_id: str_to_tinystr(arr[2])?,
                earned_runs: arr[3].trim_end().parse::<u8>()?,
            }),
            _ => Err(anyhow!("Unexpected `data` type value {:?}", record)),
        }
    }
}

/// This is for the extra-inning courtesy runner introduced in 2020
#[derive(Debug, Eq, PartialEq, Copy, Clone)]
pub struct RunnerAdjustment {
    pub runner_id: Batter,
    pub base: Base,
}

impl TryFrom<&RetrosheetEventRecord> for RunnerAdjustment {
    type Error = Error;

    fn try_from(record: &RetrosheetEventRecord) -> Result<Self> {
        let record = record.deserialize::<[&str; 3]>(None)?;

        Ok(Self {
            runner_id: str_to_tinystr(record[1])?,
            base: Base::from_str(record[2])?,
        })
    }
}

#[derive(Debug, Eq, PartialEq, Copy, Clone)]
pub struct PitcherResponsibilityAdjustment {
    pub pitcher_id: Pitcher,
    pub baserunner: BaseRunner,
}

impl TryFrom<&RetrosheetEventRecord> for PitcherResponsibilityAdjustment {
    type Error = Error;

    fn try_from(record: &RetrosheetEventRecord) -> Result<Self> {
        let record = record.deserialize::<[&str; 3]>(None)?;

        Ok(Self {
            pitcher_id: str_to_tinystr(record[1])?,
            baserunner: BaseRunner::from_str(record[2])?,
        })
    }
}

pub type Lineup = BiMap<LineupPosition, Batter>;
pub type Defense = BiMap<FieldingPosition, Fielder>;

#[inline]
pub fn parse_positive_int<T: PrimInt + FromStr>(int_str: &str) -> Option<T> {
    int_str.parse::<T>().ok().filter(|i| !i.is_zero())
}

#[inline]
pub fn parse_non_negative_int<T: PrimInt + FromStr>(int_str: &str) -> Option<T> {
    int_str.parse::<T>().ok()
}

#[inline]
pub fn digit_vec(int_str: &str) -> Vec<u8> {
    int_str
        .chars()
        .filter_map(|c| c.to_digit(10))
        .map(|u| match u.try_into() {
            Ok(u) => u,
            Err(e) => {
                warn!("Impossible error converting u32 to u8: {}", e);
                0
            }
        })
        .collect()
}

#[inline]
pub fn str_to_tinystr<T: FromStr>(s: &str) -> Result<T> {
    T::from_str(s).map_err(|_| anyhow!("TinyStr {s} not formatted properly"))
}

#[inline]
pub fn regex_split<'a>(s: &'a str, re: &'static Regex) -> (&'a str, Option<&'a str>) {
    re.find(s)
        .map_or((s, None), |m| (&s[..m.start()], Some(&s[m.start()..])))
}

#[inline]
pub fn to_str_vec(match_vec: Vec<Option<Match<'_>>>) -> Vec<&str> {
    match_vec
        .into_iter()
        .filter_map(|o| o.map(|m| m.as_str()))
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use csv::StringRecord;
    use lazy_regex::regex;

    fn rec(fields: &[&str]) -> StringRecord {
        StringRecord::from(fields.to_vec())
    }

    #[test]
    fn parse_positive_int_rejects_zero_and_invalid() {
        assert_eq!(parse_positive_int::<u8>("5"), Some(5));
        assert_eq!(parse_positive_int::<u8>("0"), None);
        assert_eq!(parse_positive_int::<u8>(""), None);
        assert_eq!(parse_positive_int::<u8>("abc"), None);
    }

    #[test]
    fn parse_non_negative_int_keeps_zero_rejects_unparseable() {
        // Non-negativity is enforced by `T::from_str`, not the helper itself.
        // Callers always use unsigned types, so negative inputs fail to parse.
        assert_eq!(parse_non_negative_int::<u32>("0"), Some(0));
        assert_eq!(parse_non_negative_int::<u32>("1234"), Some(1234));
        assert_eq!(parse_non_negative_int::<u32>("-1"), None);
        assert_eq!(parse_non_negative_int::<u32>("xx"), None);
        // Confirm the function is generic and the negative rejection comes from
        // unsigned `from_str` — with `i32`, `-1` is accepted.
        assert_eq!(parse_non_negative_int::<i32>("-1"), Some(-1));
    }

    #[test]
    fn digit_vec_extracts_only_digit_chars() {
        assert_eq!(digit_vec("1a2b3"), vec![1u8, 2, 3]);
        assert_eq!(digit_vec(""), Vec::<u8>::new());
        assert_eq!(digit_vec("987"), vec![9u8, 8, 7]);
    }

    #[test]
    fn regex_split_returns_prefix_and_optional_match() {
        let re = regex!(r"/.*");
        let (head, tail) = regex_split("S9/L", re);
        assert_eq!(head, "S9");
        assert_eq!(tail, Some("/L"));

        let (head, tail) = regex_split("S9", re);
        assert_eq!(head, "S9");
        assert_eq!(tail, None);

        // Match at index 0 yields an empty prefix.
        let (head, tail) = regex_split("/L", re);
        assert_eq!(head, "");
        assert_eq!(tail, Some("/L"));
    }

    #[test]
    fn hand_parses_known_codes() {
        assert_eq!(Hand::from_str("L").unwrap(), Hand::Left);
        assert_eq!(Hand::from_str("R").unwrap(), Hand::Right);
        assert!(Hand::from_str("X").is_err());
        assert_eq!(Hand::default(), Hand::Default);
    }

    #[test]
    fn earned_run_record_parses_data_er() {
        let record = rec(&["data", "er", "smitj001", "3"]);
        let parsed = EarnedRunRecord::try_from(&record).unwrap();
        assert_eq!(parsed.earned_runs, 3);
    }

    #[test]
    fn earned_run_record_rejects_non_er_data_type() {
        let record = rec(&["data", "xx", "smitj001", "3"]);
        assert!(EarnedRunRecord::try_from(&record).is_err());
    }

    #[test]
    fn appearance_record_parses_start() {
        let record = rec(&["start", "smitj001", "Joe Smith", "1", "3", "5"]);
        let app = AppearanceRecord::try_from(&record).unwrap();
        assert_eq!(app.player_name, "Joe Smith");
        assert_eq!(app.side, Side::Home);
    }

    #[test]
    fn game_id_parses_id_record() {
        let record = rec(&["id", "BOS202404010"]);
        let id = GameId::try_from(&record).unwrap();
        assert_eq!(id.id.as_str(), "BOS202404010");
    }
}

use std::convert::TryFrom;
use std::str::FromStr;

use anyhow::{anyhow, Error, Result};
use bimap::BiMap;
use num_traits::PrimInt;
use regex::{Match, Regex};
use serde::{Deserialize, Serialize};
use strum_macros::EnumString;

use crate::event_file::play::Base;
use crate::event_file::traits::{
    Batter, Fielder, FieldingPosition, LineupPosition, Pitcher, Player, RetrosheetEventRecord, Side,
};

use super::schemas::GameIdString;

pub type Comment = String;

/// Indicates the hands that the batter/pitcher are using. For the most part, this is not given
/// explicitly, but occasionally the batter bats from a different side than his roster data
/// indicates, and under very rare circumstances the pitcher can switch.
#[derive(Debug, Eq, PartialEq, EnumString, Copy, Clone, Serialize, Deserialize)]
pub enum Hand {
    #[strum(serialize = "L")]
    Left,
    #[strum(serialize = "R")]
    Right,
    Default,
}

impl Default for Hand {
    fn default() -> Self {
        Self::Default
    }
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
    pub base: Base,
}

impl TryFrom<&RetrosheetEventRecord> for PitcherResponsibilityAdjustment {
    type Error = Error;

    fn try_from(record: &RetrosheetEventRecord) -> Result<Self> {
        let record = record.deserialize::<[&str; 3]>(None)?;

        Ok(Self {
            pitcher_id: str_to_tinystr(record[1])?,
            base: Base::from_str(record[2])?,
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
        .map(|u| u.try_into().unwrap())
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
pub fn to_str_vec(match_vec: Vec<Option<Match>>) -> Vec<&str> {
    match_vec
        .into_iter()
        .filter_map(|o| o.map(|m| m.as_str()))
        .collect()
}

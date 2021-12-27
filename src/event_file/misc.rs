use std::convert::TryFrom;
use std::str::FromStr;

use anyhow::{anyhow, Error, Result};
use bimap::BiMap;
use serde::{Deserialize, Serialize};
use strum_macros::EnumString;
use tinystr::TinyStr16;

use crate::event_file::play::Base;
use crate::event_file::traits::{
    Batter, Fielder, FieldingPosition, LineupPosition, Pitcher, Player, RetrosheetEventRecord, Side,
};
use crate::util::str_to_tinystr;

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
    pub id: TinyStr16,
}
impl TryFrom<&RetrosheetEventRecord> for GameId {
    type Error = Error;

    fn try_from(record: &RetrosheetEventRecord) -> Result<GameId> {
        let record = record.deserialize::<[&str; 2]>(None)?;
        Ok(GameId {
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

    fn try_from(record: &RetrosheetEventRecord) -> Result<HandAdjustment> {
        let record = record.deserialize::<[&str; 3]>(None)?;

        Ok(HandAdjustment {
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

    fn try_from(record: &RetrosheetEventRecord) -> Result<LineupAdjustment> {
        let record = record.deserialize::<[&str; 3]>(None)?;

        Ok(LineupAdjustment {
            side: Side::from_str(record[1])?,
            lineup_position: LineupPosition::try_from(record[2])?,
        })
    }
}

#[derive(Debug, PartialEq, Eq, Clone)]
pub struct AppearanceRecord {
    pub player: Player,
    pub player_name: String,
    pub side: Side,
    pub lineup_position: LineupPosition,
    pub fielding_position: FieldingPosition,
}

impl TryFrom<&RetrosheetEventRecord> for AppearanceRecord {
    type Error = Error;

    fn try_from(record: &RetrosheetEventRecord) -> Result<AppearanceRecord> {
        let record = record.deserialize::<[&str; 6]>(None)?;
        Ok(AppearanceRecord {
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

#[derive(Debug, Eq, PartialEq, Copy, Clone)]
pub struct EarnedRunRecord {
    pub pitcher_id: Pitcher,
    pub earned_runs: u8,
}

impl TryFrom<&RetrosheetEventRecord> for EarnedRunRecord {
    type Error = Error;

    fn try_from(record: &RetrosheetEventRecord) -> Result<EarnedRunRecord> {
        let arr = record.deserialize::<[&str; 4]>(None)?;
        match arr[1] {
            "er" => Ok(EarnedRunRecord {
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

pub type Lineup = BiMap<LineupPosition, Batter>;
pub type Defense = BiMap<FieldingPosition, Fielder>;

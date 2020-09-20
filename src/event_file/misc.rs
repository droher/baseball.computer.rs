use std::str::FromStr;

use anyhow::{Result};
use strum_macros::EnumString;

use crate::event_file::traits::{FromRetrosheetRecord, RetrosheetEventRecord, LineupPosition, Player, FieldingPosition, Pitcher, Side};
use std::convert::TryFrom;
use tinystr::{TinyStr16};
use crate::util::str_to_tinystr;

pub type Comment = String;

#[derive(Debug, Eq, PartialEq, EnumString, Copy, Clone)]
enum Hand {L, R, S, B}

#[derive(Debug, Copy, Clone, Eq, PartialEq)]
pub struct GameId {pub id: TinyStr16}
impl FromRetrosheetRecord for GameId {

    fn new(record: &RetrosheetEventRecord) -> Result<GameId> {
        let record = record.deserialize::<[&str; 2]>(None)?;
        Ok(GameId { id: str_to_tinystr(record[1])? })
    }
}

#[derive(Debug, Eq, PartialEq, Copy, Clone)]
pub struct HandAdjustment {player_id: Player, hand: Hand}
pub type BatHandAdjustment = HandAdjustment;
pub type PitchHandAdjustment = HandAdjustment;

impl FromRetrosheetRecord for HandAdjustment {
    fn new(record: &RetrosheetEventRecord) -> Result<HandAdjustment> {
        let record = record.deserialize::<[&str; 3]>(None)?;

        Ok(HandAdjustment {
            player_id: str_to_tinystr(record[1])?,
            hand: Hand::from_str(record[2])?
        })
    }
}

#[derive(Debug, Copy, Clone, Eq, PartialEq)]
pub struct LineupAdjustment { side: Side, lineup_position: LineupPosition}

impl FromRetrosheetRecord for LineupAdjustment {
    fn new(record: &RetrosheetEventRecord) -> Result<LineupAdjustment> {
        let record = record.deserialize::<[&str; 3]>(None)?;

        Ok(LineupAdjustment {
            side: Side::from_str(record[1])?,
            lineup_position: LineupPosition::try_from(record[2])?,
        })
    }
}

#[derive(Debug, PartialEq, Eq, Copy, Clone)]
pub struct AppearanceRecord {
    pub player: Player,
    pub side: Side,
    pub lineup_position: LineupPosition,
    pub fielding_position: FieldingPosition
}
impl FromRetrosheetRecord for AppearanceRecord {
    fn new(record: &RetrosheetEventRecord) -> Result<AppearanceRecord> {
        let record = record.deserialize::<[&str; 6]>(None)?;
        Ok(AppearanceRecord {
            player: str_to_tinystr(record[1])?,
            side: Side::from_str(record[3])?,
            lineup_position: LineupPosition::try_from(record[4])?,
            fielding_position:  FieldingPosition::try_from(record[5].trim_end())?
        })
    }
}

pub type StartRecord = AppearanceRecord;
pub type SubstitutionRecord = AppearanceRecord;

#[derive(Debug, Eq, PartialEq, Copy, Clone)]
pub struct EarnedRunRecord {
    pitcher_id: Pitcher,
    earned_runs: u8
}

impl FromRetrosheetRecord for EarnedRunRecord {
    fn new(record: &RetrosheetEventRecord) -> Result<EarnedRunRecord> {
        let arr = record.deserialize::<[&str; 4]>(None)?;
        match arr[1] {
            "er" => Ok(EarnedRunRecord {
                pitcher_id: str_to_tinystr(arr[2])?,
                earned_runs: arr[3].trim_end().parse::<u8>()?
            }),
            _ => Err(Self::error("Unexpected `data` type value", record))
        }
    }
}

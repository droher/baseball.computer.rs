use std::str::FromStr;

use anyhow::{Result};
use strum_macros::EnumString;

use crate::event_file::traits::{FromRetrosheetRecord, RetrosheetEventRecord, LineupPosition, Player, FieldingPosition, Pitcher, Side};

pub type Comment = String;

#[derive(Debug, Eq, PartialEq, EnumString)]
enum Hand {L, R, S, B}

#[derive(Debug)]
pub struct GameId {pub id: String}
impl FromRetrosheetRecord for GameId {

    fn new(record: &RetrosheetEventRecord) -> Result<GameId> {
        let record = record.deserialize::<[&str; 2]>(None)?;
        Ok(GameId { id: String::from(record[1]) })
    }
}

#[derive(Debug)]
pub struct HandAdjustment {player_id: String, hand: Hand}
pub type BatHandAdjustment = HandAdjustment;
pub type PitchHandAdjustment = HandAdjustment;

impl FromRetrosheetRecord for HandAdjustment {
    fn new(record: &RetrosheetEventRecord) -> Result<HandAdjustment> {
        let record = record.deserialize::<[&str; 3]>(None)?;

        Ok(HandAdjustment {
            player_id: String::from(record[1]),
            hand: Hand::from_str(record[2])?
        })
    }
}

#[derive(Debug)]
pub struct LineupAdjustment { side: Side, lineup_position: LineupPosition}

impl FromRetrosheetRecord for LineupAdjustment {
    fn new(record: &RetrosheetEventRecord) -> Result<LineupAdjustment> {
        let record = record.deserialize::<[&str; 3]>(None)?;

        Ok(LineupAdjustment {
            side: Side::from_str(record[1])?,
            lineup_position: record[2].parse::<LineupPosition>()?,
        })
    }
}

#[derive(Debug)]
pub struct AppearanceRecord {
    player: Player,
    side: Side,
    lineup_position: LineupPosition,
    fielding_position: FieldingPosition
}
impl FromRetrosheetRecord for AppearanceRecord {
    fn new(record: &RetrosheetEventRecord) -> Result<AppearanceRecord> {
        let record = record.deserialize::<[&str; 6]>(None)?;
        Ok(AppearanceRecord {
            player: String::from(record[1]),
            side: Side::from_str(record[3])?,
            lineup_position: record[4].parse::<LineupPosition>()?,
            fielding_position:  record[5].trim_end().parse::<FieldingPosition>()?
        })
    }
}

pub type StartRecord = AppearanceRecord;
pub type SubstitutionRecord = AppearanceRecord;

#[derive(Debug)]
pub struct EarnedRunRecord {
    pitcher_id: Pitcher,
    earned_runs: u8
}

impl FromRetrosheetRecord for EarnedRunRecord {
    fn new(record: &RetrosheetEventRecord) -> Result<EarnedRunRecord> {
        let arr = record.deserialize::<[&str; 4]>(None)?;
        match arr[1] {
            "er" => Ok(EarnedRunRecord {
                pitcher_id: String::from(arr[2]),
                earned_runs: arr[3].trim_end().parse::<u8>()?
            }),
            _ => Err(Self::error("Unexpected `data` type value", record))
        }
    }
}

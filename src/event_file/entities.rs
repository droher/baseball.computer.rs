use std::convert::TryFrom;
use std::str::FromStr;

use anyhow::{Context, Result};
use strum_macros::EnumString;

use crate::event_file::box_score::{BoxScoreEvent, BoxScoreLine, LineScore};
use crate::event_file::play::{PitchSequence, Play};
use crate::event_file::traits::{FromRetrosheetRecord, RetrosheetEventRecord, LineupPosition, Player, FieldingPosition, Inning, Batter, Pitcher, Side};
use crate::event_file::info::InfoRecord;

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
struct Count { balls: Option<u8>, strikes: Option<u8> }
impl Count {
    fn new(count_str: &str) -> Result<Count> {
        let mut ints = count_str.chars().map(|c| c.to_digit(10).map(|i| i as u8));

        Ok(Count {
            balls: ints.next().flatten(),
            strikes: ints.next().flatten()
        })
    }
}

#[derive(Debug)]
pub struct PlayRecord {
    inning: Inning,
    side: Side,
    batter: Batter,
    count: Count,
    pub pitch_sequence: Option<PitchSequence>,
    pub play: Play
}

impl FromRetrosheetRecord for PlayRecord {
    fn new(record: &RetrosheetEventRecord) -> Result<PlayRecord> {
        let record = record.deserialize::<[&str; 7]>(None)?;
        Ok(PlayRecord {
            inning: record[1].parse::<Inning>()?,
            side: Side::from_str(record[2])?,
            batter: String::from(record[3]),
            count: Count::new(record[4])?,
            pitch_sequence: {match record[5] {"" => None, s => Some(PitchSequence::try_from(s)?)}},
            play: Play::try_from(record[6])?
        })
    }
}

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

#[derive(Debug)]
pub enum MappedRecord {
    GameId(GameId),
    Version,
    Info(InfoRecord),
    Start(StartRecord),
    Substitution(SubstitutionRecord),
    Play(PlayRecord),
    BatHandAdjustment(BatHandAdjustment),
    PitchHandAdjustment(PitchHandAdjustment),
    LineupAdjustment(LineupAdjustment),
    EarnedRun(EarnedRunRecord),
    Comment(Comment),
    BoxScoreLine(BoxScoreLine),
    LineScore(LineScore),
    BoxScoreEvent(BoxScoreEvent),
    Unrecognized
}

impl FromRetrosheetRecord for MappedRecord {
    fn new(record: &RetrosheetEventRecord) -> Result<MappedRecord>{
        let line_type = record.get(0).context("No record")?;
        let mapped= match line_type {
            "id" | "7d" => MappedRecord::GameId(GameId::new(record)?),
            "version" => MappedRecord::Version,
            "info" => MappedRecord::Info(InfoRecord::new(record)?),
            "start" => MappedRecord::Start(StartRecord::new(record)?),
            "sub" => MappedRecord::Substitution(SubstitutionRecord::new(record)?),
            "play" => MappedRecord::Play(PlayRecord::new(record)?),
            "badj" => MappedRecord::BatHandAdjustment(BatHandAdjustment::new(record)?),
            "padj" => MappedRecord::PitchHandAdjustment(PitchHandAdjustment::new(record)?),
            "ladj" => MappedRecord::LineupAdjustment(LineupAdjustment::new(record)?),
            "com" => MappedRecord::Comment(String::from(record.get(1).unwrap())),
            "data" => MappedRecord::EarnedRun(EarnedRunRecord::new(record)?),
            "stat" => MappedRecord::BoxScoreLine(BoxScoreLine::new(record)?),
            "line" => MappedRecord::LineScore(LineScore::new(record)?),
            "event" => MappedRecord::BoxScoreEvent(BoxScoreEvent::new(record)?),
            _ => MappedRecord::Unrecognized
        };
        match mapped {
            MappedRecord::Unrecognized => Err(Self::error("Unrecognized record type", record)),
            _ => Ok(mapped)
        }
    }
}
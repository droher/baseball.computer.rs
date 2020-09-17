use std::fs::File;
use std::io::BufReader;

use anyhow::{Result, Context, Error, anyhow};
use csv::{ReaderBuilder, StringRecord, Reader};

use crate::event_file::misc::{GameId, StartRecord, SubstitutionRecord, BatHandAdjustment, PitchHandAdjustment, LineupAdjustment, EarnedRunRecord, Comment};
use crate::event_file::traits::{FromRetrosheetRecord, RetrosheetEventRecord, Umpire, Batter, Pitcher, RetrosheetVolunteer, Scorer, Player, LineupPosition, FieldingPosition};
use crate::event_file::info::{InfoRecord, Team, GameType, DayNight, WindDirection, Sky, Park, PitchDetail, HowScored};
use crate::event_file::box_score::{BoxScoreLine, LineScore, BoxScoreEvent};
use crate::event_file::play::PlayRecord;
use std::convert::TryFrom;
use chrono::{NaiveDate, NaiveTime};
use serde::{Deserialize, Deserializer};
use std::iter::TakeWhile;
use serde::de::Unexpected::Map;


pub struct Matchup<T> { pub(crate) away: T, pub(crate) home: T }

pub type Teams = Matchup<Team>;
pub type StartingLineups = Matchup<Lineup>;

/// Zero is the pitcher if there's a DH
pub struct Lineup([LineupPosition; 10]);
/// Zero is the DH if applicable
pub struct Defense([FieldingPosition; 10]);

/// Contains the information provided in the Retrosheet info and start fields.
pub struct GameInfo {
    setting: GameSetting,
    pub starting_lineups: Matchup<Lineup>,
    pub starting_defense: Matchup<Defense>,
    results: GameResults,
    retrosheet_metadata: GameRetrosheetMetadata
}

pub struct GameUmpires {
    home: Option<Umpire>,
    first: Option<Umpire>,
    second: Option<Umpire>,
    third: Option<Umpire>,
    left: Option<Umpire>,
    right: Option<Umpire>
}

pub struct GameSetting {
    date: NaiveDate,
    game_type: GameType,
    start_time: Option<NaiveTime>,
    time_of_day: Option<DayNight>,
    use_dh: bool,
    home_team_bats_first: bool,
    sky: Option<Sky>,
    temp: Option<u8>,
    wind_direction: Option<WindDirection>,
    wind_speed: Option<u8>,
    attendance: Option<u32>,
    park: Park,
    umpires: GameUmpires
}

/// Info fields relating to how the game was scored, obtained, and inputted.
pub struct GameRetrosheetMetadata {
    pitch_detail: PitchDetail,
    edit_time: Option<String>,
    scoring_method: HowScored,
    input_program_version: Option<String>,
    inputter: Option<RetrosheetVolunteer>,
    input_time: Option<String>,
    scorer: Option<Scorer>,
    original_scorer: Option<Scorer>,
    translator: Option<RetrosheetVolunteer>
}

/// These fields only refer to data from the info section, and thus do not include
/// any kind of box score data.
pub struct GameResults {
    winning_pitcher: Option<Pitcher>,
    losing_pitcher: Option<Pitcher>,
    save: Option<Pitcher>,
    game_winning_rbi: Option<Batter>,
    time_of_game_minutes: Option<u16>,
}

pub struct RetrosheetReader(Reader<BufReader<File>>);

impl RetrosheetReader {
    pub fn next_game(&mut self) -> Result<Vec<MappedRecord>>{
        let mut v = Vec::with_capacity(200);
        if self.0.is_done() {return Ok(Vec::new())}
        for r in self.0.records() {
            let mapped = MappedRecord::new(&r?)?;
            if let MappedRecord::GameId(g) = &mapped {v.push(mapped); break}
            v.push(mapped)
        }
        Ok(v)
    }
}

impl TryFrom<&str> for RetrosheetReader {
    type Error = Error;

    fn try_from(path: &str) -> Result<Self> {
        Ok(
            RetrosheetReader(
                ReaderBuilder::new()
                    .has_headers(false)
                    .flexible(true)
                    .from_reader(BufReader::new(File::open(path)?)),
            )
        )
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
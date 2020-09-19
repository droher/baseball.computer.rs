use std::fs::File;
use std::io::BufReader;

use anyhow::{Result, Context, Error, anyhow};
use csv::{ReaderBuilder, StringRecord, Reader};

use crate::event_file::misc::{GameId, StartRecord, SubstitutionRecord, BatHandAdjustment, PitchHandAdjustment, LineupAdjustment, EarnedRunRecord, Comment};
use crate::event_file::traits::{FromRetrosheetRecord, RetrosheetEventRecord, Umpire, Batter, Pitcher, RetrosheetVolunteer, Scorer, Player, LineupPosition, FieldingPosition, Fielder, Side};
use crate::event_file::info::{InfoRecord, Team, GameType, DayNight, WindDirection, Sky, Park, PitchDetail, HowScored};
use crate::event_file::box_score::{BoxScoreLine, LineScore, BoxScoreEvent};
use crate::event_file::play::PlayRecord;
use std::convert::TryFrom;
use chrono::{NaiveDate, NaiveTime};
use serde::{Deserialize, Deserializer};
use std::iter::TakeWhile;
use serde::de::Unexpected::Map;
use smallvec::SmallVec;
use smallvec::alloc::collections::BTreeMap;
use std::collections::HashMap;
use num_traits::PrimInt;
use std::ops::Deref;
use crate::event_file::play::PlayType::DefensiveIndifference;


pub struct Matchup<T> { away: T, home: T }

impl<T> Matchup<T> {
    pub fn new(away: T, home: T) -> Self {
        Self {away, home}
    }
}

impl<T: Default> Default for Matchup<T> {
    fn default() -> Self {
        Self {away: T::default(), home: T::default() }
    }
}

pub type Teams = Matchup<Team>;
pub type StartingLineups = Matchup<Lineup>;

pub type Lineup = HashMap<LineupPosition, Batter>;
pub type Defense = HashMap<FieldingPosition, Fielder>;

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

pub struct RetrosheetReader {
    reader: Reader<BufReader<File>>,
    current_record: StringRecord,
    current_game_id: GameId
}

pub type RecordVec = Vec<MappedRecord>;

fn assemble_lineups_and_defense(start_records: Vec<StartRecord>) -> (Matchup<Lineup>, Matchup<Defense>)  {
    // TODO: DRY
    let (mut away_lineup, mut home_lineup) = (Lineup::with_capacity(10), Lineup::with_capacity(10));
    let (mut away_defense, mut home_defense) = (Defense::with_capacity(10), Defense::with_capacity(10));
    let (away_records, home_records): (Vec<StartRecord>, Vec<StartRecord>) = start_records.into_iter()
        // TODO: Partition in place once method stabilized
        .partition(|sr| sr.side == Side::Away);

    away_records.into_iter().zip(home_records).map(|(away, home)| {
        away_lineup.insert(away.lineup_position, away.player.clone());
        away_defense.insert(away.fielding_position, away.player);
        home_lineup.insert(home.lineup_position, home.player.clone());
        home_defense.insert(home.fielding_position, home.player);
    }).for_each(drop);

    (Matchup::new(away_lineup, home_lineup),
     Matchup::new(away_defense, home_defense))
}

impl Iterator for RetrosheetReader {
    type Item = Result<RecordVec>;

    fn next(&mut self) -> Option<Self::Item> {
        match self.next_game() {
            Ok(v) if v.is_empty() => None,
            r => Some(r)
        }
    }
}

impl RetrosheetReader {
    fn next_game(&mut self) -> Result<RecordVec> {
        let mut v = RecordVec::with_capacity(128);
        if self.reader.is_done() {return Ok(v)}
        v.push(MappedRecord::GameId(self.current_game_id.to_owned()));
        loop {
            self.reader.read_record(&mut self.current_record)?;
            let mapped_record = MappedRecord::new(&self.current_record)?;
            if let MappedRecord::GameId(g) = mapped_record {
                self.current_game_id = g;
                break
            } else { v.push(mapped_record) }
        }
        Ok(v)
    }

}

impl TryFrom<&str> for RetrosheetReader {
    type Error = Error;

    fn try_from(path: &str) -> Result<Self> {
        let mut reader = ReaderBuilder::new()
                    .has_headers(false)
                    .flexible(true)
                    .from_reader(BufReader::new(File::open(path)?));
        let mut current_record = StringRecord::new();
        reader.read_record(&mut current_record)?;
        let current_game_id = match MappedRecord::new(&current_record)? {
            MappedRecord::GameId(g) => Ok(g),
            _ => Err(anyhow!("First record was not a game ID, cannot read file"))
        }?;
        Ok(Self {reader, current_record, current_game_id})
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
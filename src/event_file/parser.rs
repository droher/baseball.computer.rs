use std::collections::HashMap;
use std::convert::{TryFrom, TryInto};
use std::fs::File;
use std::io::BufReader;
use std::str::FromStr;

use anyhow::{anyhow, Context, Error, Result};
use chrono::{NaiveDate, NaiveTime};
use csv::{Reader, ReaderBuilder, StringRecord};
use tinystr::{TinyStr8, TinyStr16};

use crate::event_file::box_score::{BoxScoreEvent, BoxScoreLine, LineScore};
use crate::event_file::info::{DayNight, FieldCondition, DoubleheaderStatus, HowScored, InfoRecord, Park, PitchDetail, Precipitation, Sky, Team, UmpirePosition, WindDirection};
use crate::event_file::misc::{BatHandAdjustment, Comment, EarnedRunRecord, GameId, LineupAdjustment, PitchHandAdjustment, StartRecord, SubstitutionRecord, Lineup, Defense};
use crate::event_file::play::PlayRecord;
use crate::event_file::traits::{Batter, Pitcher, RetrosheetEventRecord, RetrosheetVolunteer, Scorer, Side, Umpire};
use either::{Either, Left, Right};
use crate::event_file::pbp_to_box::{GameState, BoxScoreGame};
use std::path::PathBuf;
use serde::{Serialize, Serializer};
use serde::ser::SerializeStruct;
use itertools::Itertools;

#[derive(Debug, Eq, PartialEq, Clone)]
pub struct Matchup<T> {pub away: T, pub home: T}

impl<T: Clone> Matchup<T> {
    pub fn cloned_update(&self, side: &Side, new_val: T) -> Self {
        match side {
            Side::Away => Self {away: new_val, home: self.home.clone()},
            Side::Home => Self {home: new_val, away: self.away.clone()}
        }
    }
}

impl<T> Matchup<T> {
    pub fn new(away: T, home: T) -> Self {
        Self {away, home}
    }

    pub fn get(&self, side: &Side) -> &T {
        match side {
            Side::Away => &self.away,
            Side::Home => &self.home
        }
    }

    pub fn get_mut(&mut self, side: &Side) -> &mut T {
        match side {
            Side::Away => &mut self.away,
            Side::Home => &mut self.home
        }
    }

    pub fn get_both_mut(&mut self) -> (&mut T, &mut T) {
        (&mut self.away, &mut self.home)
    }

}

impl<T: Default> Default for Matchup<T> {
    fn default() -> Self {
        Self {away: T::default(), home: T::default() }
    }
}

impl <T: Sized + Clone> Matchup<T> {
    pub fn apply_both<F, U: Sized>(self, func: F) -> (U, U)
        where F: Copy + FnOnce(T) -> U
    {
        (func(self.away), func(self.home))
    }
}

impl <T: Serialize> Serialize for Matchup<T> {
    fn serialize<S>(&self, serializer: S) -> Result<<S as Serializer>::Ok, <S as Serializer>::Error> where
        S: Serializer {
        let mut state = serializer.serialize_struct("Matchup", 2)?;
        state.serialize_field("away", &self.away)?;
        state.serialize_field("home", &self.home)?;
        state.end()
    }
}

// TODO: Is there a rustier way to write?
impl<T: Copy> Copy for Matchup<T> {}

impl<T> From<(T, T)> for Matchup<T> {
    fn from(tup: (T, T)) -> Self {
        Matchup {away: tup.0, home: tup.1}
    }
}

impl TryFrom<&Vec<InfoRecord>> for Matchup<Team> {
    type Error = Error;

    fn try_from(infos: &Vec<InfoRecord>) -> Result<Self> {
        let home_team = infos.iter().find_map(|m| if let InfoRecord::HomeTeam(t) = m {Some(t)} else {None});
        let away_team = infos.iter().find_map(|m| if let InfoRecord::VisitingTeam(t) = m {Some(t)} else {None});
        Ok(Self {
            away: *away_team.context("Could not find away team info in records")?,
            home: *home_team.context("Could not find home team info in records")?
        })
    }
}

pub type Teams = Matchup<Team>;

pub type RecordVec = Vec<MappedRecord>;


pub struct RetrosheetReader {
    reader: Reader<BufReader<File>>,
    current_record: StringRecord,
    current_game_id: GameId,
    current_record_vec: RecordVec
}


impl Iterator for RetrosheetReader {
    type Item = Result<RecordVec>;

    fn next(&mut self) -> Option<Self::Item> {
        match self.next_game() {
            Err(e) => Some(Err(e)),
            Ok(true) => Some(Ok(self.current_record_vec.drain(..).collect())),
            _ if !&self.current_record_vec.is_empty() => Some(Ok(self.current_record_vec.drain(..).collect())),
            _ => None
        }
    }
}

impl RetrosheetReader {

    pub fn iter_box(&mut self) -> impl Iterator<Item=Result<BoxScoreGame>> + '_ {
        self.into_iter()
            .map_results(|rv| BoxScoreGame::try_from(&rv))
            .map(|r| r.and_then(|r| r))
    }

    fn next_game(&mut self) -> Result<bool> {
        if self.reader.is_done() {return Ok(false)}
        self.current_record_vec.push(MappedRecord::GameId(self.current_game_id));
        loop {
            let did_read = self.reader.read_record(&mut self.current_record)?;
            if !did_read {return Ok(false)}
            let mapped_record = MappedRecord::try_from(&self.current_record);
            match mapped_record {
                Ok(MappedRecord::GameId(g)) => {self.current_game_id = g; return Ok(true)},
                Ok(m) => {self.current_record_vec.push(m)}
                _ => println!("Error during game {} -- Error reading record: {:?}", &self.current_game_id.id, &self.current_record)
            }
        }
    }

}

impl TryFrom<&PathBuf> for RetrosheetReader {
    type Error = Error;

    fn try_from(path: &PathBuf) -> Result<Self> {
        let mut reader = ReaderBuilder::new()
                    .has_headers(false)
                    .flexible(true)
                    .from_reader(BufReader::new(File::open(path)?));
        let mut current_record = StringRecord::new();
        reader.read_record(&mut current_record)?;
        let current_game_id = match MappedRecord::try_from(&current_record)? {
            MappedRecord::GameId(g) => Ok(g),
            _ => Err(anyhow!("First record was not a game ID, cannot read file."))
        }?;
        let current_record_vec = RecordVec::new();
        Ok(Self {reader, current_record, current_game_id, current_record_vec})
    }
}


#[derive(Debug, Eq, PartialEq)]
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

pub type EventRecord = Either<PlayRecord, SubstitutionRecord>;

impl TryFrom<&RetrosheetEventRecord>for MappedRecord {
    type Error = Error;

    fn try_from(record: &RetrosheetEventRecord) -> Result<MappedRecord>{
        let line_type = record.get(0).context("No record")?;
        let mapped= match line_type {
            "id" => MappedRecord::GameId(GameId::try_from(record)?),
            "version" => MappedRecord::Version,
            "info" => MappedRecord::Info(InfoRecord::try_from(record)?),
            "start" => MappedRecord::Start(StartRecord::try_from(record)?),
            "sub" => MappedRecord::Substitution(SubstitutionRecord::try_from(record)?),
            "play" => MappedRecord::Play(PlayRecord::try_from(record)?),
            "badj" => MappedRecord::BatHandAdjustment(BatHandAdjustment::try_from(record)?),
            "padj" => MappedRecord::PitchHandAdjustment(PitchHandAdjustment::try_from(record)?),
            "ladj" => MappedRecord::LineupAdjustment(LineupAdjustment::try_from(record)?),
            "com" => MappedRecord::Comment(String::from(record.get(1).unwrap())),
            "data" => MappedRecord::EarnedRun(EarnedRunRecord::try_from(record)?),
            "stat" => MappedRecord::BoxScoreLine(BoxScoreLine::try_from(record)?),
            "line" => MappedRecord::LineScore(LineScore::try_from(record)?),
            "event" => MappedRecord::BoxScoreEvent(BoxScoreEvent::try_from(record)?),
            _ => MappedRecord::Unrecognized
        };
        match mapped {
            MappedRecord::Unrecognized => Err(anyhow!("Unrecognized record type {:?}", record)),
            _ => Ok(mapped)
        }
    }
}
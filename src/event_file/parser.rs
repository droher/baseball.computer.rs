use std::convert::TryFrom;
use std::fs::File;
use std::io::BufReader;
use std::path::{Path, PathBuf};

use anyhow::{anyhow, bail, Context, Error, Result};
use csv::{Reader, ReaderBuilder, StringRecord};
use glob::{Paths, glob, PatternError};
use lazy_static::lazy_static;
use regex::Regex;
use tracing::error;

use crate::event_file::box_score::{BoxScoreEvent, BoxScoreLine, LineScore};
use crate::event_file::info::InfoRecord;
use crate::event_file::misc::{
    BatHandAdjustment, Comment, EarnedRunRecord, GameId, LineupAdjustment, PitchHandAdjustment,
    RunnerAdjustment, StartRecord, SubstitutionRecord,
};
use crate::event_file::play::PlayRecord;
use crate::event_file::traits::{GameType, RetrosheetEventRecord};
use crate::Schema::Game;

pub type RecordVec = Vec<MappedRecord>;
pub type RecordSlice = [MappedRecord];


lazy_static! {
    static ref ALL_STAR_GAME: Regex = Regex::new(r"[0-9]{4}AS\.EVE$").unwrap();
    static ref WORLD_SERIES: Regex = Regex::new(r"[0-9]{4}WS\.EVE$").unwrap();
    static ref LCS: Regex = Regex::new(r"[0-9]{4}[AN]LCS\.EVE$").unwrap();
    static ref DIVISION_SERIES: Regex = Regex::new(r"[0-9]{4}[AN]LD[12]\.EVE$").unwrap();
    static ref WILD_CARD: Regex = Regex::new(r"[0-9]{4}[AN]W[C1234]\.EVE$").unwrap();
    static ref REGULAR_SEASON: Regex = Regex::new(r"[0-9]{4}([[:alnum:]]{3})?\.E[BVD][ANF]$").unwrap();
    static ref NEGRO_LEAGUES: Regex = Regex::new(r".*\.E[BV]$").unwrap();
    static ref PLAY_BY_PLAY: Regex = Regex::new(r".*\.EV[ANF]?").unwrap();
    static ref DERIVED: Regex = Regex::new(r".*\.ED[ANF]?").unwrap();
    static ref BOX_SCORE: Regex = Regex::new(r".*\.EB[ANF]?").unwrap();

}

pub enum AccountType {
    PlayByPlay,
    Derived,
    BoxScore
}

impl AccountType {
    pub fn glob(&self, input_prefix: &PathBuf) -> Result<Paths, PatternError> {
        let pattern = match self {
            Self::PlayByPlay => "**/*.EV*",
            Self::Derived => "**/*.ED*",
            Self::BoxScore => "**/*.EB*"
        };
        let input = input_prefix.join(Path::new(pattern)).to_str().unwrap_or_default().to_string();
        glob(&*input)
    }
}

pub struct FileInfo {
    filename: String,
    game_type: GameType,
    account_type: AccountType
}

impl From<&PathBuf> for FileInfo {
    fn from(path: &PathBuf) -> Self {
        let filename = path
            .file_name()
            .unwrap_or_default()
            .to_str()
            .unwrap_or_default()
            .to_string();
        Self {
            filename: filename.clone(),
            game_type: Self::game_type(&filename),
            account_type: Self::account_type(&filename)
        }
    }
}

impl FileInfo {
    fn game_type(s: &str) -> GameType {
        if REGULAR_SEASON.is_match(s) { GameType::RegularSeason }
        else if ALL_STAR_GAME.is_match(s) { GameType::AllStarGame }
        else if WORLD_SERIES.is_match(s) { GameType::WorldSeries }
        else if LCS.is_match(s) { GameType::LeagueChampionshipSeries }
        else if DIVISION_SERIES.is_match(s) { GameType::DivisionSeries }
        else if WILD_CARD.is_match(s) { GameType::WildCardSeries }
        else if NEGRO_LEAGUES.is_match(s) { GameType::NegroLeagues }
        else { GameType::Other }
    }

    fn account_type(s: &str) -> AccountType {
        if PLAY_BY_PLAY.is_match(s) { AccountType::PlayByPlay }
        else if BOX_SCORE.is_match(s) { AccountType::BoxScore }
        else if DERIVED.is_match(s) { AccountType::Derived }
        else { panic!("Unexpected file naming convention: {}", s) }
    }
}

pub struct RetrosheetReader {
    reader: Reader<BufReader<File>>,
    current_record: StringRecord,
    current_game_id: GameId,
    current_record_vec: RecordVec,
    pub file_info: FileInfo
}

impl Iterator for RetrosheetReader {
    type Item = Result<RecordVec>;

    fn next(&mut self) -> Option<Self::Item> {
        match self.next_game() {
            Err(e) => Some(Err(e)),
            Ok(true) => Some(Ok(self.current_record_vec.drain(..).collect())),
            _ if !&self.current_record_vec.is_empty() => {
                Some(Ok(self.current_record_vec.drain(..).collect()))
            }
            _ => None,
        }
    }
}

impl RetrosheetReader {
    fn next_game(&mut self) -> Result<bool> {
        if self.reader.is_done() {
            return Ok(false);
        }
        self.current_record_vec
            .push(MappedRecord::GameId(self.current_game_id));
        loop {
            let did_read = self.reader.read_record(&mut self.current_record)?;
            if !did_read {
                return Ok(false);
            }
            let mapped_record = MappedRecord::try_from(&self.current_record);
            match mapped_record {
                Ok(MappedRecord::GameId(g)) => {
                    self.current_game_id = g;
                    return Ok(true);
                }
                Ok(m) => self.current_record_vec.push(m),
                Err(e) => {
                    return Err(e.context(format!(
                        "Error during game {} -- Error reading record: {:?}",
                        &self.current_game_id.id, &self.current_record
                    )))
                }
            }
        }
    }
}

impl TryFrom<&PathBuf> for RetrosheetReader {
    type Error = Error;

    fn try_from(path: &PathBuf) -> Result<Self> {
        let mut reader = ReaderBuilder::new()
            .has_headers(false)
            .double_quote(false)
            .flexible(true)
            .from_reader(BufReader::new(File::open(path)?));
        let mut current_record = StringRecord::new();
        // Skip comments at top of 1991 files
        // TODO: Unmess
        loop {
            reader.read_record(&mut current_record)?;
            match MappedRecord::try_from(&current_record)? {
                MappedRecord::Comment(_) => {}
                _ => break,
            }
        }
        let current_game_id = match MappedRecord::try_from(&current_record)? {
            MappedRecord::GameId(g) => Ok(g),
            _ => Err(anyhow!("First non-comment record was not a game ID, cannot read file.")),
        }?;
        let current_record_vec = RecordVec::new();
        let file_info: FileInfo = path.into();
        Ok(Self {
            reader,
            current_record,
            current_game_id,
            current_record_vec,
            file_info
        })
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
    RunnerAdjustment(RunnerAdjustment),
    EarnedRun(EarnedRunRecord),
    Comment(Comment),
    BoxScoreLine(BoxScoreLine),
    LineScore(LineScore),
    BoxScoreEvent(BoxScoreEvent),
    Unrecognized,
}

impl TryFrom<&RetrosheetEventRecord> for MappedRecord {
    type Error = Error;

    fn try_from(record: &RetrosheetEventRecord) -> Result<MappedRecord> {
        let line_type = record.get(0).context("No record")?;
        let mapped = match line_type {
            "id" => MappedRecord::GameId(GameId::try_from(record)?),
            "version" => MappedRecord::Version,
            "info" => MappedRecord::Info(InfoRecord::try_from(record)?),
            "start" => MappedRecord::Start(StartRecord::try_from(record)?),
            "sub" => MappedRecord::Substitution(SubstitutionRecord::try_from(record)?),
            "play" => MappedRecord::Play(PlayRecord::try_from(record)?),
            "badj" => MappedRecord::BatHandAdjustment(BatHandAdjustment::try_from(record)?),
            "padj" => MappedRecord::PitchHandAdjustment(PitchHandAdjustment::try_from(record)?),
            "ladj" => MappedRecord::LineupAdjustment(LineupAdjustment::try_from(record)?),
            "radj" => MappedRecord::RunnerAdjustment(RunnerAdjustment::try_from(record)?),
            "com" => MappedRecord::Comment(String::from(record.get(1).unwrap())),
            "data" => MappedRecord::EarnedRun(EarnedRunRecord::try_from(record)?),
            "stat" => MappedRecord::BoxScoreLine(BoxScoreLine::try_from(record)?),
            "line" => MappedRecord::LineScore(LineScore::try_from(record)?),
            "event" => MappedRecord::BoxScoreEvent(BoxScoreEvent::try_from(record)?),
            _ => MappedRecord::Unrecognized,
        };
        match mapped {
            MappedRecord::Unrecognized => Err(anyhow!("Unrecognized record type {:?}", record)),
            _ => Ok(mapped),
        }
    }
}

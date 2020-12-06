use std::collections::HashMap;
use std::convert::TryFrom;
use std::fs::File;
use std::io::BufReader;
use std::str::FromStr;

use anyhow::{anyhow, Context, Error, Result};
use bimap::BiMap;
use chrono::{NaiveDate, NaiveTime};
use csv::{Reader, ReaderBuilder, StringRecord};
use tinystr::TinyStr8;

use crate::event_file::box_score::{BoxScoreEvent, BoxScoreLine, LineScore};
use crate::event_file::info::{DayNight, FieldCondition, GameType, HowScored, InfoRecord, Park, PitchDetail, Precipitation, Sky, Team, UmpirePosition, WindDirection};
use crate::event_file::misc::{BatHandAdjustment, Comment, EarnedRunRecord, GameId, LineupAdjustment, PitchHandAdjustment, StartRecord, SubstitutionRecord, Lineup, Defense};
use crate::event_file::play::PlayRecord;
use crate::event_file::traits::{Batter, Fielder, FieldingPosition, FromRetrosheetRecord, LineupPosition, Pitcher, RetrosheetEventRecord, RetrosheetVolunteer, Scorer, Side, Umpire};
use either::{Either, Left, Right};

#[derive(Debug, Eq, PartialEq, Clone)]
pub struct Matchup<T> {away: T, home: T}

impl<T: Clone> Matchup<T> {
    pub fn get(&self, side: &Side) -> &T {
        match side {
            Side::Away => &self.away,
            Side::Home => &self.home
        }
    }

    pub fn get_move(self, side: &Side) -> T {
        match side {
            Side::Away => self.away,
            Side::Home => self.home
        }
    }

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
}

impl<T: Default> Default for Matchup<T> {
    fn default() -> Self {
        Self {away: T::default(), home: T::default() }
    }
}

// TODO: Is there a rustier way to write?
impl<T: Copy> Copy for Matchup<T> {}

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

#[derive(Debug, Eq, PartialEq, Clone)]
pub struct Game {
    pub id: GameId,
    pub info: GameInfo,
    pub events: Vec<EventRecord>,
    pub starting_lineups: Matchup<Lineup>,
    pub starting_defense: Matchup<Defense>,
}

impl TryFrom<&RecordVec> for Game {
    type Error = Error;

    fn try_from(record_vec: &RecordVec) -> Result<Self> {
        let id = *record_vec.iter()
            .find_map(|m| if let MappedRecord::GameId(g) = m {Some(g)} else {None})
            .context("Did not find game ID in list of records")?;
        let infos = record_vec.iter()
            .filter_map(|m| if let MappedRecord::Info(i) = m {Some(*i)} else {None})
            .collect::<Vec<InfoRecord>>();
        let info = GameInfo::try_from(&infos)?;
        let starts =  record_vec.iter()
            .filter_map(|m| if let MappedRecord::Start(i) = m {Some(*i)} else {None})
            .collect::<Vec<StartRecord>>();
        let (starting_lineups, starting_defense) = Self::assemble_lineups_and_defense(starts);
        let events = record_vec.iter()
            .filter_map(|m| match m {
                MappedRecord::Play(pr) => Some(Left(pr.clone())),
                MappedRecord::Substitution(sr) => Some(Right(sr.clone())),
                _ => None
            })
            .collect();
        Ok(Self {
            id,
            info,
            events,
            starting_lineups,
            starting_defense
        })
    }
}

impl Game {
    fn assemble_lineups_and_defense(start_records: Vec<StartRecord>) -> (Matchup<Lineup>, Matchup<Defense>)  {
        // TODO: DRY
        let (mut away_lineup, mut home_lineup) = (Lineup::with_capacity(10), Lineup::with_capacity(10));
        let (mut away_defense, mut home_defense) = (Defense::with_capacity(10), Defense::with_capacity(10));
        let (away_records, home_records): (Vec<StartRecord>, Vec<StartRecord>) = start_records.into_iter()
            // TODO: Partition in place once method stabilized
            .partition(|sr| sr.side == Side::Away);

        away_records.into_iter().zip(home_records).map(|(away, home)| {
            away_lineup.insert(away.lineup_position, away.player);
            away_defense.insert(away.fielding_position, away.player);
            home_lineup.insert(home.lineup_position, home.player);
            home_defense.insert(home.fielding_position, home.player);
        }).for_each(drop);

        (Matchup::new(away_lineup, home_lineup),
         Matchup::new(away_defense, home_defense))
    }

}

#[derive(Debug, Default, PartialOrd, PartialEq, Eq, Clone)]
pub struct GameUmpires {
    home: Option<Umpire>,
    first: Option<Umpire>,
    second: Option<Umpire>,
    third: Option<Umpire>,
    left: Option<Umpire>,
    right: Option<Umpire>
}
impl TryFrom<&Vec<InfoRecord>> for GameUmpires {
    type Error = Error;

    fn try_from(record_vec: &Vec<InfoRecord>) -> Result<Self> {
        let asses: HashMap<UmpirePosition, Umpire> = record_vec
            .iter()
            .filter_map(|i|
                match i {
                    InfoRecord::UmpireAssignment(ass) if ass.umpire.is_some() => Some((ass.position, ass.umpire.unwrap())),
                    _ => None
                })
            .collect();
        Ok(Self {
            home: asses.get(&UmpirePosition::Home).copied(),
            first: asses.get(&UmpirePosition::First).copied(),
            second: asses.get(&UmpirePosition::Second).copied(),
            third: asses.get(&UmpirePosition::Third).copied(),
            left: asses.get(&UmpirePosition::LeftField).copied(),
            right: asses.get(&UmpirePosition::RightField).copied(),
        })

    }
}

#[derive(Debug, Eq, PartialEq, Clone)]
pub struct GameSetting {
    game_type: GameType,
    start_time: Option<NaiveTime>,
    time_of_day: DayNight,
    use_dh: bool,
    home_team_bats_first: bool,
    sky: Sky,
    temp: Option<u8>,
    field_condition: FieldCondition,
    precipitation: Precipitation,
    wind_direction: WindDirection,
    wind_speed: Option<u8>,
    attendance: Option<u32>,
    park: Park,
}
impl Default for GameSetting {
    fn default() -> Self {
        Self {
            game_type: Default::default(),
            start_time: None,
            time_of_day: Default::default(),
            use_dh: false,
            home_team_bats_first: false,
            sky: Default::default(),
            temp: None,
            field_condition: Default::default(),
            precipitation: Default::default(),
            wind_direction: Default::default(),
            wind_speed: None,
            attendance: None,
            park: TinyStr8::from_str("NA").unwrap()
        }
    }
}
impl TryFrom<&Vec<InfoRecord>> for GameSetting {
    type Error = Error;

    fn try_from(infos: &Vec<InfoRecord>) -> Result<Self> {
        let mut setting = Self::default();
        for info in infos {
            match info {
                InfoRecord::GameType(x) => {setting.game_type = *x},
                InfoRecord::StartTime(x) => {setting.start_time = *x},
                InfoRecord::DayNight(x) => {setting.time_of_day = *x},
                InfoRecord::UseDH(x) => {setting.use_dh = *x},
                InfoRecord::HomeTeamBatsFirst(x) => {setting.home_team_bats_first = *x},
                InfoRecord::Sky(x) => {setting.sky = *x},
                InfoRecord::Temp(x) => {setting.temp = *x},
                InfoRecord::FieldCondition(x) => {setting.field_condition = *x}
                InfoRecord::Precipitation(x) => {setting.precipitation = *x}
                InfoRecord::WindDirection(x) => {setting.wind_direction = *x},
                InfoRecord::WindSpeed(x) => {setting.wind_speed = *x},
                InfoRecord::Attendance(x) => {setting.attendance = *x},
                InfoRecord::Park(x) => {setting.park = *x},
                _ => {}
            }
        }
        Ok(setting)
    }
}

#[derive(Debug, Eq, PartialEq, Clone)]
pub struct GameInfo {
    matchup: Teams,
    date: NaiveDate,
    setting: GameSetting,
    umpires: GameUmpires,
    results: GameResults,
    retrosheet_metadata: GameRetrosheetMetadata
}
impl TryFrom<&Vec<InfoRecord>> for GameInfo {
    type Error = Error;

    fn try_from(infos: &Vec<InfoRecord>) -> Result<Self> {
        let matchup = Matchup::try_from(infos)?;
        let date = *infos.iter()
            .find_map(|i| if let InfoRecord::GameDate(d) = i {Some(d)} else {None})
            .ok_or_else(|| anyhow!("Game info did not include date. Full info list: {:?}", infos))?;
        let setting = GameSetting::try_from(infos)?;
        let umpires = GameUmpires::try_from(infos)?;
        let results = GameResults::try_from(infos)?;
        let retrosheet_metadata = GameRetrosheetMetadata::try_from(infos)?;
        Ok(Self {
            matchup,
            date,
            setting,
            umpires,
            results,
            retrosheet_metadata
        })
    }
}


/// Info fields relating to how the game was scored, obtained, and inputted.
#[derive(Debug, Default, Eq, PartialEq, Clone)]
pub struct GameRetrosheetMetadata {
    pitch_detail: PitchDetail,
    scoring_method: HowScored,
    inputter: Option<RetrosheetVolunteer>,
    scorer: Option<Scorer>,
    original_scorer: Option<Scorer>,
    translator: Option<RetrosheetVolunteer>
}

impl TryFrom<&Vec<InfoRecord>> for GameRetrosheetMetadata {
    type Error = Error;

    fn try_from(infos: &Vec<InfoRecord>) -> Result<Self> {
        let mut metadata = Self::default();
        for info in infos {
            match info {
                InfoRecord::PitchDetail(x) => {metadata.pitch_detail = *x},
                InfoRecord::HowScored(x) => {metadata.scoring_method = *x},
                InfoRecord::Inputter(x) => {metadata.inputter = *x},
                InfoRecord::Scorer(x) => {metadata.scorer = *x},
                InfoRecord::OriginalScorer(x) => {metadata.original_scorer = Some(*x)},
                InfoRecord::Translator(x) => {metadata.translator = *x}
                _ => {}
            }
        }
        Ok(metadata)
    }
}

/// These fields only refer to data from the info section, and thus do not include
/// any kind of box score data.
#[derive(Debug, Default, Eq, PartialEq, Clone)]
pub struct GameResults {
    winning_pitcher: Option<Pitcher>,
    losing_pitcher: Option<Pitcher>,
    save: Option<Pitcher>,
    game_winning_rbi: Option<Batter>,
    time_of_game_minutes: Option<u16>,
}

impl TryFrom<&Vec<InfoRecord>> for GameResults {
    type Error = Error;

    fn try_from(infos: &Vec<InfoRecord>) -> Result<Self> {
        let mut results = Self::default();
        for info in infos {
            match info {
                InfoRecord::WinningPitcher(x) => {results.winning_pitcher = *x},
                InfoRecord::LosingPitcher(x) => {results.losing_pitcher = *x},
                InfoRecord::SavePitcher(x) => {results.save = *x},
                InfoRecord::GameWinningRBI(x) => {results.game_winning_rbi = *x},
                InfoRecord::TimeOfGameMinutes(x) => {results.time_of_game_minutes = *x},
                _ => {}
            }
        }
        Ok(results)
    }
}

type RecordVec = Vec<MappedRecord>;


pub struct RetrosheetReader {
    reader: Reader<BufReader<File>>,
    current_record: StringRecord,
    current_game_id: GameId,
    current_record_vec: RecordVec
}


impl Iterator for RetrosheetReader {
    type Item = Result<Game>;

    fn next(&mut self) -> Option<Self::Item> {
        match self.next_game() {
            Err(e) => Some(Err(e)),
            Ok(true) => Some(Game::try_from(&self.current_record_vec)),
            _ => None
        }
    }
}

impl RetrosheetReader {
    fn next_game(&mut self) -> Result<bool> {
        self.current_record_vec.clear();
        if self.reader.is_done() {return Ok(false)}
        self.current_record_vec.push(MappedRecord::GameId(self.current_game_id));
        loop {
            let did_read = self.reader.read_record(&mut self.current_record)?;
            if !did_read {return Ok(false)}
            let mapped_record = MappedRecord::from_retrosheet_record(&self.current_record);
            match mapped_record {
                Ok(MappedRecord::GameId(g)) => {self.current_game_id = g; return Ok(true)},
                Ok(m) => {self.current_record_vec.push(m)}
                _ => println!("Error during game {} -- Error reading record: {:?}", &self.current_game_id.id, &self.current_record)
            }
        }
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
        let current_game_id = match MappedRecord::from_retrosheet_record(&current_record)? {
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

impl FromRetrosheetRecord for MappedRecord {
    fn from_retrosheet_record(record: &RetrosheetEventRecord) -> Result<MappedRecord>{
        let line_type = record.get(0).context("No record")?;
        let mapped= match line_type {
            "id" => MappedRecord::GameId(GameId::from_retrosheet_record(record)?),
            "version" => MappedRecord::Version,
            "info" => MappedRecord::Info(InfoRecord::from_retrosheet_record(record)?),
            "start" => MappedRecord::Start(StartRecord::from_retrosheet_record(record)?),
            "sub" => MappedRecord::Substitution(SubstitutionRecord::from_retrosheet_record(record)?),
            "play" => MappedRecord::Play(PlayRecord::from_retrosheet_record(record)?),
            "badj" => MappedRecord::BatHandAdjustment(BatHandAdjustment::from_retrosheet_record(record)?),
            "padj" => MappedRecord::PitchHandAdjustment(PitchHandAdjustment::from_retrosheet_record(record)?),
            "ladj" => MappedRecord::LineupAdjustment(LineupAdjustment::from_retrosheet_record(record)?),
            "com" => MappedRecord::Comment(String::from(record.get(1).unwrap())),
            "data" => MappedRecord::EarnedRun(EarnedRunRecord::from_retrosheet_record(record)?),
            "stat" => MappedRecord::BoxScoreLine(BoxScoreLine::from_retrosheet_record(record)?),
            "line" => MappedRecord::LineScore(LineScore::from_retrosheet_record(record)?),
            "event" => MappedRecord::BoxScoreEvent(BoxScoreEvent::from_retrosheet_record(record)?),
            _ => MappedRecord::Unrecognized
        };
        match mapped {
            MappedRecord::Unrecognized => Err(Self::error("Unrecognized record type", record)),
            _ => Ok(mapped)
        }
    }
}
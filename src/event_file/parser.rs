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
use crate::event_file::info::{DayNight, FieldCondition, GameType, HowScored, InfoRecord, Park, PitchDetail, Precipitation, Sky, Team, UmpirePosition, WindDirection};
use crate::event_file::misc::{BatHandAdjustment, Comment, EarnedRunRecord, GameId, LineupAdjustment, PitchHandAdjustment, StartRecord, SubstitutionRecord, Lineup, Defense};
use crate::event_file::play::PlayRecord;
use crate::event_file::traits::{Batter, Pitcher, RetrosheetEventRecord, RetrosheetVolunteer, Scorer, Side, Umpire};
use either::{Either, Left, Right};
use crate::event_file::pbp::GameState;
use std::path::PathBuf;

#[derive(Debug, Eq, PartialEq, Clone)]
pub struct Matchup<T> {away: T, home: T}

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

#[derive(Debug, Eq, PartialEq, Clone)]
pub struct Game {
    pub id: GameId,
    pub info: GameInfo,
    pub events: Vec<EventRecord>,
    pub starts: Vec<StartRecord>,
    pub starting_lineups: Matchup<Lineup>,
    pub starting_defense: Matchup<Defense>,
    pub earned_run_data: HashMap<Pitcher, u8>
}

impl TryInto<Vec<RetrosheetEventRecord>> for Game {
    type Error = Error;

    fn try_into(self) -> Result<Vec<RetrosheetEventRecord>> {
        let box_score = GameState::get_box_score(&self)?.into();

        let id_fields =  vec![
            RetrosheetEventRecord::from(vec!["id", self.id.id.as_str()]),
            RetrosheetEventRecord::from(vec!["version", "3"]),
            RetrosheetEventRecord::from(vec!["info", "inputprogvers", "version 7RS(19) of 07/07/92"])
        ];
        let info: Vec<RetrosheetEventRecord> = self.info.into();
        let starts: Vec<RetrosheetEventRecord> = self.starts.iter().map({
            |sr| RetrosheetEventRecord::from(vec![
                "start".to_string(),
                sr.player.to_string(),
                sr.player_name.clone(),
                sr.side.retrosheet_str().to_string(),
                sr.lineup_position.retrosheet_string(),
                sr.fielding_position.retrosheet_string()
            ])
        })
            .collect();

        Ok([id_fields, info, starts, box_score].concat())
    }
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
            .filter_map(|m| if let MappedRecord::Start(s) = m {Some(s.clone())} else {None})
            .collect::<Vec<StartRecord>>();
        let (starting_lineups, starting_defense) = Self::assemble_lineups_and_defense(starts.clone());
        let events = record_vec.iter()
            .filter_map(|m| match m {
                MappedRecord::Play(pr) => Some(Left(pr.clone())),
                MappedRecord::Substitution(sr) => Some(Right(sr.clone())),
                _ => None
            })
            .collect();
        let earned_run_data = record_vec.iter()
            .filter_map(|m| if let MappedRecord::EarnedRun(e) = m {
                Some((e.pitcher_id, e.earned_runs))
            } else {None})
            .collect();
        Ok(Self {
            id,
            info,
            starts,
            events,
            starting_lineups,
            starting_defense,
            earned_run_data
        })
    }


}

impl Game {

    pub fn bat_first_side(&self) -> Side {
        self.info.setting.bat_first_side
    }

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

#[derive(Debug, Default, PartialOrd, PartialEq, Eq, Clone, Copy)]
pub struct GameUmpires {
    home: Option<Umpire>,
    first: Option<Umpire>,
    second: Option<Umpire>,
    third: Option<Umpire>,
    left: Option<Umpire>,
    right: Option<Umpire>
}

impl Into<Vec<RetrosheetEventRecord>> for GameUmpires {
    fn into(self) -> Vec<RetrosheetEventRecord> {
        let opt_string = {
            |o: Option<Umpire>| o.map_or("".to_string(), |u| u.to_string())
        };
        let ump_types = vec!["umphome", "ump1b", "ump2b", "ump3b", "umplf", "umprf"];
        let ump_names = vec![self.home, self.first, self.second, self.third, self.left, self.right]
            .into_iter()
            .map(opt_string);
        let vecs: Vec<Vec<String>> = ump_names
            .zip(ump_types.iter())
            .map(|(name, pos)|vec!["info".to_string(), pos.to_string(), name])
            .collect();
        vecs.into_iter()
            .map(RetrosheetEventRecord::from).collect()
    }
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

#[derive(Debug, Eq, PartialEq, Clone, Copy)]
pub struct GameSetting {
    game_type: GameType,
    start_time: Option<NaiveTime>,
    time_of_day: DayNight,
    use_dh: bool,
    bat_first_side: Side,
    sky: Sky,
    temp: Option<u8>,
    field_condition: FieldCondition,
    precipitation: Precipitation,
    wind_direction: WindDirection,
    wind_speed: Option<u8>,
    attendance: Option<u32>,
    park: Park,
}

impl Into<Vec<RetrosheetEventRecord>> for GameSetting {
    fn into(self) -> Vec<RetrosheetEventRecord> {
        let mut vecs = vec![
            vec!["number".to_string(), self.game_type.to_string()],
            vec!["starttime".to_string(), self.start_time.map_or("".to_string(), |t| t.to_string())],
            vec!["daynight".to_string(), self.time_of_day.to_string()],
            vec!["usedh".to_string(), self.use_dh.to_string()],
            vec!["htbf".to_string(), (self.bat_first_side == Side::Home).to_string()],
            vec!["sky".to_string(), self.sky.to_string()],
            vec!["temp".to_string(), self.temp.map_or("".to_string(), |u| u.to_string())],
            vec!["fieldcond".to_string(), self.field_condition.to_string()],
            vec!["precip".to_string(), self.precipitation.to_string()],
            vec!["winddir".to_string(), self.wind_direction.to_string()],
            vec!["windspeed".to_string(), self.wind_speed.map_or("".to_string(), |u| u.to_string())],
            vec!["attendance".to_string(), self.attendance.map_or("".to_string(), |u| u.to_string())],
            vec!["site".to_string(), self.park.to_string()]
        ];
        for vec in &mut vecs {
            vec.insert(0, String::from("info"));
        }
        vecs.into_iter()
            .map(RetrosheetEventRecord::from).collect()
    }
}

impl Default for GameSetting {
    fn default() -> Self {
        Self {
            game_type: Default::default(),
            start_time: None,
            time_of_day: Default::default(),
            use_dh: false,
            bat_first_side: Side::Away,
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
                InfoRecord::HomeTeamBatsFirst(x) => {
                    setting.bat_first_side = if *x {Side::Home} else {Side::Away}
                },
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

#[derive(Debug, Eq, PartialEq, Clone, Copy)]
pub struct GameInfo {
    matchup: Teams,
    date: NaiveDate,
    setting: GameSetting,
    umpires: GameUmpires,
    results: GameResults,
    retrosheet_metadata: GameRetrosheetMetadata
}

impl Into<Vec<RetrosheetEventRecord>> for GameInfo {
    fn into(self) -> Vec<RetrosheetEventRecord> {
        let top_level_info: Vec<RetrosheetEventRecord> = vec![
            vec!["info".to_string(), "visteam".to_string(), self.matchup.away.to_string()],
            vec!["info".to_string(), "hometeam".to_string(), self.matchup.home.to_string()],
            vec!["info".to_string(), "date".to_string(), self.date.format("%Y/%m/%d").to_string()]
        ].into_iter()
            .map(RetrosheetEventRecord::from).collect();

        [top_level_info,
            self.setting.into(),
            self.umpires.into(),
            self.results.into(),
            self.retrosheet_metadata.into()
        ].concat()

    }
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
#[derive(Debug, Default, Eq, PartialEq, Clone, Copy)]
pub struct GameRetrosheetMetadata {
    pitch_detail: PitchDetail,
    scoring_method: HowScored,
    inputter: Option<RetrosheetVolunteer>,
    scorer: Option<Scorer>,
    original_scorer: Option<Scorer>,
    translator: Option<RetrosheetVolunteer>
}

impl Into<Vec<RetrosheetEventRecord>> for GameRetrosheetMetadata {
    fn into(self) -> Vec<RetrosheetEventRecord> {
        let opt_string = {
            |o: Option<TinyStr16>| o.map_or("".to_string(), |u| u.to_string())
        };

        let mut vecs = vec![
            vec!["pitches".to_string(), self.pitch_detail.to_string()],
            vec!["howscored".to_string(), self.scoring_method.to_string()],
            vec!["inputter".to_string(), opt_string(self.inputter)],
            vec!["scorer".to_string(), opt_string(self.scorer)],
            vec!["oscorer".to_string(), opt_string(self.original_scorer)],
            vec!["translator".to_string(), opt_string(self.translator)],
        ];
        for vec in &mut vecs {
            vec.insert(0, String::from("info"));
        }
        vecs.into_iter()
            .map(RetrosheetEventRecord::from).collect()
    }
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
#[derive(Debug, Default, Eq, PartialEq, Clone, Copy)]
pub struct GameResults {
    winning_pitcher: Option<Pitcher>,
    losing_pitcher: Option<Pitcher>,
    save: Option<Pitcher>,
    game_winning_rbi: Option<Batter>,
    time_of_game_minutes: Option<u16>,
}

impl Into<Vec<RetrosheetEventRecord>> for GameResults {
    fn into(self) -> Vec<RetrosheetEventRecord> {
        let opt_string = {
            |o: Option<TinyStr8>| o.map_or("".to_string(), |u| u.to_string())
        };
        let mut vecs = vec![
            vec!["wp".to_string(), opt_string(self.winning_pitcher)],
            vec!["lp".to_string(), opt_string(self.losing_pitcher)],
            vec!["save".to_string(), opt_string(self.save)],
            vec!["gwrbi".to_string(), opt_string(self.game_winning_rbi)],
            vec!["timeofgame".to_string(), self.time_of_game_minutes.map_or("".to_string(), |u| u.to_string())],
        ];

        for vec in &mut vecs {
            vec.insert(0, String::from("info"));
        }
        vecs.into_iter()
            .map(RetrosheetEventRecord::from)
            .collect()
    }
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
            _ if !&self.current_record_vec.is_empty() => Some(Game::try_from(&self.current_record_vec)),
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
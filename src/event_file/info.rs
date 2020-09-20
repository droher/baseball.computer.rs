use anyhow::{anyhow, Result};
use chrono::{NaiveDate, NaiveTime};
use strum_macros::EnumString;

use crate::event_file::traits::{FromRetrosheetRecord, Player, RetrosheetEventRecord, RetrosheetVolunteer, Scorer, Umpire, Person, MiscInfoString};
use crate::util::{parse_positive_int, str_to_tinystr};
use std::str::FromStr;
use tinystr::{TinyStr4, TinyStr8, TinyStr16};
use either::Either;

#[derive(Debug, Eq, PartialEq, EnumString, Copy, Clone)]
#[strum(serialize_all = "lowercase")]
pub enum HowScored {
    Park,
    Tv,
    Radio,
    Unknown
}
impl Default for HowScored {
    fn default() -> Self { Self::Unknown }
}

#[derive(Debug, Eq, PartialEq, EnumString, Copy, Clone)]
#[strum(serialize_all = "lowercase")]
pub enum FieldCondition {
    Dry,
    Soaked,
    Wet,
    Damp,
    Unknown
}
impl Default for FieldCondition {
    fn default() -> Self { Self::Unknown }
}

#[derive(Debug, Eq, PartialEq, EnumString, Copy, Clone)]
#[strum(serialize_all = "lowercase")]
pub enum Precipitation {
    Rain,
    Drizzle,
    Showers,
    Snow,
    None,
    Unknown
}
impl Default for Precipitation {
    fn default() -> Self { Self::Unknown }
}

#[derive(Debug, Eq, PartialEq, EnumString, Copy, Clone)]
#[strum(serialize_all = "lowercase")]
pub enum Sky {
    Cloudy,
    Dome,
    Night,
    Overcast,
    Sunny,
    Unknown
}
impl Default for Sky {
    fn default() -> Self { Self::Unknown }
}

#[derive(Debug, Eq, PartialEq, EnumString, Copy, Clone)]
#[strum(serialize_all = "lowercase")]
pub enum WindDirection {
    FromCF,
    FromLF,
    FromRF,
    #[strum(serialize = "ltor")]
    LeftToRight,
    #[strum(serialize = "rtol")]
    RightToLeft,
    ToCF,
    ToLF,
    ToRF,
    Unknown
}
impl Default for WindDirection {
    fn default() -> Self { Self::Unknown }
}

pub type Team = TinyStr4;
pub type Park = TinyStr8;


#[derive(Debug, Eq, PartialEq, EnumString, Copy, Clone)]
#[strum(serialize_all = "lowercase")]
pub enum DayNight {
    Day,
    Night,
    Unknown
}
impl Default for DayNight {
    fn default() -> Self { Self::Unknown }
}

#[derive(Debug, Eq, PartialEq, EnumString, Copy, Clone)]
pub enum GameType {
    #[strum(serialize = "0")]
    SingleGame,
    #[strum(serialize = "1")]
    DoubleHeaderGame1,
    #[strum(serialize = "2")]
    DoubleHeaderGame2,
    #[strum(serialize = "3")]
    DoubleHeaderGame3,
    #[strum(serialize = "4")]
    DoubleHeaderGame4
}
impl Default for GameType {
    fn default() -> Self { Self::SingleGame }
}


#[derive(Debug, Eq, PartialEq, EnumString, Copy, Clone, Hash)]
#[strum(serialize_all = "lowercase")]
pub enum PitchDetail {
    Pitches,
    Count,
    None,
    Unknown
}
impl Default for PitchDetail {
    fn default() -> Self { Self::Unknown }
}

#[derive(Debug, Eq, PartialOrd, PartialEq, Copy, Clone, Hash, EnumString)]
pub enum UmpirePosition {
    #[strum(serialize = "umphome")]
    Home,
    #[strum(serialize = "ump1b")]
    First,
    #[strum(serialize = "ump2b")]
    Second,
    #[strum(serialize = "ump3b")]
    Third,
    #[strum(serialize = "umplf")]
    LeftField,
    #[strum(serialize = "umprf")]
    RightField
}


#[derive(Debug, Eq, PartialEq, Copy, Clone, Hash)]
pub struct UmpireAssignment {pub position: UmpirePosition, pub umpire: Umpire}

#[derive(Debug, Eq, PartialEq, Copy, Clone)]
pub enum InfoRecord {
    VisitingTeam(Team),
    HomeTeam(Team),
    GameDate(NaiveDate),
    GameType(GameType),
    StartTime(Option<NaiveTime>),
    DayNight(DayNight),
    UseDH(bool),
    HomeTeamBatsFirst(bool),
    PitchDetail(PitchDetail),
    UmpireAssignment(UmpireAssignment),
    FieldCondition(FieldCondition),
    Precipitation(Precipitation),
    Sky(Sky),
    Temp(Option<u8>),
    WindDirection(WindDirection),
    WindSpeed(Option<u8>),
    TimeOfGameMinutes(Option<u16>),
    Attendance(Option<u32>),
    Park(Park),
    WinningPitcher(Option<Player>),
    LosingPitcher(Option<Player>),
    SavePitcher(Option<Player>),
    GameWinningRBI(Option<Player>),
    EditTime(Option<MiscInfoString>),
    HowScored(HowScored),
    InputProgramVersion(Option<MiscInfoString>),
    Inputter(Option<RetrosheetVolunteer>),
    InputTime(Option<MiscInfoString>),
    Scorer(Option<Scorer>),
    OriginalScorer(Scorer),
    Translator(Option<RetrosheetVolunteer>),
    // We currently don't parse umpire changes as they only occur in box scores
    // and are irregularly shaped
    UmpireChange,
    Unrecognized
}

impl InfoRecord {
    fn parse_time(time_str: &str) -> InfoRecord {
        let padded_time = format!("{:0>4}", time_str);
        let time = NaiveTime::parse_from_str(&padded_time, "%I%M");
        match time {
            Ok(t) => InfoRecord::StartTime(Some(t)),
            Err(_) => InfoRecord::StartTime(None)
        }
    }
}

impl FromRetrosheetRecord for InfoRecord {
    fn new(record: &RetrosheetEventRecord) -> Result<InfoRecord> {
        let record = record.deserialize::<[&str; 3]>(None)?;

        let info_type = record[1];
        let value = record[2];

        let t8 = {|| str_to_tinystr::<TinyStr8>(value)};
        let t16 = {|| str_to_tinystr::<TinyStr16>(value)};

        let to_option8 = {|s: TinyStr8| if !value.is_empty() {Some(s)} else {None}};
        let to_option16 = {|s: TinyStr16| if !value.is_empty() {Some(s)} else {None}};

        type I = InfoRecord;
        let info = match info_type {
            "visteam" => I::VisitingTeam(str_to_tinystr(value)?),
            "hometeam" => I::HomeTeam(str_to_tinystr(value)?),
            "site" => I::Park(str_to_tinystr(value)?),
            "oscorer" => I::OriginalScorer(str_to_tinystr(value)?),

            "umphome" | "ump1b" | "ump2b" | "ump3b" | "umplf" | "umprf" => {
                I::UmpireAssignment(UmpireAssignment {position: UmpirePosition::from_str(info_type)?, umpire: t8()?})
            },

            "number" => I::GameType(GameType::from_str(value)?),
            "daynight" => I::DayNight(DayNight::from_str(value)?),
            "pitches" => I::PitchDetail(PitchDetail::from_str(value)?),
            "fieldcond" | "fieldcon" => I::FieldCondition(FieldCondition::from_str(value)?),
            "precip" => I::Precipitation(Precipitation::from_str(value)?),
            "sky" => I::Sky(Sky::from_str(value)?),
            "winddir" => I::WindDirection(WindDirection::from_str(value)?),
            "howscored" => I::HowScored(HowScored::from_str(value)?),

            "windspeed" => I::WindSpeed(parse_positive_int::<u8>(value)),
            "timeofgame" => I::TimeOfGameMinutes(parse_positive_int::<u16>(value)),
            "attendance" => I::Attendance(parse_positive_int::<u32>(value)),
            "temp" => I::Temp(parse_positive_int::<u8>(value)),

            "usedh" => I::UseDH(bool::from_str(value)?),
            "htbf" => I::HomeTeamBatsFirst(bool::from_str(value)?),
            "date" => I::GameDate(NaiveDate::parse_from_str(value, "%Y/%m/%d")?),
            "starttime" => I::parse_time(value),

            "wp" => I::WinningPitcher(to_option8(t8()?)),
            "lp" => I::LosingPitcher(to_option8(t8()?)),
            "save" => I::SavePitcher(to_option8(t8()?)),
            "gwrbi" => I::GameWinningRBI(to_option8(t8()?)),
            "edittime" => I::EditTime(to_option16(t16()?)),
            "inputtime" => I::InputTime(to_option16(t16()?)),
            "scorer" => I::Scorer(to_option16(t16()?)),
            "inputter" => I::Inputter(to_option16(t16()?)),
            "inputprogvers" => I::InputProgramVersion(to_option16(t16()?)),
            "translator" => I::Translator(to_option16(t16()?)),
            "umpchange" => I::UmpireChange,
            _ => I::Unrecognized
        };
        match info {
            I::Unrecognized => Err(anyhow!("Unrecognized info type: {:?}", info_type)),
            _ => Ok(info)
        }
    }
}


use std::convert::TryFrom;
use std::str::FromStr;

use anyhow::{bail, Error, Result};
use arrayvec::ArrayString;
use chrono::{NaiveDate, NaiveDateTime, NaiveTime};
use serde::{Deserialize, Serialize};
use strum_macros::{Display, EnumString};

use crate::event_file::misc::{parse_non_negative_int, parse_positive_int, str_to_tinystr};
use crate::event_file::traits::{
    Player, RetrosheetEventRecord, RetrosheetVolunteer, Scorer, Umpire,
};

#[derive(
    Debug, Eq, PartialEq, EnumString, Copy, Clone, Display, Ord, PartialOrd, Serialize, Deserialize,
)]
#[strum(serialize_all = "lowercase")]
pub enum HowScored {
    Park,
    Tv,
    Radio,
    Unknown,
}
impl Default for HowScored {
    fn default() -> Self {
        Self::Unknown
    }
}

#[derive(
    Debug, Eq, PartialEq, EnumString, Copy, Clone, Display, Ord, PartialOrd, Serialize, Deserialize,
)]
#[strum(serialize_all = "lowercase")]
pub enum FieldCondition {
    Dry,
    Soaked,
    Wet,
    Damp,
    Unknown,
}
impl Default for FieldCondition {
    fn default() -> Self {
        Self::Unknown
    }
}

#[derive(
    Debug, Eq, PartialEq, EnumString, Copy, Clone, Display, Ord, PartialOrd, Serialize, Deserialize,
)]
#[strum(serialize_all = "lowercase")]
pub enum Precipitation {
    Rain,
    Drizzle,
    Showers,
    Snow,
    None,
    Unknown,
}
impl Default for Precipitation {
    fn default() -> Self {
        Self::Unknown
    }
}

#[derive(
    Debug, Eq, PartialEq, EnumString, Copy, Clone, Display, Ord, PartialOrd, Serialize, Deserialize,
)]
#[strum(serialize_all = "lowercase")]
pub enum Sky {
    Cloudy,
    Dome,
    Night,
    Overcast,
    Sunny,
    Unknown,
}
impl Default for Sky {
    fn default() -> Self {
        Self::Unknown
    }
}

#[derive(
    Debug, Eq, PartialEq, EnumString, Copy, Clone, Display, Ord, PartialOrd, Serialize, Deserialize,
)]
#[strum(serialize_all = "lowercase")]
pub enum WindDirection {
    FromCf,
    FromLf,
    FromRf,
    #[strum(serialize = "ltor")]
    LeftToRight,
    #[strum(serialize = "rtol")]
    RightToLeft,
    ToCf,
    ToLf,
    ToRf,
    Unknown,
}
impl Default for WindDirection {
    fn default() -> Self {
        Self::Unknown
    }
}

pub type Team = ArrayString<8>;
pub type Park = ArrayString<8>;

#[derive(
    Debug, Eq, PartialEq, EnumString, Copy, Clone, Display, Ord, PartialOrd, Serialize, Deserialize,
)]
#[strum(serialize_all = "lowercase")]
pub enum DayNight {
    Day,
    Night,
    Unknown,
}
impl Default for DayNight {
    fn default() -> Self {
        Self::Unknown
    }
}

#[derive(
    Debug, Eq, PartialEq, Ord, PartialOrd, EnumString, Copy, Clone, Display, Serialize, Deserialize,
)]
pub enum DoubleheaderStatus {
    #[strum(serialize = "0")]
    SingleGame,
    #[strum(serialize = "1")]
    DoubleHeaderGame1,
    #[strum(serialize = "2")]
    DoubleHeaderGame2,
    #[strum(serialize = "3")]
    DoubleHeaderGame3,
    #[strum(serialize = "4")]
    DoubleHeaderGame4,
}
impl Default for DoubleheaderStatus {
    fn default() -> Self {
        Self::SingleGame
    }
}

#[derive(
    Debug, Eq, PartialEq, EnumString, Copy, Clone, Display, Ord, PartialOrd, Serialize, Deserialize,
)]
#[strum(serialize_all = "lowercase")]
pub enum PitchDetail {
    Pitches,
    Count,
    None,
    Unknown,
}
impl Default for PitchDetail {
    fn default() -> Self {
        Self::Unknown
    }
}

#[derive(
    Debug,
    Eq,
    PartialEq,
    EnumString,
    Hash,
    Copy,
    Clone,
    Display,
    Ord,
    PartialOrd,
    Serialize,
    Deserialize,
)]
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
    RightField,
}

#[derive(Debug, Eq, PartialEq, Copy, Clone, Hash)]
pub struct UmpireAssignment {
    pub position: UmpirePosition,
    pub umpire: Option<Umpire>,
}

#[derive(Debug, Eq, PartialEq, Copy, Clone)]
pub enum InfoRecord {
    VisitingTeam(Team),
    HomeTeam(Team),
    GameDate(NaiveDate),
    DoubleheaderStatus(DoubleheaderStatus),
    StartTime(Option<NaiveTime>),
    DayNight(DayNight),
    UseDh(bool),
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
    GameWinningRbi(Option<Player>),
    HowScored(HowScored),
    Inputter(Option<RetrosheetVolunteer>),
    Scorer(Option<Scorer>),
    Translator(Option<RetrosheetVolunteer>),
    Innings(Option<u8>),
    InputDate(Option<NaiveDateTime>),
    EditDate(Option<NaiveDateTime>),
    Tiebreaker,
    // We currently don't parse umpire changes as they only occur in box scores
    // and are irregularly shaped
    UmpireChange,
    InputProgramVersion,
    HowEntered,
    Unrecognized,
}

impl InfoRecord {
    fn parse_datetime(datetime_str: &str) -> Option<NaiveDateTime> {
        let mut split_str = datetime_str.split(' ');
        let date = split_str.next()?;
        let time = split_str.next().unwrap_or_default();
        let parsed_date = NaiveDate::parse_from_str(date, "%Y/%m/%d").ok()?;
        let parsed_time = Self::parse_time(time).unwrap_or_default();
        Some(NaiveDateTime::new(parsed_date, parsed_time))
    }

    fn parse_time(time_str: &str) -> Option<NaiveTime> {
        let padded_time = format!("{:0>4}", time_str);
        NaiveTime::parse_from_str(&padded_time, "%I:%M%p").ok()
    }
}

impl TryFrom<&RetrosheetEventRecord> for InfoRecord {
    type Error = Error;

    fn try_from(record: &RetrosheetEventRecord) -> Result<InfoRecord> {
        type I = InfoRecord;
        let record = record.deserialize::<[&str; 3]>(None)?;

        let info_type = record[1];
        let value = record[2];

        let t8 = { || str_to_tinystr::<ArrayString<8>>(value) };
        let t16 = { || str_to_tinystr::<ArrayString<16>>(value) };

        let info = match info_type {
            "visteam" => I::VisitingTeam(str_to_tinystr(value)?),
            "hometeam" => I::HomeTeam(str_to_tinystr(value)?),
            "site" => I::Park(str_to_tinystr(value)?),

            "umphome" | "ump1b" | "ump2b" | "ump3b" | "umplf" | "umprf" => {
                I::UmpireAssignment(UmpireAssignment {
                    position: UmpirePosition::from_str(info_type)?,
                    umpire: t8().ok(),
                })
            }

            "number" => I::DoubleheaderStatus(DoubleheaderStatus::from_str(value)?),
            "daynight" => I::DayNight(DayNight::from_str(value)?),
            "pitches" => I::PitchDetail(PitchDetail::from_str(value)?),
            "fieldcond" | "fieldcon" => I::FieldCondition(FieldCondition::from_str(value)?),
            "precip" => I::Precipitation(Precipitation::from_str(value)?),
            "sky" => I::Sky(Sky::from_str(value)?),
            "winddir" => I::WindDirection(WindDirection::from_str(value)?),
            "howscored" => I::HowScored(HowScored::from_str(value)?),
            "howentered" => I::HowEntered,

            "windspeed" => I::WindSpeed(parse_positive_int::<u8>(value)),
            "timeofgame" => I::TimeOfGameMinutes(parse_positive_int::<u16>(value)),
            "attendance" => I::Attendance(parse_non_negative_int::<u32>(value)),
            "temp" => I::Temp(parse_positive_int::<u8>(value)),
            "innings" => I::Innings(parse_positive_int::<u8>(value)),

            "usedh" => I::UseDh(bool::from_str(&value.to_lowercase())?),
            "htbf" => I::HomeTeamBatsFirst(bool::from_str(value)?),
            "date" => I::GameDate(NaiveDate::parse_from_str(value, "%Y/%m/%d")?),
            "starttime" => I::StartTime(I::parse_time(value)),

            // # TODO: Add error correction for optional fields rather than passing in None
            "wp" => I::WinningPitcher(t8().ok()),
            "lp" => I::LosingPitcher(t8().ok()),
            "save" => I::SavePitcher(t8().ok()),
            "gwrbi" => I::GameWinningRbi(t8().ok()),
            "scorer" | "oscorer" => I::Scorer(t16().ok()),
            "inputter" => I::Inputter(t16().ok()),
            "translator" => I::Translator(t16().ok()),
            "inputtime" => I::InputDate(I::parse_datetime(value)),
            "edittime" => I::EditDate(I::parse_datetime(value)),
            "tiebreaker" => I::Tiebreaker,
            "inputprogvers" => I::InputProgramVersion,
            "umpchange" => I::UmpireChange,
            _ => I::Unrecognized,
        };
        match info {
            I::Unrecognized => bail!("Unrecognized info type: {:?}", record),
            _ => Ok(info),
        }
    }
}

use std::convert::TryFrom;
use std::str::FromStr;

use anyhow::{bail, Error, Result};
use arrayvec::ArrayString;
use chrono::{NaiveDate, NaiveDateTime, NaiveTime};
use serde::{Deserialize, Serialize};
use strum_macros::{AsRefStr, Display, EnumString};

use crate::event_file::misc::{parse_non_negative_int, parse_positive_int, str_to_tinystr};
use crate::event_file::traits::{
    Player, RetrosheetEventRecord, RetrosheetVolunteer, Scorer, Umpire,
};

use super::traits::GameType;

#[derive(
    Debug,
    Eq,
    PartialEq,
    EnumString,
    Copy,
    Clone,
    Display,
    Ord,
    PartialOrd,
    Serialize,
    Deserialize,
    AsRefStr,
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
    Debug,
    Eq,
    PartialEq,
    EnumString,
    Copy,
    Clone,
    Display,
    Ord,
    PartialOrd,
    Serialize,
    Deserialize,
    AsRefStr,
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
    Debug,
    Eq,
    PartialEq,
    EnumString,
    Copy,
    Clone,
    Display,
    Ord,
    PartialOrd,
    Serialize,
    Deserialize,
    AsRefStr,
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
    Debug,
    Eq,
    PartialEq,
    EnumString,
    Copy,
    Clone,
    Display,
    Ord,
    PartialOrd,
    Serialize,
    Deserialize,
    AsRefStr,
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
    Debug,
    Eq,
    PartialEq,
    EnumString,
    Copy,
    Clone,
    Display,
    Ord,
    PartialOrd,
    Serialize,
    Deserialize,
    AsRefStr,
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

pub type Team = ArrayString<3>;
pub type Park = ArrayString<16>;

#[derive(
    Debug,
    Eq,
    PartialEq,
    EnumString,
    Copy,
    Clone,
    Display,
    Ord,
    PartialOrd,
    Serialize,
    Deserialize,
    AsRefStr,
)]
#[strum(serialize_all = "lowercase")]
pub enum DayNight {
    Day,
    Night,
    #[strum(serialize = "unknown", serialize = "")]
    Unknown,
}
impl Default for DayNight {
    fn default() -> Self {
        Self::Unknown
    }
}

#[derive(
    Debug,
    Eq,
    PartialEq,
    Ord,
    PartialOrd,
    EnumString,
    Copy,
    Clone,
    Display,
    Serialize,
    Deserialize,
    AsRefStr,
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
    Debug,
    Eq,
    PartialEq,
    EnumString,
    Copy,
    Clone,
    Display,
    Ord,
    PartialOrd,
    Serialize,
    Deserialize,
    AsRefStr,
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
    AsRefStr,
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
    GameType(GameType),
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
        let padded_time = format!("{time_str:0>4}");
        NaiveTime::parse_from_str(&padded_time, "%I:%M%p").ok()
    }
}

impl TryFrom<&RetrosheetEventRecord> for InfoRecord {
    type Error = Error;

    fn try_from(record: &RetrosheetEventRecord) -> Result<Self> {
        type I = InfoRecord;
        let record = record.deserialize::<[&str; 3]>(None)?;

        let info_type = record[1];
        let value = record[2];

        let t8 = { || str_to_tinystr::<ArrayString<8>>(value) };
        let t16 = { || str_to_tinystr::<ArrayString<16>>(value) };

        let info = match info_type {
            "visteam" => Self::VisitingTeam(str_to_tinystr(value)?),
            "hometeam" => Self::HomeTeam(str_to_tinystr(value)?),
            "site" => Self::Park(str_to_tinystr(value)?),

            "umphome" | "ump1b" | "ump2b" | "ump3b" | "umplf" | "umprf" => {
                Self::UmpireAssignment(UmpireAssignment {
                    position: UmpirePosition::from_str(info_type)?,
                    umpire: t8().ok(),
                })
            }

            "number" => Self::DoubleheaderStatus(DoubleheaderStatus::from_str(value)?),
            "daynight" => Self::DayNight(DayNight::from_str(value)?),
            "pitches" => Self::PitchDetail(PitchDetail::from_str(value)?),
            "fieldcond" | "fieldcon" => Self::FieldCondition(FieldCondition::from_str(value)?),
            "precip" => Self::Precipitation(Precipitation::from_str(value)?),
            "sky" => Self::Sky(Sky::from_str(value)?),
            "winddir" => Self::WindDirection(WindDirection::from_str(value)?),
            "howscored" => Self::HowScored(HowScored::from_str(value)?),
            "gametype" => Self::GameType(GameType::from_str(value)?),
            "howentered" => Self::HowEntered,

            "windspeed" => Self::WindSpeed(parse_positive_int::<u8>(value)),
            "timeofgame" => Self::TimeOfGameMinutes(parse_positive_int::<u16>(value)),
            "attendance" => Self::Attendance(parse_non_negative_int::<u32>(value)),
            "temp" => Self::Temp(parse_positive_int::<u8>(value)),
            "innings" => Self::Innings(parse_positive_int::<u8>(value)),

            "usedh" => Self::UseDh(bool::from_str(&value.to_lowercase())?),
            "htbf" => Self::HomeTeamBatsFirst(bool::from_str(value)?),
            "date" => Self::GameDate(NaiveDate::parse_from_str(value, "%Y/%m/%d")?),
            "starttime" => Self::StartTime(Self::parse_time(value)),

            // # TODO: Add error correction for optional fields rather than passing in None
            "wp" => Self::WinningPitcher(t8().ok()),
            "lp" => Self::LosingPitcher(t8().ok()),
            "save" => Self::SavePitcher(t8().ok()),
            "gwrbi" => Self::GameWinningRbi(t8().ok()),
            "scorer" | "oscorer" => Self::Scorer(t16().ok()),
            "inputter" => Self::Inputter(t16().ok()),
            "translator" => Self::Translator(t16().ok()),
            "inputtime" => Self::InputDate(Self::parse_datetime(value)),
            "edittime" => Self::EditDate(Self::parse_datetime(value)),
            "tiebreaker" => Self::Tiebreaker,
            "inputprogvers" => Self::InputProgramVersion,
            "umpchange" => Self::UmpireChange,
            _ => Self::Unrecognized,
        };
        match info {
            Self::Unrecognized => bail!("Unrecognized info type: {:?}", record),
            _ => Ok(info),
        }
    }
}

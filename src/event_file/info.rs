use anyhow::{anyhow, Result};
use chrono::{NaiveDate, NaiveTime};
use strum_macros::EnumString;

use crate::event_file::traits::{FromRetrosheetRecord, Player, RetrosheetEventRecord, RetrosheetVolunteer, Scorer, Umpire};
use crate::util::parse_positive_int;
use std::str::FromStr;

#[derive(Debug, Eq, PartialEq, EnumString)]
#[strum(serialize_all = "lowercase")]
pub enum HowScored {
    Park,
    Tv,
    Radio,
    Unknown
}

#[derive(Debug, Eq, PartialEq, EnumString)]
#[strum(serialize_all = "lowercase")]
pub enum FieldCondition {
    Dry,
    Soaked,
    Wet,
    Damp,
    Unknown
}

#[derive(Debug, Eq, PartialEq, EnumString)]
#[strum(serialize_all = "lowercase")]
pub enum Precipitation {
    Rain,
    Drizzle,
    Showers,
    Snow,
    None,
    Unknown
}

#[derive(Debug, Eq, PartialEq, EnumString)]
#[strum(serialize_all = "lowercase")]
pub enum Sky {
    Cloudy,
    Dome,
    Night,
    Overcast,
    Sunny,
    Unknown
}

#[derive(Debug, Eq, PartialEq, EnumString)]
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

type Team = String;
type Park = String;


#[derive(Debug, Eq, PartialEq, EnumString)]
#[strum(serialize_all = "lowercase")]
pub enum DayNight {
    Day,
    Night,
    Unknown
}

#[derive(Debug, Eq, PartialEq, EnumString)]
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

#[derive(Debug, Eq, PartialEq, EnumString)]
#[strum(serialize_all = "lowercase")]
pub enum PitchDetail {
    Pitches,
    Count,
    None,
    Unknown
}

#[derive(Debug)]
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
    UmpHome(Umpire),
    Ump1B(Umpire),
    Ump2B(Umpire),
    Ump3B(Umpire),
    UmpLF(Umpire),
    UmpRF(Umpire),
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
    EditTime(Option<String>),
    HowScored(HowScored),
    InputProgramVersion(Option<String>),
    Inputter(Option<RetrosheetVolunteer>),
    InputTime(Option<String>),
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

        let as_string = String::from(value);
        let to_option = {|s: String| if s.is_empty() {Some(s)} else {None}};

        type I = InfoRecord;
        let info = match info_type {
            "visteam" => I::VisitingTeam(as_string),
            "hometeam" => I::HomeTeam(as_string),
            "umphome" => I::UmpHome(as_string),
            "ump1b" => I::Ump1B(as_string),
            "ump2b" => I::Ump2B(as_string),
            "ump3b" => I::Ump3B(as_string),
            "umplf" => I::UmpLF(as_string),
            "umprf" => I::UmpRF(as_string),
            "site" => I::Park(as_string),
            "oscorer" => I::OriginalScorer(as_string),

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

            "wp" => I::WinningPitcher(to_option(as_string)),
            "lp" => I::LosingPitcher(to_option(as_string)),
            "save" => I::SavePitcher(to_option(as_string)),
            "gwrbi" => I::GameWinningRBI(to_option(as_string)),
            "edittime" => I::EditTime(to_option(as_string)),
            "inputtime" => I::InputTime(to_option(as_string)),
            "scorer" => I::Scorer(to_option(as_string)),
            "inputter" => I::Inputter(to_option(as_string)),
            "inputprogvers" => I::InputProgramVersion(to_option(as_string)),
            "translator" => I::Translator(to_option(as_string)),
            "umpchange" => I::UmpireChange,
            _ => I::Unrecognized
        };
        match info {
            I::Unrecognized => Err(anyhow!("Unrecognized info type: {:?}", info_type)),
            _ => Ok(info)
        }
    }
}


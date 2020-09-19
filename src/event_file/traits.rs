use anyhow::{anyhow, Error, Result, Context};
use csv::StringRecord;
use strum_macros::EnumString;
use num_enum::{TryFromPrimitive, TryFromPrimitiveError};
use std::convert::TryFrom;
use crate::event_file::play::PlayType::FieldersChoice;


pub type RetrosheetEventRecord = StringRecord;


pub trait FromRetrosheetRecord {
    fn new(record: &RetrosheetEventRecord) -> Result<Self> where Self: Sized;

    fn error(msg: &str, record: &RetrosheetEventRecord) -> Error {
        anyhow!("{}\nRecord: {:?}", msg, record)
    }
}

#[derive(Debug, Eq, PartialEq, TryFromPrimitive, Copy, Clone, Hash)]
#[repr(u8)]
pub enum LineupPosition {
    PitcherWithDH = 0,
    First,
    Second,
    Third,
    Fourth,
    Fifth,
    Sixth,
    Seventh,
    Eighth,
    Ninth
}
impl TryFrom<&str> for LineupPosition {
    type Error = Error;

    //noinspection RsTypeCheck
    fn try_from(value: &str) -> Result<Self> {
        LineupPosition::try_from(value.parse::<u8>()?).context("Unable to convert to lineup position")
    }
}

#[derive(Debug, Eq, PartialEq, TryFromPrimitive, Copy, Clone, Hash)]
#[repr(u8)]
pub enum FieldingPosition {
    Unknown = 0,
    Pitcher,
    Catcher,
    FirstBaseman,
    SecondBaseman,
    ThirdBaseman,
    Shortstop,
    LeftFielder,
    CenterFielder,
    RightFielder,
    DesignatedHitter,
    PinchHitter,
    PinchRunner
}
impl TryFrom<&str> for FieldingPosition {
    type Error = Error;

    //noinspection RsTypeCheck
    fn try_from(value: &str) -> Result<Self> {
        FieldingPosition::try_from(value.parse::<u8>()?).context("Unable to convert to fielding position")
    }
}


pub type Inning = u8;

type Person = String;
pub type Player = Person;
pub type Umpire = Person;
pub type RetrosheetVolunteer = Person;
pub type Scorer = Person;

pub type Batter = Player;
pub type Pitcher = Player;
pub type Fielder = Player;

#[derive(Debug, Eq, PartialEq, EnumString, Hash, Copy, Clone)]
pub enum Side {
    #[strum(serialize = "0")]
    Away,
    #[strum(serialize = "1")]
    Home
}


use anyhow::{anyhow, Error, Result, Context};
use csv::StringRecord;
use strum_macros::EnumString;
use num_enum::{TryFromPrimitive, IntoPrimitive};
use std::convert::{TryFrom};
use tinystr::{TinyStr8, TinyStr16};

use crate::util::digit_vec;


pub type RetrosheetEventRecord = StringRecord;

#[derive(Debug, Eq, PartialEq, TryFromPrimitive, IntoPrimitive, Copy, Clone, Hash)]
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
impl Default for LineupPosition {
    fn default() -> Self {Self::First}
}

impl LineupPosition {
    //noinspection RsTypeCheck
    pub fn next(self) -> Result<Self> {
        let as_u8: u8 = self.into();
        match self {
            Self::PitcherWithDH => Err(anyhow!("Pitcher has no lineup position with DH in the game")),
            Self::Ninth => Ok(Self::First),
            _ => Ok(Self::try_from(as_u8 + 1)?)
        }
    }

    pub fn bats_in_lineup(&self) -> bool {
        *self as u8 > 0
    }

    pub fn retrosheet_string(self) -> String {
        let as_u8: u8 = self.into();
        as_u8.to_string()
    }
}

impl TryFrom<&str> for LineupPosition {
    type Error = Error;

    //noinspection RsTypeCheck
    fn try_from(value: &str) -> Result<Self> {
        LineupPosition::try_from(value.parse::<u8>()?).context("Unable to convert to lineup position")
    }
}

#[derive(Debug, Eq, PartialEq, TryFromPrimitive, IntoPrimitive, Copy, Clone, Hash)]
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
impl FieldingPosition {
    //noinspection RsTypeCheck
    pub fn fielding_vec(int_str: &str) -> Vec<Self> {
        digit_vec(int_str).iter().map(|d|Self::try_from(*d).unwrap_or(Self::Unknown)).collect()
    }

    /// Indicates whether the position is actually a true fielding position, as opposed
    /// to a player who does not appear in the field but is given a mock position
    pub fn plays_in_field(&self) -> bool {
        let numeric_position: u8 = (*self).into();
        (1..10).contains(&numeric_position)
    }

    pub fn retrosheet_string(self) -> String {
        let as_u8: u8 = self.into();
        as_u8.to_string()
    }
}
impl Default for FieldingPosition {
    fn default() -> Self { Self::Unknown }
}

impl TryFrom<&str> for FieldingPosition {
    type Error = Error;

    //noinspection RsTypeCheck
    fn try_from(value: &str) -> Result<Self> {
        FieldingPosition::try_from(value.parse::<u8>()?).context("Unable to convert to fielding position")
    }
}


pub type Inning = u8;

pub(crate) type Person = TinyStr8;
pub type MiscInfoString = TinyStr16;


pub type Player = Person;
pub type Umpire = Person;

pub type Batter = Player;
pub type Pitcher = Player;
pub type Fielder = Player;

pub type RetrosheetVolunteer = MiscInfoString;
pub type Scorer = MiscInfoString;

#[derive(Debug, Eq, PartialEq, EnumString, Hash, Copy, Clone)]
pub enum Side {
    #[strum(serialize = "0")]
    Away,
    #[strum(serialize = "1")]
    Home
}

impl Side {
    pub fn flip(&self) -> Self {
        match self {
            Self::Away => Self::Home,
            Self::Home => Self::Away
        }
    }
    pub fn retrosheet_str(&self) -> &str {
        match self {
            Self::Away => "0",
            Self::Home => "1"
        }
    }
}

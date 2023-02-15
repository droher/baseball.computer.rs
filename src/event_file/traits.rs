use std::convert::TryFrom;

use anyhow::{anyhow, Context, Error, Result};
use arrayvec::ArrayString;
use bounded_integer::BoundedUsize;
use csv::StringRecord;
use fixed_map::Key;
use num_enum::{IntoPrimitive, TryFromPrimitive};
use serde::ser::SerializeStruct;
use serde::{Deserialize, Serialize, Serializer};
use serde_repr::{Deserialize_repr, Serialize_repr};
use strum_macros::{Display, EnumIter, EnumString};

use crate::event_file::info::{InfoRecord, Team};
use crate::event_file::misc::digit_vec;
use crate::event_file::parser::{MappedRecord, RecordSlice};

pub const MAX_EVENTS_PER_GAME: usize = 255;
pub const MAX_GAMES_PER_FILE: usize = 255;
pub const EVENT_KEY_BUFFER: usize = MAX_EVENTS_PER_GAME * MAX_GAMES_PER_FILE;

pub type RetrosheetEventRecord = StringRecord;
pub type SequenceId = BoundedUsize<1, MAX_EVENTS_PER_GAME>;

#[derive(
    Ord,
    PartialOrd,
    Debug,
    Eq,
    PartialEq,
    TryFromPrimitive,
    IntoPrimitive,
    Copy,
    Clone,
    Hash,
    Serialize_repr,
    Deserialize_repr,
    Key,
)]
#[repr(u8)]
pub enum LineupPosition {
    PitcherWithDh = 0,
    First,
    Second,
    Third,
    Fourth,
    Fifth,
    Sixth,
    Seventh,
    Eighth,
    Ninth,
}
impl Default for LineupPosition {
    fn default() -> Self {
        Self::First
    }
}

impl LineupPosition {
    //noinspection RsTypeCheck
    pub fn next(self) -> Result<Self> {
        let as_u8: u8 = self.into();
        match self {
            Self::PitcherWithDh => Err(anyhow!(
                "Pitcher has no lineup position with DH in the game"
            )),
            Self::Ninth => Ok(Self::First),
            _ => Ok(Self::try_from(as_u8 + 1)?),
        }
    }

    pub fn bats_in_lineup(self) -> bool {
        let as_u8: u8 = (self).into();
        as_u8 > 0
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
        Self::try_from(value.parse::<u8>()?).context("Unable to convert to lineup position")
    }
}

#[derive(
    Ord,
    PartialOrd,
    Debug,
    Eq,
    PartialEq,
    TryFromPrimitive,
    IntoPrimitive,
    Copy,
    Clone,
    Hash,
    EnumIter,
    Serialize_repr,
    Deserialize_repr,
    Display,
    Key,
)]
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
    PinchRunner,
}
impl FieldingPosition {
    //noinspection RsTypeCheck
    pub fn fielding_vec(int_str: &str) -> Vec<Self> {
        digit_vec(int_str)
            .iter()
            .map(|d| Self::try_from(*d).unwrap_or(Self::Unknown))
            .collect()
    }

    /// Indicates whether the position is actually a true position in the lineup, as opposed
    /// to a pinch-hitter/runner placeholder. DH counts as a position for these purposes
    pub fn is_true_position(self) -> bool {
        let numeric_position: u8 = self.into();
        (1..11).contains(&numeric_position)
    }

    pub fn retrosheet_string(self) -> String {
        let as_u8: u8 = self.into();
        as_u8.to_string()
    }
}
impl Default for FieldingPosition {
    fn default() -> Self {
        Self::Unknown
    }
}

impl TryFrom<&str> for FieldingPosition {
    type Error = Error;

    //noinspection RsTypeCheck
    fn try_from(value: &str) -> Result<Self> {
        Self::try_from(value.parse::<u8>()?).context("Unable to convert to fielding position")
    }
}

#[derive(Ord, PartialOrd, Debug, Eq, PartialEq, Copy, Clone, Hash, Serialize, Deserialize)]
pub enum GameType {
    SpringTraining,
    RegularSeason,
    AllStarGame,
    WildCardSeries,
    DivisionSeries,
    LeagueChampionshipSeries,
    WorldSeries,
    NegroLeagues,
    Other,
}

#[derive(Ord, PartialOrd, Debug, Eq, PartialEq, Copy, Clone, Hash, Serialize, Deserialize)]
pub enum FieldingPlayType {
    FieldersChoice,
    Putout,
    Assist,
    Error,
}

pub type Inning = u8;

pub type Person = ArrayString<8>;
pub type MiscInfoString = ArrayString<16>;

pub type Player = Person;
pub type Umpire = Person;

pub type Batter = Player;
pub type Pitcher = Player;
pub type Fielder = Player;

pub type RetrosheetVolunteer = MiscInfoString;
pub type Scorer = MiscInfoString;

#[derive(
    Debug,
    Eq,
    PartialEq,
    EnumString,
    Hash,
    Copy,
    Clone,
    Ord,
    PartialOrd,
    Serialize,
    Deserialize,
    Display,
)]
pub enum Side {
    #[strum(serialize = "0")]
    Away,
    #[strum(serialize = "1")]
    Home,
}

impl Side {
    pub const fn flip(self) -> Self {
        match self {
            Self::Away => Self::Home,
            Self::Home => Self::Away,
        }
    }
    pub const fn retrosheet_str(self) -> &'static str {
        match self {
            Self::Away => "0",
            Self::Home => "1",
        }
    }
}

#[derive(
    Display,
    Debug,
    Eq,
    PartialOrd,
    PartialEq,
    Copy,
    Clone,
    Hash,
    EnumString,
    EnumIter,
    IntoPrimitive,
)]
#[repr(u8)]
pub enum BattingStats {
    AtBats,
    Runs,
    Hits,
    Doubles,
    Triples,
    HomeRuns,
    Rbi,
    SacrificeHits,
    SacrificeFlies,
    HitByPitch,
    Walks,
    IntentionalWalks,
    Strikeouts,
    StolenBases,
    CaughtStealing,
    GroundedIntoDoublePlays,
    ReachedOnInterference,
}

#[derive(
    Display,
    Debug,
    Eq,
    PartialOrd,
    PartialEq,
    Copy,
    Clone,
    Hash,
    EnumString,
    EnumIter,
    IntoPrimitive,
)]
#[repr(u8)]
pub enum DefenseStats {
    OutsPlayed,
    Putouts,
    Assists,
    Errors,
    DoublePlays,
    TriplePlays,
    PassedBalls,
}

#[derive(
    Display,
    Debug,
    Eq,
    PartialOrd,
    PartialEq,
    Copy,
    Clone,
    Hash,
    EnumString,
    EnumIter,
    IntoPrimitive,
)]
#[repr(u8)]
pub enum PitchingStats {
    OutsRecorded,
    NoOutBatters,
    BattersFaced,
    Hits,
    Doubles,
    Triples,
    HomeRuns,
    Runs,
    EarnedRuns,
    Walks,
    IntentionalWalks,
    Strikeouts,
    HitBatsmen,
    WildPitches,
    Balks,
    SacrificeHits,
    SacrificeFlies,
}

#[derive(Debug, Eq, PartialEq, Clone)]
pub struct Matchup<T> {
    pub away: T,
    pub home: T,
}

impl<T> Matchup<T> {
    pub const fn new(away: T, home: T) -> Self {
        Self { away, home }
    }

    pub const fn get(&self, side: Side) -> &T {
        match side {
            Side::Away => &self.away,
            Side::Home => &self.home,
        }
    }

    pub fn get_mut(&mut self, side: Side) -> &mut T {
        match side {
            Side::Away => &mut self.away,
            Side::Home => &mut self.home,
        }
    }

    pub fn get_both_mut(&mut self) -> (&mut T, &mut T) {
        (&mut self.away, &mut self.home)
    }
}

impl<T: Default> Default for Matchup<T> {
    fn default() -> Self {
        Self {
            away: T::default(),
            home: T::default(),
        }
    }
}

impl<T: Sized + Clone> Matchup<T> {
    pub fn apply_both<F, U: Sized>(self, func: F) -> (U, U)
    where
        F: Copy + FnOnce(T) -> U,
    {
        (func(self.away), func(self.home))
    }
}

impl<T: Serialize> Serialize for Matchup<T> {
    fn serialize<S>(&self, serializer: S) -> Result<<S as Serializer>::Ok, <S as Serializer>::Error>
    where
        S: Serializer,
    {
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
        Self {
            away: tup.0,
            home: tup.1,
        }
    }
}

impl TryFrom<&RecordSlice> for Matchup<Team> {
    type Error = Error;

    fn try_from(records: &RecordSlice) -> Result<Self> {
        let home_team = records.iter().find_map(|m| {
            if let MappedRecord::Info(InfoRecord::HomeTeam(t)) = m {
                Some(t)
            } else {
                None
            }
        });
        let away_team = records.iter().find_map(|m| {
            if let MappedRecord::Info(InfoRecord::VisitingTeam(t)) = m {
                Some(t)
            } else {
                None
            }
        });
        Ok(Self {
            away: *away_team.context("Could not find away team info in records")?,
            home: *home_team.context("Could not find home team info in records")?,
        })
    }
}

impl TryFrom<&Vec<InfoRecord>> for Matchup<Team> {
    type Error = Error;

    fn try_from(infos: &Vec<InfoRecord>) -> Result<Self> {
        let home_team = infos.iter().find_map(|m| {
            if let InfoRecord::HomeTeam(t) = m {
                Some(t)
            } else {
                None
            }
        });
        let away_team = infos.iter().find_map(|m| {
            if let InfoRecord::VisitingTeam(t) = m {
                Some(t)
            } else {
                None
            }
        });
        Ok(Self {
            away: *away_team.context("Could not find away team info in records")?,
            home: *home_team.context("Could not find home team info in records")?,
        })
    }
}

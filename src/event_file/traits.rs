use anyhow::{anyhow, Error, Result};
use csv::StringRecord;
use strum_macros::EnumString;


pub type RetrosheetEventRecord = StringRecord;


pub trait FromRetrosheetRecord {
    fn new(record: &RetrosheetEventRecord) -> Result<Self> where Self: Sized;

    fn error(msg: &str, record: &RetrosheetEventRecord) -> Error {
        anyhow!("{}\nRecord: {:?}", msg, record)
    }
}

pub type LineupPosition = u8;
pub type FieldingPosition = u8;
pub type Inning = u8;

type Person = String;
pub type Player = Person;
pub type Umpire = Person;
pub type RetrosheetVolunteer = Person;
pub type Scorer = Person;

pub type Batter = Player;
pub type Pitcher = Player;
pub type Fielder = Player;

#[derive(Debug, Eq, PartialEq, EnumString)]
pub enum Side {
    #[strum(serialize = "0")]
    Away,
    #[strum(serialize = "1")]
    Home
}


use std::str::FromStr;

use anyhow::{anyhow, Error, Result};
use chrono::{NaiveDate, NaiveTime};
use csv::StringRecord;
use num::Integer;
use strum_macros::{EnumDiscriminants, EnumString};

pub type LineupPosition = u8;
pub type FieldingPosition = u8;
pub type Inning = u8;
pub type PitchSequence<'a> = &'a str;
pub type Play<'a> = &'a str;
pub type Comment<'a> = &'a str;

pub type RetrosheetEventRecord = StringRecord;


type Person<'a> = &'a str;
type Player<'a> = Person<'a>;
type Umpire<'a> = Person<'a>;
type RetrosheetVolunteer<'a> = Person<'a>;
type Scorer<'a> = Person<'a>;

type Batter<'a> = Player<'a>;
type Fielder<'a> = Player<'a>;

#[derive(Debug, Eq, PartialEq, EnumString)]
#[strum(serialize_all = "lowercase")]
pub enum EventLineType {
    Id,
    Version,
    Info,
    Start,
    #[strum(serialize = "sub")]
    Substitution,
    Play,
    #[strum(serialize = "badj")]
    BatHandAdjustment,
    #[strum(serialize = "padj")]
    PitchHandAdjustment,
    #[strum(serialize = "ladj")]
    BatOutOfOrder,
    Data,
    #[strum(serialize = "com")]
    Comment
}

#[derive(Debug, Eq, PartialEq, EnumString)]
enum Hand {L, R, S, B}

#[derive(Debug, Eq, PartialEq, EnumString)]
enum TeamKind {
    #[strum(serialize = "0")]
    Away,
    #[strum(serialize = "1")]
    Home
}

pub trait FromRetrosheetRecord<'a> {
    fn new(record: &'a RetrosheetEventRecord) -> Result<Self> where Self: Sized;

    fn error(msg: &str, record: &'a RetrosheetEventRecord) -> Error {
        anyhow!("{}\nRecord: {:?}", msg, record)
    }
}

#[derive(Debug)]
pub struct GameId<'a> {pub id: &'a str}
impl<'a> FromRetrosheetRecord<'a> for GameId<'a> {
    fn new(record: &'a RetrosheetEventRecord) -> Result<GameId<'a>> {
        Ok(GameId {
            id: record.get(1).ok_or(Self::error("No game ID string found", record))?
        })
    }
}

#[derive(Debug)]
pub struct HandAdjustment<'a> {player_id: &'a str, hand: Hand}
pub type BatHandAdjustment<'a> = HandAdjustment<'a>;
pub type PitchHandAdjustment<'a> = HandAdjustment<'a>;

impl<'a> FromRetrosheetRecord<'a> for HandAdjustment<'a> {
    fn new(record: &'a RetrosheetEventRecord) -> Result<HandAdjustment<'a>> {
        let (player_id, hand) = (record.get(1), record.get(2));
        Ok(HandAdjustment {
            player_id: player_id.ok_or(Self::error("Missing player ID", record))?,
            hand: Hand::from_str(hand.unwrap_or(""))?
        })
    }
}

#[derive(Debug)]
pub struct LineupAdjustment {team_kind: TeamKind, lineup_position: LineupPosition}

impl FromRetrosheetRecord<'_> for LineupAdjustment {
    fn new(record: &RetrosheetEventRecord) -> Result<LineupAdjustment> {
        let (hv, pos) = (record.get(1), record.get(2));
        Ok(LineupAdjustment {
            team_kind: TeamKind::from_str(hv.unwrap_or(""))?,
            lineup_position: pos.unwrap_or("").parse::<LineupPosition>()?,
        })
    }
}

#[derive(Debug)]
enum Position {
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

#[derive(Debug, Eq, PartialEq, EnumString)]
#[strum(serialize_all = "lowercase")]
enum HowScored {
    Park,
    Tv,
    Radio,
    Unknown
}

#[derive(Debug, Eq, PartialEq, EnumString)]
#[strum(serialize_all = "lowercase")]
enum FieldCondition {
    Dry,
    Soaked,
    Wet,
    Damp,
    Unknown
}

#[derive(Debug, Eq, PartialEq, EnumString)]
#[strum(serialize_all = "lowercase")]
enum Precipitation {
    Rain,
    Drizzle,
    Showers,
    Snow,
    None,
    Unknown
}

#[derive(Debug, Eq, PartialEq, EnumString)]
#[strum(serialize_all = "lowercase")]
enum Sky {
    Cloudy,
    Dome,
    Night,
    Overcast,
    Sunny,
    Unknown
}

#[derive(Debug, Eq, PartialEq, EnumString)]
#[strum(serialize_all = "lowercase")]
enum WindDirection {
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

type Team<'a> = &'a str;
type Park<'a> = &'a str;


#[derive(Debug, Eq, PartialEq, EnumString)]
#[strum(serialize_all = "lowercase")]
enum DayNight {
    Day,
    Night
}

#[derive(Debug, Eq, PartialEq, EnumString)]
enum GameType {
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
enum PitchDetail {
    Pitches,
    Count,
    None,
    Unknown
}

#[derive(Debug)]
pub enum InfoRecord<'a> {
    VisitingTeam(Team<'a>),
    HomeTeam(Team<'a>),
    GameDate(NaiveDate),
    GameType(GameType),
    StartTime(Option<NaiveTime>),
    DayNight(DayNight),
    UseDH(bool),
    HomeTeamBatsFirst(bool),
    PitchDetail(PitchDetail),
    UmpHome(Umpire<'a>),
    Ump1B(Umpire<'a>),
    Ump2B(Umpire<'a>),
    Ump3B(Umpire<'a>),
    UmpLF(Umpire<'a>),
    UmpRF(Umpire<'a>),
    FieldCondition(FieldCondition),
    Precipitation(Precipitation),
    Sky(Sky),
    Temp(Option<u8>),
    WindDirection(WindDirection),
    WindSpeed(Option<u8>),
    TimeOfGameMinutes(Option<u16>),
    Attendance(Option<u32>),
    Park(Park<'a>),
    WinningPitcher(Option<Player<'a>>),
    LosingPitcher(Option<Player<'a>>),
    SavePitcher(Option<Player<'a>>),
    GameWinningRBI(Option<Player<'a>>),
    EditTime(Option<&'a str>),
    HowScored(HowScored),
    InputProgramVersion(Option<&'a str>),
    Inputter(Option<RetrosheetVolunteer<'a>>),
    InputTime(Option<&'a str>),
    Scorer(Option<Scorer<'a>>),
    OriginalScorer(Scorer<'a>),
    Translator(Option<RetrosheetVolunteer<'a>>),
    Unrecognized
}
impl InfoRecord<'_> {
    fn parse_time(time_str: &str) -> InfoRecord {
        let padded_time = format!("{:0>4}", time_str);
        let time = NaiveTime::parse_from_str(&padded_time, "%I%M");
        match time {
            Ok(t) => InfoRecord::StartTime(Some(t)),
            Err(_) => InfoRecord::StartTime(None)
        }
    }

    fn parse_positive_int<T: Integer + FromStr>(temp_str: Option<&str>) -> Option<T> {
        let unwrapped = temp_str.unwrap_or("");
        let int = unwrapped.parse::<T>();
        match int {
            Ok(i) if !i.is_zero() => Some(i),
            _ => None
        }
    }
}

impl<'a> FromRetrosheetRecord<'a> for InfoRecord<'a> {
    fn new(record: &'a RetrosheetEventRecord) -> Result<InfoRecord<'a>> {
        let info_type = record.get(1).ok_or(Self::error("Missing Info Type", record))?;
        let value = record.get(2);

        let as_ref = value.ok_or(Self::error("Unexpected missing value", record))?;
        type I<'a> = InfoRecord<'a>;
        let info = match info_type {
            "visteam" => I::VisitingTeam(as_ref),
            "hometeam" => I::HomeTeam(as_ref),
            "umphome" => I::UmpHome(as_ref),
            "ump1b" => I::Ump1B(as_ref),
            "ump2b" => I::Ump2B(as_ref),
            "ump3b" => I::Ump3B(as_ref),
            "umplf" => I::UmpLF(as_ref),
            "umprf" => I::UmpRF(as_ref),
            "site" => I::Park(as_ref),
            "oscorer" => I::OriginalScorer(as_ref),

            "number" => I::GameType(GameType::from_str(as_ref)?),
            "daynight" => I::DayNight(DayNight::from_str(as_ref)?),
            "pitches" => I::PitchDetail(PitchDetail::from_str(as_ref)?),
            "fieldcond" => I::FieldCondition(FieldCondition::from_str(as_ref)?),
            "precip" => I::Precipitation(Precipitation::from_str(as_ref)?),
            "sky" => I::Sky(Sky::from_str(as_ref)?),
            "winddir" => I::WindDirection(WindDirection::from_str(as_ref)?),
            "howscored" => I::HowScored(HowScored::from_str(as_ref)?),

            "windspeed" => I::WindSpeed(I::parse_positive_int::<u8>(value)),
            "timeofgame" => I::TimeOfGameMinutes(I::parse_positive_int::<u16>(value)),
            "attendance" => I::Attendance(I::parse_positive_int::<u32>(value)),
            "temp" => I::Temp(I::parse_positive_int::<u8>(value)),

            "usedh" => I::UseDH(bool::from_str(as_ref)?),
            "htbf" => I::HomeTeamBatsFirst(bool::from_str(as_ref)?),
            "date" => I::GameDate(NaiveDate::parse_from_str(as_ref, "%Y/%m/%d")?),
            "starttime" => I::parse_time(as_ref),

            "wp" => I::WinningPitcher(value),
            "lp" => I::LosingPitcher(value),
            "save" => I::SavePitcher(value),
            "gwrbi" => I::GameWinningRBI(value),
            "edittime" => I::EditTime(value),
            "inputtime" => I::InputTime(value),
            "scorer" => I::Scorer(value),
            "inputter" => I::Inputter(value),
            "inputprogvers" => I::InputProgramVersion(value),
            "translator" => I::Translator(value),
            _ => I::Unrecognized
        };
        match info {
            I::Unrecognized => Err(anyhow!("Unrecognized info type: {:?}", info_type)),
            _ => Ok(info)
        }
    }
}

#[derive(Debug)]
pub struct AppearanceRecord<'a> {
    player: Player<'a>,
    team_kind: TeamKind,
    lineup_position: LineupPosition,
    fielding_position: FieldingPosition
}
impl<'a> FromRetrosheetRecord<'a> for AppearanceRecord<'a> {
    fn new(record: &'a RetrosheetEventRecord) -> Result<AppearanceRecord<'a>> {
        Ok(AppearanceRecord {
            player: record.get(1).ok_or(Self::error("Missing player ID", record))?,
            team_kind: TeamKind::from_str(record.get(3).unwrap_or(""))?,
            lineup_position: record.get(4).unwrap_or("").parse::<LineupPosition>()?,
            fielding_position:  record.get(5).unwrap_or("").trim().parse::<FieldingPosition>()?
        })
    }
}

pub type StartRecord<'a> = AppearanceRecord<'a>;
pub type SubstitutionRecord<'a>= AppearanceRecord<'a>;

#[derive(Debug, Eq, PartialEq, EnumString)]
#[strum(serialize_all = "lowercase")]
enum InningFrame {
    Top,
    Bottom,
}

#[derive(Debug, Eq, PartialEq, EnumString)]
#[strum(serialize_all = "lowercase")]
enum Base {
    Home,
    First,
    Second,
    Third
}

#[derive(Debug)]
struct Count { balls: Option<u8>, strikes: Option<u8> }
impl Count {
    fn new(count_str: Option<&str>) -> Result<Count> {
        let mut count_iter = count_str.unwrap_or("").chars();
        let count_arr = [count_iter.next(), count_iter.next()];
        let mut as_ints = count_arr.iter().map(
            |c| c.unwrap_or('a').to_string().parse::<u8>().ok()
        ).flatten();
        Ok(Count {balls: as_ints.next(), strikes: as_ints.next()})
    }
}

#[derive(Debug, Eq, PartialEq, EnumString)]
#[strum(serialize_all = "camel_case")]
enum PitchType {
    PickoffFirst,
    PickoffSecond,
    PickoffThird,
    PlayNotInvolvingBatter,
    Ball,
    CalledStrike,
    Foul,
    HitBatter,
    IntentionalBall,
    StrikeUnknownType,
    FoulBunt,
    MissedBunt,
    NoPitch,
    FoulTipBunt,
    Pitchout,
    SwingingOnPitchout,
    FoulOnPitchout,
    SwingingStrike,
    FoulTip,
    Unknown,
    BallOnPitcherGoingToMouth,
    InPlay,
    InPlayOnPitchout
}

struct Pitch {
    pitch_type: PitchType,
    runners_going: bool,
    catcher_pickoff_attempt: bool,
    blocked_by_catcher: bool
}

// #[derive(Debug, EnumDiscriminants)]
// #[strum_discriminants(derive(EnumString))]
// enum BatterPlay {
//     FlyOut(Fielder),
//     GroundOut(Vec<Fielder>),
//     DoublePlay(Fielder),
//     CatcherInterference,
//     OtherFielderInterference(Fielder),
//     Single(Fielder),
//     Double(Fielder),
//     Triple(Fielder),
//     GroundRuleDouble,
//     ReachedOnError(Fielder),
//     FieldersChoice(Fielder),
//     ErrorOnFlyBall(Fielder),
//     HomeRun,
//     InsideTheParkHomeRun(Fielder),
//     HitByPitch,
//     StrikeOut(Option<RunnerPlay>),
//     NoPlay,
//     IntentionalWalk(Option<RunnerPlay>),
//     Walk(Option<RunnerPlay>)
// }
//
// #[derive(Debug, EnumDiscriminants)]
// #[strum_discriminants(derive(EnumString))]
// enum RunnerPlay {
//     Balk,
//     CaughtStealing(Base, Vec<Fielder>),
//     DefensiveIndifference,
//     OtherAdvance,
//     PassedBall,
//     WildPitch,
//     PickedOff(Base, Vec<Fielder>),
//     PickedOffCaughtStealing(Base, Vec<Fielder>),
//     StolenBase(Base)
// }
//
// struct RunnerAdvance {
//     from: Base,
//     to: Base,
//
// }
// type SuccessfulRunnerAdvance = RunnerAdvance;
// struct UnsuccessfulRunnerAdvance {
//     attempt: RunnerAdvance,
//     fielders: Vec<Fielder>
// }
//
//
// type HitLocation = String;
//
// #[derive(Debug, EnumDiscriminants)]
// #[strum_discriminants(derive(EnumString))]
// enum PlayModifier {
//     AppealPlay,
//     PopUpBunt(Option<HitLocation>),
//     GroundBallBunt(Option<HitLocation>),
//     BuntGroundIntoDoublePlay(Option<HitLocation>),
//     BatterInterference(Option<HitLocation>),
//     LineDriveBunt(Option<HitLocation>),
//     BatingOutOfTurn,
//     BuntPopUp(Option<HitLocation>),
//     BuntPoppedIntoDoublePlay(Option<HitLocation>),
//     RunnerHitByBattedBall(Option<HitLocation>),
//     CalledThirdStrike,
//     CourtesyBatter,
//     CourtesyFielder,
//     CourtesyRunner,
//     UnspecifiedDoublePlay(Option<HitLocation>),
//     ErrorOn(Position),
//     Fly(Option<HitLocation>),
//     FlyBallDoublePlay(Option<HitLocation>),
//     FanInterference,
//     Foul(Option<HitLocation>),
//     ForceOut(Option<HitLocation>),
//     GroundBall(Option<HitLocation>),
//     GroundBallDoublePlay(Option<HitLocation>),
//     GroundBallTriplePlay(Option<HitLocation>),
//     InfieldFlyRule(Option<HitLocation>),
//     Interference(Option<HitLocation>),
//     InsideTheParkHomeRun(Option<HitLocation>),
//     LineDrive(Option<HitLocation>),
//     LinedIntoDoublePlay(Option<HitLocation>),
//     LinedIntoTriplePlay(Option<HitLocation>),
//     ManageChallengeOfCallOnField,
//     NoDoublePlayCredited,
//     Obstruction,
//     PopFly(Option<HitLocation>),
//     RunnerOutPassingAnotherRunner,
//     RelayToFielderWithNoOutMade(Position),
//     RunnerInterference,
//     SacrificeFly(Option<HitLocation>),
//     SacrificeHit(Option<HitLocation>),
//     Throw,
//     ThrowToBase(Base),
//     UnspecifiedTriplePlay(Option<HitLocation>),
//     UmpireInterference(Option<HitLocation>),
//     UmpireReviewOfCallOnField
// }

#[derive(Debug)]
pub struct PlayRecord<'a> {
    inning: Inning,
    team_kind: TeamKind,
    batter: Batter<'a>,
    count: Count,
    pitch_sequence: PitchSequence<'a>,
    play: Play<'a>
}

impl<'a> FromRetrosheetRecord<'a> for PlayRecord<'a> {
    fn new(record: &'a RetrosheetEventRecord) -> Result<PlayRecord<'a>> {

        Ok(PlayRecord {
            inning: record.get(1).unwrap_or("").parse::<Inning>()?,
            team_kind: TeamKind::from_str(record.get(2).unwrap_or(""))?,
            batter: record.get(3).ok_or(Self::error("Missing batter", record))?,
            count: Count::new(record.get(4))?,
            pitch_sequence: record.get(5).unwrap_or(""),
            play: record.get(6).unwrap_or("")
        })
    }
}

#[derive(Debug)]
pub enum MappedRecord<'a> {
    GameId(GameId<'a>),
    Info(InfoRecord<'a>),
    Start(StartRecord<'a>),
    Substitution(SubstitutionRecord<'a>),
    Play(PlayRecord<'a>),
    BatHandAdjustment(BatHandAdjustment<'a>),
    PitchHandAdjustment(PitchHandAdjustment<'a>),
    LineupAdjustment(LineupAdjustment),
    Data,
    Comment(Comment<'a>)
}

impl<'a> FromRetrosheetRecord<'a> for MappedRecord<'a> {
    fn new(record: &'a RetrosheetEventRecord) -> Result<MappedRecord<'a>>{
        let line_type = record.get(0).ok_or(anyhow!("No record"))?;
        type E = EventLineType;
        let mapped= match E::from_str(line_type)? {
            E::Id => MappedRecord::GameId(GameId::new(record)?),
            E::Info => MappedRecord::Info(InfoRecord::new(record)?),
            E::Start => MappedRecord::Start(StartRecord::new(record)?),
            E::Substitution => MappedRecord::Substitution(SubstitutionRecord::new(record)?),
            E::Play => MappedRecord::Play(PlayRecord::new(record)?),
            E::BatHandAdjustment => MappedRecord::BatHandAdjustment(BatHandAdjustment::new(record)?),
            E::PitchHandAdjustment => MappedRecord::PitchHandAdjustment(PitchHandAdjustment::new(record)?),
            E::BatOutOfOrder => MappedRecord::LineupAdjustment(LineupAdjustment::new(record)?),
            E::Comment => MappedRecord::Comment(record.get(1).unwrap()),
            _ => MappedRecord::Data
        };
        Ok(mapped)
    }
}
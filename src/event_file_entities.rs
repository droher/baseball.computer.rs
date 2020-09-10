use std::str::FromStr;

use anyhow::{anyhow, Error, Result};
use chrono::{NaiveDate, NaiveTime};
use csv::StringRecord;
use num::Integer;
use strum_macros::{EnumDiscriminants, EnumString};

pub type LineupPosition = u8;
pub type FieldingPosition = u8;
pub type Inning = u8;
pub type PitchSequence = String;
pub type Play = String;
pub type Comment = String;

pub type RetrosheetEventRecord = StringRecord;


type Person = String;
type Player = Person;
type Umpire = Person;
type RetrosheetVolunteer = Person;
type Scorer = Person;

type Batter = Player;
type Fielder = Player;

type Pitcher = Player;
type Catcher = Player;
type FirstBaseman = Player;
type SecondBaseman = Player;
type ThirdBaseman = Player;
type Shortstop = Player;
type LeftFielder = Player;
type CenterFielder = Player;
type RightFielder = Player;
type DesignatedHitter = Player;
type PinchHitter = Player;
type PinchRunner = Player;

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

pub trait FromRetrosheetRecord {
    fn new(record: &RetrosheetEventRecord) -> Result<Self> where Self: Sized;

    fn error(msg: &str, record: &RetrosheetEventRecord) -> Error {
        anyhow!("{}\nRecord: {:?}", msg, record)
    }
}

#[derive(Debug)]
pub struct GameId {pub id: String}
impl FromRetrosheetRecord for GameId {
    fn new(record: &RetrosheetEventRecord) -> Result<GameId> {
        Ok(GameId {
            id: String::from(record.get(1).ok_or(Self::error("No game ID string found", record))?)
        })
    }
}

#[derive(Debug)]
pub struct HandAdjustment {player_id: String, hand: Hand}
pub type BatHandAdjustment = HandAdjustment;
pub type PitchHandAdjustment = HandAdjustment;

impl FromRetrosheetRecord for HandAdjustment {
    fn new(record: &RetrosheetEventRecord) -> Result<HandAdjustment> {
        let (player_id, hand) = (record.get(1), record.get(2));
        Ok(HandAdjustment {
            player_id: String::from(player_id.ok_or(Self::error("Missing player ID", record))?),
            hand: Hand::from_str(hand.unwrap_or(""))?
        })
    }
}

#[derive(Debug)]
pub struct LineupAdjustment {team_kind: TeamKind, lineup_position: LineupPosition}

impl FromRetrosheetRecord for LineupAdjustment {
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
    Pitcher(Pitcher),
    Catcher(Catcher),
    FirstBaseman(FirstBaseman),
    SecondBaseman(SecondBaseman),
    ThirdBaseman(ThirdBaseman),
    Shortstop(Shortstop),
    LeftFielder(LeftFielder),
    CenterFielder(CenterFielder),
    RightFielder(RightFielder),
    DesignatedHitter(DesignatedHitter),
    PinchHitter(PinchHitter),
    PinchRunner(PinchRunner)
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

type Team = String;
type Park = String;


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

    fn parse_positive_int<T: Integer + FromStr>(temp_str: Option<&str>) -> Option<T> {
        let unwrapped = temp_str.unwrap_or("");
        let int = unwrapped.parse::<T>();
        match int {
            Ok(i) if !i.is_zero() => Some(i),
            _ => None
        }
    }
}

impl FromRetrosheetRecord for InfoRecord {
    fn new(record: &RetrosheetEventRecord) -> Result<InfoRecord> {
        let info_type = record.get(1).ok_or(Self::error("Missing Info Type", record))?;
        let value = record.get(2);

        let opt_string = {|| value.map(|s| String::from(s))};
        let as_ref = value.ok_or(Self::error("Unexpected missing value", record));
        let as_string = {|| opt_string().ok_or(Self::error("Unexpected missing value", record))};
        type I = InfoRecord;
        let info = match info_type {
            "visteam" => I::VisitingTeam(as_string()?),
            "hometeam" => I::HomeTeam(as_string()?),
            "umphome" => I::UmpHome(as_string()?),
            "ump1b" => I::Ump1B(as_string()?),
            "ump2b" => I::Ump2B(as_string()?),
            "ump3b" => I::Ump3B(as_string()?),
            "umplf" => I::UmpLF(as_string()?),
            "umprf" => I::UmpRF(as_string()?),
            "site" => I::Park(as_string()?),
            "oscorer" => I::OriginalScorer(as_string()?),

            "number" => I::GameType(GameType::from_str(as_ref?)?),
            "daynight" => I::DayNight(DayNight::from_str(as_ref?)?),
            "pitches" => I::PitchDetail(PitchDetail::from_str(as_ref?)?),
            "fieldcond" => I::FieldCondition(FieldCondition::from_str(as_ref?)?),
            "precip" => I::Precipitation(Precipitation::from_str(as_ref?)?),
            "sky" => I::Sky(Sky::from_str(as_ref?)?),
            "winddir" => I::WindDirection(WindDirection::from_str(as_ref?)?),
            "howscored" => I::HowScored(HowScored::from_str(as_ref?)?),

            "windspeed" => I::WindSpeed(I::parse_positive_int::<u8>(value)),
            "timeofgame" => I::TimeOfGameMinutes(I::parse_positive_int::<u16>(value)),
            "attendance" => I::Attendance(I::parse_positive_int::<u32>(value)),
            "temp" => I::Temp(I::parse_positive_int::<u8>(value)),

            "usedh" => I::UseDH(bool::from_str(as_ref?)?),
            "htbf" => I::HomeTeamBatsFirst(bool::from_str(as_ref?)?),
            "date" => I::GameDate(NaiveDate::parse_from_str(as_ref?, "%Y/%m/%d")?),
            "starttime" => I::parse_time(as_ref?),

            "wp" => I::WinningPitcher(opt_string()),
            "lp" => I::LosingPitcher(opt_string()),
            "save" => I::SavePitcher(opt_string()),
            "gwrbi" => I::GameWinningRBI(opt_string()),
            "edittime" => I::EditTime(opt_string()),
            "inputtime" => I::InputTime(opt_string()),
            "scorer" => I::Scorer(opt_string()),
            "inputter" => I::Inputter(opt_string()),
            "inputprogvers" => I::InputProgramVersion(opt_string()),
            "translator" => I::Translator(opt_string()),
            _ => I::Unrecognized
        };
        match info {
            I::Unrecognized => Err(anyhow!("Unrecognized info type: {:?}", info_type)),
            _ => Ok(info)
        }
    }
}

#[derive(Debug)]
pub struct AppearanceRecord {
    player: Player,
    team_kind: TeamKind,
    lineup_position: LineupPosition,
    fielding_position: FieldingPosition
}
impl FromRetrosheetRecord for AppearanceRecord {
    fn new(record: &RetrosheetEventRecord) -> Result<AppearanceRecord> {
        Ok(AppearanceRecord {
            player: String::from(record.get(1).ok_or(Self::error("Missing player ID", record))?),
            team_kind: TeamKind::from_str(record.get(3).unwrap_or(""))?,
            lineup_position: record.get(4).unwrap_or("").parse::<LineupPosition>()?,
            fielding_position:  record.get(5).unwrap_or("").trim().parse::<FieldingPosition>()?
        })
    }
}

pub type StartRecord = AppearanceRecord;
pub type SubstitutionRecord = AppearanceRecord;

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

#[derive(Debug, EnumDiscriminants)]
#[strum_discriminants(derive(EnumString))]
enum BatterPlay {
    FlyOut(Fielder),
    GroundOut(Vec<Fielder>),
    DoublePlay(Fielder),
    CatcherInterference,
    OtherFielderInterference(Fielder),
    Single(Fielder),
    Double(Fielder),
    Triple(Fielder),
    GroundRuleDouble,
    ReachedOnError(Fielder),
    FieldersChoice(Fielder),
    ErrorOnFlyBall(Fielder),
    HomeRun,
    InsideTheParkHomeRun(Fielder),
    HitByPitch,
    StrikeOut(Option<RunnerPlay>),
    NoPlay,
    IntentionalWalk(Option<RunnerPlay>),
    Walk(Option<RunnerPlay>)
}

#[derive(Debug, EnumDiscriminants)]
#[strum_discriminants(derive(EnumString))]
enum RunnerPlay {
    Balk,
    CaughtStealing(Base, Vec<Fielder>),
    DefensiveIndifference,
    OtherAdvance,
    PassedBall,
    WildPitch,
    PickedOff(Base, Vec<Fielder>),
    PickedOffCaughtStealing(Base, Vec<Fielder>),
    StolenBase(Base)
}

struct RunnerAdvance {
    from: Base,
    to: Base,

}
type SuccessfulRunnerAdvance = RunnerAdvance;
struct UnsuccessfulRunnerAdvance {
    attempt: RunnerAdvance,
    fielders: Vec<Fielder>
}


type HitLocation = String;

#[derive(Debug, EnumDiscriminants)]
#[strum_discriminants(derive(EnumString))]
enum PlayModifier {
    AppealPlay,
    PopUpBunt(Option<HitLocation>),
    GroundBallBunt(Option<HitLocation>),
    BuntGroundIntoDoublePlay(Option<HitLocation>),
    BatterInterference(Option<HitLocation>),
    LineDriveBunt(Option<HitLocation>),
    BatingOutOfTurn,
    BuntPopUp(Option<HitLocation>),
    BuntPoppedIntoDoublePlay(Option<HitLocation>),
    RunnerHitByBattedBall(Option<HitLocation>),
    CalledThirdStrike,
    CourtesyBatter,
    CourtesyFielder,
    CourtesyRunner,
    UnspecifiedDoublePlay(Option<HitLocation>),
    ErrorOn(Position),
    Fly(Option<HitLocation>),
    FlyBallDoublePlay(Option<HitLocation>),
    FanInterference,
    Foul(Option<HitLocation>),
    ForceOut(Option<HitLocation>),
    GroundBall(Option<HitLocation>),
    GroundBallDoublePlay(Option<HitLocation>),
    GroundBallTriplePlay(Option<HitLocation>),
    InfieldFlyRule(Option<HitLocation>),
    Interference(Option<HitLocation>),
    InsideTheParkHomeRun(Option<HitLocation>),
    LineDrive(Option<HitLocation>),
    LinedIntoDoublePlay(Option<HitLocation>),
    LinedIntoTriplePlay(Option<HitLocation>),
    ManageChallengeOfCallOnField,
    NoDoublePlayCredited,
    Obstruction,
    PopFly(Option<HitLocation>),
    RunnerOutPassingAnotherRunner,
    RelayToFielderWithNoOutMade(Position),
    RunnerInterference,
    SacrificeFly(Option<HitLocation>),
    SacrificeHit(Option<HitLocation>),
    Throw,
    ThrowToBase(Base),
    UnspecifiedTriplePlay(Option<HitLocation>),
    UmpireInterference(Option<HitLocation>),
    UmpireReviewOfCallOnField
}

#[derive(Debug)]
pub struct PlayRecord {
    inning: Inning,
    team_kind: TeamKind,
    batter: Batter,
    count: Count,
    pitch_sequence: PitchSequence,
    play: Play
}

impl FromRetrosheetRecord for PlayRecord {
    fn new(record: &RetrosheetEventRecord) -> Result<PlayRecord> {

        Ok(PlayRecord {
            inning: record.get(1).unwrap_or("").parse::<Inning>()?,
            team_kind: TeamKind::from_str(record.get(2).unwrap_or(""))?,
            batter: String::from(record.get(3).unwrap_or("")),
            count: Count::new(record.get(4))?,
            pitch_sequence: String::from(record.get(5).unwrap_or("")),
            play: String::from(record.get(6).unwrap_or(""))
        })
    }
}

#[derive(Debug)]
pub enum MappedRecord {
    GameId(GameId),
    Info(InfoRecord),
    Start(StartRecord),
    Substitution(SubstitutionRecord),
    Play(PlayRecord),
    BatHandAdjustment(BatHandAdjustment),
    PitchHandAdjustment(PitchHandAdjustment),
    LineupAdjustment(LineupAdjustment),
    Data,
    Comment(Comment)
}

impl FromRetrosheetRecord for MappedRecord {
    fn new(record: &RetrosheetEventRecord) -> Result<MappedRecord>{
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
            E::Comment => MappedRecord::Comment(String::from(record.get(1).unwrap())),
            _ => MappedRecord::Data
        };
        Ok(mapped)
    }
}
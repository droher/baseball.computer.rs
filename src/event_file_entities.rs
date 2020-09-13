use std::str::FromStr;
use std::convert::{TryFrom};


use anyhow::{Context, Error, Result, anyhow};
use chrono::{NaiveDate, NaiveTime};
use csv::StringRecord;
use num_traits::{PrimInt};
use strum_macros::{EnumDiscriminants, EnumString};
use smallvec::SmallVec;
use arrayref::array_ref;

use crate::util::parse_positive_int;

pub type LineupPosition = u8;
pub type FieldingPosition = u8;
pub type Inning = u8;

pub type PitchSequence = String;
pub type Play = String;
pub type Comment = String;

pub type RetrosheetEventRecord = StringRecord;


type Person = String;
pub type Player = Person;
type Umpire = Person;
type RetrosheetVolunteer = Person;
type Scorer = Person;

type Batter = Player;
type Baserunner = Player;
pub type Pitcher = Player;
pub type Fielder = Player;

#[derive(Debug, Eq, PartialEq, EnumString)]
enum Hand {L, R, S, B}

#[derive(Debug, Eq, PartialEq, EnumString)]
enum Side {
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
        let record = record.deserialize::<[&str; 2]>(None)?;
        Ok(GameId { id: String::from(record[1]) })
    }
}

#[derive(Debug)]
pub struct HandAdjustment {player_id: String, hand: Hand}
pub type BatHandAdjustment = HandAdjustment;
pub type PitchHandAdjustment = HandAdjustment;

impl FromRetrosheetRecord for HandAdjustment {
    fn new(record: &RetrosheetEventRecord) -> Result<HandAdjustment> {
        let record = record.deserialize::<[&str; 3]>(None)?;

        Ok(HandAdjustment {
            player_id: String::from(record[1]),
            hand: Hand::from_str(record[2])?
        })
    }
}

#[derive(Debug)]
pub struct LineupAdjustment { side: Side, lineup_position: LineupPosition}

impl FromRetrosheetRecord for LineupAdjustment {
    fn new(record: &RetrosheetEventRecord) -> Result<LineupAdjustment> {
        let record = record.deserialize::<[&str; 3]>(None)?;

        Ok(LineupAdjustment {
            side: Side::from_str(record[1])?,
            lineup_position: record[2].parse::<LineupPosition>()?,
        })
    }
}

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

#[derive(Debug)]
pub struct AppearanceRecord {
    player: Player,
    side: Side,
    lineup_position: LineupPosition,
    fielding_position: FieldingPosition
}
impl FromRetrosheetRecord for AppearanceRecord {
    fn new(record: &RetrosheetEventRecord) -> Result<AppearanceRecord> {
        let record = record.deserialize::<[&str; 6]>(None)?;
        Ok(AppearanceRecord {
            player: String::from(record[1]),
            side: Side::from_str(record[3])?,
            lineup_position: record[4].parse::<LineupPosition>()?,
            fielding_position:  record[5].trim_end().parse::<FieldingPosition>()?
        })
    }
}

pub type StartRecord = AppearanceRecord;
pub type SubstitutionRecord = AppearanceRecord;

#[derive(Debug)]
struct Count { balls: Option<u8>, strikes: Option<u8> }
impl Count {
    fn new(count_str: &str) -> Result<Count> {
        let mut ints = count_str.chars().map(|c| c.to_digit(10).map(|i| i as u8));

        Ok(Count {
            balls: ints.next().flatten(),
            strikes: ints.next().flatten()
        })
    }
}

#[derive(Debug)]
pub struct PlayRecord {
    inning: Inning,
    side: Side,
    batter: Batter,
    count: Count,
    pub pitch_sequence: PitchSequence,
    pub play: Play
}

impl FromRetrosheetRecord for PlayRecord {
    fn new(record: &RetrosheetEventRecord) -> Result<PlayRecord> {
        let record = record.deserialize::<[&str; 7]>(None)?;
        Ok(PlayRecord {
            inning: record[1].parse::<Inning>()?,
            side: Side::from_str(record[2])?,
            batter: String::from(record[3]),
            count: Count::new(record[4])?,
            pitch_sequence: String::from(record[5]),
            play: String::from(record[6])
        })
    }
}

#[derive(Debug)]
pub struct EarnedRunRecord {
    pitcher_id: Pitcher,
    earned_runs: u8
}

impl FromRetrosheetRecord for EarnedRunRecord {
    fn new(record: &RetrosheetEventRecord) -> Result<EarnedRunRecord> {
        let arr = record.deserialize::<[&str; 4]>(None)?;
        match arr[1] {
            "er" => Ok(EarnedRunRecord {
                pitcher_id: String::from(arr[2]),
                earned_runs: arr[3].trim_end().parse::<u8>()?
            }),
            _ => Err(Self::error("Unexpected `data` type value", record))
        }
    }
}

#[derive(Debug)]
struct BattingLineStats {
    at_bats: u8,
    runs: u8,
    hits: u8,
    doubles: Option<u8>,
    triples: Option<u8>,
    home_runs: Option<u8>,
    rbi: Option<u8>,
    sacrifice_hits: Option<u8>,
    sacrifice_flies: Option<u8>,
    hit_by_pitch: Option<u8>,
    walks: Option<u8>,
    intentional_walks: Option<u8>,
    strikeouts: Option<u8>,
    stolen_bases: Option<u8>,
    caught_stealing: Option<u8>,
    grounded_into_double_plays: Option<u8>,
    reached_on_interference: Option<u8>
}

impl TryFrom<&[&str; 17]> for BattingLineStats {
    type Error = Error;

    fn try_from(value: &[&str; 17]) -> Result<BattingLineStats> {
        let o = {|i: usize| value[i].parse::<u8>().ok()};
        let u = {|i: usize|
            value[i].parse::<u8>().context("Bad value for batting line stat")
        };
        Ok(BattingLineStats {
            at_bats: u(0)?,
            runs: u(1)?,
            hits: u(2)?,
            doubles: o(3),
            triples: o(4),
            home_runs: o(5),
            rbi: o(6),
            sacrifice_hits: o(7),
            sacrifice_flies: o(8),
            hit_by_pitch: o(9),
            walks: o(10),
            intentional_walks: o(11),
            strikeouts: o(12),
            stolen_bases: o(13),
            caught_stealing: o(14),
            grounded_into_double_plays: o(15),
            reached_on_interference: o(16)
        })
    }
}


#[derive(Debug)]
pub struct BattingLine {
    batter_id: Batter,
    side: Side,
    lineup_position: LineupPosition,
    nth_player_at_position: u8,
    batting_stats: BattingLineStats,
}

impl FromRetrosheetRecord for BattingLine {
    fn new(record: &RetrosheetEventRecord) -> Result<BattingLine> {
        let arr = record.deserialize::<[&str; 23]>(None)?;
        let p = parse_positive_int::<u8>;
        Ok(BattingLine{
            batter_id: arr[2].to_string(),
            side: Side::from_str(arr[3])?,
            lineup_position: p(arr[4]).context("Invalid lineup position")?,
            nth_player_at_position: p(arr[5]).context("Invalid batting sequence position")?,
            batting_stats: BattingLineStats::try_from(array_ref![arr,6,17])?
        })
    }
}

#[derive(Debug)]
pub struct PinchHittingLine {
    pinch_hitter_id: Batter,
    inning: Option<Inning>,
    side: Side,
    batting_stats: Option<BattingLineStats>,
}
impl FromRetrosheetRecord for PinchHittingLine {
    fn new(record: &RetrosheetEventRecord) -> Result<PinchHittingLine> {
        let arr = record.deserialize::<[&str; 22]>(None)?;
        let p = parse_positive_int::<u8>;
        Ok(PinchHittingLine{
            pinch_hitter_id: arr[2].to_string(),
            inning: p(arr[3]),
            side: Side::from_str(arr[4])?,
            batting_stats: BattingLineStats::try_from(array_ref![arr,5,17]).ok()
        })
    }
}


#[derive(Debug)]
pub struct PinchRunningLine {
    pinch_runner_id: Batter,
    inning: Option<Inning>,
    side: Side,
    runs: Option<u8>,
    stolen_bases: Option<u8>,
    caught_stealing: Option<u8>
}

impl FromRetrosheetRecord for PinchRunningLine {
    fn new(record: &RetrosheetEventRecord) -> Result<PinchRunningLine>{
        let arr = record.deserialize::<[&str; 8]>(None)?;
        let p = {|i: usize| arr[i].parse::<u8>().ok()};
        Ok(PinchRunningLine{
            pinch_runner_id: arr[2].to_string(),
            inning: p(3),
            side: Side::from_str(arr[4])?,
            runs: p(5),
            stolen_bases: p(6),
            caught_stealing: p(7)
        })
    }
}

#[derive(Debug)]
pub struct DefenseLineStats {
    outs_played: Option<u8>,
    putouts: Option<u8>,
    assists: Option<u8>,
    errors: Option<u8>,
    double_plays: Option<u8>,
    triple_plays: Option<u8>,
    passed_balls: Option<u8>
}

impl TryFrom<&[&str; 7]> for DefenseLineStats {
    type Error = Error;

    fn try_from(value: &[&str; 7]) -> Result<DefenseLineStats> {
        let o = {|i: usize| value[i].parse::<u8>().ok()};
        Ok(DefenseLineStats {
            outs_played: o(0),
            putouts: o(1),
            assists: o(2),
            errors: o(3),
            double_plays: o(4),
            triple_plays: o(5),
            passed_balls: o(6)
        })
    }
}

#[derive(Debug)]
pub struct DefenseLine {
    fielder_id: Fielder,
    side: Side,
    fielding_position: FieldingPosition,
    nth_player_at_position: u8,
    defensive_stats: Option<DefenseLineStats>
}

impl FromRetrosheetRecord for DefenseLine {
    fn new(record: &RetrosheetEventRecord) -> Result<DefenseLine>{
        let arr = record.deserialize::<[&str; 13]>(None)?;
        let p = parse_positive_int::<u8>;
        Ok(DefenseLine{
            fielder_id: arr[2].to_string(),
            side: Side::from_str(arr[3])?,
            fielding_position: p(arr[4]).context("Invalid fielding position")?,
            nth_player_at_position: p(arr[5]).context("Invalid fielding sequence position")?,
            defensive_stats: DefenseLineStats::try_from(array_ref![arr,6,7]).ok(),
        })
    }
}

#[derive(Debug)]
pub struct PitchingLineStats {
    outs_recorded: u8,
    no_out_batters: Option<u8>,
    batters_faced: Option<u8>,
    hits: u8,
    doubles: Option<u8>,
    triples: Option<u8>,
    home_runs: Option<u8>,
    runs: u8,
    earned_runs: Option<u8>,
    walks: Option<u8>,
    intentional_walks: Option<u8>,
    strikeouts: Option<u8>,
    hit_batsmen: Option<u8>,
    wild_pitches: Option<u8>,
    balks: Option<u8>,
    sacrifice_hits: Option<u8>,
    sacrifice_files: Option<u8>
}

impl TryFrom<&[&str; 17]> for PitchingLineStats {
    type Error = Error;

    fn try_from(value: &[&str; 17]) -> Result<PitchingLineStats> {
        let o = {|i: usize| value[i].parse::<u8>().ok()};
        let u = {|i: usize|
            value[i].parse::<u8>().context("Bad value for pitching line stat")
        };
        Ok(PitchingLineStats {
            outs_recorded: u(0)?,
            no_out_batters: o(1),
            batters_faced: o(2),
            hits: u(3)?,
            doubles:o(4),
            triples: o(5),
            home_runs: o(6),
            runs: u(7)?,
            earned_runs: o(8),
            walks: o(9),
            intentional_walks: o(10),
            strikeouts: o(11),
            hit_batsmen: o(12),
            wild_pitches: o(13),
            balks: o(14),
            sacrifice_hits: o(15),
            sacrifice_files: o(16)
        })
    }
}

#[derive(Debug)]
pub struct PitchingLine {
    pitcher_id: Pitcher,
    side: Side,
    nth_pitcher: u8,
    pitching_stats: PitchingLineStats
}

impl FromRetrosheetRecord for PitchingLine {
    fn new(record: &RetrosheetEventRecord) -> Result<PitchingLine>{
        let arr = record.deserialize::<[&str; 22]>(None)?;
        let p = parse_positive_int::<u8>;
        Ok(PitchingLine{
            pitcher_id: arr[2].to_string(),
            side: Side::from_str(arr[3])?,
            nth_pitcher: p(arr[4]).context("Invalid fielding sequence position")?,
            pitching_stats: PitchingLineStats::try_from(array_ref![arr,5,17])?,
        })
    }
}
#[derive(Debug)]
pub struct TeamMiscellaneousLine {
    side: Side,
    left_on_base: u8,
    team_earned_runs: Option<u8>,
    double_plays_turned: Option<u8>,
    triple_plays_turned: u8
}

#[derive(Debug)]
pub struct TeamBattingLine {
    side: Side,
    batting_stats: BattingLineStats
}

impl FromRetrosheetRecord for TeamBattingLine {
    fn new(record: &RetrosheetEventRecord) -> Result<TeamBattingLine> {
        let arr = record.deserialize::<[&str; 20]>(None)?;
        Ok(TeamBattingLine {
            side: Side::from_str(arr[2])?,
            batting_stats: BattingLineStats::try_from(array_ref![arr,3,17])?
        })
    }
}

#[derive(Debug)]
pub struct TeamDefenseLine {
    side: Side,
    defensive_stats: DefenseLineStats
}

impl FromRetrosheetRecord for TeamDefenseLine {
    fn new(record: &RetrosheetEventRecord) -> Result<TeamDefenseLine> {
        let mut arr = record.deserialize::<[&str; 10]>(None)?;
        Ok(TeamDefenseLine {
            side: Side::from_str(arr[2])?,
            defensive_stats: DefenseLineStats::try_from(array_ref![arr,3, 7])?
        })
    }
}


impl FromRetrosheetRecord for TeamMiscellaneousLine {
    fn new(record: &RetrosheetEventRecord) -> Result<TeamMiscellaneousLine>{
        let arr = record.deserialize::<[&str; 7]>(None)?;
        let o = {|i: usize| arr[i].parse::<u8>().ok()};
        let u = {|i: usize|
            arr[i].parse::<u8>().context("Bad value for team line stat")
        };
        Ok(TeamMiscellaneousLine {
            side: Side::from_str(arr[2])?,
            left_on_base: u(3)?,
            team_earned_runs: o(4),
            double_plays_turned: o(5),
            triple_plays_turned: u(6)?
        })
    }
}

#[derive(Debug)]
pub enum BoxScoreLine {
    BattingLine(BattingLine),
    PinchHittingLine(PinchHittingLine),
    PinchRunningLine(PinchRunningLine),
    PitchingLine(PitchingLine),
    DefenseLine(DefenseLine),
    TeamMiscellaneousLine(Option<TeamMiscellaneousLine>),
    TeamBattingLine(TeamBattingLine),
    TeamDefenseLine(TeamDefenseLine),
    Unrecognized
}
impl FromRetrosheetRecord for BoxScoreLine {
    fn new(record: &RetrosheetEventRecord) -> Result<BoxScoreLine>{
        let stat_line_type = record.get(1).context("No stat line type")?;
        let mapped= match stat_line_type {
            "bline" => BoxScoreLine::BattingLine(BattingLine::new(&record)?),
            "phline" => BoxScoreLine::PinchHittingLine(PinchHittingLine::new(&record)?),
            "prline" => BoxScoreLine::PinchRunningLine(PinchRunningLine::new(&record)?),
            "pline" => BoxScoreLine::PitchingLine(PitchingLine::new(&record)?),
            "dline" => BoxScoreLine::DefenseLine(DefenseLine::new(&record)?),
            "tline" => BoxScoreLine::TeamMiscellaneousLine(TeamMiscellaneousLine::new(&record).ok()),
            "btline" => BoxScoreLine::TeamBattingLine(TeamBattingLine::new(&record)?),
            "dtline" => BoxScoreLine::TeamDefenseLine(TeamDefenseLine::new(&record)?),
            _ => BoxScoreLine::Unrecognized
        };
        match mapped {
            BoxScoreLine::Unrecognized => Err(Self::error("Unrecognized box score line type", record)),
            _ => Ok(mapped)
        }
    }
}

#[derive(Debug)]
pub struct LineScore {
    side: Side,
    line_score: SmallVec<[u8; 9]>
}

impl FromRetrosheetRecord for LineScore {
    fn new(record: &RetrosheetEventRecord) -> Result<LineScore>{
        let mut iter = record.iter();
        Ok(LineScore{
            side: Side::from_str(iter.nth(1).context("Missing team side")?)?,
            line_score: {
                let mut vec = SmallVec::with_capacity(9);
                for s in iter {vec.push(s.parse::<u8>()?)}
            vec
            }
        })
    }
}

#[derive(Debug)]
pub struct FieldingPlayLine {
    defense_side: Side,
    fielders: SmallVec<[Fielder; 3]>
}
pub type DoublePlayLine = FieldingPlayLine;
pub type TriplePlayLine = FieldingPlayLine;

impl FromRetrosheetRecord for FieldingPlayLine {
    fn new(record: &RetrosheetEventRecord) -> Result<FieldingPlayLine>{
        let mut iter = record.iter();
        Ok(FieldingPlayLine{
            defense_side: Side::from_str(iter.nth(2).context("Missing team side")?)?,
            fielders: iter.map(String::from).collect()
        })
    }
}


#[derive(Debug)]
pub struct HitByPitchLine {
    pitching_side: Side,
    pitcher_id: Pitcher,
    batter_id: Batter
}

impl FromRetrosheetRecord for HitByPitchLine {
    fn new(record: &RetrosheetEventRecord) -> Result<HitByPitchLine>{
        let arr = record.deserialize::<[&str; 5]>(None)?;
        Ok(HitByPitchLine{
            pitching_side: Side::from_str(arr[2])?,
            pitcher_id: arr[3].to_string(),
            batter_id: arr[4].to_string()
        })
    }
}


#[derive(Debug)]
pub struct HomeRunLine {
    batting_side: Side,
    batter_id: Batter,
    pitcher_id: Pitcher,
    inning: Option<Inning>,
    runners_on: Option<u8>,
    outs: Option<u8>
}

impl FromRetrosheetRecord for HomeRunLine {
    fn new(record: &RetrosheetEventRecord) -> Result<HomeRunLine>{
        let arr = record.deserialize::<[&str; 8]>(None)?;
        let p = {|i: usize| arr[i].parse::<u8>().ok()};
        Ok(HomeRunLine{
            batting_side: Side::from_str(arr[2])?,
            batter_id: arr[3].to_string(),
            pitcher_id: arr[4].to_string(),
            inning: p(5),
            runners_on: p(6),
            outs: p(7)
        })
    }
}

#[derive(Debug)]
pub struct StolenBaseAttemptLine {
    running_side: Side,
    runner_id: Baserunner,
    pitcher_id: Pitcher,
    catcher_id: Fielder,
    inning: Option<Inning>
}
pub type StolenBaseLine = StolenBaseAttemptLine;
pub type CaughtStealingLine = StolenBaseAttemptLine;

impl FromRetrosheetRecord for StolenBaseAttemptLine {
    fn new(record: &RetrosheetEventRecord) -> Result<StolenBaseAttemptLine>{
        let arr = record.deserialize::<[&str; 7]>(None)?;
        Ok(StolenBaseAttemptLine{
            running_side: Side::from_str(arr[2])?,
            runner_id: arr[3].to_string(),
            pitcher_id: arr[4].to_string(),
            catcher_id: arr[5].to_string(),
            inning: arr[6].parse::<u8>().ok()

        })
    }
}

#[derive(Debug)]
pub enum BoxScoreEvent {
    DoublePlay(DoublePlayLine),
    TriplePlay(TriplePlayLine),
    HitByPitch(HitByPitchLine),
    HomeRun(HomeRunLine),
    StolenBase(StolenBaseLine),
    CaughtStealing(CaughtStealingLine),
    Unrecognized
}
impl FromRetrosheetRecord for BoxScoreEvent {
    fn new(record: &RetrosheetEventRecord) -> Result<BoxScoreEvent>{
        let event_line_type = record.get(1).context("No event type")?;
        let mapped = match event_line_type {
            "dpline" => BoxScoreEvent::DoublePlay(DoublePlayLine::new(&record)?),
            "tpline" => BoxScoreEvent::TriplePlay(TriplePlayLine::new(&record)?),
            "hpline" => BoxScoreEvent::HitByPitch(HitByPitchLine::new(&record)?),
            "hrline" => BoxScoreEvent::HomeRun(HomeRunLine::new(&record)?),
            "sbline" => BoxScoreEvent::StolenBase(StolenBaseLine::new(&record)?),
            "csline" => BoxScoreEvent::CaughtStealing(CaughtStealingLine::new(&record)?),
            _ => BoxScoreEvent::Unrecognized,

        };
        match mapped {
            BoxScoreEvent::Unrecognized => Err(anyhow!("Unrecognized box score event type {}", event_line_type)),
            _ => Ok(mapped)
        }
    }
}



#[derive(Debug)]
pub enum MappedRecord {
    GameId(GameId),
    Version,
    Info(InfoRecord),
    Start(StartRecord),
    Substitution(SubstitutionRecord),
    Play(PlayRecord),
    BatHandAdjustment(BatHandAdjustment),
    PitchHandAdjustment(PitchHandAdjustment),
    LineupAdjustment(LineupAdjustment),
    EarnedRun(EarnedRunRecord),
    Comment(Comment),
    BoxScoreLine(BoxScoreLine),
    LineScore(LineScore),
    BoxScoreEvent(BoxScoreEvent),
    Unrecognized
}

impl FromRetrosheetRecord for MappedRecord {
    fn new(record: &RetrosheetEventRecord) -> Result<MappedRecord>{
        let line_type = record.get(0).context("No record")?;
        let mapped= match line_type {
            "id" | "7d" => MappedRecord::GameId(GameId::new(record)?),
            "version" => MappedRecord::Version,
            "info" => MappedRecord::Info(InfoRecord::new(record)?),
            "start" => MappedRecord::Start(StartRecord::new(record)?),
            "sub" => MappedRecord::Substitution(SubstitutionRecord::new(record)?),
            "play" => MappedRecord::Play(PlayRecord::new(record)?),
            "badj" => MappedRecord::BatHandAdjustment(BatHandAdjustment::new(record)?),
            "padj" => MappedRecord::PitchHandAdjustment(PitchHandAdjustment::new(record)?),
            "ladj" => MappedRecord::LineupAdjustment(LineupAdjustment::new(record)?),
            "com" => MappedRecord::Comment(String::from(record.get(1).unwrap())),
            "data" => MappedRecord::EarnedRun(EarnedRunRecord::new(record)?),
            "stat" => MappedRecord::BoxScoreLine(BoxScoreLine::new(record)?),
            "line" => MappedRecord::LineScore(LineScore::new(record)?),
            "event" => MappedRecord::BoxScoreEvent(BoxScoreEvent::new(record)?),
            _ => MappedRecord::Unrecognized
        };
        match mapped {
            MappedRecord::Unrecognized => Err(Self::error("Unrecognized record type", record)),
            _ => Ok(mapped)
        }
    }
}
use std::cmp::min;
use std::ops::Deref;
use std::str::FromStr;

use anyhow::{anyhow, Context, Error, Result};
use const_format::{concatcp as concat, formatcp as format};
use lazy_static::lazy_static;
use regex::{Captures, Match, Regex};
use serde::export::TryFrom;
use strum::ParseError;
use strum_macros::{EnumDiscriminants, EnumString};

use crate::util::digit_vec;
use crate::event_file::traits::{Inning, Side, Batter, FromRetrosheetRecord, RetrosheetEventRecord};


const NAMING_PREFIX: &str = r"(?P<";
const GROUP_ASSISTS: &str = r">(?:[1-9]?)+)";
const GROUP_ASSISTS1: &str = concat!(NAMING_PREFIX, "a1", GROUP_ASSISTS);
const GROUP_ASSISTS2: &str = concat!(NAMING_PREFIX, "a2", GROUP_ASSISTS);
const GROUP_ASSISTS3: &str = concat!(NAMING_PREFIX, "a3", GROUP_ASSISTS);
const GROUP_PUTOUT: &str = r">[1-9])";
const GROUP_PUTOUT1: &str = concat!(NAMING_PREFIX, "po1", GROUP_PUTOUT);
const GROUP_PUTOUT2: &str = concat!(NAMING_PREFIX, "po2", GROUP_PUTOUT);
const GROUP_PUTOUT3: &str = concat!(NAMING_PREFIX, "po3", GROUP_PUTOUT);
const GROUP_OUT_AT_BASE_PREFIX: &str = r"(?:\((?P<runner";
const GROUP_OUT_AT_BASE_SUFFIX: &str = r">[B123])\))?";
const GROUP_OUT_AT_BASE1: &str = concat!(GROUP_OUT_AT_BASE_PREFIX, "1", GROUP_OUT_AT_BASE_SUFFIX);
const GROUP_OUT_AT_BASE2: &str = concat!(GROUP_OUT_AT_BASE_PREFIX, "2", GROUP_OUT_AT_BASE_SUFFIX);
const GROUP_OUT_AT_BASE3: &str = concat!(GROUP_OUT_AT_BASE_PREFIX, "3", GROUP_OUT_AT_BASE_SUFFIX);

const OUT: &str = &format!(r"^{}{}{}({}{}{})?({}{}{})?$",
                           GROUP_ASSISTS1, GROUP_PUTOUT1, GROUP_OUT_AT_BASE1,
                           GROUP_ASSISTS2, GROUP_PUTOUT2, GROUP_OUT_AT_BASE2,
                           GROUP_ASSISTS3, GROUP_PUTOUT3, GROUP_OUT_AT_BASE3
);

const REACH_ON_ERROR: &str = &format!(r"{}E(?P<e>[1-9])$", GROUP_ASSISTS1);
const BASERUNNING_PLAY: &str = r"^(?P<play_type>SB|CS|PO|POCS)(?P<base>[123H])(?:\((?P<fielders>[0-9]*)(?P<error>E[0-9])?\)?)?(?P<unearned_run>\(T?UR\))?$";


const ADVANCE: &str = r"^(?P<from>[B123])(?:(-(?P<to>[123H])|X(?P<out_at>[123H])))(?P<mods>.*)?";

lazy_static!{
    static ref OUT_REGEX: Regex = Regex::new(OUT).unwrap();
    static ref REACHED_ON_ERROR_REGEX: Regex = Regex::new(REACH_ON_ERROR).unwrap();
    static ref BASERUNNING_PLAY_REGEX: Regex = Regex::new(BASERUNNING_PLAY).unwrap();

    static ref ADVANCE_REGEX: Regex = Regex::new(ADVANCE).unwrap();
    static ref STRIP_CHARS_REGEX: Regex = Regex::new(r"([#!0? ]|99)").unwrap();
    static ref MULTI_PLAY_REGEX: Regex = Regex::new(r"[+;]").unwrap();
    static ref NUMERIC_REGEX: Regex = Regex::new(r"[0-9]").unwrap();
    static ref MODIFIER_DIVIDER_REGEX: Regex = Regex::new(r"[+\-0-9]").unwrap();
}

#[derive(Debug, Eq, PartialEq, EnumString, Copy, Clone)]
enum Base {
    #[strum(serialize = "1")]
    First = 1,
    #[strum(serialize = "2")]
    Second,
    #[strum(serialize = "3")]
    Third,
    #[strum(serialize = "H")]
    Home
}

#[derive(Debug, Eq, PartialEq, EnumString, Copy, Clone)]
enum BaseRunner {
    #[strum(serialize = "B")]
    Batter,
    #[strum(serialize = "1")]
    First,
    #[strum(serialize = "2")]
    Second,
    #[strum(serialize = "3")]
    Third
}

#[derive(Debug, Eq, PartialEq, EnumString, Copy, Clone)]
enum PitchType {
    #[strum(serialize = "1")]
    PickoffAttemptFirst,
    #[strum(serialize = "2")]
    PickoffAttemptSecond,
    #[strum(serialize = "3")]
    PickoffAttemptThird,
    #[strum(serialize = ".")]
    PlayNotInvolvingBatter,
    #[strum(serialize = "B")]
    Ball,
    #[strum(serialize = "C")]
    CalledStrike,
    #[strum(serialize = "F")]
    Foul,
    #[strum(serialize = "H")]
    HitBatter,
    #[strum(serialize = "I")]
    IntentionalBall,
    #[strum(serialize = "K")]
    StrikeUnknownType,
    #[strum(serialize = "L")]
    FoulBunt,
    #[strum(serialize = "M")]
    MissedBunt,
    #[strum(serialize = "N")]
    NoPitch,
    #[strum(serialize = "O")]
    FoulTipBunt,
    #[strum(serialize = "P")]
    Pitchout,
    #[strum(serialize = "Q")]
    SwingingOnPitchout,
    #[strum(serialize = "R")]
    FoulOnPitchout,
    #[strum(serialize = "S")]
    SwingingStrike,
    #[strum(serialize = "T")]
    FoulTip,
    #[strum(serialize = "U")]
    Unknown,
    #[strum(serialize = "V")]
    BallOnPitcherGoingToMouth,
    #[strum(serialize = "X")]
    InPlay,
    #[strum(serialize = "Y")]
    InPlayOnPitchout
}
impl Default for PitchType {
    fn default() -> Self { PitchType::Unknown }
}

#[derive(Debug, Default, Copy, Clone)]
pub struct Pitch {
    pitch_type: PitchType,
    runners_going: bool,
    blocked_by_catcher: bool,
    catcher_pickoff_attempt: Option<Base>
}
impl Pitch {
    fn update_pitch_type(&mut self, pitch_type: PitchType) {
        self.pitch_type = pitch_type
    }
    fn update_catcher_pickoff(&mut self, base: Option<Base>) {
        self.catcher_pickoff_attempt = base
    }
    fn update_blocked_by_catcher(&mut self) {
        self.blocked_by_catcher = true
    }
    fn update_runners_going(&mut self) {
        self.runners_going = true
    }
}

#[derive(Debug, Default)]
pub struct PitchSequence(Vec<Pitch>);

impl TryFrom<&str> for PitchSequence {
    type Error = Error;

    fn try_from(str_sequence: &str) -> Result<Self> {
        let mut pitches: Vec<Pitch> = Vec::with_capacity(10);
        let mut char_iter = str_sequence.chars().peekable();
        let mut pitch = Pitch::default();

        let get_catcher_pickoff_base = { |c: Option<char>|
            Base::from_str(&c.unwrap_or('.').to_string()).ok()
        };

        // TODO: Maybe try implementing in nom? Not a priority tho
        loop {
            let opt_c = char_iter.next();
            if opt_c == None {break}
            let c = opt_c.unwrap().to_string();
            match c.deref() {
                // Tokens indicating info on the upcoming pitch
                "*" =>  {pitch.update_blocked_by_catcher(); continue}
                ">" => {pitch.update_runners_going(); continue}
                _ => {}
            }
            let pitch_type: Result<PitchType> = PitchType::from_str(c.deref()).context("hi");
            // TODO: Log this as a warning once I implement logging
            pitch_type.map(|p|{pitch.update_pitch_type(p)}).ok();

            match char_iter.peek() {
                // Tokens indicating info on the previous pitch
                Some('>') => {
                    // The sequence ">+" occurs around 70 times in the current data, usually but not always on
                    // a pickoff caught stealing initiated by the catcher. It's unclear what the '>' is for, but
                    // it might be to indicate cases in which the runner attempted to advance on the pickoff rather
                    // than get back to the base. Current approach is to just record the catcher pickoff and
                    // not apply the advance attempt info to the pitch.
                    // TODO: Figure out what's going on here and fix if needed or delete the todo
                    let mut speculative_iter = char_iter.clone();
                    match speculative_iter.nth(1) {
                        Some('+') => {
                            pitch.update_catcher_pickoff(get_catcher_pickoff_base(char_iter.nth(2)))
                        },
                        _ => {}
                    }
                }
                Some('+') => {pitch.update_catcher_pickoff(get_catcher_pickoff_base(char_iter.nth(1)))}
                _ => {}
            }
            let final_pitch = pitch;
            pitch = Pitch::default();
            pitches.push(final_pitch);
        }
        Ok(Self(pitches))
    }
}


#[derive(Debug, Eq, PartialEq, EnumString)]
#[strum(serialize_all = "lowercase")]
enum InningFrame {
    Top,
    Bottom,
}

#[derive(Debug, EnumString)]
enum UnearnedRun {
    #[strum(serialize = "UR")]
    Unearned,
    #[strum(serialize = "TUR")]
    TeamUnearned
}

type Position = u8;

#[derive(Debug)]
enum PlayType {
    Out { assists: Vec<Position>, putouts: Vec<Position>, runners_out: Vec<BaseRunner> },
    Interference,
    Single(Option<HitLocation>),
    Double(Option<HitLocation>),
    Triple(Option<HitLocation>),
    GroundRuleDouble(Option<HitLocation>),
    ErrorOnFoul(Position),
    ReachedOnError {assists: Vec<Position>, error: Position},
    FieldersChoice(Vec<u8>),
    HomeRun(Option<HitLocation>),
    HitByPitch,
    StrikeOut,
    StrikeOutPutOut(Vec<Position>),
    NoPlay,
    IntentionalWalk,
    Walk,
    Unknown,
    Balk,
    CaughtStealing {base: Base, fielders: Vec<Position>, error:Option<Position>, unearned_run: Option<UnearnedRun>},
    DefensiveIndifference,
    OtherAdvance,
    PassedBall,
    WildPitch,
    PickedOff {base: Base, fielders: Vec<Position>, error:Option<Position>, unearned_run: Option<UnearnedRun>},
    PickedOffCaughtStealing {base: Base, fielders: Vec<Position>, error:Option<Position>, unearned_run: Option<UnearnedRun>},
    StolenBase {base: Base, unearned_run: Option<UnearnedRun>},
    Unrecognized(String)
}

impl PlayType {
    fn parse_fielding_play(value: &str) -> Result<PlayType> {
        match value.parse::<u32>() {
            Ok(_) => {
                let mut digits = digit_vec(value);
                let (putouts, assists) = (vec![digits.pop().unwrap()], digits);
                return Ok(PlayType::Out { assists, putouts, runners_out: vec![]})
            }
            _ => ()
        };
        let to_str_vec: fn(Vec<Option<Match>>) -> Vec<&str> = { |v| v
            .into_iter()
            .filter_map(|o| o
                .map(|m| m.as_str()))
            .collect()
        };
        let out_captures = OUT_REGEX.captures(value);
        if out_captures.is_some() {
            let captures = out_captures.unwrap();
            let assist_matches = vec![captures.name("a1"), captures.name("a2"), captures.name("a3")];
            let putout_matches = vec![captures.name("po1"), captures.name("po2"), captures.name("po3")];
            let runner_matches = vec![captures.name("runner1"), captures.name("runner2"), captures.name("runner3")];
            let (assists, putouts) = (digit_vec(&to_str_vec(assist_matches).join("")), digit_vec(&to_str_vec(putout_matches).join("")));
            let runners_out = to_str_vec(runner_matches).into_iter().map(|s| BaseRunner::from_str(s)).collect::<Result<Vec<BaseRunner>, ParseError>>()?;
            Ok(PlayType::Out {assists, putouts, runners_out})
        }
        else {
            let error_captures = REACHED_ON_ERROR_REGEX.captures(value);
            if error_captures.is_some() {
                let captures = error_captures.unwrap();
                let assists = digit_vec(captures.name("a1").map(|s| s.as_str()).unwrap_or_default());
                let error = captures.name("e").map(|s| s.as_str().parse::<u8>()).context("No fielder specified on error play")??;
                return Ok(PlayType::ReachedOnError {assists, error})
            }
            Err(anyhow!("Unable to parse fielding play"))
        }
    }
    fn parse_baserunning_play(value: &str) -> Result<PlayType> {
        let captures = BASERUNNING_PLAY_REGEX
            .captures(value)
            .context("No matching info in baserunning detail")?;
        let (play_type, base, fielders, error, unearned_run) = (
            captures.name("play_type").map(|m| m.as_str()).context("No baserunning play type found")?,
            Base::from_str(captures.name("base").map(|m| m.as_str()).unwrap_or_default())?,
            captures.name("fielders").map(|m| digit_vec(m.as_str())).unwrap_or_default(),
            captures.name("error").map(|m| digit_vec(m.as_str()).first().copied()).unwrap_or_default(),
            captures.name("unearned_run").map(|s|
                if s.as_str().contains("T") {UnearnedRun::TeamUnearned} else {UnearnedRun::Unearned})
        );
        match play_type.into() {
            "CS" => Ok(PlayType::CaughtStealing { base, fielders, error, unearned_run }),
            "PO" => Ok(PlayType::PickedOff { base, fielders, error, unearned_run }),
            "POCS" => Ok(PlayType::PickedOffCaughtStealing { base, fielders, error, unearned_run }),
            "SB" => Ok(PlayType::StolenBase { base, unearned_run }),
            "POCSH" => Ok(PlayType::PickedOffCaughtStealing { base: Base::Home, fielders, error, unearned_run }),
            "CSH" => Ok(PlayType::CaughtStealing { base: Base::Home, fielders, error, unearned_run }),
            _ => Err(anyhow!("Unrecognized baserunning play type"))
        }

    }
    fn parse_main_play(value: &str) -> Result<Vec<PlayType>> {
        if value == "" {return Ok(vec![])}
        let multi_split = MULTI_PLAY_REGEX.find(value);
        if multi_split != None {
            let (first, last) = value.split_at(multi_split.unwrap().start());
            return Ok(Self::parse_main_play(first)?
                .into_iter()
                .chain(Self::parse_main_play(&last[1..])?.into_iter())
                .collect())
        }
        let play_type = match value {
            "99" => PlayType::Unknown,
            "C" => PlayType::Interference,
            "HP" => PlayType::HitByPitch,
            "K" => PlayType::StrikeOut,
            "I" | "IW" => PlayType::IntentionalWalk,
            "NP" => PlayType::NoPlay,
            "W" => PlayType::Walk,
            "BK" => PlayType::Balk,
            "DI" => PlayType::DefensiveIndifference,
            "OA" => PlayType::OtherAdvance,
            "PB" => PlayType::PassedBall,
            "WP" => PlayType::WildPitch,
            _ => PlayType::Unrecognized(value.to_string())
        };
        match play_type {PlayType::Unrecognized(_) => (), _ => {return Ok(vec![play_type])}}

        if BASERUNNING_PLAY_REGEX.is_match(value) {return Ok(vec![Self::parse_baserunning_play(value)?])}

        let num_split = if NUMERIC_REGEX.is_match(value) {NUMERIC_REGEX.find(value).unwrap().start()} else {value.len()};
        let (first, last) = value.split_at(num_split);
        let last = match last {"" => None, _ => Some(last.to_string())};
        let last_as_int_vec: Vec<u8> = (&last).as_ref().map(|c| digit_vec(&c)).unwrap_or_default();
        let final_match = match first {
            "E" => PlayType::ReachedOnError {assists: vec![], error: last_as_int_vec.first().map(|u| *u).context("No fielder specified")?},
            "" => Self::parse_fielding_play(&last.clone().unwrap())?,
            "S" => PlayType::Single(last),
            "D" => PlayType::Double(last),
            "T" => PlayType::Triple(last),
            "H" | "HR" => PlayType::HomeRun(last),
            "DGR" => PlayType::GroundRuleDouble(last),
            "FC" => PlayType::FieldersChoice(last_as_int_vec),
            "FLE" => PlayType::ErrorOnFoul(last_as_int_vec.first().map(|u| *u).context("No fielder specified")?),
            "K" => PlayType::StrikeOutPutOut(last_as_int_vec),
            // Special case where fielders are unknown but base of forceout is
            "(" => PlayType::Out { assists: vec![], putouts: vec![], runners_out: vec![BaseRunner::from_str(last.unwrap_or_default().get(0..1).unwrap_or_default())?]},
            _ => PlayType::Unrecognized(value.to_string())
        };
        Ok(vec![final_match])
    }
}


#[derive(Debug)]
struct RunnerAdvance {
    baserunner: BaseRunner,
    to: Base,
    out_or_error: bool,
    modifiers: Vec<RunnerAdvanceModifier>
}
impl RunnerAdvance {
    fn parse_advances(value: &str) -> Result<Vec<RunnerAdvance>> {
        value
            .split(";")
            .filter_map(|s|ADVANCE_REGEX.captures(s))
            .map(|c| Self::parse_single_advance(c))
            .collect::<Result<Vec<RunnerAdvance>>>()
    }

    fn parse_single_advance(captures: Captures) -> Result<RunnerAdvance> {
        let (from_match, to_match, out_at_match, mods) = (
            captures.name("from"), captures.name("to"), captures.name("out_at"), captures.name("mods")
        );
        let baserunner = BaseRunner::from_str(from_match
            .map(|s| s.as_str())
            .context("Missing baserunner in advance")?)?;
        let to = Base::from_str(to_match.or(out_at_match)
            .map(|s| s.as_str())
            .context("Missing destination base in advance")?)?;
        let out_or_error = out_at_match.is_some();
        let modifiers = mods.map_or(Ok(Vec::new()), |m| RunnerAdvanceModifier::parse_advance_modifiers(m.as_str()))?;
        return Ok(RunnerAdvance {baserunner, to, out_or_error, modifiers})

    }
}

#[derive(Debug)]
enum RunnerAdvanceModifier {
    UnearnedRun,
    TeamUnearnedRun,
    NoRBI,
    Interference(Option<Position>),
    RBI,
    PassedBall,
    WildPitch,
    AdvancedOnThrowTo(Option<Base>),
    AdvancedOnError {assists: Vec<Position>, error: Option<Position>},
    Putout{assists: Vec<Position>, putout: Option<Position>},
    Unrecognized(String)
}
impl RunnerAdvanceModifier {
    fn parse_advance_modifiers(value: &str) -> Result<Vec<RunnerAdvanceModifier>> {
        value
            .split(")")
            .filter(|s| !s.is_empty())
            .map(|s| Self::parse_single_advance_modifier(s))
            .collect::<Result<Vec<RunnerAdvanceModifier>>>()
    }

    fn parse_single_advance_modifier(value: &str) -> Result<RunnerAdvanceModifier> {
        let simple_match = match value {
            "(UR" => RunnerAdvanceModifier::UnearnedRun,
            "(TUR" => RunnerAdvanceModifier::TeamUnearnedRun,
            "(NR" | "(NORBI" => RunnerAdvanceModifier::NoRBI,
            "(RBI" => RunnerAdvanceModifier::RBI,
            "(PB" => RunnerAdvanceModifier::PassedBall,
            "(WP" => RunnerAdvanceModifier::WildPitch,
            "(THH" => RunnerAdvanceModifier::AdvancedOnThrowTo(Some(Base::Home)),
            "(TH" => RunnerAdvanceModifier::AdvancedOnThrowTo(None),
            "(" => RunnerAdvanceModifier::Putout { assists: vec![], putout: None },
            _ => RunnerAdvanceModifier::Unrecognized(value.into())
        };
        match simple_match {
            RunnerAdvanceModifier::Unrecognized(_) => (),
            _ => { return Ok(simple_match) }
        };
        let num_split = if NUMERIC_REGEX.is_match(value) {
            NUMERIC_REGEX.find(value).unwrap().start()
        } else {
            return Err(anyhow!("Malformed baserunner advance modifier"))
        };
        let (first, last) = value.split_at(num_split);
        let last_as_int_vec: Vec<u8> = digit_vec(last.into());
        let final_match = match first {
            "(INT" => RunnerAdvanceModifier::Interference(last_as_int_vec.first().copied()),
            "(TH" => RunnerAdvanceModifier::AdvancedOnThrowTo(Base::from_str(last).ok()),
            "(E" => RunnerAdvanceModifier::AdvancedOnError { assists: Vec::new(), error: last.get(0..1).map_or(None, |s| s.parse::<u8>().ok()) },
            "(" if last.contains("E") => {
                let (assist_str, error_str) = last.split_at(last.find("E").unwrap());
                let (assists, error) = (digit_vec(assist_str), digit_vec(error_str).first().copied());
                RunnerAdvanceModifier::AdvancedOnError { assists, error }
            },
            "(" => {
                let mut digits = digit_vec(last);
                let (putout, assists) = (digits.pop(), digits);
                RunnerAdvanceModifier::Putout { assists, putout }
            }
            _ => RunnerAdvanceModifier::Unrecognized(value.to_string())
        };
        Ok(final_match)
    }
}

type HitLocation = String;

#[derive(Debug, EnumDiscriminants)]
#[strum_discriminants(derive(EnumString))]
enum PlayModifier {
    HitLocation(HitLocation),
    AppealPlay,
    UnspecifiedBunt(Option<HitLocation>),
    PopUpBunt(Option<HitLocation>),
    GroundBallBunt(Option<HitLocation>),
    FoulBunt(Option<HitLocation>),
    BuntGroundIntoDoublePlay,
    BatterInterference,
    LineDriveBunt(Option<HitLocation>),
    BatingOutOfTurn,
    BuntPoppedIntoDoublePlay,
    RunnerHitByBattedBall,
    CalledThirdStrike,
    CourtesyBatter,
    CourtesyFielder,
    CourtesyRunner,
    UnspecifiedDoublePlay,
    ErrorOn(Position),
    Fly(Option<HitLocation>),
    FlyBallDoublePlay,
    FanInterference,
    Foul,
    ForceOut,
    GroundBall(Option<HitLocation>),
    GroundBallDoublePlay,
    GroundBallTriplePlay,
    InfieldFlyRule,
    Interference,
    InsideTheParkHomeRun,
    LineDrive(Option<HitLocation>),
    LinedIntoDoublePlay,
    LinedIntoTriplePlay,
    ManageChallengeOfCallOnField,
    NoDoublePlayCredited,
    Obstruction,
    PopFly(Option<HitLocation>),
    RunnerOutPassingAnotherRunner,
    RelayToFielderWithNoOutMade(Vec<Position>),
    RunnerInterference,
    SwingingThirdStrike,
    SacrificeFly,
    SacrificeHit,
    ThrowToBase(Option<Base>),
    UnspecifiedTriplePlay,
    UmpireInterference,
    UmpireReviewOfCallOnField,
    Unknown(Option<HitLocation>),
    Unrecognized(String)
}

impl PlayModifier {
    fn parse_modifiers(value: &str) -> Result<Vec<PlayModifier>> {
        value
            .split("/")
            .filter(|s| s.len() > 0)
            .map(|s| Self::parse_single_modifier(s))
            .collect::<Result<Vec<PlayModifier>>>()
    }

    fn parse_single_modifier(value: &str) -> Result<PlayModifier> {
        let num_split = if MODIFIER_DIVIDER_REGEX.is_match(value) { MODIFIER_DIVIDER_REGEX.find(value).unwrap().start() } else { value.len() };
        let (first, last) = value.split_at(num_split);
        let last = match last {
            "" => None,
            _ => Some(last.to_string())
        };
        let last_as_int_vec: Vec<u8> = digit_vec(&last.clone().unwrap_or_default());
        let final_match = match first {
            "" => PlayModifier::HitLocation(last.context("No play modifier info")?),
            "AP" => PlayModifier::AppealPlay,
            "B" => PlayModifier::UnspecifiedBunt(last),
            "BF" => PlayModifier::FoulBunt(last),
            "BP" => PlayModifier::PopUpBunt(last),
            "BG" => PlayModifier::GroundBallBunt(last),
            "BGDP" => PlayModifier::BuntGroundIntoDoublePlay,
            "BINT" => PlayModifier::BatterInterference,
            "BL" => PlayModifier::LineDriveBunt(last),
            "BOOT" => PlayModifier::BatingOutOfTurn,
            "BPDP" => PlayModifier::BuntPoppedIntoDoublePlay,
            "BR" => PlayModifier::RunnerHitByBattedBall,
            "C" => PlayModifier::CalledThirdStrike,
            "COUB" => PlayModifier::CourtesyBatter,
            "COUF" => PlayModifier::CourtesyFielder,
            "COUR" => PlayModifier::CourtesyRunner,
            "DP" => PlayModifier::UnspecifiedDoublePlay,
            "E" => PlayModifier::ErrorOn(*last_as_int_vec.first().context("Missing error position info")?),
            "F" => PlayModifier::Fly(last),
            "FDP" => PlayModifier::FlyBallDoublePlay,
            "FINT" => PlayModifier::FanInterference,
            "FL" => PlayModifier::Foul,
            "FO" => PlayModifier::ForceOut,
            "G" => PlayModifier::GroundBall(last),
            "GDP" => PlayModifier::GroundBallDoublePlay,
            "GTP" => PlayModifier::GroundBallTriplePlay,
            "IF" => PlayModifier::InfieldFlyRule,
            "INT" => PlayModifier::Interference,
            "IPHR" => PlayModifier::InsideTheParkHomeRun,
            "L" => PlayModifier::LineDrive(last),
            "LDP" => PlayModifier::LinedIntoDoublePlay,
            "LTP" => PlayModifier::LinedIntoTriplePlay,
            "MREV" => PlayModifier::ManageChallengeOfCallOnField,
            "NDP" => PlayModifier::NoDoublePlayCredited,
            "OBS" => PlayModifier::Obstruction,
            "P" => PlayModifier::PopFly(last),
            "PASS" => PlayModifier::RunnerOutPassingAnotherRunner,
            "R" => PlayModifier::RelayToFielderWithNoOutMade(last_as_int_vec),
            "RINT" => PlayModifier::RunnerInterference,
            "S" => PlayModifier::SwingingThirdStrike,
            "SF" => PlayModifier::SacrificeFly,
            "SH" => PlayModifier::SacrificeHit,
            "TH" | "TH)" => PlayModifier::ThrowToBase(Base::from_str(&last.unwrap_or_default()).ok()),
            "THH" => PlayModifier::ThrowToBase(Some(Base::Home)),
            "TP" => PlayModifier::UnspecifiedTriplePlay,
            "UINT" => PlayModifier::UmpireInterference,
            "UREV" => PlayModifier::UmpireReviewOfCallOnField,
            "U" => PlayModifier::Unknown(last),
            _ => PlayModifier::Unrecognized(value.into())
        };
        Ok(final_match)
    }
}

#[derive(Debug, Default)]
pub struct Play {
    main_plays: Vec<PlayType>,
    modifiers: Vec<PlayModifier>,
    advances: Vec<RunnerAdvance>,
    uncertain_flag: bool,
    exceptional_flag: bool
}



impl Play {
    fn unknown_play() -> Play {
        Play {
            main_plays: vec![PlayType::Unknown],
            modifiers: vec![],
            advances: vec![],
            uncertain_flag: false,
            exceptional_flag: false
        }
    }
}

impl TryFrom<&str> for Play {
    type Error = Error;

    fn try_from(value: &str) -> Result<Self> {
        let (uncertain_flag, exceptional_flag) = (value.contains("#"), value.contains("!"));
        let value: &str = &STRIP_CHARS_REGEX.replace_all(value, "").to_string();
        if value == "" {return Ok(Play::unknown_play())}
        let modifiers_boundary = value.find("/").unwrap_or(value.len());
        let advances_boundary = value.find(".").unwrap_or(value.len());
        let first_boundary = min(modifiers_boundary, advances_boundary);

        let main_plays = PlayType::parse_main_play(&value[..first_boundary])?;

        let modifiers = if modifiers_boundary < advances_boundary {
            PlayModifier::parse_modifiers(&value[modifiers_boundary+1..advances_boundary])?
        } else {Vec::new()};

        let advances = if advances_boundary < value.len() - 1 {
            RunnerAdvance::parse_advances(&value[advances_boundary+1..])?
        } else {Vec::new()};
        Ok(Play {
            main_plays,
            modifiers,
            advances,
            uncertain_flag,
            exceptional_flag
        })
    }
}

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
    pub pitch_sequence: Option<PitchSequence>,
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
            pitch_sequence: {match record[5] {"" => None, s => Some(PitchSequence::try_from(s)?)}},
            play: Play::try_from(record[6])?
        })
    }
}


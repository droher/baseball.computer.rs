use std::ops::Deref;
use std::str::FromStr;

use anyhow::{anyhow, Context, Error, Result};
use either::{Either, Left};
use lazy_static::lazy_static;
use num_traits::cast::FromPrimitive;
use regex::{Regex, RegexSet, SetMatches};
use serde::export::TryFrom;
use smallvec::SmallVec;
use strum_macros::{EnumDiscriminants, EnumString};

use crate::event_file_entities::{Fielder, Pitcher, Player, PlayRecord};
use std::collections::hash_map::VacantEntry;
use std::cmp::min;

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

pub fn pitch_sequence(str_sequence: &str) -> Result<Vec<Pitch>> {
    let mut pitches: Vec<Pitch> = Vec::with_capacity(10);
    let mut char_iter = str_sequence.chars().peekable();
    let mut pitch = Pitch::default();

    let get_catcher_pickoff_base = { |c: Option<char>|
        Base::from_str(c.unwrap_or('.').to_string().deref()).ok()
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
    Ok(pitches)
}

#[derive(Debug, Eq, PartialEq, EnumString)]
#[strum(serialize_all = "lowercase")]
enum InningFrame {
    Top,
    Bottom,
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
    Walk(Option<RunnerPlay>),
    Unknown
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

#[derive(Debug)]
pub enum Position {
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

const STRIP_CHARS: &str = r"[#!0?\- ]";
const UNKNOWN: &str = r"^99$";
// I'm sorry
const OUT: &str = r"^([1-9]+?)(E?[1-9])?(\([B123]\))?(?:([1-9]+?)([1-9])?(\([B123]\))?)?(?:([1-9]+?)([1-9])?(\([B123]\))?)?$";
const INTERFERENCE: &str = r"^C$";
const SINGLE: &str = r"^S([1-9])*$";
const DOUBLE: &str = r"^D([1-9])*$";
const TRIPLE: &str = r"^T([1-9])*$";
const HOME_RUN: &str = r"^(H|HR)([1-9])?";
const GROUND_RULE_DOUBLE: &str = r"^DGR([1-9])*$";
const REACH_ON_ERROR: &str = r"^E([1-9])$";
const FIELDERS_CHOICE: &str = r"^FC([1-9])?$";
const ERROR_ON_FOUL: &str = r"^FLE([1-9])$";
const HIT_BY_PITCH: &str = r"^HP$";
const STRIKEOUT: &str = r"^K$";
const STRIKEOUT_PUTOUT: &str = r"^K[1-9]+$";
const NO_PLAY: &str = r"^NP$";
const INTENTIONAL_WALK: &str = r"^(I|IW)$";
const WALK: &str = r"^W$";
const MULTI_PLAY: &str = r"^(.+)\+(.+)$";
const BALK: &str = r"^BK$";
const DEFENSIVE_INDIFFERENCE: &str = r"^DI$";
const OTHER_ADVANCE: &str = r"^OA$";
const PASSED_BALL: &str = r"^PB$";
const WILD_PITCH: &str = r"^WP$";
const CAUGHT_STEALING: &str = r"^CS([23H])(?:\(([0-9]*)(E[0-9])?\)?)?(\(T?UR\))?$";
const PICKED_OFF: &str = r"^PO([123])(?:\(([0-9]*)(E[0-9])?\)?)?$";
const PICKED_OFF_CAUGHT_STEALING: &str = r"^POCS([23H])(?:\(([0-9]*)(E[0-9])?\)?)?(\(T?UR\))?$";
const STOLEN_BASE: &str = r"^SB([23H])(\(T?UR\))?$";
const MULTI_BASE_PLAY: &str = r";";
const PLAY_REGEXES: [&str; 28] = [UNKNOWN, OUT, INTERFERENCE, SINGLE, DOUBLE, TRIPLE, HOME_RUN, GROUND_RULE_DOUBLE,
    REACH_ON_ERROR, FIELDERS_CHOICE, ERROR_ON_FOUL, HIT_BY_PITCH, STRIKEOUT, STRIKEOUT_PUTOUT, NO_PLAY, INTENTIONAL_WALK,
    WALK, MULTI_PLAY, BALK, DEFENSIVE_INDIFFERENCE, OTHER_ADVANCE, PASSED_BALL, WILD_PITCH, CAUGHT_STEALING,
    PICKED_OFF, PICKED_OFF_CAUGHT_STEALING, STOLEN_BASE, MULTI_BASE_PLAY];

const HIT_LOCATION: &str = r"^[0-9].*$";
const APPEAL_PLAY: &str = r"^AP$";
const UNSPECIFIED_BUNT: &str = r"^B([0-9].*)?";
const FOUL_BUNT: &str = r"^BF$";
const POP_UP_BUNT: &str = r"^BP([0-9].*)?$";
const GROUND_BALL_BUNT: &str = r"^BG([0-9].*)?$";
const BUNT_GIDP: &str = r"^BGDP([0-9].*)?$";
const BATTER_INTERFERENCE: &str = r"^BINT([0-9].*)?$";
const LINE_DRIVE_BUNT: &str = r"^BL([0-9].*)?$";
const BATTING_OUT_OF_TURN: &str = r"^BOOT$";
const BUNT_POP_UP: &str = r"^BP([0-9].*)?$";
const BUNT_POP_INTO_DP: &str = r"^BPDP([0-9].*)?$";
const RUNNER_HIT_BY_BALL: &str = r"^BR([0-9].*)?$";
const CALLED_THIRD_STRIKE: &str = r"^C$";
const COURTESY_BATTER: &str = r"^COUB$";
const COURTESY_FIELDER: &str = r"^COUF$";
const COURTESY_RUNNER: &str = r"^COUR$";
const UNSPECIFIED_DP: &str = r"^DP$";
const ERROR_ON: &str = r"^E([1-9])$";
const FLY: &str = r"^F\+?([0-9].*)?$";
const FLY_BALL_DP: &str = r"^FDP([0-9].*)?$";
const FAN_INTERFERENCE: &str = r"^FINT([0-9].*)?$";
const FOUL: &str = r"^FL([0-9].*)?$";
const FORCE_OUT: &str = r"^FO([0-9].*)?$";
const GROUND_BALL: &str = r"^G\+?([0-9].*)?$";
const GROUND_BALL_DP: &str = r"^GDP([0-9].*)?$";
const GROUND_BALL_TP: &str = r"^GTP([0-9].*)?$";
const INFIELD_FLY: &str = r"^IF([0-9].*)?$";
const INTERFERENCE_MOD: &str = r"^INT([0-9].*)?$";
const INSIDE_PARK_HR: &str = r"^IPHR([0-9].*)?$";
const LINE_DRIVE: &str = r"^L\+?([0-9].*)?$";
const LINE_DRIVE_DP: &str = r"^LDP([0-9].*)?$";
const LINE_DRIVE_TP: &str = r"^LTP([0-9].*)?$";
const MANAGER_CHALLENGE_CALL: &str = r"^MREV([0-9].*)?$";
const NO_DP_CREDITED: &str = r"^NDP([0-9].*)?$";
const FIELDER_OBSTRUCTING_RUNNER: &str = r"^OBS([0-9].*)?$";
const POP_FLY: &str = r"^P\+?([0-9].*)?$";
const RUNNER_PASSES_ANOTHER_RUNNER: &str = r"^PASS$";
const RELAY_NO_OUT: &str = r"^R([1-9].*)?$";
const RUNNER_INTERFERENCE: &str = r"^RINT([0-9].*)?$";
const SWINGING_THIRD_STRIKE: &str = r"^S$";
const SACRIFICE_FLY: &str = r"^SF([0-9].*)?$";
const SACRIFICE_HIT: &str = r"^SH([0-9].*)?$";
const THROW: &str = r"^TH\)?$";
const THROW_TO_BASE: &str = r"^TH([123H])$";
const TP_UNSPECIFIED: &str = r"^TP([0-9].*)?$";
const UMPIRE_INTERFERENCE: &str = r"^UINT([0-9].*)?$";
const UMPIRE_REVIEW_OF_CALL: &str = r"^UREV([0-9].*)?$";
const UNSPECIFIED_REVIEW: &str = r"^REV$";
const UNKNOWN_MODIFIER: &str = r"^U.*$?";
const MODIFIER_REGEXES: [&str; 50] = [HIT_LOCATION, APPEAL_PLAY, UNSPECIFIED_BUNT, FOUL_BUNT, POP_UP_BUNT, GROUND_BALL_BUNT,
    BUNT_GIDP, BATTER_INTERFERENCE, LINE_DRIVE_BUNT, BATTING_OUT_OF_TURN, BUNT_POP_UP,
    BUNT_POP_INTO_DP, RUNNER_HIT_BY_BALL, CALLED_THIRD_STRIKE, COURTESY_BATTER, COURTESY_FIELDER,
    COURTESY_RUNNER, UNSPECIFIED_DP, ERROR_ON, FLY, FLY_BALL_DP, FAN_INTERFERENCE, FOUL, FORCE_OUT,
    GROUND_BALL, GROUND_BALL_DP, GROUND_BALL_TP, INFIELD_FLY, INTERFERENCE_MOD, INSIDE_PARK_HR,
    LINE_DRIVE, LINE_DRIVE_DP, LINE_DRIVE_TP, MANAGER_CHALLENGE_CALL, NO_DP_CREDITED,
    FIELDER_OBSTRUCTING_RUNNER, POP_FLY, RUNNER_PASSES_ANOTHER_RUNNER, RELAY_NO_OUT,
    RUNNER_INTERFERENCE, SWINGING_THIRD_STRIKE, SACRIFICE_FLY, SACRIFICE_HIT, THROW, THROW_TO_BASE, TP_UNSPECIFIED,
    UMPIRE_INTERFERENCE, UMPIRE_REVIEW_OF_CALL, UNSPECIFIED_REVIEW, UNKNOWN_MODIFIER];


lazy_static!{
    static ref PLAY_REGEX_SET: RegexSet = RegexSet::new(&PLAY_REGEXES).unwrap();
    static ref MODIFIER_REGEX_SET: RegexSet = RegexSet::new(MODIFIER_REGEXES.iter()).unwrap();
    static ref STRIP_CHARS_REGEX: Regex = Regex::new(STRIP_CHARS).unwrap();
}


pub struct Play {
    main_plays: Vec<u8>,
    modifiers: Vec<PlayModifier>,
    advances: Vec<RunnerAdvance>,
    uncertain_flag: bool,
    exceptional_flag: bool
}

impl Play {
    fn parse_main_play(value: &str) -> Result<u8> {
        let m = PLAY_REGEX_SET.matches(value);
        Ok(0)

    }
    fn parse_modifiers(value: &str) -> Result<Vec<PlayModifier>> {
        let x: Vec<SetMatches> = value.split("/").filter(|s| s.len() > 0).map({|m| MODIFIER_REGEX_SET.matches(m)}).collect();
        let y: Vec<()> = x.iter().zip(value.split("/")).map({|t| if !t.0.matched_any() {println!("{} {:?} ,", value, t.1)}}).collect();
        Ok(vec![PlayModifier::AppealPlay])
    }
    fn parse_advances(value: &str) -> Result<Vec<RunnerAdvance>> {
        Ok(vec![RunnerAdvance { from: Base::First, to: Base::First }])
    }
}
impl TryFrom<&str> for Play {
    type Error = Error;

    fn try_from(value: &str) -> Result<Self> {
        let (uncertain, exceptional) = (value.contains("#"), value.contains("!"));
        let value: &str = &STRIP_CHARS_REGEX.replace_all(value, "").to_string();
        let modifiers_boundary = value.find("/").unwrap_or(value.len());
        let advances_boundary = value.find(".").unwrap_or(value.len());
        let first_boundary = min(modifiers_boundary, advances_boundary);
        let main_play = Self::parse_main_play(&value[..first_boundary])?;
        let modifiers = if modifiers_boundary < advances_boundary {
            Self::parse_modifiers(&value[modifiers_boundary+1..advances_boundary])?
        } else {Vec::new()};
        let advances = if advances_boundary < value.len() - 1 {
            Self::parse_advances(&value[advances_boundary+1..])?
        } else {Vec::new()};
        Ok(Play {
            main_plays: vec![],
            modifiers: vec![],
            advances: vec![],
            uncertain_flag: false,
            exceptional_flag: false
        })
    }
}
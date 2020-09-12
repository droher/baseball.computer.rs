use std::ops::Deref;
use std::str::FromStr;

use anyhow::{anyhow, Context, Error, Result};
use either::{Either, Left};
use lazy_static::lazy_static;
use num_traits::cast::FromPrimitive;
use regex::Regex;
use serde::export::TryFrom;
use smallvec::SmallVec;
use strum_macros::{EnumDiscriminants, EnumString};

use crate::event_file_entities::{Fielder, Pitcher, Player, PlayRecord};

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
        if {opt_c == None} {break}
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

struct Play {
    main_plays: Vec<Either<BatterPlay, RunnerPlay>>,
    modifiers: Vec<PlayModifier>,
    advances: Vec<RunnerAdvance>,
    uncertain_flag: bool,
    exceptional_flag: bool
}
lazy_static!{
    static ref STRIP_CHARS: Regex = Regex::new(r"[#!0]").unwrap();
    // I'm sorry
    static ref OUT: Regex = Regex::new(r"^([1-9]+?)([1-9])?(\([B123]\))?(?:([1-9]+?)([1-9])?(\([B123]\))?)?(?:([1-9]+?)([1-9])?(\([B123]\))?)?$").unwrap();
    static ref SINGLE: Regex = Regex::new(r"^S([1-9])*$").unwrap();
    static ref DOUBLE: Regex = Regex::new(r"^D([1-9])*$").unwrap();
    static ref TRIPLE: Regex = Regex::new(r"^T([1-9])*$").unwrap();
    static ref HOME_RUN: Regex = Regex::new(r"^(H|HR)([1-9])?").unwrap();
    static ref GROUND_RULE_DOUBLE: Regex = Regex::new(r"^DGR$").unwrap();
    static ref REACH_ON_ERROR: Regex = Regex::new(r"^E([1-9])$").unwrap();
    static ref FIELDERS_CHOICE: Regex = Regex::new(r"^FC([1-9])$").unwrap();
    static ref ERROR_ON_FOUL: Regex = Regex::new(r"^FLE([1-9])$").unwrap();
    static ref HIT_BY_PITCH: Regex = Regex::new(r"^HP$").unwrap();
    static ref STRIKEOUT: Regex = Regex::new(r"^K$").unwrap();
    static ref NO_PLAY: Regex = Regex::new(r"^NP$").unwrap();
    static ref INTENTIONAL_WALK: Regex = Regex::new(r"^(I|IW)$").unwrap();
    static ref WALK: Regex = Regex::new(r"^W$").unwrap();
    static ref MULTI_PLAY: Regex = Regex::new(r"^(.+)\+(.+)$").unwrap();
    static ref BALK: Regex = Regex::new(r"^BK$").unwrap();
    static ref DEFENSIVE_INDIFFERFENCE: Regex = Regex::new(r"^DI$").unwrap();
    static ref OTHER_ADVANCE: Regex = Regex::new(r"^OA$").unwrap();
    static ref PASSED_BALL: Regex = Regex::new(r"^PB$").unwrap();
    static ref WILD_PITCH: Regex = Regex::new(r"^WP$").unwrap();
    static ref CAUGHT_STEALING: Regex = Regex::new(r"^CS([23H])(?:\(([0-9]*)(E[0-9])?\))?$").unwrap();
    static ref PICKED_OFF: Regex = Regex::new(r"^PO([123])(?:\(([0-9]*)(E[0-9])?\))?$").unwrap();
    static ref PICKED_OFF_CAUGHT_STEALING: Regex = Regex::new(r"^POCS([23H])(?:\(([0-9]*)(E[0-9])?\))?$").unwrap();
    static ref STOLEN_BASE: Regex = Regex::new(r"^SB[23H]$").unwrap();

    static ref APPEAL_PLAY: Regex = Regex::new(r"^AP$").unwrap();
    static ref POP_UP_BUNT: Regex = Regex::new(r"^BP([0-9].*)$").unwrap();


}

impl Play {


    fn parse_main_play(value: &str) -> Result<Either<BatterPlay, RunnerPlay>> {
        if value == "99" {return Ok(Left(BatterPlay::Unknown))}

        Ok(Left(BatterPlay::Unknown))

    }
    fn parse_modifiers(value: &str) -> Result<Vec<PlayModifier>> {
        unimplemented!()
    }
    fn parse_advances(value: &str) -> Result<Vec<RunnerAdvance>> {
        unimplemented!()
    }
}
impl TryFrom<&str> for Play {
    type Error = Error;

    fn try_from(value: &str) -> Result<Self> {
        let (uncertain, exceptional) = (value.contains("#"), value.contains("!"));
        let cleaned_value: String = STRIP_CHARS.replace_all(value, "").to_string();
        let value = cleaned_value.deref();
        let main_play_boundary = value.find("/").unwrap_or(value.len());
        let modifiers_boundary = value.find(".").unwrap_or(value.len());

        let main_play = Self::parse_main_play(&value[..main_play_boundary])?;
        let modifiers = if main_play_boundary < value.len() {
            Self::parse_modifiers(&value[main_play_boundary+1..modifiers_boundary])?
        } else {Vec::new()};
        let advances = if modifiers_boundary < value.len() {
            Self::parse_advances(&value[modifiers_boundary+1..])?
        } else {Vec::new()};
        Err(anyhow!(""))
    }
}
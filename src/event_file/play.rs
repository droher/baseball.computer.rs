use std::cmp::min;
use std::ops::Deref;
use std::str::FromStr;

use anyhow::{anyhow, Context, Error, Result};
use const_format::{concatcp, formatcp};
use num_enum::{TryFromPrimitive, IntoPrimitive};
use lazy_static::lazy_static;
use regex::{Captures, Match, Regex, Replacer};
use strum::ParseError;
use strum_macros::{EnumDiscriminants, EnumString};

use crate::util::{str_to_tinystr, regex_split, to_str_vec, pop_with_vec};
use crate::event_file::traits::{Inning, Side, Batter, FromRetrosheetRecord, RetrosheetEventRecord, FieldingPosition};
use std::convert::TryFrom;
use crate::event_file::pbp::BaseState;
use either::Either;
use crate::event_file::misc::EarnedRunRecord;
use either::Either::Left;
use crate::event_file::play::PlayType::PlateAppearance;


const NAMING_PREFIX: &str = r"(?P<";
const GROUP_ASSISTS: &str = r">(?:[0-9]?)+)";
const GROUP_ASSISTS1: &str = concatcp!(NAMING_PREFIX, "a1", GROUP_ASSISTS);
const GROUP_ASSISTS2: &str = concatcp!(NAMING_PREFIX, "a2", GROUP_ASSISTS);
const GROUP_ASSISTS3: &str = concatcp!(NAMING_PREFIX, "a3", GROUP_ASSISTS);
const GROUP_PUTOUT: &str = r">[0-9])";
const GROUP_PUTOUT1: &str = concatcp!(NAMING_PREFIX, "po1", GROUP_PUTOUT);
const GROUP_PUTOUT2: &str = concatcp!(NAMING_PREFIX, "po2", GROUP_PUTOUT);
const GROUP_PUTOUT3: &str = concatcp!(NAMING_PREFIX, "po3", GROUP_PUTOUT);
const GROUP_OUT_AT_BASE_PREFIX: &str = r"(?:\((?P<runner";
const GROUP_OUT_AT_BASE_SUFFIX: &str = r">[B123])\))?";
const GROUP_OUT_AT_BASE1: &str = concatcp!(GROUP_OUT_AT_BASE_PREFIX, "1", GROUP_OUT_AT_BASE_SUFFIX);
const GROUP_OUT_AT_BASE2: &str = concatcp!(GROUP_OUT_AT_BASE_PREFIX, "2", GROUP_OUT_AT_BASE_SUFFIX);
const GROUP_OUT_AT_BASE3: &str = concatcp!(GROUP_OUT_AT_BASE_PREFIX, "3", GROUP_OUT_AT_BASE_SUFFIX);

const OUT: &str = &formatcp!(r"^{}{}{}({}{}{})?({}{}{})?$",
                           GROUP_ASSISTS1, GROUP_PUTOUT1, GROUP_OUT_AT_BASE1,
                           GROUP_ASSISTS2, GROUP_PUTOUT2, GROUP_OUT_AT_BASE2,
                           GROUP_ASSISTS3, GROUP_PUTOUT3, GROUP_OUT_AT_BASE3
);

const REACH_ON_ERROR: &str = &formatcp!(r"{}E(?P<e>[0-9])$", GROUP_ASSISTS1);
const BASERUNNING_FIELDING_INFO: &str = r"(?P<base>[123H])(?:\((?P<fielders>[0-9]*)(?P<error>E[0-9])?\)?)?(?P<unearned_run>\(T?UR\))?$";


const ADVANCE: &str = r"^(?P<from>[B123])(?:(-(?P<to>[123H])|X(?P<out_at>[123H])))(?P<mods>.*)?";

lazy_static!{
    static ref OUT_REGEX: Regex = Regex::new(OUT).unwrap();
    static ref REACHED_ON_ERROR_REGEX: Regex = Regex::new(REACH_ON_ERROR).unwrap();
    static ref BASERUNNING_FIELDING_INFO_REGEX: Regex = Regex::new(BASERUNNING_FIELDING_INFO).unwrap();

    static ref ADVANCE_REGEX: Regex = Regex::new(ADVANCE).unwrap();
    static ref STRIP_CHARS_REGEX: Regex = Regex::new(r"[#! ]").unwrap();
    static ref UNKNOWN_FIELDER_REGEX: Regex = Regex::new(r"999*|\?").unwrap();
    static ref MULTI_PLAY_REGEX: Regex = Regex::new(r"[+;]").unwrap();
    static ref NUMERIC_REGEX: Regex = Regex::new(r"[0-9]").unwrap();
    static ref MAIN_PLAY_FIELDING_REGEX: Regex = Regex::new(r"[0-9]").unwrap();
    static ref BASERUNNING_PLAY_FIELDING_REGEX: Regex = Regex::new(r"[123H]").unwrap();
    static ref MODIFIER_DIVIDER_REGEX: Regex = Regex::new(r"[+\-0-9]").unwrap();

    static ref HIT_LOCATION_GENERAL_REGEX: Regex = Regex::new(r"[0-9]+").unwrap();
    static ref HIT_LOCATION_STRENGTH_REGEX: Regex = Regex::new(r"[+\-]").unwrap();
    static ref HIT_LOCATION_ANGLE_REGEX: Regex = Regex::new(r"[FML]").unwrap();
    static ref HIT_LOCATION_DEPTH_REGEX: Regex = Regex::new(r"(D|S|XD)").unwrap();
}

pub trait FieldingData {
    fn putouts(&self) -> PositionVec;

    fn assists(&self) -> PositionVec;

    fn errors(&self) -> PositionVec;
}

#[derive(Debug, Eq, PartialEq, EnumString, Copy, Clone)]
pub enum Base {
    #[strum(serialize = "1")]
    First = 1,
    #[strum(serialize = "2")]
    Second,
    #[strum(serialize = "3")]
    Third,
    #[strum(serialize = "H")]
    Home
}

#[derive(Debug, Eq, PartialEq, EnumString, Copy, Clone, TryFromPrimitive, IntoPrimitive)]
#[repr(u8)]
pub enum BaseRunner {
    #[strum(serialize = "B")]
    Batter,
    #[strum(serialize = "1")]
    First,
    #[strum(serialize = "2")]
    Second,
    #[strum(serialize = "3")]
    Third
}
impl BaseRunner {
    fn from_target_base(base: Base) -> Result<Self> {
        BaseRunner::try_from((base as u8) - 1).context("Could not find baserunner for target base")
    }


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

#[derive(Debug, PartialEq, Eq, Default, Copy, Clone)]
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

#[derive(Debug, Default, Eq, PartialEq, Clone)]
pub struct PitchSequence(pub Vec<Pitch>);

impl TryFrom<&str> for PitchSequence {
    type Error = Error;

    fn try_from(str_sequence: &str) -> Result<Self> {
        let mut pitches= Vec::with_capacity(10);
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
            let pitch_type: Result<PitchType> = PitchType::from_str(c.deref()).context("Bad pitch type");
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
                    if let Some('+') = speculative_iter.nth(1) {
                        pitch.update_catcher_pickoff(get_catcher_pickoff_base(char_iter.nth(2)))
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


#[derive(Debug, Eq, PartialEq, EnumString, Copy, Clone)]
#[strum(serialize_all = "lowercase")]
pub(crate) enum InningFrame {
    Top,
    Bottom,
}
impl Default for InningFrame {
    fn default() -> Self {Self::Top}
}

#[derive(Debug, EnumString, Copy, Clone, Eq, PartialEq)]
pub enum UnearnedRunStatus {
    #[strum(serialize = "UR")]
    Unearned,
    #[strum(serialize = "TUR")]
    TeamUnearned
}

#[derive(Debug, Copy, Clone)]
pub enum RbiStatus {
    RBI,
    NoRBI
}

type PositionVec = Vec<FieldingPosition>;

impl Default for BaserunningFieldingInfo {
    fn default() -> Self {
        Self {
            assists: vec![],
            putout: Default::default(),
            error: Default::default(),
            unearned_run: None
        }
    }
}

#[derive(Debug, EnumString, Copy, Clone, Eq, PartialEq)]
pub enum HitType {
    #[strum(serialize = "S")]
    Single,
    #[strum(serialize = "D")]
    Double,
    #[strum(serialize = "DGR")]
    GroundRuleDouble,
    #[strum(serialize = "T")]
    Triple,
    #[strum(serialize = "HR", serialize = "H")]
    HomeRun
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct Hit {
    hit_type: HitType,
    positions_hit_to: PositionVec
}
impl TryFrom<(&str, &str)> for Hit {
    type Error = Error;

    fn try_from(value: (&str, &str)) -> Result<Self> {
        let (first, last) = value;
        let hit_type = HitType::from_str(first)?;
        Ok(Self {
            hit_type,
            positions_hit_to: FieldingPosition::fielding_vec(last)
        })
    }
}


/// Note that a batting out is not necessarily the same thing as an actual out,
/// just a play which never counts for a hit and usually counts for an at-bat. Exceptions
/// include reaching on a fielder's choice, error, passed ball, or wild pitch, which count as at-bats but not outs,
/// and sacrifices, which count as outs but not at-bats. Baseball!
#[derive(Debug, EnumString, Copy, Clone, Eq, PartialEq)]
pub enum OutAtBatType {
    // Note that these may not always be at bats or outs in the event of SF, SH, PB, WP, and E
    #[strum(serialize = "")]
    InPlayOut,
    #[strum(serialize = "K")]
    StrikeOut,
    #[strum(serialize = "FC")]
    FieldersChoice,
    #[strum(serialize = "E")]
    ReachedOnError
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct FieldingPlay {
    assists: PositionVec,
    putouts: PositionVec,
    runners_out: Vec<BaseRunner>,
    error: Option<FieldingPosition>
}
impl Default for FieldingPlay {
    fn default() -> Self {
        Self {
            assists: vec![],
            putouts: vec![FieldingPosition::Unknown],
            runners_out: vec![],
            error: None
        }
    }
}
impl FieldingPlay {
    pub fn conventional_strikeout() -> Self {
        Self {
            assists: vec![],
            putouts: vec![FieldingPosition::Catcher],
            runners_out: vec![],
            error: None
        }
    }
}
impl TryFrom<PositionVec> for FieldingPlay {
    type Error = Error;

    fn try_from(value: PositionVec) -> Result<Self> {
        let (putout, assists) = pop_with_vec(value);
        Ok(Self {
            assists,
            putouts: vec![putout.unwrap_or_default()],
            runners_out: vec![],
            error: None
        })
    }
}
impl TryFrom<&str> for FieldingPlay {
    type Error = Error;

    fn try_from(value: &str) -> Result<Self> {
        if let Ok(_) = value.parse::<u32>() {
            return Self::try_from(FieldingPosition::fielding_vec(value))
        }
        else if let Some(captures) = OUT_REGEX.captures(value) {
            let assist_matches = vec![captures.name("a1"), captures.name("a2"), captures.name("a3")];
            let putout_matches = vec![captures.name("po1"), captures.name("po2"), captures.name("po3")];
            let runner_matches = vec![captures.name("runner1"), captures.name("runner2"), captures.name("runner3")];
            let (assists, putouts) = (FieldingPosition::fielding_vec(&to_str_vec(assist_matches).join("")), FieldingPosition::fielding_vec(&to_str_vec(putout_matches).join("")));
            let runners_out = to_str_vec(runner_matches)
                .into_iter()
                .map(|s| BaseRunner::from_str(s))
                .collect::<Result<Vec<BaseRunner>, ParseError>>()?;
            return Ok(Self {
                assists,
                putouts,
                runners_out,
                error: None
            })
        }
        else if let Some(captures) = REACHED_ON_ERROR_REGEX.captures(value) {
                let assists = FieldingPosition::fielding_vec(captures.name("a1").map(|s| s.as_str()).unwrap_or_default());
                let error = FieldingPosition::try_from(captures.name("e").map_or("", |s| s.as_str()))?;
                return Ok(Self {
                    assists,
                    putouts: vec![],
                    runners_out: vec![],
                    error: Some(error)
                })
            }
            Err(anyhow!("Unable to parse fielding play"))
    }
}


#[derive(Debug, Clone, Eq, PartialEq)]
pub struct BattingOut {
    out_type: OutAtBatType,
    fielding_play: FieldingPlay

}
impl TryFrom<(&str, &str)> for BattingOut {
    type Error = Error;

    fn try_from(value: (&str, &str)) -> Result<Self> {
        let (first, last) = value;
        let mut rejoined = first.to_string();
        rejoined.push_str(last);
        let out_type = OutAtBatType::from_str(first)?;
        let fielding_play = match out_type {
            // Put the whole string in when reaching on error
            OutAtBatType::ReachedOnError => FieldingPlay::try_from(rejoined.as_str())?,
            OutAtBatType::StrikeOut if last.is_empty() => FieldingPlay::conventional_strikeout(),
            OutAtBatType::FieldersChoice if last.is_empty() => FieldingPlay::default(),
            _ => FieldingPlay::try_from(last)?
        };
        Ok(Self {
            out_type,
            fielding_play
        })
    }
}

#[derive(Debug, EnumString, Copy, Clone, Eq, PartialEq)]
pub enum OtherPlateAppearance {
    #[strum(serialize = "C")]
    Interference,
    #[strum(serialize = "HP")]
    HitByPitch,
    #[strum(serialize = "W")]
    Walk,
    #[strum(serialize = "I", serialize = "IW")]
    IntentionalWalk
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub enum PlateAppearanceType {
    Hit(Hit),
    BattingOut(BattingOut),
    OtherPlateAppearance(OtherPlateAppearance)
}
impl TryFrom<(&str, &str)> for PlateAppearanceType {
    type Error = Error;

    fn try_from(value: (&str, &str)) -> Result<Self> {
        if let Ok(batting_out) = BattingOut::try_from(value) {
            Ok(Self::BattingOut(batting_out))
        }
        else if let Ok(hit) = Hit::try_from(value) {
            Ok(Self::Hit(hit))
        }
        else if let Ok(pa) = OtherPlateAppearance::from_str(value.0) {
            Ok(Self::OtherPlateAppearance(pa))
        }
        else {Err(anyhow!("Unable to parse plate appearance"))}
    }
}



#[derive(Debug, Clone, Eq, PartialEq)]
pub struct BaserunningFieldingInfo {
    assists: PositionVec,
    putout: Option<FieldingPosition>,
    error: Option<FieldingPosition>,
    unearned_run: Option<UnearnedRunStatus>
}
impl From<Captures<'_>> for BaserunningFieldingInfo {
    fn from(captures: Captures) -> Self {
        let get_capture = {
            |tag: &str| captures.name(tag)
                .map(|m| FieldingPosition::fielding_vec(m.as_str())).unwrap_or_default()};

        let unearned_run = captures.name("unearned_run").map(|s| if s.as_str().contains('T') {
            UnearnedRunStatus::TeamUnearned
        } else { UnearnedRunStatus::Unearned });
        if let Some(error) = get_capture("error").get(0) {
            let assists = get_capture("fielders");
            Self {
                assists,
                putout: None,
                error: Some(*error),
                unearned_run
            }
        } else {
            let (putout, assists) = pop_with_vec(get_capture("fielders"));
            Self {
                assists,
                putout,
                error: None,
                unearned_run
            }
        }
    }
}

#[derive(Debug, EnumString, Copy, Clone, Eq, PartialEq)]
pub enum BaserunningPlayType {
    #[strum(serialize = "PO")]
    PickedOff,
    #[strum(serialize = "POCS")]
    PickedOffCaughtStealing,
    #[strum(serialize = "SB")]
    StolenBase,
    #[strum(serialize = "CS")]
    CaughtStealing,
    #[strum(serialize = "DI")]
    DefensiveIndifference,
    #[strum(serialize = "BK")]
    Balk,
    #[strum(serialize = "OA")]
    OtherAdvance,
    #[strum(serialize = "WP")]
    WildPitch,
    #[strum(serialize = "PB")]
    PassedBall
}

#[derive(Debug, Eq, PartialEq, Clone)]
pub struct BaserunningPlay {
    baserunning_play_type: BaserunningPlayType,
    to_base: Option<Base>,
    baserunning_fielding_info: Option<BaserunningFieldingInfo>
}
impl TryFrom<&str> for BaserunningPlay {
    type Error = Error;

    fn try_from(value: &str) -> Result<Self> {
        let (first, last) = regex_split(value, &BASERUNNING_PLAY_FIELDING_REGEX);
        let baserunning_play_type = BaserunningPlayType::from_str(first)?;
        if last.is_none() {return Ok(Self {
            baserunning_play_type,
            to_base: None,
            baserunning_fielding_info: None
        })}
        let captures = BASERUNNING_FIELDING_INFO_REGEX.captures(last.unwrap_or_default()).context("Could not capture info from baserunning play")?;
        let to_base = Some(Base::from_str(captures.name("base").map_or("", |m| m.as_str()))?);
        let baserunning_fielding_info = Some(BaserunningFieldingInfo::from(captures));
        Ok(Self {
            baserunning_play_type,
            to_base,
            baserunning_fielding_info
        })
    }
}

#[derive(Debug, EnumString, Copy, Clone, Eq, PartialEq)]
pub enum NoPlayType {
    #[strum(serialize = "NP")]
    NoPlay,
    #[strum(serialize = "FLE")]
    ErrorOnFoul
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct NoPlay {
    no_play_type: NoPlayType,
    error: Option<FieldingPosition>
}
impl TryFrom<(&str, &str)> for NoPlay {
    type Error = Error;

    fn try_from(value: (&str, &str)) -> Result<Self> {
        let (first, last) = value;
        let no_play_type = NoPlayType::from_str(first)?;
        match no_play_type {
            NoPlayType::NoPlay => Ok(Self{ no_play_type, error: None }),
            NoPlayType::ErrorOnFoul => Ok(Self{
                no_play_type,
                error: FieldingPosition::fielding_vec(last).get(0).copied()
            })
        }
    }
}

#[derive(Debug, Eq, PartialEq, Clone)]
pub enum PlayType {
    PlateAppearance(PlateAppearanceType),
    BaserunningPlay(BaserunningPlay),
    NoPlay(NoPlay)
}

impl PlayType {
    /// Movement on the bases is not always explicitly given in the advances section.
    /// The batter's advance is usually implied by the play type (e.g. a double means ending up
    /// at second unless otherwise specified). This gives the implied advance for those play types,
    /// which should be overridden by any explicit value in the advances section. Unsuccessful
    /// advances are also often implied (e.g. caught stealing) but those do not cause issues when
    /// determining the state of the game.
    fn implied_advance(&self) -> Option<RunnerAdvance> {
        unimplemented!()
    }

    fn parse_main_play(value: &str) -> Result<Vec<PlayType>> {
        if value.is_empty() {return Ok(vec![])}
        if MULTI_PLAY_REGEX.is_match(value) {
            let (first, last) = regex_split(value, &MULTI_PLAY_REGEX);
            return Ok(Self::parse_main_play(first)?
                .into_iter()
                .chain(Self::parse_main_play(&last.unwrap().get(1..).unwrap_or_default())?.into_iter())
                .collect::<Vec<PlayType>>())
        }
        let (first, last) = regex_split(value, &MAIN_PLAY_FIELDING_REGEX);
        let str_tuple = (first, last.unwrap_or_default());
        if let Ok(pa) = PlateAppearanceType::try_from(str_tuple) {
            Ok(vec![Self::PlateAppearance(pa)])
        }
        else if let Ok(br) = BaserunningPlay::try_from(value) {
            Ok(vec![Self::BaserunningPlay(br)])
        }
        else if let Ok(np) = NoPlay::try_from(str_tuple) {
            Ok(vec![Self::NoPlay(np)])
        }
        else {Err(anyhow!("Unable to parse play"))}

    }
}

struct ScoringInfo {unearned: Option<UnearnedRunStatus>, rbi: bool}


#[derive(Debug, PartialEq, Eq, Clone)]
pub struct RunnerAdvance {
    pub baserunner: BaseRunner,
    pub to: Base,
    out_or_error: bool,
    pub modifiers: Vec<RunnerAdvanceModifier>
}

impl FieldingData for RunnerAdvance {
    fn putouts(&self) -> PositionVec {
        self.modifiers.iter().flat_map(FieldingData::putouts).collect()
    }

    fn assists(&self) -> PositionVec {
        self.modifiers.iter().flat_map(FieldingData::assists).collect()
    }

    fn errors(&self) -> PositionVec {
        self.modifiers.iter().flat_map(FieldingData::errors).collect()
    }
}

impl RunnerAdvance {
    pub fn is_error(&self) -> bool {
        self.modifiers
            .iter()
            .any(|m| m.is_error())
    }

    pub fn is_out(&self) -> bool {
        self.out_or_error && !self.is_error()
    }

    pub fn scored(&self) -> bool {
        self.to == Base::Home && !self.is_out()
    }

    pub fn unearned_status(&self) -> Option<UnearnedRunStatus> {
        self.modifiers.iter().find_map(|m| m.unearned_status())
    }

    pub fn rbi_status(&self) -> Option<RbiStatus> {
        self.modifiers.iter().find_map(|m| m.rbi_status())
    }

    fn parse_advances(value: &str) -> Result<Vec<RunnerAdvance>> {
        value
            .split(';')
            .filter_map(|s|ADVANCE_REGEX.captures(s))
            .map(Self::parse_single_advance)
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
        Ok(RunnerAdvance {baserunner, to, out_or_error, modifiers})

    }
}

#[derive(Debug, PartialEq, Eq, EnumDiscriminants, Clone)]
pub enum RunnerAdvanceModifier {
    UnearnedRun,
    TeamUnearnedRun,
    NoRBI,
    Interference(FieldingPosition),
    RBI,
    PassedBall,
    WildPitch,
    AdvancedOnThrowTo(Option<Base>),
    AdvancedOnError {assists: PositionVec, error: FieldingPosition},
    Putout{assists: PositionVec, putout: FieldingPosition},
    Unrecognized(String)
}
impl RunnerAdvanceModifier {
    fn unearned_status(&self) -> Option<UnearnedRunStatus> {
        match self {
            Self::UnearnedRun => Some(UnearnedRunStatus::Unearned),
            Self::TeamUnearnedRun => Some(UnearnedRunStatus::TeamUnearned),
            _ => None
        }
    }

    fn rbi_status(&self) -> Option<RbiStatus> {
        match self {
            Self::RBI => Some(RbiStatus::RBI),
            Self::NoRBI => Some(RbiStatus::NoRBI),
            _ => None
        }
    }
}

impl FieldingData for RunnerAdvanceModifier {
    fn putouts(&self) -> PositionVec {
        match self {
            Self::Putout{putout, ..} => vec![*putout],
            _ => vec![]
        }
    }

    fn assists(&self) -> PositionVec {
        match self {
            Self::AdvancedOnError{assists, ..} | Self::Putout{assists, ..} => PositionVec::from(assists.deref()),
            _ => vec![]
        }
    }

    fn errors(&self) -> PositionVec {
        match self {
            Self::AdvancedOnError{error, ..} => vec![*error],
            _ => vec![]
        }
    }
}
impl RunnerAdvanceModifier {
    pub fn is_error(&self) -> bool {
        RunnerAdvanceModifierDiscriminants::AdvancedOnError == self.into()
    }

    fn parse_advance_modifiers(value: &str) -> Result<Vec<RunnerAdvanceModifier>> {
        value
            .split(')')
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
            "(" => RunnerAdvanceModifier::Putout { assists: vec![], putout: FieldingPosition::Unknown },
            _ => RunnerAdvanceModifier::Unrecognized(value.into())
        };
        match simple_match {
            RunnerAdvanceModifier::Unrecognized(_) => (),
            _ => { return Ok(simple_match) }
        };
        let (first, last) = regex_split(value, &NUMERIC_REGEX);
        let last = last.unwrap_or_default();
        let last_as_int_vec: PositionVec = FieldingPosition::fielding_vec(last);
        let final_match = match first {
            "(INT" => RunnerAdvanceModifier::Interference(last_as_int_vec.first().copied().unwrap_or(FieldingPosition::Unknown)),
            "(TH" => RunnerAdvanceModifier::AdvancedOnThrowTo(Base::from_str(last).ok()),
            "(E" => RunnerAdvanceModifier::AdvancedOnError { assists: Vec::new(), error: FieldingPosition::try_from(last.get(0..1).unwrap_or_default()).unwrap_or(FieldingPosition::Unknown) },
            "(" if last.contains('E') => {
                let (assist_str, error_str) = last.split_at(last.find('E').unwrap());
                let (assists, error) = (FieldingPosition::fielding_vec(assist_str), FieldingPosition::fielding_vec(error_str).first().copied().unwrap_or(FieldingPosition::Unknown));
                RunnerAdvanceModifier::AdvancedOnError { assists, error }
            },
            "(" => {
                let mut digits = FieldingPosition::fielding_vec(last);
                let (putout, assists) = (digits.pop().unwrap_or(FieldingPosition::Unknown), digits);
                RunnerAdvanceModifier::Putout { assists, putout }
            }
            _ => RunnerAdvanceModifier::Unrecognized(value.to_string())
        };
        Ok(final_match)
    }
}

#[derive(Debug, Eq, PartialEq, EnumString, Copy, Clone)]
pub enum HitStrength {
    #[strum(serialize = "+")]
    Hard,
    #[strum(serialize = "-")]
    Soft,
    Default
}
impl Default for HitStrength {
    fn default() -> Self {Self::Default}
}

#[derive(Debug, Eq, PartialEq, EnumString, Copy, Clone)]
pub enum HitDepth {
    #[strum(serialize = "S")]
    Shallow,
    #[strum(serialize = "D")]
    Deep,
    #[strum(serialize = "XD")]
    ExtraDeep,
    Default
}
impl Default for HitDepth {
    fn default() -> Self {Self::Default}
}

#[derive(Debug, Eq, PartialEq, EnumString, Copy, Clone)]
pub enum HitAngle {
    #[strum(serialize = "F")]
    Foul,
    #[strum(serialize = "M")]
    Middle,
    #[strum(serialize = "L")]
    FoulLine,
    Default
}
impl Default for HitAngle {
    fn default() -> Self {Self::Default}
}

#[derive(Debug, Eq, PartialEq, EnumString, Copy, Clone)]
pub enum HitLocationGeneral {
    #[strum(serialize = "1")]
    Pitcher,
    #[strum(serialize = "13")]
    PitcherFirst,
    #[strum(serialize = "15")]
    PitcherThird,
    #[strum(serialize = "2")]
    Catcher,
    #[strum(serialize = "23")]
    CatcherFirst,
    #[strum(serialize = "25")]
    CatcherThird,
    #[strum(serialize = "3")]
    First,
    #[strum(serialize = "34")]
    FirstSecond,
    #[strum(serialize = "4")]
    Second,
    #[strum(serialize = "46")]
    SecondShortstop,
    #[strum(serialize = "5")]
    Third,
    #[strum(serialize = "56")]
    ThirdShortstop,
    #[strum(serialize = "6")]
    Shortstop,
    #[strum(serialize = "7")]
    Left,
    #[strum(serialize = "78")]
    LeftCenter,
    #[strum(serialize = "8")]
    Center,
    #[strum(serialize = "89")]
    RightCenter,
    #[strum(serialize = "9")]
    Right,
}

#[derive(Debug, Eq, PartialEq, Copy, Clone)]
pub struct HitLocation {
    general_location: HitLocationGeneral,
    depth: HitDepth,
    angle: HitAngle,
    strength: HitStrength
}
impl TryFrom<&str> for HitLocation {
    type Error = Error;

    fn try_from(value: &str) -> Result<Self> {
        let as_str = {|re: &Regex| re.find(value).map_or("",  |m| m.as_str())};
        // If there's no general location found, that's unexpected behavior and we should short-circuit, but other missing info is expected
        let general_location = HitLocationGeneral::from_str(as_str(&HIT_LOCATION_GENERAL_REGEX))?;
        let depth = HitDepth::from_str(as_str(&HIT_LOCATION_DEPTH_REGEX)).unwrap_or_default();
        let angle = HitAngle::from_str(as_str(&HIT_LOCATION_ANGLE_REGEX)).unwrap_or_default();
        let strength = HitStrength::from_str(as_str(&HIT_LOCATION_STRENGTH_REGEX)).unwrap_or_default();
        Ok(Self {
            general_location,
            depth,
            angle,
            strength
        })
    }
}



#[derive(Debug, Eq, PartialEq, Copy, Clone)]
pub struct ContactDescription {
    contact_type: ContactType,
    location: Option<HitLocation>
}
impl TryFrom<(&str, &str)> for ContactDescription {
    type Error = Error;

    fn try_from(tup: (&str, &str)) -> Result<Self> {
        let (contact, loc) = tup;
        let contact_type = ContactType::from_str(contact)?;
        let location = HitLocation::try_from(loc).ok();
        Ok(Self{
            contact_type,
            location
        })

    }
}

#[derive(Debug, Eq, PartialEq, Copy, Clone, EnumString)]
pub enum ContactType {
    #[strum(serialize = "B")]
    UnspecifiedBunt,
    #[strum(serialize = "BP")]
    PopUpBunt,
    #[strum(serialize = "BG")]
    GroundBallBunt,
    #[strum(serialize = "BF")]
    FoulBunt,
    #[strum(serialize = "BL")]
    LineDriveBunt,
    #[strum(serialize = "F")]
    Fly,
    #[strum(serialize = "G")]
    GroundBall,
    #[strum(serialize = "L")]
    LineDrive,
    #[strum(serialize = "P")]
    PopFly,
    #[strum(serialize = "")]
    Unknown
}

#[derive(Debug, EnumDiscriminants, Eq, PartialEq, Clone)]
#[strum_discriminants(derive(EnumString))]
pub enum PlayModifier {
    ContactDescription(ContactDescription),
    AppealPlay,
    BuntGroundIntoDoublePlay,
    BatterInterference,
    BatingOutOfTurn,
    BuntPoppedIntoDoublePlay,
    RunnerHitByBattedBall,
    CalledThirdStrike,
    CourtesyBatter,
    CourtesyFielder,
    CourtesyRunner,
    UnspecifiedDoublePlay,
    ErrorOn(FieldingPosition),
    FlyBallDoublePlay,
    FanInterference,
    Foul,
    ForceOut,
    GroundBallDoublePlay,
    GroundBallTriplePlay,
    InfieldFlyRule,
    Interference,
    InsideTheParkHomeRun,
    LinedIntoDoublePlay,
    LinedIntoTriplePlay,
    ManageChallengeOfCallOnField,
    NoDoublePlayCredited,
    Obstruction,
    RunnerOutPassingAnotherRunner,
    RelayToFielderWithNoOutMade(PositionVec),
    RunnerInterference,
    SwingingThirdStrike,
    SacrificeFly,
    SacrificeHit,
    ThrowToBase(Option<Base>),
    UnspecifiedTriplePlay,
    UmpireInterference,
    UmpireReviewOfCallOnField,
    Unknown,
    Unrecognized(String)
}

impl FieldingData for PlayModifier {
    // No putout or assist data in modifiers
    fn putouts(&self) -> PositionVec {
        Vec::new()
    }

    fn assists(&self) -> PositionVec {
        Vec::new()
    }

    fn errors(&self) -> PositionVec {
        if let Self::ErrorOn(p) = self {vec![*p]} else {vec![]}
    }
}

impl PlayModifier {
    fn parse_modifiers(value: &str) -> Result<Vec<PlayModifier>> {
        value
            .split('/')
            .filter(|s| !s.is_empty())
            .map(|s| Self::parse_single_modifier(s))
            .collect::<Result<Vec<PlayModifier>>>()
    }

    fn parse_single_modifier(value: &str) -> Result<PlayModifier> {
        let (first, last) = regex_split(value, &MODIFIER_DIVIDER_REGEX);
        let last_as_int_vec = {|| FieldingPosition::fielding_vec(&last.unwrap_or_default()) };
        if let Ok(cd) = ContactDescription::try_from((first, last.unwrap_or_default())) {
            return Ok(PlayModifier::ContactDescription(cd))
        }
        let final_match = match first {
            "AP" => PlayModifier::AppealPlay,
            "BGDP" => PlayModifier::BuntGroundIntoDoublePlay,
            "BINT" => PlayModifier::BatterInterference,
            "BOOT" => PlayModifier::BatingOutOfTurn,
            "BPDP" => PlayModifier::BuntPoppedIntoDoublePlay,
            "BR" => PlayModifier::RunnerHitByBattedBall,
            "C" => PlayModifier::CalledThirdStrike,
            "COUB" => PlayModifier::CourtesyBatter,
            "COUF" => PlayModifier::CourtesyFielder,
            "COUR" => PlayModifier::CourtesyRunner,
            "DP" => PlayModifier::UnspecifiedDoublePlay,
            "E" => PlayModifier::ErrorOn(*last_as_int_vec().first().context("Missing error position info")?),
            "FDP" => PlayModifier::FlyBallDoublePlay,
            "FINT" => PlayModifier::FanInterference,
            "FL" => PlayModifier::Foul,
            "FO" => PlayModifier::ForceOut,
            "GDP" => PlayModifier::GroundBallDoublePlay,
            "GTP" => PlayModifier::GroundBallTriplePlay,
            "IF" => PlayModifier::InfieldFlyRule,
            "INT" => PlayModifier::Interference,
            "IPHR" => PlayModifier::InsideTheParkHomeRun,
            "LDP" => PlayModifier::LinedIntoDoublePlay,
            "LTP" => PlayModifier::LinedIntoTriplePlay,
            "MREV" => PlayModifier::ManageChallengeOfCallOnField,
            "NDP" => PlayModifier::NoDoublePlayCredited,
            "OBS" => PlayModifier::Obstruction,
            "PASS" => PlayModifier::RunnerOutPassingAnotherRunner,
            "R" => PlayModifier::RelayToFielderWithNoOutMade(last_as_int_vec()),
            "RINT" => PlayModifier::RunnerInterference,
            "S" => PlayModifier::SwingingThirdStrike,
            "SF" => PlayModifier::SacrificeFly,
            "SH" => PlayModifier::SacrificeHit,
            "TH" | "TH)" => PlayModifier::ThrowToBase(Base::from_str(&last.unwrap_or_default()).ok()),
            "THH" => PlayModifier::ThrowToBase(Some(Base::Home)),
            "TP" => PlayModifier::UnspecifiedTriplePlay,
            "UINT" => PlayModifier::UmpireInterference,
            "UREV" => PlayModifier::UmpireReviewOfCallOnField,
            // TODO: Unassisted?
            "U" => PlayModifier::Unknown,
            _ => PlayModifier::Unrecognized(value.into())
        };
        Ok(final_match)
    }
}

#[derive(Debug, Eq, PartialEq, Default, Clone)]
pub struct Play {
    pub main_plays: Vec<PlayType>,
    pub modifiers: Vec<PlayModifier>,
    pub advances: Vec<RunnerAdvance>,
    pub uncertain_flag: bool,
    pub exceptional_flag: bool
}
impl Play {
    pub fn out_advancing(&self) -> Vec<BaseRunner> {
        self.advances.iter()
            .filter(|a| a.is_out())
            .map(|a| a.baserunner)
            .collect()
    }
}

impl TryFrom<&str> for Play {
    type Error = Error;

    fn try_from(value: &str) -> Result<Self> {
        let (uncertain_flag, exceptional_flag) = (value.contains('#'), value.contains('!'));
        let value = &*STRIP_CHARS_REGEX.replace_all(value, "");
        let value = &*UNKNOWN_FIELDER_REGEX.replace_all(value, "0");
        if value.is_empty() {return Ok(Play::default())}

        let modifiers_boundary = value.find('/').unwrap_or_else(|| value.len());
        let advances_boundary = value.find('.').unwrap_or_else(|| value.len());
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

#[derive(Debug, Default, Eq, PartialEq, Copy, Clone)]
pub struct Count { balls: Option<u8>, strikes: Option<u8> }

impl Count {
    fn new(count_str: &str) -> Result<Count> {
        let mut ints = count_str.chars().map(|c| c.to_digit(10).map(|i| i as u8));

        Ok(Count {
            balls: ints.next().flatten(),
            strikes: ints.next().flatten()
        })
    }
}

#[derive(Debug, Eq, PartialEq, Clone)]
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
            batter: str_to_tinystr(record[3])?,
            count: Count::new(record[4])?,
            pitch_sequence: {match record[5] {"" => None, s => Some(PitchSequence::try_from(s)?)}},
            play: Play::try_from(record[6])?
        })
    }
}

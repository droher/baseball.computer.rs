use std::cmp::min;
use std::collections::HashSet;
use std::convert::TryFrom;
use std::hash::Hash;
use std::iter::FromIterator;
use std::mem::discriminant;
use std::str::FromStr;
use std::sync::Arc;

use anyhow::{bail, Context, Error, Result};
use arrayvec::ArrayVec;
use bounded_integer::BoundedU8;
use fixed_map::{Key, Set};
use itertools::Itertools;
use lazy_regex::{regex, Lazy};
use lazy_static::lazy_static;
use num_enum::{IntoPrimitive, TryFromPrimitive};
use quick_cache::sync::Cache;
use regex::{Captures, Match, Regex};
use serde::{Deserialize, Serialize};
use strum::ParseError;
use strum_macros::{AsRefStr, Display, EnumDiscriminants, EnumIter, EnumString};

use crate::event_file::misc::{regex_split, str_to_tinystr, to_str_vec};
use crate::event_file::pitch_sequence::{PitchSequence, PitchSequenceItem};
use crate::event_file::traits::{
    Batter, FieldingPlayType, FieldingPosition, Inning, RetrosheetEventRecord, Side,
};

use super::misc::arrow_hack;

// Sorry
pub static OUT_REGEX: &Lazy<Regex> = regex!(
    r"^(?P<a1>(?:[0-9]?)+)(?P<po1>[0-9])(?:\((?P<runner1>[B123])\))?((?P<a2>(?:[0-9]?)+)(?P<po2>[0-9])(?:\((?P<runner2>[B123])\))?)?((?P<a3>(?:[0-9]?)+)(?P<po3>[0-9])(?:\((?P<runner3>[B123])\))?)?$"
);
pub static REACHED_ON_ERROR_REGEX: &Lazy<Regex> = regex!(r"(?P<a1>(?:[0-9]?)+)E(?P<e>[0-9])$");
pub static BASERUNNING_FIELDING_INFO_REGEX: &Lazy<Regex> = regex!(
    r"(?P<base>[123H])(?:\((?P<fielders>[0-9]*)(?P<error>E[0-9])?\)?)?(?P<unearned_run>\(T?UR\))?$"
);
pub static ADVANCE_REGEX: &Lazy<Regex> =
    regex!(r"^(?P<from>[B123])(?:(-(?P<to>[123H])|X(?P<out_at>[123H])))(?P<mods>.*)?$");
pub static STRIP_CHARS_REGEX: &Lazy<Regex> = regex!(r"[#! ]");
pub static UNKNOWN_FIELDER_REGEX: &Lazy<Regex> = regex!(r"999*|\?");
pub static MULTI_PLAY_REGEX: &Lazy<Regex> = regex!(r"[+;]");
pub static NUMERIC_REGEX: &Lazy<Regex> = regex!(r"[0-9]");
pub static MAIN_PLAY_FIELDING_REGEX: &Lazy<Regex> = regex!(r"[0-9]");
pub static BASERUNNING_PLAY_FIELDING_REGEX: &Lazy<Regex> = regex!(r"[123H]");
pub static MODIFIER_DIVIDER_REGEX: &Lazy<Regex> = regex!(r"[+\-0-9]");
pub static HIT_LOCATION_GENERAL_REGEX: &Lazy<Regex> = regex!(r"[0-9]+");
pub static HIT_LOCATION_STRENGTH_REGEX: &Lazy<Regex> = regex!(r"[0-9]+");
pub static HIT_LOCATION_ANGLE_REGEX: &Lazy<Regex> = regex!(r"[FMLR]");
pub static HIT_LOCATION_DEPTH_REGEX: &Lazy<Regex> = regex!(r"(D|S|XD)");

lazy_static! {
    static ref PARSED_PLAY_CACHE: Arc<Cache<String, Arc<ParsedPlay>>> =
        preallocated_cache::<String, ParsedPlay>(10000);
    static ref MAIN_PLAY_CACHE: Arc<Cache<String, Arc<Vec<PlayType>>>> =
        preallocated_cache::<String, Vec<PlayType>>(4000);
    static ref PLAY_MODIFIER_CACHE: Arc<Cache<String, Arc<Vec<PlayModifier>>>> =
        preallocated_cache::<String, Vec<PlayModifier>>(10000);
    static ref RUNNER_ADVANCES_CACHE: Arc<Cache<String, Arc<Vec<RunnerAdvance>>>> =
        preallocated_cache::<String, Vec<RunnerAdvance>>(10000);
    static ref PLAY_STATS_CACHE: Arc<Cache<String, Arc<PlayStats>>> =
        preallocated_cache::<String, PlayStats>(10000);
    static ref PITCH_SEQUENCE_CACHE: Arc<Cache<String, Arc<PitchSequence>>> =
        preallocated_cache::<String, PitchSequence>(10000);
}

/// Instantiates a new cache with the given size and preallocates the given number of entries.
/// This reduces the number of allocations needed to insert new entries into the cache.
fn preallocated_cache<K: Hash + Eq, V: Clone>(size: usize) -> Arc<Cache<K, Arc<V>>> {
    let mut cache = Cache::new(size);
    cache.reserve(size);
    Arc::new(cache)
}

#[derive(Debug, Eq, PartialEq, Copy, Clone, Hash, Serialize, Deserialize, Ord, PartialOrd)]
pub struct FieldersData {
    pub fielding_position: FieldingPosition,
    #[serde(serialize_with = "arrow_hack")]
    pub fielding_play_type: FieldingPlayType,
}

impl FieldersData {
    const fn new(
        fielding_position: FieldingPosition,
        fielding_play_type: FieldingPlayType,
    ) -> Self {
        Self {
            fielding_position,
            fielding_play_type,
        }
    }

    pub fn find_error(fielders_datas: &[Self]) -> Option<Self> {
        fielders_datas
            .iter()
            .find(|fd| fd.fielding_play_type == FieldingPlayType::Error)
            .copied()
    }

    fn filter_by_type(
        fielders_datas: &[Self],
        fielding_play_type: FieldingPlayType,
    ) -> PositionVec {
        fielders_datas
            .iter()
            .filter_map(|fp| {
                if fp.fielding_play_type == fielding_play_type {
                    Some(fp.fielding_position)
                } else {
                    None
                }
            })
            .collect()
    }

    fn putouts(fielders_datas: &[Self]) -> PositionVec {
        Self::filter_by_type(fielders_datas, FieldingPlayType::Putout)
    }

    fn assists(fielders_datas: &[Self]) -> PositionVec {
        Self::filter_by_type(fielders_datas, FieldingPlayType::Assist)
    }

    fn errors(fielders_datas: &[Self]) -> PositionVec {
        Self::filter_by_type(fielders_datas, FieldingPlayType::Error)
    }

    fn fielders_choices(fielders_datas: &[Self]) -> PositionVec {
        Self::filter_by_type(fielders_datas, FieldingPlayType::FieldersChoice)
    }

    const fn unknown_putout() -> Self {
        Self {
            fielding_position: FieldingPosition::Unknown,
            fielding_play_type: FieldingPlayType::Putout,
        }
    }

    const fn conventional_strikeout() -> Self {
        Self {
            fielding_position: FieldingPosition::Catcher,
            fielding_play_type: FieldingPlayType::Putout,
        }
    }

    fn from_vec(vec: &[FieldingPosition], fielding_play_type: FieldingPlayType) -> Vec<Self> {
        vec.iter()
            .map(|fp| Self::new(*fp, fielding_play_type))
            .collect()
    }
}

pub trait FieldingData {
    fn fielders_data(&self) -> Vec<FieldersData>;
}

#[derive(
    Display,
    Debug,
    Eq,
    PartialEq,
    EnumString,
    Copy,
    Clone,
    Hash,
    IntoPrimitive,
    EnumIter,
    Serialize,
    Deserialize,
    Ord,
    PartialOrd,
    AsRefStr,
)]
#[repr(u8)]
pub enum Base {
    #[strum(serialize = "1")]
    First = 1,
    #[strum(serialize = "2")]
    Second,
    #[strum(serialize = "3")]
    Third,
    #[strum(serialize = "H")]
    Home,
}

#[derive(
    Display,
    Debug,
    Eq,
    PartialEq,
    Hash,
    PartialOrd,
    Ord,
    EnumString,
    Copy,
    Clone,
    TryFromPrimitive,
    IntoPrimitive,
    EnumIter,
    Serialize,
    Deserialize,
    Key,
    AsRefStr,
)]
#[repr(u8)]
pub enum BaseRunner {
    #[strum(serialize = "B")]
    Batter,
    #[strum(serialize = "1")]
    First,
    #[strum(serialize = "2")]
    Second,
    #[strum(serialize = "3")]
    Third,
}

impl BaseRunner {
    /// In some cases, only the base being advanced to is known. This function
    /// returns the baserunner that would be advancing to that base (always
    /// the baserunner on the previous base, otherwise it would be specified).
    pub const fn from_target_base(base: Base) -> Self {
        match base {
            Base::First => Self::Batter,
            Base::Second => Self::First,
            Base::Third => Self::Second,
            Base::Home => Self::Third,
        }
    }

    pub const fn from_current_base(base: Base) -> Self {
        match base {
            Base::First => Self::First,
            Base::Second => Self::Second,
            Base::Third => Self::Third,
            // This condition doesn't get hit in the current implementation,
            // but it's here for completeness
            Base::Home => Self::Batter,
        }
    }
}

#[derive(
    Debug, Eq, PartialEq, EnumString, Copy, Clone, Ord, PartialOrd, Serialize, Deserialize, AsRefStr,
)]
#[strum(serialize_all = "lowercase")]
pub enum InningFrame {
    Top,
    Bottom,
}

impl InningFrame {
    pub const fn flip(self) -> Self {
        match self {
            Self::Top => Self::Bottom,
            Self::Bottom => Self::Top,
        }
    }
}

impl Default for InningFrame {
    fn default() -> Self {
        Self::Top
    }
}

#[derive(Debug, EnumString, Copy, Clone, Eq, PartialEq, Hash, Serialize, Deserialize)]
pub enum UnearnedRunStatus {
    #[strum(serialize = "UR")]
    Unearned,
    #[strum(serialize = "TUR")]
    TeamUnearned, // Earned to the (relief) pitcher, unearned to the team
}

#[derive(Debug, Eq, PartialEq, Copy, Clone, Hash)]
pub enum RbiStatus {
    Rbi,
    NoRbi,
}

pub type PositionVec = Vec<FieldingPosition>;
pub type BaseRunners = ArrayVec<BaseRunner, 4>;
pub type Balls = BoundedU8<0, 3>;
pub type Strikes = BoundedU8<0, 2>;

/// Movement on the bases is not always explicitly given in the advances section.
/// The batter's advance is usually implied by the play type (e.g. a double means ending up
/// at second unless otherwise specified). This gives the implied advance for those play types,
/// which should be overridden by any explicit value in the advances section. Unsuccessful
/// advances are also often implied (e.g. caught stealing) but those do not cause issues when
/// determining the state of the game.
pub trait ImplicitPlayResults {
    fn implicit_advance(&self) -> Option<RunnerAdvance> {
        None
    }

    fn implicit_out(&self) -> Vec<BaseRunner> {
        vec![]
    }
}

#[derive(
    Debug,
    EnumString,
    Copy,
    Clone,
    Eq,
    PartialEq,
    Hash,
    Ord,
    PartialOrd,
    Serialize,
    Deserialize,
    Display,
)]
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
    HomeRun,
}

impl ImplicitPlayResults for HitType {
    fn implicit_advance(&self) -> Option<RunnerAdvance> {
        let base = match self {
            Self::Single => Base::First,
            Self::Double | Self::GroundRuleDouble => Base::Second,
            Self::Triple => Base::Third,
            Self::HomeRun => Base::Home,
        };
        Some(RunnerAdvance::batter_advance(base))
    }
}

#[derive(Debug, Clone, Eq, PartialEq, Hash, Serialize, Deserialize)]
pub struct Hit {
    pub hit_type: HitType,
    positions_hit_to: PositionVec,
}

impl ImplicitPlayResults for Hit {
    fn implicit_advance(&self) -> Option<RunnerAdvance> {
        self.hit_type.implicit_advance()
    }
}

impl TryFrom<(&str, &str)> for Hit {
    type Error = Error;

    fn try_from(value: (&str, &str)) -> Result<Self> {
        let (first, last) = value;
        let hit_type = HitType::from_str(first)?;
        Ok(Self {
            hit_type,
            positions_hit_to: FieldingPosition::fielding_vec(last),
        })
    }
}

/// Note that a batting out is not necessarily the same thing as an actual out,
/// just a play which never counts for a hit and usually counts for an at-bat. Exceptions
/// include reaching on a fielder's choice, error, passed ball, or wild pitch, which count as at-bats but not outs,
/// and sacrifices, which count as outs but not at-bats. Baseball!
#[derive(Debug, EnumString, Copy, Clone, Eq, PartialEq, Hash, Serialize, Deserialize)]
pub enum OutAtBatType {
    // Note that these may not always be at bats or outs in the event of SF, SH, PB, WP, and E
    #[strum(serialize = "")]
    InPlayOut,
    #[strum(serialize = "K")]
    StrikeOut,
    #[strum(serialize = "FC")]
    FieldersChoice,
    #[strum(serialize = "E")]
    ReachedOnError,
}

#[derive(Debug, Clone, Eq, PartialEq, Hash, Serialize, Deserialize)]
pub struct FieldingPlay {
    fielders_data: Vec<FieldersData>,
    runners_out: Vec<BaseRunner>,
}

impl FieldingPlay {
    fn conventional_strikeout() -> Self {
        Self {
            fielders_data: vec![FieldersData::conventional_strikeout()],
            runners_out: vec![],
        }
    }

    fn fielders_choice(position: FieldingPosition) -> Self {
        let fc = FieldersData::new(position, FieldingPlayType::FieldersChoice);
        Self {
            fielders_data: vec![fc],
            runners_out: vec![],
        }
    }
}

impl Default for FieldingPlay {
    fn default() -> Self {
        Self {
            fielders_data: vec![FieldersData::unknown_putout()],
            runners_out: vec![],
        }
    }
}

impl From<PositionVec> for FieldingPlay {
    fn from(value: PositionVec) -> Self {
        let mut fielders_data: Vec<FieldersData> = value
            .into_iter()
            .map(|fp| FieldersData::new(fp, FieldingPlayType::Assist))
            .collect();
        if let Some(fd) = fielders_data.last_mut() {
            fd.fielding_play_type = FieldingPlayType::Putout;
        }
        Self {
            fielders_data,
            runners_out: vec![],
        }
    }
}

impl TryFrom<&str> for FieldingPlay {
    type Error = Error;

    fn try_from(value: &str) -> Result<Self> {
        let to_vec = |matches: Vec<Option<Match>>| {
            FieldingPosition::fielding_vec(&to_str_vec(matches).join(""))
        };
        let to_fielding_data =
            |matches: Vec<Option<Match>>, fielding_play_type: FieldingPlayType| {
                FieldersData::from_vec(&to_vec(matches), fielding_play_type)
            };

        if value.parse::<u32>().is_ok() {
            return Ok(Self::from(FieldingPosition::fielding_vec(value)));
        } else if let Some(captures) = OUT_REGEX.captures(value) {
            let assist_matches = vec![
                captures.name("a1"),
                captures.name("a2"),
                captures.name("a3"),
            ];
            let putout_matches = vec![
                captures.name("po1"),
                captures.name("po2"),
                captures.name("po3"),
            ];
            let runner_matches = vec![
                captures.name("runner1"),
                captures.name("runner2"),
                captures.name("runner3"),
            ];
            let fielders_data = [
                to_fielding_data(assist_matches, FieldingPlayType::Assist),
                to_fielding_data(putout_matches, FieldingPlayType::Putout),
            ]
            .concat();

            let runners_out = to_str_vec(runner_matches)
                .into_iter()
                .map(BaseRunner::from_str)
                .collect::<Result<Vec<BaseRunner>, ParseError>>()?;
            return Ok(Self {
                fielders_data,
                runners_out,
            });
        } else if let Some(captures) = REACHED_ON_ERROR_REGEX.captures(value) {
            let fielders_data = [
                to_fielding_data(vec![captures.name("a1")], FieldingPlayType::Assist),
                to_fielding_data(vec![captures.name("e")], FieldingPlayType::Error),
            ]
            .concat();

            return Ok(Self {
                fielders_data,
                runners_out: vec![],
            });
        }
        bail!("Unable to parse fielding play")
    }
}

#[derive(Debug, Clone, Eq, PartialEq, Hash, Serialize, Deserialize)]
pub struct BattingOut {
    pub out_type: OutAtBatType,
    fielding_play: Option<FieldingPlay>,
}

impl ImplicitPlayResults for BattingOut {
    fn implicit_advance(&self) -> Option<RunnerAdvance> {
        let batter_advance = Some(RunnerAdvance::batter_advance(Base::First));
        match &self.fielding_play {
            // Fielder's choice
            None => batter_advance,
            Some(fp) if fp.runners_out.contains(&BaseRunner::Batter) => None,
            Some(fp) if FieldersData::find_error(&fp.fielders_data).is_some() => batter_advance,
            Some(fp) if FieldersData::putouts(&fp.fielders_data).len() <= fp.runners_out.len() => {
                batter_advance
            }
            _ => None,
        }
    }

    fn implicit_out(&self) -> Vec<BaseRunner> {
        let new_vec = Vec::new();
        let runners_out = &self
            .fielding_play
            .as_ref()
            .map_or(&new_vec, |fp| &fp.runners_out);

        let mut batter_out = if self.implicit_advance().is_none() && runners_out.is_empty() {
            vec![BaseRunner::Batter]
        } else {
            vec![]
        };
        batter_out.extend_from_slice(runners_out);
        batter_out
    }
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
            OutAtBatType::ReachedOnError => Some(FieldingPlay::try_from(rejoined.as_str())?),
            OutAtBatType::StrikeOut if last.is_empty() => {
                Some(FieldingPlay::conventional_strikeout())
            }
            // The fielder specified after a fielder's choice refers to the fielder making
            // the choice, not necessarily any assist/putout
            OutAtBatType::FieldersChoice => {
                let fp = FieldingPosition::try_from(last).unwrap_or_default();
                Some(FieldingPlay::fielders_choice(fp))
            }
            _ => Some(FieldingPlay::try_from(last)?),
        };
        Ok(Self {
            out_type,
            fielding_play,
        })
    }
}

#[derive(Debug, EnumString, Copy, Clone, Eq, PartialEq, Hash, Serialize, Deserialize)]
pub enum OtherPlateAppearance {
    #[strum(serialize = "C")]
    Interference,
    #[strum(serialize = "HP")]
    HitByPitch,
    #[strum(serialize = "W")]
    Walk,
    #[strum(serialize = "I", serialize = "IW")]
    IntentionalWalk,
}

impl ImplicitPlayResults for OtherPlateAppearance {
    fn implicit_advance(&self) -> Option<RunnerAdvance> {
        Some(RunnerAdvance::batter_advance(Base::First))
    }
}

#[derive(Debug, Clone, Eq, PartialEq, Hash, Serialize, Deserialize)]
pub enum PlateAppearanceType {
    Hit(Hit),
    BattingOut(BattingOut),
    OtherPlateAppearance(OtherPlateAppearance),
}

impl FieldingData for PlateAppearanceType {
    fn fielders_data(&self) -> Vec<FieldersData> {
        if let Self::BattingOut(bo) = self {
            bo.fielding_play
                .as_ref()
                .map_or(vec![], |fp| fp.fielders_data.clone())
        } else {
            vec![]
        }
    }
}

impl PlateAppearanceType {
    pub fn is_strikeout(&self) -> bool {
        if let Self::BattingOut(b) = self {
            b.out_type == OutAtBatType::StrikeOut
        } else {
            false
        }
    }

    pub fn hit_by_pitch(&self) -> bool {
        if let Self::OtherPlateAppearance(op) = self {
            op == &OtherPlateAppearance::HitByPitch
        } else {
            false
        }
    }
    pub fn home_run(&self) -> bool {
        if let Self::Hit(h) = self {
            h.hit_type == HitType::HomeRun
        } else {
            false
        }
    }

    pub const fn is_at_bat(&self) -> bool {
        matches!(self, Self::Hit(_) | Self::BattingOut(_))
    }

    pub const fn is_batted_ball(&self) -> bool {
        matches!(
            self,
            Self::Hit(_)
                | Self::BattingOut(BattingOut {
                    out_type: OutAtBatType::InPlayOut
                        | OutAtBatType::ReachedOnError
                        | OutAtBatType::FieldersChoice,
                    ..
                })
        )
    }

    pub const fn is_fielders_choice(&self) -> bool {
        matches!(
            self,
            Self::BattingOut(BattingOut {
                out_type: OutAtBatType::FieldersChoice,
                ..
            })
        )
    }

    pub const fn reached_on_error(&self) -> bool {
        matches!(
            self,
            Self::BattingOut(BattingOut {
                out_type: OutAtBatType::ReachedOnError,
                ..
            })
        )
    }
}

impl ImplicitPlayResults for PlateAppearanceType {
    fn implicit_advance(&self) -> Option<RunnerAdvance> {
        match self {
            Self::Hit(i) => i.implicit_advance(),
            Self::BattingOut(i) => i.implicit_advance(),
            Self::OtherPlateAppearance(i) => i.implicit_advance(),
        }
    }

    fn implicit_out(&self) -> Vec<BaseRunner> {
        match self {
            Self::Hit(i) => i.implicit_out(),
            Self::BattingOut(i) => i.implicit_out(),
            Self::OtherPlateAppearance(i) => i.implicit_out(),
        }
    }
}

impl TryFrom<(&str, &str)> for PlateAppearanceType {
    type Error = Error;

    fn try_from(value: (&str, &str)) -> Result<Self> {
        if let Ok(batting_out) = BattingOut::try_from(value) {
            Ok(Self::BattingOut(batting_out))
        } else if let Ok(hit) = Hit::try_from(value) {
            Ok(Self::Hit(hit))
        } else if let Ok(pa) = OtherPlateAppearance::from_str(value.0) {
            Ok(Self::OtherPlateAppearance(pa))
        } else {
            bail!("Unable to parse plate appearance")
        }
    }
}

#[derive(Debug, Clone, Eq, PartialEq, Hash, Default)]
pub struct BaserunningFieldingInfo {
    fielders_data: Vec<FieldersData>,
    unearned_run: Option<UnearnedRunStatus>,
}

impl From<Captures<'_>> for BaserunningFieldingInfo {
    fn from(captures: Captures) -> Self {
        let get_capture = {
            |tag: &str| {
                captures
                    .name(tag)
                    .map(|m| FieldingPosition::fielding_vec(m.as_str()))
                    .unwrap_or_default()
            }
        };

        let unearned_run = captures.name("unearned_run").map(|s| {
            if s.as_str().contains('T') {
                UnearnedRunStatus::TeamUnearned
            } else {
                UnearnedRunStatus::Unearned
            }
        });

        let mut fielders_data =
            FieldersData::from_vec(&get_capture("fielders"), FieldingPlayType::Assist);

        if let Some(fp) = get_capture("error").get(0).copied() {
            fielders_data.push(FieldersData::new(fp, FieldingPlayType::Error));
        } else if let Some(fd) = fielders_data.last_mut() {
            fd.fielding_play_type = FieldingPlayType::Putout;
        }
        Self {
            fielders_data,
            unearned_run,
        }
    }
}

#[derive(
    Display,
    Debug,
    EnumString,
    Copy,
    Clone,
    Eq,
    PartialEq,
    Hash,
    IntoPrimitive,
    EnumIter,
    Serialize,
    Deserialize,
    AsRefStr,
)]
#[repr(u8)]
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
    PassedBall,
    #[strum(serialize = "E")]
    AdvancedOnError,
}

#[derive(Debug, Eq, PartialEq, Clone, Hash)]
pub struct BaserunningPlay {
    pub baserunning_play_type: BaserunningPlayType,
    pub at_base: Option<Base>,
    baserunning_fielding_info: Option<BaserunningFieldingInfo>,
}

impl FieldingData for BaserunningPlay {
    fn fielders_data(&self) -> Vec<FieldersData> {
        self.baserunning_fielding_info
            .as_ref()
            .map_or(vec![], |fd| fd.fielders_data.clone())
    }
}

impl BaserunningPlay {
    fn error_on_play(&self) -> bool {
        self.baserunning_fielding_info
            .as_ref()
            .map(|i| FieldersData::find_error(&i.fielders_data).is_some())
            .unwrap_or_default()
    }

    fn is_attempted_stolen_base(&self) -> bool {
        [
            BaserunningPlayType::StolenBase,
            BaserunningPlayType::CaughtStealing,
            BaserunningPlayType::PickedOffCaughtStealing,
        ]
        .contains(&self.baserunning_play_type)
    }

    pub fn baserunner(&self) -> Option<BaseRunner> {
        if self.is_attempted_stolen_base() {
            self.at_base.map(BaseRunner::from_target_base)
        } else {
            self.at_base.map(BaseRunner::from_current_base)
        }
    }
}

impl ImplicitPlayResults for BaserunningPlay {
    fn implicit_advance(&self) -> Option<RunnerAdvance> {
        if let (Some(b), BaserunningPlayType::StolenBase) =
            (self.at_base, self.baserunning_play_type)
        {
            Some(RunnerAdvance::runner_advance_to(b))
        } else if let (true, true, Some(b)) = (
            self.is_attempted_stolen_base(),
            self.error_on_play(),
            self.at_base,
        ) {
            Some(RunnerAdvance::runner_advance_to(b))
        } else {
            None
        }
    }

    fn implicit_out(&self) -> Vec<BaseRunner> {
        if self.error_on_play() {
            return vec![];
        }

        match (self.at_base, self.baserunning_play_type) {
            (
                Some(b),
                BaserunningPlayType::CaughtStealing | BaserunningPlayType::PickedOffCaughtStealing,
            ) => {
                vec![BaseRunner::from_target_base(b)]
            }
            (Some(b), BaserunningPlayType::PickedOff) => {
                vec![BaseRunner::from_current_base(b)]
            }
            _ => vec![],
        }
    }
}

impl TryFrom<&str> for BaserunningPlay {
    type Error = Error;

    fn try_from(value: &str) -> Result<Self> {
        // Errors need to be handled differently than other baserunning plays
        // because the fielding info is located directly on the token
        if value.starts_with('E') {
            let captures = REACHED_ON_ERROR_REGEX
                .captures(value)
                .context("Unable to parse advance on error")?;
            return Ok(Self {
                baserunning_play_type: BaserunningPlayType::AdvancedOnError,
                at_base: None,
                baserunning_fielding_info: Some(BaserunningFieldingInfo::from(captures)),
            });
        }

        let (first, last) = regex_split(value, BASERUNNING_PLAY_FIELDING_REGEX);
        let baserunning_play_type = BaserunningPlayType::from_str(first)?;
        if last.is_none() {
            return Ok(Self {
                baserunning_play_type,
                at_base: None,
                baserunning_fielding_info: None,
            });
        }
        let captures = BASERUNNING_FIELDING_INFO_REGEX
            .captures(last.unwrap_or_default())
            .context("Could not capture info from baserunning play")?;
        let at_base = Some(Base::from_str(
            captures.name("base").map_or("", |m| m.as_str()),
        )?);
        let baserunning_fielding_info = Some(BaserunningFieldingInfo::from(captures));
        Ok(Self {
            baserunning_play_type,
            at_base,
            baserunning_fielding_info,
        })
    }
}

#[derive(Debug, EnumString, Copy, Clone, Eq, PartialEq, Hash)]
pub enum NoPlayType {
    #[strum(serialize = "NP")]
    NoPlay,
    #[strum(serialize = "FLE")]
    ErrorOnFoul,
}

impl ImplicitPlayResults for NoPlayType {}

#[derive(Debug, Clone, Eq, PartialEq, Hash)]
pub struct NoPlay {
    no_play_type: NoPlayType,
    error: Option<FieldingPosition>,
}

impl TryFrom<(&str, &str)> for NoPlay {
    type Error = Error;

    fn try_from(value: (&str, &str)) -> Result<Self> {
        let (first, last) = value;
        let no_play_type = NoPlayType::from_str(first)?;
        match no_play_type {
            NoPlayType::NoPlay => Ok(Self {
                no_play_type,
                error: None,
            }),
            NoPlayType::ErrorOnFoul => Ok(Self {
                no_play_type,
                error: FieldingPosition::fielding_vec(last).get(0).copied(),
            }),
        }
    }
}

impl FieldingData for NoPlay {
    fn fielders_data(&self) -> Vec<FieldersData> {
        self.error.map_or(vec![], |e| {
            vec![FieldersData::new(e, FieldingPlayType::Error)]
        })
    }
}

#[derive(Debug, Eq, PartialEq, Clone, Hash)]
pub enum PlayType {
    PlateAppearance(PlateAppearanceType),
    BaserunningPlay(BaserunningPlay),
    NoPlay(NoPlay),
}

impl PlayType {
    pub fn no_play(&self) -> bool {
        if let Self::NoPlay(np) = self {
            np.no_play_type == NoPlayType::NoPlay
        } else {
            false
        }
    }

    pub fn passed_ball(&self) -> bool {
        match self {
            Self::BaserunningPlay(bp) => {
                bp.baserunning_play_type == BaserunningPlayType::PassedBall
            }
            _ => false,
        }
    }
    pub fn wild_pitch(&self) -> bool {
        match self {
            Self::BaserunningPlay(bp) => bp.baserunning_play_type == BaserunningPlayType::WildPitch,
            _ => false,
        }
    }

    pub fn balk(&self) -> bool {
        match self {
            Self::BaserunningPlay(bp) => bp.baserunning_play_type == BaserunningPlayType::Balk,
            _ => false,
        }
    }

    pub fn hit_by_pitch(&self) -> bool {
        match self {
            Self::PlateAppearance(pt) => pt.hit_by_pitch(),
            _ => false,
        }
    }

    pub fn home_run(&self) -> bool {
        match self {
            Self::PlateAppearance(pt) => pt.home_run(),
            _ => false,
        }
    }

    pub fn reached_on_error(&self) -> bool {
        match self {
            Self::PlateAppearance(pt) => pt.reached_on_error(),
            _ => false,
        }
    }
}

impl FieldingData for PlayType {
    fn fielders_data(&self) -> Vec<FieldersData> {
        match self {
            Self::PlateAppearance(p) => p.fielders_data(),
            Self::BaserunningPlay(p) => p.fielders_data(),
            Self::NoPlay(p) => p.fielders_data(),
        }
    }
}

impl ImplicitPlayResults for PlayType {
    fn implicit_advance(&self) -> Option<RunnerAdvance> {
        match self {
            Self::PlateAppearance(p) => p.implicit_advance(),
            Self::BaserunningPlay(p) => p.implicit_advance(),
            Self::NoPlay(_) => None,
        }
    }

    fn implicit_out(&self) -> Vec<BaseRunner> {
        match self {
            Self::PlateAppearance(p) => p.implicit_out(),
            Self::BaserunningPlay(p) => p.implicit_out(),
            Self::NoPlay(_) => vec![],
        }
    }
}

impl PlayType {
    pub fn is_rbi_eligible(&self) -> bool {
        match self {
            Self::PlateAppearance(p) => !p.is_strikeout(),
            _ => false,
        }
    }

    fn parse_main_play(value: &str, is_extra_play: bool) -> Result<Vec<Self>> {
        if value.is_empty() {
            return Ok(vec![]);
        }
        if MULTI_PLAY_REGEX.is_match(value) {
            let (first, last) = regex_split(value, MULTI_PLAY_REGEX);
            return Ok(Self::parse_main_play(first, false)?
                .into_iter()
                .chain(
                    Self::parse_main_play(
                        last.unwrap_or_default().get(1..).unwrap_or_default(),
                        true,
                    )?
                    .into_iter(),
                )
                .collect::<Vec<Self>>());
        }
        let (first, last) = regex_split(value, MAIN_PLAY_FIELDING_REGEX);
        let str_tuple = (first, last.unwrap_or_default());
        // Extra plays cannot be plate appearances and will produce false matches in some cases,
        // so we need to check for that in addition to the regex match.
        if let (false, Ok(pa)) = (is_extra_play, PlateAppearanceType::try_from(str_tuple)) {
            Ok(vec![Self::PlateAppearance(pa)])
        } else if let Ok(br) = BaserunningPlay::try_from(value) {
            Ok(vec![Self::BaserunningPlay(br)])
        } else if let Ok(np) = NoPlay::try_from(str_tuple) {
            Ok(vec![Self::NoPlay(np)])
        } else {
            bail!("Unable to parse play: {value}")
        }
    }
}

#[derive(Debug, PartialEq, Eq, Clone, Hash)]
pub struct RunnerAdvance {
    pub baserunner: BaseRunner,
    pub to: Base,
    pub out_or_error: bool,
    pub modifiers: Vec<RunnerAdvanceModifier>,
}

impl FieldingData for RunnerAdvance {
    fn fielders_data(&self) -> Vec<FieldersData> {
        self.modifiers
            .iter()
            .flat_map(RunnerAdvanceModifier::fielders_data)
            .collect()
    }
}

impl RunnerAdvance {
    pub const fn batter_advance(to: Base) -> Self {
        Self {
            baserunner: BaseRunner::Batter,
            to,
            out_or_error: false,
            modifiers: vec![],
        }
    }

    pub const fn runner_advance_to(target_base: Base) -> Self {
        let baserunner = BaseRunner::from_target_base(target_base);
        Self {
            baserunner,
            to: target_base,
            out_or_error: false,
            modifiers: vec![],
        }
    }

    pub fn is_out(&self) -> bool {
        // In rare cases, a single advance can encompass both an error and a subsequent putout
        !FieldersData::putouts(&self.fielders_data()).is_empty()
    }

    pub fn scored(&self) -> bool {
        self.to == Base::Home && !self.is_out()
    }

    pub fn advanced_on_error(&self) -> bool {
        self.fielders_data()
            .iter()
            .any(|fd| fd.fielding_play_type == FieldingPlayType::Error)
    }

    pub fn is_this_that_one_time_jean_segura_ran_in_reverse(&self) -> bool {
        BaseRunner::from_target_base(self.to) < self.baserunner
    }

    /// When a run scores, whether or not it counts as an RBI for the batter cannot be determined
    /// from the `RunnerAdvance` data alone *unless it is explicitly given*. For instance, an non-annotated
    /// run-scoring play on a force out is usually an RBI, but if a DP modifier is present, then
    /// no RBI is awarded. As a result, the final RBI logic determination can't occur here.
    /// level.
    pub fn explicit_rbi_status(&self) -> Option<RbiStatus> {
        self.modifiers
            .iter()
            .find_map(RunnerAdvanceModifier::rbi_status)
    }

    /// BEVENT/Chadwick do not determine earned/unearned run
    /// status on a given play unless it is specified explicitly.
    /// We won't make an attempt in this library, but it should be possible
    /// to make some (occasionally arbitrary) determination downstream
    /// through deduction.
    pub fn unearned_run_status(&self) -> Option<UnearnedRunStatus> {
        self.modifiers
            .iter()
            .find_map(RunnerAdvanceModifier::unearned_status)
    }

    fn parse_advances(value: &str) -> Result<Vec<Self>> {
        value
            .split(';')
            .filter_map(|s| ADVANCE_REGEX.captures(s))
            .map(|c| Self::parse_single_advance(&c))
            .collect::<Result<Vec<Self>>>()
    }

    fn parse_single_advance(captures: &Captures) -> Result<Self> {
        let (from_match, to_match, out_at_match, mods) = (
            captures.name("from"),
            captures.name("to"),
            captures.name("out_at"),
            captures.name("mods"),
        );
        let baserunner = BaseRunner::from_str(
            from_match
                .map(|s| s.as_str())
                .context("Missing baserunner in advance")?,
        )?;
        let to = Base::from_str(
            to_match
                .or(out_at_match)
                .map(|s| s.as_str())
                .context("Missing destination base in advance")?,
        )?;
        let out_or_error = out_at_match.is_some();
        let modifiers = mods.map_or(Vec::new(), |m| {
            RunnerAdvanceModifier::parse_advance_modifiers(m.as_str())
        });
        Ok(Self {
            baserunner,
            to,
            out_or_error,
            modifiers,
        })
    }
}

#[derive(Debug, PartialEq, Eq, EnumDiscriminants, Clone, Hash)]
pub enum RunnerAdvanceModifier {
    UnearnedRun,
    TeamUnearnedRun,
    NoRbi,
    Interference(FieldingPosition),
    Rbi,
    PassedBall,
    WildPitch,
    AdvancedOnThrowTo(Option<Base>),
    AdvancedOnError {
        assists: PositionVec,
        error: FieldingPosition,
    },
    Putout {
        assists: PositionVec,
        putout: FieldingPosition,
    },
    Unrecognized(String),
}

impl RunnerAdvanceModifier {
    const fn unearned_status(&self) -> Option<UnearnedRunStatus> {
        match self {
            Self::UnearnedRun => Some(UnearnedRunStatus::Unearned),
            Self::TeamUnearnedRun => Some(UnearnedRunStatus::TeamUnearned),
            _ => None,
        }
    }

    const fn rbi_status(&self) -> Option<RbiStatus> {
        match self {
            Self::Rbi => Some(RbiStatus::Rbi),
            Self::NoRbi => Some(RbiStatus::NoRbi),
            _ => None,
        }
    }
}

impl FieldingData for RunnerAdvanceModifier {
    fn fielders_data(&self) -> Vec<FieldersData> {
        match self {
            Self::Putout { putout, assists } => [
                vec![FieldersData::new(*putout, FieldingPlayType::Putout)],
                FieldersData::from_vec(assists, FieldingPlayType::Assist),
            ]
            .concat(),
            Self::AdvancedOnError { assists, error } => [
                FieldersData::from_vec(assists, FieldingPlayType::Assist),
                vec![FieldersData::new(*error, FieldingPlayType::Error)],
            ]
            .concat(),
            _ => vec![],
        }
    }
}

impl RunnerAdvanceModifier {
    fn parse_advance_modifiers(value: &str) -> Vec<Self> {
        value
            .split(')')
            .filter(|s| !s.is_empty())
            .map(Self::parse_single_advance_modifier)
            .collect()
    }

    fn parse_single_advance_modifier(value: &str) -> Self {
        let simple_match = match value {
            "(UR" => Self::UnearnedRun,
            "(TUR" => Self::TeamUnearnedRun,
            "(NR" | "(NORBI" => Self::NoRbi,
            "(RBI" => Self::Rbi,
            "(PB" => Self::PassedBall,
            "(WP" => Self::WildPitch,
            "(THH" => Self::AdvancedOnThrowTo(Some(Base::Home)),
            "(TH" => Self::AdvancedOnThrowTo(None),
            "(" => Self::Putout {
                assists: vec![],
                putout: FieldingPosition::Unknown,
            },
            _ => Self::Unrecognized(value.into()),
        };
        match simple_match {
            Self::Unrecognized(_) => (),
            _ => return simple_match,
        };
        let (first, last) = regex_split(value, NUMERIC_REGEX);
        let last = last.unwrap_or_default();
        let last_as_int_vec: PositionVec = FieldingPosition::fielding_vec(last);
        let final_match = match first {
            "(INT" => Self::Interference(
                last_as_int_vec
                    .first()
                    .copied()
                    .unwrap_or(FieldingPosition::Unknown),
            ),
            "(TH" => Self::AdvancedOnThrowTo(Base::from_str(last).ok()),
            "(E" => Self::AdvancedOnError {
                assists: Vec::new(),
                error: FieldingPosition::try_from(last.get(0..1).unwrap_or_default())
                    .unwrap_or(FieldingPosition::Unknown),
            },
            "(" if last.contains('E') => {
                let (assist_str, error_str) = last.split_at(last.find('E').unwrap_or_default());
                let (assists, error) = (
                    FieldingPosition::fielding_vec(assist_str),
                    FieldingPosition::fielding_vec(error_str)
                        .first()
                        .copied()
                        .unwrap_or(FieldingPosition::Unknown),
                );
                Self::AdvancedOnError { assists, error }
            }
            "(" => {
                let mut digits = FieldingPosition::fielding_vec(last);
                let (putout, assists) = (digits.pop().unwrap_or(FieldingPosition::Unknown), digits);
                Self::Putout { assists, putout }
            }
            _ => Self::Unrecognized(value.into()),
        };
        final_match
    }
}

#[derive(
    Debug,
    Eq,
    PartialEq,
    Ord,
    PartialOrd,
    EnumString,
    Copy,
    Clone,
    Hash,
    Serialize,
    Deserialize,
    AsRefStr,
)]
pub enum HitStrength {
    #[strum(serialize = "+")]
    Hard,
    #[strum(serialize = "-")]
    Soft,
    Unknown,
}

impl Default for HitStrength {
    fn default() -> Self {
        Self::Unknown
    }
}

#[derive(
    Debug,
    Eq,
    PartialEq,
    Ord,
    PartialOrd,
    EnumString,
    Copy,
    Clone,
    Hash,
    Serialize,
    Deserialize,
    AsRefStr,
)]
pub enum HitDepth {
    #[strum(serialize = "S")]
    Shallow,
    #[strum(serialize = "D")]
    Deep,
    #[strum(serialize = "XD")]
    ExtraDeep,
    Unknown,
}

impl Default for HitDepth {
    fn default() -> Self {
        Self::Unknown
    }
}

#[derive(
    Debug,
    Eq,
    PartialEq,
    Ord,
    PartialOrd,
    EnumString,
    Copy,
    Clone,
    Hash,
    Serialize,
    Deserialize,
    AsRefStr,
)]
pub enum HitAngle {
    #[strum(serialize = "F")]
    Foul,
    #[strum(serialize = "M")]
    Middle,
    #[strum(serialize = "L")]
    FoulLine,
    // "L" is usually used for foul line, but for CF it means towards the left
    Left,
    #[strum(serialize = "R")]
    Right,
    Unknown,
}

impl Default for HitAngle {
    fn default() -> Self {
        Self::Unknown
    }
}

#[derive(
    Debug,
    Ord,
    PartialOrd,
    Eq,
    PartialEq,
    EnumString,
    Copy,
    Clone,
    Hash,
    Serialize,
    Deserialize,
    AsRefStr,
)]
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
    Unknown,
}

impl Default for HitLocationGeneral {
    fn default() -> Self {
        Self::Unknown
    }
}

impl HitLocationGeneral {
    const fn is_middle_position(self) -> bool {
        matches!(self, Self::Catcher | Self::Center)
    }
}

#[derive(Debug, Eq, PartialEq, Ord, PartialOrd, Copy, Clone, Hash, Serialize, Deserialize)]
pub struct HitLocation {
    pub general_location: HitLocationGeneral,
    pub depth: HitDepth,
    pub angle: HitAngle,
    pub strength: HitStrength,
}

impl Default for HitLocation {
    fn default() -> Self {
        Self {
            general_location: HitLocationGeneral::Unknown,
            depth: HitDepth::Unknown,
            angle: HitAngle::Unknown,
            strength: HitStrength::Unknown,
        }
    }
}

impl TryFrom<&str> for HitLocation {
    type Error = Error;

    fn try_from(value: &str) -> Result<Self> {
        let as_str = { |re: &Regex| re.find(value).map_or("", |m| m.as_str()) };
        // If there's no general location found, that's unexpected behavior and
        // we should short-circuit, but other missing info is expected
        let general_location = HitLocationGeneral::from_str(as_str(HIT_LOCATION_GENERAL_REGEX))?;
        // "L" is usually used for foul line, but for CF and C it means towards the left
        let angle = if general_location.is_middle_position() && value.contains('L') {
            HitAngle::Left
        } else {
            HitAngle::from_str(as_str(HIT_LOCATION_ANGLE_REGEX)).unwrap_or_default()
        };
        let depth = HitDepth::from_str(as_str(HIT_LOCATION_DEPTH_REGEX)).unwrap_or_default();
        let strength =
            HitStrength::from_str(as_str(HIT_LOCATION_STRENGTH_REGEX)).unwrap_or_default();
        Ok(Self {
            general_location,
            depth,
            angle,
            strength,
        })
    }
}

#[derive(
    Debug, Eq, PartialEq, Ord, PartialOrd, Copy, Clone, Hash, Serialize, Deserialize, Default,
)]
pub struct ContactDescription {
    pub contact_type: Option<ContactType>,
    pub location: Option<HitLocation>,
}

impl TryFrom<(&str, &str)> for ContactDescription {
    type Error = Error;

    fn try_from(tup: (&str, &str)) -> Result<Self> {
        let (contact, loc) = tup;
        let contact_type = ContactType::from_str(contact).ok();
        let location = HitLocation::try_from(loc).ok();
        if contact_type.is_none() && location.is_none() {
            bail!("Contact description should have at least one of contact type or location, but neither were found")
        }
        Ok(Self {
            contact_type,
            location,
        })
    }
}

#[derive(
    Debug,
    Ord,
    PartialOrd,
    Eq,
    PartialEq,
    Copy,
    Clone,
    EnumString,
    Hash,
    Serialize,
    Deserialize,
    AsRefStr,
)]
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
    Unknown,
    NoContact,
}

impl Default for ContactType {
    fn default() -> Self {
        Self::Unknown
    }
}

#[derive(Debug, Eq, PartialEq, Ord, PartialOrd, EnumString, Clone, Hash, Display, Serialize)]
pub enum PlayModifier {
    ContactDescription(ContactDescription),
    #[strum(serialize = "AP")]
    AppealPlay,
    #[strum(serialize = "BGDP")]
    BuntGroundIntoDoublePlay,
    #[strum(serialize = "BINT")]
    BatterInterference,
    #[strum(serialize = "BOOT")]
    BatingOutOfTurn,
    #[strum(serialize = "BPDP")]
    BuntPoppedIntoDoublePlay,
    #[strum(serialize = "BR")]
    RunnerHitByBattedBall,
    #[strum(serialize = "C")]
    CalledThirdStrike,
    #[strum(serialize = "COUB")]
    CourtesyBatter,
    #[strum(serialize = "COUF")]
    CourtesyFielder,
    #[strum(serialize = "COUR")]
    CourtesyRunner,
    #[strum(serialize = "DP")]
    UnspecifiedDoublePlay,
    #[strum(serialize = "E")]
    ErrorOn(FieldingPosition),
    #[strum(serialize = "FDP")]
    FlyBallDoublePlay,
    #[strum(serialize = "FINT")]
    FanInterference,
    #[strum(serialize = "FL")]
    Foul,
    #[strum(serialize = "FO")]
    ForceOut,
    #[strum(serialize = "GDP")]
    GroundBallDoublePlay,
    #[strum(serialize = "GTP")]
    GroundBallTriplePlay,
    #[strum(serialize = "IF")]
    InfieldFlyRule,
    #[strum(serialize = "INT")]
    Interference,
    #[strum(serialize = "IPHR")]
    InsideTheParkHomeRun,
    #[strum(serialize = "LDP")]
    LinedIntoDoublePlay,
    #[strum(serialize = "LTP")]
    LinedIntoTriplePlay,
    #[strum(serialize = "MREV")]
    ManageChallengeOfCallOnField,
    #[strum(serialize = "NDP")]
    NoDoublePlayCredited,
    #[strum(serialize = "OBS")]
    Obstruction,
    #[strum(serialize = "PASS")]
    RunnerOutPassingAnotherRunner,
    #[strum(serialize = "R")]
    RelayToFielderWithNoOutMade(PositionVec),
    #[strum(serialize = "RINT")]
    RunnerInterference,
    #[strum(serialize = "S")]
    SwingingThirdStrike,
    #[strum(serialize = "SF")]
    SacrificeFly,
    #[strum(serialize = "SH")]
    SacrificeHit,
    #[strum(serialize = "TH", serialize = "TH)", serialize = "THH")]
    ThrowToBase(Option<Base>),
    #[strum(serialize = "TP")]
    UnspecifiedTriplePlay,
    #[strum(serialize = "UINT")]
    UmpireInterference,
    #[strum(serialize = "UREV")]
    UmpireReviewOfCallOnField,
    #[strum(serialize = "U")]
    Unknown,
    Unrecognized(String),
}

impl FieldingData for PlayModifier {
    // No putout or assist data in modifiers
    fn fielders_data(&self) -> Vec<FieldersData> {
        if let Self::ErrorOn(p) = self {
            vec![FieldersData::new(*p, FieldingPlayType::Error)]
        } else {
            vec![]
        }
    }
}

impl PlayModifier {
    /// Determines whether the modifier should be included in the event type output
    /// For now this is everything except `ContactDescription`
    pub fn is_valid_event_type(&self) -> bool {
        let dummy = &Self::ContactDescription(ContactDescription::default());
        discriminant(dummy) != discriminant(self)
    }

    const fn double_plays() -> [Self; 6] {
        [
            Self::BuntGroundIntoDoublePlay,
            Self::BuntPoppedIntoDoublePlay,
            Self::FlyBallDoublePlay,
            Self::GroundBallDoublePlay,
            Self::LinedIntoDoublePlay,
            Self::UnspecifiedDoublePlay,
        ]
    }

    const fn triple_plays() -> [Self; 3] {
        [
            Self::GroundBallTriplePlay,
            Self::LinedIntoTriplePlay,
            Self::UnspecifiedTriplePlay,
        ]
    }

    fn multi_out_play(&self) -> Option<usize> {
        if Self::double_plays().contains(self) {
            Some(2)
        } else if Self::triple_plays().contains(self) {
            Some(3)
        } else {
            None
        }
    }

    fn parse_modifiers(value: &str) -> Result<Vec<Self>> {
        value
            .split('/')
            .filter(|s| !s.is_empty())
            .map(Self::parse_single_modifier)
            .collect::<Result<Vec<Self>>>()
    }

    fn parse_single_modifier(value: &str) -> Result<Self> {
        let (first, last) = regex_split(value, MODIFIER_DIVIDER_REGEX);
        if let Ok(cd) = ContactDescription::try_from((first, last.unwrap_or_default())) {
            return Ok(Self::ContactDescription(cd));
        }
        let last_as_int_vec = { || FieldingPosition::fielding_vec(last.unwrap_or_default()) };
        let play_modifier = match Self::from_str(first) {
            // Fill in other variants that have non-default cases
            Ok(Self::ErrorOn(_)) => Self::ErrorOn(
                *last_as_int_vec()
                    .first()
                    .context("Missing error position info")?,
            ),
            Ok(Self::RelayToFielderWithNoOutMade(_)) => {
                Self::RelayToFielderWithNoOutMade(last_as_int_vec())
            }
            Ok(Self::ThrowToBase(_)) if first == "THH" => Self::ThrowToBase(Some(Base::Home)),
            Ok(Self::ThrowToBase(_)) => {
                Self::ThrowToBase(Base::from_str(last.unwrap_or_default()).ok())
            }
            Ok(pm) => pm,
            Err(_) => Self::Unrecognized(value.into()),
        };
        Ok(play_modifier)
    }
}

#[derive(
    Debug, Default, Eq, PartialEq, Copy, Clone, Hash, Ord, PartialOrd, Serialize, Deserialize,
)]
pub struct Count {
    pub balls: Option<Balls>,
    pub strikes: Option<Strikes>,
}

impl Count {
    #[allow(clippy::cast_possible_truncation)]
    fn new(count_str: &str) -> Self {
        let mut ints = count_str.chars().map(|c| c.to_digit(10));

        Self {
            balls: ints.next().flatten().and_then(|b| Balls::new(b as u8)),
            strikes: ints.next().flatten().and_then(|s| Strikes::new(s as u8)),
        }
    }

    // One of the more annoying scorekeeping rules is that if a batter or pitcher is removed mid-count,
    // the substituted player can still be credited/charged with the at-bat result, and the
    // logic for that depends on both the count and the result.
    pub fn is_old_pitcher_responsible_walk(&self) -> bool {
        if let (Some(b), Some(s)) = (self.balls, self.strikes) {
            b == 3 || (b == 2 && s <= 1)
        } else {
            // If we have no count info we just assume the new player is responsible
            false
        }
    }

    pub fn is_old_batter_responsible_strikeout(&self) -> bool {
        self.strikes.map(|s| s == 2).unwrap_or_default()
    }

    /// Whether the count has > 0 balls + strikes.
    pub fn has_any_pitches(&self) -> bool {
        let balls: usize = self.balls.map(Into::into).unwrap_or_default();
        let strikes: usize = self.strikes.map(Into::into).unwrap_or_default();
        balls + strikes > 0
    }
}

#[derive(Debug, Eq, PartialEq, Clone)]
pub struct PlayRecord {
    pub inning: Inning,
    pub batting_side: Side,
    pub batter: Batter,
    pub count: Count,
    pub pitch_sequence: Arc<PitchSequence>,
    pub parsed: Arc<ParsedPlay>,
    pub stats: Arc<PlayStats>,
}

impl PlayRecord {
    fn store_parsed_play(raw_play: &str) -> Result<(Arc<ParsedPlay>, Arc<PlayStats>)> {
        let parsed_play = PARSED_PLAY_CACHE.get(raw_play).map_or_else(
            || {
                let parsed = Arc::new(ParsedPlay::try_from(raw_play)?);
                PARSED_PLAY_CACHE.insert(raw_play.to_string(), parsed.clone());
                Ok::<Arc<ParsedPlay>, Error>(parsed)
            },
            Ok,
        )?;
        let stats = PLAY_STATS_CACHE.get(raw_play).map_or_else(
            || {
                let stats = Arc::new(PlayStats::try_from(parsed_play.as_ref())?);
                PLAY_STATS_CACHE.insert(raw_play.to_string(), stats.clone());
                Ok::<Arc<PlayStats>, Error>(stats)
            },
            Ok,
        )?;
        Ok((parsed_play, stats))
    }

    fn get_pitch_sequence(sequence: &str) -> Result<Arc<PitchSequence>> {
        PITCH_SEQUENCE_CACHE.get(sequence).map_or_else(
            || {
                let ps = Arc::new(PitchSequenceItem::new_pitch_sequence(sequence)?);
                PITCH_SEQUENCE_CACHE.insert(sequence.into(), ps.clone());
                Ok(ps)
            },
            Ok,
        )
    }
}

impl TryFrom<&RetrosheetEventRecord> for PlayRecord {
    type Error = Error;

    fn try_from(record: &RetrosheetEventRecord) -> Result<Self> {
        let record = record.deserialize::<[&str; 7]>(None)?;
        let (parsed, stats) = Self::store_parsed_play(record[6])?;

        Ok(Self {
            inning: record[1].parse::<Inning>()?,
            batting_side: Side::from_str(record[2])?,
            batter: str_to_tinystr(record[3])?,
            count: Count::new(record[4]),
            pitch_sequence: {
                match record[5] {
                    "" => Arc::new(PitchSequence::default()),
                    s => Self::get_pitch_sequence(s)?,
                }
            },
            parsed,
            stats,
        })
    }
}

#[derive(Debug, Eq, PartialEq, Default, Clone, Hash)]
pub struct ParsedPlay {
    pub main_plays: Arc<Vec<PlayType>>,
    pub modifiers: Arc<Vec<PlayModifier>>,
    pub explicit_advances: Arc<Vec<RunnerAdvance>>,
}

impl ParsedPlay {
    pub fn no_play(&self) -> bool {
        self.main_plays.iter().all(PlayType::no_play)
    }

    fn explicit_baserunners(&self) -> Box<dyn Iterator<Item = BaseRunner> + '_> {
        Box::from(self.explicit_advances.iter().map(|ra| ra.baserunner))
    }

    pub fn stolen_base_plays(&self) -> Vec<&BaserunningPlay> {
        self.main_plays
            .iter()
            .filter_map(|pt| {
                if let PlayType::BaserunningPlay(br) = pt {
                    Some(br)
                } else {
                    None
                }
            })
            .filter(|br| br.is_attempted_stolen_base())
            .collect()
    }

    fn implicit_outs(&self) -> Box<dyn Iterator<Item = BaseRunner> + '_> {
        Box::from(self.main_plays.iter().flat_map(PlayType::implicit_out))
    }

    pub fn advances(&self) -> Box<dyn Iterator<Item = RunnerAdvance> + '_> {
        let cleaned_advances = self
            .explicit_advances
            .iter()
            // Occasionally there is a redundant piece of info like "3-3" that screws stuff up
            // "3X3" is OK, seems to refer to getting doubled off the bag rather than trying to advance
            .filter(|ra| Into::<u8>::into(ra.to) != Into::<u8>::into(ra.baserunner) || ra.is_out())
            .cloned();
        // If a baserunner is already explicitly represented in `advances`, or is implicitly out on another main play, don't include the implicit advance
        let implicit_advances = self
            .main_plays
            .iter()
            .filter_map(move |pt| {
                pt.implicit_advance().map(|ra| {
                    if self
                        .implicit_outs()
                        .chain(self.explicit_baserunners())
                        .any(|br| br == ra.baserunner)
                    {
                        None
                    } else {
                        Some(ra)
                    }
                })
            })
            .flatten();
        Box::from(cleaned_advances.chain(implicit_advances))
    }

    fn filtered_baserunners(
        &self,
        filter: fn(&RunnerAdvance) -> bool,
    ) -> Box<dyn Iterator<Item = BaseRunner> + '_> {
        Box::from(self.advances().filter_map(move |ra| {
            if filter(&ra) {
                Some(ra.baserunner)
            } else {
                None
            }
        }))
    }

    pub fn outs(&self) -> Result<Vec<BaseRunner>> {
        let (out_advancing, safe_advancing): (Vec<RunnerAdvance>, Vec<RunnerAdvance>) =
            self.advances().partition(RunnerAdvance::is_out);

        let implicit_outs = self
            .implicit_outs()
            .filter(|br| safe_advancing.iter().all(|ra| ra.baserunner != *br));
        let full_outs = Vec::from_iter(
            implicit_outs
                .chain(out_advancing.iter().map(|ra| ra.baserunner))
                .collect::<HashSet<BaseRunner>>(),
        );

        let extra_outs = self.modifiers.iter().find_map(PlayModifier::multi_out_play);
        if let Some(o) = extra_outs {
            if o > full_outs.len() {
                if full_outs.contains(&BaseRunner::Batter) {
                    bail!("Double play indicated, but cannot be resolved")
                }
                Ok([full_outs, vec![BaseRunner::Batter]].concat())
            } else {
                Ok(full_outs)
            }
        } else {
            Ok(full_outs)
        }
    }

    /// How to ID for our purposes:
    /// 1) Any non-batter outs included in the main play, e.g. 64(1)
    /// 2) The first baserunning-advance out on a fielders choice, e.g. FC1.2X3
    /// These are mutually exclusive, as FCs will not include outs in the main play
    /// Note: the overwhelming majority of these are forceouts, but a good number of
    /// fielders-choice outs are tags (otherwise they probably would have been
    /// specified as a forceout).
    pub fn batter_caused_baserunning_outs(&self) -> Vec<BaseRunner> {
        let first_failed_advance = self
            .explicit_advances
            .iter()
            .find(|ra| ra.is_out())
            .map(|ra| ra.baserunner);
        // Make it a Set so Batter is easy to remove
        let mut out_set: Set<BaseRunner> = match self.plate_appearance() {
            Some(pa) if pa.is_fielders_choice() => first_failed_advance.into_iter().collect(),
            Some(pa) => pa.implicit_out().into_iter().collect(),
            None => Set::new(),
        };
        out_set.remove(BaseRunner::Batter);
        out_set.into_iter().collect()
    }

    pub fn runs(&self) -> Vec<BaseRunner> {
        self.filtered_baserunners(RunnerAdvance::scored).collect()
    }

    pub fn team_unearned_runs(&self) -> Vec<BaseRunner> {
        self.filtered_baserunners(|ra: &RunnerAdvance| {
            ra.scored() && ra.unearned_run_status() == Some(UnearnedRunStatus::TeamUnearned)
        })
        .collect()
    }

    pub fn is_gidp(&self) -> bool {
        self.modifiers.iter().any(|m| {
            [
                PlayModifier::GroundBallDoublePlay,
                PlayModifier::BuntGroundIntoDoublePlay,
            ]
            .contains(m)
        })
    }

    // Determines whether runs that score on the play should be RBIs unless explicitly stated otherwise,
    // or non-RBIs unless explicitly stated otherwise.
    fn default_rbi_status(&self) -> RbiStatus {
        let has_rbi_eligible_play = self.main_plays.iter().any(PlayType::is_rbi_eligible);
        if has_rbi_eligible_play && !self.is_gidp() {
            RbiStatus::Rbi
        } else {
            RbiStatus::NoRbi
        }
    }

    // There is still some additional filtering work to do
    // that requires knowledge of how many outs are at the start of the play
    // TODO: Either add outs-at-start info to plays or talk to Retrosheet about changing
    pub fn rbi(&self) -> Vec<BaseRunner> {
        self.advances()
            .filter(|ra| match self.default_rbi_status() {
                RbiStatus::Rbi => {
                    ra.scored()
                        && ra.explicit_rbi_status() != Some(RbiStatus::NoRbi)
                        && !ra.advanced_on_error()
                        && (ra.baserunner == BaseRunner::Third || !self.reached_on_error())
                }
                RbiStatus::NoRbi => ra.scored() && ra.explicit_rbi_status() == Some(RbiStatus::Rbi),
            })
            .map(|ra| ra.baserunner)
            .collect_vec()
    }

    pub fn passed_ball(&self) -> bool {
        self.main_plays.iter().any(PlayType::passed_ball)
    }

    pub fn wild_pitch(&self) -> bool {
        self.main_plays.iter().any(PlayType::wild_pitch)
    }

    pub fn balk(&self) -> bool {
        self.main_plays.iter().any(PlayType::balk)
    }

    pub fn sacrifice_hit(&self) -> bool {
        self.modifiers
            .iter()
            .any(|pm| pm == &PlayModifier::SacrificeHit)
    }

    pub fn sacrifice_fly(&self) -> bool {
        self.modifiers
            .iter()
            .any(|pm| pm == &PlayModifier::SacrificeFly)
    }

    pub fn hit_by_pitch(&self) -> bool {
        self.main_plays.iter().any(PlayType::hit_by_pitch)
    }

    pub fn home_run(&self) -> bool {
        self.main_plays.iter().any(PlayType::home_run)
    }

    pub fn reached_on_error(&self) -> bool {
        self.main_plays.iter().any(PlayType::reached_on_error)
    }

    pub fn plate_appearance(&self) -> Option<&PlateAppearanceType> {
        self.main_plays.iter().find_map(|pt| {
            if let PlayType::PlateAppearance(pa) = pt {
                Some(pa)
            } else {
                None
            }
        })
    }

    pub fn contact_description(&self) -> Option<ContactDescription> {
        // Contact type and location be located in either the same modifier
        // or separate modifiers. If separate, they need to be combined.
        let contact_type = self.modifiers.iter().find_map(|pm| {
            if let PlayModifier::ContactDescription(cd) = pm {
                cd.contact_type
            } else {
                None
            }
        });
        let location = self.modifiers.iter().find_map(|pm| {
            if let PlayModifier::ContactDescription(cd) = pm {
                cd.location
            } else {
                None
            }
        });
        if contact_type.is_some() || location.is_some() {
            Some(ContactDescription {
                contact_type,
                location,
            })
        } else {
            None
        }
    }

    // Primary fielder of a ball in play. For outs, this is the first fielder in the play string.
    // For hits, this is the first fielder after the hit type indicator, e.g. the `8` in `S8`.
    // This data point is particularly important as it's very well populated historically and
    // serves as a good fallback for hit location, which is usually not present.
    // Some hit strings clearly indicate a deflection e.g. `S17`, but others may be
    // an irregular recording of a hit location, e.g. `S48` to mean shallow center.
    // We take the first fielder regardless, but may be worth another look.
    // TODO: Investigate possible irregular hit location storage
    pub fn hit_to_fielder(&self) -> Option<FieldingPosition> {
        self.main_plays.iter().find_map(|pt| match pt {
            PlayType::PlateAppearance(PlateAppearanceType::Hit(h)) => {
                h.positions_hit_to.get(0).copied()
            }
            PlayType::PlateAppearance(PlateAppearanceType::BattingOut(bo))
                if bo.out_type != OutAtBatType::StrikeOut =>
            {
                bo.fielding_play
                    .as_ref()
                    .and_then(|fp| fp.fielders_data.get(0).map(|fd| fd.fielding_position))
            }
            _ => None,
        })
    }
}

impl FieldingData for ParsedPlay {
    fn fielders_data(&self) -> Vec<FieldersData> {
        self.main_plays
            .iter()
            .flat_map(FieldingData::fielders_data)
            .chain(self.modifiers.iter().flat_map(PlayModifier::fielders_data))
            .chain(
                self.explicit_advances
                    .iter()
                    .flat_map(RunnerAdvance::fielders_data),
            )
            .collect()
    }
}

impl TryFrom<&str> for ParsedPlay {
    type Error = Error;

    fn try_from(raw_play: &str) -> Result<Self> {
        let value = &*STRIP_CHARS_REGEX.replace_all(raw_play, "");
        if value.is_empty() {
            return Ok(Self::default());
        }
        let value = &*UNKNOWN_FIELDER_REGEX.replace_all(value, "0");

        let modifiers_boundary = value.find('/').unwrap_or(value.len());
        let advances_boundary = value.find('.').unwrap_or(value.len());
        let first_boundary = min(modifiers_boundary, advances_boundary);

        let main_play_raw = &value[..first_boundary];

        let main_plays = if let Some(pt) = MAIN_PLAY_CACHE.get(main_play_raw) {
            pt
        } else {
            let pt = Arc::new(PlayType::parse_main_play(main_play_raw, false)?);
            MAIN_PLAY_CACHE.insert(main_play_raw.to_string(), pt.clone());
            pt
        };

        if main_plays
            .iter()
            .filter(|p| matches!(p, PlayType::PlateAppearance(_)))
            .count()
            > 1
        {
            bail!("Multiple plate appearances in play: {value}")
        };

        let modifiers = if modifiers_boundary < advances_boundary {
            let modifiers_raw = &value[modifiers_boundary + 1..advances_boundary];
            if let Some(pm) = PLAY_MODIFIER_CACHE.get(modifiers_raw) {
                pm
            } else {
                let pm = Arc::new(PlayModifier::parse_modifiers(modifiers_raw)?);
                PLAY_MODIFIER_CACHE.insert(modifiers_raw.to_string(), pm.clone());
                pm
            }
        } else {
            Arc::new(Vec::new())
        };

        let advances = if advances_boundary < value.len() - 1 {
            let advances_raw = &value[advances_boundary + 1..];
            if let Some(ra) = RUNNER_ADVANCES_CACHE.get(advances_raw) {
                ra
            } else {
                let ra = RunnerAdvance::parse_advances(advances_raw)?;
                let arc_ra = Arc::new(ra);
                RUNNER_ADVANCES_CACHE.insert(advances_raw.to_string(), arc_ra.clone());
                arc_ra
            }
        } else {
            Arc::new(Vec::new())
        };
        Ok(Self {
            main_plays,
            modifiers,
            explicit_advances: advances,
        })
    }
}

#[derive(Debug, Eq, PartialEq, Clone, Hash)]
pub struct PlayStats {
    pub fielders_data: Vec<FieldersData>,
    pub putouts: PositionVec,
    pub assists: PositionVec,
    pub errors: PositionVec,
    pub fielders_choices: PositionVec,
    pub outs: Vec<BaseRunner>,
    pub advances: Vec<RunnerAdvance>,
    pub runs: Vec<BaseRunner>,
    pub team_unearned_runs: Vec<BaseRunner>,
    pub rbi: Vec<BaseRunner>,
    pub plate_appearance: Option<PlateAppearanceType>,
    pub contact_description: Option<ContactDescription>,
    pub hit_to_fielder: Option<FieldingPosition>,
    pub batter_caused_baserunning_outs: Vec<BaseRunner>,
}

impl TryFrom<&ParsedPlay> for PlayStats {
    type Error = Error;

    fn try_from(parsed_play: &ParsedPlay) -> Result<Self> {
        let fielders_data = parsed_play.fielders_data();

        Ok(Self {
            putouts: FieldersData::putouts(&fielders_data),
            assists: FieldersData::assists(&fielders_data),
            errors: FieldersData::errors(&fielders_data),
            fielders_choices: FieldersData::fielders_choices(&fielders_data),
            fielders_data,
            outs: parsed_play.outs()?,
            advances: parsed_play.advances().collect(),
            runs: parsed_play.runs(),
            team_unearned_runs: parsed_play.team_unearned_runs(),
            rbi: parsed_play.rbi(),
            plate_appearance: parsed_play.plate_appearance().cloned(),
            contact_description: parsed_play.contact_description(),
            hit_to_fielder: parsed_play.hit_to_fielder(),
            batter_caused_baserunning_outs: parsed_play.batter_caused_baserunning_outs(),
        })
    }
}

pub fn print_cache_info() {
    println!(
        "RAW_PLAY_CACHE: {:?}",
        (PARSED_PLAY_CACHE.hits(), PARSED_PLAY_CACHE.misses())
    );
    println!(
        "MAIN_PLAY_CACHE: {:?}",
        (MAIN_PLAY_CACHE.hits(), MAIN_PLAY_CACHE.misses())
    );
    println!(
        "PLAY_MODIFIER_CACHE: {:?}",
        (PLAY_MODIFIER_CACHE.hits(), PLAY_MODIFIER_CACHE.misses())
    );
    println!(
        "RUNNER_ADVANCES_CACHE: {:?}",
        (RUNNER_ADVANCES_CACHE.hits(), RUNNER_ADVANCES_CACHE.misses())
    );
    println!(
        "PLAY_STATS_CACHE: {:?}",
        (PLAY_STATS_CACHE.hits(), PLAY_STATS_CACHE.misses())
    );
}

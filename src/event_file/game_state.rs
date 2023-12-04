use std::collections::HashMap;
use std::convert::TryFrom;
use std::sync::Arc;

use anyhow::{anyhow, bail, Context, Error, Result};
use arrayvec::{ArrayString, ArrayVec};
use bounded_integer::{BoundedU8, BoundedUsize};
use chrono::{NaiveDate, NaiveDateTime, NaiveTime};
use fixed_map::{Key, Map};
use itertools::Itertools;
use serde::{Deserialize, Serialize};
use strum_macros::{AsRefStr, Display};

use crate::event_file::info::{
    DayNight, DoubleheaderStatus, FieldCondition, HowScored, InfoRecord, Park, Precipitation, Sky,
    Team, UmpireAssignment, UmpirePosition, WindDirection,
};
use crate::event_file::misc::{
    BatHandAdjustment, EarnedRunRecord, GameId, Hand, PitchHandAdjustment,
    PitcherResponsibilityAdjustment, RunnerAdjustment, SubstitutionRecord,
};
use crate::event_file::parser::{FileInfo, MappedRecord, RecordSlice};
use crate::event_file::play::{
    Base, BaseRunner, BaserunningPlayType, ContactType, Count, FieldersData, FieldingData, HitType,
    InningFrame, OtherPlateAppearance, OutAtBatType, PlateAppearanceType, PlayModifier, PlayRecord,
    PlayType, RunnerAdvance, UnearnedRunStatus,
};
use crate::event_file::traits::{
    FieldingPosition, Inning, LineupPosition, Matchup, Pitcher, Player, RetrosheetVolunteer,
    Scorer, SequenceId, Side, Umpire, MAX_EVENTS_PER_GAME,
};
use crate::AccountType;

use super::box_score::{BoxScoreEvent, BoxScoreLine, LineScore};
use super::pitch_sequence::{PitchSequence, PitchSequenceItem, PitchType};
use super::play::{HitAngle, HitDepth, HitLocationGeneral, HitStrength, RunnerAdvanceModifier};
use super::schemas::GameIdString;
use super::traits::{EventKey, FieldingPlayType, GameType};

const UNKNOWN_STRINGS: [&str; 1] = ["unknown"];
const NONE_STRINGS: [&str; 2] = ["(none)", "none"];

#[derive(Debug, Eq, PartialEq, Copy, Clone, Hash, Display, Key)]
enum PositionType {
    Lineup(LineupPosition),
    Fielding(FieldingPosition),
}

/// A wrapper around `Player` that allows for a player to appear
/// in multiple positions in a lineup. This is used for the
/// Ohtani rule, where a player can appear in the lineup as a
/// pitcher and a DH.
#[derive(Debug, Eq, PartialEq, Copy, Clone, Hash)]
struct TrackedPlayer {
    pub player: Player,
    is_pitcher_with_dh: bool,
}

impl From<(Player, bool)> for TrackedPlayer {
    fn from((player, is_starting_pitcher_with_dh): (Player, bool)) -> Self {
        Self {
            player,
            is_pitcher_with_dh: is_starting_pitcher_with_dh,
        }
    }
}

impl std::fmt::Display for TrackedPlayer {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let dh = if self.is_pitcher_with_dh {
            "-pitcher-with-dh"
        } else {
            ""
        };
        write!(f, "{}{}", self.player, dh)
    }
}

type PersonnelState = Map<PositionType, TrackedPlayer>;
type Lineup = PersonnelState;
type Defense = PersonnelState;
pub type EventId = SequenceId;

fn get_game_id(rv: &RecordSlice) -> Result<GameId> {
    rv.iter()
        .find_map(|mr| {
            if let MappedRecord::GameId(g) = *mr {
                Some(g)
            } else {
                None
            }
        })
        .context("No Game ID found in records")
}

#[derive(Debug, Ord, PartialOrd, Eq, PartialEq, Clone, Copy, Serialize, Deserialize, AsRefStr)]
pub enum EnteredGameAs {
    Starter,
    PinchHitter,
    PinchRunner,
    DefensiveSubstitution,
}

impl EnteredGameAs {
    const fn substitution_type(sub: &SubstitutionRecord) -> Self {
        match sub.fielding_position {
            FieldingPosition::PinchHitter => Self::PinchHitter,
            FieldingPosition::PinchRunner => Self::PinchRunner,
            _ => Self::DefensiveSubstitution,
        }
    }
}

impl TryFrom<&MappedRecord> for EnteredGameAs {
    type Error = Error;

    fn try_from(record: &MappedRecord) -> Result<Self> {
        match record {
            MappedRecord::Start(_) => Ok(Self::Starter),
            MappedRecord::Substitution(sr) => Ok(Self::substitution_type(sr)),
            _ => bail!("Appearance type can only be determined from an appearance record"),
        }
    }
}

#[derive(Debug, Ord, PartialOrd, Eq, PartialEq, Clone, Copy, Serialize, Deserialize, AsRefStr)]
pub enum PlateAppearanceResultType {
    Single,
    Double,
    GroundRuleDouble,
    Triple,
    HomeRun,
    InsideTheParkHomeRun,
    InPlayOut,
    StrikeOut,
    FieldersChoice,
    ReachedOnError,
    Interference,
    HitByPitch,
    Walk,
    IntentionalWalk,
    SacrificeFly,
    SacrificeHit,
}

impl PlateAppearanceResultType {
    pub fn from_play(play: &PlayRecord) -> Option<Self> {
        let modifiers = play.parsed.modifiers.as_slice();
        play.parsed.main_plays.iter().find_map(|pt| {
            if let PlayType::PlateAppearance(pa) = pt {
                Some(Self::from_internal(pa, modifiers))
            } else {
                None
            }
        })
    }

    pub fn is_in_play(&self) -> bool {
        matches!(
            self,
            Self::Single
                | Self::Double
                | Self::Triple
                | Self::InsideTheParkHomeRun
                | Self::InPlayOut
                | Self::FieldersChoice
                | Self::ReachedOnError
                | Self::SacrificeFly
                | Self::SacrificeHit
        )
    }

    fn from_internal(plate_appearance: &PlateAppearanceType, modifiers: &[PlayModifier]) -> Self {
        let is_sac_fly = modifiers.iter().any(|m| m == &PlayModifier::SacrificeFly);
        let is_sac_hit = modifiers.iter().any(|m| m == &PlayModifier::SacrificeHit);
        let is_inside_the_park = modifiers
            .iter()
            .any(|m| m == &PlayModifier::InsideTheParkHomeRun);
        match plate_appearance {
            PlateAppearanceType::Hit(h) => match h.hit_type {
                HitType::Single => Self::Single,
                HitType::Double => Self::Double,
                HitType::GroundRuleDouble => Self::GroundRuleDouble,
                HitType::Triple => Self::Triple,
                HitType::HomeRun if is_inside_the_park => Self::InsideTheParkHomeRun,
                HitType::HomeRun => Self::HomeRun,
            },
            PlateAppearanceType::OtherPlateAppearance(opa) => match opa {
                OtherPlateAppearance::Walk => Self::Walk,
                OtherPlateAppearance::IntentionalWalk => Self::IntentionalWalk,
                OtherPlateAppearance::HitByPitch => Self::HitByPitch,
                OtherPlateAppearance::Interference => Self::Interference,
            },
            PlateAppearanceType::BattingOut(bo) => match bo.out_type {
                OutAtBatType::ReachedOnError => Self::ReachedOnError,
                OutAtBatType::StrikeOut => Self::StrikeOut,
                OutAtBatType::FieldersChoice => Self::FieldersChoice,
                OutAtBatType::InPlayOut if is_sac_fly => Self::SacrificeFly,
                OutAtBatType::InPlayOut if is_sac_hit => Self::SacrificeHit,
                // This can still include plays in which the batter reaches base,
                // such as FOs not recorded as FCs. It should result in an out
                // unless an error is made on another runner.
                OutAtBatType::InPlayOut => Self::InPlayOut,
            },
        }
    }
}

#[derive(Debug, Ord, PartialOrd, Eq, PartialEq, Clone, Serialize)]
pub struct EventFlag {
    event_key: EventKey,
    sequence_id: SequenceId,
    flag: String,
}

impl EventFlag {
    fn from_play(play: &PlayRecord, event_key: EventKey) -> Result<Vec<Self>> {
        play.parsed
            .modifiers
            .iter()
            .filter(|pm| pm.is_valid_event_type())
            .enumerate()
            .map(|(i, pm)| {
                Ok(Self {
                    event_key,
                    sequence_id: SequenceId::new(i + 1).context("Invalid sequence ID")?,
                    flag: pm.flag_string(),
                })
            })
            .collect()
    }
}

#[derive(Debug, Ord, PartialOrd, Eq, PartialEq, Copy, Clone, Serialize, Deserialize)]
pub struct Season(u16);

#[derive(Debug, Ord, PartialOrd, Eq, PartialEq, Clone, Serialize, Deserialize)]
struct League(String);

#[derive(Debug, Ord, PartialOrd, Eq, PartialEq, Clone, Serialize, Deserialize)]
pub struct GameSetting {
    pub date: NaiveDate,
    pub start_time: Option<NaiveTime>,
    pub game_type: GameType,
    pub doubleheader_status: DoubleheaderStatus,
    pub time_of_day: DayNight,
    pub bat_first_side: Side,
    pub sky: Sky,
    pub field_condition: FieldCondition,
    pub precipitation: Precipitation,
    pub wind_direction: WindDirection,
    pub season: Season,
    pub park_id: Park,
    pub temperature_fahrenheit: Option<u8>,
    pub attendance: Option<u32>,
    pub wind_speed_mph: Option<u8>,
    pub use_dh: bool,
}

impl Default for GameSetting {
    fn default() -> Self {
        Self {
            date: NaiveDate::from_num_days_from_ce_opt(0).unwrap_or_default(),
            doubleheader_status: DoubleheaderStatus::default(),
            game_type: GameType::RegularSeason,
            start_time: Option::default(),
            time_of_day: DayNight::default(),
            use_dh: false,
            bat_first_side: Side::Away,
            sky: Sky::default(),
            temperature_fahrenheit: Option::default(),
            field_condition: FieldCondition::default(),
            precipitation: Precipitation::default(),
            wind_direction: WindDirection::default(),
            wind_speed_mph: Option::default(),
            attendance: None,
            park_id: Park::default(),
            season: Season(0),
        }
    }
}

impl From<&RecordSlice> for GameSetting {
    fn from(vec: &RecordSlice) -> Self {
        let infos = vec.iter().filter_map(|rv| {
            if let MappedRecord::Info(i) = rv {
                Some(i)
            } else {
                None
            }
        });

        let mut setting = Self::default();

        for info in infos {
            match info {
                InfoRecord::GameDate(x) => setting.date = *x,
                InfoRecord::DoubleheaderStatus(x) => setting.doubleheader_status = *x,
                InfoRecord::StartTime(x) => setting.start_time = *x,
                InfoRecord::DayNight(x) => setting.time_of_day = *x,
                InfoRecord::UseDh(x) => setting.use_dh = *x,
                InfoRecord::GameType(x) => setting.game_type = *x,
                InfoRecord::HomeTeamBatsFirst(x) => {
                    setting.bat_first_side = if *x { Side::Home } else { Side::Away }
                }
                InfoRecord::Sky(x) => setting.sky = *x,
                InfoRecord::Temp(x) => setting.temperature_fahrenheit = *x,
                InfoRecord::FieldCondition(x) => setting.field_condition = *x,
                InfoRecord::Precipitation(x) => setting.precipitation = *x,
                InfoRecord::WindDirection(x) => setting.wind_direction = *x,
                InfoRecord::WindSpeed(x) => setting.wind_speed_mph = *x,
                InfoRecord::Attendance(x) => setting.attendance = *x,
                InfoRecord::Park(x) => setting.park_id = *x,
                _ => {}
            }
        }
        setting
    }
}

#[derive(Debug, Eq, PartialEq, Copy, Clone, Serialize, Deserialize, Default)]
pub struct GameMetadata {
    pub scorer: Option<Scorer>,
    pub how_scored: HowScored,
    pub inputter: Option<RetrosheetVolunteer>,
    pub translator: Option<RetrosheetVolunteer>,
    pub date_inputted: Option<NaiveDateTime>,
    pub date_edited: Option<NaiveDateTime>,
}

impl From<&RecordSlice> for GameMetadata {
    fn from(vec: &RecordSlice) -> Self {
        let infos = vec.iter().filter_map(|rv| {
            if let MappedRecord::Info(i) = rv {
                Some(i)
            } else {
                None
            }
        });
        let mut metadata = Self::default();
        for info in infos {
            match info {
                InfoRecord::Scorer(x) => metadata.scorer = *x,
                InfoRecord::HowScored(x) => metadata.how_scored = *x,
                InfoRecord::Inputter(x) => metadata.inputter = *x,
                InfoRecord::Translator(x) => metadata.translator = *x,
                InfoRecord::InputDate(x) => metadata.date_inputted = *x,
                InfoRecord::EditDate(x) => metadata.date_edited = *x,
                _ => {}
            }
        }
        metadata
    }
}

#[derive(Debug, Eq, PartialEq, Clone, Serialize, Deserialize)]
pub struct GameUmpire {
    pub game_id: GameIdString,
    pub position: UmpirePosition,
    pub umpire_id: Option<Umpire>,
}

impl GameUmpire {
    // Retrosheet has two different possible null-like values for umpire names, "none"
    // and "unknown". We take "none" to mean that there was no umpire at that position,
    // so we do not create a record. If "unknown", we assume there was someone at that position,
    // so a struct is created with a None umpire ID.
    fn from_umpire_assignment(ua: &UmpireAssignment, game_id: GameId) -> Option<Self> {
        let umpire = ua.umpire?;
        let position = ua.position;
        if NONE_STRINGS.contains(&umpire.as_str()) {
            None
        } else if UNKNOWN_STRINGS.contains(&umpire.as_str()) {
            Some(Self {
                game_id: game_id.id,
                position,
                umpire_id: None,
            })
        } else {
            Some(Self {
                game_id: game_id.id,
                position,
                umpire_id: Some(umpire),
            })
        }
    }

    fn from_record_slice(slice: &RecordSlice) -> Result<Vec<Self>> {
        let game_id = get_game_id(slice)?;
        Ok(slice
            .iter()
            .filter_map(|rv| {
                if let MappedRecord::Info(InfoRecord::UmpireAssignment(ua)) = rv {
                    Self::from_umpire_assignment(ua, game_id)
                } else {
                    None
                }
            })
            .collect_vec())
    }
}

#[derive(Debug, Eq, PartialEq, Clone, Serialize, Default)]
pub struct GameResults {
    pub winning_pitcher: Option<Player>,
    pub losing_pitcher: Option<Player>,
    pub save_pitcher: Option<Player>,
    pub game_winning_rbi: Option<Player>,
    pub time_of_game_minutes: Option<u16>,
    pub protest_info: Option<String>,
    pub completion_info: Option<String>,
    pub earned_runs: Vec<EarnedRunRecord>,
}

impl From<&[MappedRecord]> for GameResults {
    fn from(vec: &[MappedRecord]) -> Self {
        let mut results = Self::default();
        vec.iter()
            .filter_map(|rv| {
                if let MappedRecord::Info(i) = rv {
                    Some(i)
                } else {
                    None
                }
            })
            .for_each(|info| match info {
                InfoRecord::WinningPitcher(x) => results.winning_pitcher = *x,
                InfoRecord::LosingPitcher(x) => results.losing_pitcher = *x,
                InfoRecord::SavePitcher(x) => results.save_pitcher = *x,
                InfoRecord::GameWinningRbi(x) => results.game_winning_rbi = *x,
                InfoRecord::TimeOfGameMinutes(x) => results.time_of_game_minutes = *x,
                _ => {}
            });
        // Add earned runs
        vec.iter()
            .filter_map(|rv| {
                if let MappedRecord::EarnedRun(er) = rv {
                    Some(er)
                } else {
                    None
                }
            })
            .for_each(|er| results.earned_runs.push(*er));
        results
    }
}

#[derive(Debug, Eq, PartialEq, Copy, Clone, Serialize)]
pub struct GameLineupAppearance {
    pub game_id: GameIdString,
    pub player_id: Player,
    pub side: Side,
    pub lineup_position: LineupPosition,
    pub entered_game_as: EnteredGameAs,
    pub start_event_id: EventId,
    pub end_event_id: Option<EventId>,
}

impl GameLineupAppearance {
    pub fn get_at_event(
        appearances: &[Self],
        position: LineupPosition,
        event_id: EventId,
        side: Side,
    ) -> Result<Self> {
        appearances
            .iter()
            .find(|a| {
                a.lineup_position == position
                    && a.side == side
                    && a.start_event_id <= event_id
                    && a.end_event_id.map_or(true, |end| end >= event_id)
            })
            .copied()
            .context("Could not find lineup appearance")
    }

    fn new_starter(
        player: Player,
        lineup_position: LineupPosition,
        side: Side,
        game_id: GameId,
    ) -> Result<Self> {
        Ok(Self {
            game_id: game_id.id,
            player_id: player,
            lineup_position,
            side,
            entered_game_as: EnteredGameAs::Starter,
            start_event_id: EventId::new(1).context("Could not create event ID")?,
            end_event_id: None,
        })
    }

    fn finalize(self, end_event_id: EventId) -> Self {
        Self {
            end_event_id: self.end_event_id.or(Some(end_event_id)),
            ..self
        }
    }
}

#[derive(Debug, Eq, PartialEq, Clone, Serialize, Copy)]
pub struct GameFieldingAppearance {
    pub game_id: GameIdString,
    pub player_id: Player,
    pub side: Side,
    pub fielding_position: FieldingPosition,
    pub start_event_id: EventId,
    pub end_event_id: Option<EventId>,
}

impl GameFieldingAppearance {
    fn new_starter(
        player: Player,
        fielding_position: FieldingPosition,
        side: Side,
        game_id: GameId,
    ) -> Result<Self> {
        Ok(Self {
            game_id: game_id.id,
            player_id: player,
            fielding_position,
            side,
            start_event_id: EventId::new(1).context("Could not create event ID")?,
            end_event_id: None,
        })
    }

    const fn new(
        player: Player,
        fielding_position: FieldingPosition,
        side: Side,
        game_id: GameId,
        start_event: EventId,
    ) -> Self {
        Self {
            game_id: game_id.id,
            player_id: player,
            fielding_position,
            side,
            start_event_id: start_event,
            end_event_id: None,
        }
    }

    fn finalize(self, end_event_id: EventId) -> Self {
        Self {
            end_event_id: self.end_event_id.or(Some(end_event_id)),
            ..self
        }
    }
}

#[derive(Debug, Eq, PartialEq, Clone, Serialize)]
pub struct BoxScoreData {
    pub lines: Vec<BoxScoreLine>,
    pub events: Vec<BoxScoreEvent>,
    pub line_scores: Vec<LineScore>,
    pub comments: Vec<String>,
}

impl BoxScoreData {
    fn from_record_slice(slice: &RecordSlice) -> Result<Self> {
        let mut lines = Vec::new();
        let mut events = Vec::new();
        let mut line_scores = Vec::new();
        let mut comments = Vec::new();
        for record in slice.iter() {
            match record {
                MappedRecord::BoxScoreLine(bsl) => lines.push(*bsl),
                MappedRecord::BoxScoreEvent(bse) => events.push(bse.clone()),
                MappedRecord::LineScore(bsls) => line_scores.push(bsls.clone()),
                MappedRecord::Comment(c) => comments.push(c.clone()),
                _ => {}
            }
        }
        Ok(Self {
            lines,
            events,
            line_scores,
            comments,
        })
    }
}

#[derive(Debug, Eq, PartialEq, Clone, Serialize)]
pub struct GameContext {
    #[serde(flatten)]
    pub game_id: GameId,
    pub file_info: FileInfo,
    pub metadata: GameMetadata,
    pub teams: Matchup<Team>,
    pub setting: GameSetting,
    pub umpires: Vec<GameUmpire>,
    pub results: GameResults,
    pub lineup_appearances: Vec<GameLineupAppearance>,
    pub fielding_appearances: Vec<GameFieldingAppearance>,
    pub events: Vec<Event>,
    pub line_offset: usize,
    pub event_key_offset: i32,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub box_score_data: Option<BoxScoreData>,
}

impl GameContext {
    pub fn new(
        record_slice: &RecordSlice,
        file_info: FileInfo,
        line_offset: usize,
        game_num: usize,
    ) -> Result<Self> {
        let game_id = get_game_id(record_slice)?;
        let teams: Matchup<Team> = Matchup::try_from(record_slice)?;
        let setting = GameSetting::try_from(record_slice)?;
        let metadata = GameMetadata::try_from(record_slice)?;
        let umpires = GameUmpire::from_record_slice(record_slice)?;
        let results = GameResults::try_from(record_slice)?;
        let event_key_offset = Self::event_key_offset(file_info, game_num)?;
        let box_score_data = if file_info.account_type == AccountType::BoxScore {
            Some(BoxScoreData::from_record_slice(record_slice)?)
        } else {
            None
        };

        let (events, lineup_appearances, fielding_appearances) =
            if file_info.account_type == AccountType::BoxScore {
                (vec![], vec![], vec![])
            } else {
                GameState::create_events(record_slice, line_offset, event_key_offset)
                    .with_context(|| anyhow!("Could not parse events"))?
            };

        Ok(Self {
            game_id,
            file_info,
            metadata,
            teams,
            setting,
            umpires,
            results,
            lineup_appearances,
            fielding_appearances,
            events,
            line_offset,
            event_key_offset,
            box_score_data,
        })
    }

    fn event_key_offset(file_info: FileInfo, game_num: usize) -> Result<i32> {
        (file_info.file_index + (game_num * MAX_EVENTS_PER_GAME))
            .try_into()
            .context("i32 overflow on event key creation")
    }
}

#[derive(Debug, Eq, PartialEq, Clone, Serialize, Deserialize)]
pub struct EventBaserunningPlay {
    pub event_key: EventKey,
    pub sequence_id: SequenceId,
    pub baserunning_play_type: BaserunningPlayType,
    pub baserunner: Option<BaseRunner>,
}

impl EventBaserunningPlay {
    fn from_play(play: &PlayRecord, event_key: EventKey) -> Result<Vec<Self>> {
        play.parsed
            .main_plays
            .iter()
            .enumerate()
            .map(|(i, pt)| {
                Ok((
                    SequenceId::new(i + 1).context("Could not create sequence ID")?,
                    pt,
                ))
            })
            .filter_map_ok(|(i, pt)| {
                if let PlayType::BaserunningPlay(br) = pt {
                    Some(Self {
                        event_key,
                        sequence_id: i,
                        baserunning_play_type: br.baserunning_play_type,
                        baserunner: br.baserunner(),
                    })
                } else {
                    None
                }
            })
            .collect::<Result<Vec<Self>>>()
    }
}

#[derive(Debug, Eq, PartialEq, Clone, Serialize, Deserialize, Default)]
pub struct EventBattedBallInfo {
    pub event_key: EventKey,
    pub contact: ContactType,
    pub hit_to_fielder: Option<FieldingPosition>,
    pub general_location: HitLocationGeneral,
    pub depth: HitDepth,
    pub angle: HitAngle,
    pub strength: HitStrength,
}

impl EventBattedBallInfo {
    fn from_play(play: &PlayRecord, event_key: EventKey) -> Option<Self> {
        // Determine whether the ball was hit in play, and then extract all contact/location info if so
        play.parsed.main_plays.iter().find_map(|pt| {
            match pt {
                PlayType::PlateAppearance(pa) if pa.is_batted_ball() => {
                    // In the absence of any contact info, we still want to return Some to indicate that
                    // the ball was hit in play but we don't have any data on it
                    let contact_description = play.stats.contact_description.unwrap_or_default();
                    let location = contact_description.location.unwrap_or_default();
                    // Fielder can be None for home runs/ground rule doubles/fan interference,
                    // but in other cases it should be explicitly marked as Unknown if missing
                    let no_fielder_modifiers = [
                        PlayModifier::FanInterference,
                        PlayModifier::InsideTheParkHomeRun,
                    ];
                    let has_fielder = match pa {
                        PlateAppearanceType::Hit(h) if h.hit_type == HitType::HomeRun => play
                            .parsed
                            .modifiers
                            .iter()
                            .any(|m| no_fielder_modifiers.contains(m)),
                        PlateAppearanceType::Hit(h) => h.hit_type != HitType::GroundRuleDouble,
                        _ => true,
                    };
                    let hit_to_fielder = if has_fielder {
                        Some(play.stats.hit_to_fielder.unwrap_or_default())
                    } else {
                        play.stats.hit_to_fielder
                    };
                    Some(Self {
                        event_key,
                        contact: contact_description.contact_type.unwrap_or_default(),
                        hit_to_fielder,
                        general_location: location.general_location,
                        depth: location.depth,
                        angle: location.angle,
                        strength: location.strength,
                    })
                }
                _ => None,
            }
        })
    }
}

#[derive(Debug, Eq, PartialEq, Clone, Serialize, Deserialize)]
pub struct EventBaserunningAdvanceAttempt {
    pub event_key: EventKey,
    pub sequence_id: SequenceId,
    pub baserunner: BaseRunner,
    pub attempted_advance_to: Base,
    pub is_successful: bool,
    pub advanced_on_error_flag: bool,
    pub explicit_out_flag: bool,
    pub run_scored_flag: bool,
    pub rbi_flag: bool,
    pub team_unearned_flag: bool,
}

impl EventBaserunningAdvanceAttempt {
    pub fn scored(&self) -> bool {
        self.is_successful && self.attempted_advance_to == Base::Home
    }

    fn from_play(play: &PlayRecord, event_key: EventKey) -> Result<Vec<Self>> {
        play.stats
            .advances
            .iter()
            .enumerate()
            .map(|(i, ra)| {
                let advanced_on_error_flag =
                    FieldersData::find_error(ra.fielders_data().as_slice()).is_some();
                let is_successful = !ra.is_out();
                let explicit_out_flag = ra.out_or_error;
                let run_scored_flag = play.stats.runs.contains(&ra.baserunner);
                let rbi_flag = play.stats.rbi.contains(&ra.baserunner);
                let team_unearned_flag = ra
                    .modifiers
                    .contains(&RunnerAdvanceModifier::TeamUnearnedRun);
                Ok(Self {
                    event_key,
                    sequence_id: SequenceId::new(i + 1).context("Could not create sequence ID")?,
                    baserunner: ra.baserunner,
                    attempted_advance_to: ra.to,
                    advanced_on_error_flag,
                    explicit_out_flag,
                    is_successful,
                    run_scored_flag,
                    rbi_flag,
                    team_unearned_flag,
                })
            })
            .collect()
    }
}

#[derive(Debug, Eq, PartialEq, Copy, Clone, Serialize, Deserialize)]
pub struct EventRun {
    pub event_key: EventKey,
    pub runner: BaseRunner,
    pub rbi_flag: bool,
    pub explicit_unearned_run_status: Option<UnearnedRunStatus>,
}

impl EventRun {
    pub fn is_team_unearned_run(&self) -> bool {
        self.explicit_unearned_run_status
            .map_or(false, |s| s == UnearnedRunStatus::TeamUnearned)
    }

    fn from_play(play: &PlayRecord, event_key: EventKey) -> Vec<Self> {
        play.stats
            .advances
            .iter()
            .filter_map(|ra| {
                if play.stats.runs.contains(&ra.baserunner) {
                    Some(Self {
                        event_key,
                        runner: ra.baserunner,
                        rbi_flag: play.stats.rbi.contains(&ra.baserunner),
                        explicit_unearned_run_status: ra.unearned_run_status(),
                    })
                } else {
                    None
                }
            })
            .collect()
    }
}

#[derive(Debug, Eq, PartialEq, Clone, Serialize)]
pub struct EventContext {
    pub inning: u8,
    pub batting_side: Side,
    pub frame: InningFrame,
    pub at_bat: LineupPosition,
    pub batter_id: Player,
    pub pitcher_id: Player,
    pub outs: Outs,
    #[serde(skip)]
    pub starting_base_state: BaseState,
    #[serde(flatten)]
    pub rare_attributes: RareAttributes,
}

#[derive(Debug, Eq, PartialEq, Clone, Serialize)]
pub struct EventResults {
    pub count_at_event: Count,
    pub pitch_sequence: Arc<PitchSequence>,
    pub plate_appearance: Option<PlateAppearanceResultType>,
    pub batted_ball_info: Option<EventBattedBallInfo>,
    pub plays_at_base: Vec<EventBaserunningPlay>,
    pub out_on_play: Vec<BaseRunner>,
    pub fielding_plays: Vec<FieldersData>,
    pub baserunning_advances: Vec<EventBaserunningAdvanceAttempt>,
    pub runs: Vec<EventRun>,
    #[serde(skip)]
    pub ending_base_state: BaseState,
    pub play_info: Vec<EventFlag>,
    pub comment: Vec<String>,
    pub no_play_flag: bool,
}

#[derive(Debug, Eq, PartialEq, Clone, Serialize)]
pub struct Event {
    pub game_id: GameId,
    pub event_id: EventId,
    pub event_key: EventKey,
    pub context: EventContext,
    pub results: EventResults,
    pub line_number: usize,
}

impl Event {
    pub fn summary(&self) -> String {
        format!(
            r#"
        Event: {event_id}
        Inning: {frame:?} {inning}
        Outs at event: {outs_at_event}
        Batter: {ab:?}
        Plate appearance result: {pa:?}
        Baserunning: {ba:?}
        Out on play: {out:?}
        "#,
            event_id = self.event_id,
            frame = self.context.frame,
            inning = self.context.inning,
            outs_at_event = self.context.outs,
            ab = self.context.at_bat,
            pa = self.results.plate_appearance,
            ba = self.results.baserunning_advances,
            out = self.results.out_on_play,
        )
    }
}

/// This tracks unusual/miscellaneous elements,
/// such as batters batting from an unexpected side or a substitution in the middle of
/// an at-bat. Further exceptions should go here as they come up.
#[derive(Default, Debug, Eq, PartialEq, Clone, Copy, Serialize, Deserialize)]
pub struct RareAttributes {
    pub batter_hand: Option<Hand>,
    pub pitcher_hand: Option<Hand>,
    // In the case of a mid-PA substitution, the
    // credit for the result of the PA cannot be determined mid-PA
    // because the result itself is part of the determination.
    // In order to provide a priori credit, we can provide the answers
    // for each possible case. (Only strikeouts/walks need this treatment,
    // since all other results are credited to the new player).
    pub strikeout_responsible_batter: Option<Player>,
    pub walk_responsible_pitcher: Option<Player>,
}

/// Keeps track of the current players on the field at any given point
/// and records their exits/entries.
#[derive(Debug, Eq, PartialEq, Clone)]
struct Personnel {
    game_id: GameId,
    personnel_state: Matchup<(Lineup, Defense)>,
    // A player should only have one lineup position per game,
    // but can move freely from one defensive position to another.
    // However, in the rare case of a courtesy runner, a player can
    // potentially become a pinch-runner for another player before
    // switching back to his old lineup position.
    // (This also makes the convenient assumption that a player cannot play for both sides in
    // the same game, which has never happened but could theoretically).
    lineup_appearances: HashMap<TrackedPlayer, Vec<GameLineupAppearance>>,
    defense_appearances: HashMap<TrackedPlayer, Vec<GameFieldingAppearance>>,
}

impl Default for Personnel {
    fn default() -> Self {
        Self {
            game_id: GameId {
                id: GameIdString::default(),
            },
            personnel_state: Matchup::new(
                (Lineup::new(), Defense::new()),
                (Lineup::new(), Defense::new()),
            ),
            lineup_appearances: HashMap::with_capacity(30),
            defense_appearances: HashMap::with_capacity(30),
        }
    }
}

impl Personnel {
    fn new(record_slice: &RecordSlice) -> Result<Self> {
        let game_id = get_game_id(record_slice)?;
        let mut personnel = Self {
            game_id,
            ..Default::default()
        };
        let start_iter = record_slice.iter().filter_map(|rv| {
            if let MappedRecord::Start(sr) = rv {
                Some(sr)
            } else {
                None
            }
        });
        for start in start_iter {
            let (lineup, defense) = personnel.personnel_state.get_mut(start.side);
            let lineup_appearance = GameLineupAppearance::new_starter(
                start.player,
                start.lineup_position,
                start.side,
                game_id,
            );
            let fielding_appearance = GameFieldingAppearance::new_starter(
                start.player,
                start.fielding_position,
                start.side,
                game_id,
            );
            let player: TrackedPlayer = (
                start.player,
                start.lineup_position == LineupPosition::PitcherWithDh,
            )
                .into();

            lineup.insert(PositionType::Lineup(start.lineup_position), player);
            defense.insert(PositionType::Fielding(start.fielding_position), player);
            personnel
                .lineup_appearances
                .insert(player, vec![lineup_appearance?]);
            personnel
                .defense_appearances
                .insert(player, vec![fielding_appearance?]);
        }
        Ok(personnel)
    }

    fn pitcher(&self, side: Side) -> Result<Pitcher> {
        self.get_at_position(side, PositionType::Fielding(FieldingPosition::Pitcher))
            .map(|tp| tp.player)
    }

    fn get_at_position(&self, side: Side, position: PositionType) -> Result<TrackedPlayer> {
        let map_tup = self.personnel_state.get(side);
        let map = if let PositionType::Lineup(_) = position {
            &map_tup.0
        } else {
            &map_tup.1
        };
        map.get(position).copied().with_context(|| {
            anyhow!(
                "Position {} for side {} missing from current game state",
                position,
                side
            )
        })
    }

    fn get_player_lineup_position(
        &self,
        side: Side,
        player: &TrackedPlayer,
    ) -> Option<PositionType> {
        let (lineup, _) = self.personnel_state.get(side);
        lineup.iter().find_map(|(position, tracked_player)| {
            if tracked_player == player {
                Some(position)
            } else {
                None
            }
        })
    }

    fn at_bat(&self, play: &PlayRecord) -> Result<LineupPosition> {
        let player: TrackedPlayer = (play.batter, false).into();
        let position = self.get_player_lineup_position(play.batting_side, &player);
        if let Some(PositionType::Lineup(lp)) = position {
            Ok(lp)
        } else {
            bail!(
                "Fatal error parsing {}: Cannot find lineup position of player currently at bat {}.",
                self.game_id.id,
                &play.batter,
            )
        }
    }

    fn get_current_lineup_appearance(
        &mut self,
        player: &TrackedPlayer,
    ) -> Result<&mut GameLineupAppearance> {
        self.lineup_appearances
            .get_mut(player)
            .with_context(|| {
                anyhow!(
                    "Cannot find existing player {} in lineup appearance records",
                    player
                )
            })?
            .last_mut()
            .with_context(|| anyhow!("Player {} has an empty list of lineup appearances", player))
    }

    fn get_current_fielding_appearance(
        &mut self,
        player: &TrackedPlayer,
    ) -> Result<&mut GameFieldingAppearance> {
        self.defense_appearances
            .get_mut(player)
            .with_context(|| {
                anyhow!(
                    "Cannot find existing player {} in defense appearance records",
                    player
                )
            })?
            .last_mut()
            .with_context(|| {
                anyhow!(
                    "Player {} has an empty list of fielding appearances",
                    player
                )
            })
    }

    fn update_lineup_on_substitution(
        &mut self,
        sub: &SubstitutionRecord,
        event_id: EventId,
    ) -> Result<()> {
        let original_batter =
            self.get_at_position(sub.side, PositionType::Lineup(sub.lineup_position));

        if let Ok(p) = original_batter {
            let current_appearance: &mut GameLineupAppearance =
                self.get_current_lineup_appearance(&p)?;

            if p.player == sub.player && current_appearance.lineup_position == sub.lineup_position {
                return Ok(());
            }

            if current_appearance.lineup_position == sub.lineup_position {
                current_appearance.end_event_id = Some(event_id - 1);
            }
        }

        let new_player: TrackedPlayer = (
            sub.player,
            sub.lineup_position == LineupPosition::PitcherWithDh,
        )
            .into();
        // In the case of a courtesy runner, the new player may already be in the lineup
        let check_courtesy = self.get_current_lineup_appearance(&new_player);
        if let Ok(p) = check_courtesy {
            p.end_event_id = p.end_event_id.or_else(|| Some(event_id - 1));
        }

        let new_lineup_appearance = GameLineupAppearance {
            game_id: self.game_id.id,
            player_id: sub.player,
            lineup_position: sub.lineup_position,
            side: sub.side,
            entered_game_as: EnteredGameAs::substitution_type(sub),
            start_event_id: event_id,
            end_event_id: None,
        };
        let (lineup, _) = self.personnel_state.get_mut(sub.side);
        lineup.insert(PositionType::Lineup(sub.lineup_position), new_player);
        self.lineup_appearances
            .entry(new_player)
            .or_insert_with(|| Vec::with_capacity(1))
            .push(new_lineup_appearance);
        Ok(())
    }

    /// The semantics of defensive substitutions are more complicated, because the new player
    /// could already have been in the game, and the replaced player might not have left the game.
    fn update_defense_on_substitution(
        &mut self,
        sub: &SubstitutionRecord,
        event_id: EventId,
    ) -> Result<()> {
        let original_fielder =
            self.get_at_position(sub.side, PositionType::Fielding(sub.fielding_position));
        if let Ok(p) = original_fielder {
            if p.player == sub.player {
                return Ok(());
            }
            let current_appearance = self.get_current_fielding_appearance(&p)?;
            if current_appearance.fielding_position == sub.fielding_position {
                current_appearance.end_event_id = Some(event_id - 1);
            }
        }
        let new_fielder: TrackedPlayer = (
            sub.player,
            sub.lineup_position == LineupPosition::PitcherWithDh,
        )
            .into();
        // If the new fielder is already in the game, we need to close out their previous appearance
        if let Ok(gfa) = self.get_current_fielding_appearance(&new_fielder) {
            gfa.end_event_id = Some(event_id - 1);
        }

        let (_, defense) = self.personnel_state.get_mut(sub.side);
        defense.insert(PositionType::Fielding(sub.fielding_position), new_fielder);
        self.defense_appearances
            .entry(new_fielder)
            .or_insert_with(|| Vec::with_capacity(1))
            .push(GameFieldingAppearance::new(
                sub.player,
                sub.fielding_position,
                sub.side,
                self.game_id,
                event_id,
            ));

        Ok(())
    }

    /// This handles the rare but always fun case of a team vacating the DH by putting the DH
    /// into the field or the pitcher into a non-pitching position.
    /// This will be a safe no-op if the game in question isn't using a DH.
    fn update_on_dh_vacancy(&mut self, sub: &SubstitutionRecord, event_id: EventId) -> Result<()> {
        let non_batting_pitcher = self
            .get_at_position(
                sub.side,
                PositionType::Lineup(LineupPosition::PitcherWithDh),
            )
            .ok();
        let dh = self
            .get_at_position(
                sub.side,
                PositionType::Fielding(FieldingPosition::DesignatedHitter),
            )
            .ok()
            .and_then(|tp| {
                // If the DH vacancy is being created by moving having the DH
                // come into pitch, we don't need to end their fielding appearance
                if sub.fielding_position == FieldingPosition::Pitcher {
                    None
                } else {
                    Some(tp)
                }
            });
        if let Some(p) = non_batting_pitcher {
            self.get_current_lineup_appearance(&p)?.end_event_id = Some(event_id - 1);
        }
        if let Some(p) = dh {
            self.get_current_fielding_appearance(&p)?.end_event_id = Some(event_id - 1);
        }
        Ok(())
    }

    fn update_on_substitution(
        &mut self,
        sub: &SubstitutionRecord,
        event_id: EventId,
    ) -> Result<()> {
        self.update_lineup_on_substitution(sub, event_id)?;
        if sub.fielding_position.is_true_position() {
            self.update_defense_on_substitution(sub, event_id)?;
        }
        if sub.fielding_position == FieldingPosition::Pitcher
            && sub.lineup_position != LineupPosition::PitcherWithDh
        {
            self.update_on_dh_vacancy(sub, event_id)?;
        }
        Ok(())
    }
}

/// Tracks the information necessary to populate each event.
#[derive(Debug, Eq, PartialEq, Clone)]
pub struct GameState {
    game_id: GameId,
    event_id: EventId,
    inning: Inning,
    frame: InningFrame,
    count: Count,
    batting_side: Side,
    outs: Outs,
    bases: BaseState,
    at_bat: LineupPosition,
    personnel: Personnel,
    unusual_state: RareAttributes,
    comment_buffer: Vec<String>,
}

impl GameState {
    pub fn create_events(
        record_slice: &RecordSlice,
        line_offset: usize,
        event_key_offset: i32,
    ) -> Result<(
        Vec<Event>,
        Vec<GameLineupAppearance>,
        Vec<GameFieldingAppearance>,
    )> {
        let mut events: Vec<Event> = Vec::with_capacity(100);

        let mut state = Self::new(record_slice)?;
        for (i, record) in record_slice.iter().enumerate() {
            let event_key: i32 = event_key_offset + i32::try_from(state.event_id.get())?;
            let opt_play = match record {
                MappedRecord::Play(pr) => Some(pr),
                _ => None,
            };
            // TODO: Feels wrong to have to handle out total differently than everything else
            // TODO: Would be nice to clear this automatically rather than checking
            let (starting_base_state, starting_outs) =
                if matches!(opt_play.map(|p| state.is_frame_flipped(p)), Some(Ok(true))) {
                    (
                        BaseState::default(),
                        Outs::new(0).context("Unexpected outs bound error")?,
                    )
                } else {
                    (state.bases.clone(), state.outs)
                };
            // Unusual game state also needs to be grabbed before updating state
            let rare_attributes = state.unusual_state.clone();

            state.update(record, opt_play)?;
            if let Some(play) = opt_play {
                let context = EventContext {
                    inning: state.inning,
                    batting_side: state.batting_side,
                    frame: state.frame,
                    at_bat: state.at_bat,
                    batter_id: play.batter,
                    pitcher_id: state.personnel.pitcher(state.batting_side.flip())?,
                    outs: starting_outs,
                    starting_base_state,
                    rare_attributes,
                };
                let results = EventResults {
                    count_at_event: play.count,
                    pitch_sequence: play.pitch_sequence.clone(),
                    plate_appearance: PlateAppearanceResultType::from_play(play),
                    batted_ball_info: EventBattedBallInfo::from_play(play, event_key),
                    plays_at_base: EventBaserunningPlay::from_play(play, event_key)?,
                    baserunning_advances: EventBaserunningAdvanceAttempt::from_play(
                        play, event_key,
                    )?,
                    runs: EventRun::from_play(play, event_key),
                    play_info: EventFlag::from_play(play, event_key)?,
                    comment: state.comment_buffer,
                    fielding_plays: play.stats.fielders_data.clone(),
                    out_on_play: play.stats.outs.clone(),
                    ending_base_state: state.bases.clone(),
                    no_play_flag: play.stats.no_play_flag,
                };
                let line_number = line_offset + i;
                events.push(Event {
                    game_id: state.game_id,
                    event_id: state.event_id,
                    context,
                    results,
                    line_number,
                    event_key,
                });
                state.event_id += 1;
                state.comment_buffer = vec![]; // Clear comment buffer
            }
        }
        // Set all remaining blank end_event_ids to final event
        let max_event_id = EventId::new(events.len()).context("No events in list")?;
        let lineup_appearances = state
            .personnel
            .lineup_appearances
            .values()
            .flatten()
            .map(|la| la.finalize(max_event_id))
            .sorted_by_key(|la| (la.side, la.lineup_position, la.start_event_id))
            .collect_vec();
        let defense_appearances = state
            .personnel
            .defense_appearances
            .values()
            .flatten()
            .map(|la| la.finalize(max_event_id))
            .sorted_by_key(|la| (la.side, la.fielding_position, la.start_event_id))
            .collect_vec();

        Ok((events, lineup_appearances, defense_appearances))
    }

    pub(crate) fn new(record_slice: &RecordSlice) -> Result<Self> {
        let game_id = get_game_id(record_slice)?;
        let batting_side = record_slice
            .iter()
            .find_map(|rv| {
                if let MappedRecord::Info(InfoRecord::HomeTeamBatsFirst(b)) = rv {
                    Some(if *b { Side::Home } else { Side::Away })
                } else {
                    None
                }
            })
            .map_or(Side::Away, |s| s);

        Ok(Self {
            game_id,
            event_id: EventId::new(1).context("Unexpected event ID bound error")?,
            inning: 1,
            frame: InningFrame::Top,
            count: Count::default(),
            batting_side,
            outs: Outs::new(0).context("Unexpected outs bound error")?,
            bases: BaseState::default(),
            at_bat: LineupPosition::default(),
            personnel: Personnel::new(record_slice)?,
            unusual_state: RareAttributes::default(),
            comment_buffer: vec![],
        })
    }

    fn is_frame_flipped(&self, play: &PlayRecord) -> Result<bool> {
        if self.batting_side == play.batting_side {
            Ok(false)
        } else if self.outs < 3 {
            bail!("New frame without 3 outs recorded")
        } else {
            Ok(true)
        }
    }

    fn get_new_frame(&self, play: &PlayRecord) -> Result<InningFrame> {
        Ok(if self.is_frame_flipped(play)? {
            self.frame.flip()
        } else {
            self.frame
        })
    }

    fn outs_after_play(&self, play: &PlayRecord) -> Result<Outs> {
        let play_outs = play.stats.outs.len();
        let new_outs = if self.is_frame_flipped(play)? {
            play_outs
        } else {
            self.outs.get() + play_outs
        };
        Outs::new(new_outs).context("Illegal state, more than 3 outs recorded")
    }

    fn update_on_play(&mut self, play: &PlayRecord) -> Result<()> {
        let new_frame = self.get_new_frame(play)?;
        let new_outs = self.outs_after_play(play)?;

        let batter_lineup_position = self.personnel.at_bat(play)?;

        let new_base_state = self.bases.new_base_state(
            self.is_frame_flipped(play)?,
            new_outs == 3,
            play,
            batter_lineup_position,
            self.event_id,
        )?;

        let is_mid_plate_appearance = play.stats.plate_appearance.is_none() && new_outs < 3;

        match is_mid_plate_appearance {
            true => {
                self.count = play.count;
                // Hand adjustments are reset in all circumstances, including mid-PA
                self.unusual_state.batter_hand = None;
                self.unusual_state.pitcher_hand = None;
            }
            false => {
                self.count = Count::default();
                // All unusual state characteristics are reset on a new PA
                self.unusual_state = RareAttributes::default();
            }
        }
        self.inning = play.inning;
        self.frame = new_frame;
        self.batting_side = play.batting_side;
        self.outs = new_outs;
        self.bases = new_base_state;
        self.at_bat = batter_lineup_position;

        Ok(())
    }

    fn update_on_substitution(&mut self, record: &SubstitutionRecord) -> Result<()> {
        if record.lineup_position == self.at_bat
            && record.side == self.batting_side
            && self.count.is_old_batter_responsible_strikeout()
        {
            let batter = self
                .personnel
                .get_at_position(record.side, PositionType::Lineup(record.lineup_position))?
                .player;
            self.unusual_state.strikeout_responsible_batter = Some(batter);
        } else if record.fielding_position == FieldingPosition::Pitcher
            && record.side != self.batting_side
            && self.count.is_old_pitcher_responsible_walk()
        {
            self.unusual_state.walk_responsible_pitcher =
                Some(self.personnel.pitcher(record.side)?);
        };
        self.personnel.update_on_substitution(record, self.event_id)
    }

    fn update_on_bat_hand_adjustment(&mut self, record: &BatHandAdjustment) {
        self.unusual_state.batter_hand = Some(record.hand);
    }

    fn update_on_pitch_hand_adjustment(&mut self, record: &PitchHandAdjustment) {
        self.unusual_state.batter_hand = Some(record.hand);
    }

    fn update_on_runner_adjustment(&mut self, record: &RunnerAdjustment) -> Result<()> {
        // The extra innings runner record can appear before or after the first record of the next
        // inning, and it doesn't have a side associated with it, so we have to do some messy
        // state changes to get it right.
        if self.outs == 3 {
            self.frame = self.frame.flip();
            self.batting_side = self.batting_side.flip();
            self.outs = Outs::new(0).context("Unexpected outs bound error")?;
        }
        let tracked_runner: TrackedPlayer = (record.runner_id, false).into();
        let runner_pos = self
            .personnel
            .get_current_lineup_appearance(&tracked_runner)?
            .lineup_position;
        self.bases = BaseState::new_inning_tiebreaker(runner_pos, self.event_id);

        Ok(())
    }

    fn update_on_comment(&mut self, comment: &str) {
        self.comment_buffer.push(comment.trim().replace('$', ""));
    }

    fn update_on_pitcher_responsibility_adjustment(
        &mut self,
        record: &PitcherResponsibilityAdjustment,
    ) -> Result<()> {
        let mut runner = self
            .bases
            .get_runner(record.baserunner)
            .context(anyhow!(
                "Pitcher responsibility adjustment for non-existent runner: {:?}",
                record
            ))?
            .clone();
        runner.explicit_charged_pitcher_id = Some(record.pitcher_id);
        self.bases.set_runner(record.baserunner, runner);
        Ok(())
    }

    pub fn update(&mut self, record: &MappedRecord, play: Option<&PlayRecord>) -> Result<()> {
        match record {
            // We've already pulled the play record out before the call to this function
            MappedRecord::Play(_) => {
                if let Some(cp) = play {
                    self.update_on_play(cp)
                        .with_context(|| anyhow!("Failed to parse play {:?}", cp))
                } else {
                    bail!("Expected play but got None")
                }
            }?,
            MappedRecord::Substitution(r) => self.update_on_substitution(r)?,
            MappedRecord::BatHandAdjustment(r) => self.update_on_bat_hand_adjustment(r),
            MappedRecord::PitchHandAdjustment(r) => self.update_on_pitch_hand_adjustment(r),
            // Nothing to do here, since we map player to batting order anyway
            MappedRecord::LineupAdjustment(_) => (),
            MappedRecord::RunnerAdjustment(r) => self.update_on_runner_adjustment(r)?,
            MappedRecord::PitcherResponsibilityAdjustment(r) => {
                self.update_on_pitcher_responsibility_adjustment(r)?;
            }
            MappedRecord::Comment(r) => self.update_on_comment(r),
            _ => {}
        };

        Ok(())
    }
}

pub type Outs = BoundedUsize<0, 3>;

#[derive(Debug, Eq, PartialEq, Default, Clone, Serialize)]
pub struct BaseState {
    bases: Map<BaseRunner, Runner>,
    scored: ArrayVec<Runner, 4>,
}

impl BaseState {
    pub fn new_inning_tiebreaker(new_runner: LineupPosition, event_id: EventId) -> Self {
        let mut state = Self::default();
        let runner = Runner {
            lineup_position: new_runner,
            reached_on_event_id: event_id,
            charge_event_id: event_id,
            explicit_charged_pitcher_id: None,
        };
        state.bases.insert(BaseRunner::Second, runner);
        state
    }

    pub fn get_from_position(&self, position: LineupPosition) -> Option<&Runner> {
        self.bases.iter().find_map(|(_, runner)| {
            if runner.lineup_position == position {
                Some(runner)
            } else {
                None
            }
        })
    }

    pub fn get_base_state(&self) -> u8 {
        // Integer representation of the base state with each binary digit representing a base
        self.get_first().is_some() as u8
            | (self.get_second().is_some() as u8) << 1
            | (self.get_third().is_some() as u8) << 2
    }

    fn num_runners_on_base(&self) -> usize {
        self.bases.len()
    }

    pub fn get_runner(&self, baserunner: BaseRunner) -> Option<&Runner> {
        self.bases.get(baserunner)
    }

    fn get_first(&self) -> Option<&Runner> {
        self.bases.get(BaseRunner::First)
    }

    fn get_second(&self) -> Option<&Runner> {
        self.bases.get(BaseRunner::Second)
    }

    fn get_third(&self) -> Option<&Runner> {
        self.bases.get(BaseRunner::Third)
    }

    fn clear_baserunner(&mut self, baserunner: BaseRunner) -> Option<Runner> {
        self.bases.remove(baserunner)
    }

    fn set_runner(&mut self, baserunner: BaseRunner, runner: Runner) {
        self.bases.insert(baserunner, runner);
    }

    fn iter_in_reverse_order(&mut self) -> impl Iterator<Item = (BaseRunner, &mut Runner)> {
        self.bases.iter_mut().sorted_by(|(a, _), (b, _)| b.cmp(a))
    }

    fn get_advance_from_baserunner(
        baserunner: BaseRunner,
        play: &PlayRecord,
    ) -> Option<&RunnerAdvance> {
        play.stats
            .advances
            .iter()
            .find(|a| a.baserunner == baserunner)
    }

    fn current_base_occupied(&self, advance: &RunnerAdvance) -> bool {
        self.get_runner(advance.baserunner).is_some()
    }

    fn target_base_occupied(&self, advance: &RunnerAdvance) -> bool {
        let br = BaseRunner::from_target_base(advance.to);
        self.get_runner(br).is_some()
    }

    fn check_integrity(old_state: &Self, new_state: &Self, advance: &RunnerAdvance) -> Result<()> {
        if new_state.target_base_occupied(advance) {
            bail!("Runner is listed as moving to a base that is occupied by another runner")
        } else if old_state.current_base_occupied(advance) {
            Ok(())
        } else {
            bail!(
                "Advancement from a base that had no runner on it.\n\
            Old state: {:?}\n\
            New state: {:?}\n\
            Advance: {:?}\n",
                old_state,
                new_state,
                advance
            )
        }
    }

    ///  Accounts for Rule 9.16(g) regarding the assignment of trailing
    ///  baserunners as inherited if they advance on a fielder's choice 🙃.
    ///  Returns the charge_event_id of the new batter, if applicable.
    fn update_runner_charges(&mut self, play: &PlayRecord) -> Result<Option<EventId>> {
        let mut charge_event_id = None;
        for out_baserunner in &play.stats.batter_caused_baserunning_outs {
            let out_runner = self
                .get_runner(*out_baserunner)
                .context("No runner on base")?;
            charge_event_id = Some(out_runner.charge_event_id);
            for (baserunner, runner) in self.iter_in_reverse_order() {
                if baserunner < *out_baserunner {
                    let new_charge_event_id = runner.charge_event_id;
                    // This is a safe unwrap because it has to be Some to reach this code
                    runner.charge_event_id = charge_event_id.unwrap();
                    charge_event_id = Some(new_charge_event_id);
                }
            }
        }

        Ok(charge_event_id)
    }

    pub(crate) fn new_base_state(
        &self,
        start_inning: bool,
        end_inning: bool,
        play: &PlayRecord,
        batter_lineup_position: LineupPosition,
        event_id: EventId,
    ) -> Result<Self> {
        let mut new_state = if start_inning {
            Self::default()
        } else {
            Self {
                bases: self.bases,
                scored: ArrayVec::new(),
            }
        };
        let batter_charge_event_id = if !start_inning {
            new_state.update_runner_charges(play)?
        } else {
            None
        };

        // Cover cases where outs are not included in advance information
        for out in &play.stats.outs {
            new_state.clear_baserunner(*out);
        }

        if let Some(a) = Self::get_advance_from_baserunner(BaseRunner::Third, play) {
            new_state.clear_baserunner(BaseRunner::Third);
            if a.is_out() {
            } else if let Err(e) = Self::check_integrity(self, &new_state, a) {
                return Err(e);
            } else if let Some(r) = self.get_third() {
                new_state.scored.push(*r);
            }
        }
        if let Some(a) = Self::get_advance_from_baserunner(BaseRunner::Second, play) {
            new_state.clear_baserunner(BaseRunner::Second);
            if a.is_out() {
            } else if let Err(e) = Self::check_integrity(self, &new_state, a) {
                return Err(e);
            } else if let (true, Some(r)) = (
                a.is_this_that_one_time_jean_segura_ran_in_reverse(),
                self.get_second(),
            ) {
                new_state.set_runner(BaseRunner::First, *r);
            } else if let (Base::Third, Some(r)) = (a.to, self.get_second()) {
                new_state.set_runner(BaseRunner::Third, *r);
            } else if let (Base::Home, Some(r)) = (a.to, self.get_second()) {
                new_state.scored.push(*r);
            }
        }
        if let Some(a) = Self::get_advance_from_baserunner(BaseRunner::First, play) {
            new_state.clear_baserunner(BaseRunner::First);
            if a.is_out() {
            } else if let Err(e) = Self::check_integrity(self, &new_state, a) {
                return Err(e);
            } else if let (Base::Second, Some(r)) = (&a.to, self.get_first()) {
                new_state.set_runner(BaseRunner::Second, *r);
            } else if let (Base::Third, Some(r)) = (&a.to, self.get_first()) {
                new_state.set_runner(BaseRunner::Third, *r);
            } else if let (Base::Home, Some(r)) = (&a.to, self.get_first()) {
                new_state.scored.push(*r);
            }
        }
        if let Some(a) = Self::get_advance_from_baserunner(BaseRunner::Batter, play) {
            let new_runner = Runner {
                lineup_position: batter_lineup_position,
                reached_on_event_id: event_id,
                charge_event_id: batter_charge_event_id.unwrap_or(event_id),
                explicit_charged_pitcher_id: None,
            };
            match a.to {
                _ if a.is_out() || end_inning => {}
                _ if new_state.target_base_occupied(a) => {
                    return Err(anyhow!("Batter advanced to an occupied base"))
                }
                Base::Home => new_state.scored.push(new_runner),
                b => new_state.set_runner(BaseRunner::from_current_base(b), new_runner),
            }
        }
        Ok(new_state)
    }
}

#[derive(Debug, Eq, PartialEq, Copy, Clone, Serialize)]
pub struct Runner {
    pub lineup_position: LineupPosition,
    pub reached_on_event_id: EventId,
    /// This differs from the `reached_on` field in the event of a force-out
    /// or fielder's choice. The reason we track event ID instead of
    /// the pitcher is so that we can compute run assignments for any
    /// fielder in the same way.
    pub charge_event_id: EventId,
    /// However, there are some cases where the pitcher is explicitly
    /// charged with the baserunner.
    pub explicit_charged_pitcher_id: Option<Pitcher>,
}

/// Returns a dummy version of `GameContext` that
/// has at least one entry in each of its Vecs
#[allow(clippy::pedantic, clippy::nursery, clippy::unwrap_used)]
pub fn dummy() -> GameContext {
    let team = ArrayString::from("ABC").unwrap();
    let dummy_str8 = ArrayString::from("dummy").unwrap();
    let dummy_str16 = ArrayString::from("dummy").unwrap();
    let dummy_datetime = NaiveDateTime::from_timestamp_opt(0, 0).unwrap();
    let dummy_base_state = BaseState {
        bases: vec![(
            BaseRunner::First,
            Runner {
                lineup_position: LineupPosition::PitcherWithDh,
                explicit_charged_pitcher_id: Some(dummy_str8),
                reached_on_event_id: EventId::new(1).unwrap(),
                charge_event_id: EventId::new(1).unwrap(),
            },
        )]
        .into_iter()
        .collect(),
        scored: ArrayVec::new(),
    };
    GameContext {
        game_id: GameId {
            id: GameIdString::default(),
        },
        file_info: FileInfo {
            filename: ArrayString::from("dummy").unwrap(),
            account_type: AccountType::BoxScore,
            file_index: 0,
        },
        metadata: GameMetadata {
            scorer: Some(dummy_str16),
            how_scored: HowScored::Unknown,
            inputter: Some(dummy_str16),
            translator: Some(dummy_str16),
            date_inputted: Some(dummy_datetime),
            date_edited: Some(dummy_datetime),
        },
        teams: Matchup {
            away: team,
            home: team,
        },
        setting: GameSetting {
            date: NaiveDate::MIN,
            start_time: Some(NaiveTime::default()),
            game_type: GameType::RegularSeason,
            doubleheader_status: DoubleheaderStatus::DoubleHeaderGame1,
            time_of_day: DayNight::Day,
            bat_first_side: Side::Away,
            sky: Sky::Unknown,
            field_condition: FieldCondition::Unknown,
            precipitation: Precipitation::Unknown,
            wind_direction: WindDirection::Unknown,
            season: Season(1990),
            park_id: dummy_str16,
            temperature_fahrenheit: Some(1),
            attendance: Some(1),
            wind_speed_mph: Some(1),
            use_dh: true,
        },
        umpires: vec![GameUmpire {
            game_id: ArrayString::from("dummy").unwrap(),
            umpire_id: Some(dummy_str8),
            position: UmpirePosition::Home,
        }],
        results: GameResults {
            winning_pitcher: Some(dummy_str8),
            losing_pitcher: Some(dummy_str8),
            save_pitcher: Some(dummy_str8),
            game_winning_rbi: Some(dummy_str8),
            time_of_game_minutes: Some(1),
            protest_info: Some(String::from("dummy")),
            completion_info: Some(String::from("dummy")),
            earned_runs: vec![EarnedRunRecord {
                pitcher_id: dummy_str8,
                earned_runs: 1,
            }],
        },
        lineup_appearances: vec![GameLineupAppearance {
            game_id: ArrayString::from("dummy").unwrap(),
            player_id: dummy_str8,
            lineup_position: LineupPosition::PitcherWithDh,
            side: Side::Away,
            entered_game_as: EnteredGameAs::Starter,
            start_event_id: EventId::new(1).unwrap(),
            end_event_id: Some(EventId::new(1).unwrap()),
        }],
        fielding_appearances: vec![GameFieldingAppearance {
            game_id: ArrayString::from("dummy").unwrap(),
            player_id: dummy_str8,
            fielding_position: FieldingPosition::Pitcher,
            side: Side::Away,
            start_event_id: EventId::new(1).unwrap(),
            end_event_id: Some(EventId::new(1).unwrap()),
        }],
        events: vec![Event {
            game_id: GameId {
                id: GameIdString::default(),
            },
            event_id: EventId::new(1).unwrap(),
            context: EventContext {
                inning: 1,
                batting_side: Side::Away,
                frame: InningFrame::Top,
                at_bat: LineupPosition::PitcherWithDh,
                batter_id: dummy_str8,
                pitcher_id: dummy_str8,
                outs: Outs::new(0).unwrap(),
                starting_base_state: dummy_base_state.clone(),
                rare_attributes: RareAttributes {
                    batter_hand: Some(Hand::Left),
                    pitcher_hand: Some(Hand::Left),
                    strikeout_responsible_batter: Some(dummy_str8),
                    walk_responsible_pitcher: Some(dummy_str8),
                },
            },
            results: EventResults {
                count_at_event: Count {
                    balls: Some(BoundedU8::new(1).unwrap()),
                    strikes: Some(BoundedU8::new(1).unwrap()),
                },
                pitch_sequence: Arc::new(vec![PitchSequenceItem {
                    sequence_id: SequenceId::new(1).unwrap(),
                    pitch_type: PitchType::CalledStrike,
                    blocked_by_catcher: false,
                    runners_going: false,
                    catcher_pickoff_attempt: Some(Base::First),
                }]),
                plate_appearance: Some(PlateAppearanceResultType::Single),
                batted_ball_info: Some(EventBattedBallInfo::default()),
                plays_at_base: vec![EventBaserunningPlay {
                    event_key: 1,
                    sequence_id: SequenceId::new(1).unwrap(),
                    baserunning_play_type: BaserunningPlayType::Balk,
                    baserunner: Some(BaseRunner::Batter),
                }],
                baserunning_advances: vec![EventBaserunningAdvanceAttempt {
                    event_key: 1,
                    sequence_id: SequenceId::new(1).unwrap(),
                    baserunner: BaseRunner::Batter,
                    attempted_advance_to: Base::Second,
                    is_successful: true,
                    advanced_on_error_flag: true,
                    explicit_out_flag: true,
                    run_scored_flag: true,
                    rbi_flag: true,
                    team_unearned_flag: true,
                }],
                runs: vec![EventRun {
                    event_key: 1,
                    runner: BaseRunner::Batter,
                    rbi_flag: true,
                    explicit_unearned_run_status: Some(UnearnedRunStatus::TeamUnearned),
                }],
                play_info: vec![EventFlag {
                    event_key: 1,
                    sequence_id: SequenceId::new(1).unwrap(),
                    flag: String::from("dummy"),
                }],
                comment: vec![String::from("dummy")],
                fielding_plays: vec![FieldersData {
                    fielding_position: FieldingPosition::Pitcher,
                    fielding_play_type: FieldingPlayType::Assist,
                }],
                out_on_play: vec![BaseRunner::Batter],
                ending_base_state: dummy_base_state.clone(),
                no_play_flag: false,
            },
            line_number: 1,
            event_key: 2,
        }],
        line_offset: 1,
        event_key_offset: 3,
        box_score_data: Some(BoxScoreData {
            lines: vec![],
            events: vec![],
            line_scores: vec![],
            comments: vec![],
        }),
    }
}

mod base_state;
mod personnel;
mod transitions;
mod validation;

use std::collections::HashMap;
use std::convert::TryFrom;
use std::sync::Arc;

use anyhow::{Context, Error, Result, anyhow, bail};
use arrayvec::ArrayString;
use bounded_integer::{BoundedU8, BoundedUsize};
use chrono::{DateTime, NaiveDate, NaiveDateTime, NaiveTime};
use itertools::Itertools;
use serde::{Deserialize, Serialize};
use strum_macros::AsRefStr;
use tracing::warn;

use crate::AccountType;
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
    Base, BaseRunner, BaserunningPlayType, Count, FieldersData, FieldingData, HitType, InningFrame,
    OtherPlateAppearance, OutAtBatType, PlateAppearanceType, PlayModifier, PlayRecord, PlayType,
    Trajectory, UnearnedRunStatus,
};
use crate::event_file::traits::{
    FieldingPosition, Inning, LineupPosition, MAX_EVENTS_PER_GAME, Matchup, Player,
    RetrosheetVolunteer, Scorer, SequenceId, Side, Umpire,
};

use super::box_score::{BoxScoreEvent, BoxScoreLine, LineScore};
use super::pitch_sequence::{PitchSequence, PitchSequenceItem, PitchType};
use super::play::{
    BattedBallAngle, BattedBallDepth, BattedBallLocationGeneral, BattedBallStrength,
    RunnerAdvanceModifier,
};
use super::schemas::GameIdString;
use super::traits::{EventKey, FieldingPlayType, GameType};

const UNKNOWN_STRINGS: [&str; 1] = ["unknown"];
const NONE_STRINGS: [&str; 2] = ["(none)", "none"];

pub type EventId = SequenceId;

use personnel::{AppearanceDelta, Personnel, PositionType, TrackedPlayer};
use validation::{get_game_id, info_presence, reconcile_with_game_id};

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

    pub const fn is_in_play(self) -> bool {
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
                    && a.end_event_id.is_none_or(|end| end >= event_id)
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
    fn from_record_slice(slice: &RecordSlice) -> Self {
        let mut lines = Vec::new();
        let mut events = Vec::new();
        let mut line_scores = Vec::new();
        let mut comments = Vec::new();
        for record in slice {
            match record {
                MappedRecord::BoxScoreLine(bsl) => lines.push(*bsl),
                MappedRecord::BoxScoreEvent(bse) => events.push(bse.clone()),
                MappedRecord::LineScore(bsls) => line_scores.push(bsls.clone()),
                MappedRecord::Comment(c) => comments.push(c.clone()),
                _ => {}
            }
        }
        Self {
            lines,
            events,
            line_scores,
            comments,
        }
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
        let mut setting = GameSetting::from(record_slice);
        reconcile_with_game_id(game_id, &mut setting, &teams, info_presence(record_slice));
        let metadata = GameMetadata::from(record_slice);
        let umpires = GameUmpire::from_record_slice(record_slice)?;
        let results = GameResults::from(record_slice);
        let event_key_offset = Self::event_key_offset(file_info, game_num)?;
        let box_score_data = if file_info.account_type == AccountType::BoxScore {
            Some(BoxScoreData::from_record_slice(record_slice))
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
    pub trajectory: Trajectory,
    pub hit_to_fielder: Option<FieldingPosition>,
    pub general_location: BattedBallLocationGeneral,
    pub depth: BattedBallDepth,
    pub angle: BattedBallAngle,
    pub strength: BattedBallStrength,
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
                        trajectory: contact_description.trajectory.unwrap_or_default(),
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
#[allow(clippy::struct_excessive_bools)]
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
    pub fn is_team_unearned_run(self) -> bool {
        self.explicit_unearned_run_status
            .is_some_and(|s| s == UnearnedRunStatus::TeamUnearned)
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
#[allow(clippy::struct_field_names)]
pub struct Event {
    pub game_id: GameId,
    pub event_id: EventId,
    pub event_key: EventKey,
    pub context: EventContext,
    pub results: EventResults,
    pub line_number: usize,
    pub raw_play: Arc<String>,
}

impl Event {
    pub fn summary(&self) -> String {
        format!(
            r"
        Event: {event_id}
        Inning: {frame:?} {inning}
        Outs at event: {outs_at_event}
        Batter: {ab:?}
        Plate appearance result: {pa:?}
        Baserunning: {ba:?}
        Out on play: {out:?}
        ",
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

/// Holds `com,...` records seen between plays; drained onto the next event.
#[derive(Debug, Eq, PartialEq, Clone, Default)]
struct CommentAccumulator {
    buffer: Vec<String>,
}

impl CommentAccumulator {
    fn push(&mut self, comment: &str) {
        // `$` prefix marks an internal scoring note in Retrosheet; strip it.
        self.buffer.push(comment.trim().replace('$', ""));
    }

    fn drain(&mut self) -> Vec<String> {
        std::mem::take(&mut self.buffer)
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
    lineup_appearances: HashMap<TrackedPlayer, Vec<GameLineupAppearance>>,
    fielding_appearances: HashMap<TrackedPlayer, Vec<GameFieldingAppearance>>,
    unusual_state: RareAttributes,
    comments: CommentAccumulator,
}

/// Pre-update view of `GameState` fields needed by the event we're about to
/// build. Taken before `update` mutates `self`.
struct PrePlaySnapshot {
    starting_base_state: BaseState,
    starting_outs: Outs,
    rare_attributes: RareAttributes,
}

impl GameState {
    /// On a frame flip, bases/outs reset to defaults — the new event belongs
    /// to the new half-inning and shouldn't inherit the previous frame's tail.
    fn capture_pre_play_snapshot(&self, opt_play: Option<&PlayRecord>) -> Result<PrePlaySnapshot> {
        let frame_flipped =
            opt_play.is_some_and(|p| transitions::frame_changed(self.batting_side, p.batting_side));
        let (starting_base_state, starting_outs) = if frame_flipped {
            (
                BaseState::default(),
                Outs::new(0).context("Unexpected outs bound error")?,
            )
        } else {
            (self.bases.clone(), self.outs)
        };
        Ok(PrePlaySnapshot {
            starting_base_state,
            starting_outs,
            rare_attributes: self.unusual_state,
        })
    }

    /// Must run after `update`: reads post-play `inning`/`batting_side`/
    /// `frame`/`at_bat` and `bases` (as `ending_base_state`).
    fn build_event(
        &mut self,
        play: &PlayRecord,
        pre_state: PrePlaySnapshot,
        line_number: usize,
        event_key: EventKey,
    ) -> Result<Event> {
        let context = EventContext {
            inning: self.inning,
            batting_side: self.batting_side,
            frame: self.frame,
            at_bat: self.at_bat,
            batter_id: play.batter,
            pitcher_id: self.personnel.pitcher(self.batting_side.flip())?,
            outs: pre_state.starting_outs,
            starting_base_state: pre_state.starting_base_state,
            rare_attributes: pre_state.rare_attributes,
        };
        let results = EventResults {
            count_at_event: play.count,
            pitch_sequence: play.pitch_sequence.clone(),
            plate_appearance: PlateAppearanceResultType::from_play(play),
            batted_ball_info: EventBattedBallInfo::from_play(play, event_key),
            plays_at_base: EventBaserunningPlay::from_play(play, event_key)?,
            baserunning_advances: EventBaserunningAdvanceAttempt::from_play(play, event_key)?,
            runs: EventRun::from_play(play, event_key),
            play_info: EventFlag::from_play(play, event_key)?,
            comment: self.comments.drain(),
            fielding_plays: play.stats.fielders_data.clone(),
            out_on_play: play.stats.outs.clone(),
            ending_base_state: self.bases.clone(),
            no_play_flag: play.stats.no_play_flag,
        };
        Ok(Event {
            game_id: self.game_id,
            event_id: self.event_id,
            context,
            results,
            line_number,
            event_key,
            raw_play: play.raw.clone(),
        })
    }

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
            let pre_state = state.capture_pre_play_snapshot(opt_play)?;
            state.update(record, opt_play)?;
            if let Some(play) = opt_play {
                let line_number = line_offset + i;
                events.push(state.build_event(play, pre_state, line_number, event_key)?);
                state.event_id += 1;
            }
        }
        // Set all remaining blank end_event_ids to final event
        let max_event_id = EventId::new(events.len()).context("No events in list")?;
        let lineup_appearances = state
            .lineup_appearances
            .values()
            .flatten()
            .map(|la| la.finalize(max_event_id))
            .sorted_by_key(|la| (la.side, la.lineup_position, la.start_event_id))
            .collect_vec();
        let defense_appearances = state
            .fielding_appearances
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

        let (personnel, starts_delta) = personnel::from_starts(record_slice)?;
        let mut state = Self {
            game_id,
            event_id: EventId::new(1).context("Unexpected event ID bound error")?,
            inning: 1,
            frame: InningFrame::Top,
            count: Count::default(),
            batting_side,
            outs: Outs::new(0).context("Unexpected outs bound error")?,
            bases: BaseState::default(),
            at_bat: LineupPosition::default(),
            personnel,
            lineup_appearances: HashMap::with_capacity(30),
            fielding_appearances: HashMap::with_capacity(30),
            unusual_state: RareAttributes::default(),
            comments: CommentAccumulator::default(),
        };
        state.fold_appearance_delta(starts_delta)?;
        Ok(state)
    }

    fn fold_appearance_delta(&mut self, delta: AppearanceDelta) -> Result<()> {
        for (player, eid) in delta.close_lineup {
            let entry = self
                .lineup_appearances
                .get_mut(&player)
                .with_context(|| {
                    anyhow!("Cannot find existing player {player} in lineup appearance records")
                })?
                .last_mut()
                .with_context(|| {
                    anyhow!("Player {player} has an empty list of lineup appearances")
                })?;
            entry.end_event_id = Some(eid);
        }
        for (player, eid) in delta.close_fielding {
            let entry = self
                .fielding_appearances
                .get_mut(&player)
                .with_context(|| {
                    anyhow!("Cannot find existing player {player} in fielding appearance records")
                })?
                .last_mut()
                .with_context(|| {
                    anyhow!("Player {player} has an empty list of fielding appearances")
                })?;
            entry.end_event_id = Some(eid);
        }
        for (player, appearance) in delta.new_lineup {
            self.lineup_appearances
                .entry(player)
                .or_insert_with(|| Vec::with_capacity(1))
                .push(appearance);
        }
        for (player, appearance) in delta.new_fielding {
            self.fielding_appearances
                .entry(player)
                .or_insert_with(|| Vec::with_capacity(1))
                .push(appearance);
        }
        Ok(())
    }

    fn update_on_play(&mut self, play: &PlayRecord) -> Result<()> {
        let flipped = transitions::frame_changed(self.batting_side, play.batting_side);
        if flipped && self.outs.get() < 3 {
            bail!("New frame without 3 outs recorded")
        }
        let new_frame = transitions::next_frame(self.frame, flipped);
        let new_outs = transitions::outs_after_play(self.outs, flipped, play.stats.outs.len())?;

        let batter_lineup_position = self.personnel.at_bat(play)?;

        let new_base_state = base_state::advance_base_state(
            &self.bases,
            flipped,
            new_outs == 3,
            play,
            batter_lineup_position,
            self.event_id,
        )?;

        let is_mid_plate_appearance = play.stats.plate_appearance.is_none() && new_outs < 3;

        if is_mid_plate_appearance {
            self.count = play.count;
            // Hand adjustments are reset in all circumstances, including mid-PA
            self.unusual_state.batter_hand = None;
            self.unusual_state.pitcher_hand = None;
        } else {
            self.count = Count::default();
            // All unusual state characteristics are reset on a new PA
            self.unusual_state = RareAttributes::default();
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
        if transitions::detect_mid_pa_strikeout_responsible(
            self.at_bat,
            self.batting_side,
            self.count,
            record,
        ) {
            let batter = self
                .personnel
                .get_at_position(record.side, PositionType::Lineup(record.lineup_position))?
                .player;
            self.unusual_state.strikeout_responsible_batter = Some(batter);
        } else if transitions::detect_mid_pa_walk_responsible(self.batting_side, self.count, record)
        {
            self.unusual_state.walk_responsible_pitcher =
                Some(self.personnel.pitcher(record.side)?);
        }
        let (next_personnel, sub_delta) = personnel::apply_substitution(
            &self.personnel,
            &self.lineup_appearances,
            &self.fielding_appearances,
            record,
            self.event_id,
        )?;
        self.personnel = next_personnel;
        self.fold_appearance_delta(sub_delta)?;
        if record.fielding_position == FieldingPosition::Pitcher
            && record.lineup_position != LineupPosition::PitcherWithDh
        {
            let dh_delta = personnel::apply_dh_vacancy(&self.personnel, record, self.event_id);
            self.fold_appearance_delta(dh_delta)?;
        }
        Ok(())
    }

    const fn update_on_bat_hand_adjustment(&mut self, record: &BatHandAdjustment) {
        self.unusual_state.batter_hand = Some(record.hand);
    }

    const fn update_on_pitch_hand_adjustment(&mut self, record: &PitchHandAdjustment) {
        self.unusual_state.batter_hand = Some(record.hand);
    }

    fn update_on_runner_adjustment(&mut self, record: &RunnerAdjustment) -> Result<()> {
        let delta = transitions::apply_runner_adjustment(
            self.frame,
            self.batting_side,
            self.outs,
            &self.lineup_appearances,
            record,
            self.event_id,
        )?;
        self.frame = delta.new_frame;
        self.batting_side = delta.new_side;
        self.outs = delta.new_outs;
        self.bases = delta.new_bases;
        Ok(())
    }

    fn update_on_comment(&mut self, comment: &str) {
        self.comments.push(comment);
    }

    fn update_on_pitcher_responsibility_adjustment(
        &mut self,
        record: &PitcherResponsibilityAdjustment,
    ) {
        // Real corpus contains adjustments that name a base with no runner
        // (e.g. BSN191409102 in 1914BSN.EVN: `presadj,oescj101,1` after a
        // half-inning where only 2B is occupied). Skip with a warning so the
        // game still parses. ER attribution for that game loses the override
        // but everything else is preserved.
        let (next, warning) = base_state::apply_pitcher_responsibility(&self.bases, record);
        self.bases = next;
        if let Some(w) = warning {
            warn!(
                "Skipping pitcher responsibility adjustment for non-existent runner: {:?}",
                w.adjustment
            );
        }
    }

    pub fn update(&mut self, record: &MappedRecord, play: Option<&PlayRecord>) -> Result<()> {
        match record {
            // We've already pulled the play record out before the call to this function
            MappedRecord::Play(_) => {
                if let Some(cp) = play {
                    self.update_on_play(cp)
                        .with_context(|| anyhow!("Failed to parse play {cp:?}"))
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
                self.update_on_pitcher_responsibility_adjustment(r);
            }
            MappedRecord::Comment(r) => self.update_on_comment(r),
            _ => {}
        }

        Ok(())
    }
}

pub type Outs = BoundedUsize<0, 3>;

pub use base_state::{BaseState, Runner};

/// Returns a dummy version of `GameContext` that
/// has at least one entry in each of its Vecs
#[allow(clippy::pedantic, clippy::nursery, clippy::unwrap_used)]
pub fn dummy() -> GameContext {
    let team = ArrayString::from("ABC").unwrap();
    let dummy_str8 = ArrayString::from("dummy").unwrap();
    let dummy_str16 = ArrayString::from("dummy").unwrap();
    let dummy_datetime = DateTime::from_timestamp(0, 0).unwrap().naive_utc();
    let mut dummy_base_state = BaseState::default();
    dummy_base_state.set_runner(
        BaseRunner::First,
        Runner {
            lineup_position: LineupPosition::PitcherWithDh,
            explicit_charged_pitcher_id: Some(dummy_str8),
            reached_on_event_id: EventId::new(1).unwrap(),
            charge_event_id: EventId::new(1).unwrap(),
        },
    );
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
            raw_play: Arc::new(String::from("dummy")),
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

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::*;
    use crate::event_file::misc::str_to_tinystr;
    use crate::event_file::play::{Balls, Base, BaseRunner, Strikes};
    use crate::event_file::traits::{Pitcher, Side};
    use csv::StringRecord;

    fn rec(fields: &[&str]) -> StringRecord {
        StringRecord::from(fields.to_vec())
    }

    fn make_runner(lineup: LineupPosition, event_id: u8) -> Runner {
        Runner {
            lineup_position: lineup,
            reached_on_event_id: EventId::new(event_id.into()).unwrap(),
            charge_event_id: EventId::new(event_id.into()).unwrap(),
            explicit_charged_pitcher_id: None,
        }
    }

    /// Build a record slice with one starter per lineup position on each side.
    /// Lineup positions 1..=8 are non-pitcher fielders (positions 2..=9);
    /// the 9-spot is the pitcher (fielding position 1). Player IDs follow the
    /// pattern `{side}{lineup}001` where side is `a`/`h`.
    fn build_starter_slice() -> Vec<MappedRecord> {
        let mut records = vec![MappedRecord::try_from(&rec(&["id", "ABC202404010"])).unwrap()];
        for (side_idx, side_letter) in [(0, "a"), (1, "h")] {
            for lineup in 1..=9u8 {
                // lineup 1..=8 -> fielding 2..=9 (non-pitcher); lineup 9 -> pitcher (1)
                let fielding = if lineup == 9 { 1 } else { lineup + 1 };
                let player_id = format!("{side_letter}{lineup}001");
                let name = format!("{side_letter}{lineup}");
                let lineup_str = lineup.to_string();
                let fielding_str = fielding.to_string();
                let side_str = side_idx.to_string();
                records.push(
                    MappedRecord::try_from(&rec(&[
                        "start",
                        &player_id,
                        &name,
                        &side_str,
                        &lineup_str,
                        &fielding_str,
                    ]))
                    .unwrap(),
                );
            }
        }
        records
    }

    fn make_state() -> GameState {
        GameState::new(&build_starter_slice()).unwrap()
    }

    // --- BaseState invariants ---

    #[test]
    fn base_state_default_is_empty() {
        let bs = BaseState::default();
        assert_eq!(bs.num_runners_on_base(), 0);
        assert!(bs.get_runner(BaseRunner::First).is_none());
        assert!(bs.get_runner(BaseRunner::Second).is_none());
        assert!(bs.get_runner(BaseRunner::Third).is_none());
        assert_eq!(bs.get_base_state(), 0);
    }

    #[test]
    fn base_state_set_then_get_returns_runner() {
        let mut bs = BaseState::default();
        let runner = make_runner(LineupPosition::Third, 7);
        bs.set_runner(BaseRunner::Second, runner);
        let got = bs.get_runner(BaseRunner::Second).unwrap();
        assert_eq!(got.lineup_position, LineupPosition::Third);
        assert_eq!(got.reached_on_event_id, EventId::new(7).unwrap());
        assert_eq!(bs.num_runners_on_base(), 1);
    }

    #[test]
    fn base_state_clear_after_set_is_empty() {
        let mut bs = BaseState::default();
        bs.set_runner(BaseRunner::First, make_runner(LineupPosition::First, 1));
        assert!(bs.get_runner(BaseRunner::First).is_some());
        bs.clear_baserunner(BaseRunner::First);
        assert!(bs.get_runner(BaseRunner::First).is_none());
        assert_eq!(bs.num_runners_on_base(), 0);
    }

    #[test]
    fn base_state_full_bases_three_runners() {
        let mut bs = BaseState::default();
        bs.set_runner(BaseRunner::First, make_runner(LineupPosition::First, 1));
        bs.set_runner(BaseRunner::Second, make_runner(LineupPosition::Second, 2));
        bs.set_runner(BaseRunner::Third, make_runner(LineupPosition::Third, 3));
        assert_eq!(bs.num_runners_on_base(), 3);
        assert_eq!(bs.get_base_state(), 0b111);
    }

    #[test]
    fn base_state_get_base_state_encodes_occupancy_bitfield() {
        // Each table row asserts the bitfield contract derived from the
        // BaseState definition: bit 0 = First, bit 1 = Second, bit 2 = Third.
        let cases = [
            (vec![], 0b000),
            (vec![BaseRunner::First], 0b001),
            (vec![BaseRunner::Second], 0b010),
            (vec![BaseRunner::Third], 0b100),
            (vec![BaseRunner::First, BaseRunner::Third], 0b101),
        ];
        for (occupied, expected) in cases {
            let mut bs = BaseState::default();
            for (i, br) in occupied.iter().enumerate() {
                let event_id = u8::try_from(i + 1).unwrap();
                bs.set_runner(*br, make_runner(LineupPosition::First, event_id));
            }
            assert_eq!(
                bs.get_base_state(),
                expected,
                "expected {expected:#05b} for {occupied:?}"
            );
        }
    }

    #[test]
    fn base_state_new_inning_tiebreaker_places_only_on_second() {
        let bs = BaseState::new_inning_tiebreaker(LineupPosition::Fourth, EventId::new(5).unwrap());
        assert!(bs.get_runner(BaseRunner::First).is_none());
        let runner = bs.get_runner(BaseRunner::Second).unwrap();
        assert_eq!(runner.lineup_position, LineupPosition::Fourth);
        assert!(bs.get_runner(BaseRunner::Third).is_none());
        assert_eq!(bs.num_runners_on_base(), 1);
    }

    #[test]
    fn base_state_get_from_position_finds_runner_by_lineup_position() {
        let mut bs = BaseState::default();
        bs.set_runner(BaseRunner::Third, make_runner(LineupPosition::Sixth, 4));
        let runner = bs.get_from_position(LineupPosition::Sixth).unwrap();
        assert_eq!(runner.reached_on_event_id, EventId::new(4).unwrap());
        assert!(bs.get_from_position(LineupPosition::Ninth).is_none());
    }

    // --- GameState bootstrap ---

    #[test]
    fn game_state_new_initializes_to_top_first_zero_outs() {
        let gs = make_state();
        assert_eq!(gs.inning, 1);
        assert_eq!(gs.frame, InningFrame::Top);
        assert_eq!(gs.outs, Outs::new(0).unwrap());
        assert_eq!(gs.bases.num_runners_on_base(), 0);
        // Default home-bats-first is false in absence of an info record, so away bats first.
        assert_eq!(gs.batting_side, Side::Away);
    }

    // --- update_on_pitcher_responsibility_adjustment ---

    #[test]
    fn update_on_pitcher_responsibility_adjustment_sets_charge_on_existing_runner() {
        let mut gs = make_state();
        // Put a runner on second so the adjustment has a target.
        gs.bases
            .set_runner(BaseRunner::Second, make_runner(LineupPosition::Fifth, 3));
        let pitcher: Pitcher = str_to_tinystr("relp001").unwrap();
        let adj = PitcherResponsibilityAdjustment {
            pitcher_id: pitcher,
            baserunner: BaseRunner::Second,
        };
        gs.update_on_pitcher_responsibility_adjustment(&adj);
        assert_eq!(
            gs.bases
                .get_runner(BaseRunner::Second)
                .unwrap()
                .explicit_charged_pitcher_id,
            Some(pitcher)
        );
    }

    #[test]
    fn update_on_pitcher_responsibility_adjustment_skips_when_base_empty() {
        // Regression: BSN191409102 ships a presadj for an empty base. The
        // parser must skip with a warn rather than panic; surrounding state
        // (outs, frame, bases) stays put.
        let mut gs = make_state();
        let outs_before = gs.outs;
        let frame_before = gs.frame;
        let runners_before = gs.bases.num_runners_on_base();
        let adj = PitcherResponsibilityAdjustment {
            pitcher_id: str_to_tinystr("relp001").unwrap(),
            baserunner: BaseRunner::First,
        };
        gs.update_on_pitcher_responsibility_adjustment(&adj);
        assert!(gs.bases.get_runner(BaseRunner::First).is_none());
        assert_eq!(gs.outs, outs_before);
        assert_eq!(gs.frame, frame_before);
        assert_eq!(gs.bases.num_runners_on_base(), runners_before);
    }

    // --- update_on_runner_adjustment ---

    #[test]
    fn update_on_runner_adjustment_flips_frame_and_places_runner_on_second() {
        let mut gs = make_state();
        // Simulate end of inning: 3 outs recorded, away batting.
        gs.outs = Outs::new(3).unwrap();
        gs.frame = InningFrame::Top;
        gs.batting_side = Side::Away;
        // Use a known starter (h1001 is the home leadoff hitter).
        let adj = RunnerAdjustment {
            runner_id: str_to_tinystr("h1001").unwrap(),
            base: Base::Second,
        };
        gs.update_on_runner_adjustment(&adj).unwrap();
        // Frame flipped, outs reset, runner placed on second only.
        assert_eq!(gs.frame, InningFrame::Bottom);
        assert_eq!(gs.batting_side, Side::Home);
        assert_eq!(gs.outs, Outs::new(0).unwrap());
        let runner = gs.bases.get_runner(BaseRunner::Second).unwrap();
        assert_eq!(runner.lineup_position, LineupPosition::First);
        assert!(gs.bases.get_runner(BaseRunner::First).is_none());
        assert!(gs.bases.get_runner(BaseRunner::Third).is_none());
    }

    // --- update_on_substitution ---

    #[test]
    fn update_on_substitution_records_walk_responsible_pitcher_at_3_0_count() {
        let mut gs = make_state();
        // Personnel update sets `end_event_id = event_id - 1`, which underflows
        // BoundedUsize<1, _> at event_id=1. Advance to a mid-game id before
        // substituting.
        gs.event_id = EventId::new(2).unwrap();
        // Away bats, home pitches. Count 3-0 means the OLD pitcher owns the
        // walk if a reliever now comes in. We grab the original home pitcher
        // (h9001 per build_starter_slice) before the swap.
        gs.count = Count {
            balls: Balls::new(3),
            strikes: Strikes::new(0),
        };
        let original_home_pitcher: Pitcher = str_to_tinystr("h9001").unwrap();
        let sub_record =
            SubstitutionRecord::try_from(&rec(&["sub", "relp001", "Reliever", "1", "9", "1"]))
                .unwrap();
        gs.update_on_substitution(&sub_record).unwrap();
        assert_eq!(
            gs.unusual_state.walk_responsible_pitcher,
            Some(original_home_pitcher)
        );
    }
}

use std::collections::HashMap;
use std::convert::TryFrom;
use std::sync::Arc;

use anyhow::{anyhow, bail, Context, Error, Result};
use arrayvec::ArrayString;
use bimap::BiMap;
use bounded_integer::BoundedUsize;
use chrono::{NaiveDate, NaiveDateTime, NaiveTime};
use either::Either;
use itertools::Itertools;
use serde::{Deserialize, Serialize};

use crate::event_file::info::{
    DayNight, DoubleheaderStatus, FieldCondition, HowScored, InfoRecord, Park, Precipitation, Sky,
    Team, UmpireAssignment, UmpirePosition, WindDirection,
};
use crate::event_file::misc::{BatHandAdjustment, EarnedRunRecord, GameId, Hand, LineupAdjustment, PitcherResponsibilityAdjustment, PitchHandAdjustment, RunnerAdjustment, SubstitutionRecord};
use crate::event_file::parser::{FileInfo, MappedRecord, RecordSlice};
use crate::event_file::pitch_sequence::PitchSequenceItem;
use crate::event_file::play::{Play, Base, BaseRunner, BaserunningPlayType, ContactType, Count, EarnedRunStatus, FieldersData, FieldingData, HitLocation, HitType, ImplicitPlayResults, InningFrame, OtherPlateAppearance, OutAtBatType, PlateAppearanceType, PlayModifier, PlayType, RunnerAdvance};
use crate::event_file::traits::{FieldingPosition, Inning, LineupPosition, Matchup, MAX_EVENTS_PER_GAME, Pitcher, Player, RetrosheetVolunteer, Scorer, SequenceId, Side, Umpire};
use crate::AccountType;

const UNKNOWN_STRINGS: [&str; 1] = ["unknown"];
const NONE_STRINGS: [&str; 2] = ["(none)", "none"];

type Position = Either<LineupPosition, FieldingPosition>;

/// A wrapper around `Player` that allows for a player to appear
/// in multiple positions in a lineup. This is used for the
/// Ohtani rule, where a player can appear in the lineup as a
/// pitcher and a DH.
#[derive(Debug, Eq, PartialEq, Copy, Clone, Hash)]
struct TrackedPlayer {
    pub player: Player,
    is_pitcher_with_dh: bool
}

impl From<(Player, bool)> for TrackedPlayer {
    fn from((player, is_starting_pitcher_with_dh): (Player, bool)) -> Self {
        Self {
            player,
            is_pitcher_with_dh: is_starting_pitcher_with_dh,
        }
    }
}

type PersonnelState = BiMap<Position, TrackedPlayer>;
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

#[derive(Debug, Ord, PartialOrd, Eq, PartialEq, Clone, Copy, Serialize, Deserialize)]
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

#[derive(Debug, Ord, PartialOrd, Eq, PartialEq, Clone, Copy, Serialize, Deserialize)]
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
    SacrificeHit
}

impl From<(&PlateAppearanceType, &[PlayModifier])> for PlateAppearanceResultType {
    fn from(tup: (&PlateAppearanceType, &[PlayModifier])) -> Self {
        let (plate_appearance, modifiers) = tup;
        let is_sac_fly = modifiers
            .iter()
            .any(|m| m == &PlayModifier::SacrificeFly);
        let is_sac_hit = modifiers
            .iter()
            .any(|m| m == &PlayModifier::SacrificeHit);
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
                HitType::HomeRun => Self::HomeRun
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
                OutAtBatType::InPlayOut if bo.implicit_advance().is_some() => Self::ReachedOnError,
                _ => Self::InPlayOut,
            },
        }
    }
}

// TODO: Add weird game state info to flags
#[derive(Debug, Ord, PartialOrd, Eq, PartialEq, Clone, Serialize, Deserialize)]
pub struct EventFlag {
    event_key: usize,
    sequence_id: SequenceId,
    flag: String,
}

impl EventFlag {
    fn from_play(play: &Play, event_key: usize) -> Vec<Self> {
        play.parsed
            .modifiers
            .iter()
            .filter(|pm| pm.is_valid_event_type())
            .enumerate()
            .map(|(i, pm)| Self {
                event_key,
                sequence_id: SequenceId::new(i + 1).unwrap(),
                flag: pm.into(),
            })
            .collect_vec()
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
            doubleheader_status: Default::default(),
            start_time: Default::default(),
            time_of_day: Default::default(),
            use_dh: false,
            bat_first_side: Side::Away,
            sky: Default::default(),
            temperature_fahrenheit: Default::default(),
            field_condition: Default::default(),
            precipitation: Default::default(),
            wind_direction: Default::default(),
            wind_speed_mph: Default::default(),
            attendance: None,
            park_id: Park::from("unknown").unwrap(),
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
                // TODO: Season, GameType
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
    pub game_id: ArrayString<16>,
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
    pub earned_runs: Vec<EarnedRunRecord>
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
    pub game_id: ArrayString<16>,
    pub player_id: Player,
    pub side: Side,
    pub lineup_position: LineupPosition,
    pub entered_game_as: EnteredGameAs,
    pub start_event_id: EventId,
    pub end_event_id: Option<EventId>,
}

impl GameLineupAppearance {
    fn new_starter(
        player: Player,
        lineup_position: LineupPosition,
        side: Side,
        game_id: GameId,
    ) -> Self {
        Self {
            game_id: game_id.id,
            player_id: player,
            lineup_position,
            side,
            entered_game_as: EnteredGameAs::Starter,
            start_event_id: EventId::new(1).unwrap(),
            end_event_id: None,
        }
    }
}

#[derive(Debug, Eq, PartialEq, Clone, Serialize, Copy)]
pub struct GameFieldingAppearance {
    pub game_id: ArrayString<16>,
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
    ) -> Self {
        Self {
            game_id: game_id.id,
            player_id: player,
            fielding_position,
            side,
            start_event_id: EventId::new(1).unwrap(),
            end_event_id: None,
        }
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
}

#[derive(Debug, Eq, PartialEq, Clone)]
pub struct GameContext {
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
    pub event_key_offset: usize,
}

impl GameContext {
    pub fn new(record_slice: &RecordSlice, file_info: FileInfo, line_offset: usize, game_num: usize) -> Result<Self> {
        let game_id = get_game_id(record_slice)?;
        let teams: Matchup<Team> = Matchup::try_from(record_slice)?;
        let setting = GameSetting::try_from(record_slice)?;
        let metadata = GameMetadata::try_from(record_slice)?;
        let umpires = GameUmpire::from_record_slice(record_slice)?;
        let results = GameResults::try_from(record_slice)?;
        let event_key_offset = Self::event_key_offset(file_info, game_num);

        let (events, lineup_appearances, fielding_appearances) =
            if file_info.account_type == AccountType::BoxScore {
                (vec![], vec![], vec![])
            } else {
                GameState::create_events(record_slice, line_offset, event_key_offset)
                    .with_context(|| anyhow!("Could not parse game {}", game_id.id))?
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
            event_key_offset
        })
    }

    fn event_key_offset(file_info: FileInfo, game_num: usize) -> usize {
        file_info.file_index + (game_num * MAX_EVENTS_PER_GAME)
    }
}

#[derive(Debug, Eq, PartialEq, Clone, Serialize)]
pub struct EventStartingBaseState {
    pub event_key: usize,
    pub baserunner: BaseRunner,
    pub runner_lineup_position: LineupPosition,
    pub charged_to_pitcher_id: Pitcher,
}

impl EventStartingBaseState {
    fn from_base_state(state: &BaseState, event_key: usize) -> Vec<Self> {
        state
            .get_bases()
            .iter()
            .map(|(baserunner, runner)| Self {
                event_key,
                baserunner: *baserunner,
                runner_lineup_position: runner.lineup_position,
                charged_to_pitcher_id: runner.charged_to,
            })
            .collect_vec()
    }
}

#[derive(Debug, Eq, PartialEq, Clone, Serialize, Deserialize)]
pub struct EventBaserunningPlay {
    pub event_key: usize,
    pub sequence_id: SequenceId,
    pub baserunning_play_type: BaserunningPlayType,
    pub baserunner: Option<BaseRunner>
}

impl EventBaserunningPlay {
    fn from_play(play: &Play, event_key: usize) -> Option<Vec<Self>> {
        let vec = play
            .parsed
            .main_plays
            .iter()
            .enumerate()
            .filter_map(|(i, pt)| {
                if let PlayType::BaserunningPlay(br) = pt {
                    Some(Self {
                        event_key,
                        sequence_id: SequenceId::new(i + 1).unwrap(),
                        baserunning_play_type: br.baserunning_play_type,
                        baserunner: br.baserunner(),
                    })
                } else {
                    None
                }
            })
            .collect_vec();
        if vec.is_empty() {
            None
        } else {
            Some(vec)
        }
    }
}

#[derive(Debug, Eq, PartialEq, Clone, Serialize, Deserialize)]
pub struct EventPlateAppearance {
    pub event_key: usize,
    pub plate_appearance_result: PlateAppearanceResultType,
    pub contact: Option<ContactType>,
    pub hit_to_fielder: Option<FieldingPosition>,
    #[serde(skip_serializing)]
    pub hit_location: Option<HitLocation>,
}

impl EventPlateAppearance {
    fn from_play(play: &Play, event_key: usize) -> Option<Self> {
        let modifiers = play.parsed.modifiers.as_slice();
        play.parsed.main_plays.iter().find_map(|pt| {
            if let PlayType::PlateAppearance(pa) = pt {
                Some(Self {
                    event_key,
                    plate_appearance_result: PlateAppearanceResultType::from((pa, modifiers)),
                    contact: play.stats.contact_description.map(|cd| cd.contact_type),
                    hit_to_fielder: play.stats.hit_to_fielder,
                    hit_location: play.stats.contact_description.and_then(|cd| cd.location),
                })
            } else {
                None
            }
        })
    }
}

#[derive(Debug, Eq, PartialEq, Clone, Serialize, Deserialize)]
pub struct EventBaserunningAdvanceAttempt {
    pub event_key: usize,
    pub sequence_id: SequenceId,
    pub baserunner: BaseRunner,
    pub attempted_advance_to: Base,
    pub is_successful: bool,
    pub advanced_on_error_flag: bool,
    pub explicit_out_flag: bool,
    pub rbi_flag: bool,
    pub team_unearned_flag: bool,
}

impl EventBaserunningAdvanceAttempt {
    fn from_play(play: &Play, event_key: usize) -> Vec<Self> {
        play
            .stats
            .advances
            .iter()
            .enumerate()
            .map(|(i, ra)| {
                let advanced_on_error_flag = FieldersData::find_error(ra.fielders_data().as_slice()).is_some();
                let is_successful = !ra.is_out();
                let explicit_out_flag = ra.out_or_error;
                Self {
                    event_key,
                    sequence_id: SequenceId::new(i + 1).unwrap(),
                    baserunner: ra.baserunner,
                    attempted_advance_to: ra.to,
                    advanced_on_error_flag,
                    explicit_out_flag,
                    is_successful,
                    rbi_flag: play.stats.rbi.contains(&ra.baserunner),
                    team_unearned_flag: ra.earned_run_status() == Some(EarnedRunStatus::TeamUnearned),
                }
            })
            .collect_vec()
    }
}

#[derive(Debug, Eq, PartialEq, Clone, Serialize)]
pub struct EventContext {
    pub inning: u8,
    pub batting_side: Side,
    pub frame: InningFrame,
    pub at_bat: LineupPosition,
    pub outs: Outs,
    pub starting_base_state: Vec<EventStartingBaseState>,
    pub batter_hand: Hand,
    pub pitcher_hand: Hand,
}

#[derive(Debug, Eq, PartialEq, Clone, Serialize, Deserialize)]
pub struct EventResults {
    pub count_at_event: Count,
    pub pitch_sequence: Option<Vec<PitchSequenceItem>>,
    pub plate_appearance: Option<EventPlateAppearance>,
    pub plays_at_base: Option<Vec<EventBaserunningPlay>>,
    pub out_on_play: Vec<BaseRunner>,
    pub fielding_plays: Vec<FieldersData>,
    pub baserunning_advances: Vec<EventBaserunningAdvanceAttempt>,
    pub play_info: Vec<EventFlag>,
    pub comment: Option<String>,
}

#[derive(Debug, Eq, PartialEq, Clone)]
pub struct Event {
    pub game_id: GameId,
    pub event_id: EventId,
    pub event_key: usize,
    pub context: EventContext,
    pub results: EventResults,
    pub raw_play: Arc<String>,
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
            pa = self
                .results
                .plate_appearance
                .as_ref()
                .map(|pa| pa.plate_appearance_result),
            ba = self.results.baserunning_advances,
            out = self.results.out_on_play,
        )
    }
}

/// This tracks the more unusual/miscellaneous elements of the state of the game,
/// such as batters batting from an unexpected side or a substitution in the middle of
/// an at-bat. Further exceptions should go here as they come up.
#[derive(Default, Debug, Eq, PartialEq, Clone)]
pub struct WeirdGameState {
    batter_hand: Option<Hand>,
    pitcher_hand: Option<Hand>,
    // TODO
    responsible_batter: Option<Player>,
    responsible_pitcher: Option<Player>,
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
                id: ArrayString::<16>::from("N/A").unwrap(),
            },
            personnel_state: Matchup::new(
                (BiMap::with_capacity(15), BiMap::with_capacity(15)),
                (BiMap::with_capacity(15), BiMap::with_capacity(15)),
            ),
            lineup_appearances: HashMap::with_capacity(30),
            defense_appearances: HashMap::with_capacity(30),
        }
    }
}

impl Personnel {

    fn new(record_slice: &RecordSlice) -> Result<Self> {
        let game_id = get_game_id(record_slice)?;
        let mut personnel = Personnel {
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
            let (lineup, defense) = personnel.personnel_state.get_mut(&start.side);
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
            let player: TrackedPlayer = (start.player, start.lineup_position == LineupPosition::PitcherWithDh).into();

            lineup.insert(Either::Left(start.lineup_position), player);
            defense.insert(Either::Right(start.fielding_position), player);
            personnel
                .lineup_appearances
                .insert(player, vec![lineup_appearance]);
            personnel
                .defense_appearances
                .insert(player, vec![fielding_appearance]);
        }
        Ok(personnel)
    }

    fn pitcher(&self, side: &Side) -> Result<Pitcher> {
        self.get_at_position(side, &Either::Right(FieldingPosition::Pitcher))
            .map(|tp| tp.player)
    }

    fn get_at_position(&self, side: &Side, position: &Position) -> Result<TrackedPlayer> {
        let map_tup = self.personnel_state.get(side);
        let map = if let Either::Left(_) = position {
            &map_tup.0
        } else {
            &map_tup.1
        };
        map.get_by_left(position).copied().with_context(|| {
            anyhow!(
                "Position {:?} for side {:?} missing from current game state: {:?}",
                position, side, map
            )
        })
    }

    fn get_player_lineup_position(&self, side: &Side, player: &TrackedPlayer) -> Option<Position> {
        let (lineup, _) = self.personnel_state.get(side);
        lineup.get_by_right(player).copied()
    }

    fn at_bat(&self, play: &Play) -> Result<LineupPosition> {
        let player: TrackedPlayer = (play.context.batter, false).into();
        let position = self
            .personnel_state
            .get(&play.context.batting_side)
            .0
            .get_by_right(&player)
            .copied();

        if let Some(Either::Left(lp)) = position {
            Ok(lp)
        } else {
            bail!(
                "Fatal error parsing {}: Cannot find lineup position of player currently at bat {:?}.\nFull state: {:?}",
                self.game_id.id,
                &play.context.batter,
                self.personnel_state
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
                    "Cannot find existing player {:?} in lineup appearance records",
                    player
                )
            })?
            .last_mut()
            .with_context(|| {
                anyhow!(
                    "Player {:?} has an empty list of lineup appearances",
                    player
                )
            })
    }

    fn get_current_fielding_appearance(
        &mut self,
        player: &TrackedPlayer,
    ) -> Result<&mut GameFieldingAppearance> {
        self.defense_appearances
            .get_mut(player)
            .with_context(|| {
                anyhow!(
                    "Cannot find existing player {:?} in defense appearance records",
                    player
                )
            })?
            .last_mut()
            .with_context(|| {
                anyhow!(
                    "Player {:?} has an empty list of fielding appearances",
                    player
                )
            })
    }

    fn update_lineup_on_substitution(
        &mut self,
        sub: &SubstitutionRecord,
        event_id: EventId,
    ) -> Result<()> {
        let original_batter = self.get_at_position(&sub.side, &Either::Left(sub.lineup_position));

        match original_batter {
            // If this substitution is the DH/PH/PR being brought in to field, no substitution takes place
            Ok(p) if p.player == sub.player => return Ok(()),
            Ok(p) => self.get_current_lineup_appearance(&p)?.end_event_id = Some(event_id - 1),
            // There should almost always be an original batter, but in
            // the extremely rare case of a courtesy runner, there might not be.
            Err(_) => {}
        };


        let new_player: TrackedPlayer = (sub.player, sub.lineup_position == LineupPosition::PitcherWithDh).into();
        // In the case of a courtesy runner, the new player may already be in the lineup
        if self.get_player_lineup_position(&sub.side, &new_player).is_some() {
            self.get_current_lineup_appearance(&new_player)?.end_event_id = Some(event_id - 1);
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
        let (lineup, _) = self.personnel_state.get_mut(&sub.side);
        lineup.insert(Either::Left(sub.lineup_position), new_player);
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
            self.get_at_position(&sub.side, &Either::Right(sub.fielding_position));
        match original_fielder {
            Ok(p) if p.player == sub.player => return Ok(()),
            Ok(p) => self.get_current_fielding_appearance(&p)?.end_event_id = Some(event_id - 1),
            Err(_) => {}
        };

        let new_fielder: TrackedPlayer = (sub.player, sub.lineup_position == LineupPosition::PitcherWithDh).into();
        // If the new fielder is already in the game, we need to close out their previous appearance
        if let Ok(gfa) = self.get_current_fielding_appearance(&new_fielder) {
            gfa.end_event_id = Some(event_id - 1);
        }

        let (_, defense) = self.personnel_state.get_mut(&sub.side);
        // We maintain a 1:1 relationship between players and positions at all times,
        // so the entire position must be removed from the defense temporarily.
        // If the data is consistent, this state (< 9 positions) can only exist between substitutions,
        // and cannot exist at the start of a play.
        defense.remove_by_right(&new_fielder);
        defense.insert(Either::Right(sub.fielding_position), new_fielder);

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
        let non_batting_pitcher =
            self.get_at_position(&sub.side, &Either::Left(LineupPosition::PitcherWithDh));
        let dh = self.get_at_position(
            &sub.side,
            &Either::Right(FieldingPosition::DesignatedHitter),
        );
        if let Ok(p) = non_batting_pitcher {
            self.get_current_lineup_appearance(&p)?.end_event_id = Some(event_id - 1);
        }
        if let Ok(p) = dh {
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
            self.update_defense_on_substitution(sub, event_id)?
        };
        if sub.fielding_position == FieldingPosition::Pitcher
            && sub.lineup_position != LineupPosition::PitcherWithDh
        {
            self.update_on_dh_vacancy(sub, event_id)?
        };
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
    batting_side: Side,
    outs: Outs,
    bases: BaseState,
    at_bat: LineupPosition,
    personnel: Personnel,
    weird_state: WeirdGameState,
}

impl GameState {
    pub fn create_events(
        record_slice: &RecordSlice,
        line_offset: usize,
        event_key_offset: usize
    ) -> Result<(
        Vec<Event>,
        Vec<GameLineupAppearance>,
        Vec<GameFieldingAppearance>,
    )> {
        let mut events: Vec<Event> = Vec::with_capacity(100);

        let mut state = Self::new(record_slice)?;
        for (i, record) in record_slice.iter().enumerate() {
            let opt_play = if let MappedRecord::Play(pr) = record {
                Some(Play::try_from(pr)?)
            } else {
                None
            };
            let event_key = event_key_offset + state.event_id;
            let starting_base_state = EventStartingBaseState::from_base_state(
                &state.bases,
                event_key
            );

            state.update(record, &opt_play)?;
            if let Some(play) = opt_play {
                let raw_play = play.parsed.raw_play.clone();
                let context = EventContext {
                    inning: state.inning,
                    batting_side: state.batting_side,
                    frame: state.frame,
                    at_bat: state.at_bat,
                    outs: state.outs,
                    starting_base_state,
                    batter_hand: state.weird_state.batter_hand.unwrap_or_default(),
                    pitcher_hand: state.weird_state.pitcher_hand.unwrap_or_default(),
                };
                let results = EventResults {
                    count_at_event: play.context.count,
                    pitch_sequence: play.pitch_sequence.as_ref().map(|ps| ps.0.clone()),
                    plate_appearance: EventPlateAppearance::from_play(
                        &play,
                        event_key,
                    ),
                    plays_at_base: EventBaserunningPlay::from_play(
                        &play,
                        event_key,
                    ),
                    baserunning_advances: EventBaserunningAdvanceAttempt::from_play(
                        &play,
                        event_key,
                    ),
                    play_info: EventFlag::from_play(&play, event_key),
                    comment: None,
                    fielding_plays: play.stats.fielders_data.clone(),
                    out_on_play: play.stats.outs.clone(),
                };
                let line_number = line_offset + i;
                events.push(Event {
                    game_id: state.game_id,
                    event_id: state.event_id,
                    context,
                    results,
                    raw_play,
                    line_number,
                    event_key,
                });
                state.event_id += 1;
            }
        }
        // Set all remaining blank end_event_ids to final event
        let max_event_id = Some(EventId::new(events.len()).unwrap());
        let mut lineup_appearances = state
            .personnel
            .lineup_appearances
            .values()
            .flatten()
            .copied()
            .collect_vec();
        let mut defense_appearances = state
            .personnel
            .defense_appearances
            .values()
            .flatten()
            .copied()
            .collect_vec();
        for la in &mut lineup_appearances {
            la.end_event_id = la.end_event_id.or(max_event_id)
        }
        for da in &mut defense_appearances {
            da.end_event_id = da.end_event_id.or(max_event_id)
        }
        lineup_appearances.sort_by_key(|la| (la.side, la.lineup_position, la.start_event_id));
        defense_appearances.sort_by_key(|da| (da.side, da.fielding_position, da.start_event_id));

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
            event_id: EventId::new(1).unwrap(),
            inning: 1,
            frame: InningFrame::Top,
            batting_side,
            outs: Outs::new(0).unwrap(),
            bases: Default::default(),
            at_bat: Default::default(),
            personnel: Personnel::new(record_slice)?,
            weird_state: Default::default(),
        })
    }

    fn is_frame_flipped(&self, play: &Play) -> Result<bool> {
        if self.batting_side == play.context.batting_side {
            Ok(false)
        } else if self.outs < 3 {
            bail!("New frame without 3 outs recorded")
        } else {
            Ok(true)
        }
    }

    fn get_new_frame(&self, play: &Play) -> Result<InningFrame> {
        Ok(if self.is_frame_flipped(play)? {
            self.frame.flip()
        } else {
            self.frame
        })
    }

    fn outs_after_play(&self, play: &Play) -> Result<Outs> {
        let play_outs = play.stats.outs.len();
        let new_outs = if self.is_frame_flipped(play)? {
            play_outs
        } else {
            self.outs.get() + play_outs
        };
        Outs::new(new_outs).context("Illegal state, more than 3 outs recorded")
    }

    fn update_on_play(&mut self, play: &Play) -> Result<()> {

        let new_frame = self.get_new_frame(play)?;
        let new_outs = self.outs_after_play(play)?;

        // In the case of certain double switches, there is no pitcher of record,
        // which is fine because we only need to track the pitcher of record for
        // runner responsibility, which would not change on a no-play.
        let pitcher = self.personnel.pitcher(&play.context.batting_side.flip()).ok();

        let batter_lineup_position = self.personnel.at_bat(play)?;

        let new_base_state = self.bases.new_base_state(
            self.is_frame_flipped(play)?,
            new_outs == 3,
            play,
            batter_lineup_position,
            pitcher,
        )?;

        self.inning = play.context.inning;
        self.frame = new_frame;
        self.batting_side = play.context.batting_side;
        self.outs = new_outs;
        self.bases = new_base_state;
        self.at_bat = batter_lineup_position;
        self.weird_state = Default::default();

        Ok(())
    }

    fn update_on_substitution(&mut self, record: &SubstitutionRecord) -> Result<()> {
        self.personnel.update_on_substitution(record, self.event_id)
    }

    fn update_on_bat_hand_adjustment(&mut self, record: &BatHandAdjustment) {
        self.weird_state.batter_hand = Some(record.hand)
    }

    fn update_on_pitch_hand_adjustment(&mut self, record: &PitchHandAdjustment) {
        self.weird_state.batter_hand = Some(record.hand)
    }

    fn update_on_lineup_adjustment(&mut self, _record: &LineupAdjustment) {
        // Nothing to do here, since we map player to batting order anyway
    }

    fn update_on_runner_adjustment(&mut self, record: &RunnerAdjustment) -> Result<()> {
        // The extra innings runner record can appear before or after the first record of the next
        // inning, and it doesn't have a side associated with it, so we have to do some messy
        // state changes to get it right.
        if self.outs == 3 {
            self.frame = self.frame.flip();
            self.batting_side = self.batting_side.flip();
            self.outs = Outs::new(0).unwrap();
        }
        let tracked_runner: TrackedPlayer = (record.runner_id, false).into();
        let runner_pos = self
            .personnel
            .get_current_lineup_appearance(&tracked_runner)?
            .lineup_position;
        let pitcher = self.personnel.pitcher(&self.batting_side.flip())?;
        self.bases = BaseState::new_inning_tiebreaker(runner_pos, pitcher);

        Ok(())
    }

    fn update_on_comment(&mut self, _record: &str) {
        // TODO
    }

    fn update_on_pitcher_responsibility_adjustment(&mut self, _record: &PitcherResponsibilityAdjustment) {
        // TODO
    }

    pub fn update(
        &mut self,
        record: &MappedRecord,
        play: &Option<Play>,
    ) -> Result<()> {
        match record {
            MappedRecord::Play(_) => {
                if let Some(cp) = play {
                    self.update_on_play(cp)
                        .with_context(|| anyhow!("Failed to parse play {:?}", cp))
                } else {
                    bail!("Expected cached play but got None")
                }
            }?,
            MappedRecord::Substitution(r) => self.update_on_substitution(r)?,
            MappedRecord::BatHandAdjustment(r) => self.update_on_bat_hand_adjustment(r),
            MappedRecord::PitchHandAdjustment(r) => self.update_on_pitch_hand_adjustment(r),
            MappedRecord::LineupAdjustment(r) => self.update_on_lineup_adjustment(r),
            MappedRecord::RunnerAdjustment(r) => self.update_on_runner_adjustment(r)?,
            MappedRecord::PitcherResponsibilityAdjustment(r) => self.update_on_pitcher_responsibility_adjustment(r),
            MappedRecord::Comment(r) => self.update_on_comment(r),
            _ => {}
        };
        Ok(())
    }
}

pub type Outs = BoundedUsize<0, 3>;

#[derive(Debug, Eq, PartialEq, Default, Clone)]
pub struct BaseState {
    bases: HashMap<BaseRunner, Runner>,
    scored: Vec<Runner>,
}

impl BaseState {
    pub fn new_inning_tiebreaker(new_runner: LineupPosition, current_pitcher: Pitcher) -> Self {
        let mut state = Self::default();
        let runner = Runner {
            lineup_position: new_runner,
            charged_to: current_pitcher,
        };
        state.bases.insert(BaseRunner::Second, runner);
        state
    }

    pub const fn get_bases(&self) -> &HashMap<BaseRunner, Runner> {
        &self.bases
    }

    fn num_runners_on_base(&self) -> usize {
        self.bases.len()
    }

    fn get_runner(&self, baserunner: &BaseRunner) -> Option<&Runner> {
        self.bases.get(baserunner)
    }

    fn get_first(&self) -> Option<&Runner> {
        self.bases.get(&BaseRunner::First)
    }

    fn get_second(&self) -> Option<&Runner> {
        self.bases.get(&BaseRunner::Second)
    }

    fn get_third(&self) -> Option<&Runner> {
        self.bases.get(&BaseRunner::Third)
    }

    fn clear_baserunner(&mut self, baserunner: &BaseRunner) -> Option<Runner> {
        self.bases.remove(baserunner)
    }

    fn set_runner(&mut self, baserunner: BaseRunner, runner: Runner) {
        self.bases.insert(baserunner, runner);
    }

    fn get_advance_from_baserunner(
        baserunner: BaseRunner,
        play: &Play,
    ) -> Option<&RunnerAdvance> {
        play
            .stats
            .advances
            .iter()
            .find(|a| a.baserunner == baserunner)
    }

    fn current_base_occupied(&self, advance: &RunnerAdvance) -> bool {
        self.get_runner(&advance.baserunner).is_some()
    }

    fn target_base_occupied(&self, advance: &RunnerAdvance) -> Result<bool> {
        let br = BaseRunner::from_target_base(advance.to);
        Ok(self.get_runner(&br?).is_some())
    }

    fn check_integrity(old_state: &Self, new_state: &Self, advance: &RunnerAdvance) -> Result<()> {
        if new_state.target_base_occupied(advance)? {
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
    ///  baserunners as inherited if they reach on a fielder's choice
    ///  in which an inherited runner is forced out 🙃
    fn update_runner_charges(self, _play: &Play) -> Self {
        // TODO: This
        self
    }

    pub(crate) fn new_base_state(
        &self,
        start_inning: bool,
        end_inning: bool,
        play: &Play,
        batter_lineup_position: LineupPosition,
        pitcher: Option<Pitcher>,
    ) -> Result<Self> {
        let mut new_state = if start_inning {
            Self::default()
        } else {
            self.clone()
        };
        new_state.scored = vec![];

        // Cover cases where outs are not included in advance information
        for out in &play.stats.outs {
            new_state.clear_baserunner(out);
        }

        if let Some(a) = BaseState::get_advance_from_baserunner(BaseRunner::Third, play) {
            new_state.clear_baserunner(&BaseRunner::Third);
            if a.is_out() {
            } else if let Err(e) = Self::check_integrity(self, &new_state, a) {
                return Err(e);
            } else if let Some(r) = self.get_third() {
                new_state.scored.push(*r)
            }
        }
        if let Some(a) = BaseState::get_advance_from_baserunner(BaseRunner::Second, play) {
            new_state.clear_baserunner(&BaseRunner::Second);
            if a.is_out() {
            } else if let Err(e) = Self::check_integrity(self, &new_state, a) {
                return Err(e);
            } else if let (Ok(true), Some(r)) = (
                a.is_this_that_one_time_jean_segura_ran_in_reverse(),
                self.get_second(),
            ) {
                new_state.set_runner(BaseRunner::First, *r)
            } else if let (Base::Third, Some(r)) = (a.to, self.get_second()) {
                new_state.set_runner(BaseRunner::Third, *r)
            } else if let (Base::Home, Some(r)) = (a.to, self.get_second()) {
                new_state.scored.push(*r)
            }
        }
        if let Some(a) = BaseState::get_advance_from_baserunner(BaseRunner::First, play) {
            new_state.clear_baserunner(&BaseRunner::First);
            if a.is_out() {
            } else if let Err(e) = Self::check_integrity(self, &new_state, a) {
                return Err(e);
            } else if let (Base::Second, Some(r)) = (&a.to, self.get_first()) {
                new_state.set_runner(BaseRunner::Second, *r)
            } else if let (Base::Third, Some(r)) = (&a.to, self.get_first()) {
                new_state.set_runner(BaseRunner::Third, *r)
            } else if let (Base::Home, Some(r)) = (&a.to, self.get_first()) {
                new_state.scored.push(*r)
            }
        }
        if let Some(a) = BaseState::get_advance_from_baserunner(BaseRunner::Batter, play) {
            let some_pitcher = pitcher.ok_or_else(|| anyhow!("A pitcher ID must be provided if the batter reached base"))?;
            let new_runner = Runner {
                lineup_position: batter_lineup_position,
                charged_to: some_pitcher,
            };
            match a.to {
                _ if a.is_out() || end_inning => {}
                _ if new_state.target_base_occupied(a)? => {
                    return Err(anyhow!("Batter advanced to an occupied base"))
                }
                Base::Home => new_state.scored.push(new_runner),
                b => new_state.set_runner(BaseRunner::from_current_base(b)?, new_runner),
            }
        }
        Ok(new_state.update_runner_charges(play))
    }
}

#[derive(Debug, Eq, PartialEq, Copy, Clone)]
pub struct Runner {
    pub lineup_position: LineupPosition,
    pub charged_to: Pitcher,
}

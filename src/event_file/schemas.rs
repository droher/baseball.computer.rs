use anyhow::{bail, Context, Result};
use arrayvec::ArrayString;
use bounded_integer::BoundedU8;
use chrono::{NaiveDate, NaiveDateTime};
use either::Either;
use serde::{Deserialize, Serialize};
use serde_json::{Map, Value};

use crate::event_file::box_score::{BoxScoreEvent, BoxScoreLine, LineScore};
use crate::event_file::game_state::{EventId, GameContext, Outs};
use crate::event_file::info::{
    DayNight, DoubleheaderStatus, FieldCondition, HowScored, Park, Precipitation, Sky, Team,
    WindDirection,
};
use crate::event_file::pitch_sequence::PitchType;
use crate::event_file::play::{Base, BaseRunner, InningFrame};
use crate::event_file::traits::{
    EventKey, FieldingPlayType, FieldingPosition, GameType, Inning, LineupPosition, Pitcher,
    Player, RetrosheetVolunteer, Scorer, SequenceId, Side, Umpire,
};

use super::game_state::{Event as E, GameLineupAppearance, PlateAppearanceResultType};
use super::info::UmpirePosition;
use super::misc::Hand;
use super::parser::{AccountType, MappedRecord, RecordSlice};
use super::play::{
    BaserunningPlayType, ContactType, HitAngle, HitDepth, HitLocationGeneral, HitStrength,
};

pub trait ContextToVec<'a>: Serialize + Sized {
    fn from_game_context(gc: &'a GameContext) -> Box<dyn Iterator<Item = Self> + 'a>;
}

pub type GameIdString = ArrayString<12>;

#[derive(Debug, Eq, PartialEq, Copy, Clone, Serialize, Deserialize)]
pub struct Games<'a> {
    game_id: GameIdString,
    date: NaiveDate,
    start_time: Option<NaiveDateTime>,
    doubleheader_status: DoubleheaderStatus,
    time_of_day: DayNight,
    game_type: GameType,
    bat_first_side: Side,
    sky: Sky,
    field_condition: FieldCondition,
    precipitation: Precipitation,
    wind_direction: WindDirection,
    park_id: Park,
    temperature_fahrenheit: Option<u8>,
    attendance: Option<u32>,
    wind_speed_mph: Option<u8>,
    use_dh: bool,
    winning_pitcher: Option<Player>,
    losing_pitcher: Option<Player>,
    save_pitcher: Option<Player>,
    game_winning_rbi: Option<Player>,
    time_of_game_minutes: Option<u16>,
    protest_info: Option<&'a str>,
    completion_info: Option<&'a str>,
    scorer: Option<Scorer>,
    scoring_method: HowScored,
    inputter: Option<RetrosheetVolunteer>,
    translator: Option<RetrosheetVolunteer>,
    date_inputted: Option<NaiveDateTime>,
    date_edited: Option<NaiveDateTime>,
    account_type: AccountType,
    game_key: EventKey,
    away_team_id: Team,
    home_team_id: Team,
    umpire_home_id: Option<Umpire>,
    umpire_first_id: Option<Umpire>,
    umpire_second_id: Option<Umpire>,
    umpire_third_id: Option<Umpire>,
    umpire_left_id: Option<Umpire>,
    umpire_right_id: Option<Umpire>,
}

impl<'a> From<&'a GameContext> for Games<'a> {
    fn from(gc: &'a GameContext) -> Self {
        let setting = &gc.setting;
        let results = &gc.results;
        let start_time = setting
            .start_time
            .map(|time| NaiveDateTime::new(setting.date, time));
        Self {
            game_id: gc.game_id.id,
            date: setting.date,
            start_time,
            doubleheader_status: setting.doubleheader_status,
            time_of_day: setting.time_of_day,
            game_type: setting.game_type,
            bat_first_side: setting.bat_first_side,
            sky: setting.sky,
            field_condition: setting.field_condition,
            precipitation: setting.precipitation,
            wind_direction: setting.wind_direction,
            park_id: setting.park_id,
            temperature_fahrenheit: setting.temperature_fahrenheit,
            attendance: setting.attendance,
            wind_speed_mph: setting.wind_speed_mph,
            use_dh: setting.use_dh,
            winning_pitcher: results.winning_pitcher,
            losing_pitcher: results.losing_pitcher,
            save_pitcher: results.save_pitcher,
            game_winning_rbi: results.game_winning_rbi,
            time_of_game_minutes: results.time_of_game_minutes,
            protest_info: results.protest_info.as_deref(),
            completion_info: results.completion_info.as_deref(),
            game_key: gc.event_key_offset,
            scorer: gc.metadata.scorer,
            scoring_method: gc.metadata.how_scored,
            inputter: gc.metadata.inputter,
            translator: gc.metadata.translator,
            date_inputted: gc.metadata.date_inputted,
            date_edited: gc.metadata.date_edited,
            account_type: gc.file_info.account_type,
            away_team_id: gc.teams.away,
            home_team_id: gc.teams.home,
            umpire_home_id: gc
                .umpires
                .iter()
                .find(|u| u.position == UmpirePosition::Home)
                .and_then(|u| u.umpire_id),
            umpire_first_id: gc
                .umpires
                .iter()
                .find(|u| u.position == UmpirePosition::First)
                .and_then(|u| u.umpire_id),
            umpire_second_id: gc
                .umpires
                .iter()
                .find(|u| u.position == UmpirePosition::Second)
                .and_then(|u| u.umpire_id),
            umpire_third_id: gc
                .umpires
                .iter()
                .find(|u| u.position == UmpirePosition::Third)
                .and_then(|u| u.umpire_id),
            umpire_left_id: gc
                .umpires
                .iter()
                .find(|u| u.position == UmpirePosition::LeftField)
                .and_then(|u| u.umpire_id),
            umpire_right_id: gc
                .umpires
                .iter()
                .find(|u| u.position == UmpirePosition::RightField)
                .and_then(|u| u.umpire_id),
        }
    }
}

#[derive(Debug, Eq, PartialEq, Copy, Clone, Serialize, Deserialize)]
// Might generalize this to "game player totals" in case there's ever a `data` field
// other than earned runs
pub struct GameEarnedRuns {
    game_id: GameIdString,
    player_id: Pitcher,
    earned_runs: u8,
}

impl ContextToVec<'_> for GameEarnedRuns {
    fn from_game_context(gc: &GameContext) -> Box<dyn Iterator<Item = Self> + '_> {
        Box::from(gc.results.earned_runs.iter().map(move |er| Self {
            game_id: gc.game_id.id,
            player_id: er.pitcher_id,
            earned_runs: er.earned_runs,
        }))
    }
}

#[derive(Debug, Eq, PartialEq, Copy, Clone, Serialize, Deserialize)]
pub struct Events {
    game_id: GameIdString,
    event_id: EventId,
    event_key: EventKey,
    batting_side: Side,
    inning: u8,
    frame: InningFrame,
    batter_lineup_position: LineupPosition,
    batter_id: Player,
    pitcher_id: Player,
    batting_team_id: Team,
    fielding_team_id: Team,
    outs: Outs,
    base_state: u8,
    count_balls: Option<u8>,
    count_strikes: Option<u8>,
    specified_batter_hand: Option<Hand>,
    specified_pitcher_hand: Option<Hand>,
    strikeout_responsible_batter_id: Option<Player>,
    walk_responsible_pitcher_id: Option<Player>,
    plate_appearance_result: Option<PlateAppearanceResultType>,
    batted_contact_type: Option<ContactType>,
    batted_to_fielder: Option<FieldingPosition>,
    batted_location_general: Option<HitLocationGeneral>,
    batted_location_depth: Option<HitDepth>,
    batted_location_angle: Option<HitAngle>,
    batted_location_strength: Option<HitStrength>,
    outs_on_play: usize,
    runs_on_play: usize,
    runs_batted_in: usize,
    team_unearned_runs: usize,
}

impl ContextToVec<'_> for Events {
    fn from_game_context(gc: &GameContext) -> Box<dyn Iterator<Item = Self> + '_> {
        Box::from(gc.events.iter().map(move |e| {
            let batted_ball_info = e.results.batted_ball_info.as_ref();
            Self {
                game_id: gc.game_id.id,
                event_id: e.event_id,
                event_key: e.event_key,
                batting_side: e.context.batting_side,
                inning: e.context.inning,
                frame: e.context.frame,
                batter_lineup_position: e.context.at_bat,
                batter_id: e.context.batter_id,
                pitcher_id: e.context.pitcher_id,
                batting_team_id: match e.context.batting_side {
                    Side::Away => gc.teams.away,
                    Side::Home => gc.teams.home,
                },
                fielding_team_id: match e.context.batting_side {
                    Side::Away => gc.teams.home,
                    Side::Home => gc.teams.away,
                },
                outs: e.context.outs,
                base_state: e.context.starting_base_state.get_base_state(),
                count_balls: e.results.count_at_event.balls.map(BoundedU8::get),
                count_strikes: e.results.count_at_event.strikes.map(BoundedU8::get),
                specified_batter_hand: e.context.rare_attributes.batter_hand,
                specified_pitcher_hand: e.context.rare_attributes.pitcher_hand,
                strikeout_responsible_batter_id: e
                    .context
                    .rare_attributes
                    .strikeout_responsible_batter,
                walk_responsible_pitcher_id: e.context.rare_attributes.walk_responsible_pitcher,
                plate_appearance_result: e.results.plate_appearance,
                batted_contact_type: e
                    .results
                    .batted_ball_info
                    .as_ref()
                    .map(|i: &super::game_state::EventBattedBallInfo| i.contact),
                batted_to_fielder: batted_ball_info.and_then(|i| i.hit_to_fielder),
                batted_location_general: batted_ball_info.map(|i| i.general_location),
                batted_location_depth: batted_ball_info.map(|i| i.depth),
                batted_location_angle: batted_ball_info.map(|i| i.angle),
                batted_location_strength: batted_ball_info.map(|i| i.strength),
                outs_on_play: e.results.out_on_play.len(),
                runs_on_play: e.results.runs.len(),
                runs_batted_in: e.results.runs.iter().filter(|r| r.rbi_flag).count(),
                team_unearned_runs: e
                    .results
                    .runs
                    .iter()
                    .filter(|r| r.is_team_unearned_run())
                    .count(),
            }
        }))
    }
}

#[derive(Debug, Eq, PartialEq, Clone, Serialize, Deserialize)]
pub struct EventAudit {
    game_id: GameIdString,
    event_id: EventId,
    event_key: EventKey,
    filename: ArrayString<20>,
    line_number: usize,
}

impl ContextToVec<'_> for EventAudit {
    fn from_game_context(gc: &GameContext) -> Box<dyn Iterator<Item = Self> + '_> {
        Box::from(gc.events.iter().map(|e| Self {
            game_id: gc.game_id.id,
            event_id: e.event_id,
            event_key: e.event_key,
            filename: gc.file_info.filename,
            line_number: e.line_number,
        }))
    }
}

#[derive(Debug, Eq, PartialEq, Copy, Clone, Serialize, Deserialize)]
pub struct EventPitchSequences {
    game_id: GameIdString,
    event_id: EventId,
    event_key: EventKey,
    sequence_id: SequenceId,
    sequence_item: PitchType,
    runners_going_flag: bool,
    blocked_by_catcher_flag: bool,
    catcher_pickoff_attempt_at_base: Option<Base>,
}

impl ContextToVec<'_> for EventPitchSequences {
    fn from_game_context(gc: &GameContext) -> Box<dyn Iterator<Item = Self> + '_> {
        let pitch_sequences = gc.events.iter().flat_map(move |e| {
            e.results.pitch_sequence.iter().map(move |psi| Self {
                game_id: gc.game_id.id,
                event_id: e.event_id,
                event_key: e.event_key,
                sequence_id: psi.sequence_id,
                sequence_item: psi.pitch_type,
                runners_going_flag: psi.runners_going,
                blocked_by_catcher_flag: psi.blocked_by_catcher,
                catcher_pickoff_attempt_at_base: psi.catcher_pickoff_attempt,
            })
        });
        Box::from(pitch_sequences)
    }
}

#[derive(Debug, Eq, PartialEq, Copy, Clone, Serialize, Deserialize)]
pub struct EventFieldingPlays {
    game_id: GameIdString,
    event_id: EventId,
    event_key: EventKey,
    sequence_id: usize,
    fielding_position: FieldingPosition,
    fielding_play: FieldingPlayType,
}

impl ContextToVec<'_> for EventFieldingPlays {
    fn from_game_context(gc: &GameContext) -> Box<dyn Iterator<Item = Self> + '_> {
        Box::from(gc.events.iter().flat_map(move |e| {
            e.results
                .fielding_plays
                .iter()
                .enumerate()
                .map(move |(i, fp)| Self {
                    game_id: gc.game_id.id,
                    event_id: e.event_id,
                    event_key: e.event_key,
                    sequence_id: i + 1,
                    fielding_position: fp.fielding_position,
                    fielding_play: fp.fielding_play_type,
                })
        }))
    }
}

#[derive(Debug, Eq, PartialEq, Copy, Clone, Serialize, Deserialize)]
pub struct EventBaserunners {
    game_id: GameIdString,
    event_id: EventId,
    event_key: EventKey,
    baserunner: BaseRunner,
    runner_lineup_position: LineupPosition,
    runner_id: Player,
    charge_event_id: EventId,
    reached_on_event_id: Option<EventId>,
    explicit_charged_pitcher_id: Option<Player>,
    attempted_advance_to_base: Option<Base>,
    baserunning_play_type: Option<BaserunningPlayType>,
    is_out: bool,
    base_end: Option<Base>,
    advanced_on_error_flag: bool,
    explicit_out_flag: bool,
    run_scored_flag: bool,
    rbi_flag: bool,
}

impl EventBaserunners {
    fn runner(game_context: &GameContext, event: &E, baserunner: BaseRunner) -> Option<Self> {
        // Baserunning plays involve the runner if he's specifically mentioned or there is no runner mentioned
        let baserunning_play_type = event.results.plays_at_base.iter().find_map(|p| {
            if p.baserunner.unwrap_or(baserunner) == baserunner {
                Some(p.baserunning_play_type)
            } else {
                None
            }
        });
        let attempted_sb = baserunning_play_type
            .map(|p| p.is_attempted_stolen_base())
            .unwrap_or_default();
        let picked_off = match baserunning_play_type {
            Some(BaserunningPlayType::PickedOff) => true,
            _ => false,
        };

        let starting_state = event.context.starting_base_state.get_runner(baserunner);
        let advance = event
            .results
            .baserunning_advances
            .iter()
            .find(|a| a.baserunner == baserunner);
        match (starting_state, advance) {
            (Some(ss), Some(a)) => Some(Self {
                game_id: game_context.game_id.id,
                event_id: event.event_id,
                event_key: event.event_key,
                baserunner,
                runner_lineup_position: ss.lineup_position,
                runner_id: GameLineupAppearance::get_at_event(
                    &game_context.lineup_appearances,
                    ss.lineup_position,
                    event.event_id,
                    event.context.batting_side,
                )
                .unwrap()
                .player_id,
                charge_event_id: ss.charge_event_id,
                reached_on_event_id: Some(ss.reached_on_event_id),
                explicit_charged_pitcher_id: ss.explicit_charged_pitcher_id,
                attempted_advance_to_base: Some(a.attempted_advance_to),
                baserunning_play_type: baserunning_play_type,
                is_out: !a.is_successful,
                base_end: if a.is_successful {
                    Some(a.attempted_advance_to)
                } else {
                    None
                },
                advanced_on_error_flag: a.advanced_on_error_flag,
                explicit_out_flag: a.explicit_out_flag,
                run_scored_flag: a.run_scored_flag,
                rbi_flag: a.rbi_flag,
            }),
            // Runner was on base but either stayed put or got CS
            (Some(ss), None) => Some(Self {
                game_id: game_context.game_id.id,
                event_id: event.event_id,
                event_key: event.event_key,
                baserunner,
                runner_lineup_position: ss.lineup_position,
                runner_id: GameLineupAppearance::get_at_event(
                    &game_context.lineup_appearances,
                    ss.lineup_position,
                    event.event_id,
                    event.context.batting_side,
                )
                .unwrap()
                .player_id,
                charge_event_id: ss.charge_event_id,
                reached_on_event_id: Some(ss.reached_on_event_id),
                explicit_charged_pitcher_id: ss.explicit_charged_pitcher_id,
                attempted_advance_to_base: if attempted_sb {
                    Some(baserunner.to_next_base())
                } else {
                    None
                },
                baserunning_play_type,
                // If attempted_sb/pickoff is true but there's no advance, it's an out
                is_out: attempted_sb || picked_off,
                base_end: if attempted_sb || picked_off {
                    None
                } else {
                    baserunner.to_current_base()
                },
                advanced_on_error_flag: false,
                explicit_out_flag: attempted_sb,
                run_scored_flag: false,
                rbi_flag: false,
            }),
            // Batter if there was a play involving him
            (None, Some(a)) => Some(Self {
                game_id: game_context.game_id.id,
                event_id: event.event_id,
                event_key: event.event_key,
                baserunner,
                runner_lineup_position: event.context.at_bat,
                runner_id: event.context.batter_id,
                charge_event_id: event.event_id,
                reached_on_event_id: None,
                explicit_charged_pitcher_id: None,
                attempted_advance_to_base: Some(a.attempted_advance_to),
                // Batter could be involved on baserunning play for K+WP,PO,
                baserunning_play_type: baserunning_play_type,
                is_out: !a.is_successful,
                base_end: if a.is_successful {
                    Some(a.attempted_advance_to)
                } else {
                    None
                },
                advanced_on_error_flag: a.advanced_on_error_flag,
                explicit_out_flag: a.explicit_out_flag,
                run_scored_flag: a.run_scored_flag,
                rbi_flag: a.rbi_flag,
            }),
            (None, None) => None,
        }
    }
}

impl ContextToVec<'_> for EventBaserunners {
    fn from_game_context(gc: &GameContext) -> Box<dyn Iterator<Item = Self> + '_> {
        let runners = [
            BaseRunner::Batter,
            BaseRunner::First,
            BaseRunner::Second,
            BaseRunner::Third,
        ];
        Box::from(gc.events.iter().flat_map(move |e| {
            runners
                .into_iter()
                .filter_map(move |r| Self::runner(gc, e, r))
        }))
    }
}

#[derive(Debug, Eq, PartialEq, Clone, Serialize, Deserialize)]
pub struct EventComments {
    game_id: GameIdString,
    event_id: EventId,
    event_key: EventKey,
    sequence_id: usize,
    comment: String,
}

impl ContextToVec<'_> for EventComments {
    fn from_game_context(gc: &GameContext) -> Box<dyn Iterator<Item = Self> + '_> {
        Box::from(gc.events.iter().enumerate().flat_map(move |(i, e)| {
            e.results.comment.iter().map(move |c| Self {
                game_id: gc.game_id.id,
                event_id: e.event_id,
                event_key: e.event_key,
                sequence_id: i + 1,
                comment: c.clone(),
            })
        }))
    }
}

#[derive(Debug, Eq, PartialEq, Clone, Serialize, Deserialize)]
pub struct BoxScoreComments {
    game_id: GameIdString,
    sequence_id: usize,
    comment: String,
}

impl BoxScoreComments {
    pub fn from_record_slice(game_id: &GameIdString, slice: &RecordSlice) -> Vec<Self> {
        let mut comments = vec![];
        let mut sequence_id = 1;
        for record in slice {
            if let MappedRecord::Comment(c) = record {
                comments.push(Self {
                    game_id: game_id.clone(),
                    sequence_id: sequence_id,
                    comment: c.clone(),
                });
                sequence_id += 1;
            }
        }
        comments
    }
}

#[derive(Debug, Serialize, Clone)]
pub struct BoxScoreWritableRecord<'a> {
    pub game_id: GameIdString,
    #[serde(with = "either::serde_untagged")]
    pub record: Either<&'a BoxScoreLine, &'a BoxScoreEvent>,
}

impl BoxScoreWritableRecord<'_> {
    fn map_to_header(map: &Map<String, Value>) -> Result<Vec<String>> {
        let mut header = vec![];
        for (k, v) in map {
            match v {
                Value::Object(m) => {
                    header.extend(Self::map_to_header(m)?);
                }
                Value::Array(_) => bail!("Cannot make header out of struct with vec"),
                _ => header.push(k.clone()),
            }
        }
        Ok(header)
    }

    pub fn generate_header(&self) -> Result<Vec<String>> {
        let map = serde_json::to_value(self)?
            .as_object()
            .context("Unable to generate object")?
            .clone();
        Self::map_to_header(&map)
    }
}

#[derive(Debug, Serialize, Clone)]
pub struct BoxScoreLineScores {
    pub game_id: GameIdString,
    pub side: Side,
    pub inning: Inning,
    pub runs: u8,
}

impl BoxScoreLineScores {
    #[allow(clippy::cast_possible_truncation)]
    pub fn transform_line_score(
        game_id: GameIdString,
        raw_line: &LineScore,
    ) -> Box<dyn Iterator<Item = Self> + '_> {
        let iter = raw_line
            .line_score
            .iter()
            .enumerate()
            .map(move |(index, runs)| Self {
                game_id,
                side: raw_line.side,
                inning: (index + 1) as Inning,
                runs: *runs,
            });
        Box::from(iter)
    }
}

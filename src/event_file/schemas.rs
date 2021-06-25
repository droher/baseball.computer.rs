use chrono::{NaiveDate, NaiveTime};
use serde::{Deserialize, Serialize};

use crate::event_file::game_state::{EventId, EventFlag, GameContext, EventPlateAppearance};
use crate::event_file::info::{DayNight, DoubleheaderStatus, FieldCondition, HowScored, Park, Precipitation, Sky, Team, WindDirection};
use crate::event_file::misc::GameId;
use crate::event_file::pitch_sequence::{PitchType, SequenceItemTypeGeneral};
use crate::event_file::play::{Base, BaseRunner, HitAngle, HitDepth, HitLocationGeneral, HitStrength, InningFrame};
use crate::event_file::traits::{FieldingPlayType, FieldingPosition, GameType, Handedness, LineupPosition, Player, SequenceId, Side, Inning, Fielder};
use crate::event_file::box_score::PitchingLineStats;

trait ContextToVec {
    fn from_game_context(_: &GameContext) -> Box<dyn Iterator<Item=Self> + '_> where Self: Sized {
        unimplemented!()
    }
}

#[derive(Debug, Eq, PartialEq, Copy, Clone, Serialize, Deserialize)]
pub struct Game<'a> {
    game_id: GameId,
    date: NaiveDate,
    start_time: Option<NaiveTime>,
    doubleheader_status: DoubleheaderStatus,
    time_of_day: DayNight,
    game_type: GameType,
    bat_first_side: Side,
    sky: Sky,
    field_condition: FieldCondition,
    precipitation: Precipitation,
    wind_direction: WindDirection,
    scoring_method: HowScored,
    park_id: Park,
    temperature_fahrenheit: Option<u8>,
    attendance: Option<u32>,
    wind_speed_mph: Option<u8>,
    use_dh: bool,
    winning_pitcher:  Option<Player>,
    losing_pitcher: Option<Player>,
    save_pitcher: Option<Player>,
    game_winning_rbi: Option<Player>,
    time_of_game_minutes: Option<u16>,
    protest_info: Option<&'a str>,
    completion_info: Option<&'a str>
}

impl<'a> From<&'a GameContext> for Game<'a> {
    fn from(gc: &'a GameContext) -> Self {
        let setting = &gc.setting;
        let results = &gc.results;
        Self {
            game_id: gc.game_id,
            date: setting.date,
            start_time: setting.start_time,
            doubleheader_status: setting.doubleheader_status,
            time_of_day: setting.time_of_day,
            game_type: setting.game_type,
            bat_first_side: setting.bat_first_side,
            sky: setting.sky,
            field_condition: setting.field_condition,
            precipitation: setting.precipitation,
            wind_direction: setting.wind_direction,
            scoring_method: setting.how_scored,
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
            completion_info: results.completion_info.as_deref()
        }
    }
}

#[derive(Debug, Eq, PartialEq, Copy, Clone, Serialize, Deserialize)]
pub struct GameTeams {
    game_id: GameId,
    team_id: Team,
    side: Side
}

impl ContextToVec for GameTeams {
    fn from_game_context(gc: &GameContext) -> Box<dyn Iterator<Item = GameTeams>> {
        Box::from(vec![
            Self {
                game_id: gc.game_id,
                team_id: gc.teams.away,
                side: Side::Away
            },
            Self {
                game_id: gc.game_id,
                team_id: gc.teams.home,
                side: Side::Home
            }
        ]
            .into_iter()
        )
    }
}


#[derive(Debug, Eq, PartialEq, Copy, Clone, Serialize, Deserialize)]
pub struct Event {
    game_id: GameId,
    event_id: EventId,
    batting_side: Side,
    frame: InningFrame,
    at_bat: LineupPosition,
    outs: u8,
    count_balls: Option<u8>,
    count_strikes: Option<u8>,
    batter_hand: Handedness,
    pitcher_hand: Handedness,
}

impl ContextToVec for Event {
    fn from_game_context(gc: &GameContext) -> Box<dyn Iterator<Item=Event> + '_>{
        Box::from(gc.events
            .iter()
            .map(move |e| Self {
                game_id: gc.game_id,
                event_id: e.event_id,
                batting_side: e.context.batting_side,
                frame: e.context.frame,
                at_bat: e.context.at_bat,
                outs: e.context.outs,
                count_balls: e.results.count_at_event.balls,
                count_strikes: e.results.count_at_event.strikes,
                batter_hand: e.context.batter_hand,
                pitcher_hand: e.context.pitcher_hand,
            }))
    }
}


#[derive(Debug, Eq, PartialEq, Copy, Clone, Serialize, Deserialize)]
pub struct EventPitches {
    game_id: GameId,
    event_id: EventId,
    sequence_id: SequenceId,
    is_pitch: bool,
    is_strike: bool,
    sequence_item_general: SequenceItemTypeGeneral,
    sequence_item: PitchType,
    in_play_flag: bool,
    runners_going_flag: bool,
    blocked_by_catcher_flag: bool,
    catcher_pickoff_attempt_at_base: Option<Base>
}

impl ContextToVec for EventPitches {
    fn from_game_context(gc: &GameContext) -> Box<dyn Iterator<Item = EventPitches> + '_> {
        let pitch_sequences = gc.events
            .iter()
            .filter_map(|e|
                if let Some(psi) = &e.results.pitch_sequence { Some((e.event_id, psi)) }
                else { None }
            );
        let pitch_iter = pitch_sequences
            .flat_map(move |(event_id, pitches)| {
                pitches
                    .iter()
                    .map(move |psi| {
                        let general = psi.pitch_type.get_sequence_general();
                        Self {
                            game_id: gc.game_id,
                            event_id,
                            sequence_id: psi.sequence_id,
                            is_pitch: general.is_pitch(),
                            is_strike: general.is_strike(),
                            sequence_item_general: general,
                            sequence_item: psi.pitch_type,
                            in_play_flag: general.is_in_play(),
                            runners_going_flag: psi.runners_going,
                            blocked_by_catcher_flag: psi.blocked_by_catcher,
                            catcher_pickoff_attempt_at_base: psi.catcher_pickoff_attempt
                        }
                    })
            });
        Box::from(pitch_iter)
    }
}

impl ContextToVec for EventPlateAppearance {

}

#[derive(Debug, Eq, PartialEq, Copy, Clone, Serialize, Deserialize)]
pub struct EventFieldingPlays {
    game_id: GameId,
    event_id: EventId,
    sequence_id: SequenceId,
    fielding_position: FieldingPosition,
    fielding_play: FieldingPlayType
}

#[derive(Debug, Eq, PartialEq, Copy, Clone, Serialize, Deserialize)]
pub struct EventPlateAppearanceHitLocation {
    game_id: GameId,
    event_id: EventId,
    general_location: HitLocationGeneral,
    depth: HitDepth,
    angle: HitAngle,
    strength: HitStrength
}

#[derive(Debug, Eq, PartialEq, Copy, Clone, Serialize, Deserialize)]
pub struct EventOut {
    game_id: GameId,
    event_id: EventId,
    sequence_id: SequenceId,
    baserunner_out: BaseRunner
}

// Box score stats
pub struct BoxScoreLineScore {
    game_id: GameId,
    inning: Inning,
    side: Side,
    runs: u8
}

pub struct BoxScorePlayerHitting {
    game_id: GameId,
    player_id: Player,
    side: Side,
    lineup_position: LineupPosition,
    nth_player_at_position: u8,
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

pub struct BoxScorePlayerFielding {
    game_id: GameId,
    fielder_id: Fielder,
    side: Side,
    fielding_position: FieldingPosition,
    nth_position_played_by_player: u8,
    outs_played: Option<u8>,
    putouts: Option<u8>,
    assists: Option<u8>,
    errors: Option<u8>,
    double_plays: Option<u8>,
    triple_plays: Option<u8>,
    passed_balls: Option<u8>
}

pub struct BoxScorePlayerPitching {
    pitcher_id: Player,
    side: Side,
    nth_pitcher: u8,
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
    sacrifice_flies: Option<u8>
}
use chrono::{NaiveDate, NaiveTime};
use serde::{Deserialize, Serialize};

use crate::event_file::box_score::PitchingLineStats;
use crate::event_file::game_state::{EnteredGameAs, EventInfoType, PlateAppearanceResultType, Season};
use crate::event_file::info::{DayNight, DoubleheaderStatus, FieldCondition, HowScored, Precipitation, Sky, UmpirePosition, WindDirection};
use crate::event_file::pitch_sequence::{PitchType, SequenceItemTypeGeneral};
use crate::event_file::play::{Base, BaseRunner, BaserunningPlay, ContactType, HitAngle, HitDepth, HitLocationGeneral, HitStrength, InningFrame, BaserunningPlayType};
use crate::event_file::traits::{Fielder, FieldingPlayType, FieldingPosition, GameFileStatus, GameType, Handedness, LineupPosition, Side};


#[derive(Debug, Eq, PartialEq, Copy, Clone, Serialize, Deserialize)]
pub struct Game<'a> {
    game_id: &'a str,
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
    how_scored: HowScored,
    game_file_status: GameFileStatus,
    season: Season,
    park_id: &'a str,
    temperature_fahrenheit: Option<u8>,
    attendance: Option<u32>,
    wind_speed_mph: Option<u8>,
    use_dh: bool,
    winning_pitcher:  Option<&'a str>,
    losing_pitcher: Option<&'a str>,
    save_pitcher: Option<&'a str>,
    game_winning_rbi: Option<&'a str>,
    time_of_game_minutes: Option<u16>,
    protest_info: Option<&'a str>,
    completion_info: Option<&'a str>
}

#[derive(Debug, Eq, PartialEq, Copy, Clone, Serialize, Deserialize)]
pub struct GameTeams<'a> {
    game_id: &'a str,
    team_id: &'a str,
    side: Side
}

#[derive(Debug, Eq, PartialEq, Copy, Clone, Serialize, Deserialize)]
pub struct GameUmpires<'a> {
    game_id: &'a str,
    umpire_id: &'a str,
    position: UmpirePosition
}

#[derive(Debug, Eq, PartialEq, Copy, Clone, Serialize, Deserialize)]
pub struct GameLineupAppearance<'a> {
    game_id: &'a str,
    player_id: &'a str,
    side: Side,
    lineup_position: LineupPosition,
    entered_game_as: EnteredGameAs,
    start_event_id: u16,
    end_event_id: Option<u16>
}

#[derive(Debug, Eq, PartialEq, Copy, Clone, Serialize, Deserialize)]
pub struct GameFieldingAppearance<'a> {
    game_id: &'a str,
    player_id: &'a str,
    side: Side,
    fielding_position: FieldingPosition,
    start_event_id: u16,
    end_event_id: Option<u16>
}

#[derive(Debug, Eq, PartialEq, Copy, Clone, Serialize, Deserialize)]
pub struct Event<'a> {
    game_id: &'a str,
    event_id: u16,
    batting_side: Side,
    frame: InningFrame,
    at_bat: LineupPosition,
    outs: u8,
    count_balls: Option<u8>,
    count_strikes: Option<u8>,
    batter_hand: Handedness,
    pitcher_hand: Handedness,
    comment: Option<&'a str>
}

#[derive(Debug, Eq, PartialEq, Copy, Clone, Serialize, Deserialize)]
pub struct EventStartingBaseState<'a> {
    game_id: &'a str,
    event_id: u16,
    occupied_base: BaseRunner,
    runner_lineup_position: LineupPosition,
    charged_to_pitcher_id: &'a str
}

#[derive(Debug, Eq, PartialEq, Copy, Clone, Serialize, Deserialize)]
pub struct EventPitches<'a> {
    game_id: &'a str,
    event_id: u16,
    sequence_id: u8,
    is_pitch: bool,
    sequence_item_general: SequenceItemTypeGeneral,
    sequence_item: PitchType,
    runners_going_flag: bool,
    blocked_by_catcher_flag: bool,
    catcher_pickoff_attempt_at_base: Option<Base>
}

#[derive(Debug, Eq, PartialEq, Copy, Clone, Serialize, Deserialize)]
pub struct EventPlateAppearance<'a> {
    game_id: &'a str,
    event_id: u16,
    plate_appearance_result: PlateAppearanceResultType,
    contact: Option<ContactType>
}

#[derive(Debug, Eq, PartialEq, Copy, Clone, Serialize, Deserialize)]
pub struct EventFieldingPlays<'a> {
    game_id: &'a str,
    event_id: u16,
    sequence_id: u8,
    fielding_position: FieldingPosition,
    fielding_play: FieldingPlayType
}

#[derive(Debug, Eq, PartialEq, Copy, Clone, Serialize, Deserialize)]
pub struct EventPlateAppearanceHitLocation<'a> {
    game_id: &'a str,
    event_id: u16,
    general_location: HitLocationGeneral,
    depth: HitDepth,
    angle: HitAngle,
    strength: HitStrength
}

#[derive(Debug, Eq, PartialEq, Copy, Clone, Serialize, Deserialize)]
pub struct EventBaserunningPlays<'a> {
    game_id: &'a str,
    event_id: u16,
    sequence_id: u8,
    baserunning_play: BaserunningPlayType,
    at_base: Option<Base>
}


#[derive(Debug, Eq, PartialEq, Copy, Clone, Serialize, Deserialize)]
pub struct EventBaserunningAdvances<'a> {
    game_id: &'a str,
    event_id: u16,
    sequence_id: u8,
    baserunner: BaseRunner,
    attempted_advance_to: Base,
    is_out: bool,
    advanced_on_error_flag: bool,
    rbi_flag: bool,
    team_unearned_run_flag: bool
}

#[derive(Debug, Eq, PartialEq, Copy, Clone, Serialize, Deserialize)]
pub struct EventFlags<'a> {
    game_id: &'a str,
    event_id: u16,
    sequence_id: u8,
    flag: &'a str
}

// Box score stats
pub struct BoxScoreLineScore<'a> {
    game_id: &'a str,
    inning: u8,
    side: Side,
    runs: u8
}

pub struct BoxScorePlayerHitting<'a> {
    game_id: &'a str,
    player_id: &'a str,
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

pub struct BoxScorePlayerFielding<'a> {
    game_id: &'a str,
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

pub struct BoxScorePlayerPitching<'a> {
    pitcher_id: &'a str,
    side: Side,
    nth_pitcher: u8,
    pitching_stats: PitchingLineStats,
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
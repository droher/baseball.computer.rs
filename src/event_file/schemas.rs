use anyhow::{bail, Context, Result};
use bounded_integer::BoundedU8;
use chrono::{NaiveDate, NaiveDateTime};
use either::Either;
use itertools::Itertools;
use serde::{Deserialize, Serialize};
use serde_json::{Map, Value};
use tinystr::TinyStr16;

use crate::event_file::box_score::{BoxScoreEvent, BoxScoreLine, LineScore};
use crate::event_file::game_state::{EventId, GameContext, Outs};
use crate::event_file::info::{
    DayNight, DoubleheaderStatus, FieldCondition, HowScored, Park, Precipitation, Sky, Team,
    WindDirection,
};
use crate::event_file::misc::AppearanceRecord;
use crate::event_file::pitch_sequence::{PitchType, SequenceItemTypeGeneral};
use crate::event_file::play::{
    Base, BaseRunner, HitAngle, HitDepth, HitLocationGeneral, HitStrength, InningFrame,
};
use crate::event_file::traits::{
    FieldingPlayType, FieldingPosition, GameType, Inning, LineupPosition, Player, SequenceId, Side,
};
use crate::RecordSlice;

pub trait ContextToVec {
    fn from_game_context(gc: &GameContext) -> Box<dyn Iterator<Item = Self> + '_>
    where
        Self: Sized;
}

#[derive(Debug, Eq, PartialEq, Copy, Clone, Serialize, Deserialize)]
pub struct Game<'a> {
    game_id: TinyStr16,
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
    scoring_method: HowScored,
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
}

impl<'a> From<&'a GameContext> for Game<'a> {
    fn from(gc: &'a GameContext) -> Self {
        let setting = &gc.setting;
        let results = &gc.results;
        let start_time = setting.start_time.map(|time|
            NaiveDateTime::new(setting.date, time)
            );
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
            completion_info: results.completion_info.as_deref(),
        }
    }
}

#[derive(Debug, Eq, PartialEq, Copy, Clone, Serialize, Deserialize)]
pub struct GameTeam {
    game_id: TinyStr16,
    team_id: Team,
    side: Side,
}

impl ContextToVec for GameTeam {
    //noinspection RsTypeCheck
    fn from_game_context(gc: &GameContext) -> Box<dyn Iterator<Item = Self>> {
        Box::from(
            vec![
                Self {
                    game_id: gc.game_id.id,
                    team_id: gc.teams.away,
                    side: Side::Away,
                },
                Self {
                    game_id: gc.game_id.id,
                    team_id: gc.teams.home,
                    side: Side::Home,
                },
            ]
            .into_iter(),
        )
    }
}

#[derive(Debug, Eq, PartialEq, Copy, Clone, Serialize, Deserialize)]
pub struct Event {
    game_id: TinyStr16,
    event_id: EventId,
    batting_side: Side,
    frame: InningFrame,
    at_bat: LineupPosition,
    outs: Outs,
    count_balls: Option<u8>,
    count_strikes: Option<u8>,
}

impl ContextToVec for Event {
    //noinspection RsTypeCheck
    fn from_game_context(gc: &GameContext) -> Box<dyn Iterator<Item = Self> + '_> {
        Box::from(gc.events.iter().map(move |e| Self {
            game_id: gc.game_id.id,
            event_id: e.event_id,
            batting_side: e.context.batting_side,
            frame: e.context.frame,
            at_bat: e.context.at_bat,
            outs: e.context.outs,
            count_balls: e.results.count_at_event.balls.map(BoundedU8::get),
            count_strikes: e.results.count_at_event.strikes.map(BoundedU8::get),
        }))
    }
}

#[derive(Debug, Eq, PartialEq, Copy, Clone, Serialize, Deserialize)]
pub struct EventPitch {
    game_id: TinyStr16,
    event_id: EventId,
    sequence_id: SequenceId,
    is_pitch: bool,
    is_strike: bool,
    sequence_item_general: SequenceItemTypeGeneral,
    sequence_item: PitchType,
    in_play_flag: bool,
    runners_going_flag: bool,
    blocked_by_catcher_flag: bool,
    catcher_pickoff_attempt_at_base: Option<Base>,
}

impl ContextToVec for EventPitch {
    //noinspection RsTypeCheck
    fn from_game_context(gc: &GameContext) -> Box<dyn Iterator<Item = Self> + '_> {
        let pitch_sequences = gc.events.iter().filter_map(|e| {
            e.results
                .pitch_sequence
                .as_ref()
                .map(|psi| (e.event_id, psi))
        });
        let pitch_iter = pitch_sequences.flat_map(move |(event_id, pitches)| {
            pitches.iter().map(move |psi| {
                let general = psi.pitch_type.get_sequence_general();
                Self {
                    game_id: gc.game_id.id,
                    event_id,
                    sequence_id: psi.sequence_id,
                    is_pitch: general.is_pitch(),
                    is_strike: general.is_strike(),
                    sequence_item_general: general,
                    sequence_item: psi.pitch_type,
                    in_play_flag: general.is_in_play(),
                    runners_going_flag: psi.runners_going,
                    blocked_by_catcher_flag: psi.blocked_by_catcher,
                    catcher_pickoff_attempt_at_base: psi.catcher_pickoff_attempt,
                }
            })
        });
        Box::from(pitch_iter)
    }
}

#[derive(Debug, Eq, PartialEq, Copy, Clone, Serialize, Deserialize)]
pub struct EventFieldingPlay {
    game_id: TinyStr16,
    event_id: EventId,
    sequence_id: SequenceId,
    fielding_position: FieldingPosition,
    fielding_play: FieldingPlayType,
}

impl ContextToVec for EventFieldingPlay {
    //noinspection RsTypeCheck
    fn from_game_context(gc: &GameContext) -> Box<dyn Iterator<Item = Self> + '_> {
        Box::from(gc.events.iter().flat_map(|e| {
            e.results
                .fielding_plays
                .iter()
                .enumerate()
                .map(move |(i, fp)| Self {
                    game_id: e.game_id.id,
                    event_id: e.event_id,
                    sequence_id: SequenceId::new(i + 1).unwrap(),
                    fielding_position: fp.fielding_position,
                    fielding_play: fp.fielding_play_type,
                })
        }))
    }
}

#[derive(Debug, Eq, PartialEq, Copy, Clone, Serialize, Deserialize)]
pub struct EventHitLocation {
    game_id: TinyStr16,
    event_id: EventId,
    general_location: HitLocationGeneral,
    depth: HitDepth,
    angle: HitAngle,
    strength: HitStrength,
}

impl ContextToVec for EventHitLocation {
    //noinspection RsTypeCheck
    fn from_game_context(gc: &GameContext) -> Box<dyn Iterator<Item = Self> + '_> {
        Box::from(gc.events.iter().filter_map(|e| {
            if let Some(Some(hl)) = e
                .results
                .plate_appearance
                .as_ref()
                .map(|pa| pa.hit_location)
            {
                Some(Self {
                    game_id: e.game_id.id,
                    event_id: e.event_id,
                    general_location: hl.general_location,
                    depth: hl.depth,
                    angle: hl.angle,
                    strength: hl.strength,
                })
            } else {
                None
            }
        }))
    }
}

#[derive(Debug, Eq, PartialEq, Copy, Clone, Serialize, Deserialize)]
pub struct EventOut {
    game_id: TinyStr16,
    event_id: EventId,
    sequence_id: SequenceId,
    baserunner_out: BaseRunner,
}

impl ContextToVec for EventOut {
    //noinspection RsTypeCheck
    fn from_game_context(gc: &GameContext) -> Box<dyn Iterator<Item = Self> + '_> {
        Box::from(gc.events.iter().flat_map(|e| {
            e.results
                .out_on_play
                .iter()
                .enumerate()
                .map(move |(i, br)| Self {
                    game_id: e.game_id.id,
                    event_id: e.event_id,
                    sequence_id: SequenceId::new(i + 1).unwrap(),
                    baserunner_out: *br,
                })
        }))
    }
}

#[derive(Debug, Serialize, Clone)]
pub struct BoxScoreWritableRecord<'a> {
    pub game_id: TinyStr16,
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
pub struct BoxScoreLineScore {
    pub game_id: TinyStr16,
    pub side: Side,
    pub inning: Inning,
    pub runs: u8,
}

impl BoxScoreLineScore {
    pub fn transform_line_score(game_id: TinyStr16, raw_line: &LineScore) -> Vec<Self> {
        raw_line.line_score
            .iter()
            .enumerate()
            .map(|(index, runs)| Self {
                game_id,
                side: raw_line.side,
                inning: (index + 1) as Inning,
                runs: *runs
            } )
            .collect_vec()
    }
}

#[derive(Debug, Serialize, Clone)]
pub struct BoxScoreStarters {
    pub game_id: TinyStr16,
    pub record: AppearanceRecord,
}

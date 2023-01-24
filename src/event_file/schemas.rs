use anyhow::{bail, Context, Result};
use arrayvec::ArrayString;
use bounded_integer::BoundedU8;
use chrono::{NaiveDate, NaiveDateTime};
use either::Either;
use itertools::Itertools;
use serde::{Deserialize, Serialize};
use serde_json::{Map, Value};

use crate::event_file::box_score::{BoxScoreEvent, BoxScoreLine, LineScore};
use crate::event_file::game_state::{EventId, GameContext, Outs};
use crate::event_file::info::{
    DayNight, DoubleheaderStatus, FieldCondition, HowScored, Park, Precipitation, Sky, Team,
    WindDirection,
};
use crate::event_file::pitch_sequence::PitchType;
use crate::event_file::play::{
    Base, BaseRunner, HitAngle, HitDepth, HitLocationGeneral, HitStrength, InningFrame,
};
use crate::event_file::traits::{FieldingPlayType, FieldingPosition, GameType, Inning, LineupPosition, Pitcher, Player, SequenceId, Side};

use super::traits::{Scorer, RetrosheetVolunteer};

pub trait ContextToVec {
    fn from_game_context(gc: &GameContext) -> Box<dyn Iterator<Item = Self> + '_>
    where
        Self: Sized;
}

#[derive(Debug, Eq, PartialEq, Copy, Clone, Serialize, Deserialize)]
pub struct Game<'a> {
    game_id: ArrayString<16>,
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
    game_key: usize,
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
            game_type: gc.file_info.game_type,
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
        }
    }
}

#[derive(Debug, Eq, PartialEq, Copy, Clone, Serialize, Deserialize)]
pub struct GameTeam {
    game_id: ArrayString<16>,
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
// Might generalize this to "game player totals" in case there's ever a `data` field
// other than earned runs
pub struct GameEarnedRuns {
    game_id: ArrayString<16>,
    player_id: Pitcher,
    earned_runs: u8,
}

impl ContextToVec for GameEarnedRuns {
    fn from_game_context(gc: &GameContext) -> Box<dyn Iterator<Item = Self>> {
        Box::from(
            gc.results
                .earned_runs
                .iter()
                .map(|er| Self {
                    game_id: gc.game_id.id,
                    player_id: er.pitcher_id,
                    earned_runs: er.earned_runs,
                })
                .collect_vec()
                .into_iter(),
        )
    }
}

#[derive(Debug, Eq, PartialEq, Copy, Clone, Serialize, Deserialize)]
pub struct Event {
    game_id: ArrayString<16>,
    event_id: EventId,
    event_key: usize,
    batting_side: Side,
    inning: u8,
    frame: InningFrame,
    at_bat: LineupPosition,
    outs: Outs,
    count_balls: Option<u8>,
    count_strikes: Option<u8>,
}

impl ContextToVec for Event {
    fn from_game_context(gc: &GameContext) -> Box<dyn Iterator<Item = Self> + '_> {
        Box::from(gc.events.iter().map(move |e| Self {
            game_id: gc.game_id.id,
            event_id: e.event_id,
            event_key: e.event_key,
            batting_side: e.context.batting_side,
            inning: e.context.inning,
            frame: e.context.frame,
            at_bat: e.context.at_bat,
            outs: e.context.outs,
            count_balls: e.results.count_at_event.balls.map(BoundedU8::get),
            count_strikes: e.results.count_at_event.strikes.map(BoundedU8::get),
        }))
    }
}

#[derive(Debug, Eq, PartialEq, Clone, Serialize, Deserialize)]
pub struct EventRaw {
    event_key: usize,
    game_id: ArrayString<16>,
    event_id: EventId,
    filename: ArrayString<20>,
    line_number: usize,
    raw_play: String,
}

impl ContextToVec for EventRaw {
    fn from_game_context(gc: &GameContext) -> Box<dyn Iterator<Item = Self> + '_> {
        Box::from(gc.events.iter().map(move |e| Self {
            game_id: gc.game_id.id,
            event_id: e.event_id,
            event_key: e.event_key,
            filename: gc.file_info.filename,
            line_number: e.line_number,
            raw_play: e.raw_play.clone(),
        }))
    }
}

#[derive(Debug, Eq, PartialEq, Copy, Clone, Serialize, Deserialize)]
pub struct EventPitch {
    event_key: usize,
    sequence_id: SequenceId,
    sequence_item: PitchType,
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
                .map(|psi| (e.event_key, psi))
        });
        let pitch_iter = pitch_sequences.flat_map(move |(event_key, pitches)| {
            pitches.iter().map(move |psi| {
                Self {
                    event_key,
                    sequence_id: psi.sequence_id,
                    sequence_item: psi.pitch_type,
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
    event_key: usize,
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
                    event_key: e.event_key,
                    sequence_id: SequenceId::new(i + 1).unwrap(),
                    fielding_position: fp.fielding_position,
                    fielding_play: fp.fielding_play_type,
                })
        }))
    }
}

#[derive(Debug, Eq, PartialEq, Copy, Clone, Serialize, Deserialize)]
pub struct EventHitLocation {
    event_key: usize,
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
                    event_key: e.event_key,
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
    event_key: usize,
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
                    event_key: e.event_key,
                    sequence_id: SequenceId::new(i + 1).unwrap(),
                    baserunner_out: *br,
                })
        }))
    }
}

#[derive(Debug, Serialize, Clone)]
pub struct BoxScoreWritableRecord<'a> {
    pub game_id: ArrayString<16>,
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
    pub game_id: ArrayString<16>,
    pub side: Side,
    pub inning: Inning,
    pub runs: u8,
}

impl BoxScoreLineScore {
    pub fn transform_line_score(game_id: ArrayString<16>, raw_line: &LineScore) -> Vec<Self> {
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

use std::convert::TryFrom;
use std::str::FromStr;

use anyhow::{bail, Context, Error, Result};
use arrayref::array_ref;
use arrayvec::ArrayString;
use serde::{Deserialize, Serialize};

use crate::event_file::misc::{parse_positive_int, str_to_tinystr, Defense, Lineup};
use crate::event_file::traits::{
    Batter, Fielder, FieldingPosition, Inning, LineupPosition, Pitcher, RetrosheetEventRecord, Side,
};

#[derive(Debug, Eq, PartialEq, Clone, Copy, Default, Serialize, Deserialize)]
pub struct BattingLineStats {
    pub at_bats: u8,
    pub runs: u8,
    pub hits: u8,
    pub doubles: Option<u8>,
    pub triples: Option<u8>,
    pub home_runs: Option<u8>,
    pub rbi: Option<u8>,
    pub sacrifice_hits: Option<u8>,
    pub sacrifice_flies: Option<u8>,
    pub hit_by_pitch: Option<u8>,
    pub walks: Option<u8>,
    pub intentional_walks: Option<u8>,
    pub strikeouts: Option<u8>,
    pub stolen_bases: Option<u8>,
    pub caught_stealing: Option<u8>,
    pub grounded_into_double_plays: Option<u8>,
    pub reached_on_interference: Option<u8>,
}

impl From<BattingLineStats> for Vec<u8> {
    fn from(stats: BattingLineStats) -> Self {
        vec![
            stats.at_bats,
            stats.runs,
            stats.hits,
            stats.doubles.unwrap_or_default(),
            stats.triples.unwrap_or_default(),
            stats.home_runs.unwrap_or_default(),
            stats.rbi.unwrap_or_default(),
            stats.sacrifice_hits.unwrap_or_default(),
            stats.sacrifice_flies.unwrap_or_default(),
            stats.hit_by_pitch.unwrap_or_default(),
            stats.walks.unwrap_or_default(),
            stats.intentional_walks.unwrap_or_default(),
            stats.strikeouts.unwrap_or_default(),
            stats.stolen_bases.unwrap_or_default(),
            stats.caught_stealing.unwrap_or_default(),
            stats.grounded_into_double_plays.unwrap_or_default(),
            stats.reached_on_interference.unwrap_or_default(),
        ]
    }
}

impl TryFrom<&[&str; 17]> for BattingLineStats {
    type Error = Error;

    fn try_from(value: &[&str; 17]) -> Result<Self> {
        let o = { |i: usize| value[i].parse::<u8>().ok() };
        let u = {
            |i: usize| {
                value[i]
                    .parse::<u8>()
                    .context("Bad value for batting line stat")
            }
        };
        Ok(Self {
            at_bats: u(0)?,
            runs: u(1)?,
            hits: u(2)?,
            doubles: o(3),
            triples: o(4),
            home_runs: o(5),
            rbi: o(6),
            sacrifice_hits: o(7),
            sacrifice_flies: o(8),
            hit_by_pitch: o(9),
            walks: o(10),
            intentional_walks: o(11),
            strikeouts: o(12),
            stolen_bases: o(13),
            caught_stealing: o(14),
            grounded_into_double_plays: o(15),
            reached_on_interference: o(16),
        })
    }
}

#[derive(Debug, Eq, PartialEq, Clone, Copy, Serialize, Deserialize)]
pub struct BattingLine {
    pub batter_id: Batter,
    pub side: Side,
    pub lineup_position: LineupPosition,
    pub nth_player_at_position: u8,
    pub batting_stats: BattingLineStats,
}

impl BattingLine {
    pub fn from_lineup(side: Side, lineup: &Lineup) -> Vec<Self> {
        lineup
            .iter()
            .map(|(lineup_position, batter_id)| Self::new(*batter_id, side, *lineup_position, 1))
            .collect()
    }

    pub fn new(
        batter_id: Batter,
        side: Side,
        lineup_position: LineupPosition,
        nth_player_at_position: u8,
    ) -> Self {
        Self {
            batter_id,
            side,
            lineup_position,
            nth_player_at_position,
            batting_stats: BattingLineStats::default(),
        }
    }
}

impl TryFrom<&RetrosheetEventRecord> for BattingLine {
    type Error = Error;

    fn try_from(record: &RetrosheetEventRecord) -> Result<Self> {
        let arr = record.deserialize::<[&str; 23]>(None)?;
        let p = parse_positive_int::<u8>;
        Ok(Self {
            batter_id: str_to_tinystr(arr[2])?,
            side: Side::from_str(arr[3])?,
            lineup_position: LineupPosition::try_from(arr[4])?,
            nth_player_at_position: p(arr[5]).context("Invalid batting sequence position")?,
            batting_stats: BattingLineStats::try_from(array_ref![arr, 6, 17])?,
        })
    }
}

impl From<BattingLine> for RetrosheetEventRecord {
    fn from(line: BattingLine) -> Self {
        let mut record = Self::with_capacity(200, 24);
        record.push_field("stat");
        record.push_field("bline");
        record.push_field(line.batter_id.as_str());
        record.push_field(line.side.retrosheet_str());
        record.push_field(&line.lineup_position.retrosheet_string());
        record.push_field(&line.nth_player_at_position.to_string());
        let stats: Vec<u8> = line.batting_stats.into();
        for stat in stats {
            record.push_field(&stat.to_string());
        }
        record
    }
}

#[derive(Debug, Eq, PartialEq, Clone, Copy, Serialize, Deserialize)]
pub struct PinchHittingLine {
    pub pinch_hitter_id: Batter,
    inning: Option<Inning>,
    side: Side,
    pub batting_stats: BattingLineStats,
}

impl PinchHittingLine {
    pub fn new(pinch_hitter_id: Batter, inning: Option<Inning>, side: Side) -> Self {
        Self {
            pinch_hitter_id,
            side,
            inning,
            batting_stats: BattingLineStats::default(),
        }
    }
}

impl From<PinchHittingLine> for RetrosheetEventRecord {
    //noinspection RsTypeCheck
    fn from(line: PinchHittingLine) -> Self {
        let mut record = Self::with_capacity(200, 24);
        record.push_field("stat");
        record.push_field("phline");
        record.push_field(line.pinch_hitter_id.as_str());
        record.push_field(&line.inning.map_or(String::new(), |u| u.to_string()));
        record.push_field(line.side.retrosheet_str());
        let stats: Vec<u8> = line.batting_stats.into();
        for stat in stats {
            record.push_field(&stat.to_string());
        }
        record
    }
}

impl TryFrom<&RetrosheetEventRecord> for PinchHittingLine {
    type Error = Error;

    fn try_from(record: &RetrosheetEventRecord) -> Result<Self> {
        let arr = record.deserialize::<[&str; 22]>(None)?;
        let p = parse_positive_int::<u8>;
        Ok(Self {
            pinch_hitter_id: str_to_tinystr(arr[2])?,
            inning: p(arr[3]),
            side: Side::from_str(arr[4])?,
            batting_stats: BattingLineStats::try_from(array_ref![arr, 5, 17])
                .ok()
                .unwrap_or_default(),
        })
    }
}

#[derive(Debug, Eq, PartialEq, Clone, Copy, Serialize, Deserialize)]
pub struct PinchRunningLine {
    pub pinch_runner_id: Batter,
    inning: Option<Inning>,
    side: Side,
    pub runs: Option<u8>,
    pub stolen_bases: Option<u8>,
    pub caught_stealing: Option<u8>,
}

impl PinchRunningLine {
    pub const fn new(pinch_runner_id: Batter, inning: Option<Inning>, side: Side) -> Self {
        Self {
            pinch_runner_id,
            inning,
            side,
            runs: Some(0),
            stolen_bases: Some(0),
            caught_stealing: Some(0),
        }
    }
}

impl From<PinchRunningLine> for RetrosheetEventRecord {
    //noinspection RsTypeCheck
    //noinspection RsTypeCheck
    fn from(line: PinchRunningLine) -> Self {
        let mut record = Self::with_capacity(50, 7);
        record.push_field("stat");
        record.push_field("prline");
        record.push_field(line.pinch_runner_id.as_str());
        record.push_field(&line.inning.map_or(String::new(), |u| u.to_string()));
        record.push_field(line.side.retrosheet_str());
        record.push_field(&line.runs.unwrap_or_default().to_string());
        record.push_field(&line.stolen_bases.unwrap_or_default().to_string());
        record.push_field(&line.caught_stealing.unwrap_or_default().to_string());

        record
    }
}

impl TryFrom<&RetrosheetEventRecord> for PinchRunningLine {
    type Error = Error;

    fn try_from(record: &RetrosheetEventRecord) -> Result<Self> {
        let arr = record.deserialize::<[&str; 8]>(None)?;
        let p = { |i: usize| arr[i].parse::<u8>().ok() };
        Ok(Self {
            pinch_runner_id: str_to_tinystr(arr[2])?,
            inning: p(3),
            side: Side::from_str(arr[4])?,
            runs: p(5),
            stolen_bases: p(6),
            caught_stealing: p(7),
        })
    }
}

#[derive(Debug, Eq, PartialEq, Copy, Clone, Default, Serialize, Deserialize)]
pub struct DefenseLineStats {
    pub outs_played: Option<u8>,
    pub putouts: Option<u8>,
    pub assists: Option<u8>,
    pub errors: Option<u8>,
    pub double_plays: Option<u8>,
    pub triple_plays: Option<u8>,
    pub passed_balls: Option<u8>,
}

impl From<DefenseLineStats> for Vec<u8> {
    fn from(stats: DefenseLineStats) -> Self {
        vec![
            stats.outs_played.unwrap_or_default(),
            stats.putouts.unwrap_or_default(),
            stats.assists.unwrap_or_default(),
            stats.errors.unwrap_or_default(),
            stats.double_plays.unwrap_or_default(),
            stats.triple_plays.unwrap_or_default(),
            stats.passed_balls.unwrap_or_default(),
        ]
    }
}

impl TryFrom<&[&str; 7]> for DefenseLineStats {
    type Error = Error;

    fn try_from(value: &[&str; 7]) -> Result<Self> {
        let o = { |i: usize| value[i].parse::<u8>().ok() };
        Ok(Self {
            outs_played: o(0),
            putouts: o(1),
            assists: o(2),
            errors: o(3),
            double_plays: o(4),
            triple_plays: o(5),
            passed_balls: o(6),
        })
    }
}

#[derive(Debug, Eq, PartialEq, Copy, Clone, Serialize, Deserialize)]
pub struct DefenseLine {
    pub fielder_id: Fielder,
    pub side: Side,
    pub fielding_position: FieldingPosition,
    pub nth_position_played_by_player: u8,
    pub defensive_stats: Option<DefenseLineStats>,
}

impl DefenseLine {
    pub fn from_defense(side: Side, defense: &Defense) -> Vec<Self> {
        defense
            .iter()
            .map(|(fielding_position, fielder_id)| {
                Self::new(*fielder_id, side, *fielding_position, 1)
            })
            .collect()
    }

    pub fn new(
        fielder_id: Fielder,
        side: Side,
        fielding_position: FieldingPosition,
        nth_position_played_by_player: u8,
    ) -> Self {
        Self {
            fielder_id,
            side,
            fielding_position,
            nth_position_played_by_player,
            defensive_stats: Some(DefenseLineStats::default()),
        }
    }
}

impl TryFrom<&RetrosheetEventRecord> for DefenseLine {
    type Error = Error;

    fn try_from(record: &RetrosheetEventRecord) -> Result<Self> {
        let arr = record.deserialize::<[&str; 13]>(None)?;
        let p = parse_positive_int::<u8>;
        Ok(Self {
            fielder_id: str_to_tinystr(arr[2])?,
            side: Side::from_str(arr[3])?,
            nth_position_played_by_player: p(arr[4])
                .context("Invalid fielding sequence position")?,
            fielding_position: FieldingPosition::try_from(arr[5])?,
            defensive_stats: DefenseLineStats::try_from(array_ref![arr, 6, 7]).ok(),
        })
    }
}

impl From<DefenseLine> for RetrosheetEventRecord {
    fn from(line: DefenseLine) -> Self {
        let mut record = Self::with_capacity(50, 13);

        record.push_field("stat");
        record.push_field("dline");
        record.push_field(line.fielder_id.as_str());
        record.push_field(line.side.retrosheet_str());
        record.push_field(&line.nth_position_played_by_player.to_string());
        record.push_field(&line.fielding_position.retrosheet_string());

        let stats: Vec<u8> = line.defensive_stats.unwrap_or_default().into();
        for stat in stats {
            record.push_field(&stat.to_string());
        }
        record
    }
}

#[derive(Debug, Eq, PartialEq, Clone, Copy, Default, Serialize, Deserialize)]
pub struct PitchingLineStats {
    pub outs_recorded: u8,
    pub no_out_batters: Option<u8>,
    pub batters_faced: Option<u8>,
    pub hits: u8,
    pub doubles: Option<u8>,
    pub triples: Option<u8>,
    pub home_runs: Option<u8>,
    pub runs: u8,
    pub earned_runs: Option<u8>,
    pub walks: Option<u8>,
    pub intentional_walks: Option<u8>,
    pub strikeouts: Option<u8>,
    pub hit_batsmen: Option<u8>,
    pub wild_pitches: Option<u8>,
    pub balks: Option<u8>,
    pub sacrifice_hits: Option<u8>,
    pub sacrifice_flies: Option<u8>,
}

impl From<PitchingLineStats> for Vec<u8> {
    fn from(stats: PitchingLineStats) -> Self {
        vec![
            stats.outs_recorded,
            stats.no_out_batters.unwrap_or_default(),
            stats.batters_faced.unwrap_or_default(),
            stats.hits,
            stats.doubles.unwrap_or_default(),
            stats.triples.unwrap_or_default(),
            stats.home_runs.unwrap_or_default(),
            stats.runs,
            stats.earned_runs.unwrap_or_default(),
            stats.walks.unwrap_or_default(),
            stats.intentional_walks.unwrap_or_default(),
            stats.strikeouts.unwrap_or_default(),
            stats.hit_batsmen.unwrap_or_default(),
            stats.wild_pitches.unwrap_or_default(),
            stats.balks.unwrap_or_default(),
            stats.sacrifice_hits.unwrap_or_default(),
            stats.sacrifice_flies.unwrap_or_default(),
        ]
    }
}

impl TryFrom<&[&str; 17]> for PitchingLineStats {
    type Error = Error;

    fn try_from(value: &[&str; 17]) -> Result<Self> {
        let o = { |i: usize| value[i].parse::<u8>().ok() };
        let u = {
            |i: usize| {
                value[i]
                    .parse::<u8>()
                    .context("Bad value for pitching line stat")
            }
        };
        Ok(Self {
            outs_recorded: u(0)?,
            no_out_batters: o(1),
            batters_faced: o(2),
            hits: u(3)?,
            doubles: o(4),
            triples: o(5),
            home_runs: o(6),
            runs: u(7)?,
            earned_runs: o(8),
            walks: o(9),
            intentional_walks: o(10),
            strikeouts: o(11),
            hit_batsmen: o(12),
            wild_pitches: o(13),
            balks: o(14),
            sacrifice_hits: o(15),
            sacrifice_flies: o(16),
        })
    }
}

#[derive(Debug, Eq, PartialEq, Clone, Copy, Serialize, Deserialize)]
pub struct PitchingLine {
    pub pitcher_id: Pitcher,
    pub side: Side,
    nth_pitcher: u8,
    pub pitching_stats: PitchingLineStats,
}

impl PitchingLine {
    pub fn from_defense(side: Side, defense: &Defense) -> Result<Vec<Self>> {
        let pitcher_id = defense
            .get_by_left(&FieldingPosition::Pitcher)
            .context("No pitcher in defense provided")?;
        Ok(vec![Self::new(*pitcher_id, side, 1)])
    }

    pub fn new(pitcher_id: Pitcher, side: Side, nth_pitcher: u8) -> Self {
        Self {
            pitcher_id,
            side,
            nth_pitcher,
            pitching_stats: PitchingLineStats::default(),
        }
    }
}

impl TryFrom<&RetrosheetEventRecord> for PitchingLine {
    type Error = Error;

    fn try_from(record: &RetrosheetEventRecord) -> Result<Self> {
        let arr = record.deserialize::<[&str; 22]>(None)?;
        let p = parse_positive_int::<u8>;
        Ok(Self {
            pitcher_id: str_to_tinystr(arr[2])?,
            side: Side::from_str(arr[3])?,
            nth_pitcher: p(arr[4]).context("Invalid fielding sequence position")?,
            pitching_stats: PitchingLineStats::try_from(array_ref![arr, 5, 17])?,
        })
    }
}

impl From<PitchingLine> for RetrosheetEventRecord {
    fn from(line: PitchingLine) -> Self {
        let mut record = Self::with_capacity(200, 24);

        record.push_field("stat");
        record.push_field("pline");
        record.push_field(line.pitcher_id.as_str());
        record.push_field(line.side.retrosheet_str());
        record.push_field(&line.nth_pitcher.to_string());

        let stats: Vec<u8> = line.pitching_stats.into();
        for stat in stats {
            record.push_field(&stat.to_string());
        }
        record
    }
}

#[derive(Debug, Eq, PartialEq, Copy, Clone, Serialize, Deserialize)]
pub struct TeamMiscellaneousLine {
    pub side: Side,
    pub left_on_base: Option<u8>,
    pub team_earned_runs: Option<u8>,
    pub double_plays_turned: Option<u8>,
    pub triple_plays_turned: Option<u8>,
}

impl TeamMiscellaneousLine {
    pub const fn new(side: Side) -> Self {
        Self {
            side,
            left_on_base: Some(0),
            team_earned_runs: Some(0),
            double_plays_turned: Some(0),
            triple_plays_turned: Some(0),
        }
    }
}

impl From<TeamMiscellaneousLine> for RetrosheetEventRecord {
    fn from(line: TeamMiscellaneousLine) -> Self {
        let info = vec![
            "stat".to_string(),
            "tline".to_string(),
            line.side.retrosheet_str().to_string(),
            line.left_on_base.map_or(String::new(), |u| u.to_string()),
            line.team_earned_runs
                .map_or(String::new(), |u| u.to_string()),
            line.double_plays_turned
                .map_or(String::new(), |u| u.to_string()),
            line.triple_plays_turned
                .map_or(String::new(), |u| u.to_string()),
        ];
        Self::from(info)
    }
}

#[derive(Debug, Eq, PartialEq, Copy, Clone, Serialize, Deserialize)]
pub struct TeamBattingLine {
    side: Side,
    batting_stats: BattingLineStats,
}

impl TryFrom<&RetrosheetEventRecord> for TeamBattingLine {
    type Error = Error;

    fn try_from(record: &RetrosheetEventRecord) -> Result<Self> {
        let arr = record.deserialize::<[&str; 20]>(None)?;
        Ok(Self {
            side: Side::from_str(arr[2])?,
            batting_stats: BattingLineStats::try_from(array_ref![arr, 3, 17])?,
        })
    }
}

#[derive(Debug, Eq, PartialEq, Copy, Clone, Serialize, Deserialize)]
pub struct TeamDefenseLine {
    pub side: Side,
    pub defensive_stats: DefenseLineStats,
}

impl TryFrom<&RetrosheetEventRecord> for TeamDefenseLine {
    type Error = Error;

    fn try_from(record: &RetrosheetEventRecord) -> Result<Self> {
        let arr = record.deserialize::<[&str; 10]>(None)?;
        Ok(Self {
            side: Side::from_str(arr[2])?,
            defensive_stats: DefenseLineStats::try_from(array_ref![arr, 3, 7])?,
        })
    }
}

impl TryFrom<&RetrosheetEventRecord> for TeamMiscellaneousLine {
    type Error = Error;

    fn try_from(record: &RetrosheetEventRecord) -> Result<Self> {
        let arr = record.deserialize::<[&str; 7]>(None)?;
        let o = { |i: usize| arr[i].parse::<u8>().ok() };
        Ok(Self {
            side: Side::from_str(arr[2])?,
            left_on_base: o(3),
            team_earned_runs: o(4),
            double_plays_turned: o(5),
            triple_plays_turned: o(6),
        })
    }
}

#[derive(Debug, Eq, PartialEq, Copy, Clone, Serialize)]
pub enum BoxScoreLine {
    BattingLine(BattingLine),
    PinchHittingLine(PinchHittingLine),
    PinchRunningLine(PinchRunningLine),
    PitchingLine(PitchingLine),
    DefenseLine(DefenseLine),
    TeamMiscellaneousLine(TeamMiscellaneousLine),
    TeamBattingLine(TeamBattingLine),
    TeamDefenseLine(TeamDefenseLine),
    Unrecognized,
}

impl TryFrom<&RetrosheetEventRecord> for BoxScoreLine {
    type Error = Error;

    fn try_from(record: &RetrosheetEventRecord) -> Result<Self> {
        let stat_line_type = record.get(1).context("No stat line type")?;
        let mapped = match stat_line_type {
            "bline" => Self::BattingLine(BattingLine::try_from(record)?),
            "phline" => Self::PinchHittingLine(PinchHittingLine::try_from(record)?),
            "prline" => Self::PinchRunningLine(PinchRunningLine::try_from(record)?),
            "pline" => Self::PitchingLine(PitchingLine::try_from(record)?),
            "dline" => Self::DefenseLine(DefenseLine::try_from(record)?),
            "tline" => Self::TeamMiscellaneousLine(TeamMiscellaneousLine::try_from(record)?),
            "btline" => Self::TeamBattingLine(TeamBattingLine::try_from(record)?),
            "dtline" => Self::TeamDefenseLine(TeamDefenseLine::try_from(record)?),
            _ => Self::Unrecognized,
        };
        match mapped {
            Self::Unrecognized => bail!("Unrecognized box score line type {:?}", record),
            _ => Ok(mapped),
        }
    }
}

#[derive(Debug, Eq, PartialEq, Clone, Serialize, Deserialize)]
pub struct LineScore {
    pub side: Side,
    pub line_score: Vec<u8>,
}

impl TryFrom<&RetrosheetEventRecord> for LineScore {
    type Error = Error;

    fn try_from(record: &RetrosheetEventRecord) -> Result<Self> {
        let mut iter = record.iter();
        Ok(Self {
            side: Side::from_str(iter.nth(1).context("Missing team side")?)?,
            line_score: {
                let mut vec = Vec::with_capacity(9);
                for s in iter {
                    vec.push(s.parse::<u8>()?);
                }
                vec
            },
        })
    }
}

#[derive(Debug, Eq, PartialEq, Clone, Serialize, Deserialize)]
pub struct FieldingPlayLine {
    pub defense_side: Side,
    // Dashed sequence of numeric positions
    fielders: String,
}

pub type DoublePlayLine = FieldingPlayLine;
pub type TriplePlayLine = FieldingPlayLine;

impl TryFrom<&RetrosheetEventRecord> for FieldingPlayLine {
    type Error = Error;

    fn try_from(record: &RetrosheetEventRecord) -> Result<Self> {
        let mut iter = record.iter();
        Ok(Self {
            defense_side: Side::from_str(iter.nth(2).context("Missing team side")?)?,
            fielders: iter.collect::<Vec<&str>>().join("-"),
        })
    }
}

#[derive(Debug, Eq, PartialEq, Copy, Clone, Serialize, Deserialize)]
pub struct HitByPitchLine {
    pitching_side: Side,
    pitcher_id: Option<Pitcher>,
    batter_id: Batter,
}

impl HitByPitchLine {
    pub const fn new(pitching_side: Side, pitcher_id: Option<Pitcher>, batter_id: Batter) -> Self {
        Self {
            pitching_side,
            pitcher_id,
            batter_id,
        }
    }
}

impl TryFrom<&RetrosheetEventRecord> for HitByPitchLine {
    type Error = Error;

    fn try_from(record: &RetrosheetEventRecord) -> Result<Self> {
        let arr = record.deserialize::<[&str; 5]>(None)?;
        Ok(Self {
            pitching_side: Side::from_str(arr[2])?,
            pitcher_id: str_to_tinystr(arr[3]).ok(),
            batter_id: str_to_tinystr(arr[4])?,
        })
    }
}

#[derive(Debug, Eq, PartialEq, Copy, Clone, Serialize, Deserialize)]
pub struct HomeRunLine {
    batting_side: Side,
    batter_id: Batter,
    pitcher_id: Pitcher,
    inning: Option<Inning>,
    runners_on: Option<u8>,
    outs: Option<u8>,
}

impl HomeRunLine {
    pub const fn new(
        batting_side: Side,
        batter_id: Batter,
        pitcher_id: Pitcher,
        inning: Option<Inning>,
        runners_on: Option<u8>,
        outs: Option<u8>,
    ) -> Self {
        Self {
            batting_side,
            batter_id,
            pitcher_id,
            inning,
            runners_on,
            outs,
        }
    }
}

impl TryFrom<&RetrosheetEventRecord> for HomeRunLine {
    type Error = Error;

    fn try_from(record: &RetrosheetEventRecord) -> Result<Self> {
        let arr = record.deserialize::<[&str; 8]>(None)?;
        let p = { |i: usize| arr[i].parse::<u8>().ok() };
        Ok(Self {
            batting_side: Side::from_str(arr[2])?,
            batter_id: str_to_tinystr(arr[3])?,
            pitcher_id: str_to_tinystr(arr[4])?,
            inning: p(5),
            runners_on: p(6),
            outs: p(7),
        })
    }
}

#[derive(Debug, Eq, PartialEq, Copy, Clone, Serialize)]
pub struct StolenBaseAttemptLine {
    running_side: Side,
    runner_id: Batter,
    pitcher_id: Option<Pitcher>,
    catcher_id: Option<Fielder>,
    inning: Option<Inning>,
}

impl StolenBaseAttemptLine {
    pub const fn new(
        running_side: Side,
        runner_id: Batter,
        pitcher_id: Option<Pitcher>,
        catcher_id: Option<Fielder>,
        inning: Option<Inning>,
    ) -> Self {
        Self {
            running_side,
            runner_id,
            pitcher_id,
            catcher_id,
            inning,
        }
    }
}

pub type StolenBaseLine = StolenBaseAttemptLine;
pub type CaughtStealingLine = StolenBaseAttemptLine;

impl TryFrom<&RetrosheetEventRecord> for StolenBaseAttemptLine {
    type Error = Error;

    fn try_from(record: &RetrosheetEventRecord) -> Result<Self> {
        let arr = record.deserialize::<[&str; 7]>(None)?;
        Ok(Self {
            running_side: Side::from_str(arr[2])?,
            runner_id: str_to_tinystr(arr[3])?,
            pitcher_id: str_to_tinystr(arr[4]).ok(),
            catcher_id: str_to_tinystr(arr[5]).ok(),
            inning: arr[6].parse::<u8>().ok(),
        })
    }
}

#[derive(Debug, Eq, PartialEq, Clone, Serialize)]
#[serde(untagged)]
pub enum BoxScoreEvent {
    DoublePlay(DoublePlayLine),
    TriplePlay(TriplePlayLine),
    HitByPitch(HitByPitchLine),
    HomeRun(HomeRunLine),
    StolenBase(StolenBaseLine),
    CaughtStealing(CaughtStealingLine),
    Unrecognized,
}

impl From<BoxScoreEvent> for RetrosheetEventRecord {
    fn from(event: BoxScoreEvent) -> Self {
        let opt_str = |o: Option<ArrayString<8>>| o.map(|s| s.to_string()).unwrap_or_default();
        let mut record = Self::with_capacity(64, 8);
        record.push_field("event");
        match event {
            BoxScoreEvent::DoublePlay(dp) => {
                record.push_field("dpline");
                record.push_field(dp.defense_side.retrosheet_str());
                for fielder in dp.fielders.split('-') {
                    record.push_field(fielder);
                }
            }
            BoxScoreEvent::TriplePlay(tp) => {
                record.push_field("tpline");
                record.push_field(tp.defense_side.retrosheet_str());
                for fielder in tp.fielders.split('-') {
                    record.push_field(fielder);
                }
            }
            BoxScoreEvent::HitByPitch(hbp) => {
                record.push_field("hpline");
                record.push_field(hbp.pitching_side.retrosheet_str());
                record.push_field(&opt_str(hbp.pitcher_id));
                record.push_field(hbp.batter_id.as_str());
            }
            BoxScoreEvent::HomeRun(hr) => {
                record.push_field("hrline");
                record.push_field(hr.batting_side.retrosheet_str());
                record.push_field(hr.batter_id.as_str());
                record.push_field(hr.pitcher_id.as_str());
                record.push_field(&hr.inning.unwrap_or_default().to_string());
                record.push_field(&hr.outs.unwrap_or_default().to_string());
            }
            BoxScoreEvent::StolenBase(sb) => {
                record.push_field("sbline");
                record.push_field(sb.running_side.retrosheet_str());
                record.push_field(sb.runner_id.as_str());
                record.push_field(&opt_str(sb.pitcher_id));
                record.push_field(&opt_str(sb.catcher_id));
                record.push_field(&sb.inning.unwrap_or_default().to_string());
            }
            BoxScoreEvent::CaughtStealing(cs) => {
                record.push_field("sbline");
                record.push_field(cs.running_side.retrosheet_str());
                record.push_field(cs.runner_id.as_str());
                record.push_field(&opt_str(cs.pitcher_id));
                record.push_field(&opt_str(cs.catcher_id));
                record.push_field(&cs.inning.unwrap_or_default().to_string());
            }
            BoxScoreEvent::Unrecognized => (),
        };
        record
    }
}

impl TryFrom<&RetrosheetEventRecord> for BoxScoreEvent {
    type Error = Error;

    fn try_from(record: &RetrosheetEventRecord) -> Result<Self> {
        let event_line_type = record.get(1).context("No event type")?;
        let mapped = match event_line_type {
            "dpline" => Self::DoublePlay(DoublePlayLine::try_from(record)?),
            "tpline" => Self::TriplePlay(TriplePlayLine::try_from(record)?),
            "hpline" => Self::HitByPitch(HitByPitchLine::try_from(record)?),
            "hrline" => Self::HomeRun(HomeRunLine::try_from(record)?),
            "sbline" => Self::StolenBase(StolenBaseLine::try_from(record)?),
            "csline" => Self::CaughtStealing(CaughtStealingLine::try_from(record)?),
            _ => Self::Unrecognized,
        };
        match mapped {
            Self::Unrecognized => bail!("Unrecognized box score event type"),
            _ => Ok(mapped),
        }
    }
}

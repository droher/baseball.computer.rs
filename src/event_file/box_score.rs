use std::convert::TryFrom;
use std::str::FromStr;

use anyhow::{anyhow, Context, Error, Result};
use arrayref::array_ref;

use crate::event_file::traits::{FromRetrosheetRecord, RetrosheetEventRecord, Batter, LineupPosition, Inning, Fielder, FieldingPosition, Pitcher, Side};
use crate::util::{parse_positive_int, str_to_tinystr, digit_vec};
use std::num::NonZeroU8;
use tinystr::TinyStr8;

#[derive(Debug, Eq, PartialEq, Clone, Copy)]
struct BattingLineStats {
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

impl TryFrom<&[&str; 17]> for BattingLineStats {
    type Error = Error;

    fn try_from(value: &[&str; 17]) -> Result<BattingLineStats> {
        let o = {|i: usize| value[i].parse::<u8>().ok()};
        let u = {|i: usize|
            value[i].parse::<u8>().context("Bad value for batting line stat")
        };
        Ok(BattingLineStats {
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
            reached_on_interference: o(16)
        })
    }
}


#[derive(Debug, Eq, PartialEq, Clone)]
pub struct BattingLine {
    batter_id: Batter,
    side: Side,
    lineup_position: LineupPosition,
    nth_player_at_position: u8,
    batting_stats: BattingLineStats,
}

impl FromRetrosheetRecord for BattingLine {
    fn new(record: &RetrosheetEventRecord) -> Result<BattingLine> {
        let arr = record.deserialize::<[&str; 23]>(None)?;
        let p = parse_positive_int::<u8>;
        Ok(BattingLine{
            batter_id: str_to_tinystr(arr[2])?,
            side: Side::from_str(arr[3])?,
            lineup_position: LineupPosition::try_from(arr[4])?,
            nth_player_at_position: p(arr[5]).context("Invalid batting sequence position")?,
            batting_stats: BattingLineStats::try_from(array_ref![arr,6,17])?
        })
    }
}

#[derive(Debug, Eq, PartialEq, Clone, Copy)]
pub struct PinchHittingLine {
    pinch_hitter_id: Batter,
    inning: Option<Inning>,
    side: Side,
    batting_stats: Option<BattingLineStats>,
}

impl FromRetrosheetRecord for PinchHittingLine {
    fn new(record: &RetrosheetEventRecord) -> Result<PinchHittingLine> {
        let arr = record.deserialize::<[&str; 22]>(None)?;
        let p = parse_positive_int::<u8>;
        Ok(PinchHittingLine{
            pinch_hitter_id: str_to_tinystr(arr[2])?,
            inning: p(arr[3]),
            side: Side::from_str(arr[4])?,
            batting_stats: BattingLineStats::try_from(array_ref![arr,5,17]).ok()
        })
    }
}


#[derive(Debug, Eq, PartialEq, Clone)]
pub struct PinchRunningLine {
    pinch_runner_id: Batter,
    inning: Option<Inning>,
    side: Side,
    runs: Option<u8>,
    stolen_bases: Option<u8>,
    caught_stealing: Option<u8>
}

impl FromRetrosheetRecord for PinchRunningLine {
    fn new(record: &RetrosheetEventRecord) -> Result<PinchRunningLine>{
        let arr = record.deserialize::<[&str; 8]>(None)?;
        let p = {|i: usize| arr[i].parse::<u8>().ok()};
        Ok(PinchRunningLine{
            pinch_runner_id: str_to_tinystr(arr[2])?,
            inning: p(3),
            side: Side::from_str(arr[4])?,
            runs: p(5),
            stolen_bases: p(6),
            caught_stealing: p(7)
        })
    }
}

#[derive(Debug, Eq, PartialEq, Copy, Clone)]
pub struct DefenseLineStats {
    outs_played: Option<u8>,
    putouts: Option<u8>,
    assists: Option<u8>,
    errors: Option<u8>,
    double_plays: Option<u8>,
    triple_plays: Option<u8>,
    passed_balls: Option<u8>
}

impl TryFrom<&[&str; 7]> for DefenseLineStats {
    type Error = Error;

    fn try_from(value: &[&str; 7]) -> Result<DefenseLineStats> {
        let o = {|i: usize| value[i].parse::<u8>().ok()};
        Ok(DefenseLineStats {
            outs_played: o(0),
            putouts: o(1),
            assists: o(2),
            errors: o(3),
            double_plays: o(4),
            triple_plays: o(5),
            passed_balls: o(6)
        })
    }
}

#[derive(Debug, Eq, PartialEq, Copy, Clone)]
pub struct DefenseLine {
    fielder_id: Fielder,
    side: Side,
    fielding_position: FieldingPosition,
    nth_position_played_by_player: u8,
    defensive_stats: Option<DefenseLineStats>
}

impl FromRetrosheetRecord for DefenseLine {
    fn new(record: &RetrosheetEventRecord) -> Result<DefenseLine>{
        let arr = record.deserialize::<[&str; 13]>(None)?;
        let p = parse_positive_int::<u8>;
        Ok(DefenseLine{
            fielder_id: str_to_tinystr(arr[2])?,
            side: Side::from_str(arr[3])?,
            nth_position_played_by_player: p(arr[4]).context("Invalid fielding sequence position")?,
            fielding_position: FieldingPosition::try_from(arr[5])?,
            defensive_stats: DefenseLineStats::try_from(array_ref![arr,6,7]).ok(),
        })
    }
}

#[derive(Debug, Eq, PartialEq, Clone, Copy)]
pub struct PitchingLineStats {
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
    sacrifice_files: Option<u8>
}

impl TryFrom<&[&str; 17]> for PitchingLineStats {
    type Error = Error;

    fn try_from(value: &[&str; 17]) -> Result<PitchingLineStats> {
        let o = {|i: usize| value[i].parse::<u8>().ok()};
        let u = {|i: usize|
            value[i].parse::<u8>().context("Bad value for pitching line stat")
        };
        Ok(PitchingLineStats {
            outs_recorded: u(0)?,
            no_out_batters: o(1),
            batters_faced: o(2),
            hits: u(3)?,
            doubles:o(4),
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
            sacrifice_files: o(16)
        })
    }
}

#[derive(Debug, Eq, PartialEq, Clone, Copy)]
pub struct PitchingLine {
    pitcher_id: Pitcher,
    side: Side,
    nth_pitcher: u8,
    pitching_stats: PitchingLineStats
}

impl FromRetrosheetRecord for PitchingLine {
    fn new(record: &RetrosheetEventRecord) -> Result<PitchingLine>{
        let arr = record.deserialize::<[&str; 22]>(None)?;
        let p = parse_positive_int::<u8>;
        Ok(PitchingLine{
            pitcher_id: str_to_tinystr(arr[2])?,
            side: Side::from_str(arr[3])?,
            nth_pitcher: p(arr[4]).context("Invalid fielding sequence position")?,
            pitching_stats: PitchingLineStats::try_from(array_ref![arr,5,17])?,
        })
    }
}

#[derive(Debug, Eq, PartialEq, Copy, Clone)]
pub struct TeamMiscellaneousLine {
    side: Side,
    left_on_base: u8,
    team_earned_runs: Option<u8>,
    double_plays_turned: Option<u8>,
    triple_plays_turned: u8
}

#[derive(Debug, Eq, PartialEq, Copy, Clone)]
pub struct TeamBattingLine {
    side: Side,
    batting_stats: BattingLineStats
}

impl FromRetrosheetRecord for TeamBattingLine {
    fn new(record: &RetrosheetEventRecord) -> Result<TeamBattingLine> {
        let arr = record.deserialize::<[&str; 20]>(None)?;
        Ok(TeamBattingLine {
            side: Side::from_str(arr[2])?,
            batting_stats: BattingLineStats::try_from(array_ref![arr,3,17])?
        })
    }
}

#[derive(Debug, Eq, PartialEq, Copy, Clone)]
pub struct TeamDefenseLine {
    side: Side,
    defensive_stats: DefenseLineStats
}

impl FromRetrosheetRecord for TeamDefenseLine {
    fn new(record: &RetrosheetEventRecord) -> Result<TeamDefenseLine> {
        let arr = record.deserialize::<[&str; 10]>(None)?;
        Ok(TeamDefenseLine {
            side: Side::from_str(arr[2])?,
            defensive_stats: DefenseLineStats::try_from(array_ref![arr,3, 7])?
        })
    }
}


impl FromRetrosheetRecord for TeamMiscellaneousLine {
    fn new(record: &RetrosheetEventRecord) -> Result<TeamMiscellaneousLine>{
        let arr = record.deserialize::<[&str; 7]>(None)?;
        let o = {|i: usize| arr[i].parse::<u8>().ok()};
        let u = {|i: usize|
            arr[i].parse::<u8>().context("Bad value for team line stat")
        };
        Ok(TeamMiscellaneousLine {
            side: Side::from_str(arr[2])?,
            left_on_base: u(3)?,
            team_earned_runs: o(4),
            double_plays_turned: o(5),
            triple_plays_turned: u(6)?
        })
    }
}

#[derive(Debug, Eq, PartialEq, Clone)]
pub enum BoxScoreLine {
    BattingLine(BattingLine),
    PinchHittingLine(PinchHittingLine),
    PinchRunningLine(PinchRunningLine),
    PitchingLine(PitchingLine),
    DefenseLine(DefenseLine),
    TeamMiscellaneousLine(Option<TeamMiscellaneousLine>),
    TeamBattingLine(TeamBattingLine),
    TeamDefenseLine(TeamDefenseLine),
    Unrecognized
}

impl FromRetrosheetRecord for BoxScoreLine {
    fn new(record: &RetrosheetEventRecord) -> Result<BoxScoreLine>{
        let stat_line_type = record.get(1).context("No stat line type")?;
        let mapped= match stat_line_type {
            "bline" => BoxScoreLine::BattingLine(BattingLine::new(&record)?),
            "phline" => BoxScoreLine::PinchHittingLine(PinchHittingLine::new(&record)?),
            "prline" => BoxScoreLine::PinchRunningLine(PinchRunningLine::new(&record)?),
            "pline" => BoxScoreLine::PitchingLine(PitchingLine::new(&record)?),
            "dline" => BoxScoreLine::DefenseLine(DefenseLine::new(&record)?),
            "tline" => BoxScoreLine::TeamMiscellaneousLine(TeamMiscellaneousLine::new(&record).ok()),
            "btline" => BoxScoreLine::TeamBattingLine(TeamBattingLine::new(&record)?),
            "dtline" => BoxScoreLine::TeamDefenseLine(TeamDefenseLine::new(&record)?),
            _ => BoxScoreLine::Unrecognized
        };
        match mapped {
            BoxScoreLine::Unrecognized => Err(Self::error("Unrecognized box score line type", record)),
            _ => Ok(mapped)
        }
    }
}

#[derive(Debug, Eq, PartialEq, Clone)]
pub struct LineScore {
    side: Side,
    line_score: Vec<u8>
}

impl FromRetrosheetRecord for LineScore {
    fn new(record: &RetrosheetEventRecord) -> Result<LineScore>{
        let mut iter = record.iter();
        Ok(LineScore{
            side: Side::from_str(iter.nth(1).context("Missing team side")?)?,
            line_score: {
                let mut vec = Vec::with_capacity(9);
                for s in iter {vec.push(s.parse::<u8>()?)}
            vec
            }
        })
    }
}

#[derive(Debug, Eq, PartialEq, Clone)]
pub struct FieldingPlayLine {
    defense_side: Side,
    fielders: Vec<Fielder>
}

pub type DoublePlayLine = FieldingPlayLine;
pub type TriplePlayLine = FieldingPlayLine;

impl FromRetrosheetRecord for FieldingPlayLine {
    fn new(record: &RetrosheetEventRecord) -> Result<FieldingPlayLine>{
        let mut iter = record.iter();
        Ok(FieldingPlayLine{
            defense_side: Side::from_str(iter.nth(2).context("Missing team side")?)?,
            fielders: iter.filter_map(|f| str_to_tinystr(f).ok()).collect::<Vec<Fielder>>()
        })
    }
}


#[derive(Debug, Eq, PartialEq, Copy, Clone)]
pub struct HitByPitchLine {
    pitching_side: Side,
    pitcher_id: Option<Pitcher>,
    batter_id: Batter
}

impl FromRetrosheetRecord for HitByPitchLine {
    fn new(record: &RetrosheetEventRecord) -> Result<HitByPitchLine>{
        let arr = record.deserialize::<[&str; 5]>(None)?;
        Ok(HitByPitchLine{
            pitching_side: Side::from_str(arr[2])?,
            pitcher_id: str_to_tinystr(arr[3]).ok(),
            batter_id: str_to_tinystr(arr[4])?
        })
    }
}


#[derive(Debug, Eq, PartialEq, Copy, Clone)]
pub struct HomeRunLine {
    batting_side: Side,
    batter_id: Batter,
    pitcher_id: Pitcher,
    inning: Option<Inning>,
    runners_on: Option<u8>,
    outs: Option<u8>
}

impl FromRetrosheetRecord for HomeRunLine {
    fn new(record: &RetrosheetEventRecord) -> Result<HomeRunLine>{
        let arr = record.deserialize::<[&str; 8]>(None)?;
        let p = {|i: usize| arr[i].parse::<u8>().ok()};
        Ok(HomeRunLine{
            batting_side: Side::from_str(arr[2])?,
            batter_id: str_to_tinystr(arr[3])?,
            pitcher_id: str_to_tinystr(arr[4])?,
            inning: p(5),
            runners_on: p(6),
            outs: p(7)
        })
    }
}

#[derive(Debug, Eq, PartialEq, Copy, Clone)]
pub struct StolenBaseAttemptLine {
    running_side: Side,
    runner_id: Batter,
    pitcher_id: Option<Pitcher>,
    catcher_id: Option<Fielder>,
    inning: Option<Inning>
}

pub type StolenBaseLine = StolenBaseAttemptLine;
pub type CaughtStealingLine = StolenBaseAttemptLine;

impl FromRetrosheetRecord for StolenBaseAttemptLine {
    fn new(record: &RetrosheetEventRecord) -> Result<StolenBaseAttemptLine>{
        let arr = record.deserialize::<[&str; 7]>(None)?;
        Ok(StolenBaseAttemptLine{
            running_side: Side::from_str(arr[2])?,
            runner_id: str_to_tinystr(arr[3])?,
            pitcher_id: str_to_tinystr(arr[4]).ok(),
            catcher_id: str_to_tinystr(arr[5]).ok(),
            inning: arr[6].parse::<u8>().ok()

        })
    }
}

#[derive(Debug, Eq, PartialEq, Clone)]
pub enum BoxScoreEvent {
    DoublePlay(DoublePlayLine),
    TriplePlay(TriplePlayLine),
    HitByPitch(HitByPitchLine),
    HomeRun(HomeRunLine),
    StolenBase(StolenBaseLine),
    CaughtStealing(CaughtStealingLine),
    Unrecognized
}

impl FromRetrosheetRecord for BoxScoreEvent {
    fn new(record: &RetrosheetEventRecord) -> Result<BoxScoreEvent>{
        let event_line_type = record.get(1).context("No event type")?;
        let mapped = match event_line_type {
            "dpline" => BoxScoreEvent::DoublePlay(DoublePlayLine::new(&record)?),
            "tpline" => BoxScoreEvent::TriplePlay(TriplePlayLine::new(&record)?),
            "hpline" => BoxScoreEvent::HitByPitch(HitByPitchLine::new(&record)?),
            "hrline" => BoxScoreEvent::HomeRun(HomeRunLine::new(&record)?),
            "sbline" => BoxScoreEvent::StolenBase(StolenBaseLine::new(&record)?),
            "csline" => BoxScoreEvent::CaughtStealing(CaughtStealingLine::new(&record)?),
            _ => BoxScoreEvent::Unrecognized,

        };
        match mapped {
            BoxScoreEvent::Unrecognized => Err(anyhow!("Unrecognized box score event type {}", event_line_type)),
            _ => Ok(mapped)
        }
    }
}




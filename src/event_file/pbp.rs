use either::Either;
use crate::event_file::play::{PlayRecord, Count, InningFrame, Play, BaseRunner, Base, RunnerAdvance};
use crate::event_file::misc::{SubstitutionRecord, GameId, Lineup, Defense};
use crate::event_file::traits::{Inning, LineupPosition, Pitcher, Side, FieldingPosition, Fielder};
use crate::event_file::parser::{Matchup, GameInfo, Game, EventRecord};
use either::Either::Right;

use anyhow::{anyhow, Result, Context, Error};
use std::collections::HashMap;
use crate::event_file::box_score::*;
use std::convert::TryFrom;
use std::fs::read_to_string;
use crate::event_file::play::BaserunningPlayType::{DefensiveIndifference, Balk};
use arrayvec::ArrayVec;
use crate::event_file::play::PlayType::BaserunningPlay;

pub type Outs = u8;

type InitialLineSetup = (Matchup<Vec<BattingLine>>, Matchup<Vec<DefenseLine>>, Matchup<Vec<PitchingLine>>);

#[derive(Debug, Eq, PartialEq, Clone)]
pub struct BoxScore {
    pub batting_lines: Matchup<Vec<BattingLine>>,
    pub pinch_hitting_lines: Matchup<Vec<PinchHittingLine>>,
    pub pinch_running_lines: Matchup<Vec<PinchRunningLine>>,
    pub pitching_lines: Matchup<Vec<PitchingLine>>,
    pub defense_lines: Matchup<Vec<DefenseLine>>,
    pub team_miscellaneous_lines: Matchup<TeamMiscellaneousLine>,
    pub team_batting_lines: Matchup<TeamBattingLine>,
    pub team_defense_lines: Matchup<TeamDefenseLine>,
    pub events: Vec<BoxScoreEvent>
}

impl BoxScore {
    fn matchup_vec<T>(mut vecs: ArrayVec<[Vec<T>;2]>) -> Matchup<Vec<T>> {
        Matchup::new(vecs.remove(0), vecs.remove(0))
    }

    /// Finds how many players have already been slotted into this lineup position
    fn max_n_for_lineup(&self, side: Side, lineup_position: LineupPosition) -> u8 {
        self.batting_lines
            .get(&side)
            .iter()
            .filter(|bl| bl.lineup_position == lineup_position)
            .count() as u8
    }

    /// Finds how many positions this fielder has already played
    /// TODO: This currently treats PH/PR as a real position, should verify
    fn nth_position_played(&self, side: Side, fielder_id: Fielder) -> u8 {
        self.defense_lines
            .get(&side)
            .iter()
            .filter(|dl| dl.fielder_id == fielder_id)
            .count() as u8
    }
}

impl TryFrom<&Game> for BoxScore {
    type Error = Error;

    fn try_from(game: &Game) -> Result<Self> {
        if game.events.is_empty() {
            return Err(anyhow!("Cannot generate box score from events; no event records in game"))
        }
        let sides = [Side::Away, Side::Home];
        let batting_lines = Self::matchup_vec(
            sides.iter()
                .map(|s| BattingLine::from_lineup(*s, game.starting_lineups.get(s)))
                .collect());
        let defense_lines = Self::matchup_vec(
            sides.iter()
                .map(|s| DefenseLine::from_defense(*s, game.starting_defense.get(s)))
                .collect());
        let pitching_lines = Self::matchup_vec(sides.iter()
            .map(|s| PitchingLine::from_defense(*s, game.starting_defense.get(s)))
            .collect::<Result<ArrayVec<[Vec<PitchingLine>;2]>>>()?);
        unimplemented!();

    }
}



#[derive(Debug, Eq, PartialEq, Copy, Clone)]
pub struct Runner {lineup_position: LineupPosition, charged_to: Pitcher}

#[derive(Debug, Eq, PartialEq, Default, Clone)]
pub struct BaseState {
    first: Option<Runner>,
    second: Option<Runner>,
    third: Option<Runner>,
    scored: Vec<Runner>
}

impl BaseState {
    fn get_advance_from_baserunner(baserunner: BaseRunner, play: &Play) -> Option<RunnerAdvance> {
        play
            .advances()
            .iter()
            .find(|a| a.baserunner == baserunner)
            .cloned()
    }

    fn current_base_occupied(&self, advance: &RunnerAdvance) -> bool {
        advance.baserunner == BaseRunner::First && self.first.is_some() ||
            advance.baserunner == BaseRunner::Second && self.second.is_some() ||
            advance.baserunner == BaseRunner::Third && self.third.is_some()
    }

    fn target_base_occupied(&self, advance: &RunnerAdvance) -> bool {
        advance.to == Base::First && self.first.is_some() ||
            advance.to == Base::Second && self.second.is_some() ||
            advance.to == Base::Third && self.third.is_some()
    }

    fn check_integrity(old_state: &Self, new_state: &Self, advance: &RunnerAdvance) -> Result<bool> {
        if new_state.target_base_occupied(advance) {
            Err(anyhow!("Runner is listed as moving to a base that is occupied by another runner"))
        }
        else if !old_state.current_base_occupied(advance) {
            Err(anyhow!("Advancement from a base that had no runner on it"))
        }
        else {
            Ok(true)
        }
    }

    ///  Accounts for Rule 9.16(g) regarding the assignment of trailing
    ///  baserunners as inherited if they reach on a fielder's choice
    ///  in which an inherited runner is forced out 🙃
    fn update_runner_charges(&self, play: &Play) -> Self {
        // TODO: This
        self.clone()
    }

    fn update_base_state(&self, play: &Play, batter_lineup_position: LineupPosition, pitcher: Pitcher) -> Result<Self> {
        let mut new_state = self.clone();

        if let Some(a) = BaseState::get_advance_from_baserunner(BaseRunner::Third, play) {
            new_state.third = None;
            if a.is_out() {}
            else if let Err(e) = Self::check_integrity(&self, &new_state, &a) {
                return Err(e)
            }
            else if let Some(r) = self.third {
                new_state.scored.push(r)
            }
        }
        if let Some(a) = BaseState::get_advance_from_baserunner(BaseRunner::Second, play) {
            new_state.second = None;
            if a.is_out() {}
            else if let Err(e) = Self::check_integrity(&self, &new_state, &a) {
                return Err(e)
            }
            else if let (Ok(true), Some(r)) = (a.is_this_that_one_time_jean_segura_ran_in_reverse(), self.second) {
                new_state.first = Some(r)
            }
            else if let (Base::Third, Some(r)) = (a.to, self.second) {
                new_state.third = Some(r)
            }
            else if let (Base::Home, Some(r)) = (a.to, self.second) {
                new_state.scored.push(r)
            }
        }
        if let Some(a) = BaseState::get_advance_from_baserunner(BaseRunner::First, play) {
            new_state.first = None;
            if a.is_out() {}
            else if let Err(e) = Self::check_integrity(&self, &new_state, &a) {
                return Err(e)
            }
            else if let (Base::Second, Some(r)) = (&a.to, self.first) {
                new_state.second = Some(r)
            }
            else if let (Base::Third, Some(r)) = (&a.to, self.first) {
                new_state.third = Some(r)
            }
            else if let (Base::Home, Some(r)) = (&a.to, self.first) {
                new_state.scored.push(r)
            }
        }
        if let Some(a) = BaseState::get_advance_from_baserunner(BaseRunner::Batter, play) {
            let new_runner = Runner { lineup_position: batter_lineup_position, charged_to: pitcher };
            let opt_runner = Some(new_runner);
            match a.to {
                _ if a.is_out() => {},
                _ if new_state.target_base_occupied(&a) => return Err(anyhow!("Batter advanced to an occupied base")),
                Base::First => new_state.first = opt_runner,
                Base::Second => new_state.second = opt_runner,
                Base::Third => new_state.third = opt_runner,
                Base::Home => new_state.scored.push(new_runner)
            }
        }
        Ok(new_state.update_runner_charges(play))
    }


}

#[derive(Debug, Eq, PartialEq, Clone)]
pub struct GameState {
    inning: Inning,
    frame: InningFrame,
    batting_side: Side,
    outs: Outs,
    bases: BaseState,
    line_score: Matchup<Vec<u8>>,
    lineups: Matchup<Lineup>,
    defenses: Matchup<Defense>,
    at_bat: LineupPosition,
    count: Count,
    box_score: BoxScore
}

impl GameState {
    fn starting_state(game: &Game, bat_first_side: Side) -> Result<Self> {
        Ok(Self {
            inning: 1,
            frame: InningFrame::Top,
            batting_side: bat_first_side,
            outs: 0,
            bases: BaseState::default(),
            line_score: Matchup::default(),
            lineups: game.starting_lineups.clone(),
            defenses: game.starting_defense.clone(),
            at_bat: LineupPosition::First,
            count: Count::default(),
            box_score: BoxScore::try_from(game)?
        })
    }

    fn pitcher(&self) -> Pitcher {
        unimplemented!()
    }

    fn update_on_substitution(&self, sub: &SubstitutionRecord) -> Self {
        let mut new_lineup = self.lineups.get(&sub.side).clone();
        let mut new_defense = self.defenses.get(&sub.side).clone();
        let mut new_b_lines = self.box_score.batting_lines.get(&sub.side).clone();
        let mut new_d_lines = self.box_score.defense_lines.get(&sub.side).clone();
        let mut new_p_lines = self.box_score.pitching_lines.get(&sub.side).clone();

        let original_batter = self.lineups
            .get(&sub.side)
            .get_by_left(&sub.lineup_position);

        // If the substitute is already in the lineup (fielding change)
        // or is a pitcher with an active DH, no need to change batting line info
        let is_new_player = original_batter == Some(&sub.player);
        if is_new_player && sub.lineup_position.bats_in_lineup() {
            new_lineup.insert(sub.lineup_position, sub.player);
            new_b_lines.push(BattingLine::new(sub.player,
                                       sub.side,
                                       sub.lineup_position,
                                       self.box_score.max_n_for_lineup(sub.side, sub.lineup_position) + 1))
        }
        // Only update fielding info for real positions (not DH, PH, etc)
        if sub.fielding_position.plays_in_field() {
            new_defense.insert(sub.fielding_position, sub.player);
            new_d_lines.push(DefenseLine::new(sub.player,
                                       sub.side,
                                       sub.fielding_position,
                                       self.box_score.nth_position_played(sub.side,sub.player) + 1))
        }
        if sub.fielding_position == FieldingPosition::Pitcher {
            let nth_pitcher = new_p_lines.len() as u8 + 1;
            new_p_lines.push(PitchingLine::new(sub.player, sub.side, nth_pitcher));
        }

        let new_box_score = BoxScore {
            batting_lines: self.box_score.batting_lines.cloned_update(&sub.side, new_b_lines),
            defense_lines: self.box_score.defense_lines.cloned_update(&sub.side, new_d_lines),
            pitching_lines: self.box_score.pitching_lines.cloned_update(&sub.side, new_p_lines),
            ..self.box_score.clone()
        };

        Self {
            lineups: self.lineups.cloned_update(&sub.side, new_lineup),
            defenses: self.defenses.cloned_update(&sub.side, new_defense),
            box_score: new_box_score,
            ..self.clone()
        }
    }

    /// TODO:
    ///  -- Update hitting statistics for hitter and baserunners (R, SB, CS)
    ///  -- Update defense statistics for assists, putouts, errors
    ///  -- Pitching: basic stats, inherited runner logic should be settled by this point
    ///  -- Add any new events (HR, HBP, DP, etc.)
    fn update_box_score(&self, play: &Play) -> BoxScore {
        unimplemented!()
    }

    fn update_line_score(&self, play: &Play) -> Matchup<Vec<u8>> {
        unimplemented!()
    }

    fn update_on_play(&self, play_record: &PlayRecord) -> Result<Self> {
        let flipped = self.batting_side != play_record.side;
        let frame = if flipped {self.frame.flip()} else {self.frame};

        let outs = if flipped {0} else {self.outs + play_record.play.outs()?.len() as u8};
        if outs > 2 {return Err(anyhow!("Illegal state, 3 or more outs but frame continued"))}

        let at_bat = self.lineups
            .get(&play_record.side)
            .get_by_right(&play_record.batter)
            .context("Could not find batter in current side's lineup")?;

        Ok(Self {
            inning: play_record.inning,
            frame,
            batting_side: play_record.side,
            outs,
            bases: self.bases.update_base_state(&play_record.play, self.at_bat, self.pitcher())?,
            line_score: self.update_line_score(&play_record.play),
            lineups: self.lineups.clone(),
            defenses: self.defenses.clone(),
            at_bat: *at_bat,
            count: play_record.count,
            box_score: self.update_box_score(&play_record.play)
        })
    }

    fn next_state(&self, event: EventRecord) -> Self {
        self.clone()
    }
}

#[derive(Debug, Eq, PartialEq, Clone)]
pub struct Event<'a> {
    state: GameState,
    events: &'a Vec<EventRecord>
}
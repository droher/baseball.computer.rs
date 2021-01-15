use std::collections::HashMap;
use std::convert::{TryFrom, TryInto};

use anyhow::{anyhow, Context, Error, Result, bail};
use arrayvec::ArrayVec;
use either::Either;
use itertools::Itertools;

use crate::event_file::box_score::*;
use crate::event_file::misc::{Defense, Lineup, SubstitutionRecord, GameId, StartRecord};
use crate::event_file::parser::{EventRecord, RecordVec, MappedRecord, Teams};
use crate::event_file::play::{Base, BaseRunner, BaserunningPlayType, CachedPlay, HitType, InningFrame, OtherPlateAppearance, PlateAppearanceType, Play, RunnerAdvance};
use crate::event_file::traits::{Batter, Fielder, FieldingPosition, Inning, LineupPosition, Pitcher, RetrosheetEventRecord, Side, Umpire, RetrosheetVolunteer, Scorer, Matchup};
use crate::util::{count_occurrences, opt_add, u8_vec_to_string};
use crate::event_file::info::{InfoRecord, UmpirePosition, DoubleheaderStatus, DayNight, Sky, FieldCondition, Precipitation, WindDirection, Park, PitchDetail, HowScored};
use either::Either::{Left, Right};
use chrono::{NaiveTime, NaiveDate};
use tinystr::{TinyStr8, TinyStr16};
use std::str::FromStr;

pub type Outs = u8;

#[derive(Debug, Eq, PartialEq, Clone)]
pub struct BoxScore {
    batting_lines: Matchup<Vec<BattingLine>>,
    pinch_hitting_lines: Matchup<Vec<PinchHittingLine>>,
    pinch_running_lines: Matchup<Vec<PinchRunningLine>>,
    pitching_lines: Matchup<Vec<PitchingLine>>,
    defense_lines: Matchup<Vec<DefenseLine>>,
    team_miscellaneous_lines: Matchup<TeamMiscellaneousLine>,
    events: Vec<BoxScoreEvent>,
    line_score: Matchup<Vec<u8>>,
    team_unearned_runs: Matchup<u8>
}

impl BoxScore {
    fn matchup_vec<T>(mut vecs: ArrayVec<[Vec<T>;2]>) -> Matchup<Vec<T>> {
        Matchup::new(vecs.remove(0), vecs.remove(0))
    }

    fn get_batter_by_id(&mut self, side: Side, batter_id: Batter) -> Result<&mut BattingLine>  {
        Ok(self.batting_lines
            .get_mut(&side)
            .iter_mut()
            .find(|br| br.batter_id == batter_id)
            .context("Could not find batter in box score")?
        )
    }

    fn get_pinch_hitter_by_id(&mut self, side: Side, batter_id: Batter) -> Result<&mut PinchHittingLine>  {
        Ok(self.pinch_hitting_lines
            .get_mut(&side)
            .iter_mut()
            .find(|br| br.pinch_hitter_id == batter_id)
            .context("Could not find pinch-hitter in box score")?
        )
    }

    fn get_pinch_runner_by_id(&mut self, side: Side, batter_id: Batter) -> Result<&mut PinchRunningLine>  {
        Ok(self.pinch_running_lines
            .get_mut(&side)
            .iter_mut()
            .find(|br| br.pinch_runner_id == batter_id)
            .context("Could not find pinch-runner in box score")?
        )
    }

    fn get_line_from_runner(&mut self, side: Side, lineup: &Lineup, runner: &Runner) -> Result<&mut BattingLine>  {
        let runner_id = lineup
            .get_by_left(&runner.lineup_position)
            .context("Could not find runner lineup position")?;
        self.get_batter_by_id(side, *runner_id)
    }

    // TODO: Handle the Bryan Mitchell case (pitches, switches to another defensive position,
    //  pitches again)
    fn get_pitcher_by_id(&mut self, side: Side, pitcher_id: Pitcher) -> Result<&mut PitchingLine>  {
        Ok(self.pitching_lines
            .get_mut(&side)
            .iter_mut()
            .find(|pl| pl.pitcher_id == pitcher_id)
            .context("Could not find pitcher in box score")?
        )
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
    /// TODO: This currently excludes DH/PH/PR as real positions, should verify
    fn nth_position_played(&self, side: Side, fielder_id: Fielder) -> u8 {
        self.get_current_line_for_fielder(side, fielder_id)
            .map(|dl| dl.nth_position_played_by_player)
            .unwrap_or_default()
    }

    fn get_current_line_for_fielder(&self, side: Side, fielder_id: Fielder) -> Option<&DefenseLine> {
        let mut player_lines = self.defense_lines
            .get(&side)
            .iter()
            .filter(|dl| dl.fielder_id == fielder_id)
            .collect::<Vec<&DefenseLine>>();
        player_lines.sort_by_key(|pl| pl.nth_position_played_by_player);
        player_lines.pop()
    }

    fn add_earned_runs(&mut self, game: &BoxScoreGame) {
        // Individual
        let (away, home) = self.pitching_lines.get_both_mut();
        let mut team_er = Matchup::new(0, 0);
        for line in away.iter_mut().chain(home) {
            if let Some((_pitcher_id, earned_runs)) = game.earned_run_data.get_key_value(&line.pitcher_id) {
                opt_add(&mut line.pitching_stats.earned_runs, *earned_runs);
                *team_er.get_mut(&line.side) += *earned_runs;
            }
        }
        // Team
        let sides = [Side::Away, Side::Home];
        sides.iter()
            .for_each(|s| *team_er.get_mut(s) -= self.team_unearned_runs.get(s));
        sides.iter()
            .for_each(|s| opt_add(&mut self.team_miscellaneous_lines.get_mut(s).team_earned_runs,
                                  *team_er.get(s)));
    }
}

// TODO: PH, PR, Team, and Event Records
impl Into<Vec<RetrosheetEventRecord>> for BoxScore {

    fn into(self) -> Vec<RetrosheetEventRecord> {

        let (line_away, line_home) = self.line_score.apply_both(u8_vec_to_string);
        let lines = vec![line_away, line_home].into_iter()
            .zip(vec!["0", "1"])
            .map(|(line, side)| [vec!["line".to_string(), side.to_string()], line].concat())
            .map(RetrosheetEventRecord::from)
            .collect::<Vec<RetrosheetEventRecord>>();

        let (bat_away, bat_home) = self.batting_lines
            .apply_both(|v| v.into_iter()
                .sorted_by_key(|b| (b.lineup_position, b.nth_player_at_position))
                .map(RetrosheetEventRecord::from)
                .collect());
        let (d_away, d_home) = self.defense_lines
            .apply_both(|v| v.into_iter()
                // Filter out non-fielding positions
                .filter(|d| d.fielding_position.plays_in_field())
                .sorted_by_key(|d| (d.fielding_position, d.nth_position_played_by_player))
                .map(RetrosheetEventRecord::from)
                .collect());
        let (pitch_away, pitch_home) = self.pitching_lines
            .apply_both(|v| v.into_iter()
                .map(RetrosheetEventRecord::from)
                .collect());
        let (ph_away, ph_home) = self.pinch_hitting_lines
            .apply_both(|v| v.into_iter()
                .map(RetrosheetEventRecord::from)
                .collect());
        let (pr_away, pr_home) = self.pinch_running_lines
            .apply_both(|v| v.into_iter()
                .map(RetrosheetEventRecord::from)
                .collect());
        let (misc_away, misc_home) = self.team_miscellaneous_lines
            .apply_both(|m| vec![RetrosheetEventRecord::from(m)]);
        let events = self.events
            .into_iter()
            .map(RetrosheetEventRecord::from)
            .collect();

        [bat_away, bat_home, d_away, d_home, pitch_away, pitch_home, ph_away,
            ph_home, pr_away, pr_home, events, lines, misc_away, misc_home].concat()
    }
}

impl TryFrom<&BoxScoreGame> for BoxScore {
    type Error = Error;

    fn try_from(game: &BoxScoreGame) -> Result<Self> {
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
        let team_miscellaneous_lines = sides.iter()
                .map(|s| TeamMiscellaneousLine::new(*s))
                .collect::<ArrayVec<[TeamMiscellaneousLine;2]>>();
        Ok(Self {
            batting_lines,
            pinch_hitting_lines: Default::default(),
            pinch_running_lines: Default::default(),
            pitching_lines,
            defense_lines,
            team_miscellaneous_lines: Matchup::new(team_miscellaneous_lines[0], team_miscellaneous_lines[1]),
            events: vec![],
            line_score: Default::default(),
            team_unearned_runs: Default::default()
        })
    }
}



#[derive(Debug, Eq, PartialEq, Copy, Clone)]
pub struct Runner {pub lineup_position: LineupPosition, pub charged_to: Pitcher}

#[derive(Debug, Eq, PartialEq, Default, Clone)]
pub struct BaseState {
    bases: HashMap<BaseRunner, Runner>,
    scored: Vec<Runner>
}

impl BaseState {
    pub fn new_inning_tiebreaker(new_runner: LineupPosition, current_pitcher: Pitcher) -> Self {
        let mut state = Self::default();
        let runner = Runner { lineup_position: new_runner, charged_to: current_pitcher };
        state.bases.insert(BaseRunner::Second, runner);
        state
    }

    pub fn get_bases(&self) -> &HashMap<BaseRunner, Runner> {
        &self.bases
    }

    fn num_runners_on_base(&self) -> u8 {
        self.bases.len() as u8
    }

    fn get_runner(&self, baserunner: &BaseRunner) -> Option<&Runner> {
        self.bases.get(baserunner)
    }

    fn get_first(&self) -> Option<&Runner> {
        self.bases.get( &BaseRunner::First)
    }

    fn get_second(&self) -> Option<&Runner> {
        self.bases.get( &BaseRunner::Second)
    }

    fn get_third(&self) -> Option<&Runner> {
        self.bases.get( &BaseRunner::Third)
    }

    fn clear_baserunner(&mut self, baserunner: &BaseRunner) -> Option<Runner> {
        self.bases
            .remove(baserunner)
    }

    fn set_runner(&mut self, baserunner: BaseRunner, runner: Runner) {
        self.bases.insert(baserunner, runner);
    }

    fn get_advance_from_baserunner(baserunner: BaseRunner, cached_play: &CachedPlay) -> Option<&RunnerAdvance> {
        cached_play
            .advances
            .iter()
            .find(|a| a.baserunner == baserunner)
    }

    fn current_base_occupied(&self, advance: &RunnerAdvance) -> bool {
        self.get_runner(&advance.baserunner).is_some()
    }

    fn target_base_occupied(&self, advance: &RunnerAdvance) -> Result<bool> {
        let br = BaseRunner::from_target_base(&advance.to);
        Ok(self.get_runner(&br?).is_some())

    }

    fn check_integrity(old_state: &Self, new_state: &Self, advance: &RunnerAdvance) -> Result<()> {
        if new_state.target_base_occupied(advance)? {
            bail!("Runner is listed as moving to a base that is occupied by another runner")
        }
        else if !old_state.current_base_occupied(advance) {
            bail!("Advancement from a base that had no runner on it.\n\
            Old state: {:?}\n\
            New state: {:?}\n\
            Advance: {:?}\n", old_state, new_state, advance)
        }
        else {
            Ok(())
        }
    }

    ///  Accounts for Rule 9.16(g) regarding the assignment of trailing
    ///  baserunners as inherited if they reach on a fielder's choice
    ///  in which an inherited runner is forced out 🙃
    fn update_runner_charges(self, _play: &Play) -> Self {
        // TODO: This
        self
    }

    pub(crate) fn new_base_state(&self, start_inning: bool, end_inning: bool, cached_play: &CachedPlay, batter_lineup_position: LineupPosition, pitcher: Pitcher) -> Result<Self> {
        let play = &cached_play.play;

        let mut new_state = if start_inning {Self::default()} else {self.clone()};
        new_state.scored = vec![];

        // Cover cases where outs are not included in advance information
        for out in &cached_play.outs {
            new_state.clear_baserunner(out);
        }

        if let Some(a) = BaseState::get_advance_from_baserunner(BaseRunner::Third, &cached_play) {
            new_state.clear_baserunner(&BaseRunner::Third);
            if a.is_out() {}
            else if let Err(e) = Self::check_integrity(&self, &new_state, &a) {
                return Err(e)
            }
            else if let Some(r) = self.get_third() {
                new_state.scored.push(*r)
            }
        }
        if let Some(a) = BaseState::get_advance_from_baserunner(BaseRunner::Second, &cached_play) {
            new_state.clear_baserunner(&BaseRunner::Second);
            if a.is_out() {}
            else if let Err(e) = Self::check_integrity(&self, &new_state, &a) {
                return Err(e)
            }
            else if let (Ok(true), Some(r)) = (a.is_this_that_one_time_jean_segura_ran_in_reverse(), self.get_second()) {
                new_state.set_runner(BaseRunner::First, *r)
            }
            else if let (Base::Third, Some(r)) = (a.to, self.get_second()) {
                new_state.set_runner(BaseRunner::Third, *r)
            }
            else if let (Base::Home, Some(r)) = (a.to, self.get_second()) {
                new_state.scored.push(*r)
            }
        }
        if let Some(a) = BaseState::get_advance_from_baserunner(BaseRunner::First, &cached_play) {
            new_state.clear_baserunner(&BaseRunner::First);
            if a.is_out() {}
            else if let Err(e) = Self::check_integrity(&self, &new_state, &a) {
                return Err(e)
            }
            else if let (Base::Second, Some(r)) = (&a.to, self.get_first()) {
                new_state.set_runner(BaseRunner::Second, *r)
            }
            else if let (Base::Third, Some(r)) = (&a.to, self.get_first()) {
                new_state.set_runner(BaseRunner::Third, *r)
            }
            else if let (Base::Home, Some(r)) = (&a.to, self.get_first()) {
                new_state.scored.push(*r)
            }
        }
        if let Some(a) = BaseState::get_advance_from_baserunner(BaseRunner::Batter, &cached_play) {
            let new_runner = Runner { lineup_position: batter_lineup_position, charged_to: pitcher };
            match a.to {
                _ if a.is_out() || end_inning => {},
                _ if new_state.target_base_occupied(&a)? => return Err(anyhow!("Batter advanced to an occupied base")),
                Base::Home => new_state.scored.push(new_runner),
                b => new_state.set_runner(BaseRunner::from_current_base(&b)?, new_runner)
            }
        }
        Ok(new_state.update_runner_charges(&play))
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
    box_score: BoxScore,
}

impl GameState {
    fn starting_state(game: &BoxScoreGame) -> Result<Self> {
        Ok(Self {
            inning: 1,
            frame: InningFrame::Top,
            batting_side: game.bat_first_side(),
            outs: 0,
            bases: BaseState::default(),
            line_score: Matchup::default(),
            lineups: game.starting_lineups.clone(),
            defenses: game.starting_defense.clone(),
            at_bat: LineupPosition::First,
            box_score: BoxScore::try_from(game)?,
        })
    }

    fn pitcher(&self, side: &Side) -> Result<Pitcher> {
        Ok(*self.defenses
            .get(side)
            .get_by_left(&FieldingPosition::Pitcher)
            .context("Missing pitcher")?
        )
    }

    fn update_on_substitution(&mut self, sub: &SubstitutionRecord) {
        let original_batter = self.lineups
            .get(&sub.side)
            .get_by_left(&sub.lineup_position)
            .copied();
        let nth_player_at_position = self.box_score.max_n_for_lineup(sub.side, sub.lineup_position) + 1;
        let defense_incrementer: u8 = if sub.fielding_position.plays_in_field() {1} else {0};
        let nth_position_played_by_player = self.box_score.nth_position_played(sub.side,sub.player) + defense_incrementer;

        let new_lineup = self.lineups.get_mut(&sub.side);
        let new_defense = self.defenses.get_mut(&sub.side);
        let new_b_lines = self.box_score.batting_lines.get_mut(&sub.side);
        let new_d_lines = self.box_score.defense_lines.get_mut(&sub.side);
        let new_p_lines = self.box_score.pitching_lines.get_mut(&sub.side);
        let new_ph_lines = self.box_score.pinch_hitting_lines.get_mut(&sub.side);
        let new_pr_lines = self.box_score.pinch_running_lines.get_mut(&sub.side);



        // If the substitute is already in the lineup (fielding change)
        // or is a pitcher with an active DH, no need to change batting line info
        let is_new_player = original_batter != Some(sub.player);
        if is_new_player && sub.lineup_position.bats_in_lineup() {
            new_lineup.insert(sub.lineup_position, sub.player);
            new_b_lines.push(BattingLine::new(sub.player,
                                       sub.side,
                                       sub.lineup_position,
                                       nth_player_at_position));
            // Create PH/PR lines if entering as PH/PR
            match sub.fielding_position {
                FieldingPosition::PinchHitter => {
                    new_ph_lines.push(PinchHittingLine::new(sub.player,
                                                            Some(self.inning),
                                                            sub.side))
                },
                FieldingPosition::PinchRunner => {
                    new_pr_lines.push(PinchRunningLine::new(sub.player,
                                                            Some(self.inning),
                                                            sub.side))
                },
                _ => ()
            }
        }
        new_defense.insert(sub.fielding_position, sub.player);
        new_d_lines.push(DefenseLine::new(sub.player,
                                          sub.side,
                                          sub.fielding_position,
                                          nth_position_played_by_player));
        if sub.fielding_position == FieldingPosition::Pitcher {
            let nth_pitcher = new_p_lines.len() as u8 + 1;
            new_p_lines.push(PitchingLine::new(sub.player, sub.side, nth_pitcher));
        }
    }

    fn update_batter_stats(batting_stats: &mut BattingLineStats, cached_play: &CachedPlay) {
        let play = &cached_play.play;

        if cached_play.plate_appearance.is_none() {
            return
        }
        let plate_appearance = cached_play.plate_appearance.as_ref().unwrap();

        opt_add(&mut batting_stats.rbi, cached_play.rbi.len() as u8);
        if plate_appearance.is_at_bat() {batting_stats.at_bats += 1};
        if plate_appearance.is_strikeout() {opt_add(&mut batting_stats.strikeouts, 1)}
        if play.is_gidp() {opt_add(&mut batting_stats.grounded_into_double_plays, 1)}
        if play.sacrifice_hit() {opt_add(&mut batting_stats.sacrifice_hits, 1)}
        if play.sacrifice_fly() {opt_add(&mut batting_stats.sacrifice_flies, 1)}

        match plate_appearance {
            PlateAppearanceType::Hit(h) => {
                batting_stats.hits += 1;
                match h.hit_type {
                    HitType::Single => (),
                    HitType::Double | HitType::GroundRuleDouble => opt_add(&mut batting_stats.doubles, 1),
                    HitType::Triple => opt_add(&mut batting_stats.triples, 1),
                    HitType::HomeRun => opt_add(&mut batting_stats.home_runs, 1)
                }
            },
            PlateAppearanceType::OtherPlateAppearance(opa) => {
                match opa {
                    OtherPlateAppearance::Interference => { opt_add(&mut batting_stats.reached_on_interference, 1) },
                    OtherPlateAppearance::HitByPitch => { opt_add(&mut batting_stats.hit_by_pitch, 1) },
                    OtherPlateAppearance::Walk => { opt_add(&mut batting_stats.walks, 1) },
                    OtherPlateAppearance::IntentionalWalk => {
                        opt_add(&mut batting_stats.intentional_walks, 1);
                        opt_add(&mut batting_stats.walks, 1)
                    }
                }
            },
            _ => ()
        }
    }

    // TODO: No-out batters
    // Handles everything except runs, earned runs, and no out batters
    fn update_pitching_stats(pitching_stats: &mut PitchingLineStats, cached_play: &CachedPlay) {
        let play = &cached_play.play;
        pitching_stats.outs_recorded += cached_play.putouts.len() as u8;
        if play.wild_pitch() {opt_add(&mut pitching_stats.wild_pitches, 1)}
        if play.balk() {opt_add(&mut pitching_stats.balks, 1)}
        if play.sacrifice_hit() {opt_add(&mut pitching_stats.sacrifice_hits, 1)}
        if play.sacrifice_fly() {opt_add(&mut pitching_stats.sacrifice_flies, 1)}

        if let Some(pa) = &cached_play.plate_appearance {
            opt_add(&mut pitching_stats.batters_faced, 1);
            if pa.is_strikeout() {opt_add(&mut pitching_stats.strikeouts, 1)}
            match pa {
                PlateAppearanceType::Hit(h) => {
                    pitching_stats.hits += 1;
                    match h.hit_type {
                        HitType::Single => (),
                        HitType::Double | HitType::GroundRuleDouble => opt_add(&mut pitching_stats.doubles, 1),
                        HitType::Triple => opt_add(&mut pitching_stats.triples, 1),
                        HitType::HomeRun => opt_add(&mut pitching_stats.home_runs, 1)
                    }
                }
                PlateAppearanceType::OtherPlateAppearance(opa) => {
                    match opa {
                        OtherPlateAppearance::HitByPitch => { opt_add(&mut pitching_stats.hit_batsmen, 1) },
                        OtherPlateAppearance::Walk => { opt_add(&mut pitching_stats.walks, 1) },
                        OtherPlateAppearance::IntentionalWalk => {
                            opt_add(&mut pitching_stats.intentional_walks, 1);
                            opt_add(&mut pitching_stats.walks, 1)
                        }
                        _ => ()
                    }
                }
                _ => ()
            }

        }
    }

    fn update_defensive_stats(defense_stats: Option<&mut DefenseLineStats>,
                              fielding_position: FieldingPosition,
                              cached_play: &CachedPlay) -> Result<()> {
        let play = &cached_play.play;

        let ds = defense_stats.context("No defense stat object")?;

        let assists = count_occurrences(&cached_play.assists, &fielding_position);
        let putouts = count_occurrences(&cached_play.putouts, &fielding_position);
        let errors = count_occurrences(&cached_play.errors, &fielding_position);

        opt_add(&mut ds.outs_played, cached_play.putouts.len() as u8);
        opt_add(&mut ds.assists, assists);
        opt_add(&mut ds.putouts, putouts);
        opt_add(&mut ds.errors, errors);

        if cached_play.putouts.len() == 2 && assists + putouts > 0 {
            opt_add(&mut ds.double_plays, 1)
        } else if cached_play.putouts.len() == 3 && assists + putouts > 0 {
            opt_add(&mut ds.triple_plays, 1);
        }

        if fielding_position == FieldingPosition::Catcher && play.passed_ball() {
            opt_add(&mut ds.passed_balls, 1);
        }
        Ok(())
    }

    fn update_team_misc(&mut self, cached_play: &CachedPlay, new_base_state: &BaseState) -> Result<()> {
        let b_side = self.batting_side;
        let misc = &mut self.box_score.team_miscellaneous_lines;
        let lines = misc.get_both_mut();
        let (batting, defense) = match b_side {
            Side::Away => lines,
            Side::Home => (lines.1, lines.0)
        };
        let play_outs = cached_play.outs.len() as u8;
        match play_outs {
            2 => opt_add(&mut defense.double_plays_turned, 1),
            3 => defense.triple_plays_turned += 1,
            _ => ()
        }
        if play_outs > 0 && self.outs == 3 {
            batting.left_on_base += new_base_state.num_runners_on_base();
        }
        Ok(())
    }

    fn make_events(&self, cached_play: &CachedPlay) -> Result<Vec<BoxScoreEvent>> {
        let play = &cached_play.play;

        let d_side = cached_play.batting_side.flip();
        let mut events: Vec<BoxScoreEvent> = Vec::with_capacity(1);
        let putouts = &cached_play.putouts;

        let get_fielders = || {
            let mut fielders: Vec<FieldingPosition> = [cached_play.assists.clone(), cached_play.putouts.clone()].concat();
            fielders.dedup();
            fielders.iter()
                .filter_map(|f| self.defenses
                    .get(&d_side)
                    .get_by_left(f))
                .copied()
                .collect::<Vec<Fielder>>()
        };

        if putouts.len() == 2 {
            events.push(BoxScoreEvent::DoublePlay(DoublePlayLine::new(d_side, get_fielders())))
        }
        else if putouts.len() == 3 {
            events.push(BoxScoreEvent::TriplePlay(TriplePlayLine::new(d_side, get_fielders())))
        }

        if play.hit_by_pitch() {
            events.push(BoxScoreEvent::HitByPitch(HitByPitchLine::new(d_side,
                                                                      Some(self.pitcher(&d_side)?),
                                                                      cached_play.batter)))
        }
        else if play.home_run() {
            events.push(BoxScoreEvent::HomeRun(HomeRunLine::new(cached_play.batting_side,
                                                                cached_play.batter,
                                                                self.pitcher(&d_side)?,
                                                                Some(cached_play.inning),
                                                                Some(self.bases.num_runners_on_base()),
                                                                Some(self.outs))))
        }
        for sb_play in play.stolen_base_plays() {
            let base = sb_play.at_base.context("SB play missing base info")?;
            let runner = self.bases
                .get_runner(&BaseRunner::from_target_base(&base)?)
                .copied()
                .context("Missing runner info in Base State on SB play")?;
            let runner_id = *self.lineups
                .get(&cached_play.batting_side)
                .get_by_left(&runner.lineup_position)
                .context("Cannot find runner in lineup")?;
            let catcher = *self.defenses
                .get(&d_side)
                .get_by_left(&FieldingPosition::Catcher)
                .context("Cannot find catcher on SB play")?;
            let (running_side, pitcher_id, catcher_id, inning) = (
                cached_play.batting_side, Some(self.pitcher(&d_side)?), Some(catcher), Some(self.inning)
            );
            match sb_play.baserunning_play_type {
                BaserunningPlayType::StolenBase => {
                    events.push(BoxScoreEvent::StolenBase(StolenBaseLine::new(running_side, runner_id, pitcher_id, catcher_id, inning)));
                }
                BaserunningPlayType::CaughtStealing | BaserunningPlayType::PickedOffCaughtStealing => {
                    events.push(BoxScoreEvent::CaughtStealing(StolenBaseLine::new(running_side, runner_id, pitcher_id, catcher_id, inning)));
                },
                _ => ()
            }
        }
        Ok(events)
    }

    /// TODO: Unmess
    fn update_box_score(&mut self, cached_play: &CachedPlay, new_base_state: &BaseState) -> Result<()> {
        let (batting_side, fielding_side) = (cached_play.batting_side, cached_play.batting_side.flip());
        let pitcher_id = self.pitcher(&fielding_side)?;
        let events =self.make_events(cached_play)?;

        let new_box = &mut self.box_score;
        let play = &cached_play.play;
        let lineup = self.lineups.get(&batting_side);
        let defense = self.defenses.get(&fielding_side);

        // First add stats relating to the PA (if any) to the batter
        let batter_line = new_box.get_batter_by_id(batting_side, cached_play.batter)?;
        Self::update_batter_stats(&mut batter_line.batting_stats, cached_play);
        // Update PH-specific stat lines
        match  new_box.get_current_line_for_fielder(batting_side, cached_play.batter) {
            Some(fl) if fl.fielding_position == FieldingPosition::PinchHitter => {
                let pinch_hit_line = new_box.get_pinch_hitter_by_id(batting_side, cached_play.batter)?;
                if let Some(stats) = &mut pinch_hit_line.batting_stats {
                    Self::update_batter_stats(stats, cached_play);
                }

            },
            _ => ()
        }
        // Then add R/SB/CS to the batting lines of the baserunners
        for runner in &new_base_state.scored {
            let runner_line = new_box.get_line_from_runner(batting_side, lineup, runner)?;
            runner_line.batting_stats.runs += 1;
            let runner_id = runner_line.batter_id;
            match new_box.get_current_line_for_fielder(batting_side, runner_id) {
                Some(fl) if fl.fielding_position == FieldingPosition::PinchRunner => {
                    let pinch_run_line = new_box.get_pinch_runner_by_id(batting_side, runner_id)?;
                    opt_add(&mut pinch_run_line.runs, 1);
                },
                _ => ()
            }
        }
        for sb_play in play.stolen_base_plays() {
            let target_base = sb_play
                .at_base
                .context("Missing base info on stolen base attempt record")?;
            let baserunner = BaseRunner::from_target_base(&target_base)?;
            let runner = self.bases
                .get_runner(&baserunner)
                .context("Stolen base play recorded, but runner is missing")?;
            let runner_line = new_box.get_line_from_runner(batting_side, lineup, &runner)?;
            if sb_play.baserunning_play_type == BaserunningPlayType::StolenBase {
                opt_add(&mut runner_line.batting_stats.stolen_bases, 1);
            }
            else {
                opt_add(&mut runner_line.batting_stats.caught_stealing, 1);
            }

            let runner_id = runner_line.batter_id;
            match new_box.get_current_line_for_fielder(batting_side, runner_id) {
                Some(fl) if fl.fielding_position == FieldingPosition::PinchRunner => {
                    let pinch_run_line = new_box.get_pinch_runner_by_id(batting_side, runner_id)?;
                    if sb_play.baserunning_play_type == BaserunningPlayType::StolenBase {
                        opt_add(&mut pinch_run_line.stolen_bases, 1);
                    }
                    else {
                        opt_add(&mut pinch_run_line.caught_stealing, 1);
                    }
                },
                _ => ()
            }
        }

        // Pitching numbers are a bit easier to manage because for the most part there's only one pitcher
        // to keep track of, with the exception of inherited runners
        let pitching_line = new_box.get_pitcher_by_id(fielding_side, pitcher_id)?;
        Self::update_pitching_stats(&mut pitching_line.pitching_stats, cached_play);
        for runner in &new_base_state.scored {
            let pitching_line = new_box.get_pitcher_by_id(fielding_side, runner.charged_to)?;
            pitching_line.pitching_stats.runs += 1;
        }

        // Add up numbers for the defense -- every fielder needs to be adjusted because
        // we keep track of innings played
        let d_lines = new_box.defense_lines.get_mut(&fielding_side);
        for line in d_lines {
            if defense.get_by_left(&line.fielding_position) == Some(&line.fielder_id) {
                Self::update_defensive_stats(Option::from(&mut line.defensive_stats), line.fielding_position, cached_play)?;
            }
        }
        // Add team misc info
        new_box.events.extend(events);
        *new_box.team_unearned_runs.get_mut(&fielding_side) += cached_play.team_unearned_runs.len() as u8;
        self.update_team_misc(cached_play, new_base_state)?;

        Ok(())
    }

    fn update_line_score(&mut self, cached_play: &CachedPlay) -> Result<()> {
        let line_score = self.line_score.get_mut(&cached_play.batting_side);
        let diff = cached_play.inning - line_score.len() as u8;
        // Add a new frame if needed
        if diff == 1 { line_score.push(0) }
        else if diff != 0 {return Err(anyhow!("Line score out of sync with inning"))}

        let current_frame = line_score.pop().context("Empty line score")?;
        line_score.push(current_frame + cached_play.runs.len() as u8);

        Ok(())
    }

    fn outs_after_play(&self, cached_play: &CachedPlay) -> Result<u8> {
        let flipped = self.batting_side != cached_play.batting_side;
        let play_outs = cached_play.outs.len() as u8;
        match if flipped {play_outs} else {self.outs + play_outs} {
            o if o > 3 => Err(anyhow!("Illegal state, more than 3 outs recorded")),
            o => Ok(o)
        }
    }

    fn update_on_play(&mut self, cached_play: &CachedPlay) -> Result<()> {
        let play = &cached_play.play;
        if play.no_play() {
            return Ok(())
        }

        let flipped = self.batting_side != cached_play.batting_side;
        let frame = if flipped {self.frame.flip()} else {self.frame};
        let outs = self.outs_after_play(&cached_play)?;

        let at_bat = self.lineups
            .get(&cached_play.batting_side)
            .get_by_right(&cached_play.batter)
            .copied()
            .context("Could not find batter in current side's lineup")?;

        let new_base_state = self.bases.new_base_state(flipped,
                                                       outs == 3,
                                                       &cached_play,
                                                       at_bat,
                                                       self.pitcher(&cached_play.batting_side.flip())?)?;

        self.inning = cached_play.inning;
        self.frame = frame;
        self.batting_side = cached_play.batting_side;
        self.outs = outs;
        self.update_box_score(&cached_play, &new_base_state)?;
        self.bases = new_base_state;
        self.update_line_score(&cached_play)?;
        self.at_bat = at_bat;

        Ok(())
    }

    fn next_state(&mut self, event_record: &EventRecord) -> Result<()> {
        match event_record {
            Either::Left(pr) => self.update_on_play(&CachedPlay::try_from(pr)?),
            Either::Right(sr) => {
                self.update_on_substitution(sr);
                Ok(())
            }
        }
    }

    pub fn get_box_score(game: &BoxScoreGame) -> Result<BoxScore> {
        let mut state = Self::starting_state(game)?;
        for event in &game.events {
            state.next_state(event)?;
        }
        state.box_score.add_earned_runs(&game);
        state.box_score.line_score = state.line_score.clone();
        Ok(state.box_score)
    }
}

#[derive(Debug, Eq, PartialEq, Clone)]
pub struct BoxScoreGame {
    pub retrosheet_id: GameId,
    pub info: GameInfo,
    pub events: Vec<EventRecord>,
    pub starts: Vec<StartRecord>,
    pub starting_lineups: Matchup<Lineup>,
    pub starting_defense: Matchup<Defense>,
    pub earned_run_data: HashMap<Pitcher, u8>
}

impl TryInto<Vec<RetrosheetEventRecord>> for BoxScoreGame {
    type Error = Error;

    fn try_into(self) -> Result<Vec<RetrosheetEventRecord>> {
        let box_score = GameState::get_box_score(&self)?.into();

        let id_fields =  vec![
            RetrosheetEventRecord::from(vec!["id", self.retrosheet_id.id.as_str()]),
            RetrosheetEventRecord::from(vec!["version", "3"]),
            RetrosheetEventRecord::from(vec!["info", "inputprogvers", "version 7RS(19) of 07/07/92"])
        ];
        let info: Vec<RetrosheetEventRecord> = self.info.into();
        let starts: Vec<RetrosheetEventRecord> = self.starts.iter().map({
            |sr| RetrosheetEventRecord::from(vec![
                "start".to_string(),
                sr.player.to_string(),
                sr.player_name.clone(),
                sr.side.retrosheet_str().to_string(),
                sr.lineup_position.retrosheet_string(),
                sr.fielding_position.retrosheet_string()
            ])
        })
            .collect();

        Ok([id_fields, info, starts, box_score].concat())
    }
}

impl TryFrom<&RecordVec> for BoxScoreGame {
    type Error = Error;

    fn try_from(record_vec: &RecordVec) -> Result<Self> {
        let id = *record_vec.iter()
            .find_map(|m| if let MappedRecord::GameId(g) = m {Some(g)} else {None})
            .context("Did not find game ID in list of records")?;
        let infos = record_vec.iter()
            .filter_map(|m| if let MappedRecord::Info(i) = m {Some(*i)} else {None})
            .collect::<Vec<InfoRecord>>();
        let info = GameInfo::try_from(&infos)?;
        let starts =  record_vec.iter()
            .filter_map(|m| if let MappedRecord::Start(s) = m {Some(s.clone())} else {None})
            .collect::<Vec<StartRecord>>();
        let (starting_lineups, starting_defense) = Self::assemble_lineups_and_defense(starts.clone());
        let events = record_vec.iter()
            .filter_map(|m| match m {
                MappedRecord::Play(pr) => Some(Left(pr.clone())),
                MappedRecord::Substitution(sr) => Some(Right(sr.clone())),
                _ => None
            })
            .collect();
        let earned_run_data = record_vec.iter()
            .filter_map(|m| if let MappedRecord::EarnedRun(e) = m {
                Some((e.pitcher_id, e.earned_runs))
            } else {None})
            .collect();
        Ok(Self {
            retrosheet_id: id,
            info,
            starts,
            events,
            starting_lineups,
            starting_defense,
            earned_run_data
        })
    }


}

impl BoxScoreGame {

    pub fn bat_first_side(&self) -> Side {
        self.info.setting.bat_first_side
    }

    fn assemble_lineups_and_defense(start_records: Vec<StartRecord>) -> (Matchup<Lineup>, Matchup<Defense>)  {
        // TODO: DRY
        let (mut away_lineup, mut home_lineup) = (Lineup::with_capacity(10), Lineup::with_capacity(10));
        let (mut away_defense, mut home_defense) = (Defense::with_capacity(10), Defense::with_capacity(10));
        let (away_records, home_records): (Vec<StartRecord>, Vec<StartRecord>) = start_records.into_iter()
            // TODO: Partition in place once method stabilized
            .partition(|sr| sr.side == Side::Away);

        away_records.into_iter().zip(home_records).map(|(away, home)| {
            away_lineup.insert(away.lineup_position, away.player);
            away_defense.insert(away.fielding_position, away.player);
            home_lineup.insert(home.lineup_position, home.player);
            home_defense.insert(home.fielding_position, home.player);
        }).for_each(drop);

        (Matchup::new(away_lineup, home_lineup),
         Matchup::new(away_defense, home_defense))
    }

}

#[derive(Debug, Default, PartialOrd, PartialEq, Eq, Clone, Copy)]
pub struct GameUmpires {
    home: Option<Umpire>,
    first: Option<Umpire>,
    second: Option<Umpire>,
    third: Option<Umpire>,
    left: Option<Umpire>,
    right: Option<Umpire>
}

impl Into<Vec<RetrosheetEventRecord>> for GameUmpires {
    fn into(self) -> Vec<RetrosheetEventRecord> {
        let opt_string = {
            |o: Option<Umpire>| o.map_or("".to_string(), |u| u.to_string())
        };
        let ump_types = vec!["umphome", "ump1b", "ump2b", "ump3b", "umplf", "umprf"];
        let ump_names = vec![self.home, self.first, self.second, self.third, self.left, self.right]
            .into_iter()
            .map(opt_string);
        let vecs: Vec<Vec<String>> = ump_names
            .zip(ump_types.iter())
            .map(|(name, pos)|vec!["info".to_string(), pos.to_string(), name])
            .collect();
        vecs.into_iter()
            .map(RetrosheetEventRecord::from).collect()
    }
}

impl TryFrom<&Vec<InfoRecord>> for GameUmpires {
    type Error = Error;

    fn try_from(record_vec: &Vec<InfoRecord>) -> Result<Self> {
        let asses: HashMap<UmpirePosition, Umpire> = record_vec
            .iter()
            .filter_map(|i|
                match i {
                    InfoRecord::UmpireAssignment(ass) if ass.umpire.is_some() => Some((ass.position, ass.umpire.unwrap())),
                    _ => None
                })
            .collect();
        Ok(Self {
            home: asses.get(&UmpirePosition::Home).copied(),
            first: asses.get(&UmpirePosition::First).copied(),
            second: asses.get(&UmpirePosition::Second).copied(),
            third: asses.get(&UmpirePosition::Third).copied(),
            left: asses.get(&UmpirePosition::LeftField).copied(),
            right: asses.get(&UmpirePosition::RightField).copied(),
        })

    }
}

#[derive(Debug, Eq, PartialEq, Clone, Copy)]
pub struct GameSetting {
    doubleheader_status: DoubleheaderStatus,
    start_time: Option<NaiveTime>,
    time_of_day: DayNight,
    use_dh: bool,
    bat_first_side: Side,
    sky: Sky,
    temp: Option<u8>,
    field_condition: FieldCondition,
    precipitation: Precipitation,
    wind_direction: WindDirection,
    wind_speed: Option<u8>,
    attendance: Option<u32>,
    park: Park,
    scheduled_innings: Option<u8>
}

impl Into<Vec<RetrosheetEventRecord>> for GameSetting {
    fn into(self) -> Vec<RetrosheetEventRecord> {
        let mut vecs = vec![
            vec!["number".to_string(), self.doubleheader_status.to_string()],
            vec!["starttime".to_string(), self.start_time.map_or("".to_string(), |t| t.to_string())],
            vec!["daynight".to_string(), self.time_of_day.to_string()],
            vec!["usedh".to_string(), self.use_dh.to_string()],
            vec!["htbf".to_string(), (self.bat_first_side == Side::Home).to_string()],
            vec!["sky".to_string(), self.sky.to_string()],
            vec!["temp".to_string(), self.temp.map_or("".to_string(), |u| u.to_string())],
            vec!["fieldcond".to_string(), self.field_condition.to_string()],
            vec!["precip".to_string(), self.precipitation.to_string()],
            vec!["winddir".to_string(), self.wind_direction.to_string()],
            vec!["windspeed".to_string(), self.wind_speed.map_or("".to_string(), |u| u.to_string())],
            vec!["attendance".to_string(), self.attendance.map_or("".to_string(), |u| u.to_string())],
            vec!["site".to_string(), self.park.to_string()]
        ];
        for vec in &mut vecs {
            vec.insert(0, String::from("info"));
        }
        vecs.into_iter()
            .map(RetrosheetEventRecord::from).collect()
    }
}

impl Default for GameSetting {
    fn default() -> Self {
        Self {
            doubleheader_status: Default::default(),
            start_time: None,
            time_of_day: Default::default(),
            use_dh: false,
            bat_first_side: Side::Away,
            sky: Default::default(),
            temp: None,
            field_condition: Default::default(),
            precipitation: Default::default(),
            wind_direction: Default::default(),
            wind_speed: None,
            attendance: None,
            park: TinyStr8::from_str("NA").unwrap(),
            scheduled_innings: Some(9)
        }
    }
}

impl TryFrom<&Vec<InfoRecord>> for GameSetting {
    type Error = Error;

    fn try_from(infos: &Vec<InfoRecord>) -> Result<Self> {
        let mut setting = Self::default();
        for info in infos {
            match info {
                InfoRecord::DoubleheaderStatus(x) => {setting.doubleheader_status = *x},
                InfoRecord::StartTime(x) => {setting.start_time = *x},
                InfoRecord::DayNight(x) => {setting.time_of_day = *x},
                InfoRecord::UseDH(x) => {setting.use_dh = *x},
                InfoRecord::HomeTeamBatsFirst(x) => {
                    setting.bat_first_side = if *x {Side::Home} else {Side::Away}
                },
                InfoRecord::Sky(x) => {setting.sky = *x},
                InfoRecord::Temp(x) => {setting.temp = *x},
                InfoRecord::FieldCondition(x) => {setting.field_condition = *x}
                InfoRecord::Precipitation(x) => {setting.precipitation = *x}
                InfoRecord::WindDirection(x) => {setting.wind_direction = *x},
                InfoRecord::WindSpeed(x) => {setting.wind_speed = *x},
                InfoRecord::Attendance(x) => {setting.attendance = *x},
                InfoRecord::Park(x) => {setting.park = *x},
                InfoRecord::Innings(x) => {setting.scheduled_innings = *x}
                _ => {}
            }
        }
        Ok(setting)
    }
}

#[derive(Debug, Eq, PartialEq, Clone, Copy)]
pub struct GameInfo {
    matchup: Teams,
    date: NaiveDate,
    setting: GameSetting,
    umpires: GameUmpires,
    results: GameResults,
    retrosheet_metadata: GameRetrosheetMetadata
}

impl Into<Vec<RetrosheetEventRecord>> for GameInfo {
    fn into(self) -> Vec<RetrosheetEventRecord> {
        let top_level_info: Vec<RetrosheetEventRecord> = vec![
            vec!["info".to_string(), "visteam".to_string(), self.matchup.away.to_string()],
            vec!["info".to_string(), "hometeam".to_string(), self.matchup.home.to_string()],
            vec!["info".to_string(), "date".to_string(), self.date.format("%Y/%m/%d").to_string()]
        ].into_iter()
            .map(RetrosheetEventRecord::from).collect();

        [top_level_info,
            self.setting.into(),
            self.umpires.into(),
            self.results.into(),
            self.retrosheet_metadata.into()
        ].concat()

    }
}

impl TryFrom<&Vec<InfoRecord>> for GameInfo {
    type Error = Error;

    fn try_from(infos: &Vec<InfoRecord>) -> Result<Self> {
        let matchup = Matchup::try_from(infos)?;
        let date = *infos.iter()
            .find_map(|i| if let InfoRecord::GameDate(d) = i {Some(d)} else {None})
            .ok_or_else(|| anyhow!("Game info did not include date. Full info list: {:?}", infos))?;
        let setting = GameSetting::try_from(infos)?;
        let umpires = GameUmpires::try_from(infos)?;
        let results = GameResults::try_from(infos)?;
        let retrosheet_metadata = GameRetrosheetMetadata::try_from(infos)?;
        Ok(Self {
            matchup,
            date,
            setting,
            umpires,
            results,
            retrosheet_metadata
        })
    }
}


/// Info fields relating to how the game was scored, obtained, and inputted.
#[derive(Debug, Default, Eq, PartialEq, Clone, Copy)]
pub struct GameRetrosheetMetadata {
    pitch_detail: PitchDetail,
    scoring_method: HowScored,
    inputter: Option<RetrosheetVolunteer>,
    scorer: Option<Scorer>,
    original_scorer: Option<Scorer>,
    translator: Option<RetrosheetVolunteer>
}

impl Into<Vec<RetrosheetEventRecord>> for GameRetrosheetMetadata {
    fn into(self) -> Vec<RetrosheetEventRecord> {
        let opt_string = {
            |o: Option<TinyStr16>| o.map_or("".to_string(), |u| u.to_string())
        };

        let mut vecs = vec![
            vec!["pitches".to_string(), self.pitch_detail.to_string()],
            vec!["howscored".to_string(), self.scoring_method.to_string()],
            vec!["inputter".to_string(), opt_string(self.inputter)],
            vec!["scorer".to_string(), opt_string(self.scorer)],
            vec!["oscorer".to_string(), opt_string(self.original_scorer)],
            vec!["translator".to_string(), opt_string(self.translator)],
        ];
        for vec in &mut vecs {
            vec.insert(0, String::from("info"));
        }
        vecs.into_iter()
            .map(RetrosheetEventRecord::from).collect()
    }
}

impl TryFrom<&Vec<InfoRecord>> for GameRetrosheetMetadata {
    type Error = Error;

    fn try_from(infos: &Vec<InfoRecord>) -> Result<Self> {
        let mut metadata = Self::default();
        for info in infos {
            match info {
                InfoRecord::PitchDetail(x) => {metadata.pitch_detail = *x},
                InfoRecord::HowScored(x) => {metadata.scoring_method = *x},
                InfoRecord::Inputter(x) => {metadata.inputter = *x},
                InfoRecord::Scorer(x) => {metadata.scorer = *x},
                InfoRecord::OriginalScorer(x) => {metadata.original_scorer = Some(*x)},
                InfoRecord::Translator(x) => {metadata.translator = *x}
                _ => {}
            }
        }
        Ok(metadata)
    }
}

/// These fields only refer to data from the info section, and thus do not include
/// any kind of box score data.
#[derive(Debug, Default, Eq, PartialEq, Clone, Copy)]
pub struct GameResults {
    winning_pitcher: Option<Pitcher>,
    losing_pitcher: Option<Pitcher>,
    save: Option<Pitcher>,
    game_winning_rbi: Option<Batter>,
    time_of_game_minutes: Option<u16>,
}

impl Into<Vec<RetrosheetEventRecord>> for GameResults {
    fn into(self) -> Vec<RetrosheetEventRecord> {
        let opt_string = {
            |o: Option<TinyStr8>| o.map_or("".to_string(), |u| u.to_string())
        };
        let mut vecs = vec![
            vec!["wp".to_string(), opt_string(self.winning_pitcher)],
            vec!["lp".to_string(), opt_string(self.losing_pitcher)],
            vec!["save".to_string(), opt_string(self.save)],
            vec!["gwrbi".to_string(), opt_string(self.game_winning_rbi)],
            vec!["timeofgame".to_string(), self.time_of_game_minutes.map_or("".to_string(), |u| u.to_string())],
        ];

        for vec in &mut vecs {
            vec.insert(0, String::from("info"));
        }
        vecs.into_iter()
            .map(RetrosheetEventRecord::from)
            .collect()
    }
}

impl TryFrom<&Vec<InfoRecord>> for GameResults {
    type Error = Error;

    fn try_from(infos: &Vec<InfoRecord>) -> Result<Self> {
        let mut results = Self::default();
        for info in infos {
            match info {
                InfoRecord::WinningPitcher(x) => {results.winning_pitcher = *x},
                InfoRecord::LosingPitcher(x) => {results.losing_pitcher = *x},
                InfoRecord::SavePitcher(x) => {results.save = *x},
                InfoRecord::GameWinningRBI(x) => {results.game_winning_rbi = *x},
                InfoRecord::TimeOfGameMinutes(x) => {results.time_of_game_minutes = *x},
                _ => {}
            }
        }
        Ok(results)
    }
}

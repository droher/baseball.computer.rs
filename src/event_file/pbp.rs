use either::Either;
use crate::event_file::play::{OtherPlateAppearance, PlayRecord, Count, InningFrame, Play, BaseRunner, Base, RunnerAdvance, FieldingData, PlateAppearanceType, HitType, BaserunningPlayType};
use crate::event_file::misc::{SubstitutionRecord, Lineup, Defense};
use crate::event_file::traits::{Inning, LineupPosition, Pitcher, Side, FieldingPosition, Fielder, Batter, RetrosheetEventRecord};
use crate::event_file::parser::{Matchup, Game, EventRecord};

use anyhow::{anyhow, Result, Context, Error};
use crate::event_file::box_score::*;
use std::convert::TryFrom;
use arrayvec::ArrayVec;
use crate::util::{count_occurrences, opt_add, u8_vec_to_string};

pub type Outs = u8;

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
    pub events: Vec<BoxScoreEvent>,
    pub line_score: Matchup<Vec<u8>>
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
            .filter(|dl| dl.fielding_position.plays_in_field() && dl.fielder_id == fielder_id)
            .collect::<Vec<&DefenseLine>>();
        player_lines.sort_by_key(|pl| pl.nth_position_played_by_player);
        player_lines.pop()
    }

    fn add_earned_runs(&mut self, game: &Game) {
        let (away, home) = self.pitching_lines.get_both_mut();
        for line in away.iter_mut().chain(home) {
            if let Some((_pitcher_id, earned_runs)) = game.earned_run_data.get_key_value(&line.pitcher_id) {
                opt_add(&mut line.pitching_stats.earned_runs, *earned_runs)
            }
        }
    }
}

// TODO: PH, PR, Team, and Event Records
impl Into<Vec<RetrosheetEventRecord>> for BoxScore {

    fn into(self) -> Vec<RetrosheetEventRecord> {

        let (line_away, line_home) = self.line_score.apply_both(u8_vec_to_string);
        let lines: Vec<RetrosheetEventRecord> = vec![line_away, line_home].into_iter()
            .zip(vec!["0", "1"])
            .map(|(line, side)| [vec!["line".to_string(), side.to_string()], line].concat())
            .map(RetrosheetEventRecord::from)
            .collect();

        let (bat_away, bat_home) = self.batting_lines
            .apply_both(|v| v.into_iter()
                .map(|b| b.into())
                .collect::<Vec<RetrosheetEventRecord>>());
        let (d_away, d_home) = self.defense_lines
            .apply_both(|v| v.into_iter()
                .map(|b| b.into())
                .collect::<Vec<RetrosheetEventRecord>>());
        let (pitch_away, pitch_home) = self.pitching_lines
            .apply_both(|v| v.into_iter()
                .map(|b| b.into())
                .collect::<Vec<RetrosheetEventRecord>>());
        [lines, bat_away, bat_home, d_away, d_home, pitch_away, pitch_home].concat()
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
        let team_miscellaneous_lines = sides.iter()
                .map(|s| TeamMiscellaneousLine::new(*s))
                .collect::<ArrayVec<[TeamMiscellaneousLine;2]>>();
        let team_batting_lines = sides.iter()
            .map(|s| TeamBattingLine::new(*s))
            .collect::<ArrayVec<[TeamBattingLine;2]>>();
        let team_defense_lines = sides.iter()
            .map(|s| TeamDefenseLine::new(*s))
            .collect::<ArrayVec<[TeamDefenseLine;2]>>();
        Ok(Self {
            batting_lines,
            pinch_hitting_lines: Default::default(),
            pinch_running_lines: Default::default(),
            pitching_lines,
            defense_lines,
            team_miscellaneous_lines: Matchup::new(team_miscellaneous_lines[0], team_miscellaneous_lines[1]),
            team_batting_lines: Matchup::new(team_batting_lines[0], team_batting_lines[1]),
            team_defense_lines: Matchup::new(team_defense_lines[0], team_defense_lines[1]),
            events: vec![],
            line_score: Default::default()
        })
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
    fn get_baserunner(&self, baserunner: BaseRunner) -> Option<Runner> {
        match baserunner {
            BaseRunner::First => self.first,
            BaseRunner::Second => self.second,
            BaseRunner::Third => self.third,
            _ => None
        }
    }

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

    fn new_base_state(&self, start_inning: bool, end_inning: bool, play: &Play, batter_lineup_position: LineupPosition, pitcher: Pitcher) -> Result<Self> {
        let mut new_state = if start_inning {Self::default()} else {self.clone()};
        new_state.scored = vec![];

        // Cover cases where outs are not included in advance information
        for out in play.outs()? {
            match out {
                BaseRunner::Third => new_state.third = None,
                BaseRunner::Second => new_state.second = None,
                BaseRunner::First => new_state.first = None,
                _ => ()
            }
        }

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
                _ if a.is_out() || end_inning => {},
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
    fn starting_state(game: &Game) -> Result<Self> {
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
            count: Count::default(),
            box_score: BoxScore::try_from(game)?
        })
    }

    fn pitcher(&self, side: &Side) -> Result<Pitcher> {
        Ok(*self.defenses
            .get(side)
            .get_by_left(&FieldingPosition::Pitcher)
            .context("Missing pitcher")?
        )
    }

    fn fielding_side(&self) -> Side {
        self.batting_side.flip()
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
        let is_new_player = original_batter != Some(&sub.player);
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

    fn update_batter_stats(batting_stats: &mut BattingLineStats, play: &Play) {
        if play.plate_appearance().is_none() {
            return
        }
        let plate_appearance = play.plate_appearance().unwrap();

        opt_add(&mut batting_stats.rbi, play.rbi().len() as u8);
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
    fn update_pitching_stats(pitching_stats: &mut PitchingLineStats, play: &Play) {
        pitching_stats.outs_recorded += play.putouts().len() as u8;
        if play.wild_pitch() {opt_add(&mut pitching_stats.wild_pitches, 1)}
        if play.balk() {opt_add(&mut pitching_stats.balks, 1)}
        if play.sacrifice_hit() {opt_add(&mut pitching_stats.sacrifice_hits, 1)}
        if play.sacrifice_fly() {opt_add(&mut pitching_stats.sacrifice_flies, 1)}

        if let Some(pa) = play.plate_appearance() {
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
                              fielding_position: FieldingPosition, play: &Play) -> Result<()> {
        let ds = defense_stats.context("No defense stat object")?;

        let assists = count_occurrences(play.assists(), &fielding_position);
        let putouts = count_occurrences(play.putouts(), &fielding_position);
        let errors = count_occurrences(play.errors(), &fielding_position);

        opt_add(&mut ds.outs_played, play.putouts().len() as u8);
        opt_add(&mut ds.assists, assists);
        opt_add(&mut ds.putouts, putouts);
        opt_add(&mut ds.errors, errors);

        if play.putouts().len() == 2 && assists + putouts > 0 {
            opt_add(&mut ds.double_plays, 1)
        } else if play.putouts().len() == 3 && assists + putouts > 0 {
            opt_add(&mut ds.triple_plays, 1);
        }

        if fielding_position == FieldingPosition::Catcher && play.passed_ball() {
            opt_add(&mut ds.passed_balls, 1);
        }
        Ok(())
    }

    /// TODO:
    ///  -- Pitching: basic stats, inherited runner logic should be settled by this point
    ///  -- Add any new events (HR, HBP, DP, etc.)
    fn update_box_score(&self, play_record: &PlayRecord, new_base_state: &BaseState) -> Result<BoxScore> {
        let (batting_side, fielding_side) = (&play_record.side, &play_record.side.flip());
        let mut new_box = self.box_score.clone();
        let play = &play_record.play;
        let lineup = self.lineups.get(batting_side);
        let defense = self.defenses.get(fielding_side);

        // First add stats relating to the PA (if any) to the batter
        let batter_line = new_box.get_batter_by_id(*batting_side, play_record.batter)?;
        Self::update_batter_stats(&mut batter_line.batting_stats, play);

        // Then add R/SB/CS to the batting lines of the baserunners
        for runner in &new_base_state.scored {
            let runner_line = new_box.get_line_from_runner(*batting_side, lineup, runner)?;
            runner_line.batting_stats.runs += 1;
        }
        for sb_play in play.stolen_base_plays() {
            let target_base = sb_play
                .at_base
                .context("Missing base info on stolen base attempt record")?;
            let baserunner = BaseRunner::from_target_base(&target_base)?;
            let runner = self.bases
                .get_baserunner(baserunner)
                .context("Stolen base play recorded, but runner is missing")?;
            let runner_line = new_box.get_line_from_runner(*batting_side, lineup, &runner)?;
            if sb_play.baserunning_play_type == BaserunningPlayType::StolenBase {
                opt_add(&mut runner_line.batting_stats.stolen_bases, 1);
            }
            else {
                opt_add(&mut runner_line.batting_stats.caught_stealing, 1);
            }
        }

        // Pitching numbers are a bit easier to manage because for the most part there's only one pitcher
        // to keep track of, with the exception of inherited runners
        let pitching_line = new_box.get_pitcher_by_id(*fielding_side, self.pitcher(fielding_side)?)?;
        Self::update_pitching_stats(&mut pitching_line.pitching_stats, play);
        for runner in &new_base_state.scored {
            let pitching_line = new_box.get_pitcher_by_id(*fielding_side, runner.charged_to)?;
            pitching_line.pitching_stats.runs += 1;
        }

        // Add up numbers for the defense -- every fielder needs to be adjusted because
        // we keep track of innings played
        let d_lines = new_box.defense_lines.get_mut(&self.fielding_side());
        for line in d_lines {
            if defense.get_by_left(&line.fielding_position) == Some(&line.fielder_id) {
                Self::update_defensive_stats(Option::from(&mut line.defensive_stats), line.fielding_position, play)?;
            }
        };
        Ok(new_box)
    }

    fn update_line_score(&self, play_record: &PlayRecord) -> Result<Matchup<Vec<u8>>> {
        let mut line_score = self.line_score.get(&play_record.side).clone();
        let diff = play_record.inning - line_score.len() as u8;
        // Add a new frame if needed
        if diff == 1 { line_score.push(0) }
        else if diff != 0 {return Err(anyhow!("Line score out of sync with inning"))}

        let current_frame = line_score.pop().context("Empty line score")?;
        line_score.push(current_frame + play_record.play.runs().len() as u8);
        Ok(self.line_score.cloned_update(&play_record.side, line_score))
    }

    fn update_on_play(&self, play_record: &PlayRecord) -> Result<Self> {
        if play_record.play.no_play() {
            return Ok(self.clone())
        }

        let flipped = self.batting_side != play_record.side;
        let frame = if flipped {self.frame.flip()} else {self.frame};
        let play_outs = play_record.play.outs()?.len() as u8;

        let outs = if flipped {play_outs} else {self.outs + play_outs};

        if outs > 3 {return Err(anyhow!("Illegal state, more than 3 outs recorded"))}

        let at_bat = self.lineups
            .get(&play_record.side)
            .get_by_right(&play_record.batter)
            .context("Could not find batter in current side's lineup")?;

        let new_base_state = self.bases.new_base_state(flipped,
                                                       outs == 3,
                                                       &play_record.play,
                                                       *at_bat,
                                                       self.pitcher(&play_record.side.flip())?)?;

        let new_box_score = self.update_box_score(play_record, &new_base_state)?;
        let new_line_score = self.update_line_score(play_record)?;

        Ok(Self {
            inning: play_record.inning,
            frame,
            batting_side: play_record.side,
            outs,
            bases: new_base_state,
            line_score: new_line_score,
            lineups: self.lineups.clone(),
            defenses: self.defenses.clone(),
            at_bat: *at_bat,
            count: play_record.count,
            box_score: new_box_score
        })
    }

    fn next_state(&self, event: &EventRecord) -> Result<Self> {
        match event {
            Either::Left(pr) => self.update_on_play(pr),
            Either::Right(sr) => Ok(self.update_on_substitution(sr))
        }
    }

    pub fn get_box_score(game: &Game) -> Result<BoxScore> {
        let mut state = Self::starting_state(game)?;
        for event in &game.events {
            state = state.next_state(event)?;
        }
        state.box_score.add_earned_runs(game);
        state.box_score.line_score = state.line_score.clone();
        Ok(state.box_score)
    }
}
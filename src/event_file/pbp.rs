use either::Either;
use crate::event_file::play::{PlayRecord, Count, InningFrame, Play, BaseRunner, Base};
use crate::event_file::misc::SubstitutionRecord;
use crate::event_file::traits::{Inning, LineupPosition, Pitcher, Side};
use crate::event_file::parser::{Matchup, Defense, Lineup, GameInfo, Game, EventRecord};
use either::Either::Right;

use anyhow::{anyhow, Result, Context};
use std::collections::HashMap;

pub type Outs = u8;



#[derive(Debug, Eq, PartialEq, Copy, Clone)]
pub struct Runner {lineup_position: LineupPosition, charged_to: Pitcher}

#[derive(Debug, Eq, PartialEq, Default, Copy, Clone)]
pub struct BaseState {
    first: Option<Runner>,
    second: Option<Runner>,
    third: Option<Runner>
}

#[derive(Debug, Eq, PartialEq, Clone)]
pub struct GameState {
    inning: Inning,
    frame: InningFrame,
    batting_side: Side,
    outs: Outs,
    bases: BaseState,
    score: Matchup<u8>,
    lineups: Matchup<Lineup>,
    defenses: Matchup<Defense>,
    at_bat: LineupPosition,
    count: Count
}
impl GameState {
    fn starting_state(lineups: Matchup<Lineup>, defenses: Matchup<Defense>, bat_first_side: Side) -> Self {
        Self {
            inning: 1,
            frame: InningFrame::Top,
            batting_side: bat_first_side,
            outs: 0,
            bases: BaseState::default(),
            score: Matchup::default(),
            lineups,
            defenses,
            at_bat: LineupPosition::First,
            count: Count::default()
        }
    }

    fn update_on_substitution(&self, sub: &SubstitutionRecord) -> Self {
        let mut new_lineup = self.lineups.get(&sub.side).clone();
        let mut new_defense = self.defenses.get(&sub.side).clone();
        new_lineup.insert(sub.lineup_position, sub.player);
        new_defense.insert(sub.fielding_position, sub.player);
        Self {
            lineups: self.lineups.cloned_update(&sub.side, new_lineup),
            defenses: self.defenses.cloned_update(&sub.side, new_defense),
            ..self.clone()
        }
    }

    fn update_base_state(&self, play: &Play) -> BaseState { unimplemented!()}

    fn update_score(current_score: &Matchup<u8>, play: &Play) -> Matchup<u8> {
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
            bases: Self::update_base_state(&self, &play_record.play),
            score: Self::update_score(&self.score, &play_record.play),
            lineups: self.lineups.clone(),
            defenses: self.defenses.clone(),
            at_bat: *at_bat,
            count: play_record.count
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
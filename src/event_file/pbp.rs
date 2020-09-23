use either::Either;
use crate::event_file::play::{PlayRecord, Count, InningFrame};
use crate::event_file::misc::SubstitutionRecord;
use crate::event_file::traits::{Inning, LineupPosition, Pitcher};
use crate::event_file::parser::{Matchup, Defense, Lineup, GameInfo};

pub type EventRecord = Either<PlayRecord, SubstitutionRecord>;

pub type Outs = u8;



#[derive(Debug, Eq, PartialEq, Copy, Clone)]
pub struct Runner {lineup_position: LineupPosition, charged_to: Pitcher}

#[derive(Debug, Eq, PartialEq, Default, Copy, Clone)]
pub struct BaseState {
    first: Option<Runner>,
    second: Option<Runner>,
    third: Option<Runner>
}

#[derive(Debug, Eq, PartialEq, Default, Clone)]
pub struct GameState {
    inning: Inning,
    frame: InningFrame,
    outs: Outs,
    bases: BaseState,
    score: Matchup<u8>,
    lineups: Matchup<Lineup>,
    defenses: Matchup<Defense>,
    at_bat: LineupPosition,
    count: Count
}
impl GameState {
    fn new(lineups: Matchup<Lineup>, defenses: Matchup<Defense>) -> Self {
        Self {
            inning: 1,
            frame: InningFrame::Top,
            outs: 0,
            bases: BaseState::default(),
            score: Matchup::default(),
            lineups: lineups,
            defenses: defenses,
            at_bat: LineupPosition::First,
            count: Count::default()
        }
    }
}

#[derive(Debug, Eq, PartialEq, Clone)]
pub struct Event {
    starting_state: GameState,
    current_record: EventRecord,
    next_record: Option<EventRecord>
}

#[derive(Debug, Eq, PartialEq, Default, Clone)]
pub struct PlayByPlay(Vec<Event>);

impl Iterator for PlayByPlay {
    type Item = Event;

    fn next(&mut self) -> Option<Self::Item> {
        unimplemented!()
    }
}
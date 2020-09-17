use either::Either;
use crate::event_file::play::{PlayRecord, Count, InningFrame};
use crate::event_file::misc::SubstitutionRecord;
use crate::event_file::traits::{Inning, Side, LineupPosition};
use crate::event_file::parser::{Matchup, Defense, Lineup, GameSetting, GameInfo};

pub type EventRecord = Either<PlayRecord, SubstitutionRecord>;
pub type Outs = u8;

#[derive(Default)]
pub struct BaseState {
    first: Option<LineupPosition>,
    second: Option<LineupPosition>,
    third: Option<LineupPosition>
}

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
    fn new(game_info: GameInfo) -> Self {
        Self {
            inning: 1,
            frame: InningFrame::Top,
            outs: 0,
            bases: BaseState::default(),
            score: Matchup { away: 0, home: 0 },
            lineups: game_info.starting_lineups,
            defenses: game_info.starting_defense,
            at_bat: 1,
            count: Count::default()
        }
    }
}

pub struct Event {
    starting_state: GameState,
    current_record: EventRecord,
    next_record: Option<EventRecord>
}

impl Event {
    pub fn ending_state(&self) -> () {

    }
}
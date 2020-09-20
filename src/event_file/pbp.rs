use either::Either;
use crate::event_file::play::{PlayRecord, Count, InningFrame, Base};
use crate::event_file::misc::SubstitutionRecord;
use crate::event_file::traits::{Inning, Side, LineupPosition, Pitcher};
use crate::event_file::parser::{Matchup, Defense, Lineup, GameSetting, GameInfo};

pub type EventRecord = Either<PlayRecord, SubstitutionRecord>;
pub type Outs = u8;

pub struct Runner {lineup_position: LineupPosition, charged_to: Pitcher}

#[derive(Default)]
pub struct BaseState {
    first: Option<Runner>,
    second: Option<Runner>,
    third: Option<Runner>
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
            score: Matchup::default(),
            lineups: Matchup::default(),
            defenses: Matchup::default(),
            at_bat: LineupPosition::First,
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
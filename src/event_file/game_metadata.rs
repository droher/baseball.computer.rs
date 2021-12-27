use crate::event_file::misc::GameId;
use crate::event_file::parser::AccountType;
use crate::event_file::traits::GameType;

pub enum Completeness {
    Full,
    Partial,
    TeamOnly,
    Missing,
    Indeterminate
}

/// Metadata about the completeness of an account for a given game.
pub struct GameMetadata {
    game_id: GameId,
    game_type: GameType,
    account_type: AccountType,
    pitch: Completeness,
    count: Completeness,
    contact_type: Completeness,
    hit_location: Completeness,
    fielding: Completeness,
    sacrifice_fly: Completeness,
    sacrifice_hit: Completeness,
    stolen_base: Completeness,
    caught_stealing: Completeness
}
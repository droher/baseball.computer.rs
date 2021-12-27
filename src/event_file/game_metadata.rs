use crate::event_file::misc::GameId;
use crate::event_file::parser::AccountType;
use crate::event_file::traits::{GameType, Side};

/// Full: All data is present.
/// Partial: At least one data point is present *and* at least one data point is missing.
/// TeamOnly: Team data is complete, but at least one individual data point is missing.
/// Missing: No data is present.
/// Indeterminate: Unclear whether the data is missing or, for example, truly all zeros.
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
    file_name: String,
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
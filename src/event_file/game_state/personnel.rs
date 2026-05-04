//! Personnel as a value type plus the pure substitution transforms.
//!
//! The lineup and defense maps live in `Personnel` and are cloned on each
//! transition. The `HashMap` accumulators of per-player appearance records
//! (which the parent module needs to finalize at end-of-game) live in the
//! shell `GameState`; `apply_*` returns an `AppearanceDelta` describing
//! the changes the shell should fold into those maps.

use std::collections::HashMap;

use anyhow::{Context, Result, anyhow, bail};
use fixed_map::{Key, Map};
use strum_macros::Display;

use crate::event_file::misc::{GameId, SubstitutionRecord};
use crate::event_file::parser::{MappedRecord, RecordSlice};
use crate::event_file::play::PlayRecord;
use crate::event_file::traits::{FieldingPosition, LineupPosition, Matchup, Pitcher, Player, Side};

use super::validation::get_game_id;
use super::{EnteredGameAs, EventId, GameFieldingAppearance, GameLineupAppearance};

#[derive(Debug, Eq, PartialEq, Copy, Clone, Hash, Display, Key)]
pub(super) enum PositionType {
    Lineup(LineupPosition),
    Fielding(FieldingPosition),
}

/// A wrapper around `Player` that allows for a player to appear
/// in multiple positions in a lineup. This is used for the
/// Ohtani rule, where a player can appear in the lineup as a
/// pitcher and a DH.
#[derive(Debug, Eq, PartialEq, Copy, Clone, Hash)]
pub(super) struct TrackedPlayer {
    pub player: Player,
    pub side: Side,
    is_pitcher_with_dh: bool,
}

impl From<(Player, Side, bool)> for TrackedPlayer {
    fn from((player, side, is_starting_pitcher_with_dh): (Player, Side, bool)) -> Self {
        Self {
            player,
            side,
            is_pitcher_with_dh: is_starting_pitcher_with_dh,
        }
    }
}

impl std::fmt::Display for TrackedPlayer {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let dh = if self.is_pitcher_with_dh {
            "-pitcher-with-dh"
        } else {
            ""
        };
        write!(f, "{}{}", self.player, dh)
    }
}

pub(super) type PersonnelState = Map<PositionType, TrackedPlayer>;
pub(super) type Lineup = PersonnelState;
pub(super) type Defense = PersonnelState;

/// Lineup and defense state for both sides of a game. Pure value type;
/// each transition returns a fresh `Personnel`.
#[derive(Debug, Eq, PartialEq, Clone)]
pub(super) struct Personnel {
    pub(super) game_id: GameId,
    pub(super) state: Matchup<(Lineup, Defense)>,
}

impl Personnel {
    pub(super) fn pitcher(&self, side: Side) -> Result<Pitcher> {
        self.get_at_position(side, PositionType::Fielding(FieldingPosition::Pitcher))
            .map(|tp| tp.player)
    }

    pub(super) fn get_at_position(
        &self,
        side: Side,
        position: PositionType,
    ) -> Result<TrackedPlayer> {
        let map_tup = self.state.get(side);
        let map = if let PositionType::Lineup(_) = position {
            &map_tup.0
        } else {
            &map_tup.1
        };
        map.get(position).copied().with_context(|| {
            anyhow!("Position {position} for side {side} missing from current game state")
        })
    }

    fn get_player_lineup_position(
        &self,
        side: Side,
        player: &TrackedPlayer,
    ) -> Option<PositionType> {
        let (lineup, _) = self.state.get(side);
        lineup.iter().find_map(|(position, tracked_player)| {
            if tracked_player == player {
                Some(position)
            } else {
                None
            }
        })
    }

    pub(super) fn at_bat(&self, play: &PlayRecord) -> Result<LineupPosition> {
        let player: TrackedPlayer = (play.batter, play.batting_side, false).into();
        let position = self.get_player_lineup_position(play.batting_side, &player);
        if let Some(PositionType::Lineup(lp)) = position {
            Ok(lp)
        } else {
            bail!(
                "Fatal error parsing {}: Cannot find lineup position of player currently at bat {}.",
                self.game_id.id,
                &play.batter,
            )
        }
    }
}

/// Changes the shell's per-player appearance `HashMap`s should absorb after
/// the matching `apply_*` produces a new `Personnel` value.
///
/// `close_*` entries set the named player's most recent appearance's
/// `end_event_id` to the given value unconditionally. Conditional
/// "close-only-if-open" semantics are resolved inside the `apply_*` step
/// using read-only `HashMap` access — by the time a delta is emitted, the
/// condition has already been checked.
#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub(super) struct AppearanceDelta {
    pub new_lineup: Vec<(TrackedPlayer, GameLineupAppearance)>,
    pub new_fielding: Vec<(TrackedPlayer, GameFieldingAppearance)>,
    pub close_lineup: Vec<(TrackedPlayer, EventId)>,
    pub close_fielding: Vec<(TrackedPlayer, EventId)>,
}

/// Build the initial `Personnel` from `start,...` records and emit the
/// matching first-batch lineup/fielding appearances as a delta the shell
/// can absorb.
pub(super) fn from_starts(records: &RecordSlice) -> Result<(Personnel, AppearanceDelta)> {
    let game_id = get_game_id(records)?;
    let mut personnel = Personnel {
        game_id,
        state: Matchup::new(
            (Lineup::new(), Defense::new()),
            (Lineup::new(), Defense::new()),
        ),
    };
    let mut delta = AppearanceDelta::default();

    let start_iter = records.iter().filter_map(|rv| {
        if let MappedRecord::Start(sr) = rv {
            Some(sr)
        } else {
            None
        }
    });
    for start in start_iter {
        let (lineup, defense) = personnel.state.get_mut(start.side);
        let lineup_appearance = GameLineupAppearance::new_starter(
            start.player,
            start.lineup_position,
            start.side,
            game_id,
        )?;
        let fielding_appearance = GameFieldingAppearance::new_starter(
            start.player,
            start.fielding_position,
            start.side,
            game_id,
        )?;
        let player: TrackedPlayer = (
            start.player,
            start.side,
            start.lineup_position == LineupPosition::PitcherWithDh,
        )
            .into();

        lineup.insert(PositionType::Lineup(start.lineup_position), player);
        defense.insert(PositionType::Fielding(start.fielding_position), player);
        delta.new_lineup.push((player, lineup_appearance));
        delta.new_fielding.push((player, fielding_appearance));
    }
    Ok((personnel, delta))
}

/// Substitution transition: lineup + (when applicable) defense.
///
/// Reads the appearance `HashMap`s to decide whether the courtesy-runner
/// case applies (only close the new player's existing lineup appearance
/// if it's currently open) and to produce the unconditional defense close
/// when the new fielder was already in the game.
pub(super) fn apply_substitution(
    prev: &Personnel,
    lineup_appearances: &HashMap<TrackedPlayer, Vec<GameLineupAppearance>>,
    defense_appearances: &HashMap<TrackedPlayer, Vec<GameFieldingAppearance>>,
    sub: &SubstitutionRecord,
    event_id: EventId,
) -> Result<(Personnel, AppearanceDelta)> {
    let mut next = prev.clone();
    let mut delta = AppearanceDelta::default();

    apply_lineup_substitution(&mut next, lineup_appearances, sub, event_id, &mut delta)?;
    if sub.fielding_position.is_true_position() {
        apply_defense_substitution(&mut next, defense_appearances, sub, event_id, &mut delta)?;
    }

    Ok((next, delta))
}

/// DH-vacancy transition: closes the non-batting pitcher's lineup
/// appearance and the DH's fielding appearance when the pitcher comes in
/// at a non-pitching lineup slot or the DH moves into the field.
pub(super) fn apply_dh_vacancy(
    prev: &Personnel,
    sub: &SubstitutionRecord,
    event_id: EventId,
) -> AppearanceDelta {
    let mut delta = AppearanceDelta::default();

    let non_batting_pitcher = prev
        .get_at_position(
            sub.side,
            PositionType::Lineup(LineupPosition::PitcherWithDh),
        )
        .ok();
    let dh = prev
        .get_at_position(
            sub.side,
            PositionType::Fielding(FieldingPosition::DesignatedHitter),
        )
        .ok()
        .and_then(|tp| {
            // If the DH vacancy is being created by having the DH come in
            // to pitch, we don't need to end their fielding appearance.
            if sub.fielding_position == FieldingPosition::Pitcher {
                None
            } else {
                Some(tp)
            }
        });
    if let Some(p) = non_batting_pitcher {
        delta.close_lineup.push((p, event_id - 1));
    }
    if let Some(p) = dh {
        delta.close_fielding.push((p, event_id - 1));
    }
    delta
}

fn apply_lineup_substitution(
    next: &mut Personnel,
    lineup_appearances: &HashMap<TrackedPlayer, Vec<GameLineupAppearance>>,
    sub: &SubstitutionRecord,
    event_id: EventId,
    delta: &mut AppearanceDelta,
) -> Result<()> {
    let original_batter = next.get_at_position(sub.side, PositionType::Lineup(sub.lineup_position));

    if let Ok(p) = original_batter {
        let current_appearance = current_lineup_appearance(lineup_appearances, &p)?;

        if p.player == sub.player && current_appearance.lineup_position == sub.lineup_position {
            return Ok(());
        }

        if current_appearance.lineup_position == sub.lineup_position {
            delta.close_lineup.push((p, event_id - 1));
        }
    }

    let new_player: TrackedPlayer = (
        sub.player,
        sub.side,
        sub.lineup_position == LineupPosition::PitcherWithDh,
    )
        .into();
    // In the case of a courtesy runner, the new player may already be in
    // the lineup. Only close their current appearance if it's still open.
    if lineup_appearances
        .get(&new_player)
        .and_then(|v| v.last())
        .is_some_and(|a| a.end_event_id.is_none())
    {
        delta.close_lineup.push((new_player, event_id - 1));
    }

    let new_lineup_appearance = GameLineupAppearance {
        game_id: next.game_id.id,
        player_id: sub.player,
        lineup_position: sub.lineup_position,
        side: sub.side,
        entered_game_as: EnteredGameAs::substitution_type(sub),
        start_event_id: event_id,
        end_event_id: None,
    };
    let (lineup, _) = next.state.get_mut(sub.side);
    lineup.insert(PositionType::Lineup(sub.lineup_position), new_player);
    delta.new_lineup.push((new_player, new_lineup_appearance));
    Ok(())
}

/// The semantics of defensive substitutions are more complicated, because
/// the new player could already have been in the game, and the replaced
/// player might not have left the game.
fn apply_defense_substitution(
    next: &mut Personnel,
    defense_appearances: &HashMap<TrackedPlayer, Vec<GameFieldingAppearance>>,
    sub: &SubstitutionRecord,
    event_id: EventId,
    delta: &mut AppearanceDelta,
) -> Result<()> {
    let original_fielder =
        next.get_at_position(sub.side, PositionType::Fielding(sub.fielding_position));
    if let Ok(p) = original_fielder {
        if p.player == sub.player {
            return Ok(());
        }
        let current_appearance = current_fielding_appearance(defense_appearances, &p)?;
        if current_appearance.fielding_position == sub.fielding_position {
            delta.close_fielding.push((p, event_id - 1));
        }
    }
    let new_fielder: TrackedPlayer = (
        sub.player,
        sub.side,
        sub.lineup_position == LineupPosition::PitcherWithDh,
    )
        .into();
    // If the new fielder is already in the game, close out their previous
    // fielding appearance unconditionally.
    if defense_appearances
        .get(&new_fielder)
        .and_then(|v| v.last())
        .is_some()
    {
        delta.close_fielding.push((new_fielder, event_id - 1));
    }

    let (_, defense) = next.state.get_mut(sub.side);
    defense.insert(PositionType::Fielding(sub.fielding_position), new_fielder);
    delta.new_fielding.push((
        new_fielder,
        GameFieldingAppearance::new(
            sub.player,
            sub.fielding_position,
            sub.side,
            next.game_id,
            event_id,
        ),
    ));

    Ok(())
}

fn current_lineup_appearance<'a>(
    map: &'a HashMap<TrackedPlayer, Vec<GameLineupAppearance>>,
    player: &TrackedPlayer,
) -> Result<&'a GameLineupAppearance> {
    map.get(player)
        .with_context(|| {
            anyhow!("Cannot find existing player {player} in lineup appearance records")
        })?
        .last()
        .with_context(|| anyhow!("Player {player} has an empty list of lineup appearances"))
}

fn current_fielding_appearance<'a>(
    map: &'a HashMap<TrackedPlayer, Vec<GameFieldingAppearance>>,
    player: &TrackedPlayer,
) -> Result<&'a GameFieldingAppearance> {
    map.get(player)
        .with_context(|| {
            anyhow!("Cannot find existing player {player} in fielding appearance records")
        })?
        .last()
        .with_context(|| anyhow!("Player {player} has an empty list of fielding appearances"))
}

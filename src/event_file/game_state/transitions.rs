//! Pure transitions over `GameState` fields.
//!
//! Free functions that derive next-frame/next-outs/mid-PA-detection state
//! from the previous shell state plus a record. The shell in
//! `event_file/game_state.rs` calls these and folds the results back into
//! its mutable fields.

use std::collections::HashMap;

use anyhow::{Context, Result, anyhow};

use crate::event_file::misc::{RunnerAdjustment, SubstitutionRecord};
use crate::event_file::play::{Count, InningFrame};
use crate::event_file::traits::{FieldingPosition, LineupPosition, Side};

use super::base_state::BaseState;
use super::personnel::TrackedPlayer;
use super::{EventId, GameLineupAppearance, Outs};

/// Returns true iff the new play belongs to a different half-inning than
/// the previous play. Pure side comparison — no validation.
pub(super) fn frame_changed(state_side: Side, play_side: Side) -> bool {
    state_side != play_side
}

pub(super) const fn next_frame(prev: InningFrame, flipped: bool) -> InningFrame {
    if flipped { prev.flip() } else { prev }
}

pub(super) fn outs_after_play(prev_outs: Outs, flipped: bool, play_outs: usize) -> Result<Outs> {
    let new_outs = if flipped {
        play_outs
    } else {
        prev_outs.get() + play_outs
    };
    Outs::new(new_outs).context("Illegal state, more than 3 outs recorded")
}

/// True if a substitution at the current at-bat slot lands on a count
/// where the *outgoing* batter still owns any resulting strikeout — so
/// the shell needs to record the responsible batter before swapping.
pub(super) fn detect_mid_pa_strikeout_responsible(
    state_at_bat: LineupPosition,
    state_side: Side,
    state_count: Count,
    sub: &SubstitutionRecord,
) -> bool {
    sub.lineup_position == state_at_bat
        && sub.side == state_side
        && state_count.is_old_batter_responsible_strikeout()
}

/// True if a pitcher swap on the defensive side lands on a count where
/// the *outgoing* pitcher still owns any resulting walk.
pub(super) fn detect_mid_pa_walk_responsible(
    state_side: Side,
    state_count: Count,
    sub: &SubstitutionRecord,
) -> bool {
    sub.fielding_position == FieldingPosition::Pitcher
        && sub.side != state_side
        && state_count.is_old_pitcher_responsible_walk()
}

/// Per-field changes the shell folds in for an extra-innings runner
/// adjustment record. When `prev_outs == 3`, the record straddles a
/// half-inning boundary, so frame/side flip and outs reset to zero
/// alongside the new tiebreaker base state.
#[allow(clippy::struct_field_names)]
pub(super) struct RunnerAdjustmentDelta {
    pub new_frame: InningFrame,
    pub new_side: Side,
    pub new_outs: Outs,
    pub new_bases: BaseState,
}

/// Pure transform for an extra-innings runner adjustment. Reads the
/// shell's lineup-appearance accumulator to recover the placed runner's
/// lineup position. Read-only access; no mutation.
pub(super) fn apply_runner_adjustment(
    prev_frame: InningFrame,
    prev_side: Side,
    prev_outs: Outs,
    lineup_appearances: &HashMap<TrackedPlayer, Vec<GameLineupAppearance>>,
    record: &RunnerAdjustment,
    event_id: EventId,
) -> Result<RunnerAdjustmentDelta> {
    // The extra innings runner record can appear before or after the first
    // record of the next inning, and it doesn't carry a side, so we only
    // flip when the previous frame finished with 3 outs.
    let (new_frame, new_side, new_outs) = if prev_outs == 3 {
        (
            prev_frame.flip(),
            prev_side.flip(),
            Outs::new(0).context("Unexpected outs bound error")?,
        )
    } else {
        (prev_frame, prev_side, prev_outs)
    };

    let tracked_runner: TrackedPlayer = (record.runner_id, new_side, false).into();
    let runner_pos = lineup_appearances
        .get(&tracked_runner)
        .and_then(|v| v.last())
        .with_context(|| {
            anyhow!("Cannot find existing player {tracked_runner} in lineup appearance records")
        })?
        .lineup_position;
    let new_bases = BaseState::new_inning_tiebreaker(runner_pos, event_id);

    Ok(RunnerAdjustmentDelta {
        new_frame,
        new_side,
        new_outs,
        new_bases,
    })
}

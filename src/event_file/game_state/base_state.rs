//! `BaseState`, `Runner`, and the pure base-advance transform.

use anyhow::{Context, Result, anyhow, bail};
use arrayvec::ArrayVec;
use fixed_map::Map;
use itertools::Itertools;
use serde::Serialize;

use crate::event_file::misc::PitcherResponsibilityAdjustment;
use crate::event_file::play::{Base, BaseRunner, PlayRecord, RunnerAdvance};
use crate::event_file::traits::{LineupPosition, Pitcher};

use super::EventId;

/// Reported back from `apply_pitcher_responsibility` when the adjustment
/// names a base with no runner — see BSN191409102 in the corpus.
#[derive(Debug, Clone)]
pub(super) struct MissingRunnerWarning {
    pub adjustment: PitcherResponsibilityAdjustment,
}

#[derive(Debug, Eq, PartialEq, Default, Clone, Serialize)]
pub struct BaseState {
    bases: Map<BaseRunner, Runner>,
    scored: ArrayVec<Runner, 4>,
}

impl BaseState {
    pub fn new_inning_tiebreaker(new_runner: LineupPosition, event_id: EventId) -> Self {
        let runner = Runner {
            lineup_position: new_runner,
            reached_on_event_id: event_id,
            charge_event_id: event_id,
            explicit_charged_pitcher_id: None,
        };
        let mut bases = Map::new();
        bases.insert(BaseRunner::Second, runner);
        Self {
            bases,
            scored: ArrayVec::new(),
        }
    }

    pub fn get_from_position(&self, position: LineupPosition) -> Option<&Runner> {
        self.bases.iter().find_map(|(_, runner)| {
            if runner.lineup_position == position {
                Some(runner)
            } else {
                None
            }
        })
    }

    pub fn get_base_state(&self) -> u8 {
        // Integer representation of the base state with each binary digit representing a base
        u8::from(self.get_first().is_some())
            | u8::from(self.get_second().is_some()) << 1
            | u8::from(self.get_third().is_some()) << 2
    }

    pub(super) fn num_runners_on_base(&self) -> usize {
        self.bases.len()
    }

    pub fn get_runner(&self, baserunner: BaseRunner) -> Option<&Runner> {
        self.bases.get(baserunner)
    }

    fn get_first(&self) -> Option<&Runner> {
        self.bases.get(BaseRunner::First)
    }

    fn get_second(&self) -> Option<&Runner> {
        self.bases.get(BaseRunner::Second)
    }

    fn get_third(&self) -> Option<&Runner> {
        self.bases.get(BaseRunner::Third)
    }

    fn get_advance_from_baserunner(
        baserunner: BaseRunner,
        play: &PlayRecord,
    ) -> Option<&RunnerAdvance> {
        play.stats
            .advances
            .iter()
            .find(|a| a.baserunner == baserunner)
    }
}

#[derive(Debug, Eq, PartialEq, Copy, Clone, Serialize)]
pub struct Runner {
    pub lineup_position: LineupPosition,
    pub reached_on_event_id: EventId,
    /// This differs from the `reached_on` field in the event of a force-out
    /// or fielder's choice. The reason we track event ID instead of
    /// the pitcher is so that we can compute run assignments for any
    /// fielder in the same way.
    pub charge_event_id: EventId,
    /// However, there are some cases where the pitcher is explicitly
    /// charged with the baserunner.
    pub explicit_charged_pitcher_id: Option<Pitcher>,
}

fn target_base_occupied(bases: &Map<BaseRunner, Runner>, advance: &RunnerAdvance) -> bool {
    let br = BaseRunner::from_target_base(advance.to);
    bases.get(br).is_some()
}

fn current_base_occupied(bases: &Map<BaseRunner, Runner>, advance: &RunnerAdvance) -> bool {
    bases.get(advance.baserunner).is_some()
}

fn check_integrity(
    old_bases: &Map<BaseRunner, Runner>,
    new_bases: &Map<BaseRunner, Runner>,
    advance: &RunnerAdvance,
) -> Result<()> {
    if target_base_occupied(new_bases, advance) {
        bail!("Runner is listed as moving to a base that is occupied by another runner")
    } else if current_base_occupied(old_bases, advance) {
        Ok(())
    } else {
        bail!(
            "Advancement from a base that had no runner on it.\n\
            Old bases: {old_bases:?}\n\
            New bases: {new_bases:?}\n\
            Advance: {advance:?}\n"
        )
    }
}

/// Accounts for Rule 9.16(g) regarding the assignment of trailing
/// baserunners as inherited if they advance on a fielder's choice 🙃.
/// Returns the new map alongside the `charge_event_id` of the new batter,
/// if applicable.
fn update_runner_charges(
    prev: &Map<BaseRunner, Runner>,
    play: &PlayRecord,
) -> Result<(Map<BaseRunner, Runner>, Option<EventId>)> {
    let mut bases = *prev;
    let mut batter_charge_event_id = None;
    for out_baserunner in &play.stats.batter_caused_baserunning_outs {
        let out_runner = bases.get(*out_baserunner).context("No runner on base")?;
        let mut carry = Some(out_runner.charge_event_id);
        let trailing_runners: Vec<BaseRunner> = bases
            .iter()
            .map(|(b, _)| b)
            .sorted_by(|a, b| b.cmp(a))
            .filter(|b| *b < *out_baserunner)
            .collect();
        for baserunner in trailing_runners {
            // Always Some by this point: set in the prior loop iteration.
            let new_charge = carry.context("charge_event_id unset")?;
            let runner = bases.get(baserunner).context("runner vanished mid-shift")?;
            let prior_charge = runner.charge_event_id;
            let updated = Runner {
                charge_event_id: new_charge,
                ..*runner
            };
            bases.insert(baserunner, updated);
            carry = Some(prior_charge);
        }
        batter_charge_event_id = carry;
    }

    Ok((bases, batter_charge_event_id))
}

/// Pure transform: derive the post-play base state from the pre-play state.
///
/// `start_inning` resets the bases to default before applying advances.
/// `end_inning` suppresses placing the batter on a base (third out ends the
/// inning before the batter could occupy a base).
pub(super) fn advance_base_state(
    prev: &BaseState,
    start_inning: bool,
    end_inning: bool,
    play: &PlayRecord,
    batter_lineup_position: LineupPosition,
    event_id: EventId,
) -> Result<BaseState> {
    let (mut bases, batter_charge_event_id) = if start_inning {
        (Map::<BaseRunner, Runner>::new(), None)
    } else {
        update_runner_charges(&prev.bases, play)?
    };
    let mut scored: ArrayVec<Runner, 4> = ArrayVec::new();

    // Cover cases where outs are not included in advance information
    for out in &play.stats.outs {
        bases.remove(*out);
    }

    if let Some(a) = BaseState::get_advance_from_baserunner(BaseRunner::Third, play) {
        bases.remove(BaseRunner::Third);
        if a.is_out() {
        } else {
            check_integrity(&prev.bases, &bases, a)?;
            if let Some(r) = prev.get_third() {
                scored.push(*r);
            }
        }
    }
    if let Some(a) = BaseState::get_advance_from_baserunner(BaseRunner::Second, play) {
        bases.remove(BaseRunner::Second);
        if a.is_out() {
        } else {
            check_integrity(&prev.bases, &bases, a)?;
            if let (true, Some(r)) = (
                a.is_this_that_one_time_jean_segura_ran_in_reverse(),
                prev.get_second(),
            ) {
                bases.insert(BaseRunner::First, *r);
            } else if let (Base::Third, Some(r)) = (a.to, prev.get_second()) {
                bases.insert(BaseRunner::Third, *r);
            } else if let (Base::Home, Some(r)) = (a.to, prev.get_second()) {
                scored.push(*r);
            }
        }
    }
    if let Some(a) = BaseState::get_advance_from_baserunner(BaseRunner::First, play) {
        bases.remove(BaseRunner::First);
        if a.is_out() {
        } else {
            check_integrity(&prev.bases, &bases, a)?;
            if let (Base::Second, Some(r)) = (&a.to, prev.get_first()) {
                bases.insert(BaseRunner::Second, *r);
            } else if let (Base::Third, Some(r)) = (&a.to, prev.get_first()) {
                bases.insert(BaseRunner::Third, *r);
            } else if let (Base::Home, Some(r)) = (&a.to, prev.get_first()) {
                scored.push(*r);
            }
        }
    }
    if let Some(a) = BaseState::get_advance_from_baserunner(BaseRunner::Batter, play) {
        let new_runner = Runner {
            lineup_position: batter_lineup_position,
            reached_on_event_id: event_id,
            charge_event_id: batter_charge_event_id.unwrap_or(event_id),
            explicit_charged_pitcher_id: None,
        };
        match a.to {
            _ if a.is_out() || end_inning => {}
            _ if target_base_occupied(&bases, a) => {
                return Err(anyhow!("Batter advanced to an occupied base"));
            }
            Base::Home => scored.push(new_runner),
            b => {
                bases.insert(BaseRunner::from_current_base(b), new_runner);
            }
        }
    }
    Ok(BaseState { bases, scored })
}

/// Pure pitcher-responsibility adjustment. Returns the new state and an
/// `Option<MissingRunnerWarning>` when the adjustment names an empty base
/// (so the shell can warn but keep parsing).
pub(super) fn apply_pitcher_responsibility(
    prev: &BaseState,
    record: &PitcherResponsibilityAdjustment,
) -> (BaseState, Option<MissingRunnerWarning>) {
    let Some(runner) = prev.get_runner(record.baserunner).copied() else {
        return (
            prev.clone(),
            Some(MissingRunnerWarning {
                adjustment: *record,
            }),
        );
    };
    let updated = Runner {
        explicit_charged_pitcher_id: Some(record.pitcher_id),
        ..runner
    };
    let mut bases = prev.bases;
    bases.insert(record.baserunner, updated);
    (
        BaseState {
            bases,
            scored: prev.scored.clone(),
        },
        None,
    )
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::*;

    fn make_runner(lineup: LineupPosition, event_id: u8) -> Runner {
        Runner {
            lineup_position: lineup,
            reached_on_event_id: EventId::new(event_id.into()).unwrap(),
            charge_event_id: EventId::new(event_id.into()).unwrap(),
            explicit_charged_pitcher_id: None,
        }
    }

    /// Build a `BaseState` with the given runners. Test-only constructor;
    /// production code paths build `BaseState` through `advance_base_state`,
    /// `new_inning_tiebreaker`, `apply_pitcher_responsibility`, or
    /// `Default::default`.
    fn bases_with(runners: &[(BaseRunner, Runner)]) -> BaseState {
        let mut bases = Map::new();
        for (b, r) in runners {
            bases.insert(*b, *r);
        }
        BaseState {
            bases,
            scored: ArrayVec::new(),
        }
    }

    #[test]
    fn base_state_default_is_empty() {
        let bs = BaseState::default();
        assert_eq!(bs.num_runners_on_base(), 0);
        assert!(bs.get_runner(BaseRunner::First).is_none());
        assert!(bs.get_runner(BaseRunner::Second).is_none());
        assert!(bs.get_runner(BaseRunner::Third).is_none());
        assert_eq!(bs.get_base_state(), 0);
    }

    #[test]
    fn base_state_with_runner_then_get_returns_runner() {
        let bs = bases_with(&[(BaseRunner::Second, make_runner(LineupPosition::Third, 7))]);
        let got = bs.get_runner(BaseRunner::Second).unwrap();
        assert_eq!(got.lineup_position, LineupPosition::Third);
        assert_eq!(got.reached_on_event_id, EventId::new(7).unwrap());
        assert_eq!(bs.num_runners_on_base(), 1);
    }

    #[test]
    fn base_state_full_bases_three_runners() {
        let bs = bases_with(&[
            (BaseRunner::First, make_runner(LineupPosition::First, 1)),
            (BaseRunner::Second, make_runner(LineupPosition::Second, 2)),
            (BaseRunner::Third, make_runner(LineupPosition::Third, 3)),
        ]);
        assert_eq!(bs.num_runners_on_base(), 3);
        assert_eq!(bs.get_base_state(), 0b111);
    }

    #[test]
    fn base_state_get_base_state_encodes_occupancy_bitfield() {
        // Each table row asserts the bitfield contract derived from the
        // BaseState definition: bit 0 = First, bit 1 = Second, bit 2 = Third.
        let cases = [
            (vec![], 0b000),
            (vec![BaseRunner::First], 0b001),
            (vec![BaseRunner::Second], 0b010),
            (vec![BaseRunner::Third], 0b100),
            (vec![BaseRunner::First, BaseRunner::Third], 0b101),
        ];
        for (occupied, expected) in cases {
            let runners: Vec<(BaseRunner, Runner)> = occupied
                .iter()
                .enumerate()
                .map(|(i, br)| {
                    let event_id = u8::try_from(i + 1).unwrap();
                    (*br, make_runner(LineupPosition::First, event_id))
                })
                .collect();
            let bs = bases_with(&runners);
            assert_eq!(
                bs.get_base_state(),
                expected,
                "expected {expected:#05b} for {occupied:?}"
            );
        }
    }

    #[test]
    fn base_state_new_inning_tiebreaker_places_only_on_second() {
        let bs = BaseState::new_inning_tiebreaker(LineupPosition::Fourth, EventId::new(5).unwrap());
        assert!(bs.get_runner(BaseRunner::First).is_none());
        let runner = bs.get_runner(BaseRunner::Second).unwrap();
        assert_eq!(runner.lineup_position, LineupPosition::Fourth);
        assert!(bs.get_runner(BaseRunner::Third).is_none());
        assert_eq!(bs.num_runners_on_base(), 1);
    }

    #[test]
    fn base_state_get_from_position_finds_runner_by_lineup_position() {
        let bs = bases_with(&[(BaseRunner::Third, make_runner(LineupPosition::Sixth, 4))]);
        let runner = bs.get_from_position(LineupPosition::Sixth).unwrap();
        assert_eq!(runner.reached_on_event_id, EventId::new(4).unwrap());
        assert!(bs.get_from_position(LineupPosition::Ninth).is_none());
    }
}

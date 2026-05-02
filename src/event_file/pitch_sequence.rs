use std::collections::HashSet;
use std::str::FromStr;
use std::sync::Mutex;

use anyhow::{Context, Result};
use serde::{Deserialize, Serialize};
use strum_macros::{AsRefStr, EnumString};
use tracing::warn;

use crate::event_file::play::Base;
use crate::event_file::traits::SequenceId;

// Dedup the unrecognized-pitch-char warn at most once per distinct char per
// process. Without this, a single new char appearing on thousands of pitches
// in a full-corpus run would flood the log.
static UNRECOGNIZED_PITCH_CHARS_SEEN: Mutex<Option<HashSet<char>>> = Mutex::new(None);

fn warn_unrecognized_pitch_char_once(c: char) {
    if let Ok(mut guard) = UNRECOGNIZED_PITCH_CHARS_SEEN.lock() {
        let seen = guard.get_or_insert_with(HashSet::new);
        if seen.insert(c) {
            warn!(
                "Unrecognized pitch char {c:?}, mapping to {:?}",
                PitchType::Unrecognized
            );
        }
    }
}

#[derive(
    Debug,
    Default,
    Ord,
    PartialOrd,
    Eq,
    PartialEq,
    EnumString,
    Copy,
    Clone,
    Serialize,
    Deserialize,
    Hash,
    AsRefStr,
)]
pub enum PitchType {
    #[strum(serialize = "1")]
    PickoffAttemptFirst,
    #[strum(serialize = "2")]
    PickoffAttemptSecond,
    #[strum(serialize = "3")]
    PickoffAttemptThird,
    #[strum(serialize = ".")]
    PlayNotInvolvingBatter,
    #[strum(serialize = "B")]
    Ball,
    #[strum(serialize = "C")]
    CalledStrike,
    #[strum(serialize = "F")]
    Foul,
    #[strum(serialize = "H")]
    HitBatter,
    #[strum(serialize = "I")]
    IntentionalBall,
    #[strum(serialize = "K")]
    StrikeUnknownType,
    #[strum(serialize = "L")]
    FoulBunt,
    #[strum(serialize = "M")]
    MissedBunt,
    #[strum(serialize = "N")]
    NoPitch,
    #[strum(serialize = "O")]
    FoulTipBunt,
    #[strum(serialize = "P")]
    Pitchout,
    #[strum(serialize = "Q")]
    SwingingOnPitchout,
    #[strum(serialize = "R")]
    FoulOnPitchout,
    #[strum(serialize = "S")]
    SwingingStrike,
    #[strum(serialize = "T")]
    FoulTip,
    #[strum(serialize = "U", serialize = "?")]
    Unknown,
    #[strum(serialize = "V")]
    AutomaticBall,
    #[strum(serialize = "A")]
    AutomaticStrike,
    #[strum(serialize = "X")]
    InPlay,
    #[strum(serialize = "Y")]
    InPlayOnPitchout,
    #[default]
    Unrecognized,
}

#[derive(Debug, PartialEq, Eq, Copy, Clone, Serialize, Deserialize, Hash)]
pub struct PitchSequenceItem {
    pub sequence_id: SequenceId,
    pub pitch_type: PitchType,
    pub runners_going: bool,
    pub blocked_by_catcher: bool,
    pub catcher_pickoff_attempt: Option<Base>,
}

pub type PitchSequence = Vec<PitchSequenceItem>;

impl PitchSequenceItem {
    fn new(sequence_id: usize) -> Result<Self> {
        Ok(Self {
            sequence_id: SequenceId::new(sequence_id).context("Invalid sequence id")?,
            pitch_type: PitchType::default(),
            runners_going: false,
            blocked_by_catcher: false,
            catcher_pickoff_attempt: None,
        })
    }
}

impl PitchSequenceItem {
    const fn update_pitch_type(&mut self, pitch_type: PitchType) {
        self.pitch_type = pitch_type;
    }
    const fn update_catcher_pickoff(&mut self, base: Option<Base>) {
        self.catcher_pickoff_attempt = base;
    }
    const fn update_blocked_by_catcher(&mut self) {
        self.blocked_by_catcher = true;
    }
    const fn update_runners_going(&mut self) {
        self.runners_going = true;
    }

    #[allow(clippy::unused_peekable)]
    pub fn new_pitch_sequence(str_sequence: &str) -> Result<PitchSequence> {
        let mut pitches = Vec::with_capacity(10);

        // If a single PA lasts multiple events (e.g. because of a stolen base or substitution),
        // event rows will carry over the pitch sequence of all previous events in that PA.
        // An interruption in the PA is indicated with "."
        // To avoid double-counting, if there's a ".", we only take the sequence to the right of
        // the right-most "." (Should I end that sentence with a period? How does that work)
        let trimmed_sequence = if let Some((_, s)) = str_sequence.rsplit_once('.') {
            s
        } else {
            str_sequence
        };
        let mut char_iter = trimmed_sequence.chars().peekable();
        let mut pitch = Self::new(1)?;

        let get_catcher_pickoff_base =
            { |c: Option<char>| Base::from_str(&c.unwrap_or('.').to_string()).ok() };

        // PA-resume pickoff `.+N` — see docs/pitch_sequence_pa_resume.md.
        let mut pending_resume_pickoff: Option<Base> = if char_iter.peek() == Some(&'+') {
            char_iter.next();
            get_catcher_pickoff_base(char_iter.next())
        } else {
            None
        };

        while let Some(c) = char_iter.next() {
            match c {
                // Tokens indicating info on the upcoming pitch
                '*' => {
                    pitch.update_blocked_by_catcher();
                    continue;
                }
                '>' => {
                    pitch.update_runners_going();
                    continue;
                }
                // Duplicate pickoff annotation — see docs/pitch_sequence_pa_resume.md.
                // Drop `+N` only when the next char is a base digit; otherwise
                // surface as Unrecognized so a future malformed pattern stays loud.
                '+' => {
                    if matches!(char_iter.peek(), Some('1' | '2' | '3' | 'H')) {
                        let _ = char_iter.next();
                        continue;
                    }
                }
                _ => {}
            }
            let pitch_type = PitchType::from_str(&c.to_string()).unwrap_or_else(|_| {
                warn_unrecognized_pitch_char_once(c);
                PitchType::default()
            });
            pitch.update_pitch_type(pitch_type);

            if let Some(base) = pending_resume_pickoff.take() {
                pitch.update_catcher_pickoff(Some(base));
            }

            match char_iter.peek() {
                // Tokens indicating info on the previous pitch
                Some('>') => {
                    // ">+N" (~70 corpus occurrences, ~always a catcher pickoff
                    // /CS) likely means the runner was going on the pickoff,
                    // but PitchSequenceItem has no slot for "runners going on
                    // previous pitch", so we keep the "+N" base and drop ">".
                    let mut speculative_iter = char_iter.clone();
                    if speculative_iter.nth(1) == Some('+') {
                        pitch.update_catcher_pickoff(get_catcher_pickoff_base(char_iter.nth(2)));
                    }
                }
                Some('+') => {
                    pitch.update_catcher_pickoff(get_catcher_pickoff_base(char_iter.nth(1)));
                }
                _ => {}
            }
            let final_pitch = pitch;
            pitch = Self::new(final_pitch.sequence_id.get() + 1)?;
            pitches.push(final_pitch);
        }
        Ok(pitches)
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used)]
mod tests {
    use super::*;

    fn types(seq: &PitchSequence) -> Vec<PitchType> {
        seq.iter().map(|p| p.pitch_type).collect()
    }

    #[test]
    fn empty_string_yields_empty_sequence() {
        let s = PitchSequenceItem::new_pitch_sequence("").unwrap();
        assert!(s.is_empty());
    }

    #[test]
    fn basic_pitches_map_to_expected_types() {
        let s = PitchSequenceItem::new_pitch_sequence("BCSFX").unwrap();
        assert_eq!(
            types(&s),
            vec![
                PitchType::Ball,
                PitchType::CalledStrike,
                PitchType::SwingingStrike,
                PitchType::Foul,
                PitchType::InPlay,
            ]
        );
    }

    #[test]
    fn unknown_char_becomes_unrecognized_default() {
        let s = PitchSequenceItem::new_pitch_sequence("Z").unwrap();
        assert_eq!(types(&s), vec![PitchType::Unrecognized]);
    }

    #[test]
    fn pitch_clock_violation_chars_map_to_automatic_pitches() {
        // V = automatic ball (pitcher violation), A = automatic strike (batter violation).
        // Both introduced with MLB pitch clock in 2023.
        let s = PitchSequenceItem::new_pitch_sequence("VA").unwrap();
        assert_eq!(
            types(&s),
            vec![PitchType::AutomaticBall, PitchType::AutomaticStrike]
        );
    }

    #[test]
    fn question_mark_aliases_unknown_pitch() {
        // Retrosheet uses '?' for an unknown pitch in an otherwise-known sequence.
        let s = PitchSequenceItem::new_pitch_sequence("U?").unwrap();
        assert_eq!(types(&s), vec![PitchType::Unknown, PitchType::Unknown]);
    }

    #[test]
    fn pitch_type_from_str_accepts_canonical_and_alias_for_unknown() {
        // Lock down the alias semantics independent of the iterator path.
        assert_eq!(PitchType::from_str("U").unwrap(), PitchType::Unknown);
        assert_eq!(PitchType::from_str("?").unwrap(), PitchType::Unknown);
    }

    #[test]
    fn dot_truncates_to_rightmost_segment() {
        // PA spans events; only the segment after the last "." should be kept.
        let s = PitchSequenceItem::new_pitch_sequence("BB.CX").unwrap();
        assert_eq!(types(&s), vec![PitchType::CalledStrike, PitchType::InPlay]);
    }

    #[test]
    fn multiple_dots_keep_only_final_segment() {
        let s = PitchSequenceItem::new_pitch_sequence("B.C.SX").unwrap();
        assert_eq!(
            types(&s),
            vec![PitchType::SwingingStrike, PitchType::InPlay]
        );
    }

    #[test]
    fn star_marks_blocked_by_catcher_on_following_pitch() {
        let s = PitchSequenceItem::new_pitch_sequence("B*BC").unwrap();
        assert_eq!(
            types(&s),
            vec![PitchType::Ball, PitchType::Ball, PitchType::CalledStrike]
        );
        assert!(!s[0].blocked_by_catcher);
        assert!(s[1].blocked_by_catcher);
        assert!(!s[2].blocked_by_catcher);
    }

    #[test]
    fn arrow_marks_runners_going_on_following_pitch() {
        let s = PitchSequenceItem::new_pitch_sequence(">CX").unwrap();
        assert_eq!(types(&s), vec![PitchType::CalledStrike, PitchType::InPlay]);
        assert!(s[0].runners_going);
        assert!(!s[1].runners_going);
    }

    #[test]
    fn plus_records_catcher_pickoff_to_base() {
        let s = PitchSequenceItem::new_pitch_sequence("B+2C").unwrap();
        // "B" picks up the "+2" as a catcher pickoff to second base.
        assert_eq!(s[0].catcher_pickoff_attempt, Some(Base::Second));
        assert_eq!(s[1].catcher_pickoff_attempt, None);
    }

    #[test]
    fn plus_followed_by_non_base_char_yields_no_pickoff() {
        // "X" is not a valid base ("1", "2", "3", "H"); pickoff is None.
        let s = PitchSequenceItem::new_pitch_sequence("B+X").unwrap();
        assert_eq!(s[0].catcher_pickoff_attempt, None);
    }

    #[test]
    fn pa_resume_pickoff_attaches_to_first_post_resume_pitch() {
        // `BCS>B.+3FX` (e.g. MIN202309100 lewir003): rsplit gives `+3FX`.
        // The `+3` is a catcher pickoff at the moment of PA resumption — it
        // must attach to the first real pitch (`F`), not become its own
        // Unrecognized pitch followed by a phantom PickoffAttemptThird.
        let s = PitchSequenceItem::new_pitch_sequence("BCS>B.+3FX").unwrap();
        assert_eq!(types(&s), vec![PitchType::Foul, PitchType::InPlay]);
        assert_eq!(s[0].catcher_pickoff_attempt, Some(Base::Third));
        assert_eq!(s[1].catcher_pickoff_attempt, None);
    }

    #[test]
    fn pa_resume_pickoff_works_with_first_base() {
        // `BB.+1BB` (TOR201805230 cozaz001).
        let s = PitchSequenceItem::new_pitch_sequence("BB.+1BB").unwrap();
        assert_eq!(types(&s), vec![PitchType::Ball, PitchType::Ball]);
        assert_eq!(s[0].catcher_pickoff_attempt, Some(Base::First));
        assert_eq!(s[1].catcher_pickoff_attempt, None);
    }

    #[test]
    fn pa_resume_pickoff_with_no_following_pitch_is_silent() {
        // `.+1` (SLN199009300 pagnt001): degenerate case — the resumption
        // pickoff has no following pitch. Emit nothing rather than a phantom.
        let s = PitchSequenceItem::new_pitch_sequence(".+1").unwrap();
        assert!(s.is_empty());
    }

    #[test]
    fn pa_resume_pickoff_only_uses_rightmost_dot() {
        // `B*BBC+1.+1F>X` (PHI201607010 franm004) has `+1` mid-sequence and
        // another `+1` at PA resumption. The mid-sequence one is dropped by
        // the rsplit (everything before the rightmost `.` goes away); the
        // resumption one must still attach to `F`.
        let s = PitchSequenceItem::new_pitch_sequence("B*BBC+1.+1F>X").unwrap();
        assert_eq!(types(&s), vec![PitchType::Foul, PitchType::InPlay]);
        assert_eq!(s[0].catcher_pickoff_attempt, Some(Base::First));
        assert!(s[1].runners_going);
    }

    #[test]
    fn duplicate_pickoff_annotation_is_dropped_silently() {
        // `BBBC+1+1B` (WAS202508220 abrac001): C records pickoff to first
        // via the peek path; the second `+1` is a duplicate annotation with
        // no schema slot. Drop it rather than emit a phantom Unrecognized
        // pitch + phantom PickoffAttemptFirst.
        let s = PitchSequenceItem::new_pitch_sequence("BBBC+1+1B").unwrap();
        assert_eq!(
            types(&s),
            vec![
                PitchType::Ball,
                PitchType::Ball,
                PitchType::Ball,
                PitchType::CalledStrike,
                PitchType::Ball,
            ]
        );
        assert_eq!(s[3].catcher_pickoff_attempt, Some(Base::First));
    }

    #[test]
    fn arrow_plus_after_already_pickoffed_pitch_drops_extra() {
        // `M+1>+1` (BAL201706050 buxtb001): M takes pickoff to first via
        // peek; the trailing `>+1` is a duplicate annotation. The `>` flips
        // runners_going on the next (never-emitted) pitch slot; the bare
        // `+1` is dropped silently.
        let s = PitchSequenceItem::new_pitch_sequence("M+1>+1").unwrap();
        assert_eq!(types(&s), vec![PitchType::MissedBunt]);
        assert_eq!(s[0].catcher_pickoff_attempt, Some(Base::First));
    }

    #[test]
    fn arrow_followed_by_plus_pickoff_still_works() {
        // Regression: ensure the existing `>+N` handling (runners going +
        // catcher pickoff on the previous pitch) still works after the
        // resume-pickoff change.
        let s = PitchSequenceItem::new_pitch_sequence("B>+2C").unwrap();
        assert_eq!(types(&s), vec![PitchType::Ball, PitchType::CalledStrike]);
        assert_eq!(s[0].catcher_pickoff_attempt, Some(Base::Second));
    }

    #[test]
    fn sequence_ids_are_one_indexed_and_contiguous() {
        let s = PitchSequenceItem::new_pitch_sequence("BCFSX").unwrap();
        let ids: Vec<usize> = s.iter().map(|p| p.sequence_id.get()).collect();
        assert_eq!(ids, vec![1, 2, 3, 4, 5]);
    }
}

use std::collections::HashSet;
use std::str::FromStr;
use std::sync::Mutex;

use anyhow::{Context, Result, ensure};
use serde::{Deserialize, Serialize};
use strum_macros::{AsRefStr, EnumString};
use tracing::warn;

use crate::event_file::play::Base;
use crate::event_file::traits::SequenceId;

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

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct PitchSequenceAnnotationConflict {
    pub token_index: usize,
    pub existing: Base,
    pub incoming: Base,
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct ParsedPitchSequence {
    pub pitches: PitchSequence,
    pub tokens: Vec<char>,
    pub annotation_conflicts: Vec<PitchSequenceAnnotationConflict>,
}

fn catcher_annotation_follows(chars: &std::iter::Peekable<std::str::Chars<'_>>) -> bool {
    let mut chars = chars.clone();
    if chars.peek() == Some(&'>') {
        chars.next();
    }
    chars.next() == Some('+') && matches!(chars.next(), Some('1' | '2' | '3' | 'H'))
}

fn update_pending_resume_pickoff(
    pending_resume_pickoff: &mut Option<Base>,
    incoming: Base,
    token_index: usize,
    annotation_conflicts: &mut Vec<PitchSequenceAnnotationConflict>,
) {
    match pending_resume_pickoff {
        Some(existing) if *existing != incoming => {
            annotation_conflicts.push(PitchSequenceAnnotationConflict {
                token_index,
                existing: *existing,
                incoming,
            });
        }
        None => *pending_resume_pickoff = Some(incoming),
        _ => {}
    }
}

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

    pub fn new_pitch_sequence(str_sequence: &str) -> Result<PitchSequence> {
        let parsed = Self::parse_pitch_sequence(str_sequence)?;
        ensure!(
            parsed.annotation_conflicts.is_empty(),
            "Conflicting catcher-pickoff annotations in {str_sequence:?}"
        );
        Ok(parsed.pitches)
    }

    pub fn parse_pitch_sequence(str_sequence: &str) -> Result<ParsedPitchSequence> {
        let mut pitches: PitchSequence = Vec::with_capacity(10);
        let mut tokens = Vec::with_capacity(10);
        let mut annotation_conflicts = Vec::new();

        let mut char_iter = str_sequence.chars().peekable();
        let mut pitch = Self::new(1)?;

        let get_catcher_pickoff_base =
            { |c: Option<char>| Base::from_str(&c.unwrap_or('.').to_string()).ok() };

        let mut pending_resume_pickoff: Option<Base> = if char_iter.peek() == Some(&'+')
            && matches!(char_iter.clone().nth(1), Some('1' | '2' | '3' | 'H'))
        {
            char_iter.next();
            get_catcher_pickoff_base(char_iter.next())
        } else {
            None
        };
        let mut annotation_target: Option<usize> = None;

        while let Some(c) = char_iter.next() {
            match c {
                '.' => {
                    annotation_target = None;
                    if char_iter.peek() == Some(&'+') {
                        let mut next = char_iter.clone();
                        next.next();
                        if let Some(base) = get_catcher_pickoff_base(next.next()) {
                            update_pending_resume_pickoff(
                                &mut pending_resume_pickoff,
                                base,
                                tokens.len(),
                                &mut annotation_conflicts,
                            );
                            char_iter = next;
                        }
                    }
                    continue;
                }
                '*' => {
                    pitch.update_blocked_by_catcher();
                    continue;
                }
                '>' => {
                    pitch.update_runners_going();
                    continue;
                }
                '+' => {
                    if matches!(char_iter.peek(), Some('1' | '2' | '3' | 'H')) {
                        let base = get_catcher_pickoff_base(char_iter.next());
                        if let (Some(token_index), Some(incoming)) = (annotation_target, base) {
                            let existing = pitches[token_index].catcher_pickoff_attempt;
                            if existing.is_none() {
                                pitches[token_index].update_catcher_pickoff(Some(incoming));
                            } else if existing != Some(incoming) {
                                annotation_conflicts.push(PitchSequenceAnnotationConflict {
                                    token_index,
                                    existing: existing.unwrap_or(incoming),
                                    incoming,
                                });
                            }
                        } else if let Some(incoming) = base
                            && (pending_resume_pickoff.is_some() || pitches.is_empty())
                        {
                            update_pending_resume_pickoff(
                                &mut pending_resume_pickoff,
                                incoming,
                                tokens.len(),
                                &mut annotation_conflicts,
                            );
                        }
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
            let final_pitch = pitch;
            pitch = Self::new(final_pitch.sequence_id.get() + 1)?;
            pitches.push(final_pitch);
            tokens.push(c);
            let token_index = pitches.len() - 1;
            annotation_target = catcher_annotation_follows(&char_iter).then_some(token_index);
        }
        Ok(ParsedPitchSequence {
            pitches,
            tokens,
            annotation_conflicts,
        })
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
    fn sequence_only_api_rejects_conflicting_annotations() {
        assert!(PitchSequenceItem::new_pitch_sequence("C+1+2B").is_err());
        assert!(PitchSequenceItem::parse_pitch_sequence("C+1+2B").is_ok());
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
        let s = PitchSequenceItem::new_pitch_sequence("VA").unwrap();
        assert_eq!(
            types(&s),
            vec![PitchType::AutomaticBall, PitchType::AutomaticStrike]
        );
    }

    #[test]
    fn question_mark_aliases_unknown_pitch() {
        let s = PitchSequenceItem::new_pitch_sequence("U?").unwrap();
        assert_eq!(types(&s), vec![PitchType::Unknown, PitchType::Unknown]);
    }

    #[test]
    fn parsed_sequence_preserves_the_raw_character_for_each_pitch() {
        let parsed = PitchSequenceItem::parse_pitch_sequence("U?.+1N1Z").unwrap();
        assert_eq!(parsed.tokens, vec!['U', '?', 'N', '1', 'Z']);
        assert_eq!(
            types(&parsed.pitches),
            vec![
                PitchType::Unknown,
                PitchType::Unknown,
                PitchType::NoPitch,
                PitchType::PickoffAttemptFirst,
                PitchType::Unrecognized,
            ]
        );
        assert_eq!(parsed.pitches[2].catcher_pickoff_attempt, Some(Base::First));
    }

    #[test]
    fn pitch_type_from_str_accepts_canonical_and_alias_for_unknown() {
        assert_eq!(PitchType::from_str("U").unwrap(), PitchType::Unknown);
        assert_eq!(PitchType::from_str("?").unwrap(), PitchType::Unknown);
    }

    #[test]
    fn dot_preserves_all_unattributed_segments() {
        let s = PitchSequenceItem::new_pitch_sequence("BB.CX").unwrap();
        assert_eq!(
            types(&s),
            types(&PitchSequenceItem::new_pitch_sequence("BBCX").unwrap())
        );
    }

    #[test]
    fn multiple_dots_preserve_all_pitches() {
        let s = PitchSequenceItem::new_pitch_sequence("B.C.SX").unwrap();
        assert_eq!(
            types(&s),
            vec![
                PitchType::Ball,
                PitchType::CalledStrike,
                PitchType::SwingingStrike,
                PitchType::InPlay
            ]
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
        assert_eq!(s[0].catcher_pickoff_attempt, Some(Base::Second));
        assert_eq!(s[1].catcher_pickoff_attempt, None);
    }

    #[test]
    fn plus_followed_by_non_base_char_yields_no_pickoff() {
        let s = PitchSequenceItem::new_pitch_sequence("B+X").unwrap();
        assert_eq!(s[0].catcher_pickoff_attempt, None);
        assert_eq!(
            types(&s),
            vec![PitchType::Ball, PitchType::Unrecognized, PitchType::InPlay]
        );
    }

    #[test]
    fn pa_resume_pickoff_attaches_to_first_post_resume_pitch() {
        let s = PitchSequenceItem::new_pitch_sequence("BCS>B.+3FX").unwrap();
        assert_eq!(s.len(), 6);
        assert_eq!(s[4].pitch_type, PitchType::Foul);
        assert_eq!(s[4].catcher_pickoff_attempt, Some(Base::Third));
        assert_eq!(s[5].catcher_pickoff_attempt, None);
    }

    #[test]
    fn pa_resume_pickoff_works_with_first_base() {
        let s = PitchSequenceItem::new_pitch_sequence("BB.+1BB").unwrap();
        assert_eq!(types(&s), vec![PitchType::Ball; 4]);
        assert_eq!(s[2].catcher_pickoff_attempt, Some(Base::First));
        assert_eq!(s[3].catcher_pickoff_attempt, None);
    }

    #[test]
    fn pa_resume_pickoff_with_no_following_pitch_is_silent() {
        let s = PitchSequenceItem::new_pitch_sequence(".+1").unwrap();
        assert!(s.is_empty());
    }

    #[test]
    fn pa_resume_pickoff_preserves_pickoffs_on_both_segments() {
        let s = PitchSequenceItem::new_pitch_sequence("B*BBC+1.+1F>X").unwrap();
        assert_eq!(s.len(), 6);
        assert_eq!(s[3].catcher_pickoff_attempt, Some(Base::First));
        assert_eq!(s[4].catcher_pickoff_attempt, Some(Base::First));
        assert!(s[5].runners_going);
    }

    #[test]
    fn duplicate_pickoff_annotation_is_dropped_silently() {
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
    fn conflicting_pickoff_annotations_are_reported_without_dropping_the_pitch() {
        let parsed = PitchSequenceItem::parse_pitch_sequence("C+1+2B").unwrap();
        assert_eq!(parsed.pitches[0].catcher_pickoff_attempt, Some(Base::First));
        assert_eq!(
            parsed.annotation_conflicts,
            vec![PitchSequenceAnnotationConflict {
                token_index: 0,
                existing: Base::First,
                incoming: Base::Second,
            }]
        );
        assert_eq!(parsed.tokens, vec!['C', 'B']);
    }

    #[test]
    fn pending_resume_pickoff_conflicts_with_a_postfix_pickoff() {
        let parsed = PitchSequenceItem::parse_pitch_sequence(".+1F+2").unwrap();
        assert_eq!(parsed.pitches[0].catcher_pickoff_attempt, Some(Base::First));
        assert_eq!(
            parsed.annotation_conflicts,
            vec![PitchSequenceAnnotationConflict {
                token_index: 0,
                existing: Base::First,
                incoming: Base::Second,
            }]
        );
    }

    #[test]
    fn pending_pickoff_conflicts_preserve_the_first_base_without_a_pitch() {
        for sequence in [".+1.+2F", "+1+2F"] {
            let parsed = PitchSequenceItem::parse_pitch_sequence(sequence).unwrap();
            assert_eq!(parsed.pitches[0].catcher_pickoff_attempt, Some(Base::First));
            assert_eq!(
                parsed.annotation_conflicts,
                vec![PitchSequenceAnnotationConflict {
                    token_index: 0,
                    existing: Base::First,
                    incoming: Base::Second,
                }]
            );
            assert!(PitchSequenceItem::new_pitch_sequence(sequence).is_err());
        }
        let parsed = PitchSequenceItem::parse_pitch_sequence(".+1.+2").unwrap();
        assert!(parsed.pitches.is_empty());
        assert_eq!(parsed.annotation_conflicts[0].token_index, 0);
        assert!(PitchSequenceItem::new_pitch_sequence(".+1.+2").is_err());
    }

    #[test]
    fn resumption_conflicts_target_the_next_pitch_instead_of_a_prior_annotation() {
        let parsed = PitchSequenceItem::parse_pitch_sequence("B+1.+2+1F").unwrap();
        assert_eq!(parsed.pitches[0].catcher_pickoff_attempt, Some(Base::First));
        assert_eq!(
            parsed.pitches[1].catcher_pickoff_attempt,
            Some(Base::Second)
        );
        assert_eq!(
            parsed.annotation_conflicts,
            vec![PitchSequenceAnnotationConflict {
                token_index: 1,
                existing: Base::Second,
                incoming: Base::First,
            }]
        );
    }

    #[test]
    fn duplicate_pending_pickoff_annotations_fold() {
        let parsed = PitchSequenceItem::parse_pitch_sequence(".+1.+1F").unwrap();
        assert_eq!(parsed.pitches[0].catcher_pickoff_attempt, Some(Base::First));
        assert!(parsed.annotation_conflicts.is_empty());
    }

    #[test]
    fn arrow_plus_after_already_pickoffed_pitch_drops_extra() {
        let s = PitchSequenceItem::new_pitch_sequence("M+1>+1").unwrap();
        assert_eq!(types(&s), vec![PitchType::MissedBunt]);
        assert_eq!(s[0].catcher_pickoff_attempt, Some(Base::First));
    }

    #[test]
    fn arrow_followed_by_plus_pickoff_still_works() {
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

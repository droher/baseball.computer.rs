use std::str::FromStr;

use anyhow::{Context, Result};
use serde::{Deserialize, Serialize};
use strum_macros::{AsRefStr, EnumString};

use crate::event_file::misc::arrow_hack;
use crate::event_file::play::Base;
use crate::event_file::traits::SequenceId;

use super::misc::skip_ids;

#[derive(
    Debug,
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
    #[strum(serialize = "U")]
    Unknown,
    #[strum(serialize = "V")]
    AutomaticBall,
    #[strum(serialize = "X")]
    InPlay,
    #[strum(serialize = "Y")]
    InPlayOnPitchout,
    Unrecognized,
}

impl Default for PitchType {
    fn default() -> Self {
        Self::Unrecognized
    }
}

#[derive(Debug, PartialEq, Eq, Copy, Clone, Serialize, Deserialize, Hash)]
pub struct PitchSequenceItem {
    #[serde(skip_serializing_if = "skip_ids")]
    pub sequence_id: SequenceId,
    #[serde(serialize_with = "arrow_hack")]
    pub pitch_type: PitchType,
    pub runners_going: bool,
    pub blocked_by_catcher: bool,
    #[serde(serialize_with = "arrow_hack")]
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
    fn update_pitch_type(&mut self, pitch_type: PitchType) {
        self.pitch_type = pitch_type;
    }
    fn update_catcher_pickoff(&mut self, base: Option<Base>) {
        self.catcher_pickoff_attempt = base;
    }
    fn update_blocked_by_catcher(&mut self) {
        self.blocked_by_catcher = true;
    }
    fn update_runners_going(&mut self) {
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
                _ => {}
            }
            // TODO: Log unrecognized types as a warning once I implement proper spans
            let pitch_type = PitchType::from_str(&c.to_string()).unwrap_or_default();
            pitch.update_pitch_type(pitch_type);

            match char_iter.peek() {
                // Tokens indicating info on the previous pitch
                Some('>') => {
                    // The sequence ">+" occurs around 70 times in the current data, usually but not always on
                    // a pickoff caught stealing initiated by the catcher. It's unclear what the '>' is for, but
                    // it might be to indicate cases in which the runner attempted to advance on the pickoff rather
                    // than get back to the base. Current approach is to just record the catcher pickoff and
                    // not apply the advance attempt info to the pitch.
                    // TODO: Figure out what's going on here and fix if needed or delete the todo
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

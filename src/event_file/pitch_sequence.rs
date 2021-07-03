use std::convert::TryFrom;
use std::num::NonZeroU8;
use std::str::FromStr;

use anyhow::{Context, Error, Result};
use serde::{Deserialize, Serialize};
use strum_macros::EnumString;

use crate::event_file::play::Base;
use crate::event_file::traits::SequenceId;

#[derive(Debug, Ord, PartialOrd, Eq, PartialEq, Copy, Clone, Serialize, Deserialize)]
pub enum SequenceItemTypeGeneral {
    Ball,
    Strike,
    InPlay,
    NoPitch,
    Unknown
}

impl SequenceItemTypeGeneral {
    pub fn is_pitch(&self) -> bool {
        !([Self::NoPitch, Self::Unknown].contains(self))
    }

    pub fn is_strike(&self) -> bool {
        [Self::Strike, Self::InPlay].contains(self)
    }

    pub fn is_in_play(&self) -> bool {
        self == &Self::InPlay
    }

}

#[derive(Debug, Ord, PartialOrd, Eq, PartialEq, EnumString, Copy, Clone, Serialize, Deserialize)]
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
    InPlayOnPitchout
}

impl PitchType {

    pub fn get_sequence_general(&self) -> SequenceItemTypeGeneral {
        match self {
            PitchType::PickoffAttemptFirst => SequenceItemTypeGeneral::NoPitch,
            PitchType::PickoffAttemptSecond => SequenceItemTypeGeneral::NoPitch,
            PitchType::PickoffAttemptThird => SequenceItemTypeGeneral::NoPitch,
            PitchType::PlayNotInvolvingBatter => SequenceItemTypeGeneral::NoPitch,
            PitchType::Ball => SequenceItemTypeGeneral::Ball,
            PitchType::CalledStrike => SequenceItemTypeGeneral::Strike,
            PitchType::Foul => SequenceItemTypeGeneral::Strike,
            PitchType::HitBatter => SequenceItemTypeGeneral::Ball,
            PitchType::IntentionalBall => SequenceItemTypeGeneral::Ball,
            PitchType::StrikeUnknownType => SequenceItemTypeGeneral::Strike,
            PitchType::FoulBunt => SequenceItemTypeGeneral::Strike,
            PitchType::MissedBunt => SequenceItemTypeGeneral::Strike,
            PitchType::NoPitch => SequenceItemTypeGeneral::NoPitch,
            PitchType::FoulTipBunt => SequenceItemTypeGeneral::Strike,
            PitchType::Pitchout => SequenceItemTypeGeneral::Ball,
            PitchType::SwingingOnPitchout => SequenceItemTypeGeneral::Strike,
            PitchType::FoulOnPitchout => SequenceItemTypeGeneral::Strike,
            PitchType::SwingingStrike => SequenceItemTypeGeneral::Strike,
            PitchType::FoulTip => SequenceItemTypeGeneral::Strike,
            PitchType::Unknown => SequenceItemTypeGeneral::Unknown,
            PitchType::AutomaticBall => SequenceItemTypeGeneral::Ball,
            PitchType::InPlay => SequenceItemTypeGeneral::InPlay,
            PitchType::InPlayOnPitchout => SequenceItemTypeGeneral::InPlay,
        }
    }
}


impl Default for PitchType {
    fn default() -> Self { PitchType::Unknown }
}

#[derive(Debug, PartialEq, Eq, Copy, Clone, Serialize, Deserialize)]
pub struct PitchSequenceItem {
    pub sequence_id: SequenceId,
    pub pitch_type: PitchType,
    pub runners_going: bool,
    pub blocked_by_catcher: bool,
    pub catcher_pickoff_attempt: Option<Base>
}

impl PitchSequenceItem {
    fn new(sequence_id: u8) -> Self {
        Self {
            sequence_id: NonZeroU8::new(sequence_id).unwrap(),
            pitch_type: Default::default(),
            runners_going: false,
            blocked_by_catcher: false,
            catcher_pickoff_attempt: None
        }
    }
}

impl PitchSequenceItem {
    fn update_pitch_type(&mut self, pitch_type: PitchType) {
        self.pitch_type = pitch_type
    }
    fn update_catcher_pickoff(&mut self, base: Option<Base>) {
        self.catcher_pickoff_attempt = base
    }
    fn update_blocked_by_catcher(&mut self) {
        self.blocked_by_catcher = true
    }
    fn update_runners_going(&mut self) {
        self.runners_going = true
    }
}

#[derive(Debug, Default, Eq, PartialEq, Clone)]
pub struct PitchSequence(pub Vec<PitchSequenceItem>);

impl TryFrom<&str> for PitchSequence {
    type Error = Error;

    fn try_from(str_sequence: &str) -> Result<Self> {
        let mut pitches= Vec::with_capacity(10);

        // If a single PA lasts multiple events (e.g. because of a stolen base or substitution),
        // event rows will carry over the pitch sequence of all previous events in that PA.
        // An interruption in the PA is indicated with "."
        // To avoid double-counting, if there's a ".", we only take the sequence to the right of
        // the right-most "." (Should I end that sentence with a period? How does that work)
        let trimmed_sequence =
            if let Some((_, s)) = str_sequence.rsplit_once(".") { s }
            else { str_sequence };
        let mut char_iter = trimmed_sequence.chars().peekable();
        let mut pitch = PitchSequenceItem::new(1);

        let get_catcher_pickoff_base = { |c: Option<char>|
            Base::from_str(&c.unwrap_or('.').to_string()).ok()
        };

        while let Some(c) = char_iter.next() {
            match c {
                // Tokens indicating info on the upcoming pitch
                '*' =>  {pitch.update_blocked_by_catcher(); continue}
                '>' => {pitch.update_runners_going(); continue}
                _ => {}
            }
            let pitch_type: Result<PitchType> = PitchType::from_str(&c.to_string()).context("Bad pitch type");
            // TODO: Log this as a warning once I implement logging
            pitch_type.map(|p|{pitch.update_pitch_type(p)}).ok();

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
                    if let Some('+') = speculative_iter.nth(1) {
                        pitch.update_catcher_pickoff(get_catcher_pickoff_base(char_iter.nth(2)))
                    }
                }
                Some('+') => {
                    pitch.update_catcher_pickoff(get_catcher_pickoff_base(char_iter.nth(1)))
                }
                _ => {}
            }
            let final_pitch = pitch;
            pitch = PitchSequenceItem::new(final_pitch.sequence_id.get() + 1);
            pitches.push(final_pitch);
        }
        Ok(Self(pitches))
    }
}



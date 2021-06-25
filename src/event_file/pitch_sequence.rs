use std::str::FromStr;
use std::convert::TryFrom;
use serde::{Serialize, Deserialize};

use anyhow::{Error, Result, Context};
use strum_macros::EnumString;

use crate::event_file::play::Base;
use std::ops::Deref;


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
    BallOnPitcherGoingToMouth,
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
            PitchType::BallOnPitcherGoingToMouth => SequenceItemTypeGeneral::Ball,
            PitchType::InPlay => SequenceItemTypeGeneral::InPlay,
            PitchType::InPlayOnPitchout => SequenceItemTypeGeneral::InPlay,
        }
    }
}


impl Default for PitchType {
    fn default() -> Self { PitchType::Unknown }
}

#[derive(Debug, PartialEq, Eq, Default, Copy, Clone, Serialize, Deserialize)]
pub struct PitchSequenceItem {
    pub pitch_type: PitchType,
    pub runners_going: bool,
    pub blocked_by_catcher: bool,
    pub catcher_pickoff_attempt: Option<Base>
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
        let mut char_iter = str_sequence.chars().peekable();
        let mut pitch = PitchSequenceItem::default();

        let get_catcher_pickoff_base = { |c: Option<char>|
            Base::from_str(&c.unwrap_or('.').to_string()).ok()
        };

        // TODO: Maybe try implementing in nom? Not a priority tho
        loop {
            let opt_c = char_iter.next();
            if opt_c == None {break}
            let c = opt_c.unwrap().to_string();
            match c.deref() {
                // Tokens indicating info on the upcoming pitch
                "*" =>  {pitch.update_blocked_by_catcher(); continue}
                ">" => {pitch.update_runners_going(); continue}
                _ => {}
            }
            let pitch_type: Result<PitchType> = PitchType::from_str(c.deref()).context("Bad pitch type");
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
                Some('+') => {pitch.update_catcher_pickoff(get_catcher_pickoff_base(char_iter.nth(1)))}
                _ => {}
            }
            let final_pitch = pitch;
            pitch = PitchSequenceItem::default();
            pitches.push(final_pitch);
        }
        Ok(Self(pitches))
    }
}



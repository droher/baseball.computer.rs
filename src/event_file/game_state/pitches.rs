use std::sync::Arc;

use crate::event_file::pitch_sequence::{ParsedPitchSequence, PitchSequence};
use anyhow::{Context, Result};
use serde::{Deserialize, Serialize};

use super::{Event, EventId};
use crate::event_file::traits::SequenceId;

#[derive(Debug, Eq, PartialEq, Clone, Copy, Serialize, Deserialize)]
pub enum PitchSequenceStatus {
    Resolved,
    Unavailable,
    Unresolved,
}

#[derive(Debug, Eq, PartialEq, Clone, Serialize, Deserialize, Hash)]
pub enum PitchSequenceConflictReason {
    TokenMismatch,
    CatcherPickoffConflict,
    AmbiguousPickoffReplay,
}

#[derive(Debug, Eq, PartialEq, Clone, Serialize)]
pub struct PitchSequenceIssue {
    pub reason: PitchSequenceConflictReason,
    pub prior_event_id: Option<EventId>,
    pub prior_raw_pitch_sequence: Arc<str>,
    pub current_raw_pitch_sequence: Arc<str>,
}

struct OwnedPitch {
    token: char,
    event: usize,
    pitch: usize,
    omitted: bool,
}

#[derive(Default)]
struct AppearancePitches {
    history: Vec<OwnedPitch>,
    previous: Option<usize>,
}

impl AppearancePitches {
    fn reconcile(
        &mut self,
        parsed: &ParsedPitchSequence,
        outputs: &mut [PitchSequence],
    ) -> std::result::Result<usize, PitchSequenceConflictReason> {
        let mut current = 0;
        let mut retained = self.history.len();
        for (index, prior) in self.history.iter_mut().enumerate() {
            let token = parsed.tokens.get(current).copied();
            if token == Some(prior.token) {
                if prior.omitted {
                    return Err(PitchSequenceConflictReason::AmbiguousPickoffReplay);
                }
                let original = &mut outputs[prior.event][prior.pitch];
                let incoming = parsed.pitches[current];
                match (
                    original.catcher_pickoff_attempt,
                    incoming.catcher_pickoff_attempt,
                ) {
                    (Some(before), Some(after)) if before != after => {
                        return Err(PitchSequenceConflictReason::CatcherPickoffConflict);
                    }
                    (None, value) => original.catcher_pickoff_attempt = value,
                    _ => {}
                }
                original.runners_going |= incoming.runners_going;
                original.blocked_by_catcher |= incoming.blocked_by_catcher;
                current += 1;
            } else if matches!(prior.token, '1'..='3') && !matches!(token, Some('1'..='3')) {
                prior.omitted = true;
            } else if prior.token == 'N' {
                retained = index;
                break;
            } else {
                return Err(PitchSequenceConflictReason::TokenMismatch);
            }
        }
        if self.history[retained..]
            .iter()
            .any(|pitch| pitch.token != 'N')
        {
            return Err(PitchSequenceConflictReason::TokenMismatch);
        }
        self.history.truncate(retained);
        Ok(current)
    }

    fn append(
        &mut self,
        event: usize,
        parsed: &ParsedPitchSequence,
        start: usize,
        outputs: &mut [PitchSequence],
    ) -> Result<()> {
        for (token, pitch) in parsed.tokens[start..].iter().zip(&parsed.pitches[start..]) {
            let mut pitch = *pitch;
            let owner_pitch = outputs[event].len();
            pitch.sequence_id =
                SequenceId::new(owner_pitch + 1).context("Invalid event pitch sequence id")?;
            outputs[event].push(pitch);
            self.history.push(OwnedPitch {
                token: *token,
                event,
                pitch: owner_pitch,
                omitted: false,
            });
        }
        self.previous = Some(event);
        Ok(())
    }
}

fn resolve_appearance(events: &mut [Event]) -> Result<()> {
    let mut history = AppearancePitches::default();
    let mut outputs = vec![Vec::new(); events.len()];
    let mut unresolved = false;
    for current in 0..events.len() {
        if events[current].raw_pitch_sequence.is_empty() {
            continue;
        }
        let parsed = events[current].parsed_pitch_sequence.clone();
        let reconciliation = history.reconcile(&parsed, &mut outputs);
        let conflict = if parsed.annotation_conflicts.is_empty() {
            reconciliation.as_ref().err().cloned()
        } else {
            Some(PitchSequenceConflictReason::CatcherPickoffConflict)
        };
        let start = if let Some(reason) = conflict {
            let prior_event_id = history.previous.map(|index| events[index].event_id);
            let prior_raw_pitch_sequence = history.previous.map_or_else(
                || Arc::from(""),
                |index| events[index].raw_pitch_sequence.clone(),
            );
            let current_raw_pitch_sequence = events[current].raw_pitch_sequence.clone();
            events[current]
                .results
                .pitch_sequence_issues
                .push(PitchSequenceIssue {
                    reason,
                    prior_event_id,
                    prior_raw_pitch_sequence,
                    current_raw_pitch_sequence,
                });
            unresolved = true;
            history.history.clear();
            0
        } else {
            reconciliation.unwrap_or_default()
        };
        history.append(current, &parsed, start, &mut outputs)?;
    }
    let appearance_start = events[0].event_id;
    let status = if unresolved {
        PitchSequenceStatus::Unresolved
    } else if outputs.iter().all(Vec::is_empty) {
        PitchSequenceStatus::Unavailable
    } else {
        PitchSequenceStatus::Resolved
    };
    for (event, output) in events.iter_mut().zip(outputs) {
        event.results.pitch_sequence = Arc::new(if unresolved { Vec::new() } else { output });
        event.results.pitch_sequence_status = status;
        event.results.pitch_sequence_appearance_start = appearance_start;
    }
    Ok(())
}

pub(super) fn resolve_pitch_histories(events: &mut [Event]) -> Result<()> {
    let mut start = 0;
    for index in 0..events.len() {
        let event = &events[index];
        let boundary = event.results.plate_appearance.is_some()
            || event.context.outs.get() + event.results.out_on_play.len() >= 3
            || events.get(index + 1).is_none_or(|next| {
                next.context.inning != event.context.inning
                    || next.context.batting_side != event.context.batting_side
            });
        if boundary {
            resolve_appearance(&mut events[start..=index])?;
            start = index + 1;
        }
    }
    Ok(())
}

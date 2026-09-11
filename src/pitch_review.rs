use std::collections::HashSet;
use std::sync::LazyLock;

use crate::event_file::game_state::EventId;
use anyhow::{Result, anyhow, bail};
use serde::Deserialize;

use crate::event_file::game_state::{GameContext, PitchSequenceConflictReason, PitchSequenceIssue};

#[derive(Debug, Eq, PartialEq, Hash, Deserialize)]
struct ReviewedPitchSequenceConflict {
    game_id: String,
    event_id: usize,
    appearance_start_event_id: usize,
    prior_event_id: Option<usize>,
    reason: PitchSequenceConflictReason,
    prior_raw_pitch_sequence: String,
    current_raw_pitch_sequence: String,
}

static REVIEWED_CONFLICTS: LazyLock<Result<HashSet<ReviewedPitchSequenceConflict>>> =
    LazyLock::new(|| {
        serde_json::from_str(include_str!(
            "../docs/pitch_sequence_reviewed_conflicts.json"
        ))
        .map_err(Into::into)
    });

fn is_reviewed(
    reviewed_conflicts: &HashSet<ReviewedPitchSequenceConflict>,
    game_id: &str,
    event_id: usize,
    appearance_start_event_id: usize,
    issue: &PitchSequenceIssue,
) -> bool {
    reviewed_conflicts.contains(&ReviewedPitchSequenceConflict {
        game_id: game_id.to_owned(),
        event_id,
        appearance_start_event_id,
        prior_event_id: issue.prior_event_id.map(EventId::get),
        reason: issue.reason.clone(),
        prior_raw_pitch_sequence: issue.prior_raw_pitch_sequence.to_string(),
        current_raw_pitch_sequence: issue.current_raw_pitch_sequence.to_string(),
    })
}

pub fn check_pitch_sequence_conflicts(
    game_context: &GameContext,
    audit_pitch_conflicts: bool,
) -> Result<()> {
    let reviewed_conflicts = REVIEWED_CONFLICTS
        .as_ref()
        .map_err(|error| anyhow!("Failed to parse reviewed pitch conflicts: {error}"))?;
    if audit_pitch_conflicts {
        return Ok(());
    }
    for event in &game_context.events {
        for issue in &event.results.pitch_sequence_issues {
            if !is_reviewed(
                reviewed_conflicts,
                game_context.game_id.id.as_str(),
                event.event_id.get(),
                event.results.pitch_sequence_appearance_start.get(),
                issue,
            ) {
                bail!(
                    "Unreviewed pitch sequence conflict in {} event {}: {:?}, {:?} -> {:?}",
                    game_context.game_id.id,
                    event.event_id,
                    issue.reason,
                    issue.prior_raw_pitch_sequence,
                    issue.current_raw_pitch_sequence
                );
            }
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use super::*;

    fn issue(
        reason: PitchSequenceConflictReason,
        current_raw_pitch_sequence: &str,
    ) -> PitchSequenceIssue {
        PitchSequenceIssue {
            reason,
            prior_event_id: Some(EventId::new(3).unwrap_or_else(|| panic!("valid event ID"))),
            prior_raw_pitch_sequence: Arc::from("BC"),
            current_raw_pitch_sequence: Arc::from(current_raw_pitch_sequence),
        }
    }

    fn reviewed_conflicts() -> HashSet<ReviewedPitchSequenceConflict> {
        [ReviewedPitchSequenceConflict {
            game_id: "BOS202404010".to_owned(),
            event_id: 4,
            appearance_start_event_id: 3,
            prior_event_id: Some(3),
            reason: PitchSequenceConflictReason::TokenMismatch,
            prior_raw_pitch_sequence: "BC".to_owned(),
            current_raw_pitch_sequence: "BCS".to_owned(),
        }]
        .into()
    }

    #[test]
    fn registry_requires_an_exact_conflict_match() {
        let reviewed = reviewed_conflicts();
        assert!(is_reviewed(
            &reviewed,
            "BOS202404010",
            4,
            3,
            &issue(PitchSequenceConflictReason::TokenMismatch, "BCS"),
        ));
        assert!(!is_reviewed(
            &reviewed,
            "BOS202404010",
            4,
            3,
            &issue(PitchSequenceConflictReason::TokenMismatch, "BCT"),
        ));
        assert!(!is_reviewed(
            &reviewed,
            "BOS202404010",
            4,
            3,
            &issue(PitchSequenceConflictReason::CatcherPickoffConflict, "BCS"),
        ));
        assert!(!is_reviewed(
            &reviewed,
            "BOS202404010",
            4,
            4,
            &issue(PitchSequenceConflictReason::TokenMismatch, "BCS"),
        ));
    }
}

# Pitch-sequence status downstream migration

The parser's pitch-history quarantine adds two source tables while preserving all existing game, event, and nonpitch output. `event_pitch_sequences` contains normalized rows only for resolved appearances. Downstream publication must wait until the status tables are ingested and every pitch aggregate treats unavailable or unresolved data as unknown rather than zero.

## Source tables

`event_pitch_sequence_status` has one row per event:

| Column | Downstream type | Requirement |
| --- | --- | --- |
| `game_id` | `GAME_ID` | Joins to `stg_games` |
| `event_id` | `UTINYINT` | Event-local identifier |
| `event_key` | `UINTEGER` | Unique grain and join to `stg_events` |
| `appearance_start_event_id` | `UTINYINT` | Groups every event in the same appearance |
| `status` | `VARCHAR` | `Resolved`, `Unavailable`, or `Unresolved` |
| `raw_pitch_sequence` | `VARCHAR` | Exact Retrosheet field, including an empty string |

`event_pitch_sequence_issues` has zero or more diagnostic rows:

| Column | Downstream type | Requirement |
| --- | --- | --- |
| `game_id` | `GAME_ID` | Joins to `stg_games` |
| `event_id` | `UTINYINT` | Event carrying the issue |
| `event_key` | `UINTEGER` | Joins to `stg_events` |
| `appearance_start_event_id` | `UTINYINT` | Appearance quarantine key |
| `sequence_id` | `UTINYINT` | Issue order within the event |
| `reason` | `VARCHAR` | Structured diagnostic reason |
| `prior_event_id` | `UTINYINT` | Nullable when no prior event exists |
| `prior_raw_pitch_sequence` | `VARCHAR` | Exact prior Retrosheet field, including empty |
| `current_raw_pitch_sequence` | `VARCHAR` | Exact current Retrosheet field, including empty |

`bin/parquet.py` emits both Parquet schemas directly. It disables string-null inference for these tables so empty raw strings survive. Numeric empty fields such as `prior_event_id` remain null. The issues Parquet is emitted with this schema even when its CSV is zero bytes.

## Required baseball.computer changes

Add both external tables to `bc/external_models.yaml`. Add `main_models.stg_event_pitch_sequence_status` and `main_models.stg_event_pitch_sequence_issues` with their declared grains, date and season derivation where useful, accepted status values, and relationships to `stg_games` and `stg_events`. Keep status as `VARCHAR`; an accepted-values audit supplies the required domain check without adding another DuckDB enum lifecycle.

Make `event_pitch_sequence_stats` status-driven. It must emit normalized pitch counters only when the event status is `Resolved`. A resolved appearance may have a real zero. `Unavailable` and `Unresolved` counters must be null. Carry a separate `pitch_sequence_resolution_status` column through event, player-game, player-season, and modeling outputs.

The normalized counter set is `pitches`, `swings`, `swings_with_contact`, `strikes`, `strikes_called`, `strikes_swinging`, `strikes_foul`, `strikes_foul_tip`, `strikes_in_play`, `strikes_unknown`, `balls`, `balls_called`, `balls_intentional`, `balls_automatic`, `unknown_pitches`, `pitchouts`, `pitcher_pickoff_attempts`, `catcher_pickoff_attempts`, `pitches_blocked_by_catcher`, and `pitches_with_runners_going`. Do not null `passed_balls`, `wild_pitches`, or `balks`; those are derived independently from baserunner plays and are part of the preserved nonpitch record.

Remove blanket zero filling for the normalized counter set in `event_offense_stats` and `event_pitching_stats`. At player-game and player-season grain, do not publish a partial `SUM` when any contributing appearance is unavailable or unresolved. Emit null for the normalized totals and propagate the worst status, with `Unresolved` taking precedence over `Unavailable` and `Unavailable` over `Resolved`.

Join event status into `event_observation_pitch`. For unresolved pitch dimensions, keep normalized `raw_value` null, set `observed_status` and `source_acquisition_status` to `contradicted`, and set `model_input_eligible` false. The exact Retrosheet text remains in the staged status and issue models; it must not be mixed with the normalized enum-name encoding used by observation `raw_value`. Map unavailable dimensions to missing/null while retaining `pitch_sequence_resolution_status`. Only a resolved empty sequence or pitch count represents a trusted zero.

Expose the resolution status in `event_completeness_pitches`, game completeness, and player completeness. A game containing an unresolved appearance must not be classified as ordinary sparse pitch coverage in `source_acquisition_ledger`; classify its pitch source as contradicted and ineligible for event imputation or aggregate constraints.

## Required validation gates

Before publication, verify all of these invariants:

- Every event has exactly one status row, and every status/issue row joins to its game and event.
- All events sharing `(game_id, appearance_start_event_id)` have the same status.
- Every normalized pitch row joins to `Resolved` status.
- An unresolved appearance has no normalized pitch rows for any of its events.
- Conflict evidence and exact prior/current strings survive CSV-to-Parquet conversion.
- Resolved empty sequences produce trusted zero; unavailable and unresolved sequences produce null normalized counters.
- Unresolved quarantine leaves event identity, play results, baserunning, fielding, lineup, score, comments, and raw play strings unchanged.
- Mixed-status player-game and player-season groups never publish partial normalized pitch totals as complete totals.
- Unresolved observation rows are contradicted, ineligible for modeling, and distinguishable from ordinary missing coverage.

SQLMesh unit tests can cover the new staging and downstream models before the new external Parquet exists remotely. Define YAML fixtures for the two external tables and their direct dependencies, then run `sqlmesh test` against those fixtures. A local integration plan can use a temporary clone of `bc.db` with the two source tables precreated from fixture Parquet and temporary `BC_DB_PATH` and `BC_STATE_DB_PATH` values.

After these changes and gates pass, regenerate the full parser CSV and Parquet outputs with unchanged full-corpus file ordering, rebuild the affected SQLMesh models and their downstream consumers, and only then publish the refreshed database. A season-only run has different `event_key` values and cannot replace the full source tables. This migration does not authorize or perform a production rebuild or publication.

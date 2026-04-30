# Output schema reference

The parser writes one CSV per variant of `EventFileSchema`
(`src/main.rs`). `bin/parquet.py` and `bin/simple_files.py` then convert
those CSVs into the Parquet files consumed by the
[`baseball.computer`](https://github.com/droher/baseball.computer) dbt
project.

This document is the contract between the Rust parser and the dbt
sources. Each section names one variant; the row struct (or pair of
structs, for the box-score variants) is the canonical column-level
source of truth — clippy-deny enforces it stays consistent.

**Filename pattern:** `{schema}.csv`, where `{schema}` is the strum
`snake_case` rendering of the variant. Renaming a variant renames the
downstream Parquet table and breaks the dbt sources, so coordinate
before doing it.

**Event key namespacing:** Every play-by-play row carries an
`event_key` that is globally unique across the corpus
(`(file_index + i) * EVENT_KEY_BUFFER` — see
`src/event_file/traits.rs`). Parquet stores the key
`DELTA_BINARY_PACKED` after sorting.

## Variants

The 28 variants fall into six groups.

## Play-by-play, game-level

### `games`
Game metadata, weather, umpire assignments, decision pitchers, scorer
provenance. One row per PBP game.
Source: `Games` in `src/event_file/schemas.rs`.

### `game_lineup_appearances`
Each batting-order slot a player held during the game, with start/end
event ids. One row per lineup tenure.
Source: `GameLineupAppearance` in `src/event_file/game_state.rs`.

### `game_fielding_appearances`
Each fielding-position tenure, with start/end event ids. One row per
fielding tenure.
Source: `GameFieldingAppearance` in `src/event_file/game_state.rs`.

### `game_earned_runs`
Per-pitcher earned-run totals from the `data,er,...` records. One row
per pitcher who has an earned-run record.
Source: `GameEarnedRuns` in `src/event_file/schemas.rs`.

## Play-by-play, event-level

All event-level rows share an `event_key` that joins back to `events`.

### `events`
The headline event row: inning, frame, batter, pitcher, count, base
state, plate-appearance result, batted-ball description. One row per
event.
Source: `Events` in `src/event_file/schemas.rs`.

### `event_audit`
Per-event audit trail: line number in the original `.EV*` file, raw
play string, account type, file index. Used for traceability and
debugging, not analytic joins.
Source: `EventAudit` in `src/event_file/schemas.rs`.

### `event_baserunners`
One row per baserunner per event (including the batter), with starting
base, attempted advance, success/out, scored/RBI flags, error flags.
Source: `EventBaserunners` in `src/event_file/schemas.rs`.

### `event_fielding_play`
One row per fielder credited with an action (assist, putout, error) on
the event.
Source: `EventFieldingPlays` in `src/event_file/schemas.rs`.

### `event_pitch_sequences`
One row per pitch in the event's pitch sequence, including pitch type,
catcher pickoff target, runners-going flag, and blocked-by-catcher
flag.
Source: `EventPitchSequences` in `src/event_file/schemas.rs`.

### `event_flags`
Free-form play-info flags emitted by the play parser (modifiers,
unusual events). One row per flag.
Source: `EventFlag` in `src/event_file/game_state.rs`.

### `event_comments`
Comments (`com,...` records) attached to the following event. One row
per comment line.
Source: `EventComments` in `src/event_file/schemas.rs`.

## Box-score, game-level

### `box_score_games`
Same shape as `games` but populated from box-score (`.EB*`) files for
games where no PBP exists.
Source: `Games` in `src/event_file/schemas.rs` (shared with `games`).

### `box_score_line_scores`
Inning-by-inning runs from the `line,...` records. One row per
team-inning.
Source: `BoxScoreLineScores` in `src/event_file/schemas.rs`.

## Box-score, line stats

These six tables come from `stat,bline|pline|dline|...` records. They
go through the dynamic-header path (`generate_header` on
`BoxScoreWritableRecord`) — the schema is derived from the first row
written, not declared up front.

### `box_score_batting_lines`
One batter's box-score batting stats. Source: `BattingLine` in
`src/event_file/box_score.rs`.

### `box_score_pitching_lines`
One pitcher's box-score pitching stats. Source: `PitchingLine`.

### `box_score_fielding_lines`
One fielder-position's box-score fielding stats. Source: `DefenseLine`.

### `box_score_pinch_hitting_lines`
Pinch-hitter line stats. Source: `PinchHittingLine`.

### `box_score_pinch_running_lines`
Pinch-runner line stats. Source: `PinchRunningLine`.

### `box_score_team_miscellaneous_lines`
Team-level miscellaneous stats (LOB, DP, etc.). Source:
`TeamMiscellaneousLine`.

### `box_score_team_batting_lines`
Team-level batting totals. Source: `TeamBattingLine`.

### `box_score_team_fielding_lines`
Team-level fielding totals. Source: `TeamDefenseLine`.

## Box-score, event lines

These come from `event,...` records — tabular descriptions of specific
play categories that are not a full play-by-play.

### `box_score_double_plays`
Double-play participants. Source: `FieldingPlayLine` in
`src/event_file/box_score.rs`.

### `box_score_triple_plays`
Triple-play participants. Source: `FieldingPlayLine` (same struct, kept
in a distinct table for downstream clarity).

### `box_score_hit_by_pitches`
HBP details (batter, pitcher, count). Source: `HitByPitchLine`.

### `box_score_home_runs`
Home-run details (batter, pitcher, inning, runners on, distance when
known). Source: `HomeRunLine`.

### `box_score_stolen_bases`
Stolen-base events (runner, base, pitcher, catcher).
Source: `StolenBaseAttemptLine`.

### `box_score_caught_stealing`
Caught-stealing events. Source: `StolenBaseAttemptLine` (same struct).

## Box-score, comments

### `box_score_comments`
Free-form comments attached to a box-score game.
Source: `BoxScoreComments` in `src/event_file/schemas.rs`.

## Adding a new variant

1. Add the variant to `EventFileSchema` in `src/main.rs`.
2. Add the row struct in `src/event_file/schemas.rs`.
3. Implement `ContextToVec<'_>` (for play-by-play rows) **or** wire a
   custom write path in `write_box_score_files` /
   `write_play_by_play_files` for box-score rows.
4. Add a section here, named for the snake-case `{schema}` token.
5. Coordinate the column-level changes with the `baseball.computer`
   dbt project before merging.

## Renaming columns

Treat output column names as a public API. Coordinate with the dbt
project before renaming. Type-narrowing changes (e.g. widening a `u8`
to `u32`) generally do not break consumers; renames and removals do.

# retrosheet.pest design notes

## Provenance tags
Every rule has a comment immediately above it tagging its source:

- `@spec`      — directly from retrosheet.org/eventfile.htm. Cite section.
- `@derived`   — logically implied by the spec.
- `@practical` — observed in real Retrosheet files, not in docs.
- `@quirk`     — the existing Rust parser interprets differently than the spec; cross-ref `docs/divergences.md`.

## Whitespace
Retrosheet plays and records contain no inter-token whitespace. We never
define a `WHITESPACE` rule, so Pest auto-inserts nothing. Atomic and
compound-atomic rules also disable any auto-insertion locally:

- `@{ ... }` — atomic. The rule emits one `Pair` and suppresses children.
- `${ ... }` — compound atomic. No whitespace insertion, but children are
  still walkable from Rust.

The structural rules whose children we walk in tests (`play`, `main_play`,
record-level rules) are compound atomic; every leaf rule is atomic. Records
are line-oriented, so the grammar matches line breaks explicitly via
`line_terminator` (`\r\n` | `\n` | EOI) rather than relying on whitespace.

## File framing (Phase 2a)
A file starts with an `id` record and is followed by zero or more lines of
`version` / `info` / `start` / `sub` / `play` / `com` / `data` / `stat` /
`line` / `event` / `badj` / `padj` / `ladj` / `radj` / `presadj`. Blank lines
are allowed and silenced.

## Record bodies (Phases 2b–2h)
Per-record-type tightening. Each rule's comment in `retrosheet.pest` carries
the cross-reference to the legacy parser site that motivates the shape.

- **2b — info**: typed key/value pairs. Strict where the legacy parser is
  strict (`date`, `number`, enum keywords, bool, signed int); opaque where it
  tolerates anything (`starttime`, `inputtime`, player/person/umpire IDs).
  Quoted values with embedded commas (`info,scorer,"71,215"`) accepted.
- **2c — start/sub**: shared `appearance_body` — `PLAYER_ID,NAME,SIDE,LINEUP,POSITION`.
  `SIDE` is `0`/`1`, `LINEUP` is `0`–`9`, `POSITION` is `0`–`12` (with two-
  digit forms ordered first). Trailing whitespace on `POSITION` tolerated to
  mirror `record[5].trim_end()`.
- **2d — play**: typed framing of `play,INNING,SIDE,BATTER,COUNT,PITCHES,PLAY`.
  The trailing `PLAY` field embeds Phase 1's grammar via the silent
  `play_inner` rule, so play tokens flatten into the parse tree under
  `play_record` directly.
- **2e — pitch sequence**: `play_pitches` parses the inner stream as
  `pitch_token*` — ordered choice of `pickoff_token` (`+1`/`+2`/`+3`/`+H`),
  `pitch_flag_token` (`*` blocked-by-catcher, `>` runners-going),
  `pa_break_token` (`.`), and `pitch_char_token` (any other single char).
  Mirrors `PitchSequenceItem::new_pitch_sequence`.
- **2f — com**: opaque `com,TEXT` (legacy parser stores the text as-is).
  Quoted forms are common in modern fixtures.
- **2g — data / stat / line / event**:
  - `data,er,PITCHER_ID,EARNED_RUNS` (only `er` is recognized).
  - `stat,SUB_TAG,...` with `SUB_TAG` ∈ {`bline`, `phline`, `prline`, `pline`,
    `dline`, `tline`, `btline`, `dtline`}; field counts vary by sub-tag and
    are validated downstream by the per-line struct.
  - `line,SIDE,RUN1,RUN2,...` — non-negative integer inning runs.
  - `event,SUB_TAG,...` with `SUB_TAG` ∈ {`dpline`, `tpline`, `hpline`,
    `hrline`, `sbline`, `csline`}.
- **2h — adjustments**: each tag has its own typed body —
  `presadj,PITCHER_ID,BASERUNNER` (`B`/`1`/`2`/`3`),
  `radj,RUNNER_ID,BASE` (`1`/`2`/`3`/`H`),
  `badj`/`padj`,`PLAYER_ID,HAND` (`L`/`R`),
  `ladj,SIDE,LINEUP_POSITION`.

### `#` / `!` strip and unknown-fielder rewrite (`@quirk`)
`STRIP_CHARS_REGEX` at `src/event_file/play.rs:39` strips `[#! ]` from raw
plays before parsing, and `UNKNOWN_FIELDER_REGEX` rewrites `?` and `999+`
to `0`. Both are handled directly by the grammar: the silent
`play_strip = _{ ( "#" | "!" | " " )* }` rule is threaded between every
concatenation in play body rules so internal `[#! ]` chars are consumed
at token boundaries, and `fielder = @{ "?" | "9"{2,} | '0'..'9' }` accepts
the unknown-fielder forms inline (ordered choice; `99+` greedy collapses
any 2+ run of `9`s into a single fielder). The catchall rules
(`unrecognized_modifier`, `unrecognized_advance_modifier`) still capture
embedded strip chars into their spans — the integration shim does a
one-line `as_str().replace([…], "")` when constructing the legacy
`Unrecognized(...)` enum. See `docs/divergences.md`.

## Phase 3 corpus validation
`cargo run -p retrosheet-grammar --release --bin crosscheck-files -- retrosheet`
parses 423,087 / 423,087 games clean. `crosscheck-plays` parses 17,886,011
plays (231,738 unique) with zero failures. Every divergence between the
grammar and `eventfile.htm` has a tracked decision in `docs/divergences.md`.

## Quirk lint pass (`src/quirks.rs`)
`scan_quirks(pairs)` walks a successful parse tree and returns one `QuirkHit`
per `@quirk` rule match (with line/column and matched span). Lets callers
log/upgrade-to-error specific tolerated shapes without re-running the
parse. The corpus harness emits per-kind counts; the Phase 4 integration
shim plumbs hits into the existing `tracing::warn!` channel so the
legacy parser's quirk telemetry survives the cutover.

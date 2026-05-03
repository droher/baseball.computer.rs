# Divergences from the Retrosheet spec

Catalog of `@quirk` sites in `grammar/retrosheet.pest`. Each entry names what
the eventfile.htm spec implies, what the Rust parser at
`src/event_file/play.rs` actually does, and why the grammar follows the
parser. Phase 1j ran the grammar against the entire local corpus
(17,886,011 plays / 231,738 unique strings, scanned via
`crates/retrosheet-grammar/src/bin/crosscheck.rs`); the divergences below are
what was needed to land at zero unique parse failures.

## 1. Empty modifier bodies

- **Spec**: a modifier is `/<keyword>` or `/<contact-description>`.
- **Parser**: `value.split('/').filter(|s| !s.is_empty())` silently drops
  empty segments (`src/event_file/play.rs:1769`), so `0//FL`, `S/INT/`, and
  bare `S/` all parse with one or zero modifiers.
- **Grammar**: `modifier = ${ "/" ~ modifier_body? }` — body optional.

## 2. Unrecognized modifier tokens

- **Spec**: a closed enum of modifier keywords, plus contact descriptions.
- **Parser**: anything that doesn't match a known keyword and isn't a contact
  description falls through to `PlayModifier::Unrecognized(value.into())`
  (`parse_single_modifier` at `src/event_file/play.rs:1796`). The corpus has
  thousands of these (`/BFDP`, `/U7R2`, `/G3SF`, ...).
- **Grammar**: ordered choice ends with a catchall `unrecognized_modifier =
  @{ ( !( "/" | "." ) ~ ANY )+ }`. Recognized alternatives are wrapped with
  a `&modifier_terminator` lookahead so a partial-match (e.g. trajectory `B`
  against `BFDP`) backtracks cleanly into the catchall.
- **Sub-case**: bare `/E` (no fielder digit) does not match
  `error_modifier = "E" ~ fielder` and falls through to the catchall, which
  matches the parser's behavior. Corpus seed
  `valid/071-error-mod-falls-to-unrecognized.txt` pins the case.

## 3. Unrecognized advance-modifier bodies

- **Spec**: parameterized advance modifiers like `(UR)`, `(E$)`, `(82)`.
- **Parser**: `RunnerAdvanceModifier::Unrecognized(...)` for anything outside
  the enumerated forms (`src/event_file/play.rs:1320`). Real corpus has
  things like `(4E2/OBS)`, `(74H)`, `(7-2)`, `(95463/INT)`,
  `(23412E5/INT)`.
- **Grammar**: same pattern as #2, with `unrecognized_advance_modifier =
  @{ ( !")" ~ ANY )+ }` as the catchall and a `&")"` lookahead on the
  recognized alternatives.

## 4. Unknown-fielder digit `0`

- **Spec**: fielders are 1–9.
- **Parser**: `UNKNOWN_FIELDER_REGEX = r"999*|\?"` (`src/event_file/play.rs:40`)
  rewrites `?` and runs of `999`+ to `0` before parsing, and
  `FieldingPosition::Unknown` is the 0 variant. So `0`, `0(1)/FO`, etc.
  appear as legitimate normalized inputs.
- **Grammar**: `fielder = @{ '0'..'9' }`.
- **Scope**: only `fielder` accepts `0`. The `runner` (paren contents on a
  putout group), `from_base`, `to_base`, and `base_tag` rules keep their
  spec ranges (`B`/`1`/`2`/`3` and `1`/`2`/`3`/`H`). The Rust parser's
  `BASERUNNING_PLAY_FIELDING_REGEX = r"[123H]"` agrees, and the corpus has
  no `(0)` runner annotations.

## 5. Trailing chain separator with no follow-on

- **Spec**: `+` and `;` join two plays at the top level.
- **Parser**: split-and-recurse drops the empty trailing segment
  (`Self::parse_main_play("", _)` returns `Ok(vec![])` at
  `src/event_file/play.rs:1091`), so `53+` parses as just `53`.
- **Grammar**: `chained_play = ${ chain_separator ~ main_play? }`.

## 6. Multi-digit hit-zone fielders on `S/D/T/H/HR/W`

- **Spec**: a hit specifies a single fielder digit (1–9) immediately after
  the play letter.
- **Parser**: takes the entire trailing digit run via
  `FieldingPosition::fielding_vec` and only uses the first element. Corpus
  has `D12`, `D13`, `T46`, `S78`, `W0`, etc.
- **Grammar**: `single = @{ "S" ~ fielder* }` and the same shape on the
  other hit/walk rules.

## 7. `/U` and `/R` allow interleaved `U` markers

- **Spec**: `R$` is a relay sequence of fielder digits.
- **Parser**: collects digits via `FieldingPosition::fielding_vec`, which
  ignores any non-digit characters. Real corpus has `R3U92`, `R6U7R2`,
  `U7R2`, etc., where the `U`s are unknown-fielder placeholders.
- **Grammar**: `relay_modifier = @{ "R" ~ ( fielder | "U" )* }` and
  `unknown_modifier = @{ "U" ~ fielder* }`.

## 8. Throw-suffix on advance-modifier bodies

- **Spec**: an inner advance modifier is one of `(UR)`/`(E$)`/`(82)`/etc.
- **Parser**: silently accepts trailing `/TH` / `/TH$` (the regex_split on
  `NUMERIC_REGEX` at `parse_single_advance_modifier` ignores it). Forms
  `(E4/TH)`, `(E6/TH1)`, `(THH)` appear.
- **Grammar**: `advance_error`, `advance_error_with_assists`, and
  `advance_putout` accept a `throw_suffix? = "/TH" ~ ("1"|"2"|"3"|"H")?`.

## 9. Throw-suffix on baserunning fielding info

- **Spec**: `CS$(throw-fielders[E$])`.
- **Parser**: `BASERUNNING_FIELDING_INFO_REGEX` at
  `src/event_file/play.rs:34` accepts the form, and the parser drops
  trailing `/TH` annotations. Corpus has `CS2(E1/TH)`, `CS3(E1/TH)`.
- **Grammar**: `baserunning_fielding_info` accepts an optional
  `throw_suffix?` before the closing paren.

# Corpus corrections

`bin/fetch_retrosheet.py` and `bin/patch_known_corpus_bugs.py` adjust the
Retrosheet corpus before parsing. This page covers what changed for the
October 2026 Retrosheet release. Single-game patches and their reasons
are in `bin/patch_known_corpus_bugs.py`.

## 1870s box scores restored from the previous corpus

Retrosheet's `1871box.zip`, `1872box.zip` and `1874box.zip` now contain only
the `TEAM` file. The `.EBN` files fetched in May 2026 are kept in
[`corpus_overrides/boxes/`](../corpus_overrides/boxes) and cover 537 box
scores. After downloading, the fetch checks every linked box year and every
override year for a box file written during that run. If one is missing, the
fetch copies in the matching override and logs a warning. If no override
exists, the fetch exits nonzero, so CI stops before publishing.

## NLB box-score corrections

The October 2026 `ngl_b/*.EBR` files contain 757 records that the parser
rejects. Without corrections, the parser would drop each of them with an
`ERROR` log. `bin/ngl_box_corrections.py` fixes the 736 records
whose correct value can be derived from the same game's other records.
[`ngl_box_corrections.csv`](ngl_box_corrections.csv) lists every one with its
source file, original line number, game, rule, and before/after text. An
empty `after` means the line was removed. The other 21 records are in games
fixed by [reviewed overrides](#reviewed-game-overrides).

| Rule | Count | Correction |
| --- | ---: | --- |
| `trailing_line_score_placeholder` | 256 | Remove a trailing empty or `x` inning from `line`. The remaining innings must add up to the side's `bline` runs. |
| `drop_validator_message` | 136 | Remove `NUMBER OF ... DISAGREE BETWEEN ...` lines left in the files by Retrosheet's own validator. |
| `placeholder_hpline` | 91 | Remove `event,hpline,NA,NA,NA`, which names no side, pitcher, or batter. HBP totals remain in `bline`/`pline`. |
| `blank_starter_position` | 91 | Fill an empty `start` fielding position from the player's first `dline` position. |
| `multi_position_starter` | 74 | Replace a multi-position code such as `79` with the player's first `dline` position. That position must be one of the listed digits. |
| `unknown_bline_sequence` | 37 | Replace `NA` in the `bline` sequence column with the row's order within its batting slot. Every other row in the slot must already match its order. |
| `placeholder_hrline` | 24 | Remove `event,hrline` rows whose side, batter, and pitcher are all unknown. |
| `unknown_pline_sequence` | 13 | Replace `NA` in the `pline` sequence column with the pitcher's order for that side. |
| `misfiled_sub_line` | 4 | Change `event,phline`/`event,prline` to `stat,phline`/`stat,prline`. |
| `unknown_hrline_side` | 3 | Set an unknown `hrline` batting side from the batter's `bline` side. |
| `swapped_phline_inning_side` | 3 | Swap the inning and side columns of a `phline` whose side is not 0 or 1. |
| `unknown_dline_sequence` | 2 | Replace `NA` in the `dline` sequence column with the row's order for that player. |
| `mistyped_id_record` | 1 | Change `Yid,NY5193707052` to `id,NY5193707052`. Without this fix, that game merges into the previous one. |
| `unknown_fielding_play_side` | 1 | Set an unknown `dpline` side from the fielders' shared side. |

## Reviewed game overrides

The October 2026 release had 28 NLB box-score games that could not be
published as-is. The parser dropped 17 of them: 14 had a record it could not
parse, and 3 listed one player twice in a side's starting lineup. The other 11
parsed, but a batter had fewer hits than extra-base hits, and the
`baseball.computer` release build excluded them.

Each game now has a reviewed replacement in
[`corpus_overrides/games/ngl_b/`](../corpus_overrides/games/ngl_b):
`<GAMEID>.EBR` is the corrected game record, and `<GAMEID>.diff` explains the
evidence for each change, followed by the diff from the published record after
the rule-based corrections above. `bin/patch_known_corpus_bugs.py` applies
each override after those corrections, and only when the published record
still produces exactly that diff. If Retrosheet changes one of these games, the
script logs `SKIP upstream-changed` and exits nonzero, so CI stops until the
override is reviewed against the new record. To regenerate the diffs after
editing an override, run the patch script with `--game-overrides` pointing to
an empty directory on a fresh corpus, then
`uv run python bin/refresh_game_override.py <that_corpus> corpus_overrides/games/ngl_b/*.EBR`,
which keeps each explanation. With the overrides applied, the
NLB corrections report no unresolved records and the parser drops none of
these games.

Each correction is derived from the game's other records: `bline`/`dline`
lineups, pitching-line totals, `hrline` events, runs, and the line score.
Where a batter's hit total was lower than the batter's extra-base hits, the hit total was
kept when it matched the opposing pitchers' hits allowed, and the extra-base
hit was removed (from the pitcher's line too, when it was recorded there).
When no record said which extra-base hit was wrong, the double was removed.
Two corrections are judgment calls rather than derivations: PH5193408270
assigns the second "Wright" to Henry Wright, and HOM193607122 places Ray
Brown at catcher.

| Game | File | Correction |
| --- | --- | --- |
| CB2193307092 | `ngl_b/1933.EBR` | Rebuild home `start` records for slots 8–9 from `bline`/`dline` (Dial, Hughes) |
| DI1193307232 | `ngl_b/1933.EBR` | Rewrite shifted `hpline`: Roy Williams hit Anderson Pryor |
| PTC193307042 | `ngl_b/1933.EBR` | Remove Charleston's double (hit total and `hrline` home run stand) |
| NW1193408192 | `ngl_b/1934.EBR` | Assign the leadoff left fielder to Buddy Burbage instead of a second Bert Johnson |
| PH5193408270 | `ngl_b/1934.EBR` | Assign the slot 8 right fielder to Henry Wright instead of a second Bill Wright |
| BRN193505261 | `ngl_b/1935.EBR` | Remove one of Dixon's two doubles |
| BRN193507211 | `ngl_b/1935.EBR` | Remove N. Williams's double |
| BRN193507212 | `ngl_b/1935.EBR` | Remove Bill Wright's double; move the `hrline` from Zollie to Bill Wright |
| CAG193506021 | `ngl_b/1935.EBR` | Remove Harris's double; give Marshall the hit for his home run |
| CUS193507042 | `ngl_b/1935.EBR` | Slot 8 starter is Abreu at second base, not Noval |
| NW1193507141 | `ngl_b/1935.EBR` | Remove two of N. Williams's three doubles |
| NW1193507142 | `ngl_b/1935.EBR` | Rebuild home `start` records for slots 7–8 (Barker, Miller) |
| NY6193505262 | `ngl_b/1935.EBR` | Rebuild visiting `start` records from `bline`/`dline` |
| NY6193507212 | `ngl_b/1935.EBR` | Rebuild home `start` records for slots 8–9 (Martinez, Garcia) |
| PH5193506011 | `ngl_b/1935.EBR` | Rebuild home `start` records for slots 5–9 (adds Casey) |
| PH5193506030 | `ngl_b/1935.EBR` | Move a home run from Bill Wright to Zollie Wright, matching the `hrline` |
| PTC193509021 | `ngl_b/1935.EBR` | Benjamin moved from right to center; pitcher `dline` order follows `pline` |
| HOM193607122 | `ngl_b/1936.EBR` | Ray Brown's unknown position becomes the second catcher |
| HOM193607262 | `ngl_b/1936.EBR` | Rebuild home `start` records for slots 7–9 (Bassett, Jackson, Walker) |
| NY5193609072 | `ngl_b/1936.EBR` | Slot 9 starter is Longley in center field, not Porter |
| IN6193707200 | `ngl_b/1937.EBR` | Remove one of Pearson's two doubles |
| MEM193704251 | `ngl_b/1937.EBR` | Remove Davenport's double |
| NW2193708140 | `ngl_b/1937.EBR` | Remove Hayes's double |
| HOM193807031 | `ngl_b/1938.EBR` | Replace fielding positions in the `bline` sequence column with slot order |
| HOM193808283 | `ngl_b/1938.EBR` | Remove Clarkson's double |
| JAX193803270 | `ngl_b/1938.EBR` | Hardy starts at catcher; Cornelius's catcher `dline` is Hardy's |
| HOM193906101 | `ngl_b/1939.EBR` | Rewrite shifted `hrline`: Bill Wright off Partlow |
| NW2193907091 | `ngl_b/1939.EBR` | Rewrite two shifted `hrline`s: Bankhead off Day, Wells off Partlow |

The parser still rejects a game that lists one player twice in a side's
starting lineup, logging an `ERROR` and writing no output for it. A player is
listed twice when they are the first `bline` in two batting slots (in a
play-by-play game, when they have `start` records in two slots), or when they
have first-position `dline`s at two positions and no other player has a
first-position `dline` at either one. No game in the corrected October 2026
corpus is rejected.

## Other parser warnings

These are left as published:

- HOM194307080 (`ngl_b/1943.EBR`) has `info,visteam,HOM` and
  `info,hometeam,CVB` at site HAM02. The game ID names HOM as the home team.
  The parser keeps the `info` records.
- CI2194308030 (`ngl_e/1943.EVR`) and NY6194505230 (`ngl_e/1945.EVR`) each
  appear twice with identical records. The parser keeps one copy.

## Home runs with no pitcher

These games are not excluded. Each has an `hrline` with a batter but no
pitcher, and the pitching lines do not say which pitcher allowed it. The
records are not filled in from `pline` totals. The `baseball.computer` build
loads them and its `not_null` audit on `box_score_home_runs.pitcher_id`
warns.

| Game | File | Records |
| --- | --- | --- |
| AKR193307270 | `ngl_b/1933.EBR` | `event,hrline,1,palmc101,,-1,0,`; `event,hrline,1,huntb106,,-1,0,` |
| HOM193507280 | `ngl_b/1935.EBR` | `event,hrline,1,benjj101,,9,3,` |
| HOM193508100 | `ngl_b/1935.EBR` | `event,hrline,0,grifc102,,-1,0,` |
| NY6193505262 | `ngl_b/1935.EBR` | `event,hrline,0,carlm101,,-1,0,` |
| NY6193508181 | `ngl_b/1935.EBR` | `event,hrline,1,salal101,,-1,0,`; `event,hrline,1,thomd104,,-1,0,` |
| PH5193506222 | `ngl_b/1935.EBR` | `event,hrline,0,davis103,,-1,0,`; `event,hrline,0,browl107,,-1,0,`; `event,hrline,1,cread101,,-1,0,`; `event,hrline,1,seayd101,,-1,0,` |
| PH5193509222 | `ngl_b/1935.EBR` | `event,hrline,0,perkb101,,-1,0,` |
| PH5193609051 | `ngl_b/1936.EBR` | `event,hrline,1,parnr102,,5,0,`; `event,hrline,1,steat101,,9,0,`; `event,hrline,1,wilsj106,,9,0,` |
| HOM193705220 | `ngl_b/1937.EBR` | `event,hrline,1,willj110,,-1,0,`; `event,hrline,1,harrv102,,-1,0,` |
| HOM193706272 | `ngl_b/1937.EBR` | `event,hrline,1,leonb101,,5,0,` |
| HOM193708210 | `ngl_b/1937.EBR` | `event,hrline,1,gibsj101,,-1,0,` |
| PH5193709112 | `ngl_b/1937.EBR` | `event,hrline,1,paget101,,-1,0,` |
| SSA193705301 | `ngl_b/1937.EBR` | `event,hrline,0,steat101,,-1,0,` (twice); `event,hrline,0,justc101,,-1,0,`; `event,hrline,0,millp101,,-1,0,` |
| HOM193808283 | `ngl_b/1938.EBR` | `event,hrline,1,gibsj101,,5,0,`; `event,hrline,1,browr103,,8,0,` |
| NW2194507041 | `ngl_b/1945.EBR` | `event,hrline,1,willc107,,5,0,` |


## Pitch-sequence conflicts

The release adds pitch sequences, recorded from video, to CHN198606170 and
SLN197409100. Each contains one disagreement between cumulative records:
`C11C` then `C11B.MX`, and `1F1C` then `KKX`. Both fingerprints are in
[`pitch_sequence_reviewed_conflicts.json`](pitch_sequence_reviewed_conflicts.json),
which quarantines those appearances.

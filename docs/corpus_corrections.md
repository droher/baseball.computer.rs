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
empty `after` means the line was removed.

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

## Unresolved records

These records stay as published, and the parser still drops each one with an
`ERROR` log. None of them has a single correct value that can be derived
from the game's other records.

| Game | File | Record | Reason |
| --- | --- | --- | --- |
| CB2193307092 | `ngl_b/1933.EBR` | `start,brooa101,"Ameal Brooks",1,8,` | Only a second-position `dline` (7) |
| DI1193307232 | `ngl_b/1933.EBR` | `event,hpline,NA,,willr106` | willr106 has 0 HBP as a batter but the game's only hit batsman as a pitcher, so the fields are shifted |
| NW1193408192 | `ngl_b/1934.EBR` | `start,johnb111,...,1,1,79` and `...,1,4,79` | Same starter in two batting slots, with two first-position `dline`s (7, 9) |
| PH5193408270 | `ngl_b/1934.EBR` | `start,wrigb104,...,0,1,89` and `...,0,8,89` | Same starter in two batting slots, with two first-position `dline`s (8, 9) |
| CUS193507042 | `ngl_b/1935.EBR` | `start,novat101,"Tomas Noval",1,8,` | No `dline` |
| NW1193507142 | `ngl_b/1935.EBR` | `start,mccof102,"Frank McCoy",1,7,` | Only a second-position `dline` (2) |
| NY6193505262 | `ngl_b/1935.EBR` | `start,scalg101,...,0,2,`; `start,parkt101,...,0,4,` | No `dline` |
| NY6193507212 | `ngl_b/1935.EBR` | `start,dihim101,"Martin Dihigo",1,8,` | No `dline`; his `bline` is the second in slot 7 and he has a `phline` |
| PH5193506011 | `ngl_b/1935.EBR` | `start,holml101,"Lefty Holmes",1,9,` | Only a second-position `dline` (1) |
| PTC193509021 | `ngl_b/1935.EBR` | `start,benjj101,"Jerry Benjamin",0,1,29` | Two first-position `dline`s (2, 9) |
| HOM193607122 | `ngl_b/1936.EBR` | `stat,dline,browr103,1,1,?,...` | No `start` record |
| HOM193607262 | `ngl_b/1936.EBR` | `start,parkt101,"Tom Parker",1,7,` | Only a second-position `dline` (7) |
| NY5193609072 | `ngl_b/1936.EBR` | `start,porta103,"Andy Porter",0,9,` | Only a second-position `dline` (1) |
| HOM193807031 | `ngl_b/1938.EBR` | `stat,bline,bassp101,0,2,NA,...` | The game's sequence column holds fielding positions |
| JAX193803270 | `ngl_b/1938.EBR` | `start,hardp102,"Paul Hardy",0,8,` | No `dline` |
| HOM193906101 | `ngl_b/1939.EBR` | `event,hrline,partr102,,,wrigb104,0,` | Fields are shifted |
| NW2193907091 | `ngl_b/1939.EBR` | `event,hrline,banks101,day-l101,,0,0,`; `event,hrline,partr102,pearl102,,wellw101,1,` | Fields are shifted |

Other new parser warnings are left as published:

- HOM194307080 (`ngl_b/1943.EBR`) has `info,visteam,HOM` and
  `info,hometeam,CVB` at site HAM02. The game ID names HOM as the home team.
  The parser keeps the `info` records.
- CI2194308030 (`ngl_e/1943.EVR`) and NY6194505230 (`ngl_e/1945.EVR`) each
  appear twice with identical records. The parser keeps one copy.
- HOM193807031's other `bline` rows carry fielding positions in the
  sequence column. They parse as sequence numbers.

## Games rejected by the parser

The parser rejects a game when one player appears twice in a side's starting
lineup: in two `start` records, or as the first `bline` in two batting slots.
It logs an `ERROR` and writes no output for the game. A player who moves to
another batting slot or fielding position during the game is accepted. In the
October 2026 corpus these games are rejected:

| Game | File | Records | Slots |
| --- | --- | --- | --- |
| CB2193307290 | `ngl_b/1933.EBR` | `start,byrdb101,...,1,8,8`; `start,byrdb101,...,1,9,8` | 8 and 9 |
| NSH193306042 | `ngl_b/1933.EBR` | `start,browr103,...` twice | 5 and 6 |
| PTC193306010 | `ngl_b/1933.EBR` | `start,lacko101,...,1,7,6`; `start,lacko101,...,1,8,6` | 7 and 8 |
| NW1193408192 | `ngl_b/1934.EBR` | `stat,bline,johnb111,1,1,1,...`; `stat,bline,johnb111,1,4,1,...` | 1 and 4 |
| PH5193408270 | `ngl_b/1934.EBR` | `stat,bline,wrigb104,0,1,1,...`; `stat,bline,wrigb104,0,8,1,...` | 1 and 8 |
| NY5193508042 | `ngl_b/1935.EBR` | `start,hubbj101,...` twice | 8 and 9 |
| PTC193608040 | `ngl_b/1936.EBR` | `start,cartp102,...` twice | 8 and 9 |
| WBS193805220 | `ngl_b/1938.EBR` | `start,speah101,...,1,3,5`; `start,speah101,...,1,4,5` | 3 and 4 |

## Games excluded from the release build

The parser accepts these 12 games, but each box score contradicts itself in a
way the `baseball.computer` models cannot load. The release build excludes
them. As with the unresolved records, the game's other records do not supply
a single correct value, so the files stay as published.

A batter has fewer hits than extra-base hits:

| Game | File | Record | Conflict |
| --- | --- | --- | --- |
| PTC193307042 | `ngl_b/1933.EBR` | `stat,bline,charo101,1,3,1,4,3,1,1,0,1,...` | 1 hit; 1 double, 1 home run |
| BRN193505261 | `ngl_b/1935.EBR` | `stat,bline,dixor101,1,4,1,5,0,1,2,0,0,...` | 1 hit; 2 doubles |
| BRN193507211 | `ngl_b/1935.EBR` | `stat,bline,willn103,0,6,1,5,0,1,1,0,1,...` | 1 hit; 1 double, 1 home run |
| BRN193507212 | `ngl_b/1935.EBR` | `stat,bline,wrigb104,0,5,1,3,2,1,1,0,1,...` | 1 hit; 1 double, 1 home run |
| CAG193506021 | `ngl_b/1935.EBR` | `stat,bline,harrv102,0,1,1,4,1,1,1,1,0,...` | 1 hit; 1 double, 1 triple |
| NW1193507141 | `ngl_b/1935.EBR` | `stat,bline,willn103,0,8,1,4,1,1,3,0,0,...` | 1 hit; 3 doubles |
| PH5193506030 | `ngl_b/1935.EBR` | `stat,bline,wrigb104,0,6,1,4,0,0,-1,-1,1,...` | 0 hits; 1 home run |
| IN6193707200 | `ngl_b/1937.EBR` | `stat,bline,pearl101,0,4,1,4,2,1,2,0,0,...` | 1 hit; 2 doubles. The team and pitcher totals agree with these values |
| MEM193704251 | `ngl_b/1937.EBR` | `stat,bline,davel102,0,1,1,6,2,3,1,2,1,...` | 3 hits; 1 double, 2 triples, 1 home run |
| NW2193708140 | `ngl_b/1937.EBR` | `stat,bline,hayej104,1,8,1,3,2,1,1,1,0,...` | 1 hit; 1 double, 1 triple |
| HOM193808283 | `ngl_b/1938.EBR` | `stat,bline,clarb101,0,8,1,3,1,1,1,0,1,...` | 1 hit; 1 double, 1 home run |

A fielder has two first-position `dline`s, so the starting fielding lineup
lists the player twice. The outs suggest a position change and the second row is
misnumbered, but the record does not say so:

| Game | File | Records |
| --- | --- | --- |
| PTC193509021 | `ngl_b/1935.EBR` | `stat,dline,browr103,0,1,8,...`; `stat,dline,browr103,0,1,1,...` |

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

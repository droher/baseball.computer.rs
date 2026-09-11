# Pitch history and PA-resumption catcher pickoffs

`PitchSequenceItem::new_pitch_sequence` lexes the complete Retrosheet pitch
field. A period is a separator, not evidence that the preceding pitches were
already exported. For example, the first appearance records `BCBS.BT` in
ANA202304070 event 22 and `FS.S` in ANA202504040 event 12 contain six and three
pitches respectively. The former suffix-only parser exported only two and one.

`PlayRecord` caches the complete lexical result, original token characters, and
raw field. After game-state replay, `src/event_file/game_state.rs` resolves each
appearance across its events. Period positions do not affect pitch identity.
Unknown tokens retain their original identity: `U` cannot match `?`, nor can
two different unrecognized characters match merely because their enums agree.
No edit-distance, suffix-overlap, or count-based inference is used.

Only newly observed items are assigned to the current event, with one-based
event-local IDs. The cached sequence is never modified. For example,
`CFBF*B>B` followed by `CFBF*B>B.B` emits six items and then one ball.
`C1.` followed by `C1.>B` emits only the new runner-going ball.

Matching pitch tokens may differ in annotations. Runner-going and blocked
flags are combined by logical OR. A later catcher-pickoff annotation can fill
an earlier missing annotation; an identical base is compatible, but different
explicit bases conflict. Updates apply to the event that originally owned the
pitch, preserving its batter and pitcher even across substitutions. A later
omitted flag cannot erase an earlier observation. Raw fields retain every
version of the annotations for auditing.

A previously observed pitcher-pickoff throw may be absent from a later
cumulative field. It stays attached to its original event. Only an exact
ordered prefix with optional deletion of those prior throws is accepted;
inserting a throw into the middle of recorded pitches or changing its base is
unresolved. If an omitted throw later reappears at its old position, it is
ambiguous whether it repeats the old throw or records a new one, so the
appearance is quarantined. Repeated fouls and repeated throws are never
collapsed just because their characters match.

Trailing `N` placeholders may disappear, as in PIT202308210:
`>B.*B*SCN` followed by `>B.*B*SC3.X`. The original `N` stays on its original
event; only the new pickoff and in-play pitch are emitted later. Retained
trailing markers are not emitted twice. Internal markers cannot move or
disappear. The raw fields preserve these source changes.

History survives empty fields, no-play records, comments, and substitutions.
It resets after a plate-appearance result or the third out, including an
inning-ending runner out during an unfinished appearance. Inning or batting
side changes also establish a boundary.

## Conflicts and explicit availability

Each event has an `event_pitch_sequence_status` row containing its raw pitch
field and appearance-start event ID. `Resolved` means the available records
were reconciled without a sequence conflict; it does not assert that
Retrosheet observed every real-world pitch. `Unavailable` means the whole
appearance contains no parsed sequence items. A marker-only or pickoff-only
appearance can be resolved and still have no delivered pitches.

A conflict marks **every event in that appearance** `Unresolved` and removes
all of its normalized pitch rows, including earlier rows. The game, ordinary
play statistics, substitutions, counts, and raw fields remain intact. The
`event_pitch_sequence_issues` table retains the reason, current and prior raw
strings, and their event IDs. Join `event_audit` for original file/line
locations. JSONL includes the same statuses, raw strings, and issue evidence.
Comparison continues after a conflict to discover subsequent discrepancies;
those comparisons cannot make any part of the appearance trusted again.

Normal exports accept only the exact fingerprints in
[pitch_sequence_reviewed_conflicts.json](pitch_sequence_reviewed_conflicts.json).
Each entry records a reviewed decision to quarantine, not a corrected pitch
sequence. Matching includes game, current/prior events, appearance start,
reason, and both complete raw strings. An unexpected or changed conflict
fails before any rows from that game are written. Failed runs can contain
other games' partial output and must not be published.

For discovery, use `--audit-pitch-conflicts` with a temporary output directory.
It preserves unreviewed conflict evidence without failing on those conflicts.
Review the issue rows and source records before extending the manifest; do
not generate approvals automatically from every new audit. Rebuild the binary
after updating the compiled manifest, then validate without the audit flag.

## Catcher pickoffs at resumption

A `+base` immediately after a period is pending metadata for the first pitch
after resumption. It is consumed without synthesizing an unknown pitch or a
pitcher pickoff. It remains pending across additional period markers. Both
`B*BBC+1.+1F>X` pickoffs survive when the full appearance is first seen; if the
first segment was already emitted, only the new segment is exported.
A trailing `.+1` with no pitch emits no item, but a later cumulative record
that supplies the next pitch attaches that pending annotation correctly.

## Validation and regeneration

Validation on 2026-09-11 used newly downloaded, unmodified
[2023](https://www.retrosheet.org/events/2023eve.zip),
[2024](https://www.retrosheet.org/events/2024eve.zip), and
[2025](https://www.retrosheet.org/events/2025eve.zip) archives. All 7,289 games
and 683,446 events matched their source identities, appearance histories,
event-local pitch sequence IDs, pitch types, flags, and pickoff annotations.
The original ANA games are regression fixtures under `tests/pitch_history`.

Compared with the old suffix-only lexer, 59,908 event sequences change. The
net recovery is 181,611 pitch entries plus 1,396 pitcher-pickoff throws.
The corrected export contains 2,173,348 sequence items, including 759 `NoPitch`
markers. Deduplicating the complete raw fields avoids repeating 146,596
non-`NoPitch` items and 747 `NoPitch` markers.

`bin/validate_pitch_sequences.py` independently reads original archives and
checks the CSV export, including source count continuity and terminal pitch
types. It distinguishes extraction failures (nonzero exit) from source-count
and special-outcome findings. Four original sequences contain an extra ball
relative to the stated count: CIN202409050 event 79, MIA202309220 event 110,
OAK202304190 event 30, and SEA202503280 event 43. MIA202408250 event 87 has
`VVVV` but a putout result (`2`). Three batter-interference records are reported
as special outcomes. The source records are preserved; none is patched by
this change. See [the saved validation report](pitch_sequence_validation_2023_2025.json).

After extracting only the `.EVA` and `.EVN` archive members into a temporary
input directory, reproduce the checks with:

```bash
cargo build
target/debug/baseball-computer -i /tmp/pitch-validation/input -o /tmp/pitch-validation/output
uv run python bin/validate_pitch_sequences.py --archives /tmp/pitch-validation/archives --output /tmp/pitch-validation/output --report /tmp/pitch-validation/report.json
```

`--max-games 1` runs a bounded validation smoke check. The regression suite is
`cargo test`; lint with `cargo clippy --all-targets`.

The historical discovery audit processed 205,886 games and 18,141,020 events.
After reconciliation, 144 appearances in 141 games remain unresolved, spanning
333 event rows. Their 145 diagnostics comprise 144 token disagreements and one
conflicting catcher-pickoff base. All 145 exact source fingerprints were
reviewed for quarantine and recorded in the manifest. These are conservative
unresolved cases, not claimed source corrections. The earlier strict audit
rejected 3,124 games, mostly because annotations disappeared in 1991–1992.
The new audit continues through every conflict, so it also finds disagreements
hidden behind those first annotation failures. See
[pitch_impact_by_year.md](pitch_impact_by_year.md) and the
[reconciliation CSV](pitch_history_reconciliation_by_year.csv).

The normalized pitch CSV schema stays unchanged. Two new status/evidence
tables accompany it, and JSONL gains the equivalent fields. Corrected values
require regenerating the pitch export, both new tables, their Parquet files,
and downstream pitch-derived tables. Downstream models must consume the status
table before publication: unresolved or unavailable pitch totals must stay
unknown, including mixed player/game/season aggregates, rather than becoming
zero or a partial total. Follow the concrete
[downstream migration requirements](pitch_sequence_downstream_migration.md).
The downstream SQL changes and production regeneration are not part of this
parser-only change.

Keep source file ordering and the `event_key` namespace identical to retained
event tables. A season-only run has different keys from a full-corpus run and
cannot replace its source tables directly. The existing nonpitch CSV snapshots
remain unchanged; the pitch snapshot changes and two new snapshots cover the
status/evidence tables. No production data was rebuilt or published.

## Corpus occurrences (44)

Generated against the Retrosheet corpus on 2026-05-02. Filter: rightmost `.`
in the pitch sequence is immediately followed by `+`. Sequence column is the
raw value from the `play,` record; play column is the play description.

| File | Game | Inning | Batter | Pitch sequence | Play |
|------|------|--------|--------|----------------|------|
| 1989BOS.EVA | BOS198907060 | 7 | molip001 | `F1C>FFB11BB.+2FFFX` | `E6/G6.2-H(NR)(UR)` |
| 1990SLN.EVN | SLN199009300 | 5 | pagnt001 | `.+1` | `POCS2(16)` |
| 2007TEX.EVA | TEX200705031 | 8 | abreb001 | `*B>F>S.+1BS` | `K` |
| 2007TOR.EVA | TOR200705110 | 1 | yound003 | `B>B.+3CCS` | `K` |
| 2010ARI.EVN | ARI201008020 | 7 | kenna001 | `CC>B.+3F*BX` | `43/G34` |
| 2010PHI.EVN | PHI201009210 | 2 | victs001 | `CB>B.+1FX` | `64(1)/FO/G6` |
| 2010SEA.EVA | SEA201004120 | 4 | sweer001 | `>B.+1BBB` | `W.1-2` |
| 2013BOS.EVA | BOS201309170 | 2 | bogax001 | `BBC>B.+3CFB` | `W` |
| 2013CHA.EVA | CHA201306250 | 1 | byrdm001 | `>C.+1X` | `9/SF/F89S.3-H` |
| 2013COL.EVN | COL201305160 | 2 | rutlj001 | `B11F>F1F>B.+3T` | `K` |
| 2014SLN.EVN | SLN201407070 | 4 | mercj002 | `SBF*B.+3X` | `4/F4D-` |
| 2015CHA.EVA | CHA201510020 | 6 | shucj001 | `BCB.+1LX` | `6(1)/FO/G6` |
| 2015CHN.EVN | CHN201508130 | 3 | rogej002 | `*B.+3X` | `9/F89D+` |
| 2015DET.EVA | DET201508250 | 6 | iglej001 | `CB.+1X` | `13/G13S-` |
| 2016BOS.EVA | BOS201605130 | 5 | gomec002 | `BFF>B.+3S` | `K` |
| 2016PHI.EVN | PHI201607010 | 3 | franm004 | `B*BBC+1.+1F>X` | `S9/MREV/L89M.3-H;1-3` |
| 2017CLE.EVA | CLE201706130 | 8 | herne001 | `S>B.+3BBCS` | `K` |
| 2017LAN.EVN | LAN201708150 | 4 | engea001 | `BS.+2FS` | `K` |
| 2017PHI.EVN | PHI201704270 | 1 | franm004 | `B>B.+2>B` | `CS3(25)/MREV` |
| 2017SFN.EVN | SFN201708020 | 8 | semim001 | `T>T.+3BS` | `K` |
| 2017TEX.EVA | TEX201706210 | 8 | maill001 | `.>B.+3BCX` | `5/L56S` |
| 2018CLE.EVA | CLE201806200 | 6 | branm003 | `>B.+3X` | `43/G4S` |
| 2018TOR.EVA | TOR201805230 | 2 | cozaz001 | `BB.+1` | `NP` |
| 2018TOR.EVA | TOR201805230 | 2 | cozaz001 | `BB.+1BB` | `W.1-2` |
| 2019MIL.EVN | MIL201909060 | 4 | pereh001 | `BCS.+3C` | `K` |
| 2021ANA.EVA | ANA202104160 | 7 | troum001 | `CFBB.+3VV` | `IW` |
| 2021ANA.EVA | ANA202105260 | 1 | wardt002 | `B>C.+3FFX` | `HR/F7D.3-H;2-H` |
| 2021ARI.EVN | ARI202107170 | 2 | gallz001 | `B>B.+3CSX` | `43/G4D` |
| 2021NYA.EVA | NYA202110010 | 9 | franw002 | `BF1>B.+3BFX` | `S8/G4.3-H;2-H` |
| 2022NYA.EVA | NYA202209072 | 6 | kinei001 | `C*BS>*B.+2X` | `4/P4` |
| 2022SLN.EVN | SLN202209030 | 1 | orter001 | `SC>B.+3BBX` | `S9/L9.3-H;2-H` |
| 2023MIN.EVA | MIN202309100 | 3 | lewir003 | `BCS>B.+3FX` | `53/G56S+` |
| 2024ARI.EVN | ARI202405040 | 1 | bogax001 | `TB1>B.+3FC` | `K` |
| 2024ARI.EVN | ARI202408300 | 7 | smitw003 | `>B.+3BSCX` | `HR/F7D.3-H;2-H` |
| 2024DET.EVA | DET202407300 | 2 | schnd002 | `>B.+3SBSX` | `8/F8` |
| 2024KCA.EVA | KCA202404230 | 7 | pasqv001 | `F>B.+3*BCFS` | `K` |
| 2024MIA.EVN | MIA202408050 | 4 | friet001 | `BCBFF>B.+3*B` | `W` |
| 2024MIL.EVN | MIL202404300 | 2 | contw002 | `B>B.+3BFFX` | `9/SF/F9D.3-H;2-3` |
| 2024OAK.EVA | OAK202407200 | 4 | monim001 | `>F>B.+3X` | `3/L3` |
| 2024SDN.EVN | SDN202404300 | 3 | delae003 | `>C.+3FFBBFB` | `NP` |
| 2025ATH.EVA | ATH202504090 | 1 | gonzo001 | `F>B.+3X` | `9/F9LD` |
| 2025BOS.EVA | BOS202506110 | 2 | loweb001 | `>B.+3X` | `41/G4-` |
| 2025MIA.EVN | MIA202507060 | 2 | haase001 | `C>F>B.+3S` | `K` |
| 2025SFN.EVN | SFN202506050 | 3 | smitd008 | `SS.B>B.+3FFFBX` | `DGR/F8XD.3-H;2-H` |

## Related: duplicate pickoff annotations (3 rows)

A separate but related quirk: three corpus rows record a *second* catcher
pickoff annotation for the same pitch, with no schema slot to hold it. The
same-base annotations are folded into one value and both remain visible in
the raw field. Distinct bases produce structured conflict evidence and
quarantine the appearance. The sequence-only lexical API returns an error
for conflicting annotations; the parser uses the richer API to retain the
evidence. Invalid `+` shapes remain unrecognized tokens rather than being
absorbed as metadata.

| File | Game | Inning | Batter | Pitch sequence | Play |
|------|------|--------|--------|----------------|------|
| 2017BAL.EVA | BAL201706050 | 5 | buxtb001 | `M+1>+1` | `POCS2(234)` |
| 2019DET.EVA | DET201907270 | 7 | crawj002 | `.BBB+1+1` | `PO1(23)` |
| 2025WAS.EVN | WAS202508220 | 8 | abrac001 | `BBBC+1+1B` | `W.1-2` |

## Distribution notes

- Almost entirely retro-fill of pickoff annotations on data after 2007.
- Pickoff base distribution: `+3` dominant (~32), `+1` (~9), `+2` (~3).
- Two rows (`2018TOR.EVA` `cozaz001`) are the same PA across consecutive
  event rows — the first segment ends with the pickoff (`POCS` is encoded on
  the *next* row's play); the parser must keep these in sync.
- Edge case `2016PHI.EVN franm004 B*BBC+1.+1F>X` has *two* `+1` pickoffs
  (one mid-segment, one at resumption). Both survive full-field lexing;
  appearance history determines which event owns each pitch.
- `1990SLN.EVN pagnt001 .+1` is the degenerate case: trimmed segment is
  `+1` with no following pitch. Pickoff has nothing to attach to; emit no
  pitch and no warning.

## Reproducing the historical annotation inventory

```bash
uv run python - <<'PY'
import csv
from pathlib import Path
root = Path("retrosheet")
for ext in ("**/*.EV*", "**/*.EB*", "**/*.ED*"):
    for p in sorted(root.glob(ext)):
        gid = None
        for row in csv.reader(p.open("r", encoding="latin-1", newline="")):
            if not row: continue
            k = row[0].strip()
            if k == "id" and len(row) >= 2: gid = row[1].strip()
            if k != "play" or len(row) < 7: continue
            seq = row[5].strip()
            i = seq.rfind(".")
            if i >= 0 and i + 1 < len(seq) and seq[i+1] == "+":
                print(f"{p.name}|{gid}|{row[1].strip()}|{row[3].strip()}|{seq}|{row[6].strip()}")
PY
```

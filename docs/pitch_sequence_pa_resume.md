# PA-resumption catcher pickoff (`.+N`)

Retrosheet's pitch-sequence column carries the entire plate appearance, even
when the PA spans multiple `play,` rows (e.g. because a stolen base, balk, or
substitution interrupted it). A `.` separates the segments. The parser keeps
only the segment after the **rightmost** `.` so a single PA's pitches aren't
double-counted across event rows.

Occasionally a catcher pickoff occurs at the moment of resumption — Retrosheet
encodes it as `+N` immediately after the `.`, before any new pitch. After
trimming, the remainder begins with `+N…`, which is metadata for the next real
pitch, not a pitch token itself.

The parser in `src/event_file/pitch_sequence.rs` consumes a leading `+N` from
the trimmed segment and attaches it to the first real pitch as
`catcher_pickoff_attempt`. Without that handling, `+` falls through to the
pitch-type lookup and produces a phantom `Unrecognized` pitch, followed by a
phantom `PickoffAttempt{First,Second,Third}` synthesized from the digit. The
fix removes both phantoms and assigns the pickoff to the right pitch.

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
first pickoff is recorded on the previous pitch via the peek path; the
duplicate `+N` (or `>+N` after a pickoff is already attached) is dropped
silently in the main loop's control-char arm. The arm only consumes the
`+` when the following char is a base digit (`1`/`2`/`3`/`H`); any other
shape stays on the Unrecognized warn path so future Retrosheet additions
don't get silently absorbed.

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
  (one mid-segment, one at resumption). Both must survive — the rightmost-`.`
  trim already drops the first, but the resumption pickoff must attach to the
  first post-resumption pitch (`F`).
- `1990SLN.EVN pagnt001 .+1` is the degenerate case: trimmed segment is
  `+1` with no following pitch. Pickoff has nothing to attach to; emit no
  pitch and no warn.

## Regeneration

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

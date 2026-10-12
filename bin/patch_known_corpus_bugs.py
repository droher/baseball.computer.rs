"""
Apply idempotent patches to a Retrosheet corpus directory for individual game
records that the parser cannot resolve and that Retrosheet has not yet fixed
upstream.

Each patch is keyed by (relative_file_path, game_id, before_line, after_line).
The script walks every patch, finds the targeted line, and rewrites it only if
the file still contains the `before` form. Re-running on an already-patched
corpus is a no-op. Re-running after a fresh download re-applies the patch.

Run:
    uv run python bin/patch_known_corpus_bugs.py <retrosheet_dir>

Patches:

* ATN193807032 (`ngl_e/1938.EVR`) — top of 5th, plate appearance was credited
  to `hayeb104` who had been replaced by `mitca102` in batting position 9 in
  the bottom of 4th. The play below leaves the parser unable to locate the
  batter in the lineup. Newspaper box score corroborates Mitchell as pitcher
  of record at this point. Rewrite the batter to `mitca102`.

* CIN191007090 (`events/1910CIN.EVN`) — `data,er,rowaj101, 0` carries a
  stray leading space before the integer, so the strict numeric parser
  rejects the whole game. Every other CIN191007??? game in the same file
  records this pitcher's earned-run lines as `data,er,rowaj101,0` (no
  space). Strip the space.

* CHN190908040 (`events/1909CHN.EVN`) — bottom of 4th, `CS2(25).1-2`
  both retires and advances the runner on first. The 1909 box score
  attributes the caught stealing to reule101, who was on first, so drop
  the contradictory `.1-2` advance.

* CHA197409200 (`events/1974CHA.EVA`) — tenag101's 6th-inning force-out
  carries ` X` (leading space) in the pitches column. Drop the space.

* PRG193512011/PRG193512012 (`ngl_e/1935.EVR`) — both games of the
  1935-12-01 doubleheader are filed under PRG193512012. The nine-inning
  copy (wp grifr101) matches box score PRG193512011, so rename it and set
  `info,number,1`. The patch locates that copy by its `info,wp` line.

* NLB box scores (`ngl_b/*.EBR`) — `ngl_box_corrections.py` applies
  rule-based corrections derived from each game's own records and leaves
  anything ambiguous untouched with a warning. Pass `--corrections-csv` to
  list every change.

* Reviewed game overrides (`corpus_overrides/games/`) — after the NLB
  corrections, each `<GAMEID>.EBR` there replaces that game's record. Its
  `<GAMEID>.diff` explains every change and holds the expected diff from the
  corrected upstream record; the override is applied only while upstream
  still produces that diff, and otherwise the script logs `SKIP
  upstream-changed` and exits nonzero.

* WS1191105040 (`events/1911WS1.EVA`) — bottom of 2nd, a 5-to-3 putout is
  encoded as `5-3.2-3` (dash-separated fielders), but Retrosheet's modern
  event grammar concatenates the fielders (`53`). The same batter-fielder
  combo appears as `53` later in the same file (line 136). Rewrite the
  fielding code while preserving the explicit `2-3` runner advance.

* CHN198404200 (`events/1984CHN.EVN`) — 2nd inning, davij001 PA. Pitch
  sequence `*B1FCB1Bx` ends with a lowercase `x`. The play (`43/G34`) is a
  groundout, so the final pitch was put in play and the intended code is
  uppercase `X` (InPlay). One-character typo; rewrite the final char.

* CIN197510140, LAN197410090, KCA198706160 — three games carry stray
  digits (`00`/`01`) in the pitches column of `NP` (no-play) records.
  `NP` rows describe non-events such as a balk or appeal where no pitch
  was actually thrown, and the digits are not valid Retrosheet pitch
  codes. Empty the pitches column on these specific rows.

* Park-ID typos in NLB box-score files (`ngl_b/*.EBR`) — eight games carry
  malformed `info,site,...` codes that don't resolve against retrosheet's
  ballparks biodata. Each is a deterministic typo of a real ID present in
  both `ballparks.csv` and `ballparks0.csv`:

    - PH5194708140 (`ngl_b/1947.EBR`): `PHi17` → `PHI17`
      Lowercase `i` typo of Penmar Park, Philadelphia.
    - STA192107160 (`ngl_b/1921.EBR`): `DEC01` → `DCT01`
      Wrong prefix. DCT01 (Staley Field, Decatur IL) has first_g=19210716
      exactly matching this game; STA = Staleys.
    - KCM194108070, CAG194608290, CAG194709050, IN9194708210,
      CAG194807120 (across `ngl_b/{1941,1946,1947,1948}.EBR`):
      `DEC02` → `DCT02`
      Wrong prefix. DCT02 (Fans Field, Decatur IL) date range
      1937-07-14 → 1949-06-22 covers all five neutral-site games.
    - PH5194805010 (`ngl_b/1948.EBR`): `CHESTER PA` → `CHE01`
      Free-text city/state in ID slot. Of three Chester PA parks in
      biodata, only CHE01 (Lloyd Field, 1938-06-01 → 1949-06-07) covers
      the 1948-05-01 game date; CHE02 ends 1938, CHE03 ends 1936.
"""

# pyright: reportAny=false

from __future__ import annotations

import argparse
import difflib
import logging
import re
import sys
from collections.abc import Sequence
from dataclasses import dataclass
from pathlib import Path

import ngl_box_corrections

logger = logging.getLogger(__name__)

DEFAULT_GAME_OVERRIDES = (
    Path(__file__).resolve().parent.parent / "corpus_overrides" / "games"
)
DIFF_FROM_PREFIX = "--- a/"
HUNK_RE = re.compile(r"@@ -\d+(?:,\d+)? \+(?P<start>\d+)(?:,(?P<count>\d+))? @@")


@dataclass(frozen=True)
class Patch:
    relative_path: str
    game_id: str
    before: str
    after: str
    rationale: str
    context: str | None = None


PATCHES: tuple[Patch, ...] = (
    Patch(
        relative_path="ngl_e/1938.EVR",
        game_id="ATN193807032",
        before="play,5,0,hayeb104,??,,99#",
        after="play,5,0,mitca102,??,,99#",
        rationale=(
            "hayeb104 left the game in the bottom of the 4th when mitca102 "
            "replaced him at batting position 9; the original record names "
            "the wrong batter for the top of the 5th."
        ),
    ),
    Patch(
        relative_path="events/1910CIN.EVN",
        game_id="CIN191007090",
        before="data,er,rowaj101, 0",
        after="data,er,rowaj101,0",
        rationale=(
            "Stray leading space before integer 0 trips the strict numeric "
            "data-record parser. Every other CIN191007??? game in this file "
            "uses the no-space form."
        ),
    ),
    Patch(
        relative_path="events/1909CHN.EVN",
        game_id="CHN190908040",
        before="play,4,1,everj102,??,,CS2(25).1-2",
        after="play,4,1,everj102,??,,CS2(25)",
        rationale=(
            "The box score credits this inning-4 caught stealing to "
            "reule101, the runner on first, so `CS2` already retires him. "
            "The trailing `.1-2` advances the same runner and leaves the "
            "half-inning one out short."
        ),
    ),
    Patch(
        relative_path="events/1974CHA.EVA",
        game_id="CHA197409200",
        before="play,6,0,tenag101,??, X,64(1)/FO",
        after="play,6,0,tenag101,??,X,64(1)/FO",
        rationale=(
            "Leading space in the pitches column. The only recorded pitch is "
            "the ball put in play for the force-out."
        ),
    ),
    Patch(
        relative_path="ngl_e/1935.EVR",
        game_id="PRG193512012",
        context="info,wp,grifr101",
        before="id,PRG193512012",
        after="id,PRG193512011",
        rationale=(
            "Two different games share this ID. The nine-inning copy won by "
            "grifr101 matches box score PRG193512011 (game 1); the five-inning "
            "copy matches PRG193512012."
        ),
    ),
    Patch(
        relative_path="ngl_e/1935.EVR",
        game_id="PRG193512011",
        before="info,number,2",
        after="info,number,1",
        rationale="Doubleheader number for the game renamed to PRG193512011.",
    ),
    Patch(
        relative_path="events/1911WS1.EVA",
        game_id="WS1191105040",
        before="play,2,1,mcbrg101,??,,5-3.2-3",
        after="play,2,1,mcbrg101,??,,53.2-3",
        rationale=(
            "Dash-separated fielders (`5-3`) are a scorebook convention "
            "Retrosheet's modern event grammar does not accept; later in "
            "the same file the same play is encoded as `53`. Concatenate "
            "the fielders while preserving the explicit `2-3` advance."
        ),
    ),
    Patch(
        relative_path="events/1984CHN.EVN",
        game_id="CHN198404200",
        before="play,2,1,davij001,32,*B1FCB1Bx,43/G34",
        after="play,2,1,davij001,32,*B1FCB1BX,43/G34",
        rationale=(
            "Final pitch char is lowercase `x`; the play is a groundout "
            "(`43/G34`), so the intended code is uppercase `X` (InPlay). "
            "Single-character typo confined to this row."
        ),
    ),
    Patch(
        relative_path="postseason/1975WS.EVE",
        game_id="CIN197510140",
        before="play,5,0,evand002,00,00,NP",
        after="play,5,0,evand002,00,,NP",
        rationale=(
            "Stray `00` in pitches column on a no-play record. NP rows "
            "describe non-events (here a substitution) with no pitches "
            "thrown; the digits are not valid Retrosheet pitch codes."
        ),
    ),
    Patch(
        relative_path="postseason/1974NLCS.EVE",
        game_id="LAN197410090",
        before="play,8,1,yeags001,00,00,NP",
        after="play,8,1,yeags001,00,,NP",
        rationale=(
            "Stray `00` in pitches column on a no-play record. See "
            "CIN197510140 for the same pattern."
        ),
    ),
    Patch(
        relative_path="events/1987KCA.EVA",
        game_id="KCA198706160",
        before="play,4,1,whitf001,,01,NP",
        after="play,4,1,whitf001,,,NP",
        rationale=(
            "Stray `01` in pitches column on a no-play record. The com "
            "line above explains the substitution (Steinbach injured by "
            "a foul); no pitch belongs on this NP row."
        ),
    ),
    Patch(
        relative_path="ngl_b/1947.EBR",
        game_id="PH5194708140",
        before="info,site,PHi17",
        after="info,site,PHI17",
        rationale="Lowercase `i` typo of PHI17 (Penmar Park, Philadelphia).",
    ),
    Patch(
        relative_path="ngl_b/1921.EBR",
        game_id="STA192107160",
        before="info,site,DEC01",
        after="info,site,DCT01",
        rationale=(
            "Wrong prefix `DEC` for Decatur park ID. Biodata has DCT01 "
            "(Staley Field, Decatur IL) with first_g=19210716 — exact "
            "match for this game (STA = Staleys, home team)."
        ),
    ),
    Patch(
        relative_path="ngl_b/1941.EBR",
        game_id="KCM194108070",
        before="info,site,DEC02",
        after="info,site,DCT02",
        rationale=(
            "Wrong prefix `DEC` for DCT02 (Fans Field, Decatur IL). "
            "Biodata date range 1937-1949 covers this neutral-site game."
        ),
    ),
    Patch(
        relative_path="ngl_b/1946.EBR",
        game_id="CAG194608290",
        before="info,site,DEC02",
        after="info,site,DCT02",
        rationale="DEC02 → DCT02 (Fans Field, Decatur IL). See KCM194108070.",
    ),
    Patch(
        relative_path="ngl_b/1947.EBR",
        game_id="CAG194709050",
        before="info,site,DEC02",
        after="info,site,DCT02",
        rationale="DEC02 → DCT02 (Fans Field, Decatur IL). See KCM194108070.",
    ),
    Patch(
        relative_path="ngl_b/1947.EBR",
        game_id="IN9194708210",
        before="info,site,DEC02",
        after="info,site,DCT02",
        rationale="DEC02 → DCT02 (Fans Field, Decatur IL). See KCM194108070.",
    ),
    Patch(
        relative_path="ngl_b/1948.EBR",
        game_id="CAG194807120",
        before="info,site,DEC02",
        after="info,site,DCT02",
        rationale="DEC02 → DCT02 (Fans Field, Decatur IL). See KCM194108070.",
    ),
    Patch(
        relative_path="ngl_b/1948.EBR",
        game_id="PH5194805010",
        before="info,site,CHESTER PA",
        after="info,site,CHE01",
        rationale=(
            "Free-text `CHESTER PA` in park-ID slot. Of three Chester PA "
            "parks in biodata, only CHE01 (Lloyd Field, 1938-06-01 → "
            "1949-06-07) covers the 1948-05-01 game date; CHE02 ends "
            "1938, CHE03 ends 1936."
        ),
    ),
)


def find_game_block(
    lines: list[str], game_id: str, context: str | None = None, alias: str | None = None
) -> tuple[int, int] | None:
    """Return [start, end) line indices for the game block, or None."""
    starts = [
        i
        for i, line in enumerate(lines)
        if line.startswith(f"id,{game_id}") or (alias is not None and line == alias)
    ]
    for start in starts:
        end = len(lines)
        for j in range(start + 1, len(lines)):
            if lines[j].startswith("id,"):
                end = j
                break
        if context is None or context in lines[start:end]:
            return (start, end)
    return None


def apply_patch(root: Path, patch: Patch) -> str:
    target = root / patch.relative_path
    if not target.exists():
        return f"SKIP missing-file {patch.relative_path}"

    raw = target.read_bytes()
    # Retrosheet files have historically been pure ASCII; surface a clear
    # message instead of a stack trace if upstream ever ships a UTF-8 BOM
    # or other non-ASCII bytes (we'd refuse to round-trip them blindly).
    if raw.startswith(b"\xef\xbb\xbf"):
        return f"SKIP utf8-bom-not-supported {patch.relative_path}"
    try:
        text = raw.decode("ascii")
    except UnicodeDecodeError as e:
        return f"SKIP non-ascii-bytes {patch.relative_path}: {e}"
    # Preserve original line endings exactly (Retrosheet files mix CRLF/LF).
    # `keepends=True` retains them per-line so we can rejoin verbatim.
    lines = text.splitlines(keepends=True)

    alias = patch.after if patch.before.startswith("id,") else None
    block = find_game_block(
        [line.rstrip("\r\n") for line in lines], patch.game_id, patch.context, alias
    )
    if block is None:
        return f"SKIP game-not-found {patch.game_id} in {patch.relative_path}"

    start, end = block
    matched_idx: list[int] = []
    already_idx: list[int] = []
    for i in range(start, end):
        stripped = lines[i].rstrip("\r\n")
        if stripped == patch.before:
            matched_idx.append(i)
        elif stripped == patch.after:
            already_idx.append(i)

    if not matched_idx and already_idx:
        return f"NOOP already-patched {patch.game_id} in {patch.relative_path}"
    if not matched_idx:
        return (
            f"SKIP record-not-found {patch.game_id} in {patch.relative_path} "
            f"(expected line: {patch.before!r})"
        )

    for i in matched_idx:
        # Preserve original line ending on each rewritten line.
        original = lines[i]
        eol = original[len(original.rstrip("\r\n")) :]
        lines[i] = patch.after + eol

    # Atomic write: stage the new bytes in a sibling temp file and then
    # `os.replace` over the target. This prevents a concurrent reader (e.g.
    # the Rust parser) from observing a half-written file, and ensures that
    # a re-run sees either the original or the fully-patched contents.
    ngl_box_corrections.write_atomically(target, "".join(lines).encode("ascii"))
    return (
        f"PATCH {patch.game_id} in {patch.relative_path} "
        f"({len(matched_idx)} line(s); reason: {patch.rationale})"
    )


@dataclass(frozen=True)
class GameOverride:
    game_id: str
    relative_path: str
    explanation: str
    expected_diff: str
    lines: list[str]


def render_game_diff(
    relative_path: str, before: Sequence[str], after: Sequence[str]
) -> str:
    return "\n".join(
        difflib.unified_diff(
            before,
            after,
            fromfile=f"a/{relative_path}",
            tofile=f"b/{relative_path}",
            lineterm="",
            n=1,
        )
    )


def write_game_diff(
    path: Path,
    relative_path: str,
    explanation: str,
    before: Sequence[str],
    after: Sequence[str],
) -> None:
    _ = path.write_text(
        f"{explanation.strip()}\n\n{render_game_diff(relative_path, before, after)}\n"
    )


def load_game_override(record_path: Path) -> GameOverride:
    lines = record_path.read_text().splitlines()
    if not lines or lines[0] != f"id,{record_path.stem}":
        raise ValueError(f"{record_path} must start with id,{record_path.stem}")
    diff_text = record_path.with_suffix(".diff").read_text()
    explanation, marker, body = diff_text.partition(f"\n{DIFF_FROM_PREFIX}")
    if not marker:
        raise ValueError(
            f"{record_path.with_suffix('.diff')} has no {DIFF_FROM_PREFIX} header"
        )
    expected_diff = (DIFF_FROM_PREFIX + body).rstrip("\n")
    relative_path = expected_diff.splitlines()[0].removeprefix(DIFF_FROM_PREFIX)
    return GameOverride(
        record_path.stem, relative_path, explanation.strip(), expected_diff, lines
    )


def load_game_overrides(overrides_dir: Path) -> list[GameOverride]:
    if not overrides_dir.is_dir():
        return []
    return [load_game_override(p) for p in sorted(overrides_dir.rglob("*.EB*"))]


def upstream_lines(override: GameOverride) -> list[str]:
    result: list[str] = []
    position = 0
    for line in override.expected_diff.splitlines()[2:]:
        hunk = HUNK_RE.match(line)
        if hunk is not None:
            start = int(hunk["start"]) - (0 if hunk["count"] == "0" else 1)
            result.extend(override.lines[position:start])
            position = start
            continue
        tag, text = line[:1], line[1:]
        if tag in (" ", "-"):
            result.append(text)
        if tag in (" ", "+"):
            position += 1
    result.extend(override.lines[position:])
    return result


def refresh_game_diff(record_path: Path, corpus_root: Path) -> str:
    override = load_game_override(record_path)
    target = corpus_root / override.relative_path
    stripped = [line.rstrip("\r\n") for line in target.read_text().splitlines()]
    block = find_game_block(stripped, override.game_id)
    if block is None:
        raise ValueError(f"{override.game_id} not found in {target}")
    write_game_diff(
        record_path.with_suffix(".diff"),
        override.relative_path,
        override.explanation,
        stripped[block[0] : block[1]],
        override.lines,
    )
    return f"REFRESH {override.game_id} from {target}"


def apply_game_override(root: Path, override: GameOverride) -> str:
    where = f"{override.game_id} in {override.relative_path}"
    target = root / override.relative_path
    if not target.resolve().is_relative_to(root.resolve()):
        return f"SKIP outside-corpus {override.relative_path}"
    if not target.exists():
        return f"SKIP missing-file {override.relative_path}"
    try:
        lines = target.read_bytes().decode("ascii").splitlines(keepends=True)
    except UnicodeDecodeError as e:
        return f"SKIP non-ascii-bytes {override.relative_path}: {e}"
    stripped = [line.rstrip("\r\n") for line in lines]
    if stripped.count(f"id,{override.game_id}") > 1:
        return f"SKIP duplicate-game {where}"
    block = find_game_block(stripped, override.game_id)
    if block is None:
        return f"SKIP game-not-found {where}"
    start, end = block
    current = stripped[start:end]
    if current == override.lines:
        return f"NOOP already-overridden {where}"
    if (
        render_game_diff(override.relative_path, current, override.lines)
        != override.expected_diff
    ):
        return (
            f"SKIP upstream-changed {where}; review the override against the new record"
        )
    eol = lines[start][len(stripped[start]) :]
    lines[start:end] = [line + eol for line in override.lines]
    ngl_box_corrections.write_atomically(target, "".join(lines).encode("ascii"))
    return f"OVERRIDE {where}"


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    _ = parser.add_argument(
        "retrosheet_dir",
        type=Path,
        help="Root of unpacked Retrosheet corpus (alldata.zip extraction).",
    )
    _ = parser.add_argument(
        "-v",
        "--verbose",
        action="store_true",
        help="Enable debug-level logging.",
    )
    _ = parser.add_argument(
        "--corrections-csv",
        type=Path,
        default=None,
        help="Write every rule-based NLB box-score correction to this CSV.",
    )
    _ = parser.add_argument(
        "--game-overrides",
        type=Path,
        default=DEFAULT_GAME_OVERRIDES,
        help="Directory of reviewed whole-game replacements (<GAMEID>.EB* plus <GAMEID>.diff).",
    )
    args = parser.parse_args(argv)

    verbose: bool = bool(getattr(args, "verbose", False))
    raw_root: Path | str = getattr(args, "retrosheet_dir")  # type: ignore[assignment]  # noqa: B009
    root = raw_root if isinstance(raw_root, Path) else Path(raw_root)

    logging.basicConfig(
        level=logging.DEBUG if verbose else logging.INFO,
        format="%(levelname)s %(name)s: %(message)s",
    )

    if not root.is_dir():
        logger.error("Retrosheet directory does not exist: %s", root)
        return 2

    overrides_dir: Path = args.game_overrides
    try:
        overrides = load_game_overrides(overrides_dir)
    except (OSError, ValueError):
        logger.exception(
            "Invalid game overrides in %s; corpus left unchanged", overrides_dir
        )
        return 1

    rc = 0
    for patch in PATCHES:
        try:
            outcome = apply_patch(root, patch)
        except Exception:
            logger.exception("Failed applying patch for %s", patch.game_id)
            rc = 1
            continue
        logger.info("%s", outcome)
        if outcome.startswith("SKIP"):
            rc = max(rc, 1)

    override_ids = frozenset(o.game_id for o in overrides)
    corrections, unresolved = ngl_box_corrections.correct_directory(
        root, overridden_games=override_ids
    )
    corrections_csv: Path | None = getattr(args, "corrections_csv", None)
    if corrections_csv is not None and not corrections:
        logger.warning(
            "No corrections applied (corpus already corrected?); leaving %s unchanged",
            corrections_csv,
        )
    elif corrections_csv is not None:
        ngl_box_corrections.write_corrections_csv(corrections, corrections_csv)

    overridden: set[str] = set()
    for override in overrides:
        try:
            outcome = apply_game_override(root, override)
        except Exception:
            logger.exception("Failed applying override for %s", override.game_id)
            rc = 1
            continue
        logger.info("%s", outcome)
        if outcome.startswith("SKIP"):
            rc = max(rc, 1)
        else:
            overridden.add(override.game_id)
    logger.info(
        "Game overrides: %d of %d reviewed game(s) in place from %s",
        len(overridden),
        len(overrides),
        overrides_dir,
    )

    remaining = [u for u in unresolved if u.game_id not in overridden]
    for u in (u for u in remaining if u.game_id in override_ids):
        logger.warning(
            "UNRESOLVED %s:%d %s %s: %s", u.file, u.line, u.game_id, u.reason, u.record
        )
    logger.info(
        "NLB box-score normalization: %d correction(s), %d unresolved record(s)",
        len(corrections),
        len(remaining),
    )
    return rc


if __name__ == "__main__":
    sys.exit(main())

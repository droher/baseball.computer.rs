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

* WS1191105040 (`events/1911WS1.EVA`) — bottom of 2nd, a 5-to-3 putout is
  encoded as `5-3.2-3` (dash-separated fielders), but Retrosheet's modern
  event grammar concatenates the fielders (`53`). The same batter-fielder
  combo appears as `53` later in the same file (line 136). Rewrite the
  fielding code while preserving the explicit `2-3` runner advance.

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
import logging
import os
import sys
from dataclasses import dataclass
from pathlib import Path

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class Patch:
    relative_path: str
    game_id: str
    before: str
    after: str
    rationale: str


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


def find_game_block(lines: list[str], game_id: str) -> tuple[int, int] | None:
    """Return [start, end) line indices for the game block, or None."""
    start = None
    for i, line in enumerate(lines):
        if line.startswith(f"id,{game_id}"):
            start = i
            break
    if start is None:
        return None
    end = len(lines)
    for j in range(start + 1, len(lines)):
        if lines[j].startswith("id,"):
            end = j
            break
    return (start, end)


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

    block = find_game_block([line.rstrip("\r\n") for line in lines], patch.game_id)
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
    tmp_path = target.with_suffix(target.suffix + ".tmp")
    new_bytes = "".join(lines).encode("ascii")
    _ = tmp_path.write_bytes(new_bytes)
    os.replace(tmp_path, target)
    return (
        f"PATCH {patch.game_id} in {patch.relative_path} "
        f"({len(matched_idx)} line(s); reason: {patch.rationale})"
    )


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
    return rc


if __name__ == "__main__":
    sys.exit(main())

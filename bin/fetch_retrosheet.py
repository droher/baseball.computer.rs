"""Assemble a Retrosheet source tree from retrosheet.org.

Replacement for sourcing data via `alldata.zip`, which retrosheet refreshes
only sporadically (the 2026-04 download still carried 2025-01 internal
timestamps and stopped at the 2024 season). This script enumerates the
canonical per-year and bundle URLs from retrosheet.org directly so the
output mirrors what the site publishes today.

Per-year URLs are discovered at runtime by parsing the relevant retrosheet
index pages (`/game.htm`, `/gamelogs/index.html`, `/schedule/index.html`).
The script does not hardcode the earliest published year for any asset
class — retrosheet adds new years (and occasionally backfills older years)
on its own cadence, and discovery picks those up automatically. As a
consequence, every URL the script attempts is one retrosheet has linked
on its own pages, so any 404 during a run is a real failure that aborts
with a non-zero exit code.

Output layout matches what the Rust parser and `bin/simple_files.py`
expect (the same shape `alldata.zip` produces):

    retrosheet/
      events/{YYYY}{TEAM}.EV[AN]
      boxes/{YYYY}.EB[AN]
      ebe/{YYYY}.EBE
      allstar/{YYYY}AS.EVE
      postseason/{YYYY}*.EVE
      ngl_e/*.EV[FR]
      ngl_b/*.EB[FR]
      rosters/{TEAM}{YYYY}.ROS
      gamelogs/gl{YYYY}.txt
      schedules/{YYYY}schedule.csv
      discrepancies/*discs.csv
      biofile.csv, biofile0.csv, ballparks.csv, teams.csv, ...

`umpires/UMPIRES{YYYY}.txt` and `teams/team{YYYY}*.csv` from `alldata.zip`
are intentionally not mirrored: retrosheet does not publish those as
individually-downloadable per-year files, and neither the Rust parser nor
`bin/simple_files.py` reads them. The consolidated `umpires0.csv` (in
`downloads/biodata.zip`) and `teams.csv` (in `teams.zip`) are fetched.

Run with `uv run python bin/fetch_retrosheet.py -o retrosheet`.
"""

# pyright: reportAny=false, reportUnusedCallResult=false

from __future__ import annotations

import argparse
import io
import logging
import os
import re
import shutil
import sys
import threading
import time
import urllib.error
import urllib.parse
import urllib.request
import zipfile
from collections.abc import Iterable, Mapping, Sequence
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from pathlib import Path
from typing import cast

LOG = logging.getLogger("fetch_retrosheet")

USER_AGENT = (
    "baseball.computer.rs/fetch_retrosheet "
    "(+https://github.com/droher/baseball.computer.rs)"
)
HTTP_TIMEOUT_S = 180
RETRY_ATTEMPTS = 4
RETRY_BACKOFF_S = 3.0
DEFAULT_PARALLELISM = 8

# Index pages whose <a href> links describe the per-year zips retrosheet
# currently publishes for each asset class.
INDEX_URLS: tuple[str, ...] = (
    "https://www.retrosheet.org/game.htm",
    "https://www.retrosheet.org/gamelogs/index.html",
    "https://www.retrosheet.org/schedule/index.html",
)

# Decade discrepancy zips. Retrosheet publishes one zip per decade up to
# 1980s; later decades have no published discrepancies file. These URLs
# are not enumerated on a clean index page so we list them here directly.
DISCREPANCY_DECADE_STARTS: tuple[int, ...] = (
    1890,
    1900,
    1910,
    1920,
    1930,
    1940,
    1950,
    1960,
    1970,
    1980,
)

# Static (non-year-keyed) URLs: all-time bundles and reference assets.
BUNDLE_SOURCES: tuple[tuple[str, str], ...] = (
    # All-time event/score bundles, refreshed each season.
    ("https://www.retrosheet.org/events/allas.zip", "allstar"),
    ("https://www.retrosheet.org/events/allpost.zip", "postseason"),
    ("https://www.retrosheet.org/events/allebe.zip", "ebe"),
    ("https://www.retrosheet.org/events/allevr.zip", "ngl_e"),
    ("https://www.retrosheet.org/events/allebr.zip", "ngl_b"),
    # Reference data — extract members at the output root.
    ("https://www.retrosheet.org/biofile.zip", ""),
    ("https://www.retrosheet.org/ballparks.zip", ""),
    ("https://www.retrosheet.org/teams.zip", ""),
    ("https://www.retrosheet.org/rosters.zip", "rosters"),
    ("https://www.retrosheet.org/ejections.zip", ""),
    ("https://www.retrosheet.org/downloads/biodata.zip", ""),
)

# Members with these extensions always land in the same subdir regardless
# of the source bundle's hint. Everything else honors the source's hint.
# `.EVR`/`.EBR` (Negro Leagues) are forced into ngl_e/ngl_b so the per-year
# `{year}eve.zip` copy (member name `{year}NGL.EVR`) collides with the
# bundle copy from `allevr.zip` (member name `{year}.EVR`) instead of
# producing two parser inputs with the same content.
_EXTENSION_OVERRIDES: dict[str, str] = {
    ".ROS": "rosters",
    ".EBE": "ebe",
    ".EVR": "ngl_e",
    ".EBR": "ngl_b",
}

_HREF_RE = re.compile(r'href\s*=\s*["\']([^"\']+)["\']', re.IGNORECASE)

# Per-asset-class URL patterns used to categorize discovered links.
_CATEGORY_PATTERNS: dict[str, re.Pattern[str]] = {
    "events": re.compile(r"/events/(\d{4})eve\.zip$", re.IGNORECASE),
    "boxes": re.compile(r"/events/(\d{4})box\.zip$", re.IGNORECASE),
    "gamelogs": re.compile(r"/gamelogs/gl(\d{4})\.zip$", re.IGNORECASE),
    "schedules": re.compile(r"/schedule/(\d{4})SKED\.zip$", re.IGNORECASE),
}

# Each per-year category maps to the destination subdir for that class.
_CATEGORY_HINT_SUBDIR: dict[str, str] = {
    "events": "events",
    "boxes": "boxes",
    "gamelogs": "gamelogs",
    "schedules": "schedules",
}


@dataclass(frozen=True)
class Source:
    """One Retrosheet asset to fetch and extract.

    `hint_subdir` is the destination subdir under the output root for
    members whose extension does not uniquely identify a different home
    (e.g. `.ROS` always lands in `rosters/`, `.EBE` always in `ebe/`).
    Empty string means "place at output root".
    """

    url: str
    hint_subdir: str


def http_get(url: str) -> bytes:
    """GET `url` with retries on transient errors. Re-raises 4xx (incl. 404)."""
    last_exc: BaseException | None = None
    for attempt in range(1, RETRY_ATTEMPTS + 1):
        req = urllib.request.Request(url, headers={"User-Agent": USER_AGENT})
        try:
            LOG.debug("GET %s (attempt %d)", url, attempt)
            with urllib.request.urlopen(req, timeout=HTTP_TIMEOUT_S) as resp:
                return cast(bytes, resp.read())
        except urllib.error.HTTPError as exc:
            if 400 <= exc.code < 500:
                raise
            last_exc = exc
        except (urllib.error.URLError, TimeoutError, ConnectionError) as exc:
            last_exc = exc
        if attempt < RETRY_ATTEMPTS:
            sleep_s = RETRY_BACKOFF_S * (2 ** (attempt - 1))
            LOG.warning(
                "transient on %s: %s — retrying in %.1fs", url, last_exc, sleep_s
            )
            time.sleep(sleep_s)
    raise RuntimeError(
        f"GET {url} failed after {RETRY_ATTEMPTS} attempts"
    ) from last_exc


def parse_page_links(html: str, base_url: str) -> Iterable[tuple[str, str]]:
    """Yield (category, absolute_url) for every per-year zip linked in `html`.

    Pure function — no I/O. The category names match `_CATEGORY_PATTERNS`.
    """
    for m in _HREF_RE.finditer(html):
        absolute = urllib.parse.urljoin(base_url, m.group(1))
        for cls, pat in _CATEGORY_PATTERNS.items():
            if pat.search(absolute):
                yield cls, absolute
                break


def discover_per_year_urls(
    index_urls: Sequence[str] = INDEX_URLS,
) -> dict[str, list[str]]:
    """Fetch each index page and return per-category sorted, deduped URLs."""
    by_category: dict[str, set[str]] = {k: set() for k in _CATEGORY_PATTERNS}
    for index in index_urls:
        LOG.info("Discovering links from %s", index)
        html = http_get(index).decode("utf-8", errors="replace")
        for cls, url in parse_page_links(html, index):
            by_category[cls].add(url)
    counts = {cls: len(urls) for cls, urls in by_category.items()}
    LOG.info("Discovered: %s", counts)
    if not all(by_category.values()):
        empty = [k for k, v in by_category.items() if not v]
        msg = (
            f"No per-year URLs discovered for categories: {empty}. "
            f"Retrosheet may have restructured its index pages."
        )
        raise RuntimeError(msg)
    return {cls: sorted(urls) for cls, urls in by_category.items()}


def build_sources(discovered: Mapping[str, Sequence[str]]) -> list[Source]:
    """Materialize the full URL manifest from discovered URLs + static set."""
    sources = [Source(url, hint) for url, hint in BUNDLE_SOURCES]
    for cls, hint in _CATEGORY_HINT_SUBDIR.items():
        sources.extend(Source(url, hint) for url in discovered[cls])
    sources.extend(
        Source(f"https://www.retrosheet.org/{d}sdis.zip", "discrepancies")
        for d in DISCREPANCY_DECADE_STARTS
    )
    return sources


def route_member(filename: str, hint_subdir: str) -> str:
    bare = Path(filename).name
    if not bare:
        return hint_subdir
    suffix = Path(bare).suffix.upper()
    return _EXTENSION_OVERRIDES.get(suffix, hint_subdir)


def normalize_target_name(bare: str, subdir: str) -> str:
    """Normalize file extensions for downstream-glob compatibility.

    `bin/simple_files.py` globs `gamelogs/*.txt` and `schedules/*.csv`
    (lowercase, case-sensitive on Linux CI runners). Retrosheet's per-year
    schedule zips occasionally ship `.TXT` members; `alldata.zip` ships
    them as `.csv`. Normalize to the lowercase form expected downstream.

    NGL files arrive under two names depending on source: the all-time
    bundle `allevr.zip` uses `{year}.EVR`, while per-year `{year}eve.zip`
    uses `{year}NGL.EVR`. Both ultimately route to `ngl_e/` (see
    `_EXTENSION_OVERRIDES`); strip the `NGL` infix so the bundle and
    per-year copies collide on the same path instead of producing two
    identical parser inputs. Same logic for `.EBR` -> `ngl_b/`.

    Other subdirs (events, boxes, rosters, ...) are consumed by the Rust
    parser via uppercase-suffix globs, so we preserve their original case.
    """
    if subdir == "gamelogs":
        if bare.lower().endswith(".txt"):
            return bare[:-4] + ".txt"
        return bare
    if subdir == "schedules":
        lower = bare.lower()
        if lower.endswith(".txt") or lower.endswith(".csv"):
            return bare[:-4] + ".csv"
        return bare
    if subdir in ("ngl_e", "ngl_b"):
        m = re.fullmatch(r"(\d{4})NGL(\.[A-Za-z]+)", bare, re.IGNORECASE)
        if m:
            return m.group(1) + m.group(2)
        return bare
    return bare


def extract_zip(payload: bytes, output_root: Path, hint_subdir: str) -> int:
    """Extract every member of `payload` under `output_root`.

    Two concurrency-safety concerns shape the write path:

    1. Multiple sources (an all-time bundle and a per-year zip) deliberately
       collide on the same NGL target path so duplicate-game warnings are
       suppressed. With ThreadPoolExecutor concurrency, two workers can land
       in this function simultaneously targeting the same path. Stage each
       member to a per-thread `.{pid}.{tid}.tmp` sibling and `os.replace`
       onto the final path so a concurrent reader (and the second writer's
       truncate) only ever sees a fully-formed file.

    2. Defense-in-depth against zip-slip: zip members are constrained to
       their leaf filename via `Path(member.filename).name`, but resolve and
       cross-check against `output_root` to make the safety property
       explicit and survive future routing-logic refactors.
    """
    count = 0
    output_root_resolved = output_root.resolve()
    tmp_token = f"{os.getpid()}.{threading.get_ident()}"
    with zipfile.ZipFile(io.BytesIO(payload)) as zf:
        for member in zf.infolist():
            if member.is_dir():
                continue
            bare = Path(member.filename).name
            if not bare:
                continue
            subdir = route_member(bare, hint_subdir)
            normalized = normalize_target_name(bare, subdir)
            target = (
                output_root / subdir / normalized
                if subdir
                else output_root / normalized
            )
            # Defense in depth: ensure the resolved target is inside output_root.
            resolved_parent = target.parent.resolve()
            if not (
                resolved_parent == output_root_resolved
                or output_root_resolved in resolved_parent.parents
            ):
                msg = (
                    f"Refusing to extract member {member.filename!r} outside "
                    f"output root {output_root_resolved}"
                )
                raise RuntimeError(msg)
            target.parent.mkdir(parents=True, exist_ok=True)
            tmp = target.with_name(f"{target.name}.{tmp_token}.tmp")
            with zf.open(member) as src, tmp.open("wb") as dst:
                shutil.copyfileobj(src, dst)
            os.replace(tmp, target)
            count += 1
    return count


def fetch_one(src: Source, output_root: Path) -> tuple[str, int]:
    payload = http_get(src.url)
    n = extract_zip(payload, output_root, src.hint_subdir)
    return (src.url, n)


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "-o",
        "--output",
        type=Path,
        default=Path("retrosheet"),
        help="Destination directory (default: ./retrosheet)",
    )
    parser.add_argument(
        "--parallelism",
        type=int,
        default=DEFAULT_PARALLELISM,
        help=f"Concurrent download workers (default: {DEFAULT_PARALLELISM})",
    )
    parser.add_argument(
        "--log-level",
        default="INFO",
        help="Python logging level name (default: INFO)",
    )
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    log_level: str = args.log_level
    output: Path = args.output
    parallelism: int = args.parallelism

    logging.basicConfig(
        level=log_level.upper(),
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )

    output.mkdir(parents=True, exist_ok=True)
    discovered = discover_per_year_urls()
    sources = build_sources(discovered)
    LOG.info(
        "Fetching %d sources into %s with parallelism=%d",
        len(sources),
        output,
        parallelism,
    )

    started = time.time()
    extracted_total = 0
    failures: list[tuple[str, BaseException]] = []
    with ThreadPoolExecutor(max_workers=parallelism) as pool:
        futures = {pool.submit(fetch_one, src, output): src for src in sources}
        for fut in as_completed(futures):
            src = futures[fut]
            try:
                url, n = fut.result()
            except Exception as exc:  # noqa: BLE001
                failures.append((src.url, exc))
                LOG.error("FAIL %s: %s", src.url, exc)
                continue
            extracted_total += n
            LOG.info("OK %s -> %d files", url, n)

    elapsed = time.time() - started
    LOG.info(
        "Done in %.1fs: extracted=%d files, failed=%d",
        elapsed,
        extracted_total,
        len(failures),
    )
    return 1 if failures else 0


if __name__ == "__main__":
    sys.exit(main())

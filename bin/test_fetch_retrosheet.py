"""Unit tests for `fetch_retrosheet`.

Run with: `uv run python -m unittest bin/test_fetch_retrosheet.py`
"""

# pyright: reportAny=false, reportUnusedCallResult=false, reportMissingImports=false, reportUnknownMemberType=false, reportUnknownVariableType=false, reportUnknownArgumentType=false, reportUnknownLambdaType=false

from __future__ import annotations

import io
import sys
import tempfile
import unittest
import zipfile
from pathlib import Path
from unittest.mock import patch

sys.path.insert(0, str(Path(__file__).parent))

import fetch_retrosheet as fr  # noqa: E402


class RouteMemberTests(unittest.TestCase):
    def test_extension_overrides_take_precedence(self) -> None:
        self.assertEqual(fr.route_member("NYA2024.ROS", "events"), "rosters")
        self.assertEqual(fr.route_member("nya2024.ros", "boxes"), "rosters")
        self.assertEqual(fr.route_member("2024.EBE", "boxes"), "ebe")
        self.assertEqual(fr.route_member("2024.ebe", "events"), "ebe")
        # Negro Leagues files always land in ngl_e/ngl_b regardless of source.
        self.assertEqual(fr.route_member("1937NGL.EVR", "events"), "ngl_e")
        self.assertEqual(fr.route_member("1937.EVR", "ngl_e"), "ngl_e")
        self.assertEqual(fr.route_member("1937NGL.EBR", "boxes"), "ngl_b")
        self.assertEqual(fr.route_member("1937.EBR", "ngl_b"), "ngl_b")

    def test_hint_used_for_non_overridden_extensions(self) -> None:
        self.assertEqual(fr.route_member("2024NYA.EVA", "events"), "events")
        self.assertEqual(fr.route_member("2024.EBA", "boxes"), "boxes")
        self.assertEqual(fr.route_member("1933AS.EVE", "allstar"), "allstar")
        self.assertEqual(fr.route_member("2024NYALS.EVE", "postseason"), "postseason")
        self.assertEqual(fr.route_member("ballparks.csv", ""), "")

    def test_empty_filename_returns_hint(self) -> None:
        self.assertEqual(fr.route_member("", "events"), "events")
        self.assertEqual(fr.route_member("subdir/", "events"), "events")


class NormalizeTargetNameTests(unittest.TestCase):
    def test_gamelogs_lowercases_txt(self) -> None:
        self.assertEqual(
            fr.normalize_target_name("GL2024.TXT", "gamelogs"), "GL2024.txt"
        )
        self.assertEqual(
            fr.normalize_target_name("gl2024.txt", "gamelogs"), "gl2024.txt"
        )

    def test_schedules_force_csv(self) -> None:
        self.assertEqual(
            fr.normalize_target_name("2024SKED.TXT", "schedules"), "2024SKED.csv"
        )
        self.assertEqual(
            fr.normalize_target_name("2024schedule.csv", "schedules"),
            "2024schedule.csv",
        )
        self.assertEqual(
            fr.normalize_target_name("2024SKED.CSV", "schedules"), "2024SKED.csv"
        )

    def test_other_subdirs_preserve_case(self) -> None:
        self.assertEqual(
            fr.normalize_target_name("2024NYA.EVA", "events"), "2024NYA.EVA"
        )
        self.assertEqual(
            fr.normalize_target_name("NYA2024.ROS", "rosters"), "NYA2024.ROS"
        )
        self.assertEqual(
            fr.normalize_target_name("1933AS.EVE", "allstar"), "1933AS.EVE"
        )
        self.assertEqual(fr.normalize_target_name("biofile.csv", ""), "biofile.csv")

    def test_ngl_strips_NGL_infix_to_match_bundle_naming(self) -> None:
        # Per-year `{year}eve.zip` ships `{year}NGL.EVR`; bundle ships
        # `{year}.EVR`. Both must collide on the same path under ngl_e/.
        self.assertEqual(
            fr.normalize_target_name("1937NGL.EVR", "ngl_e"), "1937.EVR"
        )
        self.assertEqual(fr.normalize_target_name("1937.EVR", "ngl_e"), "1937.EVR")
        self.assertEqual(
            fr.normalize_target_name("1942NGL.EBR", "ngl_b"), "1942.EBR"
        )
        # Case-insensitive: defense in depth in case retrosheet ever ships
        # lowercase member names. `route_member` already accepts lowercase.
        self.assertEqual(
            fr.normalize_target_name("1937ngl.evr", "ngl_e"), "1937.evr"
        )
        # Members not matching `{4-digit}NGL.{ext}` are passed through.
        self.assertEqual(fr.normalize_target_name("README.txt", "ngl_e"), "README.txt")


# Sample HTML fragments mimicking retrosheet's index pages. Real pages are
# longer but follow the same href shape.
_GAME_HTM_FRAGMENT = """
<html><body>
<a href="biofile.zip">Bio</a>
<a href="events/1910eve.zip">1910 events</a>
<a href="events/2025eve.zip">2025 events</a>
<a href="events/1871box.zip">1871 box</a>
<a href="events/2025box.zip">2025 box</a>
<a href="events/allas.zip">All-Star bundle</a>
<a href="events/1933as.zip">1933 AS</a>
<!-- per-year AS/postseason zips also appear; we ignore them in favor of the bundle -->
<a href="events/allpost.zip">PS bundle</a>
<a href="game.htm#regular">internal anchor</a>
</body></html>
"""

_GAMELOGS_INDEX_FRAGMENT = """
<html><body>
<a href="gl1871.zip">1871 GL</a>
<a href="gl2025.zip">2025 GL</a>
<a href="gl2020_25.zip">decade bundle (not categorized)</a>
<a href="gl1871_2025.zip">all bundle (not categorized)</a>
</body></html>
"""

_SCHEDULE_INDEX_FRAGMENT = """
<html><body>
<a href="1877SKED.zip">1877 sked</a>
<a href="2025SKED.zip">2025 sked</a>
<a href="2026SKED.zip">2026 sked</a>
</body></html>
"""


class ParsePageLinksTests(unittest.TestCase):
    def test_categorizes_by_url_pattern(self) -> None:
        out = list(
            fr.parse_page_links(
                _GAME_HTM_FRAGMENT, "https://www.retrosheet.org/game.htm"
            )
        )
        # Expected categorized URLs (per-year `as`/`post` zips don't match
        # the categorized patterns, so they're correctly skipped).
        expected = {
            ("events", "https://www.retrosheet.org/events/1910eve.zip"),
            ("events", "https://www.retrosheet.org/events/2025eve.zip"),
            ("boxes", "https://www.retrosheet.org/events/1871box.zip"),
            ("boxes", "https://www.retrosheet.org/events/2025box.zip"),
        }
        self.assertEqual(set(out), expected)

    def test_resolves_relative_hrefs(self) -> None:
        # gamelogs/index.html base → relative `gl2025.zip` becomes absolute.
        out = list(
            fr.parse_page_links(
                _GAMELOGS_INDEX_FRAGMENT,
                "https://www.retrosheet.org/gamelogs/index.html",
            )
        )
        self.assertIn(
            ("gamelogs", "https://www.retrosheet.org/gamelogs/gl1871.zip"), out
        )
        self.assertIn(
            ("gamelogs", "https://www.retrosheet.org/gamelogs/gl2025.zip"), out
        )
        # Decade and full bundles deliberately not categorized — they don't
        # match `gl(\d{4}).zip$`.
        for _, url in out:
            self.assertNotIn("gl2020_25.zip", url)
            self.assertNotIn("gl1871_2025.zip", url)

    def test_each_url_categorized_at_most_once(self) -> None:
        urls = [url for _, url in fr.parse_page_links(_GAME_HTM_FRAGMENT, "x")]
        self.assertEqual(len(urls), len(set(urls)))


class DiscoverPerYearUrlsTests(unittest.TestCase):
    def test_aggregates_across_index_pages(self) -> None:
        # Inject an in-memory http_get so the test doesn't touch the network.
        # `unittest.mock.patch` restores the original on exit even if the body
        # raises (vs. a manual save/finally that can leave global state torn).
        fragments = {
            "https://www.retrosheet.org/game.htm": _GAME_HTM_FRAGMENT,
            "https://www.retrosheet.org/gamelogs/index.html": _GAMELOGS_INDEX_FRAGMENT,
            "https://www.retrosheet.org/schedule/index.html": _SCHEDULE_INDEX_FRAGMENT,
        }

        with patch.object(
            fr, "http_get", side_effect=lambda url: fragments[url].encode("utf-8")
        ):
            discovered = fr.discover_per_year_urls(tuple(fragments.keys()))

        self.assertEqual(
            set(discovered.keys()), {"events", "boxes", "gamelogs", "schedules"}
        )
        self.assertEqual(
            discovered["events"],
            sorted(
                [
                    "https://www.retrosheet.org/events/1910eve.zip",
                    "https://www.retrosheet.org/events/2025eve.zip",
                ]
            ),
        )
        self.assertEqual(
            discovered["boxes"],
            sorted(
                [
                    "https://www.retrosheet.org/events/1871box.zip",
                    "https://www.retrosheet.org/events/2025box.zip",
                ]
            ),
        )
        self.assertEqual(
            discovered["gamelogs"],
            sorted(
                [
                    "https://www.retrosheet.org/gamelogs/gl1871.zip",
                    "https://www.retrosheet.org/gamelogs/gl2025.zip",
                ]
            ),
        )
        self.assertEqual(
            discovered["schedules"],
            sorted(
                [
                    "https://www.retrosheet.org/schedule/1877SKED.zip",
                    "https://www.retrosheet.org/schedule/2025SKED.zip",
                    "https://www.retrosheet.org/schedule/2026SKED.zip",
                ]
            ),
        )

    def test_raises_when_a_category_is_empty(self) -> None:
        # An index that yields no links for any category should fail loud,
        # signaling that retrosheet may have restructured.
        with patch.object(
            fr,
            "http_get",
            side_effect=lambda url: b"<html><body><a href='unrelated.zip'>x</a></body></html>",
        ):
            with self.assertRaises(RuntimeError):
                fr.discover_per_year_urls(("https://www.retrosheet.org/game.htm",))


class BuildSourcesTests(unittest.TestCase):
    def test_manifest_composition_matches_inputs(self) -> None:
        discovered = {
            "events": ["https://www.retrosheet.org/events/2025eve.zip"],
            "boxes": ["https://www.retrosheet.org/events/2025box.zip"],
            "gamelogs": ["https://www.retrosheet.org/gamelogs/gl2025.zip"],
            "schedules": ["https://www.retrosheet.org/schedule/2025SKED.zip"],
        }
        sources = fr.build_sources(discovered)

        urls = [s.url for s in sources]
        # Invariant: count = bundles + reference + per-year inputs + decade dis
        per_year_count = sum(len(v) for v in discovered.values())
        expected_count = (
            len(fr.BUNDLE_SOURCES) + per_year_count + len(fr.DISCREPANCY_DECADE_STARTS)
        )
        self.assertEqual(len(sources), expected_count)
        # Every URL is unique.
        self.assertEqual(len(urls), len(set(urls)))

        # Bundles all present.
        for url, _hint in fr.BUNDLE_SOURCES:
            self.assertIn(url, urls)

        # Discovered per-year URLs all present with correct hint subdir.
        url_to_source = {s.url: s for s in sources}
        for cls, urls_for_cls in discovered.items():
            for url in urls_for_cls:
                self.assertIn(url, url_to_source)
                self.assertEqual(
                    url_to_source[url].hint_subdir, fr._CATEGORY_HINT_SUBDIR[cls]
                )

        # All discrepancy decade zips present and routed to discrepancies/.
        for d in fr.DISCREPANCY_DECADE_STARTS:
            url = f"https://www.retrosheet.org/{d}sdis.zip"
            self.assertIn(url, url_to_source)
            self.assertEqual(url_to_source[url].hint_subdir, "discrepancies")


class ExtractZipTests(unittest.TestCase):
    @staticmethod
    def _make_zip(members: dict[str, bytes]) -> bytes:
        buf = io.BytesIO()
        with zipfile.ZipFile(buf, "w", zipfile.ZIP_DEFLATED) as zf:
            for name, data in members.items():
                zf.writestr(name, data)
        return buf.getvalue()

    def test_routes_event_and_roster_members_separately(self) -> None:
        payload = self._make_zip(
            {
                "2024NYA.EVA": b"id,1\n",
                "2024BOS.EVA": b"id,2\n",
                "NYA2024.ROS": b"row\n",
                "BOS2024.ROS": b"row\n",
            }
        )
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            n = fr.extract_zip(payload, root, hint_subdir="events")
            self.assertEqual(n, 4)
            self.assertTrue((root / "events" / "2024NYA.EVA").is_file())
            self.assertTrue((root / "events" / "2024BOS.EVA").is_file())
            self.assertTrue((root / "rosters" / "NYA2024.ROS").is_file())
            self.assertTrue((root / "rosters" / "BOS2024.ROS").is_file())
            self.assertFalse(
                any(p.suffix.upper() == ".ROS" for p in (root / "events").iterdir())
            )
            self.assertFalse(
                any(p.suffix.upper() == ".EVA" for p in (root / "rosters").iterdir())
            )

    def test_routes_ebe_to_canonical_dir(self) -> None:
        payload = self._make_zip(
            {"2024.EBA": b"a\n", "2024.EBN": b"b\n", "2024.EBE": b"c\n"}
        )
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            n = fr.extract_zip(payload, root, hint_subdir="boxes")
            self.assertEqual(n, 3)
            self.assertTrue((root / "boxes" / "2024.EBA").is_file())
            self.assertTrue((root / "boxes" / "2024.EBN").is_file())
            self.assertTrue((root / "ebe" / "2024.EBE").is_file())

    def test_root_destination_when_hint_empty(self) -> None:
        payload = self._make_zip({"ballparks.csv": b"PARKID,...\n"})
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            n = fr.extract_zip(payload, root, hint_subdir="")
            self.assertEqual(n, 1)
            self.assertTrue((root / "ballparks.csv").is_file())

    def test_schedule_txt_normalized_to_csv(self) -> None:
        payload = self._make_zip({"2024SKED.TXT": b"date,...\n"})
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            n = fr.extract_zip(payload, root, hint_subdir="schedules")
            self.assertEqual(n, 1)
            self.assertTrue((root / "schedules" / "2024SKED.csv").is_file())
            self.assertFalse((root / "schedules" / "2024SKED.TXT").exists())

    def test_zip_directory_entries_skipped(self) -> None:
        buf = io.BytesIO()
        with zipfile.ZipFile(buf, "w", zipfile.ZIP_DEFLATED) as zf:
            zf.writestr("events/", b"")
            zf.writestr("2024NYA.EVA", b"x\n")
        payload = buf.getvalue()
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            n = fr.extract_zip(payload, root, hint_subdir="events")
            self.assertEqual(n, 1)
            self.assertTrue((root / "events" / "2024NYA.EVA").is_file())

    def test_rejects_zip_member_escaping_output_root(self) -> None:
        # Even if a malicious zip used `..` in member names, the leaf-name
        # extraction in extract_zip should keep us inside output_root. We
        # also verify the explicit defense-in-depth check rejects any
        # member whose resolved parent would escape (covered indirectly by
        # the leaf-name strip but exercised here for regression safety).
        payload = self._make_zip({"../etc/passwd": b"root:x:0:0\n"})
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            n = fr.extract_zip(payload, root, hint_subdir="events")
            # Member's leaf name is `passwd`, so it lands under events/.
            self.assertEqual(n, 1)
            self.assertTrue((root / "events" / "passwd").is_file())
            # Critically, nothing escapes the temp root.
            self.assertFalse((root.parent / "etc" / "passwd").exists())

    def test_concurrent_writes_to_same_target_do_not_corrupt(self) -> None:
        # Simulate the NGL collision case: bundle and per-year zips both
        # target ngl_e/1937.EVR. Multiple workers extracting the same path
        # concurrently must end with one valid copy on disk (atomic rename).
        import threading as _t

        payload_a = self._make_zip({"1937.EVR": b"A" * 1024})
        payload_b = self._make_zip({"1937NGL.EVR": b"B" * 1024})
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            results: list[Exception | None] = []

            def run(pl: bytes) -> None:
                try:
                    _ = fr.extract_zip(pl, root, hint_subdir="ngl_e")
                    results.append(None)
                except Exception as e:  # noqa: BLE001
                    results.append(e)

            threads = [
                _t.Thread(target=run, args=(payload_a,)),
                _t.Thread(target=run, args=(payload_b,)),
            ]
            for tt in threads:
                tt.start()
            for tt in threads:
                tt.join()
            self.assertEqual(results, [None, None])
            target = root / "ngl_e" / "1937.EVR"
            self.assertTrue(target.is_file())
            # Whichever wins, it must be one of the two complete payloads —
            # never a torn write.
            content = target.read_bytes()
            self.assertIn(content, (b"A" * 1024, b"B" * 1024))
            # No leftover .tmp staging files.
            self.assertEqual(
                [p for p in (root / "ngl_e").iterdir() if p.suffix == ".tmp"], []
            )

    def test_overwrites_existing_files(self) -> None:
        payload_a = self._make_zip({"2024NYA.EVA": b"first\n"})
        payload_b = self._make_zip({"2024NYA.EVA": b"second\n"})
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            fr.extract_zip(payload_a, root, hint_subdir="events")
            fr.extract_zip(payload_b, root, hint_subdir="events")
            self.assertEqual(
                (root / "events" / "2024NYA.EVA").read_bytes(), b"second\n"
            )


if __name__ == "__main__":
    unittest.main()

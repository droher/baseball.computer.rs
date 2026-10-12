from __future__ import annotations

import tempfile
import unittest
from pathlib import Path

import patch_known_corpus_bugs as pk

DUPLICATED = [
    "id,PRG193512012",
    "info,number,2",
    "info,wp,grifr101",
    "id,PRG193512012",
    "info,number,2",
    "info,wp,paigs101",
]

RENAME = pk.Patch(
    relative_path="ngl_e/1935.EVR",
    game_id="PRG193512012",
    context="info,wp,grifr101",
    before="id,PRG193512012",
    after="id,PRG193512011",
    rationale="test",
)
RENUMBER = pk.Patch(
    relative_path="ngl_e/1935.EVR",
    game_id="PRG193512011",
    before="info,number,2",
    after="info,number,1",
    rationale="test",
)


def write_corpus(root: Path) -> Path:
    target = root / "ngl_e" / "1935.EVR"
    target.parent.mkdir()
    _ = target.write_bytes(("\r\n".join(DUPLICATED) + "\r\n").encode())
    return target


def read_lines(target: Path) -> list[str]:
    return target.read_bytes().decode().split("\r\n")[:-1]


class ApplyPatchTests(unittest.TestCase):
    def test_context_selects_matching_duplicate(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            target = write_corpus(root)
            self.assertTrue(pk.apply_patch(root, RENAME).startswith("PATCH"))
            self.assertTrue(pk.apply_patch(root, RENUMBER).startswith("PATCH"))
            self.assertEqual(
                read_lines(target),
                [
                    "id,PRG193512011",
                    "info,number,1",
                    "info,wp,grifr101",
                    "id,PRG193512012",
                    "info,number,2",
                    "info,wp,paigs101",
                ],
            )

    def test_rerun_is_noop(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            target = write_corpus(root)
            _ = pk.apply_patch(root, RENAME)
            _ = pk.apply_patch(root, RENUMBER)
            patched = target.read_bytes()
            self.assertTrue(pk.apply_patch(root, RENAME).startswith("NOOP"))
            self.assertTrue(pk.apply_patch(root, RENUMBER).startswith("NOOP"))
            self.assertEqual(target.read_bytes(), patched)

    def test_missing_context_is_skipped(self) -> None:
        patch = pk.Patch(
            relative_path=RENAME.relative_path,
            game_id=RENAME.game_id,
            context="info,wp,nobody01",
            before=RENAME.before,
            after=RENAME.after,
            rationale="test",
        )
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            target = write_corpus(root)
            self.assertTrue(pk.apply_patch(root, patch).startswith("SKIP"))
            self.assertEqual(read_lines(target), DUPLICATED)


BOX_FILE = "ngl_b/1935.EBR"
UPSTREAM_GAME = [
    "id,AAA193507010",
    'start,aaaa101,"A A",0,1,',
    "stat,bline,aaaa101,0,1,1",
]
FIXED_GAME = [
    "id,AAA193507010",
    'start,aaaa101,"A A",0,1,7',
    "stat,bline,aaaa101,0,1,1",
]
OTHER_GAME = ["id,BBB193507020", 'start,bbbb101,"B B",0,1,7']


def write_box_corpus(root: Path, game: list[str]) -> Path:
    target = root / BOX_FILE
    target.parent.mkdir()
    _ = target.write_bytes(("\r\n".join(game + OTHER_GAME) + "\r\n").encode())
    return target


def write_override(overrides: Path, explanation: str) -> pk.GameOverride:
    overrides.mkdir()
    record = overrides / "AAA193507010.EBR"
    _ = record.write_text("\n".join(FIXED_GAME) + "\n")
    pk.write_game_diff(
        record.with_suffix(".diff"), BOX_FILE, explanation, UPSTREAM_GAME, FIXED_GAME
    )
    return pk.load_game_override(record)


class GameOverrideTests(unittest.TestCase):
    def test_load_reads_target_and_explanation(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            override = write_override(
                Path(tmp) / "games", "Position from dline.\n\nSecond paragraph."
            )
            self.assertEqual(override.game_id, "AAA193507010")
            self.assertEqual(override.relative_path, BOX_FILE)
            self.assertEqual(
                override.explanation, "Position from dline.\n\nSecond paragraph."
            )
            self.assertEqual(override.lines, FIXED_GAME)

    def test_replaces_matching_upstream_block_and_keeps_line_endings(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            target = write_box_corpus(root, UPSTREAM_GAME)
            override = write_override(root / "games", "why")
            self.assertTrue(
                pk.apply_game_override(root, override).startswith("OVERRIDE")
            )
            self.assertEqual(
                target.read_bytes().decode(),
                "\r\n".join(FIXED_GAME + OTHER_GAME) + "\r\n",
            )
            patched = target.read_bytes()
            self.assertTrue(pk.apply_game_override(root, override).startswith("NOOP"))
            self.assertEqual(target.read_bytes(), patched)

    def test_changed_upstream_block_is_skipped(self) -> None:
        changed = [*UPSTREAM_GAME[:2], "stat,bline,aaaa101,0,1,2"]
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            target = write_box_corpus(root, changed)
            original = target.read_bytes()
            override = write_override(root / "games", "why")
            self.assertTrue(
                pk.apply_game_override(root, override).startswith(
                    "SKIP upstream-changed"
                )
            )
            self.assertEqual(target.read_bytes(), original)

    def test_missing_game_is_skipped(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            _ = write_box_corpus(root, ["id,CCC193507030"])
            override = write_override(root / "games", "why")
            self.assertTrue(
                pk.apply_game_override(root, override).startswith("SKIP game-not-found")
            )

    def test_target_outside_corpus_is_skipped(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp) / "corpus"
            root.mkdir()
            outside = pk.GameOverride(
                "AAA193507010", "../1935.EBR", "why", "", FIXED_GAME
            )
            self.assertTrue(
                pk.apply_game_override(root, outside).startswith("SKIP outside-corpus")
            )

    def test_duplicate_game_is_skipped(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            target = write_box_corpus(root, UPSTREAM_GAME + UPSTREAM_GAME)
            original = target.read_bytes()
            override = write_override(root / "games", "why")
            self.assertTrue(
                pk.apply_game_override(root, override).startswith("SKIP duplicate-game")
            )
            self.assertEqual(target.read_bytes(), original)

    def test_non_ascii_file_is_skipped(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            target = write_box_corpus(root, [*UPSTREAM_GAME, 'com,"Jos\u00e9"'])
            _ = target.write_bytes(target.read_text().encode("latin-1"))
            override = write_override(root / "games", "why")
            self.assertTrue(
                pk.apply_game_override(root, override).startswith(
                    "SKIP non-ascii-bytes"
                )
            )

    def test_upstream_lines_reverses_the_diff(self) -> None:
        upstream = [f"line{i}" for i in range(12)]
        upstream[0] = "id,AAA193507010"
        fixed = list(upstream)
        fixed[3] = "changed3"
        fixed[9:10] = ["changed9a", "changed9b"]
        with tempfile.TemporaryDirectory() as tmp:
            record = Path(tmp) / "AAA193507010.EBR"
            _ = record.write_text("\n".join(fixed) + "\n")
            pk.write_game_diff(
                record.with_suffix(".diff"), BOX_FILE, "why", upstream, fixed
            )
            self.assertEqual(pk.upstream_lines(pk.load_game_override(record)), upstream)

    def test_refresh_rewrites_diff_against_corpus_and_keeps_explanation(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            changed = [*UPSTREAM_GAME[:2], "stat,bline,aaaa101,0,1,2"]
            _ = write_box_corpus(root, changed)
            override = write_override(root / "games", "Keep this.")
            record = root / "games" / "AAA193507010.EBR"
            self.assertTrue(pk.refresh_game_diff(record, root).startswith("REFRESH"))
            refreshed = pk.load_game_override(record)
            self.assertEqual(refreshed.explanation, override.explanation)
            self.assertTrue(
                pk.apply_game_override(root, refreshed).startswith("OVERRIDE")
            )

    def test_record_must_start_with_its_game_id(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            record = Path(tmp) / "AAA193507010.EBR"
            _ = record.write_text("\n".join(OTHER_GAME) + "\n")
            with self.assertRaises(ValueError):
                _ = pk.load_game_override(record)


class CommittedGameOverridesTests(unittest.TestCase):
    def test_every_override_matches_its_explained_diff(self) -> None:
        overrides = pk.load_game_overrides(pk.DEFAULT_GAME_OVERRIDES)
        self.assertTrue(overrides)
        for override in overrides:
            with self.subTest(game=override.game_id):
                self.assertTrue(override.explanation)
                diff_lines = override.expected_diff.splitlines()
                self.assertEqual(diff_lines[1], f"+++ b/{override.relative_path}")
                self.assertTrue(any(line.startswith("-") for line in diff_lines[2:]))
                self.assertEqual(
                    pk.render_game_diff(
                        override.relative_path,
                        pk.upstream_lines(override),
                        override.lines,
                    ),
                    override.expected_diff,
                )


if __name__ == "__main__":
    _ = unittest.main()

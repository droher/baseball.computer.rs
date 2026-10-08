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


if __name__ == "__main__":
    _ = unittest.main()

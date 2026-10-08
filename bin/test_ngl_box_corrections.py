from __future__ import annotations

import tempfile
import unittest
from pathlib import Path

import ngl_box_corrections as nbc

GAME = [
    "id,AAA193701010",
    "version,1",
    "start,aaaa101,Al A,0,1,8",
    'start,bbbb101,"B, Bo",0,2,',
    "start,cccc101,Cy C,0,3,79",
    "start,dddd101,Di D,1,1,6",
    "start,eeee101,Ed E,1,2,1",
    "stat,bline,aaaa101,0,1,1,4,1,2,0,0,0,0,0,0,0,0,0,0,0,0,0,0",
    "stat,bline,bbbb101,0,2,NA,4,0,1,0,0,0,0,0,0,0,0,0,0,0,0,0,0",
    "stat,bline,ffff101,0,2,NA,1,1,1,0,0,0,0,0,0,0,0,0,0,0,0,0,0",
    "stat,bline,cccc101,0,3,1,4,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0",
    "stat,bline,dddd101,1,1,1,4,0,1,0,0,0,0,0,0,0,0,0,0,0,0,0,0",
    "stat,bline,eeee101,1,2,1,4,1,1,0,0,0,0,0,0,0,0,0,0,0,0,0,0",
    "stat,dline,aaaa101,0,1,8,27,0,0,0,0,0,0",
    "stat,dline,bbbb101,0,1,4,27,0,0,0,0,0,0",
    "stat,dline,cccc101,0,1,9,27,0,0,0,0,0,0",
    "stat,dline,cccc101,0,NA,7,0,0,0,0,0,0,0",
    "stat,dline,dddd101,1,1,6,27,0,0,0,0,0,0",
    "stat,dline,eeee101,1,1,?,27,0,0,0,0,0,0",
    "stat,pline,eeee101,1,NA,27,0,30,4,0,0,0,2,2,1,0,2,0,0,0,0,0",
    "stat,pline,gggg101,0,NA,27,0,30,4,0,0,0,2,2,1,0,2,0,0,0,0,0",
    "stat,phline,ffff101,0,8,1,1,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0",
    "event,phline,ffff101,7,0,1,1,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0",
    "event,prline,hhhh101,9,0,0,0,0",
    "line,0,0,1,0,1,0,0,0,0,",
    "line,1,0,0,1,0,0,0,0,0,x",
    "NUMBER OF HBP DISAGREE BETWEEN BATTING AND EVENTS",
    "event,hpline,NA,NA,NA",
    "event,hrline,NA,NA,NA,-1,NA,-1",
    "event,hrline,?,aaaa101,eeee101,-1,0,",
    "event,dpline,NA,dddd101,eeee101",
    "Yid,BBB193701020",
    "version,1",
]


def correct(lines: list[str], eol: str = "\n") -> nbc.FileResult:
    return nbc.correct_text(eol.join(lines) + eol, "ngl_b/1937.EBR")


def after_lines(result: nbc.FileResult) -> list[str]:
    return result.text.splitlines()


class CorrectTextTests(unittest.TestCase):
    def test_each_rule_produces_the_derived_record(self) -> None:
        result = correct(GAME)
        out = after_lines(result)
        expected_present = [
            'start,bbbb101,"B, Bo",0,2,4',
            "start,cccc101,Cy C,0,3,9",
            "stat,bline,bbbb101,0,2,1,4,0,1,0,0,0,0,0,0,0,0,0,0,0,0,0,0",
            "stat,bline,ffff101,0,2,2,1,1,1,0,0,0,0,0,0,0,0,0,0,0,0,0,0",
            "stat,dline,cccc101,0,2,7,0,0,0,0,0,0,0",
            "stat,dline,eeee101,1,1,1,27,0,0,0,0,0,0",
            "stat,pline,eeee101,1,1,27,0,30,4,0,0,0,2,2,1,0,2,0,0,0,0,0",
            "stat,pline,gggg101,0,1,27,0,30,4,0,0,0,2,2,1,0,2,0,0,0,0,0",
            "stat,phline,ffff101,8,0,1,1,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0",
            "stat,phline,ffff101,7,0,1,1,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0",
            "stat,prline,hhhh101,9,0,0,0,0",
            "line,0,0,1,0,1,0,0,0,0",
            "line,1,0,0,1,0,0,0,0,0",
            "event,hrline,0,aaaa101,eeee101,-1,0,",
            "event,dpline,1,dddd101,eeee101",
            "id,BBB193701020",
        ]
        for line in expected_present:
            self.assertIn(line, out)
        for removed in ("NUMBER OF", "event,hpline,NA", "event,hrline,NA"):
            self.assertFalse(any(line.startswith(removed) for line in out), removed)
        self.assertEqual(result.unresolved, [])

    def test_corrections_reference_original_lines(self) -> None:
        result = correct(GAME)
        for c in result.corrections:
            self.assertEqual(GAME[c.line - 1], c.before)
        game_of = {c.before: c.game_id for c in result.corrections}
        self.assertEqual(game_of["Yid,BBB193701020"], "BBB193701020")
        self.assertEqual(game_of["event,hpline,NA,NA,NA"], "AAA193701010")

    def test_rerun_is_noop(self) -> None:
        first = correct(GAME)
        second = nbc.correct_text(first.text, "ngl_b/1937.EBR")
        self.assertEqual(second.corrections, [])
        self.assertEqual(second.text, first.text)

    def test_untouched_lines_keep_crlf(self) -> None:
        result = correct(GAME, eol="\r\n")
        self.assertTrue(result.text.endswith("version,1\r\n"))
        self.assertNotIn("\r\r", result.text)
        self.assertEqual(result.text.count("\r\n"), len(result.text.splitlines()))

    def test_clean_game_is_unchanged(self) -> None:
        clean = [
            "id,AAA193701010",
            "start,aaaa101,Al A,0,1,8",
            "stat,bline,aaaa101,0,1,1,4,1,2,0,0,0,0,0,0,0,0,0,0,0,0,0,0",
            "stat,dline,aaaa101,0,1,8,27,0,0,0,0,0,0",
            "line,0,0,1,0,0,0,0,0,0,0",
        ]
        result = correct(clean)
        self.assertEqual(result.corrections, [])
        self.assertEqual(result.text, "\n".join(clean) + "\n")


class UnresolvedTests(unittest.TestCase):
    def assert_unresolved(self, lines: list[str], record: str) -> None:
        result = correct(lines)
        self.assertIn(record, after_lines(result))
        self.assertIn(record, [u.record for u in result.unresolved])

    def test_line_score_disagreeing_with_runs_is_left(self) -> None:
        record = "line,0,0,5,0,"
        self.assert_unresolved(
            [
                "id,AAA193701010",
                "stat,bline,aaaa101,0,1,1,4,1,2,0,0,0,0,0,0,0,0,0,0,0,0,0,0",
                record,
            ],
            record,
        )

    def test_starter_absent_from_dlines_is_left(self) -> None:
        record = "start,aaaa101,Al A,0,1,"
        self.assert_unresolved(["id,AAA193701010", record], record)

    def test_multi_position_starter_must_list_first_dline_position(self) -> None:
        record = "start,aaaa101,Al A,0,1,79"
        self.assert_unresolved(
            [
                "id,AAA193701010",
                record,
                "stat,dline,aaaa101,0,1,8,27,0,0,0,0,0,0",
            ],
            record,
        )

    def test_sequence_column_used_for_other_values_is_left(self) -> None:
        record = "stat,bline,bbbb101,0,1,NA,1,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0"
        self.assert_unresolved(
            [
                "id,AAA193701010",
                "stat,bline,aaaa101,0,1,8,4,1,2,0,0,0,0,0,0,0,0,0,0,0,0,0,0",
                record,
            ],
            record,
        )

    def test_hpline_with_unknown_side_is_left(self) -> None:
        record = "event,hpline,NA,,aaaa101"
        self.assert_unresolved(
            [
                "id,AAA193701010",
                "stat,bline,aaaa101,0,1,1,4,1,2,0,0,0,0,0,0,0,0,0,0,0,0,0,0",
                record,
            ],
            record,
        )

    def test_hrline_with_unknown_batter_side_is_left(self) -> None:
        record = "event,hrline,?,zzzz101,?,-1,0,"
        self.assert_unresolved(["id,AAA193701010", record], record)

    def test_phline_side_disagreeing_with_bline_is_left(self) -> None:
        record = "stat,phline,ffff101,0,8,1,1,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0"
        self.assert_unresolved(
            [
                "id,AAA193701010",
                "stat,bline,ffff101,1,2,2,1,1,1,0,0,0,0,0,0,0,0,0,0,0,0,0,0",
                record,
            ],
            record,
        )


class CorrectDirectoryTests(unittest.TestCase):
    def test_rewrites_files_and_writes_csv(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            (root / "ngl_b").mkdir()
            target = root / "ngl_b" / "1937.EBR"
            _ = target.write_bytes(("\r\n".join(GAME) + "\r\n").encode())
            corrections, unresolved = nbc.correct_directory(root)
            self.assertEqual(unresolved, [])
            self.assertEqual(target.read_bytes().decode(), correct(GAME, "\r\n").text)
            csv_path = root / "corrections.csv"
            nbc.write_corrections_csv(corrections, csv_path)
            rows = csv_path.read_text().splitlines()
            self.assertEqual(rows[0], ",".join(nbc.CSV_FIELDS))
            self.assertEqual(len(rows), len(corrections) + 1)
            again, _ = nbc.correct_directory(root)
            self.assertEqual(again, [])


if __name__ == "__main__":
    _ = unittest.main()

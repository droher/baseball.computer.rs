use std::convert::TryFrom;
use std::fs::File;
use std::io::BufReader;
use std::path::{Path, PathBuf};

use anyhow::{anyhow, Context, Error, Result};
use arrayvec::ArrayString;
use csv::{Reader, ReaderBuilder, StringRecord};
use glob::{glob, Paths, PatternError};
use lazy_regex::{regex, Lazy};
use regex::Regex;
use serde::{Deserialize, Serialize};
use strum_macros::AsRefStr;
use tracing::debug;

use crate::event_file::box_score::{BoxScoreEvent, BoxScoreLine, LineScore};
use crate::event_file::info::InfoRecord;
use crate::event_file::misc::{
    BatHandAdjustment, Comment, EarnedRunRecord, GameId, LineupAdjustment, PitchHandAdjustment,
    PitcherResponsibilityAdjustment, RunnerAdjustment, StartRecord, SubstitutionRecord,
};
use crate::event_file::play::PlayRecord;
use crate::event_file::traits::RetrosheetEventRecord;

pub type RecordSlice = [MappedRecord];

pub static ALL_STAR_GAME: &Lazy<Regex> = regex!(r"[0-9]{4}AS\.EVE$");
pub static WORLD_SERIES: &Lazy<Regex> = regex!(r"[0-9]{4}WS\.EVE$");
pub static LCS: &Lazy<Regex> = regex!(r"[0-9]{4}[AN]LCS\.EVE$");
pub static DIVISION_SERIES: &Lazy<Regex> = regex!(r"[0-9]{4}[AN]LD[12]\.EVE$");
pub static WILD_CARD: &Lazy<Regex> = regex!(r"[0-9]{4}[AN]LW[C1234]\.EVE$");
pub static REGULAR_SEASON: &Lazy<Regex> = regex!(r"[0-9]{4}([[:alnum:]]{3})?\.E[BVD][ANF]$");
pub static NEGRO_LEAGUES: &Lazy<Regex> = regex!(r".*\.E[BV]$");
pub static PLAY_BY_PLAY: &Lazy<Regex> = regex!(r".*\.EV[ANF]?");
pub static DERIVED: &Lazy<Regex> = regex!(r".*\.ED[ANF]?");
pub static BOX_SCORE: &Lazy<Regex> = regex!(r".*\.EB[ANF]?");

#[derive(Debug, Eq, PartialEq, Clone, Copy, Serialize, AsRefStr, Deserialize)]
pub enum AccountType {
    PlayByPlay,
    Deduced,
    BoxScore,
}

impl AccountType {
    pub fn glob(self, input_prefix: &Path) -> Result<Paths, PatternError> {
        let pattern = match self {
            Self::PlayByPlay => "**/*.EV*",
            Self::Deduced => "**/*.ED*",
            Self::BoxScore => "**/*.EB*",
        };
        let input = input_prefix
            .join(Path::new(pattern))
            .to_str()
            .unwrap_or_default()
            .to_string();
        glob(&input)
    }
}

#[derive(Debug, Eq, PartialEq, Clone, Copy, Serialize)]
pub struct FileInfo {
    pub filename: ArrayString<20>,
    pub account_type: AccountType,
    pub file_index: usize,
}

impl FileInfo {
    fn new(path: &Path, file_index: usize) -> Result<Self> {
        let raw_filename = path
            .file_name()
            .unwrap_or_default()
            .to_str()
            .unwrap_or_default()
            .to_string();
        let filename = ArrayString::from(&raw_filename)
            .map_err(|_| anyhow!("Capacity error converting {raw_filename} to array string"))?;
        Ok(Self {
            filename,
            account_type: Self::account_type(&raw_filename),
            file_index,
        })
    }

    pub fn account_type(s: &str) -> AccountType {
        if PLAY_BY_PLAY.is_match(s) {
            AccountType::PlayByPlay
        } else if BOX_SCORE.is_match(s) {
            AccountType::BoxScore
        } else if DERIVED.is_match(s) {
            AccountType::Deduced
        } else {
            panic!("Unexpected file naming convention: {s}")
        }
    }
}

pub struct RecordVec {
    pub record_vec: Vec<MappedRecord>,
    pub line_offset: usize,
}

pub struct RetrosheetReader {
    reader: Reader<BufReader<File>>,
    current_record: StringRecord,
    current_game_id: GameId,
    current_record_vec: Vec<MappedRecord>,
    pub line_offset: usize,
    pub file_info: FileInfo,
}

impl Iterator for RetrosheetReader {
    type Item = Result<RecordVec>;

    fn next(&mut self) -> Option<Self::Item> {
        let did_process_full_game = self.next_game();
        let old_offset = self.line_offset;
        self.line_offset += self.current_record_vec.len();

        let game = match did_process_full_game {
            Err(e) => Some(Err(e)),
            Ok(true) => Some(Ok(self.current_record_vec.drain(..).collect())),
            _ if !&self.current_record_vec.is_empty() => {
                Some(Ok(self.current_record_vec.drain(..).collect()))
            }
            _ => None,
        };
        game.map(|g| {
            g.map(|v| RecordVec {
                record_vec: v,
                line_offset: old_offset,
            })
        })
    }
}

impl RetrosheetReader {
    pub fn new(path: &PathBuf, file_index: usize) -> Result<Self> {
        let mut reader = ReaderBuilder::new()
            .has_headers(false)
            .double_quote(false)
            .flexible(true)
            .from_reader(BufReader::new(File::open(path)?));
        let mut current_record = StringRecord::new();
        let mut line_number = 1;
        // Skip comments at top of 1991 files
        // TODO: Unmess
        loop {
            reader.read_record(&mut current_record)?;
            match MappedRecord::try_from(&current_record)? {
                MappedRecord::Comment(_) => line_number += 1,
                _ => break,
            }
        }
        let current_game_id = match MappedRecord::try_from(&current_record)? {
            MappedRecord::GameId(g) => Ok(g),
            _ => Err(anyhow!(
                "First non-comment record was not a game ID, cannot read file."
            )),
        }?;
        let current_record_vec = Vec::<MappedRecord>::new();
        let file_info = FileInfo::new(path, file_index)?;
        Ok(Self {
            reader,
            current_record,
            current_game_id,
            current_record_vec,
            file_info,
            line_offset: line_number,
        })
    }

    fn next_game(&mut self) -> Result<bool> {
        if self.reader.is_done() {
            return Ok(false);
        }
        self.current_record_vec
            .push(MappedRecord::GameId(self.current_game_id));
        loop {
            let did_read = self.reader.read_record(&mut self.current_record)?;
            // Some Retrosheet files end with the "substitute" char, best to skip it
            if self.current_record.as_slice() == "\u{001A}" {
                debug!("Found substitute char in file {}", self.file_info.filename);
                continue;
            }
            if !did_read {
                return Ok(false);
            }
            let mapped_record = MappedRecord::try_from(&self.current_record);
            match mapped_record {
                Ok(MappedRecord::GameId(g)) => {
                    self.current_game_id = g;
                    return Ok(true);
                }
                Ok(m) => self.current_record_vec.push(m),
                Err(e) => {
                    let line = self.reader.position().line();
                    return Err(anyhow!(
                        "Error file {} line {} during game {} -- Error reading record [{}]: {:#}",
                        &self.file_info.filename,
                        line,
                        &self.current_game_id.id,
                        &self.current_record.iter().collect::<Vec<&str>>().join(","),
                        e
                    ));
                }
            }
        }
    }
}

#[derive(Debug, Eq, PartialEq)]
pub enum MappedRecord {
    GameId(GameId),
    Version,
    Info(InfoRecord),
    Start(StartRecord),
    Substitution(SubstitutionRecord),
    Play(PlayRecord),
    BatHandAdjustment(BatHandAdjustment),
    PitchHandAdjustment(PitchHandAdjustment),
    LineupAdjustment(LineupAdjustment),
    RunnerAdjustment(RunnerAdjustment),
    PitcherResponsibilityAdjustment(PitcherResponsibilityAdjustment),
    EarnedRun(EarnedRunRecord),
    Comment(Comment),
    BoxScoreLine(BoxScoreLine),
    LineScore(LineScore),
    BoxScoreEvent(BoxScoreEvent),
    Unrecognized,
}

impl TryFrom<&RetrosheetEventRecord> for MappedRecord {
    type Error = Error;

    fn try_from(record: &RetrosheetEventRecord) -> Result<Self> {
        let line_type = record.get(0).context("No record")?;
        let mapped = match line_type {
            "id" => Self::GameId(GameId::try_from(record)?),
            "version" => Self::Version,
            "info" => Self::Info(InfoRecord::try_from(record)?),
            "start" => Self::Start(StartRecord::try_from(record)?),
            "sub" => Self::Substitution(SubstitutionRecord::try_from(record)?),
            "play" => Self::Play(PlayRecord::try_from(record)?),
            "badj" => Self::BatHandAdjustment(BatHandAdjustment::try_from(record)?),
            "padj" => Self::PitchHandAdjustment(PitchHandAdjustment::try_from(record)?),
            "ladj" => Self::LineupAdjustment(LineupAdjustment::try_from(record)?),
            "radj" => Self::RunnerAdjustment(RunnerAdjustment::try_from(record)?),
            "presadj" => Self::PitcherResponsibilityAdjustment(
                PitcherResponsibilityAdjustment::try_from(record)?,
            ),
            "com" => Self::Comment(String::from(record.get(1).context("Empty comment")?)),
            "data" => Self::EarnedRun(EarnedRunRecord::try_from(record)?),
            "stat" => Self::BoxScoreLine(BoxScoreLine::try_from(record)?),
            "line" => Self::LineScore(LineScore::try_from(record)?),
            "event" => Self::BoxScoreEvent(BoxScoreEvent::try_from(record)?),
            _ => Self::Unrecognized,
        };
        match mapped {
            Self::Unrecognized => Err(anyhow!("Unrecognized record type {:?}", record)),
            _ => Ok(mapped),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use csv::StringRecord;

    fn rec(fields: &[&str]) -> StringRecord {
        StringRecord::from(fields.to_vec())
    }

    #[test]
    fn play_by_play_extensions_are_classified_correctly() {
        for s in ["2024BOS.EVN", "2024NYA.EVA", "1939NL.EVF"] {
            assert_eq!(FileInfo::account_type(s), AccountType::PlayByPlay);
        }
    }

    #[test]
    fn box_score_extensions_are_classified_correctly() {
        for s in ["2024BOS.EBA", "2024NYA.EBN", "1948.EBR"] {
            assert_eq!(FileInfo::account_type(s), AccountType::BoxScore);
        }
    }

    #[test]
    fn deduced_extensions_are_classified_correctly() {
        for s in ["2024BOS.EDA", "2024NYA.EDN", "1938.EDF"] {
            assert_eq!(FileInfo::account_type(s), AccountType::Deduced);
        }
    }

    #[test]
    #[should_panic(expected = "Unexpected file naming convention")]
    fn unexpected_extension_panics() {
        FileInfo::account_type("README.txt");
    }

    #[test]
    fn account_type_glob_picks_only_matching_extensions() {
        use std::fs::File;

        // Standalone tempdir under the system temp root — we don't pull in `tempfile`
        // here (it's a dev-dep used by integration tests, not unit tests).
        let dir = std::env::temp_dir().join(format!("baseball_glob_test_{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();
        for name in [
            "a.EVA", "b.EVN", "c.EBA", "d.EBN", "e.EDA", "f.EDN", "g.txt",
        ] {
            File::create(dir.join(name)).unwrap();
        }

        let collect = |at: AccountType| -> Vec<String> {
            let mut names: Vec<String> = at
                .glob(&dir)
                .unwrap()
                .filter_map(Result::ok)
                .filter_map(|p| p.file_name().and_then(|s| s.to_str().map(String::from)))
                .collect();
            names.sort();
            names
        };

        assert_eq!(collect(AccountType::PlayByPlay), vec!["a.EVA", "b.EVN"]);
        assert_eq!(collect(AccountType::BoxScore), vec!["c.EBA", "d.EBN"]);
        assert_eq!(collect(AccountType::Deduced), vec!["e.EDA", "f.EDN"]);

        std::fs::remove_dir_all(&dir).unwrap();
    }

    #[test]
    fn mapped_record_classifies_each_record_kind() {
        assert!(matches!(
            MappedRecord::try_from(&rec(&["id", "BOS202404010"])).unwrap(),
            MappedRecord::GameId(_)
        ));
        assert_eq!(
            MappedRecord::try_from(&rec(&["version", "2"])).unwrap(),
            MappedRecord::Version
        );
        assert!(matches!(
            MappedRecord::try_from(&rec(&["info", "visteam", "BOS"])).unwrap(),
            MappedRecord::Info(_)
        ));
        assert!(matches!(
            MappedRecord::try_from(&rec(&["start", "smitj001", "Joe Smith", "0", "1", "5"]))
                .unwrap(),
            MappedRecord::Start(_)
        ));
        assert!(matches!(
            MappedRecord::try_from(&rec(&["sub", "doej101", "Joe Doe", "1", "1", "5"])).unwrap(),
            MappedRecord::Substitution(_)
        ));
        assert!(matches!(
            MappedRecord::try_from(&rec(&["play", "1", "0", "smitj001", "00", "", "S7"])).unwrap(),
            MappedRecord::Play(_)
        ));
        assert!(matches!(
            MappedRecord::try_from(&rec(&["data", "er", "smitj001", "3"])).unwrap(),
            MappedRecord::EarnedRun(_)
        ));
        assert!(matches!(
            MappedRecord::try_from(&rec(&["com", "some comment"])).unwrap(),
            MappedRecord::Comment(_)
        ));
    }

    #[test]
    fn mapped_record_unknown_line_type_errors() {
        assert!(MappedRecord::try_from(&rec(&["bogus", "x"])).is_err());
    }

    #[test]
    fn mapped_record_dispatches_box_score_kinds() {
        // Stat lines route through BoxScoreLine.
        let r = rec(&[
            "stat", "bline", "smitj001", "1", "5", "1", "4", "1", "1", "0", "0", "1", "0", "0",
            "0", "0", "1", "0", "0", "0", "0", "0", "0",
        ]);
        assert!(matches!(
            MappedRecord::try_from(&r).unwrap(),
            MappedRecord::BoxScoreLine(_)
        ));

        // Line scores route through LineScore.
        let r = rec(&["line", "0", "0", "1", "0", "2", "0", "0", "1", "0", "0"]);
        assert!(matches!(
            MappedRecord::try_from(&r).unwrap(),
            MappedRecord::LineScore(_)
        ));

        // Box-score events route through BoxScoreEvent.
        let r = rec(&["event", "hpline", "0", "pitch001", "batt001", ""]);
        assert!(matches!(
            MappedRecord::try_from(&r).unwrap(),
            MappedRecord::BoxScoreEvent(_)
        ));
    }

    #[test]
    fn play_by_play_regex_distinguishes_from_box_score() {
        assert!(PLAY_BY_PLAY.is_match("2024BOS.EVN"));
        assert!(!PLAY_BY_PLAY.is_match("2024BOS.EBN"));
        assert!(BOX_SCORE.is_match("2024BOS.EBN"));
    }
}

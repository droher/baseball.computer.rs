#![allow(dead_code)]
#![forbid(unsafe_code)]
#![deny(clippy::all, clippy::cargo)]
#![warn(
    clippy::nursery,
    clippy::pedantic,
    clippy::unwrap_used,
    clippy::expect_used
)]
#![allow(clippy::module_name_repetitions, clippy::significant_drop_tightening)]

use event_file::schemas::{BoxScoreComments, EventBaserunners, EventComments, EventPitchSequences};
use glob::GlobError;
use itertools::Itertools;
use serde::Serialize;
use std::collections::HashSet;
use std::fs::File;
use std::hash::Hash;
use std::io::{BufWriter, Write};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Mutex, MutexGuard};
use std::time::Instant;

use anyhow::{anyhow, bail, Context, Result};
use clap::Parser;
use csv::{Writer, WriterBuilder};
use either::Either;
use fixed_map::{Key, Map};
use lazy_static::lazy_static;
use rayon::prelude::*;
use strum::IntoEnumIterator;
use strum_macros::{Display, EnumIter};
use tracing::{debug, error, info, warn, Level};
use tracing_subscriber::FmtSubscriber;

use event_file::game_state::GameContext;
use event_file::parser::RetrosheetReader;

use crate::event_file::box_score::{BoxScoreEvent, BoxScoreLine};
use crate::event_file::misc::GameId;
use crate::event_file::parser::{AccountType, MappedRecord, RecordSlice};
use crate::event_file::schemas::{
    BoxScoreLineScores, BoxScoreWritableRecord, ContextToVec, EventAudit, EventFieldingPlays,
    Events, GameEarnedRuns, Games,
};
use crate::event_file::traits::{GameType, EVENT_KEY_BUFFER};

mod event_file;

const ABOUT: &str = "Creates structured datasets from raw Retrosheet files.";

lazy_static! {
    static ref OUTPUT_ROOT: PathBuf = get_output_root(&Opt::parse());
    static ref WRITER_MAP: WriterMap = WriterMap::new(&OUTPUT_ROOT);
    static ref JSON_WRITER: ThreadSafeJsonWriter = ThreadSafeJsonWriter::new();
}

struct ThreadSafeJsonWriter {
    json: Mutex<BufWriter<File>>,
}

impl ThreadSafeJsonWriter {
    #[allow(clippy::expect_used)]
    pub fn new() -> Self {
        let output_path = OUTPUT_ROOT.join("games.jsonl");
        debug!("Creating file {}", output_path.display());
        let file = BufWriter::new(File::create(output_path).expect("Failed to create file"));
        Self {
            json: Mutex::new(file),
        }
    }

    pub fn json(&self) -> Result<MutexGuard<BufWriter<File>>> {
        self.json
            .lock()
            .map_err(|e| anyhow!("Failed to acquire writer lock: {}", e))
    }

    pub fn flush(&self) -> Result<()> {
        let mut json = self.json()?;
        json.flush()?;
        Ok(())
    }
}

struct ThreadSafeCsvWriter {
    csv: Mutex<Writer<File>>,
    has_header_written: AtomicBool,
}
impl ThreadSafeCsvWriter {
    #[allow(clippy::expect_used)]
    pub fn new(schema: EventFileSchema) -> Self {
        let file_name = format!("{schema}.csv");
        let output_path = OUTPUT_ROOT.join(file_name);
        debug!("Creating file {}", output_path.display());
        let csv = WriterBuilder::new()
            .has_headers(!schema.uses_custom_header())
            .from_path(output_path)
            .expect("Failed to create file");
        Self {
            csv: Mutex::new(csv),
            has_header_written: AtomicBool::new(!schema.uses_custom_header()),
        }
    }

    pub fn csv(&self) -> Result<MutexGuard<Writer<File>>> {
        self.csv
            .lock()
            .map_err(|e| anyhow!("Failed to acquire writer lock: {}", e))
    }
}

struct WriterMap {
    output_prefix: PathBuf,
    map: Map<EventFileSchema, ThreadSafeCsvWriter>,
}

impl WriterMap {
    #[allow(clippy::expect_used)]
    fn new(output_prefix: &Path) -> Self {
        let mut map = Map::new();
        for schema in EventFileSchema::iter() {
            map.insert(schema, ThreadSafeCsvWriter::new(schema));
        }
        Self {
            output_prefix: output_prefix.to_path_buf(),
            map,
        }
    }

    fn flush_all(&self) -> Result<Vec<()>> {
        self.map
            .iter()
            .par_bridge()
            .map(|(_, writer)| {
                writer
                    .csv()?
                    .flush()
                    .map_err(|e| anyhow!("Failed to flush writer: {}", e))
            })
            .collect::<Result<Vec<()>>>()
    }

    fn get_csv(&self, schema: EventFileSchema) -> Result<MutexGuard<Writer<File>>> {
        self.map
            .get(schema)
            .context("Failed to initialize writer for schema")?
            .csv()
    }

    fn write_csv<'a, C: ContextToVec<'a>>(
        &self,
        schema: EventFileSchema,
        game_context: &'a GameContext,
    ) -> Result<()> {
        let writer = self
            .map
            .get(schema)
            .context("Failed to initialize writer for schema")?;
        let mut csv = writer.csv()?;
        for row in C::from_game_context(game_context) {
            csv.serialize(row)?;
        }
        Ok(())
    }

    fn write_box_score_line(&self, line: &BoxScoreWritableRecord) -> Result<()> {
        let schema = EventFileSchema::box_score_schema(line)?;
        let writer = self.map.get(schema).context("Failed to get writer")?;
        let mut csv = writer.csv()?;
        if !writer.has_header_written.load(Ordering::Relaxed) {
            let header = line.generate_header()?;
            csv.serialize(header)?;
            writer.has_header_written.store(true, Ordering::Relaxed);
        }
        csv.serialize(line).context("Failed to write line")
    }
}

#[derive(Debug, Eq, PartialEq, Clone, Serialize)]
struct FileInfo {
    pub filename: String,
    pub game_type: GameType,
    pub account_type: AccountType,
    pub file_index: usize,
}

#[derive(Debug, Eq, PartialEq, Copy, Clone, Ord, PartialOrd, Hash, Display, EnumIter, Key)]
#[strum(serialize_all = "snake_case")]
enum EventFileSchema {
    Games,
    GameLineupAppearances,
    GameFieldingAppearances,
    GameEarnedRuns,
    Events,
    EventAudit,
    EventBaserunners,
    EventFieldingPlay,
    EventPitchSequences,
    EventFlags,
    EventComments,
    BoxScoreGames,
    BoxScoreLineScores,
    BoxScoreBattingLines,
    BoxScorePitchingLines,
    BoxScoreFieldingLines,
    BoxScorePinchHittingLines,
    BoxScorePinchRunningLines,
    BoxScoreTeamMiscellaneousLines,
    BoxScoreTeamBattingLines,
    BoxScoreTeamFieldingLines,
    BoxScoreDoublePlays,
    BoxScoreTriplePlays,
    BoxScoreHitByPitches,
    BoxScoreHomeRuns,
    BoxScoreStolenBases,
    BoxScoreCaughtStealing,
    BoxScoreComments,
}

impl EventFileSchema {
    const fn uses_custom_header(self) -> bool {
        matches!(
            self,
            Self::BoxScoreBattingLines
                | Self::BoxScorePitchingLines
                | Self::BoxScoreFieldingLines
                | Self::BoxScorePinchHittingLines
                | Self::BoxScorePinchRunningLines
                | Self::BoxScoreTeamMiscellaneousLines
                | Self::BoxScoreTeamBattingLines
                | Self::BoxScoreTeamFieldingLines
                | Self::BoxScoreDoublePlays
                | Self::BoxScoreTriplePlays
                | Self::BoxScoreHitByPitches
                | Self::BoxScoreHomeRuns
                | Self::BoxScoreStolenBases
                | Self::BoxScoreCaughtStealing
        )
    }

    fn write(
        reader: RetrosheetReader,
        parsed_games: Option<&HashSet<GameId>>,
        use_json: bool,
    ) -> Result<Vec<GameId>> {
        let file_info = reader.file_info;
        debug!("Processing file {}", file_info.filename);

        let mut game_ids = Vec::with_capacity(81);

        for (game_num, record_vec_result) in reader.enumerate() {
            if let Err(e) = record_vec_result {
                error!("{:?}", e);
                continue;
            }
            let record_vec = record_vec_result?;
            let record_slice = &record_vec.record_vec;

            let game_context_result =
                GameContext::new(record_slice, file_info, record_vec.line_offset, game_num);
            if let Err(e) = game_context_result {
                error!("{:?}", e);
                continue;
            }
            let game_context = game_context_result?;
            game_ids.push(game_context.game_id);
            if parsed_games
                .map(|pg| pg.contains(&game_context.game_id))
                .unwrap_or_default()
            {
                warn!(
                    "File {} contains already-processed game {}, ignoring",
                    file_info.filename, &game_context.game_id.id
                );
                continue;
            }
            if use_json {
                let mut json_writer = JSON_WRITER.json()?;
                serde_json::to_writer(&mut *json_writer, &game_context)?;
                json_writer.write("\n".as_bytes())?;
            } else if game_context.file_info.account_type == AccountType::BoxScore {
                Self::write_box_score_files(&game_context, record_slice)?;
            } else {
                Self::write_play_by_play_files(&game_context)?;
            }
        }
        Ok(game_ids)
    }

    fn box_score_schema(line: &BoxScoreWritableRecord) -> Result<Self> {
        Ok(match line.record {
            Either::Left(bsl) => match bsl {
                BoxScoreLine::BattingLine(_) => Self::BoxScoreBattingLines,
                BoxScoreLine::PinchHittingLine(_) => Self::BoxScorePinchHittingLines,
                BoxScoreLine::PinchRunningLine(_) => Self::BoxScorePinchRunningLines,
                BoxScoreLine::PitchingLine(_) => Self::BoxScorePitchingLines,
                BoxScoreLine::DefenseLine(_) => Self::BoxScoreFieldingLines,
                BoxScoreLine::TeamMiscellaneousLine(_) => Self::BoxScoreTeamMiscellaneousLines,
                BoxScoreLine::TeamBattingLine(_) => Self::BoxScoreTeamBattingLines,
                BoxScoreLine::TeamDefenseLine(_) => Self::BoxScoreTeamFieldingLines,
                BoxScoreLine::Unrecognized => bail!("Unrecognized box score line"),
            },
            Either::Right(bse) => match bse {
                BoxScoreEvent::DoublePlay(_) => Self::BoxScoreDoublePlays,
                BoxScoreEvent::TriplePlay(_) => Self::BoxScoreTriplePlays,
                BoxScoreEvent::HitByPitch(_) => Self::BoxScoreHitByPitches,
                BoxScoreEvent::HomeRun(_) => Self::BoxScoreHomeRuns,
                BoxScoreEvent::StolenBase(_) => Self::BoxScoreStolenBases,
                BoxScoreEvent::CaughtStealing(_) => Self::BoxScoreCaughtStealing,
                BoxScoreEvent::Unrecognized => bail!("Unrecognized box score event"),
            },
        })
    }

    fn write_box_score_files(game_context: &GameContext, record_slice: &RecordSlice) -> Result<()> {
        // Write Game
        WRITER_MAP
            .get_csv(Self::BoxScoreGames)?
            .serialize(Games::from(game_context))?;
        // Write Linescores
        let line_scores = record_slice
            .iter()
            .filter_map(|mr| match mr {
                MappedRecord::LineScore(ls) => Some(ls),
                _ => None,
            })
            .flat_map(|ls| BoxScoreLineScores::transform_line_score(game_context.game_id.id, ls));
        let mut w = WRITER_MAP.get_csv(Self::BoxScoreLineScores)?;
        for row in line_scores {
            w.serialize(row)?;
        }
        // Write Comments
        let mut w = WRITER_MAP.get_csv(Self::BoxScoreComments)?;
        for row in BoxScoreComments::from_record_slice(&game_context.game_id.id, record_slice) {
            w.serialize(row)?;
        }
        // Write Lines/Events
        let game_id = game_context.game_id.id;
        let box_score_lines = record_slice
            .iter()
            .filter_map(|mr| match mr {
                MappedRecord::BoxScoreLine(bsl) => Some(Either::Left(bsl)),
                MappedRecord::BoxScoreEvent(bse) => Some(Either::Right(bse)),
                _ => None,
            })
            .map(|record| BoxScoreWritableRecord { game_id, record });

        for line in box_score_lines {
            WRITER_MAP.write_box_score_line(&line)?;
        }
        Ok(())
    }

    fn write_play_by_play_files(game_context: &GameContext) -> Result<()> {
        // Write schemas directly serializable from GameContext
        WRITER_MAP.write_csv::<GameEarnedRuns>(Self::GameEarnedRuns, game_context)?;
        WRITER_MAP.write_csv::<Events>(Self::Events, game_context)?;
        WRITER_MAP.write_csv::<EventAudit>(Self::EventAudit, game_context)?;
        WRITER_MAP.write_csv::<EventFieldingPlays>(Self::EventFieldingPlay, game_context)?;
        WRITER_MAP.write_csv::<EventPitchSequences>(Self::EventPitchSequences, game_context)?;
        WRITER_MAP.write_csv::<EventComments>(Self::EventComments, game_context)?;
        WRITER_MAP.write_csv::<EventBaserunners>(Self::EventBaserunners, game_context)?;
        // Write Game
        WRITER_MAP
            .get_csv(Self::Games)?
            .serialize(Games::from(game_context))?;
        // Write GameLineupAppearance
        let mut w = WRITER_MAP.get_csv(Self::GameLineupAppearances)?;
        for row in &game_context.lineup_appearances {
            w.serialize(row)?;
        }
        // Write GameFieldingAppearance
        let mut w = WRITER_MAP.get_csv(Self::GameFieldingAppearances)?;
        for row in &game_context.fielding_appearances {
            w.serialize(row)?;
        }
        //Write EventFlag
        let mut w = WRITER_MAP.get_csv(Self::EventFlags)?;
        let event_flags = game_context
            .events
            .iter()
            .flat_map(|e| &e.results.play_info);
        for row in event_flags {
            w.serialize(row)?;
        }
        Ok(())
    }
}

#[derive(Parser, Debug)]
#[command(name = "pbp-to-box", about = ABOUT)]
struct Opt {
    #[arg(short, long)]
    input: PathBuf,

    #[arg(short, long)]
    output_dir: PathBuf,

    #[arg(short, long)]
    json: bool,
}

#[allow(clippy::expect_used)]
fn get_output_root(opt: &Opt) -> PathBuf {
    std::fs::create_dir_all(&opt.output_dir).expect("Error occurred on output dir check");
    opt.output_dir
        .canonicalize()
        .expect("Error occurred on output dir canonicalization")
}

struct FileProcessor {
    index: usize,
    opt: Opt,
    game_ids: HashSet<GameId>,
}

impl FileProcessor {
    pub fn new(opt: Opt) -> Self {
        Self {
            index: 0,
            opt,
            game_ids: HashSet::with_capacity(200_000),
        }
    }

    fn process_file(
        input_path: &PathBuf,
        parsed_games: Option<&HashSet<GameId>>,
        file_index: usize,
        use_json: bool,
    ) -> Result<Vec<GameId>> {
        let reader = RetrosheetReader::new(input_path, file_index)?;
        EventFileSchema::write(reader, parsed_games, use_json)
    }

    fn contains_nlb_dupes(path: &PathBuf) -> bool {
        let s = path.to_str().unwrap_or_default();
        if s.ends_with(".EVR") {
            s.contains("allas") || s.contains("allpost")
        } else {
            false
        }
    }

    pub fn par_process_files(&mut self, account_type: AccountType) -> Result<()> {
        // Box score accounts are expected to be duplicates so we don't need to check against them
        let parsed_games = if account_type == AccountType::BoxScore {
            None
        } else {
            Some(&self.game_ids)
        };
        let mut files = account_type
            .glob(&self.opt.input)?
            // TODO: Remove once we remove NLB AS dupes
            .filter_ok(|p| !Self::contains_nlb_dupes(p))
            .collect::<Result<Vec<PathBuf>, GlobError>>()?;
        files.par_sort();
        let file_count = files.len();
        let games = files
            .into_par_iter()
            .enumerate()
            .map(|(i, f)| {
                Self::process_file(
                    &f,
                    parsed_games,
                    (self.index + i) * EVENT_KEY_BUFFER,
                    self.opt.json,
                )
            })
            .collect::<Result<Vec<Vec<GameId>>>>()?;
        self.index += file_count;
        let games = games.iter().flatten();
        self.game_ids.extend(games);
        Ok(())
    }

    pub fn process_files(&mut self) -> Result<()> {
        info!("Parsing conventional play-by-play files");
        self.par_process_files(AccountType::PlayByPlay)?;

        info!("Parsing deduced play-by-play files");
        self.par_process_files(AccountType::Deduced)?;

        info!("Parsing box score files");
        self.par_process_files(AccountType::BoxScore)?;

        WRITER_MAP.flush_all()?;
        JSON_WRITER.flush()?;
        Ok(())
    }
}

#[allow(clippy::expect_used)]
fn main() {
    let subscriber = FmtSubscriber::builder()
        .with_max_level(Level::INFO)
        .finish();
    tracing::subscriber::set_global_default(subscriber).expect("Failed to initialize trace");

    let start = Instant::now();
    let opt: Opt = Opt::parse();

    FileProcessor::new(opt)
        .process_files()
        .expect("Error occurred while processing files");

    let end = start.elapsed();
    info!("Elapsed: {:?}", end);
}

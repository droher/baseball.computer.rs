#![allow(dead_code)]
#![forbid(unsafe_code)]
#![deny(clippy::all, clippy::cargo)]
#![warn(
    clippy::nursery,
    clippy::pedantic,
    clippy::unwrap_used,
    clippy::expect_used
)]
#![allow(
    clippy::module_name_repetitions,
    clippy::significant_drop_tightening,
    // multiple_crate_versions reflects transitive deps (windows-sys, hashbrown,
    // etc.) we don't directly control. Allow at the crate level.
    clippy::multiple_crate_versions
)]

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

use anyhow::{Context, Result, anyhow, bail};
use clap::Parser;
use csv::{Writer, WriterBuilder};
use either::Either;
use fixed_map::{Key, Map};
use rayon::prelude::*;
use strum::IntoEnumIterator;
use strum_macros::{Display, EnumIter};
use tracing::{Level, debug, error, info, warn};
use tracing_subscriber::FmtSubscriber;

use event_file::game_state::GameContext;
use event_file::parser::RetrosheetReader;

use crate::event_file::box_score::{BoxScoreEvent, BoxScoreLine};
use crate::event_file::misc::GameId;
use crate::event_file::parser::{AccountType, MappedRecord, RecordSlice};
use crate::event_file::play::print_cache_info;
use crate::event_file::schemas::{
    BoxScoreLineScores, BoxScoreWritableRecord, ContextToVec, EventAudit, EventFieldingPlays,
    Events, GameEarnedRuns, Games,
};
use crate::event_file::traits::{EVENT_KEY_BUFFER, GameType};

mod event_file;

const ABOUT: &str = "Creates structured datasets from raw Retrosheet files.";

struct ThreadSafeJsonWriter {
    json: Mutex<BufWriter<File>>,
}

impl ThreadSafeJsonWriter {
    pub fn new(output_root: &Path) -> Result<Self> {
        let output_path = output_root.join("games.jsonl");
        debug!("Creating file {}", output_path.display());
        let file = BufWriter::new(
            File::create(&output_path)
                .with_context(|| format!("Failed to create {}", output_path.display()))?,
        );
        Ok(Self {
            json: Mutex::new(file),
        })
    }

    pub fn json(&self) -> Result<MutexGuard<'_, BufWriter<File>>> {
        self.json
            .lock()
            .map_err(|e| anyhow!("Failed to acquire writer lock: {e}"))
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
    pub fn new(schema: EventFileSchema, output_root: &Path) -> Result<Self> {
        let file_name = format!("{schema}.csv");
        let output_path = output_root.join(file_name);
        debug!("Creating file {}", output_path.display());
        let csv = WriterBuilder::new()
            .has_headers(!schema.uses_custom_header())
            .from_path(&output_path)
            .with_context(|| format!("Failed to create {}", output_path.display()))?;
        Ok(Self {
            csv: Mutex::new(csv),
            has_header_written: AtomicBool::new(!schema.uses_custom_header()),
        })
    }

    pub fn csv(&self) -> Result<MutexGuard<'_, Writer<File>>> {
        self.csv
            .lock()
            .map_err(|e| anyhow!("Failed to acquire writer lock: {e}"))
    }
}

struct WriterMap {
    output_prefix: PathBuf,
    map: Map<EventFileSchema, ThreadSafeCsvWriter>,
}

impl WriterMap {
    fn new(output_prefix: &Path) -> Result<Self> {
        let mut map = Map::new();
        for schema in EventFileSchema::iter() {
            map.insert(schema, ThreadSafeCsvWriter::new(schema, output_prefix)?);
        }
        Ok(Self {
            output_prefix: output_prefix.to_path_buf(),
            map,
        })
    }

    fn flush_all(&self) -> Result<Vec<()>> {
        self.map
            .iter()
            .par_bridge()
            .map(|(_, writer)| {
                writer
                    .csv()?
                    .flush()
                    .map_err(|e| anyhow!("Failed to flush writer: {e}"))
            })
            .collect::<Result<Vec<()>>>()
    }

    fn get_csv(&self, schema: EventFileSchema) -> Result<MutexGuard<'_, Writer<File>>> {
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
        in_pass_seen: Option<&Mutex<HashSet<GameId>>>,
        writer_map: &WriterMap,
        json_writer: Option<&ThreadSafeJsonWriter>,
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
                let game_id = if let Some(MappedRecord::GameId(id)) = record_slice.first() {
                    id.id.as_str()
                } else {
                    "unknown"
                };
                let filename = file_info.filename.as_str();
                error!(
                    "Error initializing game {game_id} in file {filename}: {:?}",
                    e
                );
                continue;
            }
            let game_context = game_context_result?;
            game_ids.push(game_context.game_id);
            // Cross-pass dedup: skip games seen in earlier passes.
            if parsed_games
                .is_some_and(|pg| pg.contains(&game_context.game_id))
            {
                warn!(
                    "File {} contains already-processed game {}, ignoring",
                    file_info.filename, &game_context.game_id.id
                );
                continue;
            }
            // Within-pass dedup: claim ownership atomically. The first file to
            // see a game_id within this pass writes it; later occurrences (from
            // a duplicate file or a duplicate within the same file) are skipped.
            if let Some(seen) = in_pass_seen {
                let mut guard = seen
                    .lock()
                    .map_err(|e| anyhow!("in-pass seen-set lock poisoned: {e}"))?;
                if !guard.insert(game_context.game_id) {
                    warn!(
                        "File {} contains duplicate game {}, ignoring later occurrence",
                        file_info.filename, &game_context.game_id.id
                    );
                    continue;
                }
            }
            if let Some(jw) = json_writer {
                let mut handle = jw.json()?;
                serde_json::to_writer(&mut *handle, &game_context)?;
                handle.write_all(b"\n")?;
            } else if game_context.file_info.account_type == AccountType::BoxScore {
                Self::write_box_score_files(&game_context, record_slice, writer_map)?;
            } else {
                Self::write_play_by_play_files(&game_context, writer_map)?;
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

    fn write_box_score_files(
        game_context: &GameContext,
        record_slice: &RecordSlice,
        writer_map: &WriterMap,
    ) -> Result<()> {
        // Write Game
        writer_map
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
        let mut w = writer_map.get_csv(Self::BoxScoreLineScores)?;
        for row in line_scores {
            w.serialize(row)?;
        }
        // Write Comments
        let mut w = writer_map.get_csv(Self::BoxScoreComments)?;
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
            writer_map.write_box_score_line(&line)?;
        }
        Ok(())
    }

    fn write_play_by_play_files(game_context: &GameContext, writer_map: &WriterMap) -> Result<()> {
        // Write schemas directly serializable from GameContext
        writer_map.write_csv::<GameEarnedRuns>(Self::GameEarnedRuns, game_context)?;
        writer_map.write_csv::<Events>(Self::Events, game_context)?;
        writer_map.write_csv::<EventAudit>(Self::EventAudit, game_context)?;
        writer_map.write_csv::<EventFieldingPlays>(Self::EventFieldingPlay, game_context)?;
        writer_map.write_csv::<EventPitchSequences>(Self::EventPitchSequences, game_context)?;
        writer_map.write_csv::<EventComments>(Self::EventComments, game_context)?;
        writer_map.write_csv::<EventBaserunners>(Self::EventBaserunners, game_context)?;
        // Write Game
        writer_map
            .get_csv(Self::Games)?
            .serialize(Games::from(game_context))?;
        // Write GameLineupAppearance
        let mut w = writer_map.get_csv(Self::GameLineupAppearances)?;
        for row in &game_context.lineup_appearances {
            w.serialize(row)?;
        }
        // Write GameFieldingAppearance
        let mut w = writer_map.get_csv(Self::GameFieldingAppearances)?;
        for row in &game_context.fielding_appearances {
            w.serialize(row)?;
        }
        //Write EventFlag
        let mut w = writer_map.get_csv(Self::EventFlags)?;
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

fn get_output_root(opt: &Opt) -> Result<PathBuf> {
    std::fs::create_dir_all(&opt.output_dir)
        .with_context(|| format!("Failed to create output dir {}", opt.output_dir.display()))?;
    opt.output_dir
        .canonicalize()
        .with_context(|| format!("Failed to canonicalize {}", opt.output_dir.display()))
}

struct FileProcessor {
    index: usize,
    opt: Opt,
    // Game IDs seen during the PlayByPlay + Deduced passes. Used to skip
    // duplicates across those two passes (a deduced file should not re-emit
    // a game already seen in a PBP file).
    pbp_game_ids: HashSet<GameId>,
    // Game IDs seen during the BoxScore pass. Tracked separately because every
    // PBP game also has a box-score row by design, and we want to write
    // box_score_* tables for those. Cross-file dupes within the box-score pass
    // (e.g. a Negro Leagues `.EBR` file shadowing a standard `.EBN` for the
    // same game_id) are skipped.
    box_game_ids: HashSet<GameId>,
    writer_map: WriterMap,
    json_writer: Option<ThreadSafeJsonWriter>,
}

impl FileProcessor {
    pub fn new(opt: Opt) -> Result<Self> {
        let output_root = get_output_root(&opt)?;
        let writer_map = WriterMap::new(&output_root)?;
        let json_writer = if opt.json {
            Some(ThreadSafeJsonWriter::new(&output_root)?)
        } else {
            None
        };
        Ok(Self {
            index: 0,
            opt,
            pbp_game_ids: HashSet::with_capacity(200_000),
            box_game_ids: HashSet::with_capacity(250_000),
            writer_map,
            json_writer,
        })
    }

    fn process_file(
        input_path: &PathBuf,
        parsed_games: Option<&HashSet<GameId>>,
        in_pass_seen: Option<&Mutex<HashSet<GameId>>>,
        file_index: usize,
        writer_map: &WriterMap,
        json_writer: Option<&ThreadSafeJsonWriter>,
    ) -> Result<Vec<GameId>> {
        let reader = RetrosheetReader::new(input_path, file_index)?;
        EventFileSchema::write(reader, parsed_games, in_pass_seen, writer_map, json_writer)
    }

    fn contains_nlb_dupes(path: &Path) -> bool {
        let s = path.to_str().unwrap_or_default();
        if s.ends_with(".EVR") {
            s.contains("allas") || s.contains("allpost")
        } else {
            false
        }
    }

    pub fn par_process_files(&mut self, account_type: AccountType) -> Result<()> {
        // Cross-pass dedup: PBP + Deduced share one set so the deduced pass
        // skips PBP-covered games; BoxScore uses its own set since every PBP
        // game also has a box-score row by design.
        let parsed_games = match account_type {
            AccountType::PlayByPlay | AccountType::Deduced => Some(&self.pbp_game_ids),
            AccountType::BoxScore => Some(&self.box_game_ids),
        };
        let mut files = account_type
            .glob(&self.opt.input)?
            // TODO: Remove once we remove NLB AS dupes
            .filter_ok(|p| !Self::contains_nlb_dupes(p))
            .collect::<Result<Vec<PathBuf>, GlobError>>()?;
        files.par_sort();
        let file_count = files.len();
        let writer_map = &self.writer_map;
        let json_writer = self.json_writer.as_ref();
        let base_index = self.index;
        // Within-pass dedup: a single shared set claimed atomically per game.
        // Catches both intra-file dups (same game_id twice in one file) and
        // cross-file dups (same game_id in two files within a pass — e.g. an
        // NLB `.EBR` shadowing a standard `.EBN`). The accumulated parsed_games
        // sets are populated AFTER the pass, so cross-pass dedup alone wouldn't
        // catch within-pass collisions.
        let in_pass_seen: Mutex<HashSet<GameId>> =
            Mutex::new(HashSet::with_capacity(file_count * 81));
        let games = files
            .into_par_iter()
            .enumerate()
            .map(|(i, f)| {
                Self::process_file(
                    &f,
                    parsed_games,
                    Some(&in_pass_seen),
                    (base_index + i) * EVENT_KEY_BUFFER,
                    writer_map,
                    json_writer,
                )
            })
            .collect::<Result<Vec<Vec<GameId>>>>()?;
        self.index += file_count;
        let games = games.iter().flatten();
        match account_type {
            AccountType::PlayByPlay | AccountType::Deduced => self.pbp_game_ids.extend(games),
            AccountType::BoxScore => self.box_game_ids.extend(games),
        }
        Ok(())
    }

    pub fn process_files(&mut self) -> Result<()> {
        info!("Parsing conventional play-by-play files");
        self.par_process_files(AccountType::PlayByPlay)?;

        info!("Parsing deduced play-by-play files");
        self.par_process_files(AccountType::Deduced)?;

        info!("Parsing box score files");
        self.par_process_files(AccountType::BoxScore)?;

        self.writer_map.flush_all()?;
        if let Some(jw) = &self.json_writer {
            jw.flush()?;
        }
        Ok(())
    }
}

fn main() -> Result<()> {
    let subscriber = FmtSubscriber::builder()
        .with_max_level(Level::INFO)
        .finish();
    tracing::subscriber::set_global_default(subscriber)
        .context("Failed to initialize tracing subscriber")?;

    let opt = Opt::parse();
    let start = Instant::now();

    FileProcessor::new(opt)?.process_files()?;

    let end = start.elapsed();
    info!("Elapsed: {:?}", end);
    print_cache_info();
    Ok(())
}

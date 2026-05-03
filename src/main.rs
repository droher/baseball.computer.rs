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
use std::cell::RefCell;
use std::collections::{HashMap, HashSet};
use std::fs::File;
use std::hash::Hash;
use std::io::{BufWriter, Write};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Mutex, MutexGuard};
use std::time::Instant;

use anyhow::{Context, Result, anyhow, bail};
use clap::Parser;
use csv::WriterBuilder;
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

// Per-schema appender thread. Each schema owns one writer thread that drains
// a channel of pre-serialized byte chunks into the final CSV file. Worker
// threads serialize game rows into local Vec<u8> buffers and send them; they
// never touch the file directly. Removes the shard concat phase entirely.
struct WriteChunk {
    has_header: bool,
    bytes: Vec<u8>,
}

struct SchemaSink {
    tx: crossbeam_channel::Sender<WriteChunk>,
    has_header_written: AtomicBool,
}

const FILE_BUFFER_BYTES: usize = 1 << 20;
const BATCH_THRESHOLD_BYTES: usize = 64 * 1024;

// Per-thread per-schema accumulator. Workers serialize many games into the
// same buffer and only ship it to the writer thread once it crosses the size
// threshold. Drops per-game allocation and channel-send count by ~100x.
struct ThreadSchemaBuffer {
    writer: csv::Writer<Vec<u8>>,
    is_first_batch: bool,
    has_serialized_anything: bool,
}

thread_local! {
    static THREAD_BUFFERS: RefCell<HashMap<EventFileSchema, ThreadSchemaBuffer>> =
        RefCell::new(HashMap::new());
}

fn fresh_writer(custom_header: bool, has_headers: bool) -> csv::Writer<Vec<u8>> {
    let _ = custom_header;
    WriterBuilder::new()
        .has_headers(has_headers)
        .from_writer(Vec::with_capacity(BATCH_THRESHOLD_BYTES * 2))
}

struct WriterMap {
    sinks: Option<Map<EventFileSchema, SchemaSink>>,
    handles: Mutex<Option<Vec<std::thread::JoinHandle<Result<()>>>>>,
}

fn writer_loop(
    rx: crossbeam_channel::Receiver<WriteChunk>,
    mut file: BufWriter<File>,
    path_display: String,
) -> Result<()> {
    // Body chunks may arrive before the header chunk because workers race on
    // the per-schema header CAS. Buffer body chunks until the header chunk
    // shows up, then drain in receive order.
    let mut header_written = false;
    let mut pending: Vec<Vec<u8>> = Vec::new();
    while let Ok(chunk) = rx.recv() {
        if chunk.has_header {
            file.write_all(&chunk.bytes)
                .with_context(|| format!("Failed to write header to {path_display}"))?;
            header_written = true;
            for buf in pending.drain(..) {
                file.write_all(&buf)
                    .with_context(|| format!("Failed to flush pending body to {path_display}"))?;
            }
        } else if header_written {
            file.write_all(&chunk.bytes)
                .with_context(|| format!("Failed to write body to {path_display}"))?;
        } else {
            pending.push(chunk.bytes);
        }
    }
    if !pending.is_empty() && !header_written {
        // No header chunk ever arrived but body chunks did. Drop them rather
        // than write a headerless CSV — losing rows is the lesser evil here.
        warn!(
            "Writer thread for {path_display} closing with {} pending body chunks but no header. Dropping them.",
            pending.len(),
        );
    } else {
        for buf in pending.drain(..) {
            file.write_all(&buf)
                .with_context(|| format!("Failed to flush pending body to {path_display}"))?;
        }
    }
    file.flush()
        .with_context(|| format!("Failed to flush {path_display}"))?;
    Ok(())
}

impl WriterMap {
    fn new(output_prefix: &Path) -> Result<Self> {
        let mut sinks = Map::new();
        let mut handles = Vec::new();
        for schema in EventFileSchema::iter() {
            let path = output_prefix.join(format!("{schema}.csv"));
            debug!("Creating final file {}", path.display());
            let file = File::create(&path)
                .with_context(|| format!("Failed to create {}", path.display()))?;
            let bw = BufWriter::with_capacity(FILE_BUFFER_BYTES, file);
            let (tx, rx) = crossbeam_channel::unbounded::<WriteChunk>();
            let display = path.display().to_string();
            let handle = std::thread::Builder::new()
                .name(format!("write-{schema}"))
                .spawn(move || writer_loop(rx, bw, display))
                .with_context(|| format!("Failed to spawn writer for {schema}"))?;
            handles.push(handle);
            sinks.insert(
                schema,
                SchemaSink {
                    tx,
                    has_header_written: AtomicBool::new(false),
                },
            );
        }
        Ok(Self {
            sinks: Some(sinks),
            handles: Mutex::new(Some(handles)),
        })
    }

    fn sink(&self, schema: EventFileSchema) -> Result<&SchemaSink> {
        self.sinks
            .as_ref()
            .context("Sinks already closed")?
            .get(schema)
            .context("Sink missing for schema")
    }

    // Serialize `rows` into the calling thread's per-schema accumulator buffer.
    // The buffer is shipped to the writer thread once it crosses
    // `BATCH_THRESHOLD_BYTES`. csv::Writer auto-emits the header on the first
    // serialize call of the first batch; later batches use has_headers=false.
    fn append_serialize<S, I>(&self, schema: EventFileSchema, rows: I) -> Result<()>
    where
        S: Serialize,
        I: IntoIterator<Item = S>,
    {
        THREAD_BUFFERS.with(|cell| -> Result<()> {
            let mut buffers = cell.borrow_mut();
            let entry = buffers.entry(schema).or_insert_with(|| ThreadSchemaBuffer {
                writer: fresh_writer(false, true),
                is_first_batch: true,
                has_serialized_anything: false,
            });
            let mut wrote_any = false;
            for row in rows {
                match entry.writer.serialize(row) {
                    Ok(()) => wrote_any = true,
                    Err(e) => {
                        // Drop the writer so any partial bytes from the failed
                        // serialize are discarded. Keep `is_first_batch` so
                        // the next batch emits (or skips) the header
                        // consistently with what's already on the wire.
                        entry.writer = fresh_writer(false, entry.is_first_batch);
                        entry.has_serialized_anything = false;
                        return Err(e.into());
                    }
                }
            }
            if wrote_any {
                entry.has_serialized_anything = true;
            }
            if entry.writer.get_ref().len() >= BATCH_THRESHOLD_BYTES {
                self.flush_thread_entry(schema, entry)?;
            }
            Ok(())
        })
    }

    fn append_box_score_line(&self, line: &BoxScoreWritableRecord) -> Result<()> {
        let schema = EventFileSchema::box_score_schema(line)?;
        THREAD_BUFFERS.with(|cell| -> Result<()> {
            let mut buffers = cell.borrow_mut();
            let entry = buffers.entry(schema).or_insert_with(|| ThreadSchemaBuffer {
                writer: fresh_writer(true, false),
                is_first_batch: true,
                has_serialized_anything: false,
            });
            let result: Result<()> = (|| {
                // Custom-header schemas: csv::Writer is built with has_headers=false.
                // Manually emit the wide-column header on the first row of the very
                // first batch so the file gets exactly one header line.
                if entry.is_first_batch && !entry.has_serialized_anything {
                    entry.writer.serialize(line.generate_header()?)?;
                }
                entry.writer.serialize(line)?;
                Ok(())
            })();
            if let Err(e) = result {
                // Drop the writer so any partial bytes (a half-written header
                // row, or a half-written body row) are discarded.
                entry.writer = fresh_writer(true, false);
                entry.has_serialized_anything = false;
                return Err(e);
            }
            entry.has_serialized_anything = true;
            if entry.writer.get_ref().len() >= BATCH_THRESHOLD_BYTES {
                self.flush_thread_entry(schema, entry)?;
            }
            Ok(())
        })
    }

    // Drain the entry's accumulator into a chunk and send it to the writer
    // thread. Replaces `entry.writer` with a fresh one (has_headers=false)
    // because csv::Writer must never re-emit the header in subsequent batches.
    fn flush_thread_entry(
        &self,
        schema: EventFileSchema,
        entry: &mut ThreadSchemaBuffer,
    ) -> Result<()> {
        let custom = schema.uses_custom_header();
        // After the first batch, never let csv emit a header again.
        let new_writer = fresh_writer(custom, false);
        let old_writer = std::mem::replace(&mut entry.writer, new_writer);
        let starts_with_header = entry.is_first_batch && entry.has_serialized_anything;
        entry.is_first_batch = false;
        entry.has_serialized_anything = false;
        let bytes = old_writer
            .into_inner()
            .map_err(|e| anyhow!("csv into_inner: {e}"))?;
        if bytes.is_empty() {
            return Ok(());
        }
        self.send_buffered_chunk(schema, bytes, starts_with_header)
    }

    fn send_buffered_chunk(
        &self,
        schema: EventFileSchema,
        mut bytes: Vec<u8>,
        starts_with_header: bool,
    ) -> Result<()> {
        let sink = self.sink(schema)?;
        if starts_with_header {
            let need_header_global = !sink.has_header_written.swap(true, Ordering::AcqRel);
            let chunk = if need_header_global {
                WriteChunk {
                    has_header: true,
                    bytes,
                }
            } else {
                let cut = bytes
                    .iter()
                    .position(|&b| b == b'\n')
                    .map_or(bytes.len(), |p| p + 1);
                let body = bytes.split_off(cut);
                WriteChunk {
                    has_header: false,
                    bytes: body,
                }
            };
            sink.tx
                .send(chunk)
                .map_err(|e| anyhow!("schema sink send: {e}"))?;
        } else {
            sink.tx
                .send(WriteChunk {
                    has_header: false,
                    bytes,
                })
                .map_err(|e| anyhow!("schema sink send: {e}"))?;
        }
        Ok(())
    }

    // Flush every thread-local buffer this calling thread holds. Run once per
    // rayon worker, after parsing finishes, to ship leftover under-threshold
    // batches before the writer threads see channel close.
    fn flush_local(&self) -> Result<()> {
        THREAD_BUFFERS.with(|cell| -> Result<()> {
            let mut buffers = cell.borrow_mut();
            let schemas: Vec<EventFileSchema> = buffers.keys().copied().collect();
            for schema in schemas {
                if let Some(mut entry) = buffers.remove(&schema) {
                    self.flush_thread_entry(schema, &mut entry)?;
                }
            }
            Ok(())
        })
    }

    fn write_csv<'a, C: ContextToVec<'a>>(
        &self,
        schema: EventFileSchema,
        game_context: &'a GameContext,
    ) -> Result<()> {
        self.append_serialize(schema, C::from_game_context(game_context))
    }

    // Drop every sender so the writer threads see channel close, then join.
    fn close(&mut self) -> Result<()> {
        self.sinks = None;
        let handles = self
            .handles
            .lock()
            .map_err(|e| anyhow!("handles mutex poisoned: {e}"))?
            .take()
            .context("WriterMap already closed")?;
        for handle in handles {
            match handle.join() {
                Ok(Ok(())) => {}
                Ok(Err(e)) => return Err(e),
                Err(e) => bail!("writer thread panicked: {e:?}"),
            }
        }
        Ok(())
    }
}

#[derive(Debug, Eq, PartialEq, Clone, Serialize)]
struct FileInfo {
    pub filename: String,
    pub game_type: GameType,
    pub account_type: AccountType,
    pub file_index: usize,
}

struct GameTask {
    file_info: event_file::parser::FileInfo,
    game_num: usize,
    record_vec: event_file::parser::RecordVec,
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

    fn run_task(
        task: GameTask,
        parsed_games: Option<&HashSet<GameId>>,
        claimed_games: &papaya::HashSet<GameId>,
        writer_map: &WriterMap,
        json_writer: Option<&ThreadSafeJsonWriter>,
        errors: &Mutex<Vec<anyhow::Error>>,
    ) {
        if let Err(e) = Self::process_game(
            task.file_info,
            task.game_num,
            task.record_vec,
            parsed_games,
            Some(claimed_games),
            writer_map,
            json_writer,
        ) {
            errors.lock().map(|mut g| g.push(e)).ok();
        }
    }

    fn process_game(
        file_info: event_file::parser::FileInfo,
        game_num: usize,
        record_vec: event_file::parser::RecordVec,
        parsed_games: Option<&HashSet<GameId>>,
        claimed_games: Option<&papaya::HashSet<GameId>>,
        writer_map: &WriterMap,
        json_writer: Option<&ThreadSafeJsonWriter>,
    ) -> Result<Option<GameId>> {
        let record_slice = &record_vec.record_vec;
        let game_context = match GameContext::new(
            record_slice,
            file_info,
            record_vec.line_offset,
            game_num,
        ) {
            Ok(c) => c,
            Err(e) => {
                let game_id = if let Some(MappedRecord::GameId(id)) = record_slice.first() {
                    id.id.as_str()
                } else {
                    "unknown"
                };
                let filename = file_info.filename.as_str();
                error!("Error initializing game {game_id} in file {filename}: {:?}", e);
                return Ok(None);
            }
        };
        let game_id = game_context.game_id;
        if parsed_games.is_some_and(|pg| pg.contains(&game_id)) {
            warn!(
                "File {} contains already-processed game {}, ignoring",
                file_info.filename, &game_id.id
            );
            return Ok(Some(game_id));
        }
        if let Some(seen) = claimed_games
            && !seen.pin().insert(game_id)
        {
            warn!(
                "File {} contains duplicate game {}, ignoring later occurrence",
                file_info.filename, &game_id.id
            );
            return Ok(Some(game_id));
        }
        if let Some(jw) = json_writer {
            let mut handle = jw.json()?;
            serde_json::to_writer(&mut *handle, &game_context)?;
            handle.write_all(b"\n")?;
        } else if file_info.account_type == AccountType::BoxScore {
            Self::write_box_score_files(&game_context, record_slice, writer_map)?;
        } else {
            Self::write_play_by_play_files(&game_context, writer_map)?;
        }
        Ok(Some(game_id))
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
        writer_map.append_serialize(Self::BoxScoreGames, std::iter::once(Games::from(game_context)))?;
        let line_scores = record_slice
            .iter()
            .filter_map(|mr| match mr {
                MappedRecord::LineScore(ls) => Some(ls),
                _ => None,
            })
            .flat_map(|ls| BoxScoreLineScores::transform_line_score(game_context.game_id.id, ls));
        writer_map.append_serialize(Self::BoxScoreLineScores, line_scores)?;
        writer_map.append_serialize(
            Self::BoxScoreComments,
            BoxScoreComments::from_record_slice(&game_context.game_id.id, record_slice),
        )?;
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
            writer_map.append_box_score_line(&line)?;
        }
        Ok(())
    }

    fn write_play_by_play_files(game_context: &GameContext, writer_map: &WriterMap) -> Result<()> {
        writer_map.write_csv::<GameEarnedRuns>(Self::GameEarnedRuns, game_context)?;
        writer_map.write_csv::<Events>(Self::Events, game_context)?;
        writer_map.write_csv::<EventAudit>(Self::EventAudit, game_context)?;
        writer_map.write_csv::<EventFieldingPlays>(Self::EventFieldingPlay, game_context)?;
        writer_map.write_csv::<EventPitchSequences>(Self::EventPitchSequences, game_context)?;
        writer_map.write_csv::<EventComments>(Self::EventComments, game_context)?;
        writer_map.write_csv::<EventBaserunners>(Self::EventBaserunners, game_context)?;
        writer_map.append_serialize(Self::Games, std::iter::once(Games::from(game_context)))?;
        writer_map.append_serialize(Self::GameLineupAppearances, game_context.lineup_appearances.iter())?;
        writer_map.append_serialize(
            Self::GameFieldingAppearances,
            game_context.fielding_appearances.iter(),
        )?;
        writer_map.append_serialize(
            Self::EventFlags,
            game_context.events.iter().flat_map(|e| &e.results.play_info),
        )
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

    fn contains_nlb_dupes(path: &Path) -> bool {
        let is_evr = path
            .extension()
            .is_some_and(|ext| ext.eq_ignore_ascii_case("EVR"));
        if !is_evr {
            return false;
        }
        let s = path.to_str().unwrap_or_default();
        s.contains("allas") || s.contains("allpost")
    }

    pub fn par_process_files(&mut self, account_type: AccountType) -> Result<()> {
        // PBP and Deduced share one set so the deduced pass skips PBP-covered
        // games. BoxScore uses its own set because every PBP game also has a
        // box-score row by design.
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
        let claimed_games: papaya::HashSet<GameId> =
            papaya::HashSet::with_capacity(file_count * 81);
        // Tag each file with its alphabetical index (used to derive event_key),
        // then reorder so the largest files run first. Largest-first reduces
        // tail latency: by the time we get to small files, work-stealing has
        // many cheap items to balance the few cores still finishing big ones.
        let mut indexed: Vec<(usize, PathBuf)> = files.into_iter().enumerate().collect();
        indexed.par_sort_by_key(|(_, p)| {
            std::cmp::Reverse(std::fs::metadata(p).map(|m| m.len()).unwrap_or(0))
        });

        // Pipeline: every thread plays both roles. It first tries to grab a
        // game off the work queue. If the queue is empty, it pulls a file off
        // the file queue, opens a reader, and pumps that file's games onto the
        // work queue. Once the file queue is drained, the thread drops its
        // sender clone so the channel closes after the in-flight readers
        // finish, and all threads fall through to draining game work.
        let total_threads = rayon::current_num_threads().max(2);
        let (file_tx, file_rx) =
            crossbeam_channel::bounded::<(usize, PathBuf)>(indexed.len().max(1));
        for item in indexed {
            file_tx.send(item).ok();
        }
        drop(file_tx);
        let (game_tx, game_rx) = crossbeam_channel::bounded::<GameTask>(total_threads * 4);
        let errors: Mutex<Vec<anyhow::Error>> = Mutex::new(Vec::new());
        let claimed_games_ref = &claimed_games;

        rayon::scope(|s| {
            for _ in 0..total_threads {
                let file_rx = file_rx.clone();
                let game_rx = game_rx.clone();
                let game_tx = game_tx.clone();
                let errors_ref = &errors;
                s.spawn(move |_| {
                    let mut my_game_tx = Some(game_tx);
                    loop {
                        match game_rx.try_recv() {
                            Ok(task) => {
                                EventFileSchema::run_task(
                                    task,
                                    parsed_games,
                                    claimed_games_ref,
                                    writer_map,
                                    json_writer,
                                    errors_ref,
                                );
                                continue;
                            }
                            Err(crossbeam_channel::TryRecvError::Disconnected) => break,
                            Err(crossbeam_channel::TryRecvError::Empty) => {}
                        }
                        if let Some(tx) = my_game_tx.as_ref() {
                            match file_rx.try_recv() {
                                Ok((i, f)) => {
                                    let file_index = (base_index + i) * EVENT_KEY_BUFFER;
                                    match RetrosheetReader::new(&f, file_index) {
                                        Ok(reader) => {
                                            let file_info = reader.file_info;
                                            debug!("Reading file {}", file_info.filename);
                                            for (game_num, r) in reader.enumerate() {
                                                match r {
                                                    Ok(rv) => {
                                                        let mut task = GameTask {
                                                            file_info,
                                                            game_num,
                                                            record_vec: rv,
                                                        };
                                                        // Drain a game before
                                                        // pushing if the queue
                                                        // is full so we never
                                                        // park all threads on
                                                        // send at once.
                                                        loop {
                                                            match tx.try_send(task) {
                                                                Ok(()) => break,
                                                                Err(crossbeam_channel::TrySendError::Full(returned)) => {
                                                                    task = returned;
                                                                    if let Ok(other) = game_rx.try_recv() {
                                                                        EventFileSchema::run_task(
                                                                            other,
                                                                            parsed_games,
                                                                            claimed_games_ref,
                                                                            writer_map,
                                                                            json_writer,
                                                                            errors_ref,
                                                                        );
                                                                    } else {
                                                                        std::thread::yield_now();
                                                                    }
                                                                }
                                                                Err(crossbeam_channel::TrySendError::Disconnected(_)) => return,
                                                            }
                                                        }
                                                    }
                                                    Err(e) => error!("{:?}", e),
                                                }
                                            }
                                        }
                                        Err(e) => {
                                            error!("Failed to open {}: {:?}", f.display(), e);
                                            errors_ref.lock().map(|mut g| g.push(e)).ok();
                                        }
                                    }
                                    continue;
                                }
                                Err(_) => {
                                    my_game_tx = None;
                                }
                            }
                        }
                        match game_rx.recv() {
                            Ok(task) => EventFileSchema::run_task(
                                task,
                                parsed_games,
                                claimed_games_ref,
                                writer_map,
                                json_writer,
                                errors_ref,
                            ),
                            Err(_) => break,
                        }
                    }
                });
            }
            drop(game_tx);
        });

        let mut errors = errors
            .into_inner()
            .map_err(|e| anyhow!("error mutex poisoned: {e}"))?;
        if let Some(err) = errors.pop() {
            return Err(err);
        }

        // Pull this pass's IDs out of the lock-free set so the next pass can
        // dedup against them.
        let pinned = claimed_games.pin();
        let games: Vec<GameId> = pinned.iter().copied().collect();
        self.index += file_count;
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

        info!("Flushing thread-local CSV buffers");
        let writer_map_ref = &self.writer_map;
        let flush_results: Vec<Result<()>> =
            rayon::broadcast(|_| writer_map_ref.flush_local());
        for r in flush_results {
            r?;
        }
        info!("Closing per-schema writer threads");
        self.writer_map.close()?;
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

#![allow(dead_code)]
#![forbid(unsafe_code)]
#![deny(clippy::all)]
#![warn(clippy::cargo, clippy::nursery)]

use std::collections::HashSet;
use std::fs::File;
use std::hash::Hash;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Mutex;
use std::time::Instant;

use anyhow::{bail, Context, Result};
use csv::{Writer, WriterBuilder};
use either::Either;
use fixed_map::{Key, Map};
use itertools::Itertools;
use lazy_static::lazy_static;
use rayon::prelude::*;
use structopt::StructOpt;
use strum::IntoEnumIterator;
use strum_macros::{Display, EnumIter};
use tracing::{debug, error, info, warn, Level};
use tracing_subscriber::FmtSubscriber;

use event_file::game_state::GameContext;
use event_file::parser::RetrosheetReader;
use event_file::schemas::{ContextToVec, Event};

use crate::event_file::box_score::{BoxScoreEvent, BoxScoreLine};
use crate::event_file::misc::GameId;
use crate::event_file::parser::{AccountType, MappedRecord, RecordSlice};
use crate::event_file::schemas::{
    BoxScoreLineScore, BoxScoreWritableRecord, EventFieldingPlay, EventHitLocation, EventOut,
    EventPitch, EventRaw, Game, GameEarnedRuns, GameTeam,
};
use crate::event_file::traits::EVENT_KEY_BUFFER;

mod event_file;

const ABOUT: &str = "Creates structured datasets from raw Retrosheet files.";

lazy_static! {
    static ref OUTPUT_ROOT: PathBuf = get_output_root(&Opt::from_args()).unwrap();
    static ref WRITER_MAP: WriterMap = WriterMap::new(&OUTPUT_ROOT);
}

struct ThreadSafeWriter {
    csv: Mutex<Writer<File>>,
    has_header_written: AtomicBool,
}

impl ThreadSafeWriter {
    pub fn new(schema: &EventFileSchema) -> Self {
        let file_name = format!("{schema}.csv");
        let output_path = OUTPUT_ROOT.join(file_name);
        debug!("Creating file {}", output_path.display());
        let csv = WriterBuilder::new()
            .has_headers(!schema.uses_custom_header())
            .from_path(output_path)
            .unwrap();
        Self {
            csv: Mutex::new(csv),
            has_header_written: AtomicBool::new(!schema.uses_custom_header()),
        }
    }
}

struct WriterMap {
    output_prefix: PathBuf,
    map: Map<EventFileSchema, ThreadSafeWriter>,
}

impl WriterMap {
    fn new(output_prefix: &Path) -> Self {
        let mut map = Map::new();
        for schema in EventFileSchema::iter() {
            map.insert(schema, ThreadSafeWriter::new(&schema));
        }
        Self {
            output_prefix: output_prefix.to_path_buf(),
            map,
        }
    }

    fn flush_all(&self) {
        self.map.iter().par_bridge().for_each(|(_, writer)| {
            let mut csv = writer.csv.lock().unwrap();
            csv.flush().unwrap();
        })
    }

    fn get(&self, schema: &EventFileSchema) -> &ThreadSafeWriter {
        self.map.get(*schema).unwrap()
    }

    fn write_context<'a, C: ContextToVec<'a>>(
        &self,
        schema: &EventFileSchema,
        game_context: &'a GameContext,
    ) -> Result<()> {
        let writer = self.get(schema);
        let mut csv = writer.csv.lock().unwrap();
        for row in C::from_game_context(game_context) {
            csv.serialize(row)?;
        }
        Ok(())
    }

    fn write_box_score_line(&self, line: &BoxScoreWritableRecord) -> Result<()> {
        let schema = EventFileSchema::box_score_schema(line)?;
        let writer = self.map.get(schema).unwrap();
        let mut csv = writer.csv.lock().unwrap();
        if !writer.has_header_written.load(Ordering::Relaxed) {
            let header = line.generate_header()?;
            csv.serialize(header)?;
            writer.has_header_written.store(true, Ordering::Relaxed);
        }
        csv.serialize(line).context("Failed to write line")
    }
}

#[derive(Debug, Eq, PartialEq, Copy, Clone, Ord, PartialOrd, Hash, Display, EnumIter, Key)]
#[strum(serialize_all = "snake_case")]
enum EventFileSchema {
    Game,
    GameTeam,
    GameUmpire,
    GameLineupAppearance,
    GameFieldingAppearance,
    GameEarnedRuns,
    Event,
    EventRaw,
    EventStartingBaseState,
    EventPlateAppearance,
    EventOut,
    EventFieldingPlay,
    EventBaserunningAdvanceAttempt,
    EventHitLocation,
    EventBaserunningPlay,
    EventPitch,
    EventFlag,
    BoxScoreGame,
    BoxScoreTeam,
    BoxScoreUmpire,
    BoxScoreLineScore,
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
}

impl EventFileSchema {
    const fn uses_custom_header(&self) -> bool {
        matches!(
            self,
            EventFileSchema::BoxScoreBattingLines
                | EventFileSchema::BoxScorePitchingLines
                | EventFileSchema::BoxScoreFieldingLines
                | EventFileSchema::BoxScorePinchHittingLines
                | EventFileSchema::BoxScorePinchRunningLines
                | EventFileSchema::BoxScoreTeamMiscellaneousLines
                | EventFileSchema::BoxScoreTeamBattingLines
                | EventFileSchema::BoxScoreTeamFieldingLines
                | EventFileSchema::BoxScoreDoublePlays
                | EventFileSchema::BoxScoreTriplePlays
                | EventFileSchema::BoxScoreHitByPitches
                | EventFileSchema::BoxScoreHomeRuns
                | EventFileSchema::BoxScoreStolenBases
                | EventFileSchema::BoxScoreCaughtStealing
        )
    }

    fn write(reader: RetrosheetReader, parsed_games: Option<&HashSet<GameId>>) -> Vec<GameId> {
        let file_info = reader.file_info;
        debug!("Processing file {}", file_info.filename);

        let mut game_ids = Vec::with_capacity(81);

        for (game_num, record_vec_result) in reader.enumerate() {
            if let Err(e) = record_vec_result {
                error!("{:?}", e);
                continue;
            }
            let record_vec = record_vec_result.as_ref().unwrap();
            let record_slice = &record_vec.record_vec;

            let game_context_result =
                GameContext::new(record_slice, file_info, record_vec.line_offset, game_num);
            if let Err(e) = game_context_result {
                error!("{:?}", e);
                continue;
            }
            let game_context = game_context_result.unwrap();
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
            if game_context.file_info.account_type == AccountType::BoxScore {
                Self::write_box_score_files(&game_context, record_slice).unwrap();
            } else {
                Self::write_play_by_play_files(&game_context).unwrap();
            }
        }
        game_ids
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
            .get(&Self::BoxScoreGame)
            .csv
            .lock()
            .unwrap()
            .serialize(Game::from(game_context))?;
        // Write BoxScoreTeam
        WRITER_MAP.write_context::<GameTeam>(&Self::BoxScoreTeam, game_context)?;
        // Write GameUmpire
        let mut w = WRITER_MAP.get(&Self::BoxScoreUmpire).csv.lock().unwrap();
        for row in &game_context.umpires {
            w.serialize(row)?;
        }
        // Write Linescores
        let line_scores = record_slice
            .iter()
            .filter_map(|mr| match mr {
                MappedRecord::LineScore(ls) => Some(ls),
                _ => None,
            })
            .flat_map(|ls| BoxScoreLineScore::transform_line_score(game_context.game_id.id, ls));
        let mut w = WRITER_MAP.get(&Self::BoxScoreLineScore).csv.lock().unwrap();
        for row in line_scores {
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
        WRITER_MAP.write_context::<GameTeam>(&Self::GameTeam, game_context)?;
        WRITER_MAP.write_context::<GameEarnedRuns>(&Self::GameEarnedRuns, game_context)?;
        WRITER_MAP.write_context::<Event>(&Self::Event, game_context)?;
        WRITER_MAP.write_context::<EventRaw>(&Self::EventRaw, game_context)?;
        WRITER_MAP.write_context::<EventOut>(&Self::EventOut, game_context)?;
        WRITER_MAP.write_context::<EventFieldingPlay>(&Self::EventFieldingPlay, game_context)?;
        WRITER_MAP.write_context::<EventPitch>(&Self::EventPitch, game_context)?;
        WRITER_MAP.write_context::<EventHitLocation>(&Self::EventHitLocation, game_context)?;
        // Write Game
        WRITER_MAP
            .get(&Self::Game)
            .csv
            .lock()
            .unwrap()
            .serialize(Game::from(game_context))?;
        // Write GameUmpire
        let mut w = WRITER_MAP.get(&Self::GameUmpire).csv.lock().unwrap();
        for row in &game_context.umpires {
            w.serialize(row)?;
        }
        // Write GameLineupAppearance
        let mut w = WRITER_MAP
            .get(&Self::GameLineupAppearance)
            .csv
            .lock()
            .unwrap();
        for row in &game_context.lineup_appearances {
            w.serialize(row)?;
        }
        // Write GameFieldingAppearance
        let mut w = WRITER_MAP
            .get(&Self::GameFieldingAppearance)
            .csv
            .lock()
            .unwrap();
        for row in &game_context.fielding_appearances {
            w.serialize(row)?;
        }
        // Write EventStartingBaseState
        let mut w = WRITER_MAP
            .get(&Self::EventStartingBaseState)
            .csv
            .lock()
            .unwrap();
        let base_states = game_context
            .events
            .iter()
            .flat_map(|e| &e.context.starting_base_state);
        for row in base_states {
            w.serialize(row)?;
        }
        // Write EventPlateAppearance
        let mut w = WRITER_MAP
            .get(&Self::EventPlateAppearance)
            .csv
            .lock()
            .unwrap();
        let pa = game_context
            .events
            .iter()
            .filter_map(|e| e.results.plate_appearance.as_ref());
        for row in pa {
            w.serialize(row)?;
        }
        // Write EventBaserunningAdvanceAttempt
        let mut w = WRITER_MAP
            .get(&Self::EventBaserunningAdvanceAttempt)
            .csv
            .lock()
            .unwrap();
        let advance_attempts = game_context
            .events
            .iter()
            .flat_map(|e| &e.results.baserunning_advances);
        for row in advance_attempts {
            w.serialize(row)?;
        }
        // Write EventBaserunningPlay
        let mut w = WRITER_MAP
            .get(&Self::EventBaserunningPlay)
            .csv
            .lock()
            .unwrap();
        let baserunning_plays = game_context
            .events
            .iter()
            .filter_map(|e| e.results.plays_at_base.as_ref())
            .flatten();
        for row in baserunning_plays {
            w.serialize(row)?;
        }
        //Write EventFlag
        let mut w = WRITER_MAP.get(&Self::EventFlag).csv.lock().unwrap();
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

#[derive(StructOpt, Debug)]
#[structopt(name = "pbp-to-box", about = ABOUT)]
struct Opt {
    #[structopt(short, long, parse(from_os_str))]
    input: PathBuf,

    #[structopt(short, long, parse(from_os_str))]
    output_dir: PathBuf,
}

fn get_output_root(opt: &Opt) -> Result<PathBuf> {
    std::fs::create_dir_all(&opt.output_dir).context("Error occurred on output dir check")?;
    opt.output_dir
        .canonicalize()
        .context("Invalid output directory")
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
            game_ids: HashSet::with_capacity(200000),
        }
    }

    fn process_file(
        input_path: &PathBuf,
        parsed_games: Option<&HashSet<GameId>>,
        file_index: usize,
    ) -> Vec<GameId> {
        let reader = RetrosheetReader::new(input_path, file_index).unwrap();
        EventFileSchema::write(reader, parsed_games)
    }

    pub fn par_process_files(&mut self, account_type: AccountType) {
        // Box score accounts are expected to be duplicates so we don't need to check against them
        let parsed_games = if account_type == AccountType::BoxScore {
            None
        } else {
            Some(&self.game_ids)
        };
        let files = account_type
            .glob(&self.opt.input)
            .unwrap()
            .map(Result::unwrap)
            .sorted_by_key(|p| p.clone())
            .collect_vec();
        let file_count = files.len();
        let games: Vec<GameId> = files
            .into_par_iter()
            .enumerate()
            .flat_map(|(i, f)| {
                Self::process_file(&f, parsed_games, (self.index + i) * EVENT_KEY_BUFFER)
            })
            .collect();
        self.index += file_count;
        self.game_ids.extend(games);
    }

    pub fn process_files(&mut self) {
        info!("Parsing conventional play-by-play files");
        self.par_process_files(AccountType::PlayByPlay);

        info!("Parsing deduced play-by-play files");
        self.par_process_files(AccountType::Deduced);

        info!("Parsing box score files");
        self.par_process_files(AccountType::BoxScore);

        WRITER_MAP.flush_all();
    }
}

fn main() {
    let subscriber = FmtSubscriber::builder()
        .with_max_level(Level::INFO)
        .finish();
    tracing::subscriber::set_global_default(subscriber).unwrap();

    let start = Instant::now();
    let opt: Opt = Opt::from_args();

    FileProcessor::new(opt).process_files();

    let end = start.elapsed();
    info!("Elapsed: {:?}", end);
}

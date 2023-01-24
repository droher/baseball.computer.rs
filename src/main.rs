#![allow(dead_code)]
#![forbid(unsafe_code)]

use std::collections::{HashMap, HashSet};
use std::fs::{File, remove_file};
use std::hash::Hash;
use std::io::{BufRead, BufReader, BufWriter, copy};
use std::path::{Path, PathBuf};
use std::time::Instant;

use anyhow::{bail, Context, Result};
use csv::{Writer, WriterBuilder};
use either::Either;
use glob::glob;
use itertools::Itertools;
use rayon::prelude::*;
use structopt::StructOpt;
use strum::IntoEnumIterator;
use strum_macros::{Display, EnumIter};
use tracing::{debug, error, info, Level, warn};
use tracing_subscriber::FmtSubscriber;

use event_file::game_state::GameContext;
use event_file::parser::RetrosheetReader;
use event_file::schemas::{ContextToVec, Event};

use crate::event_file::box_score::{BoxScoreEvent, BoxScoreLine};
use crate::event_file::misc::GameId;
use crate::event_file::parser::{AccountType, MappedRecord, RecordSlice};
use crate::event_file::schemas::{BoxScoreLineScore, BoxScoreWritableRecord, EventFieldingPlay, EventHitLocation, EventOut, EventPitch, EventRaw, Game, GameEarnedRuns, GameTeam};
use crate::event_file::traits::EVENT_KEY_BUFFER;

mod event_file;

const ABOUT: &str = "Creates structured datasets from raw Retrosheet files.";

struct WriterMap {
    output_prefix: PathBuf,
    account_type: AccountType,
    map: HashMap<EventFileSchema, Writer<File>>,
}

impl WriterMap {
    pub fn new(output_prefix: &Path, account_type: AccountType) -> Self {
        Self {
            output_prefix: output_prefix.to_path_buf(),
            account_type,
            map: HashMap::with_capacity(25),
        }
    }

    pub fn get_mut(&mut self, schema: &EventFileSchema) -> &mut Writer<File> {
        self.map
            .entry(*schema)
            .or_insert_with(|| Self::new_writer(&self.output_prefix, true, schema))
    }
    pub fn get_mut_write_header(
        &mut self,
        schema: &EventFileSchema,
        header_template: &BoxScoreWritableRecord,
    ) -> &mut Writer<File> {
        self.map.entry(*schema).or_insert_with(|| {
            let mut w = Self::new_writer(&self.output_prefix, false, schema);
            let header = header_template.generate_header().unwrap();
            w.serialize(header).unwrap();
            w
        })
    }

    fn new_writer(
        output_prefix: &Path,
        has_headers: bool,
        schema: &EventFileSchema,
    ) -> Writer<File> {
        let suffix = output_prefix.file_name().unwrap().to_str().unwrap();
        let file_name = format!("{schema}__{suffix}.csv");
        let output_path = output_prefix.with_file_name(file_name);
        WriterBuilder::new()
            .has_headers(has_headers)
            .from_path(output_path)
            .unwrap()
    }
}

#[derive(Debug, Eq, PartialEq, Copy, Clone, Ord, PartialOrd, Hash, Display, EnumIter)]
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
    fn write(
        reader: RetrosheetReader,
        output_prefix: &Path,
        parsed_games: Option<&HashSet<GameId>>,
    ) -> Vec<GameId> {
        let file_info = reader.file_info;
        let output_prefix_display = output_prefix.to_str().unwrap_or_default();
        debug!("Processing file {}", output_prefix_display);

        let mut game_ids = Vec::with_capacity(81);
        let mut writer_map = WriterMap::new(output_prefix, file_info.account_type);

        for (game_num, record_vec_result) in reader.enumerate() {
            if let Err(e) = record_vec_result {
                error!("{:?}", e);
                continue;
            }
            let record_vec = record_vec_result.as_ref().unwrap();
            let record_slice = &record_vec.record_vec;

            let game_context_result = GameContext::new(record_slice, file_info, record_vec.line_offset, game_num);
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
                    output_prefix_display, &game_context.game_id.id
                );
                continue;
            }
            if game_context.file_info.account_type == AccountType::BoxScore {
                Self::write_box_score_files(&mut writer_map, &game_context, record_slice)
                    .unwrap();
            } else {
                Self::write_play_by_play_files(&mut writer_map, &game_context).unwrap();
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

    fn write_box_score_files(
        writer_map: &mut WriterMap,
        game_context: &GameContext,
        record_slice: &RecordSlice,
    ) -> Result<()> {
        // Write Game
        writer_map
            .get_mut(&Self::BoxScoreGame)
            .serialize(Game::from(game_context))?;
        // Write GameTeam
        let w = writer_map.get_mut(&Self::BoxScoreTeam);
        for row in GameTeam::from_game_context(game_context) {
            w.serialize(row)?;
        }
        // Write GameUmpire
        let w = writer_map.get_mut(&Self::BoxScoreUmpire);
        for row in &game_context.umpires {
            w.serialize(row)?;
        }
        // Write Linescores
        let line_scores = record_slice.iter()
            .filter_map(|mr| match mr {
                MappedRecord::LineScore(ls) => Some(ls),
                _ => None
            })
            .flat_map(|ls| BoxScoreLineScore::transform_line_score(game_context.game_id.id, ls));
        let w = writer_map.get_mut(&Self::BoxScoreLineScore);
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
            let schema = Self::box_score_schema(&line)?;
            let w = writer_map.get_mut_write_header(&schema, &line);
            if let Err(e) = w.serialize(&line) {
                error!("{e}");
            }
        }
        Ok(())
    }

    fn write_play_by_play_files(
        writer_map: &mut WriterMap,
        game_context: &GameContext,
    ) -> Result<()> {
        // Write Game
        writer_map
            .get_mut(&Self::Game)
            .serialize(Game::from(game_context))?;
        // Write GameTeam
        let w = writer_map.get_mut(&Self::GameTeam);
        for row in GameTeam::from_game_context(game_context) {
            w.serialize(row)?;
        }
        // Write GameUmpire
        let w = writer_map.get_mut(&Self::GameUmpire);
        for row in &game_context.umpires {
            w.serialize(row)?;
        }
        // Write GameLineupAppearance
        let w = writer_map.get_mut(&Self::GameLineupAppearance);
        for row in &game_context.lineup_appearances {
            w.serialize(row)?;
        }
        // Write GameFieldingAppearance
        let w = writer_map.get_mut(&Self::GameFieldingAppearance);
        for row in &game_context.fielding_appearances {
            w.serialize(row)?;
        }
        // Write GameEarnedRun
        let w = writer_map.get_mut(&Self::GameEarnedRuns);
        for row in GameEarnedRuns::from_game_context(game_context) {
            w.serialize(row)?;
        }
        // Write Event
        let w = writer_map.get_mut(&Self::Event);
        for row in Event::from_game_context(game_context) {
            w.serialize(row)?;
        }
        // Write EventRaw
        let w = writer_map.get_mut(&Self::EventRaw);
        for row in EventRaw::from_game_context(game_context) {
            w.serialize(row)?;
        }
        // Write EventStartingBaseState
        let w = writer_map.get_mut(&Self::EventStartingBaseState);
        let base_states = game_context
            .events
            .iter()
            .flat_map(|e| &e.context.starting_base_state)
            .collect_vec();
        for row in base_states {
            w.serialize(row)?;
        }
        // Write EventPlateAppearance
        let w = writer_map.get_mut(&Self::EventPlateAppearance);
        let pa = game_context
            .events
            .iter()
            .filter_map(|e| e.results.plate_appearance.as_ref())
            .collect_vec();
        for row in pa {
            w.serialize(row)?;
        }
        // Write EventOut
        let w = writer_map.get_mut(&Self::EventOut);
        for row in EventOut::from_game_context(game_context) {
            w.serialize(row)?;
        }
        // Write EventFieldingPlay
        let w = writer_map.get_mut(&Self::EventFieldingPlay);
        for row in EventFieldingPlay::from_game_context(game_context) {
            w.serialize(row)?;
        }
        // Write EventBaserunningAdvanceAttempt
        let w = writer_map.get_mut(&Self::EventBaserunningAdvanceAttempt);
        let advance_attempts = game_context
            .events
            .iter()
            .flat_map(|e| &e.results.baserunning_advances)
            .collect_vec();
        for row in advance_attempts {
            w.serialize(row)?;
        }
        // Write EventHitLocation
        let w = writer_map.get_mut(&Self::EventHitLocation);
        for row in EventHitLocation::from_game_context(game_context) {
            w.serialize(row)?;
        }
        // Write EventBaserunningPlay
        let w = writer_map.get_mut(&Self::EventBaserunningPlay);
        let baserunning_plays = game_context
            .events
            .iter()
            .filter_map(|e| e.results.plays_at_base.as_ref())
            .flatten()
            .collect_vec();
        for row in baserunning_plays {
            w.serialize(row)?;
        }
        // Write EventPitch
        // Not in every PBP so avoid writing empty file
        if EventPitch::from_game_context(game_context)
            .next()
            .is_some()
        {
            let w = writer_map.get_mut(&Self::EventPitch);
            for row in EventPitch::from_game_context(game_context) {
                w.serialize(row)?;
            }
        }
        //Write EventFlag
        let w = writer_map.get_mut(&Self::EventFlag);
        let event_flags = game_context
            .events
            .iter()
            .flat_map(|e| &e.results.play_info)
            .collect_vec();
        for row in event_flags {
            w.serialize(row)?;
        }
        Ok(())
    }

    pub fn concat(output_root: &str) {
        EventFileSchema::iter().par_bridge().for_each( |schema| {
            let new_file = format!("{}/{}.csv", output_root, schema);
            let mut exists = false;
            let file = File::create(new_file).unwrap();
            let mut writer = BufWriter::new(file);
            let pattern = format!("{}/{}__*.csv", output_root, schema);
            let glob = glob(&pattern).unwrap();
            for g in glob {
                let path = g.unwrap();
                let mut reader = BufReader::new(File::open(&path).unwrap());
                if exists {
                    reader.read_line(&mut String::new()).unwrap();
                }
                copy(&mut reader, &mut writer).unwrap();
                remove_file(&path).unwrap();
                exists = true;
            }
        })
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
        output_root: &Path,
        parsed_games: Option<&HashSet<GameId>>,
        file_index: usize,
    ) -> Vec<GameId> {
        let output_prefix = output_root.join(input_path.file_name().unwrap());
        let reader = RetrosheetReader::new(input_path, file_index).unwrap();
        EventFileSchema::write(reader, &output_prefix, parsed_games)
    }

    pub fn par_process_files(&mut self, account_type: AccountType) {
        // Box score accounts are expected to be duplicates so we don't need to check against them
        let parsed_games = if account_type == AccountType::BoxScore {
            None
        }
        else {
            Some(&self.game_ids)
        };
        let files = account_type
            .glob(&self.opt.input)
            .unwrap()
            .map(|g| g.unwrap())
            .sorted_by_key(|p| p.clone())
            .collect_vec();
        let file_count = files.len();
        let games: Vec<GameId> = files
            .into_par_iter()
            .enumerate()
            .flat_map(|(i, f)| {
                Self::process_file(
                    &f,
                    &get_output_root(&self.opt).unwrap(),
                    parsed_games,
                    (self.index + i) * EVENT_KEY_BUFFER,
                )
            })
            .collect();
        self.index += file_count;
        self.game_ids.extend(games);
    }

    pub fn process_files(&mut self) {
        let output_root = get_output_root(&self.opt).unwrap();

        info!("Parsing conventional play-by-play files");
        self.par_process_files( AccountType::PlayByPlay);

        info!("Parsing deduced play-by-play files");
        self.par_process_files(AccountType::Deduced);

        info!("Parsing box score files");
        self.par_process_files(AccountType::BoxScore);

        info!("Merging files by schema");
        EventFileSchema::concat(output_root.to_str().unwrap());
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

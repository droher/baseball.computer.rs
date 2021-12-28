#![allow(dead_code)]
#![forbid(unsafe_code)]

use std::collections::{HashMap, HashSet};
use std::convert::TryFrom;
use std::fs::{File, FileType, OpenOptions, remove_file};
use std::io::{BufRead, BufReader, BufWriter, copy};
use std::path::{Path, PathBuf};
use std::time::Instant;

use anyhow::{Context, Result};
use csv::{Writer, WriterBuilder};
use glob::{glob, GlobResult, Paths};
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

use crate::event_file::misc::GameId;
use crate::event_file::parser::AccountType;
use crate::event_file::schemas::{EventFieldingPlay, EventHitLocation, EventOut, EventPitch, Game, GameTeam};

mod event_file;

const ABOUT: &str = "Transforms Retrosheet .EV* files (play-by-play) into .EB* files (box score).";

#[derive(Debug, Eq, PartialEq, Copy, Clone, Ord, PartialOrd, Hash, Display, EnumIter)]
#[strum(serialize_all = "snake_case")]
enum EventFileSchema {
    Game,
    GameTeam,
    GameUmpire,
    GameLineupAppearance,
    GameFieldingAppearance,
    Event,
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
    BoxScoreStarters,
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
    BoxScoreCaughtStealing
}

impl EventFileSchema {
    fn write(reader: RetrosheetReader, output_prefix: &Path, parsed_games: Option<&HashSet<GameId>>) -> Vec<GameId> {
        let file_info = (&reader).file_info.clone();
        let output_prefix_display = output_prefix.to_str().unwrap_or_default();
        debug!("Processing file {}", output_prefix_display);

        let mut game_ids = Vec::with_capacity(81);
        let mut writer_map = Self::writer_map(output_prefix, file_info.account_type);

        for record_vec_result in reader {
            if let Err(e) = record_vec_result {
                error!("{:?}", e);
                continue
            }
            let game_context_result = GameContext::try_from((record_vec_result.unwrap().as_slice(), file_info.clone()));
            if let Err(e) = game_context_result {
                error!("{:?}", e);
                continue
            }
            let game_context = game_context_result.unwrap();
            game_ids.push(game_context.game_id);
            if parsed_games.map(|pg| pg.contains(&game_context.game_id)).unwrap_or_default() {
                warn!("File {} contains already-processed game {}, ignoring",
                    output_prefix_display,
                    &game_context.game_id.id);
                continue
            }
            Self::write_individual_files(&mut writer_map, &game_context).unwrap()
        }
        game_ids
    }

    fn write_individual_files(writer_map: &mut HashMap<Self, Writer<File>>, game_context: &GameContext) -> Result<()> {
        if game_context.file_info.account_type == AccountType::BoxScore {
            Self::write_box_score_files(writer_map, game_context)
        }
        else {
            Self::write_play_by_play_files(writer_map, game_context)
        }
    }

    fn write_box_score_files(writer_map: &mut HashMap<Self, Writer<File>>, game_context: &GameContext) -> Result<()> {

        let info = game_context.box_score_info
            .as_ref()
            .context("No box score data for box score write routine")?;
        let game_id = game_context.game_id;
        writer_map
            .get_mut(&Self::BoxScoreGame)
            .unwrap()
            .serialize(Game::from(game_context))?;
        let w = writer_map.get_mut(&Self::BoxScoreTeam).unwrap();
        for row in GameTeam::from_game_context(game_context) {
            w.serialize(&row)?;
        }
        let w = writer_map.get_mut(&Self::BoxScoreUmpire).unwrap();
        for row in &game_context.umpires {
            w.serialize(row)?;
        }
        Ok(())
        // TODO: The other box score stuff (maybe have to create full flat schemas ugh )
    }

    fn write_play_by_play_files(writer_map: &mut HashMap<Self, Writer<File>>, game_context: &GameContext) -> Result<()> {
        // Write Game
        writer_map
            .get_mut(&Self::Game)
            .unwrap()
            .serialize(Game::from(game_context))?;
        // Write GameTeam
        let w = writer_map.get_mut(&Self::GameTeam).unwrap();
        for row in GameTeam::from_game_context(game_context) {
            w.serialize(&row)?;
        }
        // Write GameUmpire
        let w = writer_map.get_mut(&Self::GameUmpire).unwrap();
        for row in &game_context.umpires {
            w.serialize(row)?;
        }
        // Write GameLineupAppearance
        let w = writer_map.get_mut(&Self::GameLineupAppearance).unwrap();
        for row in &game_context.lineup_appearances {
            w.serialize(row)?;
        }
        // Write GameFieldingAppearance
        let w = writer_map.get_mut(&Self::GameFieldingAppearance).unwrap();
        for row in &game_context.fielding_appearances {
            w.serialize(row)?;
        }
        // Write Event
        let w = writer_map.get_mut(&Self::Event).unwrap();
        for row in Event::from_game_context(game_context) {
            w.serialize(row)?;
        }
        // Write EventStartingBaseState
        let w = writer_map.get_mut(&Self::EventStartingBaseState).unwrap();
        let base_states = game_context
            .events
            .iter()
            .flat_map(|e| &e.context.starting_base_state)
            .collect_vec();
        for row in base_states {
            w.serialize(row)?;
        }
        // Write EventPlateAppearance
        let w = writer_map.get_mut(&Self::EventPlateAppearance).unwrap();
        let pa = game_context
            .events
            .iter()
            .filter_map(|e| e.results.plate_appearance.as_ref())
            .collect_vec();
        for row in pa {
            w.serialize(row)?;
        }
        // Write EventOut
        let w = writer_map.get_mut(&Self::EventOut).unwrap();
        for row in EventOut::from_game_context(game_context) {
            w.serialize(&row)?;
        }
        // Write EventFieldingPlay
        let w = writer_map.get_mut(&Self::EventFieldingPlay).unwrap();
        for row in EventFieldingPlay::from_game_context(game_context) {
            w.serialize(&row)?;
        }
        // Write EventBaserunningAdvanceAttempt
        let w = writer_map
            .get_mut(&Self::EventBaserunningAdvanceAttempt)
            .unwrap();
        let advance_attempts = game_context
            .events
            .iter()
            .flat_map(|e| &e.results.baserunning_advances)
            .collect_vec();
        for row in advance_attempts {
            w.serialize(row)?;
        }
        // Write EventHitLocation
        let w = writer_map.get_mut(&Self::EventHitLocation).unwrap();
        for row in EventHitLocation::from_game_context(game_context) {
            w.serialize(&row)?;
        }
        // Write EventBaserunningPlay
        let w = writer_map.get_mut(&Self::EventBaserunningPlay).unwrap();
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
        let w = writer_map.get_mut(&Self::EventPitch).unwrap();
        for row in EventPitch::from_game_context(game_context) {
            w.serialize(&row)?;
        }
        //Write EventFlag
        let w = writer_map.get_mut(&Self::EventFlag).unwrap();
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

    fn writer_map(output_prefix: &Path, account_type: AccountType) -> HashMap<Self, Writer<File>> {
        let (box_score, event): (Vec<Self>, Vec<Self>) = Self::iter().partition(|s| s.to_string().starts_with("box_score"));
        let schema_iter = if account_type == AccountType::BoxScore { box_score } else { event };
        let mut map = HashMap::new();
        for schema in schema_iter {
            let file_name = format!(
                "{}__{}.csv",
                schema.to_string(),
                output_prefix.file_name().unwrap().to_str().unwrap()
            );
            let output_path = output_prefix.with_file_name(file_name);
            let writer = WriterBuilder::new().from_path(output_path).unwrap();
            map.insert(schema, writer);
        }
        map
    }

    pub fn concat(output_root: &str) {
        for schema in EventFileSchema::iter() {
            let new_file = format!("{}/{}.csv", output_root, schema);
            let mut exists = false;
            let file = OpenOptions::new()
                .create(true)
                .write(true)
                .open(new_file)
                .unwrap();
            let mut writer = BufWriter::new(file);

            let pattern = format!("{}/{}__*.csv", output_root, schema);
            let glob = glob(&*pattern).unwrap();
            for g in glob {
                let path = g.unwrap();
                let mut reader = BufReader::new(File::open(&path).unwrap());
                if exists { reader.read_line(&mut String::new()).unwrap(); }
                copy(&mut reader, &mut writer).unwrap();
                remove_file(&path).unwrap();
                exists = true;
            }
        }
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

fn process_file(glob_result: GlobResult, output_root: &Path, parsed_games: Option<&HashSet<GameId>>) -> Vec<GameId> {
    let input_path = glob_result.unwrap();
    let output_prefix = output_root.join(input_path.file_name().unwrap());
    let reader = RetrosheetReader::try_from(&input_path).unwrap();
    EventFileSchema::write(reader, &output_prefix, parsed_games)
}

fn par_process_files(opt: &Opt, account_type: AccountType, parsed_games: Option<&HashSet<GameId>>) -> Vec<GameId> {
    account_type
        .glob(&opt.input)
        .unwrap()
        .par_bridge()
        .flat_map(|f| process_file(f, &opt.output_dir, parsed_games))
        .collect()
}

fn get_output_root(opt: &Opt) -> Result<PathBuf> {
    std::fs::create_dir_all(&opt.output_dir).context("Error occurred on output dir check")?;
    opt
        .output_dir
        .canonicalize()
        .context("Invalid output directory")
}

fn main() {
    let mut parsed_game_ids = HashSet::with_capacity(200000);
    let subscriber = FmtSubscriber::builder()
        .with_max_level(Level::INFO)
        .finish();
    tracing::subscriber::set_global_default(subscriber).unwrap();

    let start = Instant::now();
    let opt: Opt = Opt::from_args();
    let output_root = get_output_root(&opt).unwrap();

    info!("Parsing conventional play-by-play files");
    let mut event_files = par_process_files(&opt, AccountType::PlayByPlay, Some(&parsed_game_ids));
    parsed_game_ids.extend(event_files.drain(..));

    info!("Parsing derived play-by-play files");
    par_process_files(&opt, AccountType::Derived, Some(&parsed_game_ids));

    info!("Parsing box score files");
    par_process_files(&opt, AccountType::BoxScore, None);


    info!("Merging files by schema");
    EventFileSchema::concat(output_root.to_str().unwrap());
    let end = start.elapsed();
    info!("Elapsed: {:?}", end);
}

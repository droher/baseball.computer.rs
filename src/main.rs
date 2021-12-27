#![allow(dead_code)]
#![forbid(unsafe_code)]

use std::collections::HashMap;
use std::convert::TryFrom;
use std::fs::{File, OpenOptions, remove_file};
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
use tracing::{debug, error, info, Level};
use tracing_subscriber::FmtSubscriber;

use event_file::game_state::GameContext;
use event_file::parser::RetrosheetReader;
use event_file::schemas::{ContextToVec, Event};

use crate::event_file::schemas::{
    EventFieldingPlay, EventHitLocation, EventOut, EventPitch, Game, GameTeam,
};

mod event_file;
mod util;

const ABOUT: &str = "Transforms Retrosheet .EV* files (play-by-play) into .EB* files (box score).";
const GLOB_PATTERN: &str = "**/*.E[VD]*";
const RETROSHEET_URL: &str = "https://github.com/droher/retrosheet/archive/refs/heads/master.zip";

#[derive(Debug, Eq, PartialEq, Copy, Clone, Ord, PartialOrd, Hash, Display, EnumIter)]
#[strum(serialize_all = "snake_case")]
enum Schema {
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
}

impl Schema {
    fn write(reader: RetrosheetReader, output_prefix: &Path) -> Result<()> {
        debug!(
            "Processing file {}",
            output_prefix.to_str().unwrap_or_default()
        );

        let mut writer_map = Self::writer_map(output_prefix);

        for record_vec_result in reader {
            let game_context = GameContext::try_from(record_vec_result?.as_slice())?;
            // Write Game
            writer_map
                .get_mut(&Self::Game)
                .unwrap()
                .serialize(Game::from(&game_context))?;
            // Write GameTeam
            let w = writer_map.get_mut(&Self::GameTeam).unwrap();
            for row in GameTeam::from_game_context(&game_context) {
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
            for row in Event::from_game_context(&game_context) {
                w.serialize(row)?;
            }
            // Write EventStartingBaseState
            let w = writer_map.get_mut(&Self::EventStartingBaseState).unwrap();
            let base_states = &game_context
                .events
                .iter()
                .flat_map(|e| &e.context.starting_base_state)
                .collect_vec();
            for row in base_states {
                w.serialize(row)?;
            }
            // Write EventPlateAppearance
            let w = writer_map.get_mut(&Self::EventPlateAppearance).unwrap();
            let pa = &game_context
                .events
                .iter()
                .filter_map(|e| e.results.plate_appearance.as_ref())
                .collect_vec();
            for row in pa {
                w.serialize(row)?;
            }
            // Write EventOut
            let w = writer_map.get_mut(&Self::EventOut).unwrap();
            for row in EventOut::from_game_context(&game_context) {
                w.serialize(&row)?;
            }
            // Write EventFieldingPlay
            let w = writer_map.get_mut(&Self::EventFieldingPlay).unwrap();
            for row in EventFieldingPlay::from_game_context(&game_context) {
                w.serialize(&row)?;
            }
            // Write EventBaserunningAdvanceAttempt
            let w = writer_map
                .get_mut(&Self::EventBaserunningAdvanceAttempt)
                .unwrap();
            let advance_attempts = &game_context
                .events
                .iter()
                .flat_map(|e| &e.results.baserunning_advances)
                .collect_vec();
            for row in advance_attempts {
                w.serialize(row)?;
            }
            // Write EventHitLocation
            let w = writer_map.get_mut(&Self::EventHitLocation).unwrap();
            for row in EventHitLocation::from_game_context(&game_context) {
                w.serialize(&row)?;
            }
            // Write EventBaserunningPlay
            let w = writer_map.get_mut(&Self::EventBaserunningPlay).unwrap();
            let baserunning_plays = &game_context
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
            for row in EventPitch::from_game_context(&game_context) {
                w.serialize(&row)?;
            }
            //Write EventFlag
            let w = writer_map.get_mut(&Self::EventFlag).unwrap();
            let event_flags = &game_context
                .events
                .iter()
                .flat_map(|e| &e.results.play_info)
                .collect_vec();
            for row in event_flags {
                w.serialize(row)?;
            }
        }
        Ok(())
    }

    fn writer_map(output_prefix: &Path) -> HashMap<Self, Writer<File>> {
        let mut map = HashMap::new();
        for schema in Self::iter() {
            let file_name = format!(
                "{}__{}.csv",
                schema,
                output_prefix.file_name().unwrap().to_str().unwrap()
            );
            let output_path = output_prefix.with_file_name(file_name);
            let writer = WriterBuilder::new().from_path(output_path).unwrap();
            map.insert(schema, writer);
        }
        map
    }

    pub fn concat(output_root: &str) {
        for schema in Schema::iter() {
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

fn process_file(glob_result: GlobResult, output_root: &Path) -> Result<()> {
    let input_path = glob_result?;
    let output_prefix = output_root.join(input_path.file_name().unwrap());
    let reader = RetrosheetReader::try_from(&input_path).unwrap();
    Schema::write(reader, &output_prefix)
}

fn get_input_glob(opt: Opt) -> Result<Paths> {
    let full_path_pattern = opt
        .input
        .canonicalize()
        .expect("Invalid input path")
        .join(Path::new(GLOB_PATTERN));
    glob(full_path_pattern.to_str().unwrap()).context("Unable to read input path")
}


fn main() {
    let subscriber = FmtSubscriber::builder()
        .with_max_level(Level::DEBUG)
        .finish();
    tracing::subscriber::set_global_default(subscriber).unwrap();

    let start = Instant::now();
    let opt: Opt = Opt::from_args();
    std::fs::create_dir_all(&opt.output_dir).unwrap();

    let output_root = opt
        .output_dir
        .canonicalize()
        .expect("Invalid output directory");

    let errors: Vec<Result<()>> = get_input_glob(opt)
        .unwrap()
        .par_bridge()
        .filter_map(|f| {
            if let Err(e) = process_file(f, &output_root) {
                Some(Err(e))
            } else {
                None
            }
        })
        .collect();
    errors.into_iter().for_each(|r| error!("{:?}", r));

    Schema::concat(output_root.to_str().unwrap());
    let end = start.elapsed();
    info!("Elapsed: {:?}", end);
}

#![forbid(unsafe_code)]
#![allow(dead_code)]

use std::convert::TryFrom;
use std::path::PathBuf;
use std::time::Instant;

use anyhow::Result;
use csv::{Writer, WriterBuilder};
use structopt::StructOpt;
use strum::IntoEnumIterator;
use strum_macros::{Display, EnumIter};

use crate::event_file::schemas::{
    EventFieldingPlay, EventHitLocation, EventOut, EventPitch, Game, GameTeam,
};
use event_file::game_state::GameContext;
use event_file::parser::{RetrosheetReader};
use event_file::schemas::{ContextToVec, Event};
use itertools::Itertools;
use std::collections::HashMap;
use std::fs::File;

mod event_file;
mod util;

const ABOUT: &str = "Transforms Retrosheet .EV* files (play-by-play) into .EB* files (box score).";

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
    fn write(reader: RetrosheetReader) -> Result<()> {
        let mut writer_map = Self::get_writer_map();

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
                .flat_map(|e| e.results.plate_appearance.as_ref())
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

    fn get_writer_map() -> HashMap<Self, Writer<File>> {
        let mut map = HashMap::new();
        for schema in Self::iter() {
            map.insert(schema, Self::get_writer(&schema.to_string()));
        }
        map
    }

    fn get_writer(filename: &str) -> Writer<File> {
        let mut opt: Opt = Opt::from_args();
        opt.output_dir.push(filename);
        opt.output_dir.set_extension("csv");
        WriterBuilder::new().from_path(&opt.output_dir).unwrap()
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

impl Opt {}

fn main() {
    let start = Instant::now();
    let opt: Opt = Opt::from_args();

    let reader = RetrosheetReader::try_from(&opt.input).unwrap();
    Schema::write(reader).unwrap();
    let end = start.elapsed();
    println!("Elapsed: {:?}", end);
}

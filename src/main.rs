#![forbid(unsafe_code)]
#![allow(dead_code)]

use std::convert::{TryFrom};
use std::path::PathBuf;
use std::time::Instant;

use anyhow::{Context, Result};
use csv::{WriterBuilder, QuoteStyle, Writer};
use structopt::StructOpt;
use serde::{Serialize, Deserialize};
use strum::IntoEnumIterator;
use strum_macros::{Display, EnumIter};

use event_file::parser::{MappedRecord, RetrosheetReader};
use event_file::game_state::GameContext;
use event_file::schemas::{Event, ContextToVec};
use crate::event_file::schemas::{Game, GameTeam};
use std::fs::File;
use itertools::Itertools;
use std::collections::HashMap;
use crate::event_file::parser::RecordVec;
use crate::event_file::game_state::GameUmpire;

mod util;
mod event_file;

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
    EventFlag
}

impl Schema {
    fn write(reader: RetrosheetReader) -> Result<()> {
        let mut writer_map = Self::get_writer_map();

        for record_vec_result in reader {
            let game_context = GameContext::try_from(&record_vec_result?)?;
            // Write Game
            writer_map.get_mut(&Self::Game).unwrap()
                .serialize(Game::from(&game_context))?;
            // Write GameTeam
            let mut w = writer_map.get_mut(&Self::GameTeam).unwrap();
            for row in GameTeam::from_game_context(&game_context) {
                w.serialize(&row)?;
            }
            // Write GameUmpire
            let mut w = writer_map.get_mut(&Self::GameUmpire).unwrap();
            for row in game_context.umpires {
                w.serialize(&row)?;
            }
            // Write GameLineupAppearance
            let mut w = writer_map.get_mut(&Self::GameLineupAppearance).unwrap();
            for row in game_context.lineup_appearances {
                w.serialize(&row)?;
            }
            // Write GameFieldingAppearance
            let mut w = writer_map.get_mut(&Self::GameFieldingAppearance).unwrap();
            for row in game_context.fielding_appearances {
                w.serialize(&row)?;
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
        WriterBuilder::new()
            .from_path(&opt.output_dir)
            .unwrap()
    }
}


#[derive(StructOpt, Debug)]
#[structopt(name = "pbp-to-box", about = ABOUT)]
struct Opt {
    #[structopt(short, long, parse(from_os_str))]
    input: PathBuf,

    #[structopt(short, long, parse(from_os_str))]
    output_dir: PathBuf
}

impl Opt {
}


fn main() {
    let start = Instant::now();
    let opt: Opt = Opt::from_args();

    let reader = RetrosheetReader::try_from(&opt.input).unwrap();
    Schema::write(reader).unwrap();
    let end = start.elapsed();
    println!("Elapsed: {:?}", end);
}
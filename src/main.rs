#![forbid(unsafe_code)]

use std::convert::{TryFrom, TryInto};
use std::fs::File;
use std::path::PathBuf;
use std::time::Instant;

use anyhow::Result;
use csv::{Writer, WriterBuilder};
use structopt::StructOpt;

use event_file::parser::{MappedRecord, RetrosheetReader};
use event_file::event_output::GameState2;
use event_file::pbp_to_box::BoxScoreGame;
use crate::event_file::traits::RetrosheetEventRecord;

mod util;
mod event_file;

const ABOUT: &str = "Transforms Retrosheet .EV* files (play-by-play) into .EB* files (box score).";

#[derive(StructOpt, Debug)]
#[structopt(name = "pbp-to-box", about = ABOUT)]
struct Opt {
    #[structopt(short, long, parse(from_os_str))]
    input: PathBuf,

    #[structopt(short, long, parse(from_os_str))]
    output: PathBuf
}

impl Opt {
}

fn main() {
    let start = Instant::now();
    let opt: Opt = Opt::from_args();

    let mut reader = RetrosheetReader::try_from(&opt.input).unwrap();
    let mut writer = WriterBuilder::new()
        .has_headers(false)
        .flexible(true)
        .from_path(&opt.output).unwrap();

    for vec in reader {
        if let Ok(rv) = vec {
            if rv.iter().any(|mr| if let MappedRecord::BoxScoreEvent(_) = mr {true} else {false}) {
                continue
            }
            let mut init = GameState2::new(&rv);
            for record in &rv {
                let updated = init.update(record, 0);
                if let Err(e) = updated {
                    println!("Game: {:?}:\n{:?}", &rv.first(), e);
                    break
                }
            }
        }
    }
    let end = start.elapsed();
    println!("Elapsed: {:?}", end);
}

fn handle_game(writer: &mut Writer<File>, game: Result<BoxScoreGame>) -> Result<()> {
    let g = game?;
    if g.events.is_empty() { return Ok(()) }
    let vec: Vec<RetrosheetEventRecord> = g.try_into()?;
    for record in vec {
        writer.write_record(&record)?;
    }
    Ok(())
}
#![forbid(unsafe_code)]

use std::convert::{TryFrom, TryInto};
use std::fs::File;
use std::path::PathBuf;
use std::time::Instant;

use anyhow::Result;
use csv::{Writer, WriterBuilder};
use structopt::StructOpt;

use event_file::parser::RetrosheetReader;

use crate::event_file::parser::Game;
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

    let reader= RetrosheetReader::try_from(&opt.input).unwrap();
    let mut writer = WriterBuilder::new()
        .has_headers(false)
        .flexible(true)
        .from_path(&opt.output).unwrap();

    for game in reader {
        match handle_game(&mut writer, game) {
            Err(e) => println!("{:?}", e),
            _ => ()
        }
    }
    let end = start.elapsed();
    println!("Elapsed: {:?}", end);
}

fn handle_game(writer: &mut Writer<File>, game: Result<Game>) -> Result<()> {
    let g = game?;
    if g.events.is_empty() { return Ok(()) }
    let vec: Vec<RetrosheetEventRecord> = g.try_into()?;
    for record in vec {
        writer.write_record(&record)?;
    }
    Ok(())
}
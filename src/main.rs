#![forbid(unsafe_code)]

use std::convert::{TryFrom, TryInto};
use std::fs::File;
use std::time::Instant;

use anyhow::Result;
use csv::{Writer, WriterBuilder};

use event_file::parser::RetrosheetReader;

use crate::event_file::parser::Game;
use crate::event_file::traits::RetrosheetEventRecord;

mod util;
mod event_file;

fn main() {
    let start = Instant::now();
    let reader= RetrosheetReader::try_from("/home/davidroher/Downloads/sample.ev").unwrap();
    let mut writer = WriterBuilder::new()
        .has_headers(false)
        .flexible(true)
        .from_path("/home/davidroher/Downloads/SAMPLE-WINDOWS.EB").unwrap();

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
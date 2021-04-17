#![forbid(unsafe_code)]

use std::convert::{TryFrom};
use std::path::PathBuf;
use std::time::Instant;

use csv::{WriterBuilder, QuoteStyle};
use structopt::StructOpt;

use event_file::parser::{MappedRecord, RetrosheetReader};
use crate::event_file::event_output::GameContext;

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

    let reader = RetrosheetReader::try_from(&opt.input).unwrap();
    let mut writer = WriterBuilder::new()
        .has_headers(false)
        .flexible(true)
        .quote_style(QuoteStyle::Never)
        .from_path(&opt.output).unwrap();

    for vec in reader {
        if let Ok(rv) = vec {
            if rv.iter().any(|mr| matches!(mr, MappedRecord::BoxScoreEvent(_))) {
                continue
            }
            let game = GameContext::try_from(&rv);
            match game {
                Ok(v) =>  {writer.write_record(serde_json::to_string(&v)).unwrap();},
                Err(e) => println!("Game: {:?}:\n{:?}", &rv.first(), e)
            }
        }
    }
    let end = start.elapsed();
    println!("Elapsed: {:?}", end);
}
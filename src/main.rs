use std::time::Instant;
use event_file_parser::readit;
use std::str::FromStr;


mod event_file_entities;
mod event_file_parser;
mod play;
mod util;

fn main() {
    let start = Instant::now();
    readit();
    let end = start.elapsed();
    println!("Elapsed: {:?}", end);
}

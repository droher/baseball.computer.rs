#![forbid(unsafe_code)]

use std::time::Instant;
use event_file::parser::RetrosheetReader;
use std::convert::TryFrom;

mod util;
mod event_file;

fn main() {
    let start = Instant::now();
    for record in RetrosheetReader::try_from("/home/davidroher/Downloads/retrosheet/event/all.txt").unwrap().into_iter() {}
    let end = start.elapsed();
    println!("Elapsed: {:?}", end);
}

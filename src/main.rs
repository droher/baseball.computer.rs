#![forbid(unsafe_code)]

use std::time::Instant;
use event_file::parser::RetrosheetReader;
use std::convert::TryFrom;

mod util;
mod event_file;

fn main() {
    let start = Instant::now();
    let reader= RetrosheetReader::try_from("/home/davidroher/Repos/3p/retrosheet/event/all.txt").unwrap();
    reader.for_each(drop);
    let end = start.elapsed();
    println!("Elapsed: {:?}", end);
}

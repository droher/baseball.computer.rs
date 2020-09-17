#![forbid(unsafe_code)]

use std::time::Instant;
use event_file::parser::RetrosheetReader;
use std::convert::TryFrom;
use crate::event_file::parser::MappedRecord;
use crate::event_file::info::InfoRecord;

mod util;
mod event_file;

fn main() {
    let start = Instant::now();
    let mut reader= RetrosheetReader::try_from("/home/davidroher/Downloads/retrosheet/event/all.txt").unwrap();
    loop {
        let game = reader.next_game();
        if game.as_ref().map(|v| v.is_empty()).unwrap_or(false) {break}
        match game {
            Err(e) => println!("{:?}", e),
            Ok(v) => ()
        }
    }
    let end = start.elapsed();
    println!("Elapsed: {:?}", end);
}

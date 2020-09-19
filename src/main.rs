#![forbid(unsafe_code)]

use std::time::Instant;
use event_file::parser::RetrosheetReader;
use std::convert::TryFrom;
use crate::event_file::play::{PlayModifier};
use crate::event_file::parser::{MappedRecord, RecordVec};
use crate::event_file::info::InfoRecord;

mod util;
mod event_file;

fn main() {
    let start = Instant::now();
    let mut reader= RetrosheetReader::try_from("/home/davidroher/Downloads/retrosheet/event/all.txt").unwrap();
    reader.for_each(drop);
    let end = start.elapsed();
    println!("Elapsed: {:?}", end);
}

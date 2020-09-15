#![forbid(unsafe_code)]

use std::time::Instant;
use event_file::parser::readit;

mod util;
mod event_file;

fn main() {
    let start = Instant::now();
    readit();
    let end = start.elapsed();
    println!("Elapsed: {:?}", end);
}

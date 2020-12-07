#![forbid(unsafe_code)]

use std::time::Instant;
use event_file::parser::RetrosheetReader;
use event_file::pbp::GameState;
use std::convert::TryFrom;

mod util;
mod event_file;

fn main() {
    let start = Instant::now();
    let reader= RetrosheetReader::try_from("/home/davidroher/Repos/3p/retrosheet/event/regular/1916BOS.EVA").unwrap();
    for game in reader {
        match game {
            Ok(g) => {
                println!("{:?}", g.id);
                match GameState::get_box_score(&g) {
                    Ok(g) => (),
                    Err(e) => {println!("{:?}", e)}
                }
            }
            Err(e) => {}
        }
    }
    let end = start.elapsed();
    println!("Elapsed: {:?}", end);
}

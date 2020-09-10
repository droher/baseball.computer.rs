use std::fs::File;
use std::io::BufReader;
use std::str::FromStr;

use anyhow::{anyhow, Result};
use csv::{ReaderBuilder, Trim};

use crate::event_file_entities::{BatHandAdjustment, EventLineType, FromRetrosheetRecord, GameId, InfoRecord, LineupAdjustment, MappedRecord, PitchHandAdjustment, RetrosheetEventRecord, StartRecord, SubstitutionRecord, PlayRecord};

pub fn readit() {
    let file = File::open("/home/davidroher/Downloads/all.txt");
    let reader = BufReader::new(file.unwrap());
    let mut rdr = ReaderBuilder::new()
        .has_headers(false)
        .flexible(true)
        .from_reader(reader);
    let mut line_count = 0;

    for row in rdr.records() {
        let record = row.unwrap() as RetrosheetEventRecord;
        let m = MappedRecord::new(&record);
        line_count += 1;
        match m {
            Err(_) => println!("{:?}", (m, &record)),
            _ => ()
        };
    }
}

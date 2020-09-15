use std::fs::File;
use std::io::BufReader;

use csv::{ReaderBuilder, StringRecord};
use crate::event_file::entities::{RetrosheetEventRecord, MappedRecord, FromRetrosheetRecord};


pub fn readit() {
    let file = File::open("/home/davidroher/Downloads/retrosheet/event/all.txt");
    let mut rdr = ReaderBuilder::new()
        .has_headers(false)
        .flexible(true)
        .from_reader(BufReader::new(file.unwrap()));
    let mut record: RetrosheetEventRecord = StringRecord::new();
    loop {
        if rdr.read_record(&mut record).unwrap() {
            let m = MappedRecord::new(&record);
            match m {
                Ok(MappedRecord::Play(_)) => {}
                Err(e) => println!("{:?}", (e.to_string(), &record)),
                _ => ()
            }
        } else { break }
    }
}

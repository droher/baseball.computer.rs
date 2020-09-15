use std::fs::File;
use std::io::BufReader;

use anyhow::{Result, Context};
use csv::{ReaderBuilder, StringRecord};

use crate::event_file::misc::{GameId, StartRecord, SubstitutionRecord, BatHandAdjustment, PitchHandAdjustment, LineupAdjustment, EarnedRunRecord, Comment};
use crate::event_file::traits::{FromRetrosheetRecord, RetrosheetEventRecord};
use crate::event_file::info::InfoRecord;
use crate::event_file::box_score::{BoxScoreLine, LineScore, BoxScoreEvent};
use crate::event_file::play::PlayRecord;

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

#[derive(Debug)]
pub enum MappedRecord {
    GameId(GameId),
    Version,
    Info(InfoRecord),
    Start(StartRecord),
    Substitution(SubstitutionRecord),
    Play(PlayRecord),
    BatHandAdjustment(BatHandAdjustment),
    PitchHandAdjustment(PitchHandAdjustment),
    LineupAdjustment(LineupAdjustment),
    EarnedRun(EarnedRunRecord),
    Comment(Comment),
    BoxScoreLine(BoxScoreLine),
    LineScore(LineScore),
    BoxScoreEvent(BoxScoreEvent),
    Unrecognized
}
impl FromRetrosheetRecord for MappedRecord {
    fn new(record: &RetrosheetEventRecord) -> Result<MappedRecord>{
        let line_type = record.get(0).context("No record")?;
        let mapped= match line_type {
            "id" | "7d" => MappedRecord::GameId(GameId::new(record)?),
            "version" => MappedRecord::Version,
            "info" => MappedRecord::Info(InfoRecord::new(record)?),
            "start" => MappedRecord::Start(StartRecord::new(record)?),
            "sub" => MappedRecord::Substitution(SubstitutionRecord::new(record)?),
            "play" => MappedRecord::Play(PlayRecord::new(record)?),
            "badj" => MappedRecord::BatHandAdjustment(BatHandAdjustment::new(record)?),
            "padj" => MappedRecord::PitchHandAdjustment(PitchHandAdjustment::new(record)?),
            "ladj" => MappedRecord::LineupAdjustment(LineupAdjustment::new(record)?),
            "com" => MappedRecord::Comment(String::from(record.get(1).unwrap())),
            "data" => MappedRecord::EarnedRun(EarnedRunRecord::new(record)?),
            "stat" => MappedRecord::BoxScoreLine(BoxScoreLine::new(record)?),
            "line" => MappedRecord::LineScore(LineScore::new(record)?),
            "event" => MappedRecord::BoxScoreEvent(BoxScoreEvent::new(record)?),
            _ => MappedRecord::Unrecognized
        };
        match mapped {
            MappedRecord::Unrecognized => Err(Self::error("Unrecognized record type", record)),
            _ => Ok(mapped)
        }
    }
}
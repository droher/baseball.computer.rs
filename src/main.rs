#![allow(dead_code)]
#![forbid(unsafe_code)]
#![deny(clippy::all, clippy::cargo)]
#![warn(
    clippy::nursery,
    clippy::pedantic,
    clippy::unwrap_used,
    clippy::expect_used
)]
#![allow(clippy::module_name_repetitions, clippy::significant_drop_tightening)]

use event_file::writer::EventFileSchema;
use glob::GlobError;
use std::collections::HashSet;
use std::path::PathBuf;
use std::time::Instant;

use anyhow::Result;
use clap::Parser;
use lazy_static::lazy_static;
use rayon::prelude::*;
use tracing::{info, warn, Level};
use tracing_subscriber::FmtSubscriber;

use event_file::parser::RetrosheetReader;

use crate::event_file::misc::GameId;
use crate::event_file::parser::AccountType;
use crate::event_file::traits::EVENT_KEY_BUFFER;
use crate::event_file::writer::WriterMap;

mod event_file;

const ABOUT: &str = "Creates structured datasets from raw Retrosheet files.";

lazy_static! {
    static ref OUTPUT_ROOT: PathBuf = get_output_root(&Opt::parse());
    static ref WRITER_MAP: WriterMap = WriterMap::new(&OUTPUT_ROOT);
}

#[derive(Parser, Debug)]
#[command(name = "pbp-to-box", about = ABOUT)]
struct Opt {
    #[arg(short, long)]
    input: PathBuf,

    #[arg(short, long)]
    output_dir: PathBuf,
}

#[allow(clippy::expect_used)]
fn get_output_root(opt: &Opt) -> PathBuf {
    std::fs::create_dir_all(&opt.output_dir).expect("Error occurred on output dir check");
    opt.output_dir
        .canonicalize()
        .expect("Error occurred on output dir canonicalization")
}

struct FileProcessor {
    index: usize,
    opt: Opt,
    game_ids: HashSet<GameId>,
}

impl FileProcessor {
    pub fn new(opt: Opt) -> Self {
        Self {
            index: 0,
            opt,
            game_ids: HashSet::with_capacity(200_000),
        }
    }

    fn process_file(
        input_path: &PathBuf,
        parsed_games: Option<&HashSet<GameId>>,
        file_index: usize,
    ) -> Result<Vec<GameId>> {
        let reader = RetrosheetReader::new(input_path, file_index)?;
        EventFileSchema::write(reader, parsed_games)
    }

    pub fn par_process_files(&mut self, account_type: AccountType) -> Result<()> {
        // Box score accounts are expected to be duplicates so we don't need to check against them
        let parsed_games = if account_type == AccountType::BoxScore {
            None
        } else {
            Some(&self.game_ids)
        };
        let mut files = account_type
            .glob(&self.opt.input)?
            .collect::<Result<Vec<PathBuf>, GlobError>>()?;
        files.par_sort();
        let file_count = files.len();
        let games = files
            .into_par_iter()
            .enumerate()
            .map(|(i, f)| Self::process_file(&f, parsed_games, (self.index + i) * EVENT_KEY_BUFFER))
            .collect::<Result<Vec<Vec<GameId>>>>()?;
        self.index += file_count;
        let games = games.iter().flatten();
        self.game_ids.extend(games);
        Ok(())
    }

    pub fn process_files(&mut self) -> Result<()> {
        info!("Parsing conventional play-by-play files");
        self.par_process_files(AccountType::PlayByPlay)?;

        info!("Parsing deduced play-by-play files");
        self.par_process_files(AccountType::Deduced)?;

        info!("Parsing box score files");
        self.par_process_files(AccountType::BoxScore)?;

        WRITER_MAP.flush_all()?;
        Ok(())
    }
}

#[allow(clippy::expect_used)]
fn main() {
    let subscriber = FmtSubscriber::builder()
        .with_max_level(Level::INFO)
        .finish();
    tracing::subscriber::set_global_default(subscriber).expect("Failed to initialize trace");

    let start = Instant::now();
    let opt: Opt = Opt::parse();

    FileProcessor::new(opt)
        .process_files()
        .expect("Error occurred while processing files");

    let end = start.elapsed();
    info!("Elapsed: {:?}", end);
}

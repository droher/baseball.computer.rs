#![allow(dead_code)]
#![forbid(unsafe_code)]

use std::collections::HashSet;
use std::path::PathBuf;
use std::time::Instant;

use rayon::iter::{ParallelBridge, ParallelIterator};
use structopt::StructOpt;
use tracing::{info, Level};
use tracing_subscriber::FmtSubscriber;
use anyhow::Context;

use event_file::writer;
use event_file::writer::EventFileSchema;

use crate::event_file::misc::GameId;
use crate::event_file::parser::AccountType;

mod event_file;

const ABOUT: &str = "Creates structured datasets from raw Retrosheet files.";

#[derive(StructOpt, Debug)]
#[structopt(name = "pbp-to-box", about = ABOUT)]
struct Opt {
    #[structopt(short, long, parse(from_os_str))]
    input: PathBuf,

    #[structopt(short, long, parse(from_os_str))]
    output_dir: PathBuf,
}

fn main() {
    let mut parsed_game_ids = HashSet::with_capacity(200000);
    let subscriber = FmtSubscriber::builder()
        .with_max_level(Level::INFO)
        .finish();
    tracing::subscriber::set_global_default(subscriber).unwrap();

    let start = Instant::now();
    let opt: Opt = Opt::from_args();
    let output_root = get_output_root(&opt).unwrap();

    info!("Parsing conventional play-by-play files");
    let mut event_files = par_process_files(&opt, AccountType::PlayByPlay, Some(&parsed_game_ids));
    parsed_game_ids.extend(event_files.drain(..));

    info!("Parsing deduced play-by-play files");
    par_process_files(&opt, AccountType::Deduced, Some(&parsed_game_ids));

    info!("Parsing box score files");
    par_process_files(&opt, AccountType::BoxScore, None);

    info!("Merging files by schema");
    EventFileSchema::concat(output_root.to_str().unwrap());

    let end = start.elapsed();
    info!("Elapsed: {:?}", end);
}

fn par_process_files(
    opt: &Opt,
    account_type: AccountType,
    parsed_games: Option<&HashSet<GameId>>,
) -> Vec<GameId> {
    account_type
        .glob(&opt.input)
        .unwrap()
        .par_bridge()
        .flat_map(|f| writer::process_file(f, &opt.output_dir, parsed_games))
        .collect()
}

fn get_output_root(opt: &Opt) -> anyhow::Result<PathBuf> {
    std::fs::create_dir_all(&opt.output_dir).context("Error occurred on output dir check")?;
    opt.output_dir
        .canonicalize()
        .context("Invalid output directory")
}

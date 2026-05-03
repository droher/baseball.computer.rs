use pest_derive::Parser;

#[derive(Parser)]
#[grammar = "grammar/retrosheet.pest"]
pub struct RetrosheetParser;

pub mod quirks;
pub use quirks::{QuirkHit, QuirkKind, scan as scan_quirks};

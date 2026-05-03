//! Lint pass over a successful parse that surfaces every `@quirk` rule match.
//!
//! The grammar deliberately accepts more than `eventfile.htm` would suggest,
//! to match the corpus's tolerated divergences (see `docs/divergences.md`).
//! Each tolerated shape gets its own `Quirk` variant here so callers can
//! decide whether to log, ignore, or upgrade specific kinds to errors.

use pest::iterators::{Pair, Pairs};

use crate::Rule;

/// One quirk hit in the parse tree. `value` is the matched span (trimmed for
/// the noisy variants); `line_col` is the 1-indexed `(line, col)` of the
/// span start, useful for log messages.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct QuirkHit {
    pub kind: QuirkKind,
    pub value: String,
    pub line_col: (usize, usize),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub enum QuirkKind {
    /// Modifier token after `/` not in the recognized set
    /// (`unrecognized_modifier`).
    UnrecognizedModifier,
    /// Advance modifier inside `(...)` not in the recognized set
    /// (`unrecognized_advance_modifier`).
    UnrecognizedAdvanceModifier,
    /// `info,KEY,VALUE` had extra trailing chars after the typed value
    /// (whitespace, stray `"`, or `,...` extra fields).
    InfoTrailing,
    /// `info,KEY,VALUE` integer-typed value was not a pure decimal
    /// (empty, `unknown`, `1789 (paid)`, `5000c.`, trailing space, etc.).
    InfoIntNonNumeric,
    /// Quoted info value had non-comma chars after the closing quote
    /// (`info,inputter,"DWV/Smith"/RWood`).
    QuotedAtomTrailing,
    /// `play,...,COUNT` contained a stray `.` (one row in 1964WS.EVE).
    PlayCountStrayDot,
    /// `#` (questionable play), `!` (exceptional play), or space character
    /// consumed by the silent `play_strip` rule threaded through the play
    /// body grammar. Mirrors `STRIP_CHARS_REGEX = [#! ]` in the legacy
    /// parser.
    PlayStripTrailing,
}

impl QuirkKind {
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::UnrecognizedModifier => "unrecognized_modifier",
            Self::UnrecognizedAdvanceModifier => "unrecognized_advance_modifier",
            Self::InfoTrailing => "info_trailing",
            Self::InfoIntNonNumeric => "info_int_non_numeric",
            Self::QuotedAtomTrailing => "quoted_atom_trailing",
            Self::PlayCountStrayDot => "play_count_stray_dot",
            Self::PlayStripTrailing => "play_strip_trailing",
        }
    }
}

/// Walks the parse tree and returns every quirk hit, in source order.
#[must_use]
pub fn scan(pairs: Pairs<'_, Rule>) -> Vec<QuirkHit> {
    let mut out = Vec::new();
    walk(pairs, &mut out);
    out
}

fn walk(pairs: Pairs<'_, Rule>, out: &mut Vec<QuirkHit>) {
    for pair in pairs {
        match pair.as_rule() {
            Rule::unrecognized_modifier => push(out, &pair, QuirkKind::UnrecognizedModifier),
            Rule::unrecognized_advance_modifier => {
                push(out, &pair, QuirkKind::UnrecognizedAdvanceModifier);
            }
            Rule::info_trailing => push(out, &pair, QuirkKind::InfoTrailing),
            Rule::info_int_value => {
                if !is_pure_decimal(pair.as_str()) {
                    push(out, &pair, QuirkKind::InfoIntNonNumeric);
                }
            }
            Rule::quoted_atom => {
                if has_chars_after_close_quote(pair.as_str()) {
                    push(out, &pair, QuirkKind::QuotedAtomTrailing);
                }
            }
            Rule::play_count => {
                if pair.as_str().contains('.') {
                    push(out, &pair, QuirkKind::PlayCountStrayDot);
                }
            }
            _ => {}
        }
        walk(pair.into_inner(), out);
    }
}

fn push(out: &mut Vec<QuirkHit>, pair: &Pair<'_, Rule>, kind: QuirkKind) {
    out.push(QuirkHit {
        kind,
        value: pair.as_str().to_string(),
        line_col: pair.line_col(),
    });
}

fn is_pure_decimal(s: &str) -> bool {
    let trimmed = s.strip_prefix('-').unwrap_or(s);
    !trimmed.is_empty() && trimmed.bytes().all(|b| b.is_ascii_digit())
}

fn has_chars_after_close_quote(s: &str) -> bool {
    let bytes = s.as_bytes();
    if bytes.first() != Some(&b'"') {
        return false;
    }
    let mut i = 1;
    while i < bytes.len() && bytes[i] != b'"' {
        i += 1;
    }
    // bytes[i] is the closing quote (or EOF without one — span guarantees
    // the closer exists since `quoted_field` matched).
    i + 1 < bytes.len()
}

#[cfg(test)]
#[allow(clippy::expect_used)]
mod tests {
    use super::{QuirkKind, scan};
    use crate::{RetrosheetParser, Rule};
    use pest::Parser;

    fn quirks(input: &str) -> Vec<QuirkKind> {
        let pairs = RetrosheetParser::parse(Rule::file, input).expect("parse");
        scan(pairs).into_iter().map(|q| q.kind).collect()
    }

    #[test]
    fn clean_input_emits_no_quirks() {
        let input = "id,FOO\n\
                     info,visteam,NYA\n\
                     info,number,0\n\
                     info,timeofgame,180\n\
                     play,1,0,batt001,00,X,S8/L\n";
        let hits = quirks(input);
        assert!(hits.is_empty(), "expected no quirks, got: {hits:?}");
    }

    #[test]
    fn unrecognized_modifier_fires() {
        let input = "id,X\nplay,1,0,b,00,,8/BFDP\n";
        assert_eq!(quirks(input), vec![QuirkKind::UnrecognizedModifier]);
    }

    #[test]
    fn info_trailing_fires_on_extra_field() {
        let input = "id,X\ninfo,scorer,abc,extra\n";
        assert_eq!(quirks(input), vec![QuirkKind::InfoTrailing]);
    }

    #[test]
    fn info_int_non_numeric_fires_on_unknown() {
        let input = "id,X\ninfo,temp,unknown\n";
        assert_eq!(quirks(input), vec![QuirkKind::InfoIntNonNumeric]);
    }

    #[test]
    fn info_int_non_numeric_silent_on_clean_value() {
        let input = "id,X\ninfo,temp,72\n";
        assert!(quirks(input).is_empty());
    }

    #[test]
    fn info_int_non_numeric_silent_on_negative() {
        let input = "id,X\ninfo,temp,-1\n";
        assert!(quirks(input).is_empty());
    }

    #[test]
    fn quoted_atom_trailing_fires_on_chars_after_close() {
        let input = "id,X\ninfo,inputter,\"abc\"/extra\n";
        assert_eq!(quirks(input), vec![QuirkKind::QuotedAtomTrailing]);
    }

    #[test]
    fn quoted_atom_silent_when_no_trailing() {
        let input = "id,X\ninfo,scorer,\"71,215\"\n";
        assert!(quirks(input).is_empty());
    }

    #[test]
    fn play_count_stray_dot_fires() {
        let input = "id,X\nplay,1,0,b,00.,,NP\n";
        assert_eq!(quirks(input), vec![QuirkKind::PlayCountStrayDot]);
    }

    #[test]
    fn multiple_quirks_in_one_game() {
        let input = "id,X\n\
                     info,temp,unknown\n\
                     info,scorer,abc,extra\n\
                     play,1,0,b,00,,8/BFDP\n";
        let kinds = quirks(input);
        assert!(kinds.contains(&QuirkKind::InfoIntNonNumeric));
        assert!(kinds.contains(&QuirkKind::InfoTrailing));
        assert!(kinds.contains(&QuirkKind::UnrecognizedModifier));
    }
}

use std::fmt::Debug;
use std::path::Path;
use std::sync::{Arc, Mutex, PoisonError};

use anyhow::{Context, Result};
use tracing::field::{Field, Visit};
use tracing::span::{Attributes, Id};
use tracing::{Event, Level, Subscriber};
use tracing_subscriber::Layer;
use tracing_subscriber::layer::Context as LayerContext;
use tracing_subscriber::registry::LookupSpan;

pub const FINDINGS_FILE: &str = "findings.csv";
const SOURCE: &str = "retrosheet";
const HEADER: [&str; 6] = ["source", "kind", "game_id", "file", "line", "message"];

#[derive(Debug, Clone, Default, PartialEq, Eq, PartialOrd, Ord)]
pub struct FindingRow {
    pub kind: String,
    pub game_id: String,
    pub file: String,
    pub line: String,
    pub message: String,
}

#[derive(Debug, Default, Clone)]
struct Fields {
    kind: Option<String>,
    game_id: Option<String>,
    file: Option<String>,
    line: Option<String>,
    message: Option<String>,
}

impl Fields {
    fn set(&mut self, name: &str, value: String) {
        match name {
            "kind" => self.kind = Some(value),
            "game_id" => self.game_id = Some(value),
            "file" => self.file = Some(value),
            "line" => self.line = Some(value),
            "message" => self.message = Some(value),
            _ => {}
        }
    }
}

impl Visit for Fields {
    fn record_str(&mut self, field: &Field, value: &str) {
        self.set(field.name(), value.to_string());
    }

    fn record_u64(&mut self, field: &Field, value: u64) {
        self.set(field.name(), value.to_string());
    }

    fn record_i64(&mut self, field: &Field, value: i64) {
        self.set(field.name(), value.to_string());
    }

    fn record_debug(&mut self, field: &Field, value: &dyn Debug) {
        self.set(field.name(), format!("{value:?}"));
    }
}

#[derive(Clone, Default)]
pub struct FindingsLayer {
    rows: Arc<Mutex<Vec<FindingRow>>>,
}

impl FindingsLayer {
    pub fn rows(&self) -> Vec<FindingRow> {
        let mut rows = self
            .rows
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .clone();
        rows.sort();
        rows
    }

    pub fn write_csv(&self, path: &Path) -> Result<usize> {
        let rows = self.rows();
        let mut writer = csv::Writer::from_path(path)
            .with_context(|| format!("Failed to create {}", path.display()))?;
        writer.write_record(HEADER)?;
        for row in &rows {
            writer.write_record([
                SOURCE,
                &row.kind,
                &row.game_id,
                &row.file,
                &row.line,
                &row.message,
            ])?;
        }
        writer
            .flush()
            .with_context(|| format!("Failed to write {}", path.display()))?;
        Ok(rows.len())
    }
}

impl<S> Layer<S> for FindingsLayer
where
    S: Subscriber + for<'a> LookupSpan<'a>,
{
    fn on_new_span(&self, attrs: &Attributes<'_>, id: &Id, ctx: LayerContext<'_, S>) {
        let mut fields = Fields::default();
        attrs.record(&mut fields);
        if fields.game_id.is_none() && fields.file.is_none() {
            return;
        }
        if let Some(span) = ctx.span(id) {
            span.extensions_mut().insert(fields);
        }
    }

    fn on_event(&self, event: &Event<'_>, ctx: LayerContext<'_, S>) {
        if *event.metadata().level() > Level::WARN {
            return;
        }
        let mut fields = Fields::default();
        event.record(&mut fields);
        let Some(kind) = fields.kind.take() else {
            return;
        };
        if let Some(scope) = ctx.event_scope(event) {
            for span in scope {
                if let Some(outer) = span.extensions().get::<Fields>() {
                    fields.game_id = fields.game_id.or_else(|| outer.game_id.clone());
                    fields.file = fields.file.or_else(|| outer.file.clone());
                }
            }
        }
        let row = FindingRow {
            kind,
            game_id: fields.game_id.unwrap_or_default(),
            file: fields.file.unwrap_or_default(),
            line: fields.line.unwrap_or_default(),
            message: fields.message.unwrap_or_default(),
        };
        self.rows
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .push(row);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tracing::{info_span, warn};
    use tracing_subscriber::layer::SubscriberExt;

    fn capture(f: impl FnOnce()) -> Vec<FindingRow> {
        let layer = FindingsLayer::default();
        let subscriber = tracing_subscriber::registry().with(layer.clone());
        tracing::subscriber::with_default(subscriber, f);
        layer.rows()
    }

    #[test]
    fn kinded_warnings_take_file_and_game_from_the_span() {
        let rows = capture(|| {
            let _span = info_span!(
                "game",
                file = "retrosheet/ngl_b/1926.EBR",
                game_id = "PRG192601010"
            )
            .entered();
            warn!(kind = "duplicate_game", "duplicate game {}", "PRG192601010");
        });
        assert_eq!(
            rows,
            vec![FindingRow {
                kind: "duplicate_game".into(),
                game_id: "PRG192601010".into(),
                file: "retrosheet/ngl_b/1926.EBR".into(),
                line: String::new(),
                message: "duplicate game PRG192601010".into(),
            }]
        );
    }

    #[test]
    fn warnings_without_a_kind_and_lower_levels_are_ignored() {
        let rows = capture(|| {
            warn!("writer thread closing");
            tracing::info!(kind = "not_a_finding", "info level");
        });
        assert!(rows.is_empty());
    }

    #[test]
    fn event_fields_override_the_span() {
        let rows = capture(|| {
            let _span = info_span!("game", file = "a", game_id = "G1").entered();
            warn!(kind = "k", game_id = "G2", "m");
        });
        assert_eq!(rows[0].game_id, "G2");
        assert_eq!(rows[0].file, "a");
    }

    #[test]
    fn csv_has_a_header_even_when_empty() {
        let dir = std::env::temp_dir().join(format!("findings-test-{}", std::process::id()));
        std::fs::create_dir_all(&dir).ok();
        let path = dir.join(FINDINGS_FILE);
        let written = FindingsLayer::default().write_csv(&path).ok();
        let text = std::fs::read_to_string(&path).unwrap_or_default();
        std::fs::remove_dir_all(&dir).ok();
        assert_eq!(written, Some(0));
        assert_eq!(text, "source,kind,game_id,file,line,message\n");
    }
}

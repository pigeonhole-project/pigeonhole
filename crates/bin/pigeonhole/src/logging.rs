//! The process's log: `[log]` (level, format) to stderr (docs/observability.md).

use pigeonhole_chunk_store::{LogFormat, LoggingSettings};
use std::io::IsTerminal;
use tracing_subscriber::EnvFilter;

/// Install the global subscriber. `RUST_LOG`, when set, overrides
/// `[log] level`; colours only when stderr is a terminal.
pub fn init(settings: &LoggingSettings) {
    let filter = match std::env::var("RUST_LOG") {
        Ok(env) if !env.trim().is_empty() => EnvFilter::new(env),
        _ => EnvFilter::new(&settings.level),
    };
    let builder = tracing_subscriber::fmt().with_env_filter(filter).with_writer(std::io::stderr);
    let installed = match settings.format {
        LogFormat::Text => builder.with_ansi(std::io::stderr().is_terminal()).try_init(),
        LogFormat::Json => builder.json().with_current_span(true).with_span_list(true).try_init(),
    };
    // A second init (tests) keeps the first subscriber.
    let _ = installed;
}

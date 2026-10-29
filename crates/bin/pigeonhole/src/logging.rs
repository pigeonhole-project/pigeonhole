//! The process's log: `[log]` (level, format) to stderr (docs/observability.md).

use crate::config::{LogFormat, LoggingSettings};
use std::io::IsTerminal;
use tracing_subscriber::EnvFilter;

/// `RUST_LOG=debug` should not bring back openraft's per-step chatter or
/// s3s's ERROR lines for refused signatures: unless the directives name
/// them, they stay as quiet as in the default level.
fn with_quiet_libraries(directives: &str) -> String {
    let mut out = directives.to_string();
    for (target, level) in [("openraft", "warn"), ("s3s", "off")] {
        if !directives.contains(target) {
            out.push_str(&format!(",{target}={level}"));
        }
    }
    out
}

/// Install the global subscriber. `RUST_LOG`, when set, overrides
/// `[log] level`; colours only when stderr is a terminal.
pub fn init(settings: &LoggingSettings) {
    let filter = match std::env::var("RUST_LOG") {
        Ok(env) if !env.trim().is_empty() => EnvFilter::new(with_quiet_libraries(&env)),
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

#[cfg(test)]
mod tests {
    use super::with_quiet_libraries;

    #[test]
    fn rust_log_keeps_libraries_quiet_unless_it_names_them() {
        assert_eq!(with_quiet_libraries("debug"), "debug,openraft=warn,s3s=off");
        assert_eq!(with_quiet_libraries("debug,openraft=info"), "debug,openraft=info,s3s=off");
    }
}

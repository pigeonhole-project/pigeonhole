//! `pigeonhole` binary: argument parsing and subcommand dispatch.

mod admin;
mod background;
mod channel_log;
mod runtime;
mod serve;

use admin::{cmd_purge, cmd_restore};
use anyhow::{bail, Context};
use serve::cmd_serve;
use tracing::warn;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    pigeonhole::logging::init(&pigeonhole::config::LoggingSettings::from_config_file());

    let mut args = std::env::args().skip(1);
    match args.next().as_deref() {
        Some("restore") => {
            let mut lossy = false;
            for a in args {
                match a.as_str() {
                    "--lossy" => lossy = true,
                    other => bail!("unknown restore flag {other}; usage: pigeonhole restore --lossy"),
                }
            }
            cmd_restore(lossy).await
        }
        Some("purge") => {
            let mut yes = false;
            let mut expect_chat: Option<String> = None;
            let mut rest = args;
            while let Some(a) = rest.next() {
                match a.as_str() {
                    "--yes" => yes = true,
                    "--expect-messages" => {
                        let _ = rest.next().context("--expect-messages requires a number")?;
                        warn!("--expect-messages ignored (refs live in blob.db; sweeper is stage H)");
                    }
                    "--expect-chat" => {
                        expect_chat = Some(rest.next().context("--expect-chat requires chat_id")?.clone());
                    }
                    other => bail!("unknown purge flag {other}; usage: pigeonhole purge --yes [--expect-chat CHAT_ID]"),
                }
            }
            cmd_purge(yes, expect_chat.as_deref()).await
        }
        Some(other) => {
            bail!(
                "unknown command {other:?}; usage: pigeonhole \
                 [purge --yes | restore --lossy]"
            )
        }
        None => cmd_serve().await,
    }
}

//! `pigeonhole` binary: argument parsing and subcommand dispatch.

mod admin;
mod background;
mod runtime;
mod serve;

use admin::{cmd_purge, cmd_restore};
use anyhow::{bail, Context};
use serve::cmd_serve;
use tracing::warn;
use tracing_subscriber::EnvFilter;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info")),
        )
        .init();

    let mut args = std::env::args().skip(1);
    match args.next().as_deref() {
        Some("restore") => {
            let mut from = None;
            let mut lossy = false;
            let mut rest = args;
            while let Some(a) = rest.next() {
                match a.as_str() {
                    "--lossy" => lossy = true,
                    "--from-export" => {
                        from = Some(rest.next().context("--from-export requires a directory")?.into());
                    }
                    other => bail!(
                        "unknown restore flag {other}; usage: pigeonhole restore [--from-export DIR] --lossy"
                    ),
                }
            }
            cmd_restore(from, lossy).await
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
                        expect_chat = Some(
                            rest.next()
                                .context("--expect-chat requires chat_id")?
                                .to_string(),
                        );
                    }
                    other => bail!(
                        "unknown purge flag {other}; usage: pigeonhole purge --yes [--expect-chat CHAT_ID]"
                    ),
                }
            }
            cmd_purge(yes, expect_chat.as_deref()).await
        }
        Some(other) => {
            bail!(
                "unknown command {other:?}; usage: pigeonhole \
                 [purge --yes | restore [--from-export DIR] --lossy]"
            )
        }
        None => cmd_serve().await,
    }
}

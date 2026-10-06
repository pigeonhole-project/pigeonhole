use crate::config::Config;
use crate::index::{Index, OrphanMsg};
use crate::registry;
use crate::telegram::{TelegramClient, Update};
use std::collections::VecDeque;
use std::sync::Arc;
use tokio::sync::Mutex;
use tracing::{info, warn};

#[derive(Clone)]
struct PendingChat {
    chat_id: String,
    title: String,
}

pub fn spawn(index: Index, tg: TelegramClient, cfg: Config) {
    tokio::spawn(async move {
        info!(admin_chat = %cfg.admin_chat_id, "admin bot polling enabled");
        let pending: Arc<Mutex<VecDeque<PendingChat>>> = Arc::new(Mutex::new(VecDeque::new()));
        let mut offset: i64 = 0;

        match tg.get_updates(-1, 0).await {
            Ok(updates) => {
                if let Some(last) = updates.last() {
                    offset = last.update_id + 1;
                }
            }
            Err(e) => warn!(error = %e, "admin getUpdates bootstrap failed"),
        }

        let _ = tg
            .send_message(
                &cfg.admin_chat_id,
                "s3gram admin online.\n\
Commands:\n\
/bucket <name> [chat_id] — bind Telegram chat to S3 bucket\n\
/buckets — list buckets\n\
/unbind <name> — delete empty bucket\n\
/help\n\n\
Add the bot to a dedicated *data* chat (not service/admin), then /bucket <name>.
Each data chat binds to exactly one bucket; the bot renames it to s3:<name> when possible.",
            )
            .await;

        loop {
            match tg.get_updates(offset, 25).await {
                Ok(updates) => {
                    for u in updates {
                        offset = offset.max(u.update_id + 1);
                        if let Err(e) =
                            handle_update(&u, &index, &tg, &cfg, &pending).await
                        {
                            warn!(error = %e, "admin update handler failed");
                        }
                    }
                }
                Err(e) => {
                    warn!(error = %e, "admin getUpdates failed");
                    tokio::time::sleep(std::time::Duration::from_secs(3)).await;
                }
            }
        }
    });
}

async fn cleanup_orphans(tg: &TelegramClient, orphans: Vec<OrphanMsg>) {
    for (chat_id, message_id) in orphans {
        match tg.delete_message(&chat_id, message_id).await {
            Ok(true) => {}
            Ok(false) => {
                warn!(chat_id, message_id, "deleteMessage not confirmed during admin cleanup");
            }
            Err(e) => warn!(error = %e, chat_id, message_id, "deleteMessage error"),
        }
    }
}

async fn handle_update(
    update: &Update,
    index: &Index,
    tg: &TelegramClient,
    cfg: &Config,
    pending: &Arc<Mutex<VecDeque<PendingChat>>>,
) -> anyhow::Result<()> {
    if let Some(mcm) = &update.my_chat_member {
        if mcm.new_chat_member.user.is_bot {
            let status = mcm.new_chat_member.status.as_str();
            let chat_id = mcm.chat.id.to_string();
            let title = mcm
                .chat
                .title
                .clone()
                .unwrap_or_else(|| mcm.chat.chat_type.clone());

            if matches!(status, "member" | "administrator") {
                if cfg.is_service_chat(&chat_id) {
                    return Ok(());
                }
                {
                    let mut q = pending.lock().await;
                    q.retain(|p| p.chat_id != chat_id);
                    q.push_back(PendingChat {
                        chat_id: chat_id.clone(),
                        title: title.clone(),
                    });
                }
                tg.send_message(
                    &cfg.admin_chat_id,
                    &format!(
                        "Bot added to «{title}» (`{chat_id}`).\n\
Bind it:\n/bucket <name>\nor\n/bucket <name> {chat_id}"
                    ),
                )
                .await?;
            } else if matches!(status, "left" | "kicked") {
                pending.lock().await.retain(|p| p.chat_id != chat_id);
                tg.send_message(
                    &cfg.admin_chat_id,
                    &format!("Bot removed from «{title}» (`{chat_id}`)."),
                )
                .await?;
            }
        }
        return Ok(());
    }

    let Some(msg) = &update.message else {
        return Ok(());
    };
    let chat_id = msg.chat.id.to_string();
    if chat_id != cfg.admin_chat_id {
        return Ok(());
    }
    let Some(text) = msg.text.as_deref().map(str::trim) else {
        return Ok(());
    };
    if text.is_empty() {
        return Ok(());
    }

    if let Some(reply) = dispatch_command(text, index, tg, cfg, pending).await? {
        tg.send_message(&cfg.admin_chat_id, &reply).await?;
    }
    Ok(())
}

async fn dispatch_command(
    text: &str,
    index: &Index,
    tg: &TelegramClient,
    cfg: &Config,
    pending: &Arc<Mutex<VecDeque<PendingChat>>>,
) -> anyhow::Result<Option<String>> {
    let mut parts = text.split_whitespace();
    let raw_cmd = parts.next().unwrap_or("");
    let cmd = raw_cmd
        .split('@')
        .next()
        .unwrap_or(raw_cmd)
        .to_ascii_lowercase();

    match cmd.as_str() {
        "/help" | "help" => Ok(Some(
            "/bucket <name> [chat_id] — register bucket\n\
/buckets — list\n\
/unbind <name> — delete empty bucket\n\
/help"
                .into(),
        )),
        "/buckets" | "buckets" => {
            let buckets = index.list_buckets().await?;
            let mut lines = vec!["Buckets:".to_string()];
            for b in buckets {
                lines.push(format!("• {} → {}", b.name, b.chat_id));
            }
            let q = pending.lock().await;
            if !q.is_empty() {
                lines.push("Pending chats:".into());
                for p in q.iter() {
                    lines.push(format!("• «{}» ({})", p.title, p.chat_id));
                }
            }
            Ok(Some(lines.join("\n")))
        }
        "/bucket" | "bucket" => {
            let name = match parts.next() {
                Some(n) => n,
                None => {
                    return Ok(Some(
                        "Usage: /bucket <name> [chat_id]\nAdd the bot to a chat first, then /bucket <name>.".into(),
                    ));
                }
            };
            let chat = if let Some(explicit) = parts.next() {
                explicit.to_string()
            } else {
                let mut q = pending.lock().await;
                match q.pop_back() {
                    Some(p) => p.chat_id,
                    None => {
                        return Ok(Some(
                            "No pending chat. Add the bot to a *data* Telegram chat first (not the service chat), or:\n/bucket <name> <chat_id>".into(),
                        ));
                    }
                }
            };

            if cfg.is_service_chat(&chat) {
                return Ok(Some(
                    "That chat is the service/admin chat — object data cannot go there. Create a separate chat for the bucket.".into(),
                ));
            }

            match registry::register_bucket(
                index,
                tg,
                &cfg.service_bucket,
                &cfg.service_chat_id,
                &cfg.admin_chat_id,
                name,
                &chat,
                true,
            )
            .await
            {
                Ok((orphans, rename_note)) => {
                    cleanup_orphans(tg, orphans).await;
                    let mut msg = format!(
                        "OK: bucket `{name}` → chat `{chat}`\nRegistry: s3://{}/buckets/{name}.json",
                        cfg.service_bucket
                    );
                    if let Some(note) = rename_note {
                        msg.push('\n');
                        msg.push_str(&note);
                    }
                    Ok(Some(msg))
                }
                Err(e) => Ok(Some(format!("Failed: {e}"))),
            }
        }
        "/unbind" | "unbind" => {
            let name = match parts.next() {
                Some(n) => n,
                None => return Ok(Some("Usage: /unbind <name>".into())),
            };
            match registry::unregister_bucket(index, &cfg.service_bucket, name).await {
                Ok(Some(orphans)) => {
                    cleanup_orphans(tg, orphans).await;
                    Ok(Some(format!("Deleted bucket `{name}`.")))
                }
                Ok(None) => Ok(Some(format!("Bucket `{name}` not found."))),
                Err(e) => Ok(Some(format!("Failed: {e}"))),
            }
        }
        _ if cmd.starts_with('/') => Ok(Some("Unknown command. /help".into())),
        _ => Ok(None),
    }
}

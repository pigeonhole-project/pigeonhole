//! One channel through several bots (docs/shared-limiter.md, § Choosing the
//! account: a pool). Every API call goes to the bot whose budget for it is
//! ready soonest, and moves to another when Discord answers 429 (the call
//! was not applied). A send keeps its nonce when it moves: the other bot
//! never saw it (nonces are per bot), and the first did not apply it.
//!
//! Message ids and CDN URLs are the channel's, not a bot's, so any bot reads
//! what another posted. CDN downloads are not API calls: the primary (the
//! first bot) makes them.

use crate::limits::{DELETE, READ, SEND};
use crate::{DcMessage, DiscordApi, ThreadStart};
use anyhow::Result;
use async_trait::async_trait;
use bytes::Bytes;
use pigeonhole_limits::chat::Pooled;
use pigeonhole_limits::{ChatLimiter, Spend};
use std::sync::Arc;
use std::time::Duration;

/// How long an operation keeps moving and waiting before it gives up.
const DEADLINE: Duration = Duration::from_secs(3600);

/// A channel through several bots; the first is the primary.
pub struct PooledDiscord<C>(Pooled<C>);

impl<C: DiscordApi> PooledDiscord<C> {
    /// `members` with their limiters, the primary first. A member must give
    /// way on a 429 (end the call rate limited instead of waiting it out),
    /// unless it is alone: there is no one to move to.
    pub fn new(chat: &str, members: Vec<(C, Arc<ChatLimiter>)>) -> Self {
        Self(Pooled::new(chat, members, DEADLINE))
    }

    /// The primary's limiter.
    pub fn limiter(&self) -> &Arc<ChatLimiter> {
        self.0.limiter()
    }

    /// Bot `i`.
    pub fn member(&self, i: usize) -> &C {
        self.0.member(i)
    }
}

#[async_trait]
impl<C: DiscordApi> DiscordApi for PooledDiscord<C> {
    fn channel_id(&self) -> u64 {
        self.0.primary().channel_id()
    }

    async fn send(&self, content: &str, file: Option<(&str, Bytes)>, nonce: &str) -> Result<DcMessage> {
        self.0
            .run(Spend::Class(SEND), |m| {
                let file = file.clone();
                self.member(m).send(content, file, nonce)
            })
            .await
    }

    async fn get_message(&self, id: u64) -> Result<Option<DcMessage>> {
        self.0.run(Spend::Class(READ), |m| self.member(m).get_message(id)).await
    }

    async fn list(&self, before: Option<u64>, after: Option<u64>, limit: usize) -> Result<Vec<DcMessage>> {
        self.0.run(Spend::Class(READ), |m| self.member(m).list(before, after, limit)).await
    }

    async fn delete_messages(&self, ids: &[u64]) -> Result<()> {
        self.0.run(Spend::Class(DELETE), |m| self.member(m).delete_messages(ids)).await
    }

    async fn start_thread(&self, message_id: u64, name: &str) -> Result<ThreadStart> {
        self.0.run(Spend::Class(SEND), |m| self.member(m).start_thread(message_id, name)).await
    }

    async fn thread_name(&self, message_id: u64) -> Result<Option<String>> {
        self.0.run(Spend::Class(READ), |m| self.member(m).thread_name(message_id)).await
    }

    async fn archive_thread(&self, message_id: u64) -> Result<()> {
        self.0.run(Spend::Class(DELETE), |m| self.member(m).archive_thread(message_id)).await
    }

    async fn delete_thread(&self, message_id: u64) -> Result<()> {
        self.0.run(Spend::Class(DELETE), |m| self.member(m).delete_thread(message_id)).await
    }

    /// Not an API call: the primary makes it.
    async fn download(&self, url: &str, range: Option<(u64, u64)>) -> Result<Bytes> {
        self.0.primary().download(url, range).await
    }

    async fn pinned(&self) -> Result<Vec<DcMessage>> {
        self.0.run(Spend::Class(READ), |m| self.member(m).pinned()).await
    }

    async fn pin(&self, id: u64) -> Result<()> {
        self.0.run(Spend::Class(SEND), |m| self.member(m).pin(id)).await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::fake::bot::FakeBot;
    use crate::fake::FakeDiscord;
    use pigeonhole_limits::{LimiterRegistry, Rate};

    #[tokio::test(start_paused = true)]
    async fn a_429_moves_the_send_to_the_other_bot_and_reads_see_it() {
        let reg =
            LimiterRegistry::new(crate::limits().with(SEND, Rate::new(100.0, 10.0)), crate::limits::CLASSES).unwrap();
        let channel = Arc::new(FakeDiscord::new(7));
        let bot = |i: usize| {
            let limiter = reg.chat(&format!("bot-{i}"), "7");
            (FakeBot::new(channel.clone(), limiter.clone()), limiter)
        };
        let pooled = PooledDiscord::new("7", vec![bot(0), bot(1)]);
        pooled.member(0).flood_next(1, Duration::from_secs(40));
        let t0 = tokio::time::Instant::now();
        let m = pooled.send("entry", None, "n-1").await.unwrap();
        assert!(t0.elapsed() < Duration::from_secs(1), "the 40 s 429 was not waited out");
        assert_eq!((pooled.member(0).calls().send, pooled.member(1).calls().send), (0, 1));
        assert_eq!(pooled.get_message(m.id).await.unwrap().map(|m| m.content), Some("entry".into()));
        assert!(pooled.limiter().peek_wait(SEND) > Duration::from_secs(30), "the primary stays cooled");
    }
}

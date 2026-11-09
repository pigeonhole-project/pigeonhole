//! What layers do each time this node starts serving a term, before it
//! serves it ([`MetaStore::on_serve`]): a leader's writes and background
//! work rely on it, so [`MetaStore::is_leader`] waits for it.

use crate::{raft, serving, MetaStore};
use anyhow::Result;
use futures::future::BoxFuture;
use std::collections::BTreeMap;
use std::sync::{Arc, Mutex, Weak};
use std::time::Duration;
use tokio::sync::{watch, Notify};
use tracing::Instrument;

/// A layer's take-over: given the tablet, it writes what the leader needs
/// written before it serves. Run again in each term, so it must be
/// harmless to repeat; it proposes as usual (its proposals are not held
/// back by it).
pub type OnServe = Arc<dyn Fn(Arc<MetaStore>) -> BoxFuture<'static, Result<()>> + Send + Sync>;

/// Wait this long before running a failed hook again.
const RETRY: Duration = Duration::from_secs(1);

/// The hooks of a tablet and how their last run went.
#[derive(Default)]
pub(crate) struct Hooks {
    list: Mutex<Registered>,
    ran: watch::Sender<Ran>,
    added: Notify,
}

#[derive(Default)]
struct Registered {
    by_name: BTreeMap<&'static str, OnServe>,
    /// Bumped on every registration.
    version: u64,
}

/// The last run of every hook: in `term`, of the hooks as of `version`.
#[derive(Debug, Clone, Default)]
pub(crate) struct Ran {
    term: u64,
    version: u64,
    error: Option<String>,
}

impl Hooks {
    /// Whether every hook ran in `term` (true with none).
    pub(crate) fn ran_in(&self, term: u64) -> bool {
        let version = self.list.lock().unwrap().version;
        let ran = self.ran.borrow();
        version == 0 || (ran.term == term && ran.version == version && ran.error.is_none())
    }

    /// Add (or replace) `name`'s hook; the version it makes.
    pub(crate) fn add(&self, name: &'static str, hook: OnServe) -> u64 {
        let mut list = self.list.lock().unwrap();
        list.by_name.insert(name, hook);
        list.version += 1;
        self.added.notify_one();
        list.version
    }

    pub(crate) fn subscribe(&self) -> watch::Receiver<Ran> {
        self.ran.subscribe()
    }
}

impl Ran {
    /// The outcome of a run in `term` of the hooks as of `version` or later;
    /// `None` until there is one.
    pub(crate) fn outcome(&self, term: u64, version: u64) -> Option<Result<()>> {
        (self.term == term && self.version >= version)
            .then(|| self.error.clone().map_or(Ok(()), |e| Err(anyhow::anyhow!(e))))
    }
}

/// Run the hooks each time this node starts serving a term (Raft-wise: its
/// term's first entry applied), again when one is added, and every
/// [`RETRY`] after a run failed; ends with Raft. It holds the tablet only
/// while a run goes on.
pub(crate) fn spawn(meta: Weak<MetaStore>, mut metrics: watch::Receiver<raft::Metrics>, hooks: Arc<Hooks>) {
    tokio::spawn(
        async move {
            loop {
                let term = {
                    let m = metrics.borrow_and_update();
                    serving(&m).then_some(m.current_term)
                };
                if let Some(term) = term.filter(|&t| !hooks.ran_in(t)) {
                    let Some(meta) = meta.upgrade() else { return };
                    let ok = run(&meta, &hooks, term).await;
                    drop(meta);
                    if !ok {
                        tokio::time::sleep(RETRY).await;
                    }
                    continue;
                }
                tokio::select! {
                    changed = metrics.changed() => {
                        if changed.is_err() {
                            return;
                        }
                    }
                    () = hooks.added.notified() => {}
                }
            }
        }
        .in_current_span(),
    );
}

/// One run of every hook, in name order, stopping at the first that fails.
/// Whether they all succeeded.
async fn run(meta: &Arc<MetaStore>, hooks: &Hooks, term: u64) -> bool {
    let (list, version) = {
        let list = hooks.list.lock().unwrap();
        (list.by_name.clone(), list.version)
    };
    let mut error = None;
    for (name, hook) in list {
        if let Err(e) = hook(meta.clone()).await {
            tracing::warn!(term, hook = name, error = %format!("{e:#}"), "taking over failed; again soon");
            error = Some(format!("{name}: {e:#}"));
            break;
        }
    }
    if error.is_none() {
        tracing::info!(term, "serving: layers took over");
    }
    let ok = error.is_none();
    hooks.ran.send_replace(Ran { term, version, error });
    ok
}

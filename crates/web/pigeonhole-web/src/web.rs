//! What the site's Web Worker calls (site/src/assets/web-worker.js).
//!
//! No pointers cross the boundary: a request is one JSON line on stdin, then
//! `len` bytes of body; its answer is a JSON line on stdout,
//! `{"call": n, "reply": …}`, among the log events (JSON lines too, the
//! server's format).
//!
//! Nothing here blocks on a timer. [`ph_call`] only queues a request;
//! [`ph_run`] runs the node until it has nothing to do but wait, then
//! returns, and the worker calls it again when the next timer is due (the
//! shim's `poll_oneoff` tells it when). Waiting is the browser's, not a
//! spin: the worker stays free, and a call's events come out as they happen.

use crate::cluster::Nodes;
use crate::{World, DEMO_CONFIG};
use bytes::Bytes;
use pigeonhole_admin::Request;
use serde_json::{json, Value};
use std::cell::{Cell, RefCell};
use std::future::Future;
use std::io::{BufRead, Read, Write};
use std::pin::Pin;
use std::rc::Rc;
use std::sync::atomic::{AtomicBool, AtomicU8, Ordering};
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll, Waker};

/// Shared with the shim's `poll_oneoff` ([`ph_yield_flag`]):
/// - `[0]` = 1: [`ph_run`] wants to return, so a park must not wait for its
///   timer (the shim notes when the timer is due instead);
/// - `[1]` = 1: the shim did so, the next timer is known.
///
/// A park can end without reaching `poll_oneoff` (a wakeup was pending), so
/// [`ph_run`] returns only after a park that did reach it.
static YIELD: [AtomicU8; 2] = [AtomicU8::new(0), AtomicU8::new(0)];
/// Whether [`ph_run`] is running the node: only then does a park end it.
static RUNNING: AtomicBool = AtomicBool::new(false);
/// [`Idle`] waiting for the runtime to run out of work.
static IDLE: Mutex<Option<Waker>> = Mutex::new(None);

/// The runtime is about to park (nothing ready, only timers or nothing to
/// wait for): during [`ph_run`], end it instead.
fn parking() {
    if !RUNNING.load(Ordering::SeqCst) {
        return;
    }
    if YIELD[1].load(Ordering::SeqCst) == 1 {
        if let Some(w) = IDLE.lock().unwrap().take() {
            w.wake();
        }
    } else {
        YIELD[0].store(1, Ordering::SeqCst);
    }
}

/// Ready once the runtime has parked with nothing to run, its next timer
/// noted by the shim.
struct Idle;

impl Future for Idle {
    type Output = ();
    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<()> {
        if YIELD[1].load(Ordering::SeqCst) == 1 {
            return Poll::Ready(());
        }
        *IDLE.lock().unwrap() = Some(cx.waker().clone());
        Poll::Pending
    }
}

thread_local! {
    static RT: tokio::runtime::Runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .on_thread_park(parking)
        .build()
        .expect("tokio runtime");
    /// The requests in flight: tasks of this thread (the nodes are `Rc`).
    static LOCAL: tokio::task::LocalSet = tokio::task::LocalSet::new();
    static CALLS: Cell<u32> = const { Cell::new(0) };
    static WORLD: Arc<World> = Arc::new(World::default());
    static NODES: RefCell<Option<Rc<Nodes>>> = const { RefCell::new(None) };
    static CONFIG: RefCell<String> = RefCell::new(saved_config().unwrap_or_else(|| DEMO_CONFIG.to_string()));
}

/// Where the config that last booted is kept (the page's OPFS, as
/// `/data`): one node's, or the cluster's, every node in one file.
const SAVED_CONFIG: &str = "/data/pigeonhole.toml";

fn saved_config() -> Option<String> {
    std::fs::read_to_string(SAVED_CONFIG).ok()
}

/// The answer to call `call` (`None`: [`ph_open`]'s, which has no number).
fn answer(call: Option<u32>, result: anyhow::Result<Value>) -> i32 {
    let (v, code) = match result {
        Ok(v) => (v, 0),
        Err(e) => (json!({ "error": format!("{e:#}") }), 1),
    };
    let line = match call {
        Some(n) => json!({ "call": n, "reply": v }),
        None => json!({ "reply": v }),
    };
    let mut out = std::io::stdout().lock();
    let _ = writeln!(out, "{line}");
    let _ = out.flush();
    code
}

/// Sets up tracing to stdout; the node starts with the first `open` call.
#[allow(unsafe_code)] // `no_mangle` only: exported to JS, no unsafe code.
#[no_mangle]
pub extern "C" fn ph_open() -> i32 {
    tracing_subscriber::fmt()
        .json()
        .with_current_span(true)
        .with_span_list(true)
        .with_env_filter(
            "warn,pigeonhole_web=info,pigeonhole_meta=debug,pigeonhole_blob=debug,pigeonhole_gateway_s3=info,\
             pigeonhole_chunk_store=debug,pigeonhole_storage_telegram=debug,pigeonhole_storage_discord=debug,pigeonhole_fake_channels=debug,\
             openraft_grpc=debug,openraft_chat=debug,pigeonhole_raft_telegram=debug",
        )
        .with_writer(std::io::stdout)
        .init();
    let saved = saved_config();
    answer(
        None,
        Ok(
            json!({ "config": saved.clone().unwrap_or_else(|| DEMO_CONFIG.to_string()), "demo": DEMO_CONFIG, "saved": saved.is_some() }),
        ),
    )
}

/// Where [`YIELD`] is in the module's memory: the shim reads it.
#[allow(unsafe_code)] // `no_mangle` only: exported to JS, no unsafe code.
#[no_mangle]
pub extern "C" fn ph_yield_flag() -> u32 {
    YIELD.as_ptr() as usize as u32
}

/// Queues the request on stdin; its number (≥ 1), or 0 for a request that
/// could not be read (its error answered at once). Its answer comes out of
/// a later [`ph_run`].
#[allow(unsafe_code)] // `no_mangle` only: exported to JS, no unsafe code.
#[no_mangle]
pub extern "C" fn ph_call() -> i32 {
    let (r, body) = match read_request() {
        Ok(read) => read,
        Err(e) => return answer(Some(0), Err(e)) - 1,
    };
    let call = CALLS.with(|c| {
        c.set(c.get() + 1);
        c.get()
    });
    LOCAL.with(|local| {
        local.spawn_local(async move {
            answer(Some(call), run(r, body).await);
        })
    });
    call as i32
}

/// Runs the node (the calls queued, its timers, its background work) until
/// nothing is ready to run, and returns: the shim knows the next timer.
#[allow(unsafe_code)] // `no_mangle` only: exported to JS, no unsafe code.
#[no_mangle]
pub extern "C" fn ph_run() -> i32 {
    let clear = || YIELD.iter().for_each(|f| f.store(0, Ordering::SeqCst));
    clear();
    RUNNING.store(true, Ordering::SeqCst);
    RT.with(|rt| LOCAL.with(|local| rt.block_on(local.run_until(Idle))));
    RUNNING.store(false, Ordering::SeqCst);
    clear();
    0
}

fn read_request() -> anyhow::Result<(Request, Bytes)> {
    let mut stdin = std::io::stdin().lock();
    let mut line = String::new();
    stdin.read_line(&mut line)?;
    let req: Request = serde_json::from_str(&line)?;
    let mut body = vec![0; req.len];
    stdin.read_exact(&mut body)?;
    Ok((req, Bytes::from(body)))
}

/// (Re)starts on `text`: its node, or every node of its cluster; the old
/// ones are back if this fails.
async fn start(text: String) -> anyhow::Result<Value> {
    let world = WORLD.with(Arc::clone);
    let old = NODES.with(|n| n.borrow_mut().take());
    if let Some(old) = &old {
        old.close().await?;
    }
    let nodes = match Nodes::open(&world, &text).await {
        Ok(nodes) => nodes,
        Err(e) => {
            // Back on the config that ran: its tablets claim their logs again.
            if old.is_some() {
                let previous = CONFIG.with(|c| c.borrow().clone());
                NODES.with(|n| n.borrow_mut().take());
                let nodes = Nodes::open(&world, &previous).await?;
                NODES.with(|n| *n.borrow_mut() = Some(Rc::new(nodes)));
            }
            return Err(e);
        }
    };
    let leader = nodes.node(None)?;
    let replayed = leader.replayed();
    if leader.list("demo").await.is_err() {
        leader.create_bucket("demo").await?;
    }
    let ids: Vec<u64> = nodes.nodes.keys().copied().collect();
    let leading = nodes.leader();
    NODES.with(|n| *n.borrow_mut() = Some(Rc::new(nodes)));
    // Kept for the next page load; nowhere to keep it without a file system.
    let _ = std::fs::write(SAVED_CONFIG, &text);
    CONFIG.with(|c| *c.borrow_mut() = text);
    Ok(json!({ "replayed": replayed, "restarted": old.is_some(), "nodes": ids, "leader": leading }))
}

async fn run(r: Request, body: Bytes) -> anyhow::Result<Value> {
    match r.op.as_str() {
        "open" | "restart" => {
            let text = r.config.unwrap_or_else(|| CONFIG.with(|c| c.borrow().clone()));
            return start(text).await;
        }
        "config" => return Ok(json!({ "config": CONFIG.with(|c| c.borrow().clone()) })),
        // The page is going (a reload, another page): close as a server does
        // on SIGTERM, so a lease log is released and the next page's claim
        // need not wait the lease out.
        "close" => {
            let nodes = NODES.with(|n| n.borrow_mut().take());
            if let Some(nodes) = nodes {
                nodes.close().await?;
            }
            return Ok(json!({ "closed": true }));
        }
        _ => {}
    }
    let nodes = NODES.with(|n| n.borrow().clone()).ok_or_else(|| anyhow::anyhow!("no node: open first"))?;
    if r.op == "isolate" {
        let node = r.node.ok_or_else(|| anyhow::anyhow!("isolate: which node"))?;
        nodes.isolate(&WORLD.with(Arc::clone), node, r.down)?;
        return Ok(json!({ "node": node, "isolated": r.down }));
    }
    nodes.node(r.node)?.admin.handle(&r, body).await
}

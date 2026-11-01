//! What the site's Web Worker calls (site/src/assets/web-worker.js).
//!
//! No pointers cross the boundary: a request is one JSON line on stdin, then
//! `len` bytes of body; the answer is one JSON line on stdout, after the log
//! events the call produced (JSON lines too, the server's format).

use crate::{parse_config, Node, World, DEMO_CONFIG};
use bytes::Bytes;
use pigeonhole_admin::Request;
use serde_json::{json, Value};
use std::cell::RefCell;
use std::io::{BufRead, Read, Write};
use std::rc::Rc;
use std::sync::Arc;

thread_local! {
    static RT: tokio::runtime::Runtime =
        tokio::runtime::Builder::new_current_thread().enable_all().build().expect("tokio runtime");
    static WORLD: Arc<World> = Arc::new(World::default());
    static NODE: RefCell<Option<Rc<Node>>> = const { RefCell::new(None) };
    static CONFIG: RefCell<String> = RefCell::new(saved_config().unwrap_or_else(|| DEMO_CONFIG.to_string()));
}

/// Where the last config that booted is kept (the page's OPFS, as `/data`).
const SAVED_CONFIG: &str = "/data/pigeonhole.toml";

fn saved_config() -> Option<String> {
    std::fs::read_to_string(SAVED_CONFIG).ok()
}

fn answer(v: &Value) {
    let mut out = std::io::stdout().lock();
    let _ = writeln!(out, "{}", json!({ "reply": v }));
    let _ = out.flush();
}

fn reply(result: anyhow::Result<Value>) -> i32 {
    match result {
        Ok(v) => {
            answer(&v);
            0
        }
        Err(e) => {
            answer(&json!({ "error": format!("{e:#}") }));
            1
        }
    }
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
             pigeonhole_chunk_store=debug,pigeonhole_storage_telegram=debug,pigeonhole_storage_discord=debug,pigeonhole_fake_channels=debug",
        )
        .with_writer(std::io::stdout)
        .init();
    let saved = saved_config();
    reply(Ok(
        json!({ "config": saved.clone().unwrap_or_else(|| DEMO_CONFIG.to_string()), "demo": DEMO_CONFIG, "saved": saved.is_some() }),
    ))
}

/// Lets the node's background work run for a moment (a snapshot being
/// built, timers): between calls nothing else runs on this thread. The
/// worker calls it while idle; what it logged goes to stdout as usual.
#[allow(unsafe_code)] // `no_mangle` only: exported to JS, no unsafe code.
#[no_mangle]
pub extern "C" fn ph_tick() -> i32 {
    // The timer is made inside the runtime: outside it, it panics.
    RT.with(|rt| rt.block_on(async { tokio::time::sleep(std::time::Duration::from_millis(15)).await }));
    0
}

/// Runs one request read from stdin.
#[allow(unsafe_code)] // `no_mangle` only: exported to JS, no unsafe code.
#[no_mangle]
pub extern "C" fn ph_call() -> i32 {
    reply(read_request().and_then(|(r, body)| RT.with(|rt| rt.block_on(run(r, body)))))
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

/// (Re)starts the node on `text`; the old one keeps running if this fails.
async fn start(text: String) -> anyhow::Result<Value> {
    let cfg = parse_config(&text)?;
    let world = WORLD.with(Arc::clone);
    let old = NODE.with(|n| n.borrow_mut().take());
    if let Some(old) = &old {
        old.close().await?;
    }
    let node = match Node::open(&world, &cfg).await {
        Ok(node) => node,
        Err(e) => {
            // Back on the config that ran: its tablet claims the log again.
            if old.is_some() {
                let previous = CONFIG.with(|c| c.borrow().clone());
                let node = Node::open(&world, &parse_config(&previous)?).await?;
                NODE.with(|n| *n.borrow_mut() = Some(Rc::new(node)));
            }
            return Err(e);
        }
    };
    let replayed = node.replayed();
    if node.list("demo").await.is_err() {
        node.create_bucket("demo").await?;
    }
    NODE.with(|n| *n.borrow_mut() = Some(Rc::new(node)));
    // Kept for the next page load; nowhere to keep it without a file system.
    let _ = std::fs::write(SAVED_CONFIG, &text);
    CONFIG.with(|c| *c.borrow_mut() = text);
    // Which channel pane each store is drawn in.
    let stores: Vec<Value> =
        cfg.instances.iter().map(|i| json!({ "id": i.info.id, "kind": i.kind.as_str() })).collect();
    Ok(json!({ "replayed": replayed, "restarted": old.is_some(), "stores": stores }))
}

async fn run(r: Request, body: Bytes) -> anyhow::Result<Value> {
    match r.op.as_str() {
        "open" | "restart" => {
            let text = r.config.unwrap_or_else(|| CONFIG.with(|c| c.borrow().clone()));
            return start(text).await;
        }
        "config" => return Ok(json!({ "config": CONFIG.with(|c| c.borrow().clone()) })),
        _ => {}
    }
    let node = NODE.with(|n| n.borrow().clone()).ok_or_else(|| anyhow::anyhow!("no node: open first"))?;
    node.admin.handle(&r, body).await
}

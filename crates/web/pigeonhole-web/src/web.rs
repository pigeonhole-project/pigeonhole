//! What the site's Web Worker calls (site/src/assets/web-worker.js).
//!
//! No pointers cross the boundary: a request is one JSON line on stdin, then
//! `len` bytes of body; the answer is one JSON line on stdout, after the log
//! events the call produced (JSON lines too, the server's format).

use crate::cluster::Nodes;
use crate::{parse_config, World, DEMO_CONFIG};
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
    static NODES: RefCell<Option<Rc<Nodes>>> = const { RefCell::new(None) };
    static CONFIGS: RefCell<Vec<String>> = RefCell::new(saved_configs().unwrap_or_else(|| vec![DEMO_CONFIG.to_string()]));
}

/// Where the configs that last booted are kept (the page's OPFS, as
/// `/data`): one node's, or each node's of a cluster, as a JSON list.
const SAVED_CONFIGS: &str = "/data/configs.json";

fn saved_configs() -> Option<Vec<String>> {
    serde_json::from_str(&std::fs::read_to_string(SAVED_CONFIGS).ok()?).ok()
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
    let saved = saved_configs();
    let configs = saved.clone().unwrap_or_else(|| vec![DEMO_CONFIG.to_string()]);
    reply(Ok(json!({ "configs": configs, "demo": DEMO_CONFIG, "saved": saved.is_some() })))
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

/// (Re)starts on `texts` (one node's config, or each node's of a
/// cluster); the old nodes are back if this fails.
async fn start(texts: Vec<String>) -> anyhow::Result<Value> {
    let configs = texts.iter().map(|t| parse_config(t)).collect::<anyhow::Result<Vec<_>>>()?;
    let world = WORLD.with(Arc::clone);
    let old = NODES.with(|n| n.borrow_mut().take());
    if let Some(old) = &old {
        old.close().await?;
    }
    let nodes = match Nodes::open(&world, &configs).await {
        Ok(nodes) => nodes,
        Err(e) => {
            // Back on the configs that ran: their tablets claim their logs again.
            if old.is_some() {
                let previous = CONFIGS.with(|c| c.borrow().clone());
                let previous = previous.iter().map(|t| parse_config(t)).collect::<anyhow::Result<Vec<_>>>()?;
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
    let _ = std::fs::write(SAVED_CONFIGS, serde_json::to_string(&texts)?);
    CONFIGS.with(|c| *c.borrow_mut() = texts);
    Ok(json!({ "replayed": replayed, "restarted": old.is_some(), "nodes": ids, "leader": leading }))
}

async fn run(r: Request, body: Bytes) -> anyhow::Result<Value> {
    match r.op.as_str() {
        "open" | "restart" => {
            let texts = match (r.configs, r.config) {
                (Some(configs), _) => configs,
                (None, Some(one)) => vec![one],
                (None, None) => CONFIGS.with(|c| c.borrow().clone()),
            };
            return start(texts).await;
        }
        "config" => return Ok(json!({ "configs": CONFIGS.with(|c| c.borrow().clone()) })),
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

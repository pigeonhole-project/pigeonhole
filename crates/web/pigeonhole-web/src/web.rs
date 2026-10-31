//! What the site's Web Worker calls (site/src/assets/web-worker.js).
//!
//! No pointers cross the boundary: a request is one JSON line on stdin, then
//! `len` bytes of body; the answer is one JSON line on stdout, after the log
//! events the call produced (JSON lines too, the server's format).

use crate::{Node, Options};
use bytes::Bytes;
use serde::Deserialize;
use serde_json::{json, Value};
use std::cell::RefCell;
use std::io::{BufRead, Read, Write};
use std::rc::Rc;

thread_local! {
    static RT: tokio::runtime::Runtime =
        tokio::runtime::Builder::new_current_thread().enable_all().build().expect("tokio runtime");
    static NODE: RefCell<Option<Rc<Node>>> = const { RefCell::new(None) };
}

#[derive(Deserialize)]
struct Request {
    op: String,
    #[serde(default = "demo")]
    bucket: String,
    #[serde(default)]
    key: String,
    #[serde(default)]
    to: String,
    #[serde(default)]
    range: Option<(u64, u64)>,
    #[serde(default)]
    len: usize,
}

fn demo() -> String {
    "demo".into()
}

fn answer(v: &Value) {
    let mut out = std::io::stdout().lock();
    let _ = writeln!(out, "{}", json!({ "reply": v }));
    let _ = out.flush();
}

/// Boots the node: tracing to stdout, the tablet, the stores, bucket `demo`.
#[allow(unsafe_code)] // `no_mangle` only: exported to JS, no unsafe code.
#[no_mangle]
pub extern "C" fn ph_open() -> i32 {
    tracing_subscriber::fmt()
        .json()
        .with_current_span(true)
        .with_span_list(true)
        .with_env_filter(
            "warn,pigeonhole_meta=debug,pigeonhole_storage_memory=debug,pigeonhole_blob=debug,\
             pigeonhole_gateway_s3=info,pigeonhole_chunk_store=debug",
        )
        .with_writer(std::io::stdout)
        .init();
    let opened = RT.with(|rt| {
        rt.block_on(async {
            let node = Node::open(&Options::default()).await?;
            node.create_bucket("demo").await?;
            anyhow::Ok(node)
        })
    });
    match opened {
        Ok(node) => {
            NODE.with(|n| *n.borrow_mut() = Some(Rc::new(node)));
            answer(&json!({ "ok": true }));
            0
        }
        Err(e) => {
            answer(&json!({ "error": format!("{e:#}") }));
            1
        }
    }
}

/// Runs one request read from stdin.
#[allow(unsafe_code)] // `no_mangle` only: exported to JS, no unsafe code.
#[no_mangle]
pub extern "C" fn ph_call() -> i32 {
    let result = read_request().and_then(|(r, body)| {
        let node = NODE.with(|n| n.borrow().clone()).ok_or_else(|| anyhow::anyhow!("ph_open first"))?;
        RT.with(|rt| rt.block_on(run(&node, r, body)))
    });
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

fn read_request() -> anyhow::Result<(Request, Bytes)> {
    let mut stdin = std::io::stdin().lock();
    let mut line = String::new();
    stdin.read_line(&mut line)?;
    let req: Request = serde_json::from_str(&line)?;
    let mut body = vec![0; req.len];
    stdin.read_exact(&mut body)?;
    Ok((req, Bytes::from(body)))
}

async fn run(node: &Node, r: Request, body: Bytes) -> anyhow::Result<Value> {
    Ok(match r.op.as_str() {
        "put" => {
            node.put(&r.bucket, &r.key, body).await?;
            json!({ "ok": true })
        }
        "get" => {
            let bytes = node.get(&r.bucket, &r.key, r.range).await?;
            json!({ "len": bytes.len() })
        }
        "copy" => {
            node.copy(&r.bucket, &r.key, &r.to).await?;
            json!({ "ok": true })
        }
        "delete" => {
            node.delete(&r.bucket, &r.key).await?;
            json!({ "ok": true })
        }
        "list" => json!({ "objects": node.list(&r.bucket).await? }),
        "repair" => {
            let s = node.repair().await?;
            json!({ "repaired": s.repaired, "failed": s.failed, "queued": s.scrub_enqueued })
        }
        "sweep" => {
            let s = node.sweep().await?;
            json!({ "deleted": s.keys_deleted, "zero_ref": s.zero_ref_chunks })
        }
        other => anyhow::bail!("unknown op {other:?}"),
    })
}

//! SlateDB on pigeonhole's S3 gateway: the steps of the demo, one per
//! subcommand. The database lives in `$SLATE_BUCKET` under `$SLATE_PATH` at
//! `$SLATE_S3_URL`; keys from `AWS_ACCESS_KEY_ID` / `AWS_SECRET_ACCESS_KEY`.
//!
//!   slate put <from> <count>   write pigeons <from>.. and close
//!   slate writer               write a tick every 200 ms, say which are durable
//!   slate scan <prefix>        read as a DbReader (no write rights, fences no one)
//!   slate fence                two writers on one database: the older is fenced
//!   slate flush <rounds>       write and flush memtables to L0 SSTs, let them compact
//!   slate status               the latest manifest: epochs, L0, sorted runs, checkpoints
//!   slate checkpoint           a checkpoint of the database as it is now; prints its id
//!   slate clone <path> <id>    a new database at <path>, a copy of the checkpoint

use anyhow::{bail, Context, Result};
use slatedb::admin::{Admin, CloneSourceSpec};
use slatedb::config::{CheckpointOptions, FlushOptions, FlushType, Settings};
use slatedb::object_store::aws::{AmazonS3Builder, S3ConditionalPut};
use slatedb::object_store::ObjectStore;
use slatedb::{Db, DbReader};
use std::io::Write;
use std::sync::Arc;
use std::time::{Duration, Instant};

const NAMES: &[&str] = &["Gerald", "Margaret", "Pip", "Dmitri", "Wanda", "Olga", "Bartholomew", "Ivy"];

fn env(name: &str) -> Result<String> {
    std::env::var(name).with_context(|| format!("{name} is not set"))
}

/// The bucket as an object store: path-style requests over plain HTTP,
/// conditional puts by ETag (`If-None-Match: *`, `If-Match`).
fn store() -> Result<Arc<dyn ObjectStore>> {
    let s3 = AmazonS3Builder::from_env()
        .with_endpoint(env("SLATE_S3_URL")?)
        .with_allow_http(true)
        .with_virtual_hosted_style_request(false)
        .with_bucket_name(env("SLATE_BUCKET")?)
        .with_conditional_put(S3ConditionalPut::ETagMatch)
        .build()?;
    Ok(Arc::new(s3))
}

/// Settings from `SLATE_FLUSH_MS`: how often the WAL goes to the bucket (a
/// PUT each, so a Telegram channel's ~20 posts a minute wants seconds).
fn settings() -> Result<Settings> {
    let mut settings = Settings::default();
    if let Ok(ms) = std::env::var("SLATE_FLUSH_MS") {
        settings.flush_interval = Some(Duration::from_millis(ms.parse()?));
    }
    Ok(settings)
}

async fn open() -> Result<Db> {
    let started = Instant::now();
    let db = Db::builder(env("SLATE_PATH")?, store()?).with_settings(settings()?).build().await?;
    println!("  opened in {:.1?}", started.elapsed());
    Ok(db)
}

async fn put(from: u64, count: u64) -> Result<()> {
    let db = open().await?;
    let started = Instant::now();
    let mut last = None;
    for i in from..from + count {
        let name = NAMES[i as usize % NAMES.len()];
        last = Some(db.put(format!("pigeon/{i:04}"), name).await?);
    }
    println!("  {count} pigeons in memory after {:.1?}", started.elapsed());
    if let Some(last) = last {
        last.await_durable().await?;
    }
    println!("  durable in the bucket after {:.1?} (the WAL goes as one object per flush)", started.elapsed());
    db.close().await?;
    Ok(())
}

/// Writes ticks until killed; a tick is printed once its put returned, i.e.
/// once it is durable in the bucket.
async fn writer() -> Result<()> {
    let db = open().await?;
    for n in 0.. {
        db.put(format!("tick/{n:06}"), n.to_string()).await?.await_durable().await?;
        println!("  durable tick/{n:06}");
        std::io::stdout().flush()?;
        tokio::time::sleep(Duration::from_millis(200)).await;
    }
    Ok(())
}

async fn scan(prefix: &str) -> Result<()> {
    let reader = DbReader::builder(env("SLATE_PATH")?, store()?).build().await?;
    let mut it = reader.scan_prefix(prefix.as_bytes(), ..).await?;
    let mut rows = Vec::new();
    while let Some(kv) = it.next().await? {
        rows.push((String::from_utf8_lossy(&kv.key).into_owned(), String::from_utf8_lossy(&kv.value).into_owned()));
    }
    match rows.len() {
        0 => println!("  (nothing under {prefix})"),
        n if n <= 10 => rows.iter().for_each(|(k, v)| println!("  {k} = {v}")),
        n => {
            rows[..3].iter().for_each(|(k, v)| println!("  {k} = {v}"));
            println!("  … {} more …", n - 6);
            rows[n - 3..].iter().for_each(|(k, v)| println!("  {k} = {v}"));
        }
    }
    println!("  {} keys under {prefix}", rows.len());
    reader.close().await?;
    Ok(())
}

async fn fence() -> Result<()> {
    println!("writer A:");
    let a = open().await?;
    a.put("fence/a", "first").await?.await_durable().await?;
    println!("  put fence/a");
    println!("writer B, the same database:");
    let b = open().await?;
    b.put("fence/b", "second").await?.await_durable().await?;
    println!("  put fence/b");
    println!("writer A again:");
    match async { a.put("fence/a", "late").await?.await_durable().await }.await {
        Ok(_) => bail!("writer A was not fenced"),
        Err(e) => println!("  refused: {e}"),
    }
    b.close().await?;
    Ok(())
}

async fn flush(rounds: u64) -> Result<()> {
    let db = open().await?;
    for round in 0..rounds {
        for i in 0..50 {
            db.put(format!("flock/{round:02}/{i:03}"), NAMES[i % NAMES.len()]).await?;
        }
        db.flush_with_options(FlushOptions { flush_type: FlushType::MemTable }).await?;
        println!("  round {round}: 50 keys, memtable flushed to an L0 SST");
    }
    // The compactor runs in this process and merges L0 SSTs into sorted
    // runs once there are 4: wait until the manifest shows L0 empty.
    let admin = admin(env("SLATE_PATH")?)?;
    let started = Instant::now();
    while started.elapsed() < Duration::from_secs(300) {
        let m = admin.read_manifest(None).await?.context("no manifest")?;
        if m.l0().is_empty() && !m.compacted().is_empty() {
            println!("  compacted into sorted runs after {:.1?}", started.elapsed());
            return Ok(db.close().await?);
        }
        tokio::time::sleep(Duration::from_secs(1)).await;
    }
    bail!("L0 not compacted in 300 s")
}

fn admin(path: String) -> Result<Admin> {
    Ok(Admin::builder(path, store()?).build())
}

async fn status() -> Result<()> {
    let Some(m) = admin(env("SLATE_PATH")?)?.read_manifest(None).await? else {
        bail!("no manifest: no database here");
    };
    let runs: Vec<usize> = m.compacted().iter().map(|run| run.sst_views().len()).collect();
    println!("  manifest {}: writer_epoch {}, compactor_epoch {}", m.id(), m.writer_epoch(), m.compactor_epoch());
    println!("  L0 SSTs: {}, sorted runs: {} (SSTs in each: {runs:?})", m.l0().len(), runs.len());
    println!("  next WAL object: {}, checkpoints: {}", m.next_wal_sst_id(), m.checkpoints().len());
    Ok(())
}

async fn checkpoint() -> Result<()> {
    let options = CheckpointOptions { name: Some("demo".into()), ..CheckpointOptions::default() };
    let made = admin(env("SLATE_PATH")?)?.create_detached_checkpoint(&options).await?;
    println!("{}", made.id);
    Ok(())
}

async fn clone(path: &str, id: &str) -> Result<()> {
    let source = CloneSourceSpec::with_checkpoint(env("SLATE_PATH")?, id.parse()?);
    admin(path.to_string())?.create_clone_builder_from_source(source).build().await?;
    println!("  {path}: a clone of checkpoint {id} (it shares the parent's SSTs, copies only its WAL)");
    Ok(())
}

#[tokio::main]
async fn main() -> Result<()> {
    let args: Vec<String> = std::env::args().skip(1).collect();
    let args: Vec<&str> = args.iter().map(String::as_str).collect();
    match args.as_slice() {
        ["put", from, count] => put(from.parse()?, count.parse()?).await,
        ["writer"] => writer().await,
        ["scan", prefix] => scan(prefix).await,
        ["fence"] => fence().await,
        ["flush", rounds] => flush(rounds.parse()?).await,
        ["status"] => status().await,
        ["checkpoint"] => checkpoint().await,
        ["clone", path, id] => clone(path, id).await,
        _ => bail!("usage: slate put <from> <count> | writer | scan <prefix> | fence | flush <rounds> | status | checkpoint | clone <path> <id>"),
    }
}

//! Smoke run: the demo's stack, a few S3 calls, every event as a JSON line.
//!
//!     cargo build --release --target wasm32-wasip1 -p pigeonhole-web
//!     node crates/web/run-wasi.mjs target/wasm32-wasip1/release/pigeonhole-web.wasm

use bytes::Bytes;
use pigeonhole_web::{Node, Options};

fn noise(len: usize) -> Bytes {
    // xorshift: random-looking bytes without a dependency.
    let mut x: u64 = 0x9E37_79B9_7F4A_7C15;
    (0..len)
        .map(|_| {
            x ^= x << 13;
            x ^= x >> 7;
            x ^= x << 17;
            x as u8
        })
        .collect()
}

fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt()
        .json()
        .with_current_span(true)
        .with_span_list(true)
        .with_env_filter("warn,pigeonhole_meta=debug,pigeonhole_storage_memory=debug,pigeonhole_blob=debug,pigeonhole_gateway_s3=info")
        .with_writer(std::io::stdout)
        .init();
    let rt = tokio::runtime::Builder::new_current_thread().enable_all().build()?;
    rt.block_on(async {
        let t0 = std::time::Instant::now();
        let node = Node::open(&Options::default()).await?;
        node.create_bucket("demo").await?;
        let photo = noise(7 << 20);
        node.put("demo", "photo.jpg", photo.clone()).await?;
        let text = Bytes::from("pigeonhole keeps its bytes in chat messages. ".repeat(70_000));
        node.put("demo", "notes.md", text.clone()).await?;
        let part = node.get("demo", "photo.jpg", Some((5_000_000, 5_100_000))).await?;
        anyhow::ensure!(part == photo.slice(5_000_000..=5_100_000), "range read differs");
        anyhow::ensure!(node.get("demo", "notes.md", None).await? == text, "text read differs");
        node.copy("demo", "photo.jpg", "photo-copy.jpg").await?;
        node.delete("demo", "photo.jpg").await?;
        anyhow::ensure!(node.get("demo", "photo-copy.jpg", None).await? == photo, "copy differs");
        eprintln!("smoke ok in {:?}", t0.elapsed());
        Ok(())
    })
}

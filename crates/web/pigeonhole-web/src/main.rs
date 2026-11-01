//! Smoke run: the demo's config and stack, a few S3 calls, a restart on
//! another config; every event as a JSON line on stdout.
//!
//!     make web-smoke     # under Node's WASI
//!     cargo run -p pigeonhole-web

use bytes::Bytes;
use pigeonhole_web::{parse_config, Node, World, DEMO_CONFIG};

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
        .with_env_filter(
            "warn,pigeonhole_meta=debug,pigeonhole_blob=debug,pigeonhole_gateway_s3=info,\
             pigeonhole_storage_telegram=debug,pigeonhole_storage_discord=debug,pigeonhole_fake_channels=debug",
        )
        .with_writer(std::io::stdout)
        .init();
    let rt = tokio::runtime::Builder::new_current_thread().enable_all().build()?;
    rt.block_on(async {
        let t0 = std::time::Instant::now();
        // The demo keeps its channels in /data/channels: here in memory first,
        // then in a temp dir (WASI has no temp_dir(); the runner passes TMPDIR).
        let in_memory = DEMO_CONFIG
            .replace("fake = \"channel-data\"", "fake = \"memory\"")
            .replace("[[store]]\nid = \"channel-data\"\nkind = \"local_disk\"\npath = \"/data/channels\"\n", "");
        let tmp = std::env::var("TMPDIR").map_or_else(|_| std::env::temp_dir(), std::path::PathBuf::from);
        let dir = tmp.join(format!(
            "pigeonhole-web-smoke-{}",
            std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH)?.as_nanos()
        ));
        let on_disk = DEMO_CONFIG.replace("\"/data/channels\"", &format!("{:?}", dir.display().to_string()));
        let world = std::sync::Arc::new(World::default());
        let node = Node::open(&world, &parse_config(&in_memory)?).await?;
        node.create_bucket("demo").await?;
        let photo = noise(7 << 20);
        node.put("demo", "photo.jpg", photo.clone()).await?;
        let text = Bytes::from("pigeonhole keeps its bytes in chat messages. ".repeat(70_000));
        node.put("demo", "notes.md", text.clone()).await?;
        // Inline (in the log), a text message in both channels, Telegram text only.
        let tiny = Bytes::from_static(b"tiny");
        let small = noise(3000);
        let medium = noise(6000);
        node.put("demo", "tiny.txt", tiny.clone()).await?;
        node.put("demo", "small.bin", small.clone()).await?;
        node.put("demo", "medium.bin", medium.clone()).await?;
        let part = node.get("demo", "photo.jpg", Some((5_000_000, 5_100_000))).await?;
        anyhow::ensure!(part == photo.slice(5_000_000..=5_100_000), "range read differs");
        anyhow::ensure!(node.get("demo", "notes.md", None).await? == text, "text read differs");
        // The log inspector: the entries as they lie in the log's channel.
        let view = node.log_entries(None, 50).await?;
        let text = view.to_string();
        anyhow::ensure!(text.contains("photo.jpg") && text.contains("\"namespace\":\"chunk\""), "log view: {text}");
        if std::env::var("SMOKE_SHOW_LOG").is_ok() {
            eprintln!("{}", serde_json::to_string_pretty(&view)?);
        }
        // What the channels hold, as a page draws them after a reload.
        for c in &node.channels {
            let v = world.describe(c, 50).await?;
            anyhow::ensure!(v["messages"].as_array().is_some_and(|m| !m.is_empty()), "channel {}: empty", c.store);
            if std::env::var("SMOKE_SHOW_LOG").is_ok() {
                eprintln!("{} ({}): {}", c.store, c.role, v["messages"].as_array().map_or(0, Vec::len));
            }
        }
        node.copy("demo", "photo.jpg", "photo-copy.jpg").await?;
        node.delete("demo", "photo.jpg").await?;

        // A restart on another config: the log is replayed, the objects stay.
        node.close().await?;
        let cfg =
            in_memory.replace("codec = \"zstd\"", "codec = \"gzip\"").replace("size = \"4 MiB\"", "size = \"2 MiB\"");
        let node = Node::open(&world, &parse_config(&cfg)?).await?;
        anyhow::ensure!(node.replayed() > 0, "nothing replayed");
        anyhow::ensure!(node.get("demo", "photo-copy.jpg", None).await? == photo, "copy differs after restart");
        for (key, want) in [("tiny.txt", &tiny), ("small.bin", &small), ("medium.bin", &medium)] {
            anyhow::ensure!(node.get("demo", key, None).await? == *want, "{key} differs after restart");
        }
        node.put("demo", "after.bin", noise(5 << 20)).await?;
        eprintln!("memory: ok in {:?}, {} entries replayed", t0.elapsed(), node.replayed());

        // The channels in a local_disk store: a new process finds them as they were.
        let _ = std::fs::remove_dir_all(&dir);
        let t1 = std::time::Instant::now();
        {
            let world = std::sync::Arc::new(World::default());
            let node = Node::open(&world, &parse_config(&on_disk)?).await?;
            node.create_bucket("demo").await?;
            node.put("demo", "photo.jpg", photo.clone()).await?;
            node.put("demo", "small.bin", small.clone()).await?;
            node.close().await?;
            world.close().await?;
        }
        let world = std::sync::Arc::new(World::default());
        let node = Node::open(&world, &parse_config(&on_disk)?).await?;
        anyhow::ensure!(node.get("demo", "photo.jpg", None).await? == photo, "photo differs after a new process");
        anyhow::ensure!(node.get("demo", "small.bin", None).await? == small, "small differs after a new process");
        eprintln!("disk: ok in {:?}, {} entries replayed", t1.elapsed(), node.replayed());
        node.close().await?;
        world.close().await?;
        let _ = std::fs::remove_dir_all(&dir);
        eprintln!("smoke ok in {:?}", t0.elapsed());
        Ok(())
    })
}

//! Binary-only: `pigeonhole config …` and `pigeonhole run …`, clients of a
//! running process's admin API (`[admin] listen_addr`).

use anyhow::{bail, Context};
use pigeonhole::config::AdminSettings;

/// `config`: show; `config set KEY VALUE`; `config unset KEY`.
pub async fn cmd_config(args: &[String]) -> anyhow::Result<()> {
    let base = base_url()?;
    let client = reqwest::Client::new();
    match args {
        [] => show(&client, &base).await,
        [op, key, value] if op == "set" => {
            answer(client.put(format!("{base}/config/{key}")).body(value.clone()).send().await?).await?;
            println!("{key} = {value}");
            Ok(())
        }
        [op, key] if op == "unset" => {
            answer(client.delete(format!("{base}/config/{key}")).send().await?).await?;
            println!("{key}: back to the file's value");
            Ok(())
        }
        _ => bail!("usage: pigeonhole config [set KEY VALUE | unset KEY]"),
    }
}

/// `run TASK`: a pass of a background loop now.
pub async fn cmd_run(args: &[String]) -> anyhow::Result<()> {
    let [task] = args else {
        bail!("usage: pigeonhole run TASK (sweep, repair, cool, snapshot, metrics, cas_gc, upload_gc)")
    };
    let base = base_url()?;
    answer(reqwest::Client::new().post(format!("{base}/run/{task}")).send().await?).await?;
    println!("{task}: a pass is on its way");
    Ok(())
}

fn base_url() -> anyhow::Result<String> {
    let admin = AdminSettings::from_config_file()?;
    let addr = admin.listen_addr.context("[admin] listen_addr is not set: the process has no admin API")?;
    Ok(format!("http://{addr}/admin"))
}

/// The response, or its error text as the error.
async fn answer(resp: reqwest::Response) -> anyhow::Result<reqwest::Response> {
    if resp.status().is_success() {
        return Ok(resp);
    }
    let status = resp.status();
    bail!("{status}: {}", resp.text().await.unwrap_or_default().trim())
}

async fn show(client: &reqwest::Client, base: &str) -> anyhow::Result<()> {
    let view: serde_json::Value = answer(client.get(format!("{base}/config")).send().await?).await?.json().await?;
    let cell = |v: &serde_json::Value| v.as_str().unwrap_or("-").to_string();
    let mut rows = vec![["KEY".to_string(), "VALUE".into(), "FILE".into(), "OVERRIDE".into()]];
    for k in view["keys"].as_array().into_iter().flatten() {
        let mut overridden = cell(&k["override"]);
        if let Some(why) = k["refused"].as_str() {
            overridden = format!("{overridden} (refused: {why})");
        }
        rows.push([cell(&k["key"]), cell(&k["value"]), cell(&k["file"]), overridden]);
    }
    print_table(&rows);
    println!();
    let mut tasks = vec![["TASK".to_string(), "EVERY".into(), "NEXT IN".into()]];
    for t in view["tasks"].as_array().into_iter().flatten() {
        let next = t["next_in_ms"].as_u64().map_or_else(|| "-".into(), |ms| format!("{:.1}s", ms as f64 / 1000.0));
        tasks.push([cell(&t["task"]), cell(&t["every"]), next]);
    }
    print_table(&tasks);
    Ok(())
}

fn print_table<const N: usize>(rows: &[[String; N]]) {
    let widths: Vec<usize> = (0..N).map(|i| rows.iter().map(|r| r[i].len()).max().unwrap_or(0)).collect();
    for row in rows {
        let line: Vec<String> = row.iter().zip(&widths).map(|(c, w)| format!("{c:<w$}")).collect();
        println!("{}", line.join("  ").trim_end());
    }
}

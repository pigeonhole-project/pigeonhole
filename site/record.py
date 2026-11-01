#!/usr/bin/env python3
"""Record a scenario against a real pigeonhole into site/traces/<name>.json.

Starts the release binary on in-memory stores named `telegram` and `discord`
(a replica group: every chunk goes to both), with JSON logs at debug, runs the
scenario's AWS CLI commands, stops the server and keeps what the site plays
back: the commands and their output, every store call, every log batch and
commit, the S3 operations that changed something, and the log's entries
with their commands (the admin API's `log` op, as the demo reads it).

    cargo build --release -p pigeonhole
    python3 site/record.py              # every scenario
    python3 site/record.py hero         # one
"""

from __future__ import annotations

import argparse
import json
import os
import shlex
import socket
import subprocess
import sys
import tempfile
import time
import urllib.request
from datetime import datetime, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
TRACES = ROOT / "site" / "traces"
BIN = ROOT / "target" / "release" / "pigeonhole"

CONFIG = """\
listen_addr = "127.0.0.1:{port}"
region = "us-east-1"

[[store]]
id = "meta"
kind = "memory"

[[store]]
id = "telegram"
kind = "memory"

[[store]]
id = "discord"
kind = "memory"

[[replicas]]
id = "channels"
members = ["telegram", "discord"]
write_quorum = 2

[meta]
log = "meta"
snapshot = "meta"

[chunk]
store = "channels"
size = "{chunk}"
codec = "zstd"
block_size = "1 MiB"

[inline]
max_object_size = "off"

[cache]
enabled = {cache}

[admin]
listen_addr = "127.0.0.1:{admin}"

[log]
level = "warn,pigeonhole_meta=debug,pigeonhole_storage_memory=debug,pigeonhole_blob=debug,pigeonhole_gateway_s3=info,pigeonhole_chunk_store=info"
format = "json"
"""


def files(tmp: Path) -> None:
    """The files the scenarios upload: random bytes, and text that compresses."""
    (tmp / "photo.jpg").write_bytes(os.urandom(7 * 1024 * 1024))
    docs = "".join(p.read_text() for p in sorted((ROOT / "docs").glob("*.md")))
    text = (docs * (3 * 1024 * 1024 // len(docs) + 1))[: 3 * 1024 * 1024]
    (tmp / "notes.md").write_text(text)


# name → (config knobs, shell commands run in the temp dir)
SCENARIOS = {
    "hero": ({"chunk": "4 MiB", "cache": "true"}, [
        "aws s3 mb s3://demo",
        "aws s3 cp photo.jpg s3://demo/",
        "aws s3 cp notes.md s3://demo/",
        "aws s3 cp s3://demo/photo.jpg s3://demo/photo-copy.jpg",
        "aws s3 ls s3://demo/",
    ]),
}


def log_entries(admin: int) -> list[dict]:
    """The log's entries, decoded: the `log` op on the admin API, framed as
    the demo frames it (a JSON line, then the body: none)."""
    body = json.dumps({"op": "log", "limit": 200, "len": 0}).encode() + b"\n"
    req = urllib.request.Request(f"http://127.0.0.1:{admin}/admin/ops", data=body, method="POST")
    with urllib.request.urlopen(req, timeout=10) as r:
        reply = json.load(r)["reply"]
    if "error" in reply:
        raise SystemExit(f"log: {reply['error']}")
    return reply["entries"]


def free_port() -> int:
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


def now() -> float:
    return datetime.now(timezone.utc).timestamp()


def parse_ts(s: str) -> float:
    return datetime.fromisoformat(s.replace("Z", "+00:00")).timestamp()


def event(line: dict) -> dict | None:
    """One log line → one replay step, or None if the site does not show it."""
    f = line.get("fields", {})
    msg = f.get("message", "")
    spans = {s["name"]: s for s in line.get("spans", [])}
    store = spans.get("store")
    if msg == "store call" and store:
        return {"type": "blob", "store": store.get("instance"), "op": store.get("op"),
                "bytes": int(store["bytes"]) if "bytes" in store else None}
    if msg == "stored" and f.get("store") not in (None, "memory"):
        # The memory store names the message it made; joined to the store call below.
        return {"type": "post", "store": f["store"], "msg": f["message_id"], "name": f["name"], "bytes": f["bytes"]}
    if msg == "log batch linked":
        return {"type": "log", "first": f["first"], "last": f["last"]}
    if msg == "stored" and str(f.get("name", "")).startswith("log-batch"):
        return {"type": "logpost", "bytes": f["bytes"]}
    if msg == "commit" and "commit" in spans:
        c = spans["commit"]
        return {"type": "commit", "position": int(c["position"]), "entries": int(c["entries"])}
    if line.get("target", "").startswith("pigeonhole_gateway_s3") and msg.endswith(" ok"):
        return {"type": "s3", "op": msg[:-3], **{k: v for k, v in f.items() if k != "message"}}
    return None


def record(name: str) -> Path:
    knobs, commands = SCENARIOS[name]
    port, admin = free_port(), free_port()
    with tempfile.TemporaryDirectory(prefix="ph-rec-") as d:
        tmp = Path(d)
        files(tmp)
        (tmp / "pigeonhole.toml").write_text(CONFIG.format(port=port, admin=admin, **knobs))
        log = (tmp / "server.jsonl").open("w")
        env = {"HOME": os.environ["HOME"], "PATH": os.environ["PATH"],
               "PIGEONHOLE_CONFIG": str(tmp / "pigeonhole.toml")}
        server = subprocess.Popen([str(BIN)], cwd=tmp, env=env, stdout=subprocess.DEVNULL, stderr=log)
        try:
            for _ in range(100):
                try:
                    socket.create_connection(("127.0.0.1", port), timeout=0.1).close()
                    break
                except OSError:
                    time.sleep(0.05)
            else:
                raise SystemExit("pigeonhole did not start")
            aws_env = dict(os.environ, AWS_ACCESS_KEY_ID="pigeonhole", AWS_SECRET_ACCESS_KEY="pigeonholesecret",
                           AWS_DEFAULT_REGION="us-east-1", AWS_EC2_METADATA_DISABLED="true")
            steps = []
            for cmd in commands:
                argv = shlex.split(cmd)
                argv[1:1] = ["--endpoint-url", f"http://127.0.0.1:{port}"]
                t0 = now()
                out = subprocess.run(argv, cwd=tmp, env=aws_env, text=True, capture_output=True)
                if out.returncode:
                    raise SystemExit(f"{cmd}: {out.stderr}")
                steps.append({"t": t0, "type": "cmd", "text": cmd})
                # Drop aws's progress lines ("Completed 256.0 KiB/7.0 MiB …").
                text = "\n".join(l.split("\r")[-1].rstrip() for l in out.stdout.splitlines()
                                 if l.strip() and not l.startswith("Completed "))
                steps.append({"t": now(), "type": "out", "text": text})
                time.sleep(0.3)  # let commits that follow the answer land in this step
            entries = log_entries(admin)
        finally:
            server.terminate()
            server.wait()
            log.close()
        first = steps[0]["t"]
        for raw in (tmp / "server.jsonl").read_text().splitlines():
            try:
                line = json.loads(raw)
            except json.JSONDecodeError:
                continue
            t = parse_ts(line["timestamp"])
            if t < first or not (e := event(line)):
                continue
            steps.append({"t": t, **e})
    steps.sort(key=lambda s: (s["t"], s["type"] != "cmd"))
    steps = join_posts(steps)
    for s in steps:
        s["t"] = round((s["t"] - first) * 1000, 1)
    sha = subprocess.run(["git", "rev-parse", "--short", "HEAD"], cwd=ROOT, text=True, capture_output=True).stdout.strip()
    out = {"name": name, "recorded": datetime.now(timezone.utc).isoformat(timespec="seconds"),
           "commit": sha, "config": knobs, "steps": steps, "log": entries}
    TRACES.mkdir(parents=True, exist_ok=True)
    path = TRACES / f"{name}.json"
    path.write_text(json.dumps(out, indent=1, ensure_ascii=False) + "\n")
    return path


def join_posts(steps: list[dict]) -> list[dict]:
    """Fold each store's `stored` line into the store call it belongs to, and
    each log batch's post into its `log batch linked`."""
    out, pending = [], {}
    for s in steps:
        if s["type"] == "post":
            pending[s["store"]] = s
            continue
        if s["type"] == "blob" and s["op"] == "put" and s["store"] in pending:
            p = pending.pop(s["store"])
            s = {**s, "msg": p["msg"], "name": p["name"]}
        if s["type"] == "logpost":
            pending["log"] = s
            continue
        if s["type"] == "log" and "log" in pending:
            s = {**s, "bytes": pending.pop("log")["bytes"]}
        out.append(s)
    return out


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("names", nargs="*", default=list(SCENARIOS))
    args = ap.parse_args()
    if not BIN.exists():
        raise SystemExit(f"{BIN} missing: cargo build --release -p pigeonhole")
    for n in args.names:
        print(record(n))
    return 0


if __name__ == "__main__":
    sys.exit(main())

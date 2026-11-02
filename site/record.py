#!/usr/bin/env python3
"""Record a scenario against a real pigeonhole into site/traces/<name>.json.

Starts the release binary on fake channels (feature `fake`, as the site's
demo has them): a Telegram and a Discord channel for the chunks (a replica
group: every chunk goes to both), a Telegram channel for the metadata log and
one for its snapshots. JSON logs at debug; it runs the scenario's AWS CLI
commands, asks the admin API for the log's entries and the log's channel,
stops the server and keeps what the site plays back: the commands and their
output, every post (text or file) and store call, every log batch and commit,
the S3 operations that changed something, the entries with their commands
and the log channel's posts, decoded (the `log` and `channels` ops, as the
demo reads them).

    cargo build --release -p pigeonhole --features fake
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
id = "telegram"
kind = "telegram"
chat_id = -1001000000001
fake = "memory"

[[store]]
id = "discord"
kind = "discord"
channel_id = 1000000000000000001
fake = "memory"

[[store]]
id = "tg-log"
kind = "telegram"
chat_id = -1001000000002
fake = "memory"

[[store]]
id = "tg-snapshots"
kind = "telegram"
chat_id = -1001000000003
fake = "memory"

[[replicas]]
id = "channels"
members = ["telegram", "discord"]
write_quorum = 2

[meta]
log = "tg-log"
snapshot = "tg-snapshots"

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
level = "warn,pigeonhole_meta=debug,pigeonhole_storage_telegram=debug,pigeonhole_storage_discord=debug,pigeonhole_blob=debug,pigeonhole_gateway_s3=info,pigeonhole_chunk_store=info"
format = "json"

# Posts go out at once: the recording shows the code, not the services' pace.
[telegram.send]
rate_per_sec = 1000.0
burst = 1000.0

[discord.send]
rate_per_sec = 1000.0
burst = 1000.0
"""


def files(tmp: Path) -> None:
    """The files the scenarios upload: random bytes, text that compresses,
    and a small file."""
    (tmp / "photo.jpg").write_bytes(os.urandom(7 * 1024 * 1024))
    docs = "".join(p.read_text() for p in sorted((ROOT / "docs").glob("*.md")))
    text = (docs * (3 * 1024 * 1024 // len(docs) + 1))[: 3 * 1024 * 1024]
    (tmp / "notes.md").write_text(text)
    # Small enough for a text message: the channels get CJK text, not a file.
    settings = {"theme": "dark", "language": "en", "sync": {"every": "5m", "on_wifi_only": True},
                "recent": [f"photo-{i}.jpg" for i in range(40)]}
    (tmp / "settings.json").write_text(json.dumps(settings, indent=2))


# name → (config knobs, shell commands run in the temp dir)
SCENARIOS = {
    "hero": ({"chunk": "4 MiB", "cache": "true"}, [
        "aws s3 mb s3://demo",
        "aws s3 cp photo.jpg s3://demo/",
        "aws s3 cp notes.md s3://demo/",
        "aws s3 cp settings.json s3://demo/",
        "aws s3 cp s3://demo/photo.jpg s3://demo/photo-copy.jpg",
        "aws s3 ls s3://demo/",
    ]),
}


def admin_op(admin: int, request: dict) -> dict:
    """An op on the node's admin API, framed as the demo frames it (a JSON
    line, then the body: none); its reply."""
    body = json.dumps({**request, "len": 0}).encode() + b"\n"
    req = urllib.request.Request(f"http://127.0.0.1:{admin}/admin/ops", data=body, method="POST")
    with urllib.request.urlopen(req, timeout=10) as r:
        reply = json.load(r)["reply"]
    if "error" in reply:
        raise SystemExit(f"{request['op']}: {reply['error']}")
    return reply


def free_port() -> int:
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


def now() -> float:
    return datetime.now(timezone.utc).timestamp()


def parse_ts(s: str) -> float:
    return datetime.fromisoformat(s.replace("Z", "+00:00")).timestamp()


def events(lines: list[dict]) -> list[tuple[float, dict]]:
    """The log lines the site shows, as replay steps with their times. A
    fake channel's own tablet (span `fake_channel`) is not the node's; the
    text a fake channel got comes on the line before its post."""
    out, preview = [], None
    for line in lines:
        f = line.get("fields", {})
        msg = f.get("message", "")
        spans = {s["name"]: s for s in line.get("spans", [])}
        if "fake_channel" in spans:
            continue
        store = spans.get("store")
        step = None
        if msg == "fake channel: text":
            preview = f.get("preview")
        elif msg == "posted":
            # Discord's ids exceed a JS number: every id is a string.
            step = {"type": "post", "store": f["store"], "msg": str(f["message_id"]), "name": f["name"],
                    "bytes": int(f["bytes"]), "kind": f["kind"]}
            if f["kind"] == "text" and preview:
                step["preview"] = preview
            preview = None
        elif msg == "store call" and store:
            step = {"type": "blob", "store": store.get("instance"), "op": store.get("op"),
                    "bytes": int(store["bytes"]) if "bytes" in store else None}
        elif line.get("target", "").startswith("pigeonhole_gateway_s3") and msg.endswith(" ok"):
            step = {"type": "s3", "op": msg[:-3], **{k: v for k, v in f.items() if k != "message"}}
        if step:
            out.append((parse_ts(line["timestamp"]), step))
    return out


def join_posts(steps: list[dict], log_channel: dict, entries: list[dict]) -> list[dict]:
    """Fold each chunk's post into the store call that put it, and turn each
    post of the log's channel that holds a batch into a log step: which
    positions (the channel's posts, decoded), how many commands."""
    decoded = {str(m["id"]): m.get("log") or {} for m in log_channel["messages"]}
    commands = {e["position"]: len(e["commands"]) for e in entries}
    out, pending = [], {}
    for s in steps:
        if s["type"] == "post" and s["store"] == log_channel["store"]:
            log = decoded.get(s["msg"], {})
            if log.get("what") == "batch" and "first" in log:
                n = sum(commands.get(p, 0) for p in range(log["first"], log["last"] + 1))
                out.append({"t": s["t"], "type": "log", "first": log["first"], "last": log["last"],
                            "bytes": s["bytes"], "msg": s["msg"], "commands": n})
            continue
        if s["type"] == "post":
            pending[s["store"]] = s
            continue
        if s["type"] == "blob" and s["op"] == "put" and s["store"] in pending:
            p = pending.pop(s["store"])
            s = {**s, **{k: p[k] for k in ("msg", "name", "kind", "preview") if k in p}}
        out.append(s)
    return out


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
            entries = admin_op(admin, {"op": "log", "limit": 200})["entries"]
            # The log's channel as the demo shows it: its posts, each decoded.
            channels = admin_op(admin, {"op": "channels", "limit": 200})["channels"]
            log_channel = next(c for c in channels if c["role"] == "log")
        finally:
            server.terminate()
            server.wait()
            log.close()
        first = steps[0]["t"]
        lines = []
        for raw in (tmp / "server.jsonl").read_text().splitlines():
            try:
                lines.append(json.loads(raw))
            except json.JSONDecodeError:
                continue
        steps += [{"t": t, **e} for t, e in events(lines) if t >= first]
    steps.sort(key=lambda s: (s["t"], s["type"] != "cmd"))
    steps = join_posts(steps, log_channel, entries)
    for s in steps:
        s["t"] = round((s["t"] - first) * 1000, 1)
    sha = subprocess.run(["git", "rev-parse", "--short", "HEAD"], cwd=ROOT, text=True, capture_output=True).stdout.strip()
    out = {"name": name, "recorded": datetime.now(timezone.utc).isoformat(timespec="seconds"),
           "commit": sha, "config": knobs, "steps": steps, "log": entries, "log_channel": log_channel}
    TRACES.mkdir(parents=True, exist_ok=True)
    path = TRACES / f"{name}.json"
    path.write_text(json.dumps(out, indent=1, ensure_ascii=False) + "\n")
    return path


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

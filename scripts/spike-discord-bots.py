#!/usr/bin/env python3
"""Spike for docs/shared-limiter.md, Discord side: is a channel's message
limit per bot or per channel, and can one bot read and delete what another
posted (Message Content intent)?

    set -a; source .env; set +a; python3 scripts/spike-discord-bots.py

Needs DISCORD_BOT_TOKEN, DISCORD_BOT_TOKEN_2 and DISCORD_BLOBS_CHANNEL_ID.
SPIKE_SECS sets each phase's length (default 60). Each bot follows the
X-RateLimit headers it gets (no 429 on purpose); a 429 is printed with its
scope. Deletes only what it posted.
"""

import json
import os
import threading
import time
import urllib.error
import urllib.request
import uuid

API = "https://discord.com/api/v10"
UA = "DiscordBot (https://github.com/pigeonhole-project/pigeonhole, 0.1)"
CHANNEL = os.environ["DISCORD_BLOBS_CHANNEL_ID"]
SECS = int(os.environ.get("SPIKE_SECS", "60"))


def call(token, method, path, body=None, files=None):
    """(status, headers, json) of one request."""
    headers = {"Authorization": f"Bot {token}", "User-Agent": UA}
    data = None
    if files:
        boundary = uuid.uuid4().hex
        parts = [
            f'--{boundary}\r\nContent-Disposition: form-data; name="payload_json"\r\n'
            f"Content-Type: application/json\r\n\r\n{json.dumps(body or {})}\r\n".encode()
        ]
        for i, (name, blob) in enumerate(files):
            parts.append(
                f'--{boundary}\r\nContent-Disposition: form-data; name="files[{i}]"; filename="{name}"\r\n'
                f"Content-Type: application/octet-stream\r\n\r\n".encode() + blob + b"\r\n"
            )
        parts.append(f"--{boundary}--\r\n".encode())
        data = b"".join(parts)
        headers["Content-Type"] = f"multipart/form-data; boundary={boundary}"
    elif body is not None:
        data = json.dumps(body).encode()
        headers["Content-Type"] = "application/json"
    req = urllib.request.Request(API + path, data=data, method=method, headers=headers)
    try:
        with urllib.request.urlopen(req, timeout=30) as r:
            raw = r.read()
            return r.status, r.headers, json.loads(raw) if raw else None
    except urllib.error.HTTPError as e:
        raw = e.read()
        try:
            payload = json.loads(raw) if raw else None
        except ValueError:
            payload = {"raw": raw[:200].decode(errors="replace")}
        return e.code, e.headers, payload


def rl(h):
    return {k: h.get(f"X-RateLimit-{k}") for k in ("Limit", "Remaining", "Reset-After", "Bucket", "Scope", "Global")}


def hammer(token, label, out):
    """Post text messages for SECS, pacing on the headers."""
    sent, limits, throttled = [], set(), []
    end = time.time() + SECS
    n = 0
    while time.time() < end:
        status, h, body = call(token, "POST", f"/channels/{CHANNEL}/messages", {"content": f"spike {label} {n}"})
        n += 1
        if status == 429:
            wait = float((body or {}).get("retry_after", 1))
            throttled.append((round(wait, 2), (body or {}).get("global"), h.get("X-RateLimit-Scope")))
            print(f"{label}: 429 wait {wait} global={(body or {}).get('global')} scope={h.get('X-RateLimit-Scope')}")
            time.sleep(wait)
            continue
        if status != 200:
            raise SystemExit(f"{label}: POST {status} {body}")
        sent.append(body["id"])
        r = rl(h)
        limits.add((r["Limit"], r["Bucket"]))
        if r["Remaining"] == "0":
            time.sleep(float(r["Reset-After"] or 1))
    out[label] = {"sent": sent, "limits": limits, "throttled": throttled}


def report(phase, label, run):
    n = len(run["sent"])
    print(f"{phase} {label}: {n} posts in {SECS} s = {n * 60 / SECS:.1f}/min, "
          f"buckets {sorted(run['limits'])}, 429s {run['throttled']}")


def delete_all(token, ids):
    for i in range(0, len(ids), 100):
        part = ids[i:i + 100]
        if len(part) == 1:
            status, _, body = call(token, "DELETE", f"/channels/{CHANNEL}/messages/{part[0]}")
        else:
            status, _, body = call(token, "POST", f"/channels/{CHANNEL}/messages/bulk-delete", {"messages": part})
        if status == 429:
            time.sleep(float((body or {}).get("retry_after", 2)))
            status, _, body = call(token, "POST", f"/channels/{CHANNEL}/messages/bulk-delete", {"messages": part})
        if status not in (200, 204):
            print(f"delete: {status} {body}")
        time.sleep(1.5)


def main():
    a, b = os.environ["DISCORD_BOT_TOKEN"], os.environ["DISCORD_BOT_TOKEN_2"]
    ids = [call(t, "GET", "/users/@me")[2]["id"] for t in (a, b)]
    print(f"bots {ids[0]} and {ids[1]} in channel {CHANNEL}")

    # 1. Across bots: B reads A's text and attachment, then deletes them.
    _, _, text = call(a, "POST", f"/channels/{CHANNEL}/messages", {"content": "spike cross text"})
    _, _, doc = call(a, "POST", f"/channels/{CHANNEL}/messages", {}, files=[("spike-cross.bin", os.urandom(4096))])
    for m in (text, doc):
        status, _, seen = call(b, "GET", f"/channels/{CHANNEL}/messages/{m['id']}")
        print(f"B GET A's message: {status} content={seen.get('content')!r} attachments={len(seen.get('attachments', []))}")
    status, _, listed = call(b, "GET", f"/channels/{CHANNEL}/messages?limit=5")
    mine = [m for m in listed if m["id"] in (text["id"], doc["id"])]
    print(f"B lists A's messages: {[(m['content'], len(m['attachments'])) for m in mine]}")
    for m in (text, doc):
        status, _, body = call(b, "DELETE", f"/channels/{CHANNEL}/messages/{m['id']}")
        print(f"B DELETE A's message: {status} {body or ''}")
        time.sleep(1)

    # 2. One bot alone, then both at once.
    out = {}
    hammer(a, "A", out)
    report("alone", "A", out["A"])
    delete_all(a, out["A"]["sent"])
    time.sleep(10)

    out = {}
    threads = [threading.Thread(target=hammer, args=(t, label, out)) for t, label in ((a, "A"), (b, "B"))]
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    report("together", "A", out["A"])
    report("together", "B", out["B"])
    total = len(out["A"]["sent"]) + len(out["B"]["sent"])
    print(f"together: {total * 60 / SECS:.1f}/min for the channel")
    delete_all(a, out["A"]["sent"] + out["B"]["sent"])


if __name__ == "__main__":
    main()

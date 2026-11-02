#!/usr/bin/env python3
"""Spike for docs/proposals/albums.md, Discord side: several attachments
per message, their limits, Range reads, nonces, upload speed.

    set -a; source .env; set +a; python3 scripts/spike-discord-album.py [case ...]

Cases: small 11 5x20 10x20 over nonce par (all but par by default; PAR sets
par's width). Needs DISCORD_BOT_TOKEN and DISCORD_BLOBS_CHANNEL_ID. Deletes
only what it posted.
"""
import json, os, sys, threading, time, uuid, urllib.request, urllib.error

API = "https://discord.com/api/v10"
UA = "DiscordBot (https://github.com/pigeonhole-project/pigeonhole, 0.1)"
TOKEN = os.environ["DISCORD_BOT_TOKEN"]
CH = os.environ["DISCORD_BLOBS_CHANNEL_ID"]
MIB = 1024 * 1024
posted = []

def req(method, url, data=None, headers=None, timeout=900):
    h = {"User-Agent": UA}
    if url.startswith(API):
        h["Authorization"] = f"Bot {TOKEN}"
    h.update(headers or {})
    r = urllib.request.Request(url, data=data, method=method, headers=h)
    try:
        with urllib.request.urlopen(r, timeout=timeout) as resp:
            return resp.status, resp.headers, resp.read()
    except urllib.error.HTTPError as e:
        return e.code, e.headers, e.read()

def send(files, content, nonce=None):
    boundary = uuid.uuid4().hex
    body = {"content": content, "attachments": [{"id": i, "filename": n} for i, (n, _) in enumerate(files)]}
    if nonce:
        body["nonce"] = nonce; body["enforce_nonce"] = True
    parts = [f'--{boundary}\r\nContent-Disposition: form-data; name="payload_json"\r\nContent-Type: application/json\r\n\r\n{json.dumps(body)}\r\n'.encode()]
    for i, (n, blob) in enumerate(files):
        parts.append(f'--{boundary}\r\nContent-Disposition: form-data; name="files[{i}]"; filename="{n}"\r\nContent-Type: application/octet-stream\r\n\r\n'.encode() + blob + b"\r\n")
    parts.append(f"--{boundary}--\r\n".encode())
    data = b"".join(parts)
    t = time.time()
    st, h, raw = req("POST", f"{API}/channels/{CH}/messages", data, {"Content-Type": f"multipart/form-data; boundary={boundary}"})
    dt = time.time() - t
    try: js = json.loads(raw)
    except Exception: js = {"raw": raw[:300].decode(errors="replace")}
    rl = {k: h.get(f"X-RateLimit-{k}") for k in ("Limit", "Remaining", "Reset-After", "Bucket")} if h else {}
    if st == 200:
        posted.append(js["id"])
    return st, js, dt, len(data), rl

def files(n, size, tag):
    return [(f"ph-probe-{tag}-{i}.bin", os.urandom(size)) for i in range(n)]

def case(label, n, size, nonce=None):
    fs = files(n, size, label)
    st, js, dt, total, rl = send(fs, f"album-probe {label}", nonce)
    mb = total / MIB
    if st == 200:
        atts = js.get("attachments", [])
        print(f"[{label}] {n}x{size/MIB:.2f} MiB = {mb:.1f} MiB: 200 in {dt:.1f}s ({mb/dt:.2f} MiB/s), msg {js['id']}, {len(atts)} attachments, order ok={[a['filename'] for a in atts]==[f[0] for f in fs] or [a['filename'] for a in atts]}, rl={rl}")
    else:
        print(f"[{label}] {n}x{size/MIB:.2f} MiB = {mb:.1f} MiB: {st} in {dt:.1f}s: {js}")
    sys.stdout.flush()
    return st, js, fs

def ranged(js, fs):
    # Range read inside each attachment and its last byte; whole of one.
    for a, (n, blob) in zip(js["attachments"], fs):
        r0, r1 = len(blob) // 3, len(blob) // 3 + MIB
        st, h, raw = req("GET", a["url"], headers={"Range": f"bytes={r0}-{r1-1}"})
        ok = st == 206 and raw == blob[r0:r1]
        st2, h2, raw2 = req("GET", a["url"], headers={"Range": f"bytes={len(blob)-1}-"})
        ok2 = st2 == 206 and raw2 == blob[-1:]
        print(f"   range {a['filename']}: {st} ok={ok}, last byte {st2} ok={ok2}, size field {a['size']} == {len(blob)}: {a['size']==len(blob)}")
    t = time.time(); st, h, raw = req("GET", js["attachments"][0]["url"]); dt = time.time() - t
    print(f"   whole first attachment: {st}, equal={raw==fs[0][1]}, {len(raw)/MIB/dt:.1f} MiB/s")
    st, h, raw = req("GET", f"{API}/channels/{CH}/messages/{js['id']}")
    print(f"   GET message: {st}, attachments listed: {len(json.loads(raw).get('attachments', []))}")

try:
    which = sys.argv[1:] or ["small", "11", "5x20", "10x20", "over", "nonce"]
    if "small" in which:
        st, js, fs = case("small-10x1", 10, 1 * MIB)
        if st == 200: ranged(js, fs)
    if "11" in which:
        case("eleven-11x1k", 11, 1024)
    if "5x20" in which:
        st, js, fs = case("5x20", 5, 20 * MIB)
        if st == 200: ranged(js, fs)
    if "10x20" in which:
        st, js, fs = case("10x20", 10, 20 * MIB)
        if st == 200: ranged(js, fs)
    if "over" in which:
        case("one-over-20", 2, 20 * MIB + 1)
    if "nonce" in which:
        nonce = "albprobe" + uuid.uuid4().hex[:12]
        st, js, _ = case("nonce-a", 3, 2 * MIB, nonce)
        st2, js2, _ = case("nonce-b", 3, 2 * MIB, nonce)
        if st == 200 and st2 == 200:
            print(f"   nonce repeat returned same message: {js['id']==js2['id']}")
    if "par" in which:
        n = int(os.environ.get("PAR", "4"))
        res = []
        t = time.time()
        ts = [threading.Thread(target=lambda i=i: res.append(send(files(1, 20 * MIB, f"par{i}"), f"album-probe par{i}"))) for i in range(n)]
        [x.start() for x in ts]
        [x.join() for x in ts]
        dt = time.time() - t
        print(f"[par] {n} messages of 20 MiB at once: {[r[0] for r in res]}, {n * 20 / dt:.2f} MiB/s together in {dt:.1f}s")
finally:
    for mid in dict.fromkeys(posted):
        st, _, _ = req("DELETE", f"{API}/channels/{CH}/messages/{mid}")
        print(f"deleted {mid}: {st}")
        time.sleep(1)

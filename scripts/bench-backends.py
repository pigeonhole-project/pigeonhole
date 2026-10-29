#!/usr/bin/env python3
"""The same S3 workload against a running pigeonhole, timed from the client
and from its /metrics; and a Markdown comparison of two or more runs.

    uv run --with boto3 scripts/bench-backends.py run --label discord --out .cache/bench/discord.json
    uv run --with boto3 scripts/bench-backends.py report .cache/bench/discord.json .cache/bench/telegram.json

`scripts/bench.sh` starts the server per backend (scripts/metrics-demo.sh,
no cache, no demo traffic) and runs both. Phases are time-boxed, so a slow
backend does fewer operations rather than taking longer.
"""

import argparse
import json
import os
import re
import statistics
import sys
import threading
import time
import urllib.request

MIB = 1024 * 1024
BUCKET = "bench"


# --- /metrics --------------------------------------------------------------

LINE = re.compile(r'^([a-zA-Z_:][a-zA-Z0-9_:]*)(\{[^}]*\})?\s+(\S+)$')
LABEL = re.compile(r'(\w+)="((?:[^"\\]|\\.)*)"')


def scrape(url):
    """{(name, frozenset(labels)): value}, buckets left out."""
    out = {}
    for line in urllib.request.urlopen(url, timeout=10).read().decode().splitlines():
        m = LINE.match(line)
        if not m or line.startswith("#") or m.group(1).endswith("_bucket"):
            continue
        labels = frozenset(LABEL.findall(m.group(2) or ""))
        out[(m.group(1), labels)] = float(m.group(3))
    return out


def delta(before, after):
    return {k: v - before.get(k, 0.0) for k, v in after.items() if v - before.get(k, 0.0)}


def total(d, name, **match):
    return sum(v for (n, labels), v in d.items()
               if n == name and all((k, str(x)) in labels for k, x in match.items()))


def server_view(d):
    """What the server says about a phase (deltas of its counters)."""
    def mean(base, **m):
        c = total(d, base + "_count", **m)
        return total(d, base + "_sum", **m) / c if c else None

    view = {
        "proposals": total(d, "pigeonhole_meta_propose_seconds_count"),
        "log_entries": total(d, "pigeonhole_meta_proposals_per_entry_count"),
        "propose_mean_s": mean("pigeonhole_meta_propose_seconds"),
        "queue_wait_mean_s": mean("pigeonhole_meta_queue_wait_seconds"),
        "commit_mean_s": mean("pigeonhole_meta_commit_seconds"),
        "log_append_mean_s": mean("pigeonhole_meta_log_append_seconds"),
        "bytes_to_backend": total(d, "pigeonhole_bytes_to_backend_total"),
        "bytes_from_backend": total(d, "pigeonhole_bytes_from_backend_total"),
        "throttle_wait_s": total(d, "pigeonhole_backend_throttle_wait_seconds_sum"),
        "throttles": total(d, "pigeonhole_backend_throttle_wait_seconds_count"),
        "rate_limited_calls": total(d, "pigeonhole_instance_rate_limited_total"),
        "limiter_wait_s": total(d, "pigeonhole_limiter_wait_seconds_sum"),
        "calls": {},
    }
    if view["log_entries"]:
        view["proposals_per_entry"] = total(d, "pigeonhole_meta_proposals_per_entry_sum") / view["log_entries"]
    for (n, labels), v in d.items():
        if n == "pigeonhole_instance_call_duration_seconds_count":
            l = dict(labels)
            key = f'{l.get("instance")} {l.get("op")} {l.get("outcome")}'
            s = total(d, "pigeonhole_instance_call_duration_seconds_sum", **l)
            view["calls"][key] = {"count": v, "mean_s": s / v}
    return view


# --- workload --------------------------------------------------------------

def client(endpoint):
    import boto3
    from botocore.config import Config

    return boto3.client(
        "s3",
        endpoint_url=endpoint,
        aws_access_key_id=os.environ.get("AWS_ACCESS_KEY_ID", "pigeonhole"),
        aws_secret_access_key=os.environ.get("AWS_SECRET_ACCESS_KEY", "pigeonholesecret"),
        region_name="us-east-1",
        config=Config(read_timeout=600, connect_timeout=10, retries={"max_attempts": 1},
                      max_pool_connections=32, s3={"addressing_style": "path"}),
    )


class Phase:
    """Timed operations of one phase."""

    def __init__(self, name):
        self.name, self.lat, self.errors, self.bytes, self.lock = name, [], [], 0, threading.Lock()

    def op(self, f, nbytes=0):
        t0 = time.monotonic()
        try:
            f()
        except Exception as e:  # noqa: BLE001 - every failure is a result here
            with self.lock:
                self.errors.append(f"{type(e).__name__}: {e}"[:200])
            return False
        with self.lock:
            self.lat.append(time.monotonic() - t0)
            self.bytes += nbytes
        return True

    def summary(self, wall):
        lat = sorted(self.lat)
        q = lambda p: lat[min(len(lat) - 1, int(p * len(lat)))] if lat else None
        return {
            "ops": len(lat), "errors": len(self.errors), "error_samples": self.errors[:3],
            "wall_s": wall, "p50_s": q(0.5), "p95_s": q(0.95), "max_s": lat[-1] if lat else None,
            "mean_s": statistics.mean(lat) if lat else None,
            "ops_per_s": len(lat) / wall if wall else None,
            "mb_per_s": self.bytes / MIB / wall if wall and self.bytes else None,
        }



def run(args):
    s3 = client(args.endpoint)
    metrics_url = args.metrics
    try:
        s3.create_bucket(Bucket=BUCKET)
    except s3.exceptions.BucketAlreadyOwnedByYou:
        pass
    except Exception as e:  # noqa: BLE001
        if "BucketAlreadyExists" not in str(e):
            raise
    result = {"label": args.label, "started": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()), "phases": []}
    scale = args.minutes / 15.0

    def phase(name, desc, body, budget):
        print(f"== {name}: {desc} (up to {budget:.0f} s)", flush=True)
        p = Phase(name)
        before = scrape(metrics_url)
        t0 = time.monotonic()
        body(p, t0 + budget)
        wall = time.monotonic() - t0
        time.sleep(3)  # let the last gauges and background calls land
        after = scrape(metrics_url)
        r = {"name": name, "desc": desc, "client": p.summary(wall), "server": server_view(delta(before, after))}
        print(f"   {r['client']['ops']} ops, {r['client']['errors']} errors, p50 {fmt_s(r['client']['p50_s'])}, "
              f"p95 {fmt_s(r['client']['p95_s'])}", flush=True)
        result["phases"].append(r)

    def small_seq(p, deadline):
        data = os.urandom(4096)
        i = 0
        while time.monotonic() < deadline and i < 60:
            p.op(lambda i=i: s3.put_object(Bucket=BUCKET, Key=f"small/{i}", Body=data), len(data))
            i += 1

    sizes = [(1, 4), (8, 3), (32, 2)]

    def blobs(p, deadline):
        for mib, n in sizes:
            for j in range(n):
                if time.monotonic() >= deadline:
                    return
                data = os.urandom(mib * MIB)
                p.op(lambda: s3.put_object(Bucket=BUCKET, Key=f"blob/{mib}m-{j}", Body=data), len(data))

    def cold_get(p, deadline):
        for mib, n in sizes:
            for j in range(n):
                if time.monotonic() >= deadline:
                    return
                key = f"blob/{mib}m-{j}"
                p.op(lambda key=key: s3.get_object(Bucket=BUCKET, Key=key)["Body"].read(), mib * MIB)

    def ranged_get(p, deadline):
        for k in range(6):
            if time.monotonic() >= deadline:
                return
            off = (k * 5 + 3) * MIB
            p.op(lambda off=off: s3.get_object(Bucket=BUCKET, Key="blob/32m-0",
                                               Range=f"bytes={off}-{off + MIB - 1}")["Body"].read(), MIB)

    def small_parallel(p, deadline, workers=8):
        data = os.urandom(4096)

        def worker(w):
            i = 0
            while time.monotonic() < deadline:
                p.op(lambda i=i: s3.put_object(Bucket=BUCKET, Key=f"par/{w}/{i}", Body=data), len(data))
                i += 1

        ts = [threading.Thread(target=worker, args=(w,)) for w in range(workers)]
        for t in ts:
            t.start()
        for t in ts:
            t.join()

    def metadata(p, deadline):
        for i in range(50):
            if time.monotonic() >= deadline:
                return
            p.op(lambda i=i: s3.head_object(Bucket=BUCKET, Key=f"small/{i % 10}"))
            p.op(lambda: s3.list_objects_v2(Bucket=BUCKET, Prefix="blob/"))

    phase("small_put", "sequential 4 KiB PUTs: one blob + one log entry each", small_seq, 180 * scale)
    phase("blob_put", "PUT 4×1, 3×8, 2×32 MiB, incompressible, one at a time", blobs, 270 * scale)
    phase("cold_get", "GET those objects back whole (no cache)", cold_get, 150 * scale)
    phase("ranged_get", "six 1 MiB ranged GETs inside a 32 MiB object", ranged_get, 60 * scale)
    phase("small_put_x8", "4 KiB PUTs from 8 clients at once (group commit)", small_parallel, 180 * scale)
    phase("metadata", "HEAD + LIST, answered from the tablet's memory", metadata, 30 * scale)

    os.makedirs(os.path.dirname(os.path.abspath(args.out)), exist_ok=True)
    with open(args.out, "w") as f:
        json.dump(result, f, indent=1)
    print(f"wrote {args.out}")


# --- report ----------------------------------------------------------------

def fmt_s(v):
    if v is None:
        return "–"
    return f"{v * 1000:.0f} ms" if v < 1 else f"{v:.2f} s"


def fmt(v, digits=2):
    return "–" if v is None else f"{v:.{digits}f}"


def report(args):
    runs = [json.load(open(p)) for p in args.files]
    labels = [r["label"] for r in runs]
    names = [p["name"] for p in runs[0]["phases"]]
    by = lambda r, n: next((p for p in r["phases"] if p["name"] == n), None)
    out = []
    head = "| | " + " | ".join(labels) + " |\n|---|" + "---|" * len(labels)
    for n in names:
        ps = [by(r, n) for r in runs]
        out.append(f"### {n}: {ps[0]['desc']}\n\n{head}")
        rows = [
            ("ops (errors)", lambda p: f"{p['client']['ops']} ({p['client']['errors']})"),
            ("phase wall time", lambda p: fmt_s(p["client"]["wall_s"])),
            ("client p50 / p95", lambda p: f"{fmt_s(p['client']['p50_s'])} / {fmt_s(p['client']['p95_s'])}"),
            ("client max", lambda p: fmt_s(p["client"]["max_s"])),
            ("ops/s", lambda p: fmt(p["client"]["ops_per_s"])),
            ("MiB/s", lambda p: fmt(p["client"]["mb_per_s"])),
            ("log entries (proposals/entry)",
             lambda p: f"{p['server']['log_entries']:.0f} ({fmt(p['server'].get('proposals_per_entry'))})"),
            ("propose mean (queue wait)",
             lambda p: f"{fmt_s(p['server']['propose_mean_s'])} ({fmt_s(p['server']['queue_wait_mean_s'])})"),
            ("log append mean", lambda p: fmt_s(p["server"]["log_append_mean_s"])),
            ("backend calls", lambda p: "<br>".join(
                f"{k}: {c['count']:.0f} × {fmt_s(c['mean_s'])}" for k, c in sorted(p["server"]["calls"].items())) or "–"),
            ("our limiter waits", lambda p: fmt_s(p["server"]["limiter_wait_s"])),
            ("service throttles (wait)",
             lambda p: f"{p['server']['throttles']:.0f} ({fmt_s(p['server']['throttle_wait_s'])})"),
        ]
        for title, f in rows:
            out.append(f"| {title} | " + " | ".join(f(p) if p else "–" for p in ps) + " |")
        errs = [(r["label"], e) for r, p in zip(runs, ps) if p for e in p["client"]["error_samples"]]
        for label, e in errs:
            out.append(f"\n{label} error: `{e}`")
        out.append("")
    print("\n".join(out))


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    sub = ap.add_subparsers(dest="cmd", required=True)
    r = sub.add_parser("run")
    r.add_argument("--label", required=True)
    r.add_argument("--out", required=True)
    r.add_argument("--endpoint", default="http://127.0.0.1:8333")
    r.add_argument("--metrics", default="http://127.0.0.1:9100/metrics")
    r.add_argument("--minutes", type=float, default=15.0, help="scales every phase's time box")
    rep = sub.add_parser("report")
    rep.add_argument("files", nargs="+")
    a = ap.parse_args()
    run(a) if a.cmd == "run" else report(a)


if __name__ == "__main__":
    sys.exit(main())

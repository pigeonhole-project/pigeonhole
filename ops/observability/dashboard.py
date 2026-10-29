#!/usr/bin/env python3
"""Generate the pigeonhole Grafana dashboard (JSON on stdout).

    python3 ops/observability/dashboard.py > ops/observability/grafana/dashboards/pigeonhole.json

Metric names and labels: docs/metrics.md.
"""

import json
import sys

DS = {"type": "prometheus", "uid": "prom"}
R = "[$__rate_interval]"

panels = []
_y = 0
_id = 0


def _next_id():
    global _id
    _id += 1
    return _id


def row(title):
    global _y
    panels.append({"type": "row", "title": title, "id": _next_id(), "collapsed": False,
                   "gridPos": {"h": 1, "w": 24, "x": 0, "y": _y}})
    _y += 1


def place(items, h=8):
    global _y
    w = 24 // len(items)
    for i, p in enumerate(items):
        p["gridPos"] = {"h": h, "w": w, "x": i * w, "y": _y}
        panels.append(p)
    _y += h


def ts(title, targets, unit="short", stack=False, desc=""):
    return {
        "type": "timeseries", "title": title, "id": _next_id(), "datasource": DS, "description": desc,
        "targets": [{"refId": chr(65 + i), "expr": e, "legendFormat": l, "datasource": DS}
                    for i, (e, l) in enumerate(targets)],
        "fieldConfig": {"defaults": {"unit": unit, "custom": {
            "fillOpacity": 15, "lineWidth": 1, "showPoints": "never",
            "stacking": {"mode": "normal" if stack else "none"}}}, "overrides": []},
        "options": {"legend": {"displayMode": "list", "placement": "bottom"},
                    "tooltip": {"mode": "multi", "sort": "desc"}},
    }


def stat(title, expr, unit="short", steps=None, decimals=None):
    d = {"unit": unit, "thresholds": {"mode": "absolute",
                                      "steps": steps or [{"color": "green", "value": None}]}}
    if decimals is not None:
        d["decimals"] = decimals
    return {
        "type": "stat", "title": title, "id": _next_id(), "datasource": DS,
        "targets": [{"refId": "A", "expr": expr, "datasource": DS}],
        "fieldConfig": {"defaults": d, "overrides": []},
        "options": {"reduceOptions": {"calcs": ["lastNotNull"]}, "graphMode": "area",
                    "colorMode": "value", "textMode": "value"},
    }


def q(p, metric, by="", where=""):
    b = f", {by}" if by else ""
    return f"histogram_quantile({p}, sum by (le{b}) (rate({metric}_bucket{where}{R})))"


def ratio(num, den):
    return f"({num}) / ({den})"


def warn_at(orange, red):
    return [{"color": "green", "value": None}, {"color": "orange", "value": orange}, {"color": "red", "value": red}]


GW = "pigeonhole_gateway_request_duration_seconds"
CALL = "pigeonhole_instance_call_duration_seconds"
CACHE = "pigeonhole_cache_requests_total"

row("Overview")
place([
    stat("S3 requests/s", f"sum(rate({GW}_count{R}))", "reqps", decimals=1),
    stat("S3 errors", ratio(f'sum(rate({GW}_count{{outcome!="ok"}}{R}))', f"sum(rate({GW}_count{R}))"),
         "percentunit", warn_at(0.05, 0.2)),
    stat("S3 p99", q(0.99, GW), "s"),
    stat("Egress", f"sum(rate(pigeonhole_bytes_to_clients_total{R}))", "Bps"),
    stat("Ingress", f"sum(rate(pigeonhole_bytes_from_clients_total{R}))", "Bps"),
    stat("Objects", "pigeonhole_s3_objects"),
    stat("Object bytes", "pigeonhole_s3_object_bytes", "bytes"),
    stat("Integrity alarms",
         "sum(pigeonhole_chunk_gone_total or vector(0)) + sum(pigeonhole_double_release_total or vector(0))",
         "short", warn_at(1, 1)),
], 4)

row("S3 gateway")
place([
    ts("Requests/s by operation", [(f"sum by (op) (rate({GW}_count{R}))", "{{op}}")], "reqps", stack=True),
    ts("p99 by operation", [(q(0.99, GW, "op"), "{{op}}")], "s",
       desc="Time to response headers; GET bodies stream after it"),
    ts("Errors/s by operation", [(f'sum by (op, outcome) (rate({GW}_count{{outcome!="ok"}}{R}))', "{{op}} {{outcome}}")],
       "reqps", stack=True),
], 8)
place([
    ts("Latency (all)", [(q(p, GW), f"p{int(p * 100)}") for p in (0.5, 0.95, 0.99)], "s"),
    ts("Egress by operation", [(f"sum by (op) (rate(pigeonhole_bytes_to_clients_total{R}))", "{{op}}")], "Bps", stack=True),
    ts("Ingress by operation", [(f"sum by (op) (rate(pigeonhole_bytes_from_clients_total{R}))", "{{op}}")], "Bps", stack=True),
], 8)

row("Blob stores")
place([
    ts("Calls/s", [(f"sum by (instance, op, outcome) (rate({CALL}_count{R}))", "{{instance}} {{op}} {{outcome}}")], "ops"),
    ts("Call p99", [(q(0.99, CALL, "instance, op"), "{{instance}} {{op}}")], "s",
       desc="get: until the stream starts"),
    ts("In flight", [("sum by (instance, op) (pigeonhole_instance_inflight)", "{{instance}} {{op}}")]),
], 8)
place([
    ts("Bytes written", [(f"sum by (instance) (rate(pigeonhole_bytes_to_backend_total{R}))", "{{instance}}")], "Bps"),
    ts("Bytes read", [(f"sum by (instance) (rate(pigeonhole_bytes_from_backend_total{R}))", "{{instance}}")], "Bps"),
    ts("Stored bytes", [("pigeonhole_chunk_stored_bytes", "{{instance}}")], "bytes"),
], 8)

row("Block cache")
place([
    ts("Lookups/s", [(f"sum by (level, outcome) (rate({CACHE}{R}))", "{{level}} {{outcome}}")], "ops", stack=True),
    ts("Hit ratio", [(ratio(f'sum by (level) (rate({CACHE}{{outcome!="miss"}}{R}))', f"sum by (level) (rate({CACHE}{R}))"),
                      "{{level}}")], "percentunit", desc="hit + collapsed over all lookups"),
    ts("Size", [("pigeonhole_cache_bytes", "{{level}}")], "bytes"),
], 8)

row("Codec & ingest")
place([
    ts("Compression (stored / logical)", [
        (ratio(f"rate(pigeonhole_compression_ratio_sum{R})", f"rate(pigeonhole_compression_ratio_count{R})"), "mean"),
        (q(0.5, "pigeonhole_compression_ratio"), "p50")], "percentunit"),
    ts("Ingest budget wait", [(q(p, "pigeonhole_ingest_budget_wait_seconds"), f"p{int(p * 100)}") for p in (0.5, 0.99)],
       "s", desc="Only PUTs that had to wait"),
    ts("Parts per chunk", [(ratio(f"sum by (instance) (rate(pigeonhole_parts_per_chunk_sum{R}))",
                                  f"sum by (instance) (rate(pigeonhole_parts_per_chunk_count{R}))"), "{{instance}}")]),
], 8)

row("Metadata tablet")
place([
    ts("Entries, proposals, commands /s", [
        (f"sum(rate(pigeonhole_meta_commit_seconds_count{R}))", "entries"),
        (f"sum(rate(pigeonhole_meta_proposals_per_entry_sum{R}))", "proposals"),
        (f"sum(rate(pigeonhole_meta_commands_per_entry_sum{R}))", "commands")], "ops",
       desc="Group commit: proposals queued while one entry is written share the next"),
    ts("Propose p99, split", [
        (q(0.99, "pigeonhole_meta_propose_seconds"), "propose (caller)"),
        (q(0.99, "pigeonhole_meta_queue_wait_seconds"), "queue wait"),
        (q(0.99, "pigeonhole_meta_commit_seconds"), "commit (batcher)"),
        (q(0.99, "pigeonhole_meta_log_append_seconds"), "log append")], "s",
       desc="propose ≈ queue wait + commit; commit includes the log append and applying"),
    ts("Batcher queue", [
        ("pigeonhole_meta_queue_depth", "waiting"),
        (f"rate(pigeonhole_meta_batch_full_total{R})", "full entries/s")]),
], 8)
place([
    ts("Proposals per entry", [
        (ratio(f"rate(pigeonhole_meta_proposals_per_entry_sum{R})", f"rate(pigeonhole_meta_proposals_per_entry_count{R})"), "mean"),
        (q(0.99, "pigeonhole_meta_proposals_per_entry"), "p99")]),
    ts("Log written", [(f"rate(pigeonhole_meta_log_bytes_total{R})", "bytes/s")], "Bps"),
], 8)
place([
    ts("Entries since snapshot", [("pigeonhole_meta_applied_position - pigeonhole_meta_base_position", "log length")]),
    ts("Snapshot size", [("pigeonhole_meta_snapshot_bytes", "bytes")], "bytes"),
    stat("Last boot", "pigeonhole_meta_boot_seconds", "s"),
    stat("Replayed on boot", "pigeonhole_meta_boot_replayed_entries"),
    stat("Generation", "pigeonhole_meta_generation"),
    stat("Stopped", "pigeonhole_meta_stopped", "short", warn_at(1, 1)),
], 8)

row("Contents")
place([
    ts("Objects / uploads", [("pigeonhole_s3_objects", "objects"), ("pigeonhole_s3_uploads", "uploads in progress"),
                             ("pigeonhole_s3_buckets", "buckets")]),
    ts("Chunks by state", [("pigeonhole_chunks", "{{state}}")], stack=True),
    ts("Logical vs stored", [("pigeonhole_s3_object_bytes", "object bytes"),
                             ("pigeonhole_chunk_logical_bytes", "chunk logical"),
                             ("sum(pigeonhole_chunk_stored_bytes)", "stored, all replicas")], "bytes"),
], 8)

row("Background")
place([
    ts("Sweeper", [(f"sum by (outcome) (rate(pigeonhole_sweep_keys_total{R}))", "{{outcome}}")], "ops", stack=True),
    ts("Repair", [(f"sum by (outcome) (rate(pigeonhole_repair_jobs_total{R}))", "{{outcome}}")], "ops", stack=True),
    ts("Pass duration p99", [(q(0.99, "pigeonhole_background_pass_seconds", "task"), "{{task}}")], "s"),
    ts("Repair queue", [("pigeonhole_repair_queue_depth", "depth")]),
], 8)

row("Chat rate limits")
place([
    ts("Throttle waits asked (s/s)", [(f"sum by (service, method) (rate(pigeonhole_backend_throttle_wait_seconds_sum{R}))",
                                       "{{service}} {{method}}")], "s",
       desc="Seconds of 429 / FLOOD_WAIT per second, also the ones waited out inside a call"),
    ts("Calls failed rate limited", [(f"sum by (instance, op) (rate(pigeonhole_instance_rate_limited_total{R}))",
                                      "{{instance}} {{op}}")], "ops"),
    ts("Own limiter wait p99", [(q(0.99, "pigeonhole_limiter_wait_seconds", "op"), "{{op}}")], "s"),
], 8)

json.dump({
    "uid": "pigeonhole", "title": "pigeonhole", "tags": ["pigeonhole"], "schemaVersion": 39,
    "editable": True, "refresh": "5s", "time": {"from": "now-15m", "to": "now"}, "panels": panels,
}, sys.stdout, indent=1)
sys.stdout.write("\n")

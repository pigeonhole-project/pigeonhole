### small_put: sequential 4 KiB PUTs: one blob + one log entry each

| | disk | discord | telegram |
|---|---|---|---|
| ops (errors) | 60 (0) | 28 (0) | 55 (0) |
| phase wall time | 576 ms | 186.01 s | 180.17 s |
| client p50 / p95 | 9 ms / 11 ms | 6.66 s / 7.63 s | 3.33 s / 3.44 s |
| client max | 20 ms | 7.83 s | 3.58 s |
| ops/s | 104.25 | 0.15 | 0.31 |
| MiB/s | 0.41 | 0.00 | 0.00 |
| log entries (proposals/entry) | 60 (1.00) | 28 (1.00) | 55 (1.00) |
| propose mean (queue wait) | 4 ms (0 ms) | 6.07 s (0 ms) | 2.53 s (0 ms) |
| log append mean | 4 ms | 6.07 s | 2.53 s |
| backend calls | disk put ok: 60 × 4 ms | dc-blobs put ok: 28 × 563 ms | tg-blobs put ok: 55 × 745 ms |
| our limiter waits | 0 ms | 0 ms | 171.38 s |
| service throttles (wait) | 0 (0 ms) | 0 (0 ms) | 1 (3.00 s) |

### blob_put: PUT 4×1, 3×8, 2×32 MiB, incompressible, one at a time

| | disk | discord | telegram |
|---|---|---|---|
| ops (errors) | 9 (0) | 9 (0) | 9 (0) |
| phase wall time | 914 ms | 69.00 s | 33.56 s |
| client p50 / p95 | 48 ms / 180 ms | 6.67 s / 13.90 s | 3.31 s / 6.51 s |
| client max | 180 ms | 13.90 s | 6.51 s |
| ops/s | 9.84 | 0.13 | 0.27 |
| MiB/s | 100.61 | 1.33 | 2.74 |
| log entries (proposals/entry) | 9 (1.00) | 9 (1.00) | 9 (1.00) |
| propose mean (queue wait) | 4 ms (0 ms) | 3.78 s (0 ms) | 92 ms (0 ms) |
| log append mean | 4 ms | 3.78 s | 92 ms |
| backend calls | disk put ok: 26 × 5 ms | dc-blobs put ok: 11 × 3.09 s | tg-blobs put ok: 11 × 2.89 s |
| our limiter waits | 0 ms | 0 ms | 22.63 s |
| service throttles (wait) | 0 (0 ms) | 0 (0 ms) | 0 (0 ms) |

### cold_get: GET those objects back whole (no cache)

| | disk | discord | telegram |
|---|---|---|---|
| ops (errors) | 9 (0) | 9 (0) | 9 (0) |
| phase wall time | 248 ms | 14.05 s | 16.40 s |
| client p50 / p95 | 21 ms / 75 ms | 1.23 s / 4.44 s | 978 ms / 6.60 s |
| client max | 75 ms | 4.44 s | 6.60 s |
| ops/s | 36.29 | 0.64 | 0.55 |
| MiB/s | 370.98 | 6.55 | 5.61 |
| log entries (proposals/entry) | 0 (–) | 0 (–) | 0 (–) |
| propose mean (queue wait) | – (–) | – (–) | – (–) |
| log append mean | – | – | – |
| backend calls | disk get ok: 92 × 0 ms | dc-blobs get ok: 92 × 149 ms | tg-blobs get ok: 92 × 0 ms |
| our limiter waits | 0 ms | 0 ms | 0 ms |
| service throttles (wait) | 0 (0 ms) | 0 (0 ms) | 4 (6.00 s) |

### ranged_get: six 1 MiB ranged GETs inside a 32 MiB object

| | disk | discord | telegram |
|---|---|---|---|
| ops (errors) | 6 (0) | 6 (0) | 6 (0) |
| phase wall time | 14 ms | 665 ms | 692 ms |
| client p50 / p95 | 2 ms / 5 ms | 111 ms / 114 ms | 115 ms / 145 ms |
| client max | 5 ms | 114 ms | 145 ms |
| ops/s | 441.05 | 9.03 | 8.66 |
| MiB/s | 441.05 | 9.03 | 8.66 |
| log entries (proposals/entry) | 0 (–) | 0 (–) | 0 (–) |
| propose mean (queue wait) | – (–) | – (–) | – (–) |
| log append mean | – | – | – |
| backend calls | disk get ok: 6 × 0 ms | dc-blobs get ok: 6 × 109 ms | tg-blobs get ok: 6 × 0 ms |
| our limiter waits | 0 ms | 0 ms | 0 ms |
| service throttles (wait) | 0 (0 ms) | 0 (0 ms) | 0 (0 ms) |

### small_put_x8: 4 KiB PUTs from 8 clients at once (group commit)

| | disk | discord | telegram |
|---|---|---|---|
| ops (errors) | 23964 (0) | 56 (0) | 61 (0) |
| phase wall time | 180.03 s | 187.40 s | 203.58 s |
| client p50 / p95 | 61 ms / 70 ms | 13.09 s / 119.38 s | 23.48 s / 39.86 s |
| client max | 105 ms | 125.34 s | 46.81 s |
| ops/s | 133.11 | 0.30 | 0.30 |
| MiB/s | 0.52 | 0.00 | 0.00 |
| log entries (proposals/entry) | 21725 (1.10) | 21 (2.67) | 60 (1.02) |
| propose mean (queue wait) | 12 ms (4 ms) | 23.88 s (15.38 s) | 94 ms (1 ms) |
| log append mean | 8 ms | 3.84 s | 93 ms |
| backend calls | disk put ok: 23964 × 45 ms | dc-blobs put ok: 56 × 2.52 s | tg-blobs put ok: 61 × 25.07 s |
| our limiter waits | 0 ms | 80.51 s | 398.48 s |
| service throttles (wait) | 0 (0 ms) | 2 (600 ms) | 3 (13.00 s) |

### metadata: HEAD + LIST, answered from the tablet's memory

| | disk | discord | telegram |
|---|---|---|---|
| ops (errors) | 100 (0) | 100 (0) | 100 (0) |
| phase wall time | 72 ms | 73 ms | 84 ms |
| client p50 / p95 | 1 ms / 1 ms | 1 ms / 1 ms | 1 ms / 2 ms |
| client max | 3 ms | 3 ms | 5 ms |
| ops/s | 1382.09 | 1365.99 | 1194.33 |
| MiB/s | – | – | – |
| log entries (proposals/entry) | 0 (–) | 0 (–) | 0 (–) |
| propose mean (queue wait) | – (–) | – (–) | – (–) |
| log append mean | – | – | – |
| backend calls | – | – | – |
| our limiter waits | 0 ms | 0 ms | 0 ms |
| service throttles (wait) | 0 (0 ms) | 0 (0 ms) | 0 (0 ms) |


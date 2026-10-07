#!/usr/bin/env python3
"""Enforce workspace layering rules via `cargo metadata` JSON on stdin."""

from __future__ import annotations

import argparse
import json
import sys

# Map package name → role. Supports legacy s3gram-* and target pigeonhole-*.
ROLE = {
    "s3gram-core": "types",
    "pigeonhole-types": "types",
    "s3gram-chunk": "codec",
    "pigeonhole-codec": "codec",
    "s3gram-blob": "blob",
    "pigeonhole-blob": "blob",
    "s3gram-index": "blob-store",
    "s3gram-engine": "blob-store",
    "pigeonhole-index": "blob-store",
    "pigeonhole-engine": "blob-store",
    "pigeonhole-blob-store": "blob-store",
    # index remains a blob-store-layer crate until fully merged
    "s3gram-telegram": "storage",
    "s3gram-discord": "storage",
    "pigeonhole-storage-telegram": "storage",
    "pigeonhole-storage-discord": "storage",
    "pigeonhole-storage-memory": "storage",
    "s3gram-s3": "gateway",
    "s3gram-bytestream": "gateway",
    "pigeonhole-gateway-s3": "gateway",
    "pigeonhole-gateway-bytestream": "gateway",
    "pigeonhole-gateway-kafka": "gateway",
    "pigeonhole-gateway-webdav": "gateway",
    "s3gram": "bin",
    "pigeonhole": "bin",
    "pigeonhole-testkit": "testkit",
}

FORBIDDEN_SOFT = {
    "storage": {"blob-store", "gateway", "storage", "bin"},
    "gateway": {"storage", "gateway", "bin"},
}

BLOB_STORE_FORBIDDEN_CRATES = {"s3s", "tonic", "axum"}


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--strict",
        action="store_true",
        help="full pigeonhole target rules (fails on current engine→s3s)",
    )
    args = parser.parse_args()
    strict = args.strict

    meta = json.load(sys.stdin)
    workspace_members = set(meta["workspace_members"])
    packages = {p["id"]: p for p in meta["packages"] if p["id"] in workspace_members}
    by_name = {p["name"]: p for p in packages.values()}

    errors: list[str] = []

    for pkg in packages.values():
        name = pkg["name"]
        role = ROLE.get(name)
        if role is None:
            errors.append(
                f"{name}: unknown workspace package (add a role in check_deps.py)"
            )
            continue
        if role in ("bin", "testkit"):
            continue

        dep_names = []
        for dep in pkg.get("dependencies", []):
            if dep.get("kind") in ("dev", "build"):
                continue
            dep_name = dep["name"]
            if dep_name in by_name:
                dep_names.append(dep_name)

        if role == "storage":
            allowed = {"blob", "types"}
            for dep_name in dep_names:
                dr = ROLE.get(dep_name)
                if dr is None:
                    continue
                if strict and dr not in allowed:
                    errors.append(
                        f"{name} (storage) → {dep_name} ({dr}); "
                        f"allowed: {sorted(allowed)}"
                    )
                elif not strict and dr in FORBIDDEN_SOFT["storage"]:
                    errors.append(f"{name} (storage) → {dep_name} ({dr})")

        elif role == "gateway":
            if strict:
                allowed = {"blob-store", "codec", "types"}
                for dep_name in dep_names:
                    dr = ROLE.get(dep_name)
                    if dr is None:
                        continue
                    if dr not in allowed:
                        errors.append(
                            f"{name} (gateway) → {dep_name} ({dr}); "
                            f"allowed: {sorted(allowed)}"
                        )
                    # Gateways must not take a direct dependency on pigeonhole-index;
                    # S3/CAS indexes live in the gateway or behind blob-store APIs.
                    if dep_name in ("pigeonhole-index", "s3gram-index"):
                        errors.append(
                            f"{name} (gateway) must not depend on {dep_name} directly"
                        )
            else:
                for dep_name in dep_names:
                    dr = ROLE.get(dep_name)
                    if dr is None:
                        continue
                    if dr in FORBIDDEN_SOFT["gateway"]:
                        errors.append(f"{name} (gateway) → {dep_name} ({dr})")

        elif role == "blob-store" and strict:
            for dep_name in dep_names:
                dr = ROLE.get(dep_name)
                if dr in {"storage", "gateway", "bin", "testkit"}:
                    errors.append(f"{name} (blob-store) → {dep_name} ({dr})")
            for dep in pkg.get("dependencies", []):
                if dep.get("kind") in ("dev", "build"):
                    continue
                if dep["name"] in BLOB_STORE_FORBIDDEN_CRATES:
                    errors.append(
                        f"{name} (blob-store) must not depend on {dep['name']}"
                    )

        elif role in ("blob", "codec", "types") and strict:
            allowed = {
                "blob": {"types"},
                "codec": {"types"},
                "types": set(),
            }[role]
            for dep_name in dep_names:
                dr = ROLE.get(dep_name)
                if dr is None:
                    continue
                if dr not in allowed:
                    errors.append(
                        f"{name} ({role}) → {dep_name} ({dr}); "
                        f"allowed: {sorted(allowed) or 'crates.io only'}"
                    )

    mode = "strict" if strict else "soft"
    if errors:
        print(f"check-deps ({mode}): FAILED", file=sys.stderr)
        for err in sorted(set(errors)):
            print(f"  - {err}", file=sys.stderr)
        return 1

    print(f"check-deps ({mode}): ok ({len(packages)} workspace packages)")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

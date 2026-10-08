#!/usr/bin/env python3
"""Enforce workspace layering rules via `cargo metadata` JSON on stdin."""

from __future__ import annotations

import argparse
import json
import sys

# Map package name → role.
ROLE = {
    "pigeonhole-types": "types",
    "pigeonhole-codec": "codec",
    "pigeonhole-blob": "blob",
    "pigeonhole-chunk-store": "chunk-store",
    "pigeonhole-storage-telegram": "storage",
    "pigeonhole-storage-discord": "storage",
    "pigeonhole-storage-memory": "storage",
    "pigeonhole-gateway-s3": "gateway",
    "pigeonhole-gateway-bytestream": "gateway",
    "pigeonhole-gateway-kafka": "gateway",
    "pigeonhole-gateway-webdav": "gateway",
    "pigeonhole": "bin",
    "pigeonhole-testkit": "testkit",
}

FORBIDDEN_SOFT = {
    "storage": {"chunk-store", "gateway", "storage", "bin"},
    "gateway": {"storage", "gateway", "bin"},
}

CHUNK_STORE_FORBIDDEN_CRATES = {"s3s", "tonic", "axum"}


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--strict",
        action="store_true",
        help="full pigeonhole target rules",
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
                allowed = {"chunk-store", "codec", "types"}
                for dep_name in dep_names:
                    dr = ROLE.get(dep_name)
                    if dr is None:
                        continue
                    if dr not in allowed:
                        errors.append(
                            f"{name} (gateway) → {dep_name} ({dr}); "
                            f"allowed: {sorted(allowed)}"
                        )
            else:
                for dep_name in dep_names:
                    dr = ROLE.get(dep_name)
                    if dr is None:
                        continue
                    if dr in FORBIDDEN_SOFT["gateway"]:
                        errors.append(f"{name} (gateway) → {dep_name} ({dr})")

        elif role == "chunk-store" and strict:
            for dep_name in dep_names:
                dr = ROLE.get(dep_name)
                if dr in {"storage", "gateway", "bin", "testkit"}:
                    errors.append(f"{name} (chunk-store) → {dep_name} ({dr})")
            for dep in pkg.get("dependencies", []):
                if dep.get("kind") in ("dev", "build"):
                    continue
                if dep["name"] in CHUNK_STORE_FORBIDDEN_CRATES:
                    errors.append(
                        f"{name} (chunk-store) must not depend on {dep['name']}"
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

    if strict:
        errors.extend(scan_forbidden_lexemes())

    mode = "strict" if strict else "soft"
    if errors:
        print(f"check-deps ({mode}): FAILED", file=sys.stderr)
        for err in sorted(set(errors)):
            print(f"  - {err}", file=sys.stderr)
        return 1

    print(f"check-deps ({mode}): ok ({len(packages)} workspace packages)")
    return 0


# Backend-specific identifiers must stay inside storage-* (and bin/config).
FORBIDDEN_LEXEMES = ("file_id", "chat_id", "channel_id")
LEXEME_ALLOW_DIR_PREFIXES = (
    "crates/storage/",
    "crates/bin/",
    "scripts/",
    "docs/",
    "compat/",
    "tests/",
)
# Legacy config / migrate bridges still parse old TOML keys and s3gram columns.
LEXEME_ALLOW_FILES = {
    "crates/blob/pigeonhole-chunk-store/src/config.rs",
    "crates/blob/pigeonhole-chunk-store/src/instances.rs",
    "crates/blob/pigeonhole-chunk-store/src/migrate.rs",
    # Only remaining mentions are ALTER RENAME of the legacy column + serde alias.
    "crates/gateway/pigeonhole-gateway-s3/src/index.rs",
}


def scan_forbidden_lexemes() -> list[str]:
    """Grep workspace Rust (non-storage) for Telegram/Discord field names."""
    import os
    import re
    from pathlib import Path

    root = Path(__file__).resolve().parents[1]
    crates = root / "crates"
    found: list[str] = []
    # Word-ish boundaries so we don't match e.g. unmatched_chat_identity in comments poorly;
    # keep it simple: substring match on token-ish patterns.
    patterns = {lex: re.compile(rf"\b{re.escape(lex)}\b") for lex in FORBIDDEN_LEXEMES}

    for path in crates.rglob("*.rs"):
        rel = path.relative_to(root).as_posix()
        if any(rel.startswith(p) for p in LEXEME_ALLOW_DIR_PREFIXES):
            continue
        if rel in LEXEME_ALLOW_FILES:
            continue
        if "/target/" in rel:
            continue
        try:
            text = path.read_text(encoding="utf-8")
        except OSError:
            continue
        for lex, rx in patterns.items():
            if rx.search(text):
                found.append(f"{rel}: forbidden lexeme `{lex}` outside storage-*/bin")
                break
    return found


if __name__ == "__main__":
    raise SystemExit(main())

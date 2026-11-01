#!/usr/bin/env python3
"""Build the project site into target/site (English at /, Russian at /ru/).

Pages are templates in site/src with placeholders:

    {{key}}           a string from site/i18n/<lang>.json (may hold HTML)
    {{@root}}         the path to the site root from this page ("" or "../")
    {{@lang}}         en | ru
    {{@stat:name}}    a number counted from the repository at build time
    {{@trace:name}}   site/traces/<name>.json, as a JS object literal
    {{@js}}           the i18n strings whose keys start with "js.", as JSON

    python3 site/build.py
    python3 site/build.py -o /tmp/site
"""

from __future__ import annotations

import argparse
import hashlib
import json
import re
import shutil
import subprocess
import sys
from pathlib import Path

SITE = Path(__file__).resolve().parent
ROOT = SITE.parent
LANGS = {"en": "", "ru": "ru/"}


def stats() -> dict[str, int]:
    sys.path.insert(0, str(ROOT / "scripts"))
    import code_report as cr

    rust = tests = 0
    crates = cr.workspace()
    for c in crates:
        for f in c.files:
            rust += sum(cr.scan(f).code)
            tests += len(re.findall(r"#\[(?:tokio::|madsim::)?test\b", f.read_text(errors="replace")))
    git = lambda *a: subprocess.run(["git", *a], cwd=ROOT, text=True, capture_output=True, check=True).stdout
    dates = set(git("log", "--format=%ad", "--date=short").split())
    return {
        "commits": int(git("rev-list", "--count", "HEAD")),
        "days": len(dates),
        "rust": rust,
        "crates": len(crates),
        "tests": tests,
        "docs": len(list((ROOT / "docs").glob("*.md"))),
    }


def fmt_stat(n: int, lang: str) -> str:
    s = f"{n:,}"
    return s.replace(",", " " if lang == "ru" else ",")


ASSETS_VERSION = ""


def assets_version() -> str:
    """A hash of every asset and the wasm, for the assets' URLs."""
    h = hashlib.sha256()
    wasm = ROOT / "target" / "wasm32-wasip1" / "release" / "pigeonhole_web.wasm"
    for f in sorted((SITE / "src" / "assets").iterdir()) + [wasm]:
        if f.is_file():
            h.update(f.name.encode() + f.read_bytes())
    return h.hexdigest()[:10]


def render(tpl: str, lang: str, prefix: str, strings: dict, st: dict) -> str:
    root = "../" * prefix.count("/")

    def sub(m: re.Match) -> str:
        key = m.group(1)
        if key == "@root":
            return root
        if key == "@lang":
            return lang
        if key == "@js":
            return json.dumps({k[3:]: v for k, v in strings.items() if k.startswith("js.")}, ensure_ascii=False)
        if key.startswith("@stat:"):
            return fmt_stat(st[key[6:]], lang)
        if key.startswith("@trace:"):
            return (SITE / "traces" / f"{key[7:]}.json").read_text().strip()
        if key not in strings:
            raise SystemExit(f"{lang}: no string {key!r}")
        return strings[key]

    html = re.sub(r"\{\{\s*([@\w.:-]+)\s*\}\}", sub, tpl)
    # Scripts and styles carry the assets' version: a browser never mixes a
    # cached old worker with a new page.
    return re.sub(r'(assets/[\w.-]+\.(?:js|css))"', rf'\1?v={ASSETS_VERSION}"', html)


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("-o", "--out", type=Path, default=ROOT / "target" / "site")
    args = ap.parse_args()

    global ASSETS_VERSION
    ASSETS_VERSION = assets_version()
    st = stats()
    if args.out.exists():
        shutil.rmtree(args.out)
    shutil.copytree(SITE / "src" / "assets", args.out / "assets")
    wasm = ROOT / "target" / "wasm32-wasip1" / "release" / "pigeonhole_web.wasm"
    if wasm.exists():
        shutil.copy(wasm, args.out / "assets" / "pigeonhole.wasm")
    else:
        print(f"warning: {wasm} missing (make web): try.html will not load", file=sys.stderr)
    pages = sorted((SITE / "src").glob("*.html"))
    for lang, prefix in LANGS.items():
        strings = json.loads((SITE / "i18n" / f"{lang}.json").read_text())
        for page in pages:
            out = args.out / prefix / page.name
            out.parent.mkdir(parents=True, exist_ok=True)
            out.write_text(render(page.read_text(), lang, prefix, strings, st))
            print(out)
    return 0


if __name__ == "__main__":
    sys.exit(main())

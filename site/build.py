#!/usr/bin/env python3
"""Build the project site into target/site (English at /, Russian at /ru/).

Pages are templates in site/src with placeholders:

    {{key}}           a string from site/i18n/<lang>.json (may hold HTML)
    {{@root}}         the path to the site root from this page ("" or "../")
    {{@lang}}         en | ru
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
import sys
from pathlib import Path

SITE = Path(__file__).resolve().parent
ROOT = SITE.parent
LANGS = {"en": "", "ru": "ru/"}


ASSETS_VERSION = ""


def assets_version() -> str:
    """A hash of every asset and the wasm, for the assets' URLs."""
    h = hashlib.sha256()
    wasm = ROOT / "target" / "wasm32-wasip1" / "web" / "pigeonhole_web.wasm"
    for f in sorted((SITE / "src" / "assets").rglob("*")) + [wasm]:
        if f.is_file():
            name = f.name if f == wasm else f.relative_to(SITE).as_posix()
            h.update(name.encode() + f.read_bytes())
    return h.hexdigest()[:10]


IMPORT = re.compile(r'(?:from|import)\s*"(\.{1,2}/[\w./-]+\.js)"')


def module_graph(entry: str) -> list[str]:
    """Every module `entry` (a path under assets/) imports, at any depth."""
    seen, todo = [], [entry]
    while todo:
        name = todo.pop()
        if name in seen:
            continue
        seen.append(name)
        here = (SITE / "src" / "assets" / name).parent
        for dep in IMPORT.findall((SITE / "src" / "assets" / name).read_text()):
            todo.append((here / dep).resolve().relative_to(SITE / "src" / "assets").as_posix())
    return seen[1:]


def preload_modules(html: str, root: str) -> str:
    """The modules the page's scripts import, as modulepreload links in its
    head: the browser fetches them at once, not one level of imports after
    another."""
    entries = re.findall(r'<script type="module"[^>]* src="[./]*assets/([\w./-]+\.js)"', html)
    deps = [d for e in entries for d in module_graph(e) if d not in entries]
    links = "".join(f'<link rel="modulepreload" href="{root}assets/{d}">\n' for d in dict.fromkeys(deps))
    return html.replace("</head>", links + "</head>", 1)


def render(tpl: str, lang: str, prefix: str, strings: dict) -> str:
    root = "../" * prefix.count("/")

    def sub(m: re.Match) -> str:
        key = m.group(1)
        if key == "@root":
            return root
        if key == "@lang":
            return lang
        if key == "@js":
            return json.dumps({k[3:]: v for k, v in strings.items() if k.startswith("js.")}, ensure_ascii=False)
        if key.startswith("@trace:"):
            return (SITE / "traces" / f"{key[7:]}.json").read_text().strip()
        if key not in strings:
            raise SystemExit(f"{lang}: no string {key!r}")
        return strings[key]

    html = preload_modules(re.sub(r"\{\{\s*([@\w.:-]+)\s*\}\}", sub, tpl), root)
    # Scripts and styles carry the assets' version: a browser never mixes a
    # cached old worker with a new page.
    return re.sub(r'(assets/[\w./-]+\.(?:js|css))"', rf'\1?v={ASSETS_VERSION}"', html)


def version_imports(assets: Path) -> None:
    """The modules' imports of each other carry the assets' version too (a
    module's URL is its cache key: Safari keeps old ones hard)."""
    for f in assets.rglob("*.js"):
        js = f.read_text()
        f.write_text(IMPORT.sub(lambda m: m.group(0)[:-1] + f'?v={ASSETS_VERSION}"', js))


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("-o", "--out", type=Path, default=ROOT / "target" / "site")
    args = ap.parse_args()

    global ASSETS_VERSION
    ASSETS_VERSION = assets_version()
    if args.out.exists():
        shutil.rmtree(args.out)
    shutil.copytree(SITE / "src" / "assets", args.out / "assets")
    version_imports(args.out / "assets")
    wasm = ROOT / "target" / "wasm32-wasip1" / "web" / "pigeonhole_web.wasm"
    if wasm.exists():
        shutil.copy(wasm, args.out / "assets" / "pigeonhole.wasm")
    else:
        print(f"warning: {wasm} missing (make web): demo.html will not load", file=sys.stderr)
    pages = sorted((SITE / "src").glob("*.html"))
    for lang, prefix in LANGS.items():
        strings = json.loads((SITE / "i18n" / f"{lang}.json").read_text())
        for page in pages:
            out = args.out / prefix / page.name
            out.parent.mkdir(parents=True, exist_ok=True)
            out.write_text(render(page.read_text(), lang, prefix, strings))
            print(out)
    return 0


if __name__ == "__main__":
    sys.exit(main())

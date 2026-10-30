#!/usr/bin/env python3
"""Workspace code report: size, function complexity, trait graph → Markdown.

Size is counted here (code / comment / blank lines; test code split out:
`tests/`, `benches/`, the testkit crate and `#[cfg(test)] mod` blocks).
Complexity comes from `rust-code-analysis-cli` (cognitive + cyclomatic per fn).
The trait graph comes from rustdoc JSON, built on the stable toolchain with
RUSTC_BOOTSTRAP=1, and is rendered as a Mermaid classDiagram.
"""

from __future__ import annotations

import argparse
import json
import os
import re
import subprocess
import sys
from collections import defaultdict
from dataclasses import dataclass, field
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
RUSTDOC_TARGET = ROOT / "target" / "rustdoc-json"

# Crates whose every line counts as test code.
TEST_CRATES = {"pigeonhole-testkit"}
TEST_DIRS = {"tests", "benches"}


def run(cmd: list[str], **kw) -> str:
    return subprocess.run(cmd, check=True, text=True, stdout=subprocess.PIPE, **kw).stdout


# ---------------------------------------------------------------- workspace


@dataclass
class Crate:
    name: str
    dir: Path
    has_lib: bool
    files: list[Path] = field(default_factory=list)


def workspace() -> list[Crate]:
    meta = json.loads(run(["cargo", "metadata", "--format-version", "1", "--no-deps"], cwd=ROOT))
    members = set(meta["workspace_members"])
    crates = []
    for p in meta["packages"]:
        if p["id"] not in members:
            continue
        d = Path(p["manifest_path"]).parent
        has_lib = any("lib" in t["kind"] for t in p["targets"])
        c = Crate(p["name"], d, has_lib)
        c.files = sorted(f for f in d.rglob("*.rs") if "target" not in f.relative_to(d).parts)
        crates.append(c)
    return sorted(crates, key=lambda c: c.name)


# ---------------------------------------------------------------- source scan

RAW_STR = re.compile(r'[bc]?r(#*)"')
CHAR_LIT = re.compile(r"'(?:\\(?:x[0-9a-fA-F]{2}|u\{[0-9a-fA-F]+\}|.)|[^\\'\n])'")
TEST_MOD = re.compile(
    r"#\s*\[\s*cfg\s*\(\s*test\s*\)\s*\]\s*(?:#\s*\[[^\]]*\]\s*)*"
    r"(?:pub(?:\s*\([^)]*\))?\s+)?mod\s+(\w+)\s*([{;])"
)


def strip(text: str) -> tuple[str, str]:
    """Return (text without comments, text without comments and literals).

    Newlines are kept, so offsets and line numbers match the original.
    """
    no_comments = list(text)
    no_literals = list(text)
    n = len(text)

    def blank(buf: list[str], a: int, b: int) -> None:
        for k in range(a, b):
            if buf[k] != "\n":
                buf[k] = " "

    i = 0
    while i < n:
        c = text[i]
        prev_ident = i > 0 and (text[i - 1].isalnum() or text[i - 1] == "_")
        if text.startswith("//", i):
            j = text.find("\n", i)
            j = n if j < 0 else j
            blank(no_comments, i, j)
            blank(no_literals, i, j)
            i = j
        elif text.startswith("/*", i):
            depth, j = 1, i + 2
            while j < n and depth:
                if text.startswith("/*", j):
                    depth, j = depth + 1, j + 2
                elif text.startswith("*/", j):
                    depth, j = depth - 1, j + 2
                else:
                    j += 1
            blank(no_comments, i, j)
            blank(no_literals, i, j)
            i = j
        elif c in "bcr" and not prev_ident and (m := RAW_STR.match(text, i)):
            end = text.find('"' + m.group(1), m.end())
            j = n if end < 0 else end + 1 + len(m.group(1))
            blank(no_literals, m.end(), j - 1 - len(m.group(1)))
            i = j
        elif c == '"':
            j = i + 1
            while j < n and text[j] != '"':
                j += 2 if text[j] == "\\" else 1
            blank(no_literals, i + 1, j)
            i = j + 1
        elif c == "'" and (m := CHAR_LIT.match(text, i)):
            blank(no_literals, i + 1, m.end() - 1)
            i = m.end()
        else:
            i += 1
    return "".join(no_comments), "".join(no_literals)


@dataclass
class FileScan:
    code: list[bool]      # per line (1-based index - 1): has code
    comment: list[bool]   # per line: comment only
    test_ranges: list[tuple[int, int]]  # 1-based inclusive line ranges
    test_mod_files: list[str]  # `#[cfg(test)] mod x;` names


def scan(path: Path) -> FileScan:
    text = path.read_text(encoding="utf-8", errors="replace")
    no_comments, no_literals = strip(text)
    orig_lines = text.split("\n")
    code_lines = no_comments.split("\n")
    code = [bool(l.strip()) for l in code_lines]
    comment = [bool(o.strip()) and not c for o, c in zip(orig_lines, code)]

    ranges, mod_files = [], []
    for m in TEST_MOD.finditer(no_literals):
        start = no_literals.count("\n", 0, m.start()) + 1
        if m.group(2) == ";":
            mod_files.append(m.group(1))
            continue
        depth, j = 1, m.end()
        while j < len(no_literals) and depth:
            depth += {"{": 1, "}": -1}.get(no_literals[j], 0)
            j += 1
        ranges.append((start, no_literals.count("\n", 0, j) + 1))
    return FileScan(code, comment, ranges, mod_files)


def child_mod_paths(path: Path, name: str) -> list[Path]:
    base = path.parent if path.name in ("lib.rs", "main.rs", "mod.rs") else path.with_suffix("")
    return [base / f"{name}.rs", base / name / "mod.rs"]


# ---------------------------------------------------------------- complexity


@dataclass
class Fn:
    crate: str
    file: str
    name: str
    line: int
    lines: int
    cognitive: int
    cyclomatic: int
    test: bool


def rca_units(paths: list[Path]) -> dict[str, dict]:
    """Run rust-code-analysis-cli; map relative file path → unit metrics tree."""
    cmd = ["rust-code-analysis-cli", "-m", "-O", "json", "-I", "*.rs"]
    for p in paths:
        cmd += ["-p", str(p.relative_to(ROOT))]
    out = run(cmd, cwd=ROOT)
    dec, i, units = json.JSONDecoder(), 0, {}
    while True:
        while i < len(out) and out[i].isspace():
            i += 1
        if i >= len(out):
            break
        obj, i = dec.raw_decode(out, i)
        units[obj["name"]] = obj
    return units


def functions(unit: dict, crate: str, file: str, is_test) -> list[Fn]:
    out = []

    def walk(node: dict, owner: str | None) -> None:
        for s in node["spaces"]:
            if s["kind"] == "function":
                m = s["metrics"]
                name = f"{owner}::{s['name']}" if owner else s["name"]
                out.append(Fn(
                    crate, file, name, s["start_line"], int(m["loc"]["sloc"]),
                    int(m["cognitive"]["sum"]), int(m["cyclomatic"]["sum"]),
                    is_test(s["start_line"]),
                ))
                walk(s, owner)
            else:
                walk(s, s["name"] if s["kind"] in ("impl", "trait") else owner)

    walk(unit, None)
    return out


# ---------------------------------------------------------------- traits


def build_rustdoc(crates: list[Crate]) -> None:
    env = dict(os.environ, RUSTC_BOOTSTRAP="1", RUSTFLAGS="-A warnings")
    for c in crates:
        if not c.has_lib:
            continue
        print(f"rustdoc json: {c.name}", file=sys.stderr)
        subprocess.run(
            ["cargo", "rustdoc", "-q", "-p", c.name, "--lib",
             "--target-dir", str(RUSTDOC_TARGET), "--",
             "-Z", "unstable-options", "--output-format", "json",
             "--document-private-items", "-A", "warnings"],
            check=True, cwd=ROOT, env=env,
        )


@dataclass
class TraitGraph:
    traits: set[tuple[str, str]] = field(default_factory=set)
    types: set[tuple[str, str]] = field(default_factory=set)
    supers: set[tuple[tuple[str, str], tuple[str, str]]] = field(default_factory=set)
    impls: set[tuple[tuple[str, str], tuple[str, str], bool]] = field(default_factory=set)  # (type, trait, blanket)


def type_key(t: dict, paths: dict, crate: str) -> tuple[str, str] | None:
    """Node key (owning crate, display name) for an impl's `for` type."""
    if "resolved_path" in t:
        rp = t["resolved_path"]
        p = paths.get(str(rp["id"]), {}).get("path")
        name = rp["path"].split("::")[-1]
        args = ((rp.get("args") or {}).get("angle_bracketed") or {}).get("args") or []
        inner = [type_key(a["type"], paths, crate) for a in args if "type" in a]
        if inner:
            name += "<" + ", ".join(i[1] if i else "_" for i in inner) + ">"
        return (p[0] if p else crate, name)
    if "generic" in t:
        return (crate, t["generic"])
    if "borrowed_ref" in t:
        r = type_key(t["borrowed_ref"]["type"], paths, crate)
        return (r[0], "&" + r[1]) if r else None
    if "dyn_trait" in t:
        names = [b["trait"]["path"].split("::")[-1] for b in t["dyn_trait"]["traits"]]
        return (crate, "dyn " + " + ".join(names))
    return None


def trait_graph(crates: list[Crate]) -> TraitGraph:
    g = TraitGraph()
    ours = {c.name.replace("-", "_") for c in crates}

    def key(item_id: int, paths: dict, fallback: str, crate: str) -> tuple[str, str]:
        p = paths.get(str(item_id), {}).get("path")
        return (p[0], p[-1]) if p else (crate, fallback.split("::")[-1])

    for c in crates:
        if not c.has_lib:
            continue
        crate = c.name.replace("-", "_")
        doc = json.loads((RUSTDOC_TARGET / "doc" / f"{crate}.json").read_text())
        index, paths = doc["index"], doc["paths"]
        for item in index.values():
            if item["crate_id"] != 0:
                continue
            inner = item["inner"]
            if "trait" in inner:
                tk = key(int(item["id"]), paths, item["name"], crate)
                g.traits.add(tk)
                for b in inner["trait"]["bounds"]:
                    tb = b.get("trait_bound")
                    if not tb:
                        continue
                    sk = key(tb["trait"]["id"], paths, tb["trait"]["path"], crate)
                    if sk[0] in ours:
                        g.supers.add((sk, tk))
            elif "impl" in inner:
                im = inner["impl"]
                if not im["trait"] or im["is_synthetic"] or im["blanket_impl"] or im["is_negative"]:
                    continue
                # Generated code (tonic/prost in OUT_DIR) is plumbing, not design.
                span = item.get("span")
                if span and not (ROOT / span["filename"]).resolve().is_relative_to(c.dir):
                    continue
                tk = key(im["trait"]["id"], paths, im["trait"]["path"], crate)
                ty = type_key(im["for"], paths, crate)
                if tk[0] not in ours or ty is None or ty[0] not in ours:
                    continue
                g.types.add(ty)
                g.impls.add((ty, tk, "generic" in im["for"]))
    return g


def short_crate(crate: str) -> str:
    return "bin" if crate == "pigeonhole" else crate.removeprefix("pigeonhole_").removeprefix("storage_")


def render_traits(g: TraitGraph) -> list[str]:
    """One Mermaid block per family (connected component), plain syntax only:
    no namespaces or labels, which many renderers get wrong."""
    nodes = g.traits | g.types
    base = lambda k: k[1].split("<")[0]
    clash = defaultdict(int)
    for k in nodes:
        clash[base(k)] += 1
    ident = {k: (f"{short_crate(k[0])}_{base(k)}" if clash[base(k)] > 1 else base(k)) for k in nodes}

    edges = [(sup, sub, False) for sup, sub in g.supers]
    supers_of: dict[tuple[str, str], set] = defaultdict(set)
    for sup, sub in g.supers:
        supers_of[sub].add(sup)
    implemented = defaultdict(set)
    for ty, tr, _ in g.impls:
        implemented[ty].add(tr)
    for ty, tr, _ in g.impls:
        # `T: Sub` already says `T: Super`; skip the redundant arrow.
        if not any(tr in supers_of[other] for other in implemented[ty]):
            edges.append((tr, ty, True))

    # Union-find over edges → families.
    parent = {k: k for k in nodes}

    def find(k):
        while parent[k] != k:
            parent[k] = parent[parent[k]]
            k = parent[k]
        return k

    for a, b, _ in edges:
        parent[find(a)] = find(b)
    families: dict = defaultdict(set)
    for k in nodes:
        families[find(k)].add(k)

    out, lonely = [], []
    for fam in sorted(families.values(), key=lambda f: (-len(f), sorted(ident[k] for k in f))):
        if len(fam) == 1:
            (k,) = fam
            lonely.append(f"`{ident[k]}` ({short_crate(k[0])})")
            continue
        roots = sorted(ident[k] for k in fam if k in g.traits and not supers_of[k])
        crates = sorted({short_crate(k[0]) for k in fam})
        out += [f"### {', '.join(roots)}", "", f"Crates: {', '.join(crates)}.", "", "```mermaid", "classDiagram"]
        for k in sorted(fam, key=lambda k: (k not in g.traits, ident[k])):
            generic = k[1][len(base(k)):]
            decl = ident[k] + (f"~{generic[1:-1]}~" if generic else "")
            if k in g.traits:
                out.append(f"  class {decl} {{\n    <<trait>>\n  }}")
            else:
                out.append(f"  class {decl}")
        for a, b, impl in sorted(edges, key=lambda e: (ident[e[0]], ident[e[1]])):
            if a in fam:
                out.append(f"  {ident[a]} <|{'..' if impl else '--'} {ident[b]}")
        out += ["```", ""]
    if lonely:
        out.append("Traits with no workspace implementors: " + ", ".join(sorted(lonely)) + ".")
    return out


# ---------------------------------------------------------------- report


@dataclass
class Size:
    files: int = 0
    code: int = 0
    test: int = 0
    comment: int = 0
    blank: int = 0


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("-o", "--out", type=Path, help="write Markdown here (default: stdout)")
    ap.add_argument("--no-traits", action="store_true", help="skip the rustdoc trait graph (fast, for hooks)")
    ap.add_argument("--top", type=int, default=20, help="hotspot rows (default 20)")
    ap.add_argument("--max-cognitive", type=int,
                    help="fail if a non-test fn exceeds this cognitive complexity")
    ap.add_argument("--max-lines", type=int, help="fail if a non-test fn is longer than this")
    args = ap.parse_args()

    crates = workspace()
    sizes: dict[str, Size] = {}
    fns: list[Fn] = []
    units = rca_units([c.dir for c in crates])

    for c in crates:
        s = sizes[c.name] = Size()
        scans = {f: scan(f) for f in c.files}
        test_files = {
            p for f, fs in scans.items() for name in fs.test_mod_files for p in child_mod_paths(f, name)
        }
        for f, fs in scans.items():
            rel = f.relative_to(c.dir)
            whole_test = (
                c.name in TEST_CRATES or rel.parts[0] in TEST_DIRS or f in test_files
            )

            def is_test(line: int, fs=fs, whole_test=whole_test) -> bool:
                return whole_test or any(a <= line <= b for a, b in fs.test_ranges)

            s.files += 1
            for n, (code, comment) in enumerate(zip(fs.code, fs.comment), 1):
                if code:
                    if is_test(n):
                        s.test += 1
                    else:
                        s.code += 1
                elif comment:
                    s.comment += 1
                else:
                    s.blank += 1
            relroot = str(f.relative_to(ROOT))
            unit = units.get(relroot)
            if unit is None:
                print(f"warning: no complexity data for {relroot}", file=sys.stderr)
            else:
                fns += functions(unit, c.name, relroot, is_test)

    sha = run(["git", "rev-parse", "--short", "HEAD"], cwd=ROOT).strip()
    md = [f"# Code report", "", f"Commit `{sha}`. Generated by `scripts/code_report.py`.", ""]

    md += ["## Size", "",
           "Lines of code, without comments and blanks. *test* counts `tests/`, `benches/`, "
           "the testkit crate and `#[cfg(test)]` modules.", "",
           "| crate | files | code | test | comments |",
           "|---|--:|--:|--:|--:|"]
    tot = Size()
    for name, s in sizes.items():
        md.append(f"| {name} | {s.files} | {s.code} | {s.test} | {s.comment} |")
        for k in ("files", "code", "test", "comment", "blank"):
            setattr(tot, k, getattr(tot, k) + getattr(s, k))
    md += [f"| **total** | **{tot.files}** | **{tot.code}** | **{tot.test}** | **{tot.comment}** |", ""]

    prod = [f for f in fns if not f.test]
    md += ["## Complexity", "",
           "Non-test functions. Cognitive complexity (SonarSource): nesting costs more "
           "than a flat chain of branches. Over 15 is usually worth a look.", "",
           "| crate | fns | cognitive sum | avg | max | > 15 |",
           "|---|--:|--:|--:|--:|--:|"]
    by_crate: dict[str, list[Fn]] = defaultdict(list)
    for f in prod:
        by_crate[f.crate].append(f)
    for name in sizes:
        fs = by_crate.get(name, [])
        if not fs:
            continue
        cog = [f.cognitive for f in fs]
        md.append(f"| {name} | {len(fs)} | {sum(cog)} | {sum(cog) / len(fs):.1f} | {max(cog)} | "
                  f"{sum(1 for x in cog if x > 15)} |")
    md += ["", f"### Top {args.top} functions", "",
           "| fn | location | cognitive | cyclomatic | lines |",
           "|---|---|--:|--:|--:|"]
    for f in sorted(prod, key=lambda f: (-f.cognitive, -f.cyclomatic))[:args.top]:
        md.append(f"| `{f.name}` | {f.file}:{f.line} | {f.cognitive} | {f.cyclomatic} | {f.lines} |")
    md += ["", "### Top 10 files by cognitive sum", "",
           "| file | fns | cognitive sum | max |", "|---|--:|--:|--:|"]
    by_file: dict[str, list[Fn]] = defaultdict(list)
    for f in prod:
        by_file[f.file].append(f)
    for file, fs in sorted(by_file.items(), key=lambda kv: -sum(f.cognitive for f in kv[1]))[:10]:
        md.append(f"| {file} | {len(fs)} | {sum(f.cognitive for f in fs)} | {max(f.cognitive for f in fs)} |")
    md.append("")

    if not args.no_traits:
        build_rustdoc(crates)
        md += ["## Traits", "",
               "Workspace traits, their supertraits (`<|--`) and workspace implementors (`<|..`). "
               "External traits and types are left out.", ""]
        md += render_traits(trait_graph(crates))
        md.append("")

    text = "\n".join(md)
    if args.out:
        args.out.write_text(text)
    else:
        sys.stdout.write(text)

    over = []
    if args.max_cognitive is not None:
        over += [f"{f.file}:{f.line} `{f.name}` cognitive {f.cognitive} > {args.max_cognitive}"
                 for f in prod if f.cognitive > args.max_cognitive]
    if args.max_lines is not None:
        over += [f"{f.file}:{f.line} `{f.name}` {f.lines} lines > {args.max_lines}" for f in prod if f.lines > args.max_lines]
    for msg in over:
        print(f"error: {msg}", file=sys.stderr)
    if over:
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())

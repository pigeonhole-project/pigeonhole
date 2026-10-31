#!/usr/bin/env python3
"""Repository infographic: one self-contained HTML page with charts.

Collects the crate graph, code size and complexity (via code_report.py), and
the git history (lines per layer over time, commits per day and hour, churn,
merged branches), and writes them into scripts/infographic.html.

    python3 scripts/infographic.py            # → target/infographic/index.html
    python3 scripts/infographic.py -o x.html
"""

from __future__ import annotations

import argparse
import json
import re
import subprocess
import sys
from collections import Counter, defaultdict
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
import code_report as cr  # noqa: E402

ROOT = cr.ROOT
TEMPLATE = Path(__file__).resolve().parent / "infographic.html"
ROLES = ["core", "meta", "net", "blob", "storage", "gateway", "bin", "testing"]
# Crate dirs before the split into role dirs (5db01a1), by name fragment.
OLD_ROLE = {
    "s3gram": "bin", "s3gram-s3": "gateway", "s3gram-bytestream": "gateway",
    "s3gram-telegram": "storage", "s3gram-discord": "storage", "s3gram-index": "meta",
    "s3gram-chunk": "core", "s3gram-core": "core", "s3gram-blob": "blob", "s3gram-engine": "blob",
}
TOPICS = {
    "Telegram": r"telegram|mtproto|grammers",
    "Discord": r"discord",
    "Raft": r"raft",
    "S3": r"\bs3\b|s3s",
    "Снапшоты": r"snapshot",
    "Лог": r"\blog\b|logstore|chain",
    "Свипер / GC": r"sweep|gc\b|orphan|refcount",
    "Метрики": r"metric|grafana|prometheus|dashboard",
    "Чанки": r"chunk|block|extent",
    "Лимитер": r"limit",
    "Лиз": r"lease",
    "Диск": r"\bdisk\b|segment",
    "Симуляция": r"madsim|simulat|fault",
    "Бенчмарки": r"bench",
    "Конфиг": r"config|toml",
    "Тесты": r"\btest",
    "Документация": r"\bdocs?\b|readme|roadmap",
    "Переименования": r"renam",
}


def git(*args: str) -> str:
    return cr.run(["git", *args], cwd=ROOT)


def role_of(path: str) -> str | None:
    """Layer of a .rs path at any point in history; None for vendored code."""
    parts = path.split("/")
    if parts[0] in ("vendor", ".cache"):
        return None
    if parts[0] == "crates" and len(parts) > 2:
        return parts[1] if parts[1] in ROLES else OLD_ROLE[parts[1]]
    if parts[0] == "demos":
        return None
    return "bin"  # the single-crate days: src/, tests/


class BlobLines:
    """Line counts of git blobs, through one `git cat-file --batch`."""

    def __init__(self) -> None:
        self.p = subprocess.Popen(["git", "cat-file", "--batch"], cwd=ROOT,
                                  stdin=subprocess.PIPE, stdout=subprocess.PIPE)
        self.cache: dict[str, int] = {}

    def __call__(self, sha: str) -> int:
        if sha not in self.cache:
            self.p.stdin.write(sha.encode() + b"\n")
            self.p.stdin.flush()
            size = int(self.p.stdout.readline().split()[2])
            body = self.p.stdout.read(size + 1)
            self.cache[sha] = body.count(b"\n")
        return self.cache[sha]


def history() -> dict:
    """Rust lines per layer and doc lines at each first-parent commit of HEAD."""
    lines = BlobLines()
    out = []
    revs = git("log", "--first-parent", "--reverse", "--format=%H %h %aI %s").splitlines()
    for rev in revs:
        sha, short, date, subject = rev.split(" ", 3)
        per = Counter()
        for row in git("ls-tree", "-r", sha).splitlines():
            meta, path = row.split("\t", 1)
            blob = meta.split()[2]
            if path.endswith(".rs"):
                role = role_of(path)
                if role:
                    per[role] += lines(blob)
            elif path.endswith(".md") and not path.startswith(("vendor/", ".cache/")):
                per["docs"] += lines(blob)
        out.append({"sha": short, "date": date, "subject": subject, **per})
    return out


def commits() -> list[dict]:
    fmt = "%x1e%h%x1f%aI%x1f%P%x1f%s%x1f%(trailers:key=Co-Authored-By,valueonly,separator=;)"
    raw = git("log", "--numstat", f"--format={fmt}")
    out = []
    for rec in raw.split("\x1e")[1:]:
        head, _, stat = rec.partition("\n")
        sha, date, parents, subject, coauth = head.split("\x1f")
        add = dele = 0
        files = []
        for row in stat.strip().splitlines():
            a, d, path = row.split("\t", 2)
            if a == "-":
                continue
            add, dele = add + int(a), dele + int(d)
            files.append(path)
        agent = "Claude" if "Claude" in coauth else "Cursor" if "Cursor" in coauth else "solo"
        out.append({
            "sha": sha, "date": date, "subject": subject, "agent": agent,
            "merge": len(parents.split()) > 1, "add": add, "del": dele, "files": files,
        })
    return out


def churn(cs: list[dict], top: int) -> list[dict]:
    """Most often changed source files that still exist (renames followed loosely)."""
    alive = set(git("ls-files").splitlines())
    n = Counter()
    for c in cs:
        if c["merge"]:
            continue
        for f in c["files"]:
            m = re.match(r"(.*)\{(.*) => (.*)\}(.*)", f)
            path = (m.group(1) + m.group(3) + m.group(4)).replace("//", "/") if m else f.split(" => ")[-1]
            n[path] += 1
    rows = [(p, k) for p, k in n.most_common() if p in alive and p.endswith(".rs")]
    return [{"path": p, "commits": k} for p, k in rows[:top]]


def crates() -> tuple[list[dict], list[dict], list[dict]]:
    """Crates with size and deps, files for the treemap, functions for complexity."""
    ws = cr.workspace()
    names = {c.name for c in ws}
    meta = json.loads(cr.run(["cargo", "metadata", "--format-version", "1", "--no-deps"], cwd=ROOT))
    pkgs = {p["name"]: p for p in meta["packages"]}
    units = cr.rca_units([c.dir for c in ws])
    out, files, fns = [], [], []
    for c in ws:
        pkg = pkgs[c.name]
        deps = {d["name"] for d in pkg["dependencies"] if d["kind"] is None}
        dev = {d["name"] for d in pkg["dependencies"] if d["kind"] == "dev"}
        scans = {f: cr.scan(f) for f in c.files}
        test_files = {p for f, fs in scans.items() for m in fs.test_mod_files
                      for p in cr.child_mod_paths(f, m)}
        size = Counter()
        tests = 0
        for f, fs in scans.items():
            rel = f.relative_to(c.dir)
            whole = c.name in cr.TEST_CRATES or rel.parts[0] in cr.TEST_DIRS or f in test_files

            def is_test(line: int, fs=fs, whole=whole) -> bool:
                return whole or any(a <= line <= b for a, b in fs.test_ranges)

            fsize = Counter()
            for n, (code, comment) in enumerate(zip(fs.code, fs.comment), 1):
                key = ("test" if is_test(n) else "code") if code else "comment" if comment else "blank"
                fsize[key] += 1
            size.update(fsize)
            text = f.read_text(errors="replace")
            tests += len(re.findall(r"#\[(?:tokio::|madsim::)?test\b", text))
            relroot = str(f.relative_to(ROOT))
            files.append({"crate": c.name, "path": str(rel), **fsize})
            if unit := units.get(relroot):
                fns += [{"crate": c.name, "file": relroot, "name": fn.name, "line": fn.line,
                         "lines": fn.lines, "cog": fn.cognitive, "test": fn.test}
                        for fn in cr.functions(unit, c.name, relroot, is_test)]
        role = c.dir.relative_to(ROOT / "crates").parts[0]
        out.append({
            "name": c.name, "role": role, "files": len(c.files), "tests": tests, **size,
            "deps": sorted(deps & names), "devDeps": sorted((dev & names) - deps),
            "external": sorted(deps - names),
            "description": pkg.get("description") or "",
        })
    return out, files, fns


def roadmap() -> list[dict]:
    steps = []
    for line in (ROOT / "docs/roadmap.md").read_text().splitlines():
        if m := re.match(r"### Step (\w+) — (.*)", line):
            title = m.group(2)
            status = ("dropped" if "dropped" in title else
                      "partial" if "✅" in title and "left" in title else
                      "done" if "✅" in title else "todo")
            title = re.sub(r"\s*(✅.*|— dropped.*)$", "", title).replace("`", "")
            steps.append({"step": m.group(1), "title": title, "status": status})
    return steps


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("-o", "--out", type=Path, default=ROOT / "target/infographic/index.html")
    args = ap.parse_args()

    print("history…", file=sys.stderr)
    hist = history()
    cs = commits()
    print("crates…", file=sys.stderr)
    cr_list, files, fns = crates()
    topics = {k: sum(1 for c in cs if not c["merge"] and re.search(v, c["subject"], re.I))
              for k, v in TOPICS.items()}
    merges = [{"date": c["date"], "branch": m.group(1), "what": m.group(2)}
              for c in cs if (m := re.match(r"Merge branch '([^']+)':? ?(.*)", c["subject"]))]
    docs = sorted(
        ({"path": str(p.relative_to(ROOT)), "lines": p.read_text().count("\n")}
         for p in [*ROOT.glob("docs/**/*.md"), ROOT / "README.md"]),
        key=lambda d: -d["lines"])
    data = {
        "head": git("rev-parse", "--short", "HEAD").strip(),
        "generated": git("log", "-1", "--format=%aI").strip(),
        "history": hist,
        "commits": [{k: c[k] for k in ("sha", "date", "subject", "agent", "merge", "add", "del")}
                    for c in cs],
        "churn": churn(cs, 15),
        "crates": cr_list,
        "files": files,
        "fns": [f for f in fns if not f["test"]],
        "topics": topics,
        "merges": merges[::-1],
        "roadmap": roadmap(),
        "docs": docs,
    }
    html = TEMPLATE.read_text().replace("/*DATA*/null", json.dumps(data, separators=(",", ":"), ensure_ascii=False))
    args.out.parent.mkdir(parents=True, exist_ok=True)
    args.out.write_text(html)
    print(args.out)
    return 0


if __name__ == "__main__":
    sys.exit(main())

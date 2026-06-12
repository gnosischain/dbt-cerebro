#!/usr/bin/env python3
"""
Generate the API endpoint migration guide (old path -> new path) by diffing the
api_* model tags in git HEAD vs the working tree, replaying the cerebro-api route
template /{category}/{resource}/{granularity}[/{window}].

Usage: python scripts/checks/gen_api_migration.py <out.md>
"""
import os, re, subprocess, sys, datetime

REPO = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
SYSTEM_TAGS = {"production", "view", "table", "incremental", "staging", "intermediate",
               "daily", "weekly", "monthly", "hourly", "latest", "in_ranges",
               "last_30d", "last_7d", "all_time"}
TIER_RE = re.compile(r"^tier\d+$")
TAGS_RE = re.compile(r"tags\s*=\s*\[(.*?)\]", re.DOTALL)


def parse_tags(text):
    m = TAGS_RE.search(text or "")
    if not m:
        return None
    return [t.strip().strip("'\"") for t in m.group(1).split(",") if t.strip()]


def category_of(tags):
    for t in tags:
        tl = t.lower()
        if tl in SYSTEM_TAGS or TIER_RE.match(tl) or ":" in t:
            continue
        return tl
    return "general"


def path_for(tags):
    res = next((t[4:] for t in tags if t.startswith("api:")), None)
    if not res:
        return None
    gran = next((t[len("granularity:"):].lower() for t in tags if t.startswith("granularity:")), None)
    win = next((t[len("window:"):].lower() for t in tags if t.startswith("window:")), None)
    parts = [category_of(tags), res]
    if gran:
        parts.append(gran)
    if win:
        parts.append(win)
    return "/" + "/".join(parts)


def git_head(path):
    try:
        return subprocess.check_output(["git", "show", f"HEAD:{path}"], cwd=REPO,
                                       stderr=subprocess.DEVNULL).decode()
    except subprocess.CalledProcessError:
        return None


def main():
    out = sys.argv[1] if len(sys.argv) > 1 else os.path.join(REPO, "API_MIGRATION.md")
    changed = subprocess.check_output(
        ["git", "diff", "--name-only", "HEAD", "--", "models"], cwd=REPO).decode().splitlines()
    changed = [p for p in changed if re.search(r"/api_[^/]*\.sql$", p)]

    rows = []
    for p in sorted(changed):
        new = parse_tags(open(os.path.join(REPO, p)).read())
        old = parse_tags(git_head(p))
        if not new or not old:
            continue
        op, np = path_for(old), path_for(new)
        if op and np and op != np:
            rows.append((category_of(new), op, np, os.path.basename(p)[:-4]))

    rows.sort()
    today = datetime.date.today().isoformat()  # informational; pass a fixed date if reproducibility needed
    lines = [
        f"# API endpoint migration ({today})",
        "",
        "The `api:` resource id no longer embeds the time grain or metric window. Each endpoint",
        "is now `/{category}/{resource}/{granularity}[/{window}]`:",
        "",
        "- **Time grain** (`daily`/`weekly`/`monthly`/`hourly`/`latest`) moved out of the resource",
        "  name into the `granularity` path segment. Daily/weekly/monthly variants that used to be",
        "  separate endpoints are now **one resource** selected by the granularity segment.",
        "- **Metric windows** (`7d`/`30d`/`60d`) moved out of the name into a new trailing",
        "  `window` path segment.",
        "",
        f"**{len(rows)} endpoint paths changed.** Update clients per the table below.",
        "",
        "| Category | Old path | New path | Model |",
        "|---|---|---|---|",
    ]
    for cat, op, np, name in rows:
        lines.append(f"| {cat} | `{op}` | `{np}` | {name} |")
    lines.append("")

    os.makedirs(os.path.dirname(out), exist_ok=True)
    open(out, "w").write("\n".join(lines))
    print(f"Wrote {out} with {len(rows)} endpoint path changes.")


if __name__ == "__main__":
    main()

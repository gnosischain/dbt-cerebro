#!/usr/bin/env python3
"""
Add `as_of_date` to point-in-time api endpoints that lack a date column.

For each target api view, find the nearest upstream model that has a date column
(BFS over depends_on) and wrap the view body to expose
`as_of_date = toDate(max(<that date col>))` of that ancestor — the data's real
freshness (correct for views; today()/now() would be query-time). Documents the
column in the model's schema.yml (ruamel round-trip). Models with no dated ancestor
are reported for manual handling.

Usage: python scripts/checks/add_as_of_date.py [--apply]   (default: dry-run)
"""
import json, os, re, sys
from ruamel.yaml import YAML

REPO = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
man = json.load(open(os.path.join(REPO, "target", "manifest.json")))
try:
    cat = json.load(open(os.path.join(REPO, "target", "catalog.json")))
except Exception:
    cat = {"nodes": {}}
nodes = man["nodes"]

DATE_PRIORITY = ["date", "block_date", "day", "week", "month", "quarter", "hour", "block_timestamp"]
DATE = set(DATE_PRIORITY) | {"as_of_date", "snapshot_date", "ts", "timestamp", "dt"}
POINT = {"latest", "snapshot", "all_time", "total", "history", "last_7d", "last_30d",
         "last_day", "last_week", "last_month", "in_ranges", "7d", "30d", "60d", "rolling_180d"}
CONFIG_RE = re.compile(r"\{\{\s*config[\s\S]*?\}\}")


def cols(uid):
    c = {k.lower() for k in (nodes.get(uid, {}).get("columns") or {})}
    c |= {k.lower() for k in (cat.get("nodes", {}).get(uid, {}).get("columns") or {})}
    return c


def date_col(uid):
    c = cols(uid)
    for d in DATE_PRIORITY:
        if d in c:
            return d
    return None


def dated_ancestor(uid, maxd=6):
    seen = {uid}; frontier = [uid]
    for _ in range(maxd):
        nxt = []
        for u in frontier:
            for p in nodes.get(u, {}).get("depends_on", {}).get("nodes", []):
                if p.startswith("model.") and p not in seen:
                    seen.add(p)
                    dc = date_col(p)
                    if dc:
                        return nodes[p]["name"], dc
                    nxt.append(p)
        frontier = nxt
    return None, None


targets = []  # (uid, name, sqlpath, ancestor_name, ancestor_datecol, patch)
for uid, n in nodes.items():
    if n.get("resource_type") != "model" or "production" not in (n.get("tags") or []):
        continue
    if not any(t.startswith("api:") for t in n["tags"]):
        continue
    g = next((t[12:] for t in n["tags"] if t.startswith("granularity:")), "")
    if g in POINT and not (cols(uid) & DATE):
        anc, dc = dated_ancestor(uid)
        targets.append((uid, n["name"], n["original_file_path"], anc, dc, n.get("patch_path")))

apply = "--apply" in sys.argv
wrapped = skipped = 0
skips = []

# 1) wrap SQL
for uid, name, sqlpath, anc, dc, patch in targets:
    full = os.path.join(REPO, sqlpath)
    text = open(full).read()
    if "AS as_of_date" in text:
        continue
    m = CONFIG_RE.search(text)
    if not m:
        skips.append(name + " (no config block)"); skipped += 1; continue
    cfg, body = text[:m.end()], text[m.end():].strip()
    if anc:
        # as_of_date from the data's freshness (correct for views)
        expr = f"(SELECT toDate(max({dc})) FROM {{{{ ref('{anc}') }}}})"
    else:
        # no dated ancestor: a pure current-state/reference endpoint -> query-time today()
        expr = "today()"
        skips.append(name + " (today() fallback — no dated ancestor)"); skipped += 1
    new = (f"{cfg}\n\n"
           f"SELECT sub.*, {expr} AS as_of_date\n"
           f"FROM (\n{body}\n) AS sub\n")
    if apply:
        open(full, "w").write(new)
    wrapped += 1

# 2) document as_of_date in schema.yml (grouped per file)
yaml = YAML(); yaml.preserve_quotes = True; yaml.width = 200
by_file = {}
for uid, name, sqlpath, anc, dc, patch in targets:
    if patch:
        by_file.setdefault(patch.split("://")[-1], set()).add(name)
files_touched = 0
for pf, names in by_file.items():
    full = os.path.join(REPO, pf)
    data = yaml.load(open(full))
    changed = False
    for mod in (data.get("models") or []):
        if mod.get("name") in names:
            colz = mod.get("columns")
            if colz is None:
                mod["columns"] = colz = []
            if not any((c or {}).get("name") == "as_of_date" for c in colz):
                colz.append({"name": "as_of_date",
                             "description": "Date the snapshot is computed as of (max date in the underlying data).",
                             "data_type": "Date"})
                changed = True
    if changed:
        files_touched += 1
        if apply:
            yaml.dump(data, open(full, "w"))

print(f"targets={len(targets)} | wrapped={wrapped} | skipped(no dated ancestor)={skipped} | schema files={files_touched}")
if skips:
    print("MANUAL (no dated ancestor):")
    for s in skips:
        print("  -", s)
print("DRY RUN — re-run with --apply" if not apply else "APPLIED")

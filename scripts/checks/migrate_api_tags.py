#!/usr/bin/env python3
"""
Normalize api:/granularity:/window:/tier tags on api_* models.

Canonical convention:
  api:<resource>      grain- and window-free, stable endpoint id
  granularity:<g>     daily|weekly|monthly|hourly|latest|snapshot|quarterly|...
  window:<w>          metric lookback (7d|30d|...), replaces a _Nd suffix in the name
  tier{0|1|2}         required

Reads target/manifest.json for the authoritative tag set, rewrites each model's
config(tags=[...]) literal in its .sql, and emits:
  - a change report (stdout)
  - the old->new endpoint PATH map (factory template /{category}/{resource}/{granularity}[/{window}])
    written to target/api_migration_paths.tsv (consumed by the migration-guide step)

Usage:
  python scripts/checks/migrate_api_tags.py --dry-run    # report only, no edits
  python scripts/checks/migrate_api_tags.py --apply      # rewrite the .sql files
"""
import argparse, json, os, re, sys

REPO = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
MANIFEST = os.path.join(REPO, "target", "manifest.json")

GRAIN_SUFFIXES = {
    "daily": "daily", "weekly": "weekly", "monthly": "monthly", "hourly": "hourly",
    "latest": "latest", "snapshot": "snapshot", "quarterly": "quarterly",
}
WINDOW_RE = re.compile(r"_(\d+d)$")              # _7d, _30d, _90d  -> window
# factory._extract_category skip-set (mirror app/factory.py)
SYSTEM_TAGS = {
    "production", "view", "table", "incremental", "staging", "intermediate",
    "daily", "weekly", "monthly", "hourly", "latest", "in_ranges",
    "last_30d", "last_7d", "all_time",
}
TIER_RE = re.compile(r"^tier\d+$")
TAGS_RE = re.compile(r"tags\s*=\s*\[(.*?)\]", re.DOTALL)


def category_of(tags):
    for t in tags:
        tl = t.lower()
        if tl in SYSTEM_TAGS or TIER_RE.match(tl) or ":" in t:
            continue
        return tl
    return "general"


def path_for(tags):
    """Replay app/factory.py._build_url_path -> /{category}/{resource}/{granularity}[/{window}]."""
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


def transform(tags):
    """Return (new_tags, notes[]). Only touches api:/granularity:/window:/tier tokens."""
    notes = []
    api_idx = next((i for i, t in enumerate(tags) if t.startswith("api:")), None)
    if api_idx is None:
        return tags, notes
    new = list(tags)
    resource = new[api_idx][4:]
    has_gran = any(t.startswith("granularity:") for t in new)
    has_win = any(t.startswith("window:") for t in new)
    has_tier = any(TIER_RE.match(t.lower()) for t in new)

    # 1) strip a trailing grain/window suffix from the api: resource
    grain_added = None
    base = resource
    last = resource.rsplit("_", 1)[-1] if "_" in resource else ""
    wm = WINDOW_RE.search(resource)
    if last in GRAIN_SUFFIXES:
        base = resource[: -(len(last) + 1)]
        grain_added = GRAIN_SUFFIXES[last]
        new[api_idx] = "api:" + base
        if not has_gran:
            new.append("granularity:" + grain_added)
        else:
            cur = next(t[len("granularity:"):].lower() for t in new if t.startswith("granularity:"))
            if cur != grain_added:
                notes.append(f"grain mismatch: name '{last}' vs granularity:'{cur}' (kept granularity)")
    elif wm:
        w = wm.group(1)
        base = resource[: wm.start()]
        new[api_idx] = "api:" + base
        if not has_win:
            new.append("window:" + w)
        if not has_gran:
            notes.append(f"window stripped but NO granularity: present -> REVIEW")
    else:
        # already grain/window-free in the name
        if not has_gran:
            notes.append("no api-name suffix and missing granularity: -> REVIEW")

    # 2) ensure a tier
    if not has_tier:
        new.append("tier1")
        notes.append("added tier1 (was missing)")

    return new, notes


def find_sql(node):
    p = node.get("original_file_path") or node.get("path")
    return os.path.join(REPO, p) if p else None


def model_config_tags(sql_text):
    """Extract the literal tags list tokens from config(tags=[...]) in the .sql."""
    m = TAGS_RE.search(sql_text)
    if not m:
        return None, None
    toks = [tok.strip().strip("'\"") for tok in m.group(1).split(",")]
    toks = [t for t in toks if t]
    return toks, m


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--apply", action="store_true")
    ap.add_argument("--dry-run", action="store_true")
    args = ap.parse_args()
    if not args.apply and not args.dry_run:
        args.dry_run = True

    man = json.load(open(MANIFEST))
    rows, collisions, reviews, path_map = [], {}, [], []
    edits = 0

    for n, node in man["nodes"].items():
        if node.get("resource_type") != "model":
            continue
        name = node["name"]
        if not any(t.startswith("api:") for t in node.get("tags", [])):
            continue
        sql_path = find_sql(node)
        if not sql_path or not os.path.exists(sql_path):
            continue
        text = open(sql_path).read()
        cfg_tags, m = model_config_tags(text)
        if cfg_tags is None:
            reviews.append(f"{name}: could not find config(tags=[...]) literal")
            continue
        new_tags, notes = transform(cfg_tags)
        old_path = path_for(cfg_tags)
        new_path = path_for(new_tags)
        if new_tags != cfg_tags:
            rows.append((name, cfg_tags, new_tags, notes))
            if old_path and new_path and old_path != new_path:
                path_map.append((old_path, new_path, name))
            key = new_path
            collisions.setdefault(key, []).append(name)
            if notes and any("REVIEW" in x for x in notes):
                reviews.append(f"{name}: {notes}")
            if args.apply:
                new_literal = "tags=[" + ", ".join("'%s'" % t for t in new_tags) + "]"
                text = text[: m.start()] + new_literal + text[m.end():]
                open(sql_path, "w").write(text)
                edits += 1

    print(f"\n=== {len(rows)} models to change ===")
    for name, old, new, notes in sorted(rows):
        oa = next(t for t in old if t.startswith("api:"))
        na = next(t for t in new if t.startswith("api:"))
        extra = [t for t in new if t not in old]
        if oa != na or extra:
            print(f"  {name}: {oa} -> {na}" + (f"  +{extra}" if extra else "") + (f"  !! {notes}" if notes else ""))

    real_coll = {k: v for k, v in collisions.items() if len(v) > 1}
    if real_coll:
        print(f"\n=== {len(real_coll)} PATH COLLISIONS (manual review) ===")
        for k, v in real_coll.items():
            print(f"  {k}  <-  {v}")
    if reviews:
        print(f"\n=== {len(reviews)} REVIEW flags ===")
        for r in reviews:
            print("  " + r)

    if args.apply:
        os.makedirs(os.path.join(REPO, "target"), exist_ok=True)
        with open(os.path.join(REPO, "target", "api_migration_paths.tsv"), "w") as f:
            f.write("old_path\tnew_path\tmodel\n")
            for o, nw, nm in sorted(set(path_map)):
                f.write(f"{o}\t{nw}\t{nm}\n")
        print(f"\nAPPLIED {edits} file edits; wrote target/api_migration_paths.tsv ({len(set(path_map))} path changes)")
    else:
        print(f"\nDRY RUN: {len(rows)} files would change; {len(set(path_map))} endpoint paths would change. Re-run with --apply.")


if __name__ == "__main__":
    main()

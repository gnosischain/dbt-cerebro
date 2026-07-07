#!/usr/bin/env python3
"""
CI guard for the API/MCP exposure convention.

Every `production` model carrying an `api:` tag (an exposed endpoint) must:
  1. have an `api:<resource>` tag whose name does NOT end in a time-grain or window
     suffix (grain/window live in `granularity:` / `window:` tags, not the endpoint id);
  2. carry exactly one `granularity:` tag;
  3. carry a `tier{0|1|2}` tag;
  4. have a complete column schema in schema.yml — at least one column, every column
     typed with `data_type` (so the endpoint publishes typed, documented columns).

Reads target/manifest.json (run `dbt parse` first). Violations fail the build unless
listed in scripts/checks/check_api_tags.allow (one `unique_id::rule` per line; '#' comments).

Usage: dbt parse && python scripts/checks/check_api_tags.py
"""
import json, os, re, sys

REPO = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
MANIFEST = os.path.join(REPO, "target", "manifest.json")
ALLOW = os.path.join(os.path.dirname(os.path.abspath(__file__)), "check_api_tags.allow")

GRAIN_SUFFIXES = ("_daily", "_weekly", "_monthly", "_hourly", "_latest", "_snapshot", "_quarterly")
WINDOW_RE = re.compile(r"_\d+d$")
TIER_RE = re.compile(r"^tier\d+$")
POINT_GRANS = {"latest", "snapshot", "all_time", "total", "history", "last_7d", "last_30d",
               "last_day", "last_week", "last_month", "in_ranges", "7d", "30d", "60d", "rolling_180d"}
GRAIN_COL = {"daily": {"date", "block_date", "day"}, "weekly": {"week"}, "monthly": {"month"},
             "quarterly": {"quarter"}, "hourly": {"hour", "ts"}}


def load_allow():
    if not os.path.exists(ALLOW):
        return set()
    out = set()
    for line in open(ALLOW):
        line = line.split("#", 1)[0].strip()
        if line:
            out.add(line)
    return out


def main():
    man = json.load(open(MANIFEST))
    allow = load_allow()
    violations = []
    used_allow = set()

    for uid, node in man["nodes"].items():
        if node.get("resource_type") != "model":
            continue
        tags = node.get("tags", []) or []
        if "production" not in tags:
            continue
        api = [t for t in tags if t.startswith("api:")]
        if not api:
            continue
        name = node["name"]

        def fail(rule, msg):
            for key in (f"{uid}::{rule}", f"{name}::{rule}"):
                if key in allow:
                    used_allow.add(key)
                    return
            violations.append(f"{name}  [{rule}]  {msg}")

        # 1. api: resource must be grain/window-free
        for t in api:
            res = t[4:]
            if res.endswith(GRAIN_SUFFIXES) or WINDOW_RE.search(res):
                fail("api_suffix", f"'{t}' embeds a grain/window suffix — move it to granularity:/window:")
        if len(api) > 1:
            fail("multi_api", f"multiple api: tags {api}")

        # 2. exactly one granularity:
        gran = [t for t in tags if t.startswith("granularity:")]
        if len(gran) != 1:
            fail("granularity", f"expected exactly one granularity: tag, found {gran}")

        # 3. a tier
        if not any(TIER_RE.match(t.lower()) for t in tags):
            fail("tier", "missing tier{0|1|2} tag")

        # 4. complete column schema
        cols = node.get("columns", {}) or {}
        if not cols:
            fail("columns_missing", "no columns in schema.yml (endpoint columns undocumented)")
        else:
            untyped = [c for c, meta in cols.items() if not (meta.get("data_type"))]
            if untyped:
                fail("columns_untyped", f"{len(untyped)} column(s) missing data_type, e.g. {untyped[:3]}")

        # 5. granularity-aware freshness column
        gtag = gran[0][len("granularity:"):].lower() if len(gran) == 1 else ""
        colset = {c.lower() for c in cols}
        FRESH_POINT = {"as_of_date", "snapshot_date", "date", "block_date",
                       "block_timestamp", "ts", "timestamp", "day"}
        if gtag in POINT_GRANS and not (colset & FRESH_POINT):
            fail("no_as_of_date", "point-in-time endpoint needs as_of_date (or date/snapshot_date)")
        elif gtag in GRAIN_COL and not (colset & GRAIN_COL[gtag]):
            fail("no_grain_col", f"{gtag} endpoint must expose a grain column {sorted(GRAIN_COL[gtag])}")

    if violations:
        print(f"API tag/schema convention: {len(violations)} violation(s):\n")
        for v in sorted(violations):
            print("  " + v)
        print(f"\nFix per the convention, or allowlist in {os.path.relpath(ALLOW, REPO)} (unique_id::rule).")
        sys.exit(1)

    # Shrink-only ratchet: an allow entry that suppressed nothing this run is
    # FIXED — force its removal so the backlog can only go down.
    stale = allow - used_allow
    if stale:
        print(f"API tag/schema convention: {len(stale)} STALE allowlist entr(ies) — the "
              "violation is fixed; delete these lines from "
              f"{os.path.relpath(ALLOW, REPO)}:\n")
        for entry in sorted(stale):
            print("  " + entry)
        sys.exit(1)

    print("API tag/schema convention OK: all production api: endpoints are grain/window-free, "
          "have granularity + tier, and complete typed column schemas.")


if __name__ == "__main__":
    main()

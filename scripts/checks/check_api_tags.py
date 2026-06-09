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
            if f"{uid}::{rule}" not in allow and f"{name}::{rule}" not in allow:
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

    if violations:
        print(f"API tag/schema convention: {len(violations)} violation(s):\n")
        for v in sorted(violations):
            print("  " + v)
        print(f"\nFix per the convention, or allowlist in {os.path.relpath(ALLOW, REPO)} (unique_id::rule).")
        sys.exit(1)
    print("API tag/schema convention OK: all production api: endpoints are grain/window-free, "
          "have granularity + tier, and complete typed column schemas.")


if __name__ == "__main__":
    main()

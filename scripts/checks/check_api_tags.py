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

Naming rule (first-party models): anything NAMED `api_*` is claiming to be an
endpoint, so it must either
  - carry an `api:` tag AND the `production` tag (entering the checks above), or
  - opt out explicitly with `meta.api.exclude_from_api: true` (the documented
    internal-model contract — permanently fine, no allowlist entry needed).
  [missing_api_tag]     api_-named, no api: tag, no meta opt-out
  [api_not_production]  api:-tagged but not production (never validated otherwise)

Reads target/manifest.json (run `dbt parse` first). Violations fail the build unless
listed in scripts/checks/check_api_tags.allow (one `unique_id::rule` per line; '#'
comments). The allowlist is SHRINK-ONLY: entries that stop suppressing a violation
fail the build until deleted.

Usage: dbt parse && python scripts/checks/check_api_tags.py
"""
import json, os, re, sys

REPO = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
MANIFEST = os.path.join(REPO, "target", "manifest.json")
ALLOW = os.path.join(os.path.dirname(os.path.abspath(__file__)), "check_api_tags.allow")

PROJECT_PACKAGE = "gnosis_dbt"

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


def _merged_meta(node):
    cfg_meta = (node.get("config") or {}).get("meta") or {}
    top_meta = node.get("meta") or {}
    merged = dict(cfg_meta)
    merged.update(top_meta)
    return merged


def run_checks(man, allow):
    """Returns (violations, used_allow). Violations are display strings
    'name  [rule]  msg'; used_allow tracks which entries suppressed something."""
    violations = []
    used_allow = set()

    def fail(uid, name, rule, msg):
        for key in ("%s::%s" % (uid, rule), "%s::%s" % (name, rule)):
            if key in allow:
                used_allow.add(key)
                return
        violations.append("%s  [%s]  %s" % (name, rule, msg))

    for uid, node in man["nodes"].items():
        if node.get("resource_type") != "model":
            continue
        name = node["name"]
        tags = node.get("tags", []) or []
        api = [t for t in tags if t.startswith("api:")]

        # Naming rule: api_* names claim endpoint-hood — first-party only.
        if name.startswith("api_") and node.get("package_name") == PROJECT_PACKAGE:
            excluded = (_merged_meta(node).get("api") or {}).get("exclude_from_api") is True
            if not excluded:
                if not api:
                    fail(uid, name, "missing_api_tag",
                         "named api_* but has no api: tag — tag it (entering the "
                         "endpoint convention) or set meta.api.exclude_from_api: true")
                elif "production" not in tags:
                    fail(uid, name, "api_not_production",
                         "api:-tagged but not production-tagged — the endpoint "
                         "convention is never validated for it")

        if "production" not in tags:
            continue
        if not api:
            continue

        # 1. api: resource must be grain/window-free
        for t in api:
            res = t[4:]
            if res.endswith(GRAIN_SUFFIXES) or WINDOW_RE.search(res):
                fail(uid, name, "api_suffix",
                     "'%s' embeds a grain/window suffix — move it to granularity:/window:" % t)
        if len(api) > 1:
            fail(uid, name, "multi_api", "multiple api: tags %s" % api)

        # 2. exactly one granularity:
        gran = [t for t in tags if t.startswith("granularity:")]
        if len(gran) != 1:
            fail(uid, name, "granularity", "expected exactly one granularity: tag, found %s" % gran)

        # 3. a tier
        if not any(TIER_RE.match(t.lower()) for t in tags):
            fail(uid, name, "tier", "missing tier{0|1|2} tag")

        # 4. complete column schema
        cols = node.get("columns", {}) or {}
        if not cols:
            fail(uid, name, "columns_missing", "no columns in schema.yml (endpoint columns undocumented)")
        else:
            untyped = [c for c, meta in cols.items() if not (meta.get("data_type"))]
            if untyped:
                fail(uid, name, "columns_untyped",
                     "%d column(s) missing data_type, e.g. %s" % (len(untyped), untyped[:3]))

        # 5. granularity-aware freshness column
        gtag = gran[0][len("granularity:"):].lower() if len(gran) == 1 else ""
        colset = {c.lower() for c in cols}
        FRESH_POINT = {"as_of_date", "snapshot_date", "date", "block_date",
                       "block_timestamp", "ts", "timestamp", "day"}
        if gtag in POINT_GRANS and not (colset & FRESH_POINT):
            fail(uid, name, "no_as_of_date", "point-in-time endpoint needs as_of_date (or date/snapshot_date)")
        elif gtag in GRAIN_COL and not (colset & GRAIN_COL[gtag]):
            fail(uid, name, "no_grain_col",
                 "%s endpoint must expose a grain column %s" % (gtag, sorted(GRAIN_COL[gtag])))

    return violations, used_allow


def main():
    man = json.load(open(MANIFEST))
    allow = load_allow()
    violations, used_allow = run_checks(man, allow)

    if violations:
        print("API tag/schema convention: %d violation(s):\n" % len(violations))
        for v in sorted(violations):
            print("  " + v)
        print("\nFix per the convention, or allowlist in %s (unique_id::rule)."
              % os.path.relpath(ALLOW, REPO))
        sys.exit(1)

    # Shrink-only ratchet: an allow entry that suppressed nothing this run is
    # FIXED — force its removal so the backlog can only go down.
    stale = allow - used_allow
    if stale:
        print("API tag/schema convention: %d STALE allowlist entr(ies) — the "
              "violation is fixed; delete these lines from %s:\n"
              % (len(stale), os.path.relpath(ALLOW, REPO)))
        for entry in sorted(stale):
            print("  " + entry)
        sys.exit(1)

    suffix = (" (%d grandfathered exemption(s) remain in the shrink-only allowlist)"
              % len(used_allow)) if used_allow else ""
    print("API tag/schema convention OK: api_* models are tagged or explicitly "
          "excluded, endpoints are grain/window-free with granularity + tier and "
          "complete typed column schemas.%s" % suffix)


if __name__ == "__main__":
    main()

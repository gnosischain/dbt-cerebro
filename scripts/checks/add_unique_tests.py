#!/usr/bin/env python3
"""Add/normalize a recency-bounded dbt_utils.unique_combination_of_columns test
on every in-scope incremental model that meets the write-time no-duplicate
invariant (append/insert_overwrite/table — NOT the allowlisted delete+insert
exceptions). The combination key is the model's ReplacingMergeTree dedup key
(its `order_by`), so the test is the direct proof that the stored, plain
(non-FINAL) relation holds no duplicate business keys.

The `where` predicate bounds the daily test to the last N days (cheap) and
becomes a full-table scan under `--vars '{test_full_refresh: true}'` (weekly):

    {% if var('test_full_refresh', false) %}1=1
    {% else %}toDate(<datecol>) >= today() - {{ var('test_lookback_days', 7) }}{% endif %}

<datecol> is the column inside the model's partition_by (toStartOf*/toYYYYMM).
Models without a partition_by date column get an unbounded uniqueness test.

Behaviour:
  * in-scope, NOT allowlisted -> upsert: combination := current order_by,
    where := current datecol (corrects stale keys from older manifests).
  * in-scope, allowlisted (delete+insert exception) -> strip any managed test
    (a recency-bounded unique_combination test) — those models rely on async
    mutations and legitimately show transient plain-relation duplicates, so the
    strict write-time test does not apply to them.

A "managed" test = dbt_utils.unique_combination_of_columns whose config.where
contains the recency convention (test_lookback_days / test_full_refresh) OR has
no where at all. ruamel preserves formatting. Idempotent.
"""
import json, glob, pathlib, re, io
from ruamel.yaml import YAML

ROOT = pathlib.Path("/app")
yaml = YAML(); yaml.preserve_quotes = True; yaml.width = 10000
# Never emit YAML anchors/aliases. Auto-generated anchors (&id001 ...) make
# multiple model entries SHARE one `tests`/`meta` object, so a per-model edit
# silently entangles other models (and gives them the wrong unique-test key).
# Expanding on load + never re-anchoring on dump keeps every entry independent.
yaml.representer.ignore_aliases = lambda *_a: True


def load_dealiased(path):
    """Load YAML with all anchors/aliases expanded into independent objects."""
    doc = yaml.load(pathlib.Path(path).read_text())
    if doc is None:
        return None
    buf = io.StringIO()
    yaml.dump(doc, buf)            # ignore_aliases -> fully-expanded text
    return yaml.load(buf.getvalue())

WHERE = ("{% if var('test_full_refresh', false) %}1=1{% else %}"
         "toDate(__COL__) >= today() - {{ var('test_lookback_days', 7) }}{% endif %}")

TEST_KEY = "dbt_utils.unique_combination_of_columns"


def parse_cols(order_by):
    if order_by is None:
        return []
    s = order_by if isinstance(order_by, str) else ",".join(order_by)
    s = s.strip()
    if s.startswith("(") and s.endswith(")"):
        s = s[1:-1]
    return [c.strip() for c in s.split(",") if c.strip()]


def date_col(partition_by):
    """Innermost column of a (possibly nested) date partition key, e.g.
    toStartOfMonth(toDate(block_timestamp)) -> block_timestamp."""
    if not partition_by:
        return None
    s = partition_by.strip()
    for _ in range(6):  # peel nested to*( ... ) wrappers
        m = re.match(r"^to\w+\(\s*(.*)$", s)
        if not m:
            break
        s = m.group(1)
    m = re.match(r"^([A-Za-z_]\w*)", s.strip())
    return m.group(1) if m else None


def is_managed(test):
    """A recency-bounded (or unbounded) unique_combination test the script owns."""
    if not (isinstance(test, dict) and TEST_KEY in test):
        return False
    cfg = (test.get(TEST_KEY) or {}).get("config") or {}
    where = cfg.get("where")
    if where is None:
        return True
    return ("test_lookback_days" in where) or ("test_full_refresh" in where)


# ---- allowlist (delete+insert exceptions) ----
allow = set()
allow_path = ROOT / "scripts" / "checks" / "no_delete_insert.allow"
if allow_path.exists():
    for line in allow_path.read_text().splitlines():
        line = line.strip()
        if not line or line.startswith("#"):
            continue
        allow.add(line.split(".")[-1])  # model.gnosis_dbt.<name> -> <name>
print(f"allowlisted (excluded): {len(allow)}")

# ---- in-scope incremental models ----
m = json.loads((ROOT / "target" / "manifest.json").read_text())
inscope = {}   # name -> {cols, datecol}
for n in m["nodes"].values():
    if n.get("resource_type") != "model":
        continue
    cfg = n.get("config", {})
    if cfg.get("materialized") != "incremental":
        continue
    tags = cfg.get("tags") or []
    if "dev" in tags:
        continue
    name = n["name"]
    if name.startswith("circles_v1"):
        continue
    cols = parse_cols(cfg.get("order_by"))
    if not cols:
        continue
    inscope[name] = {"cols": cols, "datecol": date_col(cfg.get("partition_by"))}

want = {k: v for k, v in inscope.items() if k not in allow}
strip = set(inscope) & allow
print(f"in-scope incremental: {len(inscope)}  -> add/normalize: {len(want)}  strip-from-allowlisted: {len(strip)}")

added = upserted = stripped = 0
changed_files = 0
no_entry = set(want)

for sf in glob.glob(str(ROOT / "models" / "**" / "*.yml"), recursive=True):
    p = pathlib.Path(sf)
    try:
        doc = load_dealiased(p)
    except Exception as e:
        print(f"  SKIP (parse) {sf}: {repr(e)[:80]}")
        continue
    if not isinstance(doc, dict) or "models" not in doc:
        continue
    dirty = False
    for entry in (doc.get("models") or []):
        if not isinstance(entry, dict):
            continue
        name = entry.get("name")
        tests = entry.get("tests")

        # The managed recency-bounded unique test belongs ONLY on in-scope
        # incremental non-allowlisted models (`want`). Strip it from everything
        # else (allowlisted delete+insert exceptions, table/out-of-scope models,
        # and any stray copy left behind by a previously-shared anchor block).
        if name not in want:
            if tests:
                kept = [t for t in tests if not is_managed(t)]
                if len(kept) != len(tests):
                    entry["tests"] = kept
                    stripped += 1
                    dirty = True
            continue
        no_entry.discard(name)
        spec = want[name]
        desired = {"combination_of_columns": list(spec["cols"])}
        if spec["datecol"]:
            desired["config"] = {"where": WHERE.replace("__COL__", spec["datecol"])}

        tests = entry.setdefault("tests", [])
        existing = next((t for t in tests if is_managed(t)), None)
        if existing is None:
            tests.append({TEST_KEY: desired})
            added += 1
            dirty = True
        else:
            cur = existing[TEST_KEY]
            def _norm(w):
                return " ".join(w.split()) if isinstance(w, str) else w
            cur_where = _norm((cur.get("config") or {}).get("where"))
            des_where = _norm((desired.get("config") or {}).get("where"))
            if list(cur.get("combination_of_columns") or []) != desired["combination_of_columns"] or \
               cur_where != des_where:
                existing[TEST_KEY] = desired
                upserted += 1
                dirty = True
    if dirty:
        yaml.dump(doc, p.open("w"))
        changed_files += 1

print(f"added {added}, corrected {upserted}, stripped-from-allowlisted {stripped}; files changed {changed_files}")
print(f"in-scope (non-allow) but NO schema.yml entry ({len(no_entry)}): {sorted(no_entry)}")

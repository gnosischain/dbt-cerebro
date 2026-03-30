#!/usr/bin/env python3
"""Add `where` config to not_null/unique tests for recent-data filtering.

Reads elementary_model_classification.csv to find the timestamp column for each
model, then patches schema.yml files so that not_null and unique tests use
the test_recency_filter macro.

Idempotent — skips tests that already have a `where` config.

Usage:
    python scripts/apply_test_where_filters.py --dry-run   # preview changes
    python scripts/apply_test_where_filters.py              # apply all
"""

import argparse
import csv
import sys
from pathlib import Path

from ruamel.yaml import YAML
from ruamel.yaml.comments import CommentedMap

REPO_ROOT = Path(__file__).resolve().parent.parent
CSV_PATH = REPO_ROOT / "scripts" / "analysis" / "elementary_model_classification.csv"

TEST_TYPES_TO_PATCH = {"not_null", "unique"}

yaml = YAML()
yaml.preserve_quotes = True
yaml.width = 200


def load_model_timestamp_map() -> dict[str, str]:
    """Return {model_name: timestamp_column} from the classification CSV."""
    mapping = {}
    with open(CSV_PATH) as f:
        for row in csv.DictReader(f):
            ts_col = row.get("timestamp_column", "").strip()
            if ts_col:
                mapping[row["model_name"]] = ts_col
    return mapping


def make_where_value(ts_col: str) -> str:
    """Build an inline Jinja where expression using only built-in constructs.

    dbt's test config parser does NOT support custom macros in `where`,
    so we inline the logic using var() and if/else which are always available.
    """
    return (
        "{% if var('test_full_refresh', false) %}1=1"
        "{% else %}"
        f"toDate({ts_col}) >= today() - {{{{ var('test_lookback_days', 7) }}}}"
        "{% endif %}"
    )


def patch_test(test_entry, ts_col: str):
    """Convert a bare test string or dict to include where config.

    Returns the patched entry, or None if no change needed.
    """
    where_val = make_where_value(ts_col)

    if isinstance(test_entry, str):
        # Bare string like "not_null" or "unique"
        if test_entry not in TEST_TYPES_TO_PATCH:
            return None
        config_block = CommentedMap()
        config_block["where"] = where_val
        test_body = CommentedMap()
        test_body["config"] = config_block
        new = CommentedMap([(test_entry, test_body)])
        return new

    if isinstance(test_entry, dict):
        test_name = next(iter(test_entry))
        if test_name not in TEST_TYPES_TO_PATCH:
            return None
        cfg = test_entry[test_name]
        if cfg is None:
            cfg = CommentedMap()
            test_entry[test_name] = cfg
        if isinstance(cfg, CommentedMap) or isinstance(cfg, dict):
            config_block = cfg.get("config")
            if config_block and config_block.get("where"):
                return None  # already has where
            if not config_block:
                config_block = CommentedMap()
                cfg["config"] = config_block
            config_block["where"] = where_val
            return test_entry

    return None


def process_schema_file(schema_path: Path, ts_map: dict[str, str], dry_run: bool) -> int:
    """Patch a single schema.yml file. Returns count of tests modified."""
    data = yaml.load(schema_path)
    if not data:
        return 0

    models = data.get("models", []) or []
    sources = data.get("sources", []) or []

    modified = 0

    # Process models
    for model in models:
        model_name = model.get("name", "")
        ts_col = ts_map.get(model_name)
        if not ts_col:
            continue

        columns = model.get("columns", []) or []
        for col in columns:
            tests = col.get("tests")
            if not tests:
                continue
            for i, test_entry in enumerate(tests):
                patched = patch_test(test_entry, ts_col)
                if patched is not None:
                    tests[i] = patched
                    modified += 1

        # Model-level tests
        model_tests = model.get("tests")
        if model_tests:
            for i, test_entry in enumerate(model_tests):
                patched = patch_test(test_entry, ts_col)
                if patched is not None:
                    model_tests[i] = patched
                    modified += 1

    # Process sources (if any have tests)
    for source in sources:
        for table in source.get("tables", []) or []:
            table_name = table.get("name", "")
            ts_col = ts_map.get(table_name)
            if not ts_col:
                continue
            columns = table.get("columns", []) or []
            for col in columns:
                tests = col.get("tests")
                if not tests:
                    continue
                for i, test_entry in enumerate(tests):
                    patched = patch_test(test_entry, ts_col)
                    if patched is not None:
                        tests[i] = patched
                        modified += 1

    if modified and not dry_run:
        import io
        buf = io.StringIO()
        yaml.dump(data, buf)
        content = buf.getvalue()
        # ruamel sometimes inserts blank lines between '-' and the mapping key
        # when replacing a scalar with a mapping in a sequence. Clean that up.
        import re
        content = re.sub(r'(\n\s*)-\s*\n\s*\n(\s+)', r'\1- ', content)
        schema_path.write_text(content)

    return modified


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--dry-run", action="store_true", help="Preview without writing")
    args = parser.parse_args()

    ts_map = load_model_timestamp_map()
    print(f"Loaded {len(ts_map)} models with timestamp columns from CSV")

    schema_files = sorted(REPO_ROOT.glob("models/**/schema.yml"))
    print(f"Found {len(schema_files)} schema.yml files")

    total_modified = 0
    for schema_path in schema_files:
        rel = schema_path.relative_to(REPO_ROOT)
        count = process_schema_file(schema_path, ts_map, args.dry_run)
        if count:
            label = "(dry run)" if args.dry_run else ""
            print(f"  {rel}: {count} tests patched {label}")
            total_modified += count

    action = "would patch" if args.dry_run else "patched"
    print(f"\nDone: {action} {total_modified} tests across {len(schema_files)} files")


if __name__ == "__main__":
    main()

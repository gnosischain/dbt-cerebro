#!/usr/bin/env python3
"""Add Elementary tests to schema.yml files based on classification CSV.

Idempotent YAML patcher — skips models that already have Elementary tests
(unless --migrate-legacy is set for the 3 known legacy files).

Usage:
    python scripts/add_elementary_tests.py --dry-run                    # preview all
    python scripts/add_elementary_tests.py --module consensus --dry-run  # preview one module
    python scripts/add_elementary_tests.py --wave 1                     # apply wave 1
    python scripts/add_elementary_tests.py                              # apply all
"""

import argparse
import csv
import sys
from collections import defaultdict
from pathlib import Path

from ruamel.yaml import YAML
from ruamel.yaml.comments import CommentedMap, CommentedSeq

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

REPO_ROOT = Path(__file__).resolve().parent.parent
MODELS_DIR = REPO_ROOT / "models"
CSV_PATH = REPO_ROOT / "scripts" / "analysis" / "elementary_model_classification.csv"

# Legacy files that need days_back → time_bucket migration
LEGACY_FILES = {
    "models/consensus/marts/schema.yml",
    "models/execution/blocks/marts/schema.yml",
    "models/execution/transactions/marts/schema.yml",
}

# ---------------------------------------------------------------------------
# Test template builders
# ---------------------------------------------------------------------------


def make_volume_anomalies(ts_col: str, cls: str, severity: str = "warn") -> CommentedMap:
    """Build an elementary.volume_anomalies test config."""
    cfg = CommentedMap()
    cfg["timestamp_column"] = ts_col

    if cls == "daily":
        cfg["time_bucket"] = CommentedMap([("period", "day"), ("count", 1)])
        cfg["training_period"] = CommentedMap([("period", "day"), ("count", 56)])
        cfg["seasonality"] = "day_of_week"
        cfg["detection_period"] = CommentedMap([("period", "day"), ("count", 2)])
        cfg["detection_delay"] = CommentedMap([("period", "day"), ("count", 1)])
        cfg["anomaly_sensitivity"] = 3
        cfg["ignore_small_changes"] = CommentedMap([
            ("spike_failure_percent_threshold", 10),
            ("drop_failure_percent_threshold", 20),
        ])
    elif cls == "hourly":
        cfg["time_bucket"] = CommentedMap([("period", "hour"), ("count", 1)])
        cfg["training_period"] = CommentedMap([("period", "day"), ("count", 21)])
        cfg["seasonality"] = "hour_of_week"
        cfg["detection_period"] = CommentedMap([("period", "hour"), ("count", 6)])
        cfg["detection_delay"] = CommentedMap([("period", "hour"), ("count", 1)])
        cfg["anomaly_sensitivity"] = 3.5
        cfg["ignore_small_changes"] = CommentedMap([
            ("spike_failure_percent_threshold", 15),
            ("drop_failure_percent_threshold", 25),
        ])
    elif cls == "weekly":
        cfg["time_bucket"] = CommentedMap([("period", "week"), ("count", 1)])
        cfg["training_period"] = CommentedMap([("period", "week"), ("count", 26)])
        cfg["detection_period"] = CommentedMap([("period", "week"), ("count", 2)])
        cfg["detection_delay"] = CommentedMap([("period", "week"), ("count", 1)])
    elif cls == "event_grain":
        cfg["time_bucket"] = CommentedMap([("period", "day"), ("count", 1)])
        cfg["training_period"] = CommentedMap([("period", "day"), ("count", 90)])
        cfg["anomaly_sensitivity"] = 3

    cfg["severity"] = severity
    cfg["tags"] = ["elementary"]

    test = CommentedMap()
    test["elementary.volume_anomalies"] = cfg
    return test


def make_freshness_anomalies(ts_col: str, cls: str, severity: str = "warn") -> CommentedMap:
    """Build an elementary.freshness_anomalies test config."""
    cfg = CommentedMap()
    cfg["timestamp_column"] = ts_col

    if cls in ("daily", "event_grain"):
        cfg["time_bucket"] = CommentedMap([("period", "day"), ("count", 1)])
    elif cls == "hourly":
        cfg["time_bucket"] = CommentedMap([("period", "hour"), ("count", 1)])
    elif cls == "weekly":
        cfg["time_bucket"] = CommentedMap([("period", "week"), ("count", 1)])

    cfg["severity"] = severity
    cfg["tags"] = ["elementary"]

    test = CommentedMap()
    test["elementary.freshness_anomalies"] = cfg
    return test


def make_schema_changes(severity: str = "warn") -> CommentedMap:
    """Build an elementary.schema_changes test config."""
    cfg = CommentedMap()
    cfg["severity"] = severity
    cfg["tags"] = ["elementary"]

    test = CommentedMap()
    test["elementary.schema_changes"] = cfg
    return test


def make_column_anomalies(ts_col: str, severity: str = "warn") -> CommentedMap:
    """Build an elementary.column_anomalies test config."""
    cfg = CommentedMap()
    cfg["column_anomalies"] = ["null_count", "min", "max"]
    if ts_col:
        cfg["timestamp_column"] = ts_col
        cfg["time_bucket"] = CommentedMap([("period", "day"), ("count", 1)])
    cfg["severity"] = severity
    cfg["tags"] = ["elementary"]

    test = CommentedMap()
    test["elementary.column_anomalies"] = cfg
    return test


# ---------------------------------------------------------------------------
# Test presence detection
# ---------------------------------------------------------------------------


def has_elementary_test(tests_list, test_type: str) -> bool:
    """Check if a specific Elementary test type already exists."""
    if not tests_list:
        return False
    for test in tests_list:
        if isinstance(test, dict) and test_type in test:
            return True
    return False


def has_any_elementary_test(model: dict) -> bool:
    """Check if model has any Elementary test at model or column level."""
    tests = model.get("tests", [])
    for test in (tests or []):
        if isinstance(test, dict):
            for key in test:
                if key.startswith("elementary."):
                    return True
    for col in (model.get("columns", []) or []):
        for test in (col.get("tests", []) or []):
            if isinstance(test, dict):
                for key in test:
                    if key.startswith("elementary."):
                        return True
    return False


def is_legacy_elementary(model: dict) -> bool:
    """Check if model has old-style Elementary tests (days_back parameter)."""
    tests = model.get("tests", [])
    for test in (tests or []):
        if isinstance(test, dict):
            for key, val in test.items():
                if key.startswith("elementary.") and isinstance(val, dict) and "days_back" in val:
                    return True
    return False


# ---------------------------------------------------------------------------
# Core patching logic
# ---------------------------------------------------------------------------


def determine_severity(row: dict) -> str:
    """Determine severity based on tier and model type."""
    tier = row.get("tier", "tier3")
    name = row["model_name"]

    # Error severity for critical models
    if tier == "tier0":
        return "error"
    if tier == "tier1" and name.startswith("api_"):
        # Schema changes on api_ models get error
        return "warn"  # anomalies stay warn, schema_changes override below

    return "warn"


def determine_schema_severity(row: dict) -> str:
    """Determine severity specifically for schema_changes tests."""
    name = row["model_name"]
    if name.startswith("api_"):
        return "error"
    return "warn"


def patch_model(model: dict, row: dict, schema_file: str, stats: dict) -> bool:
    """Add Elementary tests to a single model. Returns True if modified."""
    name = row["model_name"]
    cls = row["class"]
    ts_col = row["timestamp_column"]
    anomaly_enabled = row["anomaly_enabled"] == "True"
    schema_change_enabled = row["schema_change_enabled"] == "True"
    kpi_cols = [c for c in row.get("kpi_columns", "").split("|") if c]
    has_fr = row["has_full_refresh"] == "True"

    severity = determine_severity(row)
    schema_severity = determine_schema_severity(row)

    # Skip staging models entirely
    if cls == "staging":
        return False

    modified = False

    # Handle legacy migration: remove old tests and re-add
    if schema_file in LEGACY_FILES and is_legacy_elementary(model):
        old_tests = model.get("tests", [])
        new_tests = CommentedSeq()
        for test in (old_tests or []):
            if isinstance(test, dict):
                keys = list(test.keys())
                if any(k.startswith("elementary.") for k in keys):
                    stats["legacy_migrated"] += 1
                    continue  # drop old elementary test
            new_tests.append(test)
        model["tests"] = new_tests
        modified = True
    elif has_any_elementary_test(model):
        # Already has elementary tests and not a legacy file — skip
        return False

    # Ensure model has a tests list
    if "tests" not in model or model["tests"] is None:
        model["tests"] = CommentedSeq()

    tests = model["tests"]

    # Add model-level tests based on class
    if anomaly_enabled and ts_col:
        # Volume anomalies
        if not has_elementary_test(tests, "elementary.volume_anomalies"):
            tests.append(make_volume_anomalies(ts_col, cls, severity))
            stats["volume_added"] += 1
            modified = True

        # Freshness anomalies
        if not has_elementary_test(tests, "elementary.freshness_anomalies"):
            tests.append(make_freshness_anomalies(ts_col, cls, severity))
            stats["freshness_added"] += 1
            modified = True

    # Schema changes
    if schema_change_enabled and not has_elementary_test(tests, "elementary.schema_changes"):
        tests.append(make_schema_changes(schema_severity))
        stats["schema_changes_added"] += 1
        modified = True

    # Column anomalies on KPI columns
    if anomaly_enabled and kpi_cols and ts_col:
        columns = model.get("columns", []) or []
        for col in columns:
            col_name = col.get("name", "")
            if col_name in kpi_cols:
                col_tests = col.get("tests")
                if col_tests is None:
                    col["tests"] = CommentedSeq()
                    col_tests = col["tests"]

                if not has_elementary_test(col_tests, "elementary.column_anomalies"):
                    col_tests.append(make_column_anomalies(ts_col, severity))
                    stats["column_anomalies_added"] += 1
                    modified = True

    if modified:
        stats["models_modified"] += 1

    return modified


def patch_schema_file(path: Path, yaml_inst: YAML, classifications: dict,
                      dry_run: bool, stats: dict) -> bool:
    """Patch a single schema.yml file. Returns True if modified."""
    data = yaml_inst.load(path)
    if data is None:
        return False

    rel_path = str(path.relative_to(REPO_ROOT))
    models = data.get("models", [])
    if not models:
        return False

    file_modified = False
    for model in models:
        name = model.get("name", "")
        row = classifications.get(name)
        if not row:
            continue

        if patch_model(model, row, rel_path, stats):
            file_modified = True

    if file_modified:
        stats["files_modified"] += 1
        if not dry_run:
            yaml_inst.dump(data, path)
        print(f"  {'[DRY] ' if dry_run else ''}Modified: {rel_path}")

    return file_modified


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------


def load_classifications(csv_path: Path) -> dict:
    """Load classification CSV into a dict keyed by model_name."""
    classifications = {}
    with open(csv_path) as f:
        reader = csv.DictReader(f)
        for row in reader:
            classifications[row["model_name"]] = row
    return classifications


def main():
    parser = argparse.ArgumentParser(description="Add Elementary tests to schema.yml files")
    parser.add_argument("--dry-run", action="store_true", help="Preview without modifying files")
    parser.add_argument("--module", type=str, help="Only process models in this module")
    parser.add_argument("--wave", type=int, help="Only process models in this rollout wave")
    parser.add_argument("--csv", type=Path, default=CSV_PATH, help="Classification CSV path")
    args = parser.parse_args()

    if not args.csv.exists():
        print(f"ERROR: Classification CSV not found at {args.csv}")
        print("Run scripts/classify_models.py first.")
        sys.exit(1)

    classifications = load_classifications(args.csv)

    # Filter by module or wave if specified
    if args.module:
        classifications = {k: v for k, v in classifications.items() if v["module"] == args.module}
    if args.wave:
        classifications = {k: v for k, v in classifications.items() if v["rollout_wave"] == str(args.wave)}

    if not classifications:
        print("No models match the filter criteria.")
        return

    # Group by schema file
    by_file = defaultdict(list)
    for name, row in classifications.items():
        by_file[row["schema_file"]].append(name)

    yaml_inst = YAML()
    yaml_inst.preserve_quotes = True
    yaml_inst.width = 4096

    stats = {
        "files_modified": 0,
        "models_modified": 0,
        "volume_added": 0,
        "freshness_added": 0,
        "schema_changes_added": 0,
        "column_anomalies_added": 0,
        "legacy_migrated": 0,
    }

    print(f"Processing {len(classifications)} models across {len(by_file)} schema files")
    if args.dry_run:
        print("DRY RUN — no files will be modified\n")

    schema_files = sorted(MODELS_DIR.rglob("schema.yml"))
    for path in schema_files:
        rel = str(path.relative_to(REPO_ROOT))
        if rel not in by_file:
            continue
        patch_schema_file(path, yaml_inst, classifications, args.dry_run, stats)

    # Summary
    print(f"\n{'DRY RUN ' if args.dry_run else ''}Summary:")
    print(f"  Files modified:          {stats['files_modified']}")
    print(f"  Models modified:         {stats['models_modified']}")
    print(f"  volume_anomalies added:  {stats['volume_added']}")
    print(f"  freshness_anomalies:     {stats['freshness_added']}")
    print(f"  schema_changes added:    {stats['schema_changes_added']}")
    print(f"  column_anomalies added:  {stats['column_anomalies_added']}")
    print(f"  Legacy tests migrated:   {stats['legacy_migrated']}")


if __name__ == "__main__":
    main()

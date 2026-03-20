#!/usr/bin/env python3
"""Classify all dbt models for Elementary test rollout.

Reads schema.yml files directly (no manifest required) and emits a CSV at
scripts/analysis/elementary_model_classification.csv.

Usage:
    python scripts/classify_models.py
    python scripts/classify_models.py --output /path/to/output.csv
"""

import argparse
import csv
import re
import sys
from pathlib import Path

from ruamel.yaml import YAML

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

MODELS_DIR = Path(__file__).resolve().parent.parent / "models"
DEFAULT_OUTPUT = Path(__file__).resolve().parent / "analysis" / "elementary_model_classification.csv"

# Suffix-based classification rules
SUFFIX_RULES = [
    (r"_hourly$", "hourly"),
    (r"_hourly_recent$", "hourly"),
    (r"_daily$", "daily"),
    (r"_daily_\w+$", "daily"),
    (r"_weekly$", "weekly"),
    (r"_monthly$", "monthly"),
    (r"_latest$", "latest_snapshot"),
    (r"_7d$", "latest_snapshot"),
    (r"_30d$", "latest_snapshot"),
    (r"_last_\d+_days$", "latest_snapshot"),
    (r"_total$", "latest_snapshot"),
    (r"_total_\w+$", "latest_snapshot"),
    (r"_all_time$", "latest_snapshot"),
    (r"_alltime_\w+$", "latest_snapshot"),
    (r"_cumulative$", "latest_snapshot"),
    (r"_snapshot$", "latest_snapshot"),
    (r"_snapshots$", "latest_snapshot"),
    (r"_top\d+$", "latest_snapshot"),
    (r"_ranges$", "latest_snapshot"),
    (r"_ranges_top\d+$", "latest_snapshot"),
    (r"_by_token$", "latest_snapshot"),
    (r"_by_bridge$", "daily"),  # bridge flows are daily grain
    (r"_composition$", "latest_snapshot"),
]

# Name-based overrides for specific non-time-series models
NON_TIMESERIES_NAMES = {
    "api_consensus_forks", "fct_consensus_forks",
    "api_consensus_graffiti_cloud", "fct_consensus_graffiti_cloud",
    "int_consensus_validators_labels",
    "api_execution_circles_avatars", "fct_execution_circles_avatars",
    "fct_execution_circles_backing", "int_execution_circles_backing",
    "int_execution_circles_transitive_transfers",
    "int_execution_circles_v1_avatars", "int_execution_circles_v2_avatars",
    "int_execution_gpay_activity", "int_execution_gpay_wallet_owners",
    "int_execution_rwa_backedfi_prices",
    "int_execution_transactions_unique_addresses",
    "int_execution_transfers_whitelisted_raw",
    "int_p2p_discv4_peers", "int_p2p_discv5_peers",
    "int_esg_node_classification", "int_esg_node_client_distribution",
    "int_esg_node_geographic_distribution", "int_esg_node_population_chao1",
    "int_esg_dynamic_power_consumption", "int_esg_carbon_intensity_ensemble",
    "fct_esg_carbon_footprint_uncertainty", "api_esg_carbon_timeseries_bands",
    "fct_crawlers_data_distinct_projects_sectors",
    "api_crawlers_data_distinct_projects_sectors_totals",
    "int_crawlers_data_labels",
    "api_execution_gpay_user_activity",
    "fct_execution_gpay_user_lifetime_metrics",
    "api_execution_gpay_user_lifetime_metrics",
    "api_execution_gpay_wallet_balance_composition",
}

# Known KPI column names
KPI_COLUMN_PATTERNS = {
    "value", "cnt", "total", "count", "txs", "n_txs",
    "gas_used", "gas_limit", "fee", "fees",
    "volume", "amount", "balance", "supply",
    "active_users", "active_accounts", "holders",
    "pct", "percentage", "share",
}
KPI_COLUMN_PREFIXES = ("fee_", "active_", "total_", "n_")

# Known dimension column names (low-cardinality, stable)
DIMENSION_COLUMNS = {
    "label", "client", "sector", "project", "transaction_type",
    "country", "bridge", "cloud", "cloud_provider", "region",
    "credentials_type", "status", "version", "agent_version_type",
    "is_cloud", "quic_support", "reachability", "token_class",
    "symbol", "contract_type",
}

# Rollout wave assignments by module path prefix
WAVE_RULES = [
    ("execution/tokens", 1),
    ("execution/transactions", 1),
    ("contracts/", 1),
    ("execution/blocks", 1),
    ("execution/gpay", 1),
    ("execution/", 1),  # remaining execution
    ("consensus/", 2),
    ("p2p/", 3),
    ("bridges/", 3),
    ("crawlers_data/", 4),
    ("probelab/", 4),
    ("ESG/", 4),
]

# CSV columns
CSV_COLUMNS = [
    "model_name",
    "schema_file",
    "module",
    "tags",
    "tier",
    "class",
    "timestamp_column",
    "has_full_refresh",
    "has_existing_elementary_tests",
    "rollout_wave",
    "anomaly_enabled",
    "kpi_columns",
    "dimension_columns",
    "schema_change_enabled",
    "notes",
]


# ---------------------------------------------------------------------------
# Classification logic
# ---------------------------------------------------------------------------


def classify_model_name(name: str) -> str:
    """Classify a model by its name suffix/prefix."""
    # Prefix-based rules (highest priority)
    if name.startswith("stg_"):
        return "staging"
    if name.startswith("contracts_") and ("_events" in name or "_calls" in name):
        return "event_grain"

    # Explicit non-time-series overrides
    if name in NON_TIMESERIES_NAMES:
        return "non_timeseries"

    # Suffix-based rules
    for pattern, cls in SUFFIX_RULES:
        if re.search(pattern, name):
            return cls

    return "manual_review"


def infer_timestamp_column(name: str, columns: list, cls: str) -> str:
    """Infer the monitoring timestamp column from model metadata."""
    col_names = [c.get("name", "") for c in columns]

    if cls in ("daily", "weekly", "monthly"):
        if "date" in col_names:
            return "date"
        if "month" in col_names:
            return "month"
        if "week" in col_names:
            return "week"
    if cls == "hourly":
        if "hour" in col_names:
            return "hour"
        if "datetime" in col_names:
            return "datetime"
        if "date" in col_names:
            return "date"
    if cls == "event_grain":
        if "block_timestamp" in col_names:
            return "block_timestamp"

    # Fallback: look for common timestamp columns
    for candidate in ("date", "block_timestamp", "slot_timestamp", "month", "hour"):
        if candidate in col_names:
            return candidate

    return ""


def find_kpi_columns(columns: list) -> list:
    """Identify numeric KPI columns suitable for column_anomalies."""
    kpi = []
    for col in columns:
        name = col.get("name", "")
        dtype = col.get("data_type", "").lower()

        # Skip non-numeric types
        if dtype and not any(t in dtype for t in ("int", "uint", "float", "decimal", "double")):
            continue

        if name in KPI_COLUMN_PATTERNS:
            kpi.append(name)
        elif any(name.startswith(p) for p in KPI_COLUMN_PREFIXES):
            kpi.append(name)

    return kpi


def find_dimension_columns(columns: list) -> list:
    """Identify low-cardinality dimension columns for segmented tests."""
    dims = []
    for col in columns:
        name = col.get("name", "")
        if name in DIMENSION_COLUMNS:
            dims.append(name)
    return dims


def has_elementary_tests(model: dict) -> bool:
    """Check if a model already has Elementary tests."""
    tests = model.get("tests", [])
    for test in tests:
        if isinstance(test, dict):
            for key in test:
                if key.startswith("elementary."):
                    return True
    # Check column-level tests too
    for col in model.get("columns", []):
        for test in col.get("tests", []):
            if isinstance(test, dict):
                for key in test:
                    if key.startswith("elementary."):
                        return True
    return False


def get_module(schema_file: Path) -> str:
    """Extract module name from schema file path."""
    rel = schema_file.relative_to(MODELS_DIR)
    parts = rel.parts
    if len(parts) >= 1:
        return parts[0]
    return ""


def get_module_path(schema_file: Path) -> str:
    """Get relative path for wave assignment."""
    rel = schema_file.relative_to(MODELS_DIR)
    return str(rel)


def assign_wave(module_path: str) -> int:
    """Assign rollout wave based on module path."""
    for prefix, wave in WAVE_RULES:
        if module_path.startswith(prefix):
            return wave
    return 4  # default to last wave


def determine_tier(name: str, cls: str) -> str:
    """Assign a tier based on model name and class."""
    # Critical execution and consensus models
    if any(x in name for x in ["execution_blocks", "execution_transactions"]):
        if cls in ("daily", "hourly"):
            return "tier0"
    if "consensus_blocks" in name or "consensus_validators" in name:
        if cls in ("daily",):
            return "tier1"
    if name.startswith("api_"):
        return "tier1"
    if name.startswith("fct_"):
        return "tier1"
    if name.startswith("int_"):
        return "tier2"
    return "tier3"


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------


def process_schema_file(path: Path, yaml: YAML) -> list:
    """Process a single schema.yml file and return classification rows."""
    data = yaml.load(path)
    if data is None:
        return []

    rows = []
    models = data.get("models", [])
    rel_path = str(path.relative_to(MODELS_DIR.parent))
    module = get_module(path)
    module_path = get_module_path(path)

    for model in models:
        name = model.get("name", "")
        if not name:
            continue

        meta = model.get("meta", {}) or {}
        columns = model.get("columns", []) or []
        tags_raw = model.get("tags", []) or meta.get("tags", []) or []

        cls = classify_model_name(name)
        has_fr = bool(meta.get("full_refresh"))
        has_elem = has_elementary_tests(model)
        ts_col = infer_timestamp_column(name, columns, cls)
        kpi_cols = find_kpi_columns(columns)
        dim_cols = find_dimension_columns(columns)
        wave = assign_wave(module_path)
        tier = determine_tier(name, cls)

        # Anomaly eligibility: time-series model with timestamp, no full_refresh
        anomaly_eligible = (
            cls in ("daily", "hourly", "weekly")
            and bool(ts_col)
            and not has_fr
        )

        # Schema change eligibility: api_*, fct_*, reused int_* (not stg_*)
        schema_change_eligible = (
            name.startswith("api_")
            or name.startswith("fct_")
            or (name.startswith("int_") and cls != "staging")
        ) and cls != "staging"

        # Notes for manual review
        notes = []
        if cls == "manual_review":
            notes.append("needs manual classification")
        if has_fr and cls in ("daily", "hourly", "weekly"):
            notes.append("full_refresh: skip volume/freshness anomalies")
        if cls == "event_grain" and has_fr:
            notes.append("contract event: schema_changes only")
        if not ts_col and cls in ("daily", "hourly", "weekly"):
            notes.append("no timestamp column found")

        rows.append({
            "model_name": name,
            "schema_file": rel_path,
            "module": module,
            "tags": "|".join(str(t) for t in tags_raw) if tags_raw else "",
            "tier": tier,
            "class": cls,
            "timestamp_column": ts_col,
            "has_full_refresh": str(has_fr),
            "has_existing_elementary_tests": str(has_elem),
            "rollout_wave": str(wave),
            "anomaly_enabled": str(anomaly_eligible),
            "kpi_columns": "|".join(kpi_cols),
            "dimension_columns": "|".join(dim_cols),
            "schema_change_enabled": str(schema_change_eligible),
            "notes": "; ".join(notes),
        })

    return rows


def main():
    parser = argparse.ArgumentParser(description="Classify dbt models for Elementary rollout")
    parser.add_argument("--output", type=Path, default=DEFAULT_OUTPUT, help="Output CSV path")
    args = parser.parse_args()

    yaml = YAML()
    yaml.preserve_quotes = True

    schema_files = sorted(MODELS_DIR.rglob("schema.yml"))
    print(f"Found {len(schema_files)} schema.yml files")

    all_rows = []
    for path in schema_files:
        rows = process_schema_file(path, yaml)
        all_rows.extend(rows)

    # Sort by wave, then module, then name
    all_rows.sort(key=lambda r: (int(r["rollout_wave"]), r["module"], r["model_name"]))

    # Write CSV
    args.output.parent.mkdir(parents=True, exist_ok=True)
    with open(args.output, "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=CSV_COLUMNS)
        writer.writeheader()
        writer.writerows(all_rows)

    # Summary
    from collections import Counter
    class_counts = Counter(r["class"] for r in all_rows)
    anomaly_counts = Counter(r["anomaly_enabled"] for r in all_rows)
    wave_counts = Counter(r["rollout_wave"] for r in all_rows)
    fr_counts = Counter(r["has_full_refresh"] for r in all_rows)

    print(f"\nClassified {len(all_rows)} models → {args.output}")
    print(f"\nBy class:")
    for cls, count in sorted(class_counts.items()):
        print(f"  {cls:20s} {count:4d}")
    print(f"\nBy wave:")
    for wave, count in sorted(wave_counts.items()):
        print(f"  Wave {wave:5s} {count:4d}")
    print(f"\nAnomaly enabled:  {anomaly_counts.get('True', 0)}")
    print(f"Anomaly disabled: {anomaly_counts.get('False', 0)}")
    print(f"Has full_refresh: {fr_counts.get('True', 0)}")
    print(f"Manual review:    {class_counts.get('manual_review', 0)}")


if __name__ == "__main__":
    main()

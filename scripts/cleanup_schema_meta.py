#!/usr/bin/env python3
"""Remove schema-gen noise from model meta and normalize owner values.

One-time migration script. Uses ruamel.yaml for round-trip YAML preservation
(comments, key order, formatting). Requires: pip install ruamel.yaml

Usage:
    python scripts/cleanup_schema_meta.py --dry-run   # preview changes
    python scripts/cleanup_schema_meta.py              # apply changes
"""

import argparse
import sys
from pathlib import Path

from ruamel.yaml import YAML

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

MODELS_DIR = Path(__file__).resolve().parent.parent / "models"

# Keys to remove from model meta
KEYS_TO_REMOVE = {"generated_by", "_generated_at", "_generated_fields"}

# All owner variants → analytics_team
OWNER_NORMALIZATION = {
    "analytics-team": "analytics_team",
    "analytics_team": "analytics_team",  # already correct
    "analytics_engineer": "analytics_team",
    "data-team": "analytics_team",
    "analytics": "analytics_team",
    "Data Team": "analytics_team",
}


# ---------------------------------------------------------------------------
# Processing
# ---------------------------------------------------------------------------


def process_meta(meta, model_name: str, stats: dict) -> bool:
    """Clean a single model's meta block. Returns True if modified."""
    changed = False

    # Remove schema-gen noise keys
    for key in list(KEYS_TO_REMOVE):
        if key in meta:
            del meta[key]
            changed = True
            stats["keys_removed"] += 1

    # Normalize owner
    if "owner" in meta:
        current = str(meta["owner"])
        normalized = OWNER_NORMALIZATION.get(current)
        if normalized and current != normalized:
            meta["owner"] = normalized
            changed = True
            stats["owners_normalized"] += 1
            stats["owner_changes"].append(f"  {model_name}: {current!r} → {normalized!r}")

    return changed


def process_file(path: Path, yaml: YAML, dry_run: bool, stats: dict) -> bool:
    """Process a single schema.yml file. Returns True if modified."""
    data = yaml.load(path)
    if data is None:
        return False

    file_changed = False

    # Process models
    models = data.get("models", [])
    if models:
        for model in models:
            name = model.get("name", "<unnamed>")
            meta = model.get("meta")
            if meta and isinstance(meta, dict):
                if process_meta(meta, name, stats):
                    file_changed = True
                    stats["models_modified"] += 1

    # Process sources (source-level meta)
    sources = data.get("sources", [])
    if sources:
        for source in sources:
            source_name = source.get("name", "<unnamed>")
            meta = source.get("meta")
            if meta and isinstance(meta, dict):
                if process_meta(meta, f"source:{source_name}", stats):
                    file_changed = True
                    stats["models_modified"] += 1

            # Source table-level meta
            tables = source.get("tables", [])
            if tables:
                for table in tables:
                    table_name = table.get("name", "<unnamed>")
                    meta = table.get("meta")
                    if meta and isinstance(meta, dict):
                        if process_meta(meta, f"source:{source_name}.{table_name}", stats):
                            file_changed = True
                            stats["models_modified"] += 1

    if file_changed:
        stats["files_modified"] += 1
        if not dry_run:
            yaml.dump(data, path)

    return file_changed


def main():
    parser = argparse.ArgumentParser(description="Clean schema-gen noise from model meta")
    parser.add_argument("--dry-run", action="store_true", help="Preview changes without modifying files")
    args = parser.parse_args()

    yaml = YAML()
    yaml.preserve_quotes = True
    yaml.width = 4096  # prevent line wrapping

    stats = {
        "files_scanned": 0,
        "files_modified": 0,
        "models_modified": 0,
        "keys_removed": 0,
        "owners_normalized": 0,
        "owner_changes": [],
    }

    schema_files = sorted(MODELS_DIR.rglob("schema.yml"))
    print(f"Found {len(schema_files)} schema.yml files")

    if args.dry_run:
        print("DRY RUN — no files will be modified\n")

    for path in schema_files:
        stats["files_scanned"] += 1
        rel = path.relative_to(MODELS_DIR.parent)
        try:
            changed = process_file(path, yaml, args.dry_run, stats)
            if changed:
                print(f"  {'[DRY] ' if args.dry_run else ''}Modified: {rel}")
        except Exception as e:
            print(f"  ERROR processing {rel}: {e}", file=sys.stderr)

    # Summary
    print(f"\n{'DRY RUN ' if args.dry_run else ''}Summary:")
    print(f"  Files scanned:      {stats['files_scanned']}")
    print(f"  Files modified:     {stats['files_modified']}")
    print(f"  Models modified:    {stats['models_modified']}")
    print(f"  Meta keys removed:  {stats['keys_removed']}")
    print(f"  Owners normalized:  {stats['owners_normalized']}")

    if stats["owner_changes"]:
        print(f"\nOwner normalizations:")
        for change in stats["owner_changes"]:
            print(change)


if __name__ == "__main__":
    main()

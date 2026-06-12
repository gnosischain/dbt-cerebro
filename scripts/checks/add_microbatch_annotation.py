#!/usr/bin/env python3
"""Add meta.full_refresh.incremental annotation to every microbatch-tagged decode
model that lacks it, so dbt_incremental_runner can slice catch-up per-day.
Edits each model's schema.yml entry in place (ruamel preserves formatting).
Per-model start_date/batch_months are kept."""
import json, glob, pathlib
from ruamel.yaml import YAML

ROOT = pathlib.Path("/app")
yaml = YAML(); yaml.preserve_quotes = True; yaml.width = 10000

# target set: microbatch-tagged, missing the annotation
m = json.loads((ROOT/"target"/"manifest.json").read_text())
targets = {}
for n in m["nodes"].values():
    if n.get("resource_type") != "model": continue
    cfg = n.get("config", {}); tags = cfg.get("tags") or []
    if "microbatch" not in tags: continue
    fr = (n.get("meta") or {}).get("full_refresh") or (cfg.get("meta") or {}).get("full_refresh") or {}
    if (fr.get("incremental") or {}).get("enabled"): continue
    targets[n["name"]] = n.get("patch_path")  # which schema.yml

names = set(targets)
print(f"targets: {len(names)}")

ANN = {"enabled": True, "date_column": "block_timestamp", "batch_days": 1}
changed_files = 0; changed_models = 0
for sf in glob.glob(str(ROOT/"models"/"**"/"*.yml"), recursive=True):
    p = pathlib.Path(sf)
    try:
        doc = yaml.load(p.read_text())
    except Exception:
        continue
    if not isinstance(doc, dict) or "models" not in doc:
        continue
    dirty = False
    for entry in (doc.get("models") or []):
        if not isinstance(entry, dict) or entry.get("name") not in names:
            continue
        meta = entry.setdefault("meta", {})
        fr = meta.setdefault("full_refresh", {})
        if "incremental" not in fr:
            fr["incremental"] = dict(ANN)
            dirty = True; changed_models += 1
    if dirty:
        yaml.dump(doc, p.open("w")); changed_files += 1

print(f"annotated {changed_models} models across {changed_files} schema.yml files")

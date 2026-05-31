"""Privacy invariant: mixpanel_ga models must never be exposed via cerebro-api.

cerebro-api (see cerebro-api/app/factory.py) decides exposure via two
conditions on the dbt manifest:

  1. model has `production` in its tags AND
  2. model has a tag starting with `api:` (e.g. `api:validators`)
  3. UNLESS `meta.api.exclude_from_api == True` overrides it.

mixpanel_ga contains user-behavior data and must remain internal. This
test enforces, for every model whose file path is under `models/mixpanel_ga/`:

  - No tag starting with `api:` is present (no accidental opt-in), AND
  - `meta.api.exclude_from_api` resolves to True (defense-in-depth).

Run with: `dbt parse && pytest tests/test_mixpanel_privacy.py`
"""
from __future__ import annotations

import json
import os
from pathlib import Path

import pytest


MANIFEST_PATH = Path(__file__).resolve().parent.parent / "target" / "manifest.json"

# Models outside mixpanel_ga/ that ALSO must never be exposed via cerebro-api
# because they join hashed user identifiers (user_pseudonym / user_id_hash) to
# plaintext addresses (wallet EOAs or Gnosis Pay Safe addresses).
IDENTITY_BRIDGE_MODELS = {
    "int_execution_gnosis_app_user_identity_bridge",
    "int_execution_gnosis_app_user_identities",
    "int_execution_gpay_user_identity_bridge",
    "int_execution_gpay_safe_identities",
}

# Mixpanel models that are aggregate-only-blocked from cerebro-api but
# additionally blocked from MCP because of per-user grain. Must carry
# `meta.expose_to_mcp: false`.
MCP_BLOCKED_MIXPANEL_MODELS = {
    "api_mixpanel_ga_users_daily",
    "int_mixpanel_ga_user_acquisition",
    "int_mixpanel_ga_gpay_first_events",
    "int_mixpanel_ga_gnosis_app_first_events",
}


def _load_manifest():
    if not MANIFEST_PATH.exists():
        pytest.skip(
            f"manifest not found at {MANIFEST_PATH}; run `dbt parse` first"
        )
    with MANIFEST_PATH.open() as fh:
        return json.load(fh)


def _mixpanel_model_nodes(manifest):
    for unique_id, node in manifest.get("nodes", {}).items():
        if node.get("resource_type") != "model":
            continue
        path = node.get("original_file_path", "")
        if path.startswith("models/mixpanel_ga/"):
            yield unique_id, node


def test_mixpanel_models_have_no_api_tag():
    """No model under models/mixpanel_ga/ may carry an `api:*` tag."""
    manifest = _load_manifest()
    offenders = []
    for unique_id, node in _mixpanel_model_nodes(manifest):
        tags = node.get("tags", []) or []
        bad = [t for t in tags if isinstance(t, str) and t.startswith("api:")]
        if bad:
            offenders.append((unique_id, bad))
    assert not offenders, (
        "mixpanel_ga models must not have api:* tags (would expose internal "
        f"analytics data via cerebro-api). Offenders: {offenders}"
    )


def test_mixpanel_models_have_exclude_from_api_true():
    """Every model under models/mixpanel_ga/ must have meta.api.exclude_from_api=True."""
    manifest = _load_manifest()
    offenders = []
    for unique_id, node in _mixpanel_model_nodes(manifest):
        meta = (node.get("config", {}) or {}).get("meta", {}) or {}
        api_meta = meta.get("api", {}) or {}
        if api_meta.get("exclude_from_api") is not True:
            offenders.append((unique_id, api_meta))
    assert not offenders, (
        "mixpanel_ga models missing meta.api.exclude_from_api=true. "
        f"Check dbt_project.yml `models.gnosis_dbt.mixpanel_ga` block and "
        f"per-model schema.yml entries. Offenders: {offenders}"
    )


def test_mixpanel_models_present_in_manifest():
    """Sanity: ensure the path filter actually matches some models."""
    manifest = _load_manifest()
    count = sum(1 for _ in _mixpanel_model_nodes(manifest))
    assert count >= 10, (
        f"expected at least 10 mixpanel_ga models, found {count} — "
        "the path filter may be stale"
    )


def _named_nodes(manifest, names):
    for unique_id, node in manifest.get("nodes", {}).items():
        if node.get("resource_type") != "model":
            continue
        if node.get("name") in names:
            yield unique_id, node


def test_identity_bridge_models_excluded_from_api():
    """Bridges that map hashed user IDs to plaintext addresses must be excluded."""
    manifest = _load_manifest()
    seen = set()
    offenders = []
    for unique_id, node in _named_nodes(manifest, IDENTITY_BRIDGE_MODELS):
        seen.add(node["name"])
        meta = (node.get("config", {}) or {}).get("meta", {}) or {}
        api_meta = meta.get("api", {}) or {}
        if api_meta.get("exclude_from_api") is not True:
            offenders.append((unique_id, api_meta))
    missing = IDENTITY_BRIDGE_MODELS - seen
    assert not missing, (
        f"expected identity-bridge models not found in manifest: {missing} — "
        "rename or list-update may be needed"
    )
    assert not offenders, (
        "identity-bridge models missing meta.api.exclude_from_api=true. "
        f"These join hashed user IDs to addresses and must not be served. "
        f"Offenders: {offenders}"
    )


def test_mcp_blocked_mixpanel_models_have_expose_to_mcp_false():
    """Per-user-grain mixpanel models must carry meta.expose_to_mcp=false."""
    manifest = _load_manifest()
    seen = set()
    offenders = []
    for unique_id, node in _named_nodes(manifest, MCP_BLOCKED_MIXPANEL_MODELS):
        seen.add(node["name"])
        meta = (node.get("config", {}) or {}).get("meta", {}) or {}
        if meta.get("expose_to_mcp") is not False:
            offenders.append((unique_id, meta.get("expose_to_mcp")))
    missing = MCP_BLOCKED_MIXPANEL_MODELS - seen
    assert not missing, (
        f"expected MCP-blocked mixpanel models not found in manifest: {missing}"
    )
    assert not offenders, (
        "MCP-blocked mixpanel models must have meta.expose_to_mcp=false. "
        f"Offenders: {offenders}"
    )


def test_identity_bridge_models_have_no_api_tag():
    """No identity-bridge model may carry an api:* tag (defensive)."""
    manifest = _load_manifest()
    offenders = []
    for unique_id, node in _named_nodes(manifest, IDENTITY_BRIDGE_MODELS):
        tags = node.get("tags", []) or []
        bad = [t for t in tags if isinstance(t, str) and t.startswith("api:")]
        if bad:
            offenders.append((unique_id, bad))
    assert not offenders, (
        f"identity-bridge models must not have api:* tags. Offenders: {offenders}"
    )

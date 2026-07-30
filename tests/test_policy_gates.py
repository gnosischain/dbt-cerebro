"""Contract tests for the policy gates and agent-context facts.

These fixtures ARE the policy contracts: each test encodes exactly what a gate
must reject or accept, including the four acceptance violations the CI pipeline
must catch before any image publishes:
  (a) a new delete+insert incremental          -> no_delete_insert
  (b) an untagged api_ model                   -> check_api_tags missing_api_tag
  (c) a tracked mart missing semantic authoring -> scaffold gate
  (d) a staged model with a literal/unsafe insert_overwrite strategy
                                               -> no_delete_insert staged rule

Run with `python -m pytest tests/test_policy_gates.py` from the repo root
(the `scripts.*` namespace imports need the root on sys.path).
"""

from __future__ import annotations

import subprocess
from pathlib import Path

import pytest

from scripts.agent_context import build_agent_context as bac
from scripts.agent_context import check as impact_check
from scripts.agent_context import context as ctx
from scripts.agent_context.strategy import analyze_strategy
from scripts.checks import check_api_tags
from scripts.checks import check_meta_keys
from scripts.checks import no_delete_insert
from scripts.semantic import scaffold_candidates


# ---------------------------------------------------------------------------
# Synthetic manifest helpers
# ---------------------------------------------------------------------------

SAFE_STAGED_RAW = """
{% set start_month = var('start_month', none) %}
{{
    config(
        materialized='incremental',
        incremental_strategy=('append' if start_month else 'insert_overwrite'),
        partition_by='toStartOfMonth(date)',
        tags=["production", "microbatch"]
    )
}}
SELECT 1
"""

LITERAL_OVERWRITE_RAW = """
{{
    config(
        materialized='incremental',
        incremental_strategy='insert_overwrite',
        partition_by='toStartOfMonth(date)'
    )
}}
SELECT 1
"""

UNSAFE_SCOPED_RAW = """
{% set start_month = var('start_month', none) %}
{{
    config(
        materialized='incremental',
        incremental_strategy=('delete+insert' if start_month else 'insert_overwrite'),
        partition_by='toStartOfMonth(date)'
    )
}}
SELECT 1
"""


def make_node(
    name,
    materialized="view",
    strategy=None,
    partition_by=None,
    tags=(),
    meta=None,
    config_meta=None,
    raw_code="SELECT 1",
    package="gnosis_dbt",
    columns=None,
    path=None,
    description="A model.",
):
    uid = f"model.{package}.{name}"
    config = {"materialized": materialized, "tags": list(tags)}
    if strategy is not None:
        config["incremental_strategy"] = strategy
    if partition_by is not None:
        config["partition_by"] = partition_by
    if config_meta is not None:
        config["meta"] = config_meta
    return {
        "unique_id": uid,
        "resource_type": "model",
        "package_name": package,
        "name": name,
        "fqn": [package, "execution", name],
        "original_file_path": path or f"models/execution/{name}.sql",
        "config": config,
        "tags": list(tags),
        "meta": meta or {},
        "raw_code": raw_code,
        "columns": columns or {},
        "checksum": {"checksum": f"sum-{name}"},
        "description": description,
    }


def make_manifest(*nodes, child_map=None):
    return {
        "metadata": {"project_name": "gnosis_dbt"},
        "nodes": {n["unique_id"]: n for n in nodes},
        "child_map": child_map or {},
    }


STAGED_META = {"full_refresh": {"stages": [{"name": "2024"}, {"name": "2025"}]}}


# ---------------------------------------------------------------------------
# analyze_strategy — raw-code detection (the manifest value is parse-resolved)
# ---------------------------------------------------------------------------

class TestAnalyzeStrategy:
    def test_literal_single_quotes(self):
        info = analyze_strategy(LITERAL_OVERWRITE_RAW)
        assert info["assigned"] and not info["expression"]
        assert info["literal"] == "insert_overwrite"

    def test_literal_double_quotes(self):
        info = analyze_strategy('config(incremental_strategy="append")')
        assert info["literal"] == "append"

    def test_safe_scoped_expression(self):
        info = analyze_strategy(SAFE_STAGED_RAW)
        assert info["expression"] is True
        assert info["literal"] is None
        assert info["scoped_branch"] == "append"
        assert info["scoped_append"] is True

    def test_unsafe_scoped_expression(self):
        info = analyze_strategy(UNSAFE_SCOPED_RAW)
        assert info["expression"] is True
        assert info["scoped_branch"] == "delete+insert"
        assert info["scoped_append"] is False

    def test_negated_condition_flips_branch(self):
        raw = "config(incremental_strategy=('insert_overwrite' if not start_month else 'append'))"
        info = analyze_strategy(raw)
        assert info["scoped_branch"] == "append"
        assert info["scoped_append"] is True

    def test_no_assignment(self):
        info = analyze_strategy("{{ config(materialized='view') }} SELECT 1")
        assert info["assigned"] is False
        assert info["expression"] is False

    def test_non_scope_conditional_is_undeterminable(self):
        raw = "config(incremental_strategy=('append' if some_flag else 'insert_overwrite'))"
        info = analyze_strategy(raw)
        assert info["expression"] is True
        assert info["scoped_append"] is None


# ---------------------------------------------------------------------------
# no_delete_insert — incremental policy incl. the staged-strategy rule
# ---------------------------------------------------------------------------

def ndi_rules(manifest, allow=frozenset()):
    violations, _used = no_delete_insert.find_violations(manifest, set(allow))
    return {(uid, rule) for uid, rule, _msg in violations}


class TestIncrementalPolicy:
    def test_delete_insert_rejected(self):
        # Acceptance violation (a)
        n = make_node("int_bad_di", materialized="incremental", strategy="delete+insert")
        assert (n["unique_id"], "delete_insert") in ndi_rules(make_manifest(n))

    def test_overwrite_without_partition_rejected(self):
        n = make_node("int_bad_ow", materialized="incremental", strategy="insert_overwrite")
        assert (n["unique_id"], "overwrite_no_partition") in ndi_rules(make_manifest(n))

    def test_append_without_microbatch_rejected(self):
        n = make_node("int_bad_ap", materialized="incremental", strategy="append")
        assert (n["unique_id"], "append_no_microbatch") in ndi_rules(make_manifest(n))

    def test_append_with_microbatch_passes(self):
        n = make_node("int_ok_ap", materialized="incremental", strategy="append",
                      tags=("microbatch",))
        assert ndi_rules(make_manifest(n)) == set()

    def test_plain_uid_allowlist_suppresses_all_rules(self):
        n = make_node("int_bad_di", materialized="incremental", strategy="delete+insert")
        assert ndi_rules(make_manifest(n), allow={n["unique_id"]}) == set()

    def test_rule_scoped_allowlist_suppresses_only_that_rule(self):
        n = make_node("int_bad_di", materialized="incremental", strategy="delete+insert")
        allow = {f"{n['unique_id']}::append_no_microbatch"}
        assert (n["unique_id"], "delete_insert") in ndi_rules(make_manifest(n), allow)

    def test_used_allow_entries_are_reported(self):
        n = make_node("int_bad_di", materialized="incremental", strategy="delete+insert")
        _violations, used = no_delete_insert.find_violations(
            make_manifest(n), {n["unique_id"], "model.gnosis_dbt.never_used"}
        )
        assert used == {n["unique_id"]}

    def test_staged_literal_overwrite_rejected(self):
        # Acceptance violation (d): meta.full_refresh stages + literal insert_overwrite
        n = make_node(
            "int_staged_bad", materialized="incremental", strategy="insert_overwrite",
            partition_by="toStartOfMonth(date)", meta=STAGED_META,
            raw_code=LITERAL_OVERWRITE_RAW,
        )
        assert (n["unique_id"], "staged_literal_overwrite") in ndi_rules(make_manifest(n))

    def test_staged_inherited_overwrite_rejected(self):
        # No in-file assignment; project default resolves to insert_overwrite.
        n = make_node(
            "int_staged_inherit", materialized="incremental", strategy="insert_overwrite",
            partition_by="toStartOfMonth(date)", meta=STAGED_META,
            raw_code="{{ config(materialized='incremental') }} SELECT 1",
        )
        assert (n["unique_id"], "staged_literal_overwrite") in ndi_rules(make_manifest(n))

    def test_staged_safe_expression_passes(self):
        # The wipe-safe pattern: resolved value is insert_overwrite, but the raw
        # expression sends scoped (start_month) runs down the append path.
        n = make_node(
            "int_staged_ok", materialized="incremental", strategy="insert_overwrite",
            partition_by="toStartOfMonth(date)", meta=STAGED_META,
            raw_code=SAFE_STAGED_RAW,
        )
        assert ndi_rules(make_manifest(n)) == set()

    def test_staged_unsafe_scoped_branch_rejected(self):
        n = make_node(
            "int_staged_scoped_bad", materialized="incremental",
            strategy="insert_overwrite", partition_by="toStartOfMonth(date)",
            meta=STAGED_META, raw_code=UNSAFE_SCOPED_RAW,
        )
        assert (n["unique_id"], "staged_scoped_branch") in ndi_rules(make_manifest(n))

    def test_staged_meta_in_config_also_detected(self):
        n = make_node(
            "int_staged_cfgmeta", materialized="incremental", strategy="insert_overwrite",
            partition_by="toStartOfMonth(date)", config_meta=STAGED_META,
            raw_code=LITERAL_OVERWRITE_RAW,
        )
        assert (n["unique_id"], "staged_literal_overwrite") in ndi_rules(make_manifest(n))

    def test_third_party_and_non_incremental_skipped(self):
        a = make_node("elementary_thing", materialized="incremental",
                      strategy="delete+insert", package="elementary")
        b = make_node("api_some_view", materialized="view", strategy="delete+insert")
        assert ndi_rules(make_manifest(a, b)) == set()


# ---------------------------------------------------------------------------
# check_api_tags — api_* naming rule with meta.api.exclude_from_api semantics
# ---------------------------------------------------------------------------

def api_violation_rules(manifest, allow=frozenset()):
    violations, _used = check_api_tags.run_checks(manifest, set(allow))
    return {v.split("[", 1)[1].split("]", 1)[0] for v in violations}


def valid_api_node(name="api_good_daily_thing", **overrides):
    kwargs = dict(
        materialized="view",
        tags=("production", "api:good_thing", "granularity:daily", "tier1"),
        columns={
            "date": {"data_type": "Date"},
            "value": {"data_type": "UInt64"},
        },
    )
    kwargs.update(overrides)
    return make_node(name, **kwargs)


class TestApiNamingRule:
    def test_untagged_api_model_rejected(self):
        # Acceptance violation (b)
        n = make_node("api_bad_probe", tags=("production",))
        assert "missing_api_tag" in api_violation_rules(make_manifest(n))

    def test_meta_opt_out_is_permanently_fine(self):
        n = make_node("api_internal_thing", tags=("production",),
                      meta={"api": {"exclude_from_api": True}})
        assert api_violation_rules(make_manifest(n)) == set()

    def test_config_meta_opt_out_also_honored(self):
        n = make_node("api_internal_cfg", tags=("production",),
                      config_meta={"api": {"exclude_from_api": True}})
        assert api_violation_rules(make_manifest(n)) == set()

    def test_api_tagged_but_not_production_rejected(self):
        n = valid_api_node(tags=("api:good_thing", "granularity:daily", "tier1"))
        assert "api_not_production" in api_violation_rules(make_manifest(n))

    def test_allowlist_suppresses_and_is_marked_used(self):
        n = make_node("api_backlog_model", tags=("production",))
        allow = {"api_backlog_model::missing_api_tag"}
        violations, used = check_api_tags.run_checks(make_manifest(n), set(allow))
        assert violations == []
        assert used == allow

    def test_fully_conventional_api_model_passes(self):
        assert api_violation_rules(make_manifest(valid_api_node())) == set()

    def test_non_api_named_models_ignored_by_naming_rule(self):
        n = make_node("int_plain_model", tags=("production",))
        assert api_violation_rules(make_manifest(n)) == set()


# ---------------------------------------------------------------------------
# check_meta_keys — generator-noise denylist (NOT a whitelist: real contracts
# like privacy_tier / api.exclude_from_api / expose_to_mcp must pass)
# ---------------------------------------------------------------------------

class TestMetaKeys:
    def test_generator_noise_rejected(self):
        n = make_node("fct_noisy", meta={"generated_by": "dbt-schema-gen",
                                         "_generated_at": "2025-01-01"})
        violations, _counts = check_meta_keys.find_violations(make_manifest(n))
        assert {(name, key) for name, key in violations} == {
            ("fct_noisy", "_generated_at"), ("fct_noisy", "generated_by"),
        }

    def test_legitimate_contract_keys_pass(self):
        n = make_node("fct_fine", meta={
            "owner": "analytics_team", "privacy_tier": "mixpanel",
            "api": {"exclude_from_api": True}, "expose_to_mcp": False,
            "agent": {"grain": "one row per day"}, "full_refresh": {"stages": []},
        })
        violations, counts = check_meta_keys.find_violations(make_manifest(n))
        assert violations == []
        assert counts["owner"] == 1 and counts["privacy_tier"] == 1

    def test_config_meta_also_scanned(self):
        n = make_node("fct_cfg_noisy", config_meta={"_generated_fields": ["a"]})
        violations, _counts = check_meta_keys.find_violations(make_manifest(n))
        assert violations == [("fct_cfg_noisy", "_generated_fields")]


# ---------------------------------------------------------------------------
# scaffold gate — tracked marts must have semantic authoring (ratcheted)
# ---------------------------------------------------------------------------

class TestScaffoldGate:
    def make_tracked(self):
        return make_manifest(
            make_node("api_unauthored_daily"),
            make_node("fct_unauthored"),
            make_node("int_unauthored"),
            make_node("stg_ignored"),
            make_node("api_foreign", package="elementary"),
        )

    def test_find_missing_returns_tracked_unauthored(self):
        # Acceptance violation (c): a tracked mart with no semantic authoring
        missing = scaffold_candidates.find_missing(self.make_tracked(), {})
        assert {n["name"] for n in missing} == {
            "api_unauthored_daily", "fct_unauthored", "int_unauthored",
        }

    def test_authored_models_not_missing(self):
        authored = {"api_unauthored_daily": {"name": "api_unauthored_daily"}}
        missing = scaffold_candidates.find_missing(self.make_tracked(), authored)
        assert "api_unauthored_daily" not in {n["name"] for n in missing}

    def test_gate_violations_new_and_stale(self):
        new, stale = scaffold_candidates.gate_violations(
            missing={"api_a", "fct_b"}, allow={"fct_b", "fct_gone"}
        )
        assert new == {"api_a"}
        assert stale == {"fct_gone"}


# ---------------------------------------------------------------------------
# build_agent_context — facts normalization, guides, lineage, privacy
# ---------------------------------------------------------------------------

class TestModelFacts:
    def test_non_incremental_models_carry_no_strategy(self):
        # dbt_project.yml sets +incremental_strategy project-wide, so resolved
        # config carries it even on views — the fact must be gated.
        n = make_node("api_a_view", materialized="view", strategy="insert_overwrite")
        assert bac.model_facts(n)["incremental_strategy"] is None

    def test_incremental_models_keep_strategy(self):
        n = make_node("int_inc", materialized="incremental", strategy="insert_overwrite")
        assert bac.model_facts(n)["incremental_strategy"] == "insert_overwrite"

    def test_strategy_expression_detected_from_raw_code(self):
        n = make_node("int_expr", materialized="incremental",
                      strategy="insert_overwrite", raw_code=SAFE_STAGED_RAW)
        assert bac.model_facts(n)["strategy_expression"] is True

    def test_literal_strategy_is_not_an_expression(self):
        n = make_node("int_lit", materialized="incremental",
                      strategy="insert_overwrite", raw_code=LITERAL_OVERWRITE_RAW)
        assert bac.model_facts(n)["strategy_expression"] is False


class TestGuideAccumulation:
    def test_scalar_guides_accumulate_across_layers(self):
        contract = {}
        bac.merge_layer(contract, {"agents_md": "models/contracts/AGENTS.md"})
        bac.merge_layer(contract, {"agents_md": "scripts/full_refresh/AGENTS.md"})
        assert contract["agents_md"] == [
            "models/contracts/AGENTS.md", "scripts/full_refresh/AGENTS.md",
        ]

    def test_list_input_and_dedup(self):
        contract = {}
        bac.merge_layer(contract, {"agents_md": ["a.md", "b.md"]})
        bac.merge_layer(contract, {"agents_md": "a.md"})
        assert contract["agents_md"] == ["a.md", "b.md"]


class TestTransitiveLineage:
    def test_transitive_api_descendants_found(self):
        child_map = {
            "model.gnosis_dbt.int_root": ["model.gnosis_dbt.int_mid"],
            "model.gnosis_dbt.int_mid": ["model.gnosis_dbt.api_leaf"],
            "model.gnosis_dbt.api_leaf": [],
        }
        desc = bac.transitive_descendants("model.gnosis_dbt.int_root", child_map, {})
        assert desc == {"model.gnosis_dbt.int_mid", "model.gnosis_dbt.api_leaf"}

    def test_non_model_children_excluded_and_cycles_safe(self):
        child_map = {
            "model.gnosis_dbt.a": ["model.gnosis_dbt.b", "test.gnosis_dbt.t1"],
            "model.gnosis_dbt.b": ["model.gnosis_dbt.a"],
        }
        desc = bac.transitive_descendants("model.gnosis_dbt.a", child_map, {})
        assert desc == {"model.gnosis_dbt.b", "model.gnosis_dbt.a"}


class TestIsPublic:
    def facts_for(self, node):
        return bac.model_facts(node)

    def test_direct_expose_to_mcp_false_is_private(self):
        n = make_node("int_private", meta={"expose_to_mcp": False})
        assert bac.is_public(self.facts_for(n), n) is False

    def test_nested_semantic_expose_false_is_private(self):
        n = make_node("int_private2", config_meta={"semantic": {"expose_to_mcp": False}})
        assert bac.is_public(self.facts_for(n), n) is False

    def test_privacy_tag_is_private(self):
        n = make_node("int_mixpanel", tags=("privacy:mixpanel_ga",))
        assert bac.is_public(self.facts_for(n), n) is False

    def test_default_is_public(self):
        n = make_node("int_open")
        assert bac.is_public(self.facts_for(n), n) is True


class TestInputsFingerprint:
    def make_repo(self, tmp_path, lesson_body="body one"):
        (tmp_path / "agent_context").mkdir()
        (tmp_path / "agent_context" / "profiles.yml").write_text("version: 1\n")
        (tmp_path / "docs" / "lessons").mkdir(parents=True)
        (tmp_path / "docs" / "lessons" / "a-lesson.md").write_text(lesson_body)
        (tmp_path / "AGENTS.md").write_text("root guide")
        return tmp_path

    def test_fingerprint_changes_with_lesson_content(self, tmp_path):
        repo = self.make_repo(tmp_path)
        fp1 = bac.compute_inputs_fingerprint("hash", repo)
        (repo / "docs" / "lessons" / "a-lesson.md").write_text("body two")
        fp2 = bac.compute_inputs_fingerprint("hash", repo)
        assert fp1 != fp2

    def test_fingerprint_changes_with_models_hash(self, tmp_path):
        repo = self.make_repo(tmp_path)
        assert bac.compute_inputs_fingerprint("h1", repo) != bac.compute_inputs_fingerprint("h2", repo)

    def test_fingerprint_stable(self, tmp_path):
        repo = self.make_repo(tmp_path)
        assert bac.compute_inputs_fingerprint("h", repo) == bac.compute_inputs_fingerprint("h", repo)


# ---------------------------------------------------------------------------
# check.py — base-ref failure must be able to fail closed
# ---------------------------------------------------------------------------

class TestBaseRefHandling:
    def test_require_base_fails_closed(self, monkeypatch):
        def boom(base_ref):
            raise subprocess.CalledProcessError(128, ["git", "diff"])
        monkeypatch.setattr(impact_check, "changed_model_files", boom)
        with pytest.raises(SystemExit):
            impact_check.get_changed_models("bogus-ref", require_base=True)

    def test_without_require_base_warns_and_continues(self, monkeypatch):
        def boom(base_ref):
            raise subprocess.CalledProcessError(128, ["git", "diff"])
        monkeypatch.setattr(impact_check, "changed_model_files", boom)
        assert impact_check.get_changed_models("bogus-ref", require_base=False) == []


# ---------------------------------------------------------------------------
# check.py — deleted model files must not trip the unknown-model block
# ---------------------------------------------------------------------------

class TestDeletedModelHandling:
    def test_deleted_paths_are_partitioned_out(self, tmp_path, monkeypatch):
        monkeypatch.setattr(impact_check, "REPO_ROOT", tmp_path)
        kept = Path("models/revenue/marts/kept_model.sql")
        gone = Path("models/revenue/marts/deleted_model.sql")
        (tmp_path / kept).parent.mkdir(parents=True)
        (tmp_path / kept).write_text("select 1")
        present, deleted = impact_check.partition_existing([kept, gone])
        assert present == [kept]
        assert deleted == [gone]

    def test_all_present_when_nothing_deleted(self, tmp_path, monkeypatch):
        monkeypatch.setattr(impact_check, "REPO_ROOT", tmp_path)
        kept = Path("models/a.sql")
        (tmp_path / kept).parent.mkdir(parents=True, exist_ok=True)
        (tmp_path / kept).write_text("select 1")
        present, deleted = impact_check.partition_existing([kept])
        assert present == [kept] and deleted == []


# ---------------------------------------------------------------------------
# context.py — artifact staleness must consider inputs, not just existence
# ---------------------------------------------------------------------------

class TestArtifactStaleness:
    def test_newer_input_marks_stale(self, tmp_path):
        artifact = tmp_path / "agent_context.json"
        artifact.write_text("{}")
        inp = tmp_path / "profiles.yml"
        inp.write_text("v1")
        import os
        past = artifact.stat().st_mtime - 100
        os.utime(artifact, (past, past))
        assert ctx.artifact_is_stale(artifact, [inp]) is True

    def test_older_inputs_are_fresh(self, tmp_path):
        inp = tmp_path / "profiles.yml"
        inp.write_text("v1")
        artifact = tmp_path / "agent_context.json"
        artifact.write_text("{}")
        import os
        future = inp.stat().st_mtime + 100
        os.utime(artifact, (future, future))
        assert ctx.artifact_is_stale(artifact, [inp]) is False

    def test_missing_inputs_ignored(self, tmp_path):
        artifact = tmp_path / "agent_context.json"
        artifact.write_text("{}")
        assert ctx.artifact_is_stale(artifact, [tmp_path / "nope.yml"]) is False

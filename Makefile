# Local gates — mirrors the CI enforcement in .github/workflows/build-and-release.yaml
# so an agent (or human) gets the same signal BEFORE handing work back, not after
# merge. See AGENTS.md "Required workflow".
#
# Targets:
#   make check-fast     static gates only (no warehouse, no manifest needed)
#   make check          everything CI runs (requires target/manifest.json etc. —
#                       run `make manifest` first if stale; needs the dbt container)
#   make agent-context  rebuild target/agent_context.json from the manifest
#   make context M=<model> [T=build|fix|backfill|review]   print a change packet
#   make impact [BASE=main]   change-aware contract/impact gate

PY ?= python3
BASE ?= main
T ?= fix

.PHONY: check-fast check manifest agent-context context impact

check-fast:
	$(PY) scripts/checks/no_delete_insert.py
	$(PY) scripts/checks/check_api_tags.py
	$(PY) scripts/checks/check_doc_coverage.py

# dbt runs inside the `dbt` container in this setup; parse refreshes
# target/manifest.json (bind-mounted back to the host checkout).
manifest:
	docker exec dbt bash -lc "cd /app && dbt parse --no-partial-parse"

agent-context:
	$(PY) scripts/agent_context/build_agent_context.py

context:
	$(PY) scripts/agent_context/context.py --select $(M) --task $(T)

impact:
	$(PY) scripts/agent_context/check.py --base-ref $(BASE) --skip-static

check: check-fast agent-context
	$(PY) scripts/agent_context/check.py --base-ref $(BASE) --skip-static
	$(PY) scripts/semantic/build_registry.py --target-dir target --validate --max-warnings 0
	$(PY) scripts/semantic/graph_gate.py --target-dir target
	$(PY) scripts/semantic/generate_entities.py --target-dir target --check
	$(PY) -m pytest tests/test_semantic_registry.py tests/test_run_state.py -q

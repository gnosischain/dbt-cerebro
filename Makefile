# Local gates — thin aliases over scripts/checks/run_all.py, THE vendor-neutral
# verification command (mirrors CI; works in the dbt container without make).
# See AGENTS.md "Required workflow".
#
# Targets:
#   make check-fast     static manifest gates (no warehouse; target/manifest.json
#                       is auto-bootstrapped via `dbt parse` when dbt is installed,
#                       otherwise the runner prints the docker command)
#   make check          everything local: static gates + agent context (build,
#                       determinism, change-aware contract gate) + pytest + the
#                       semantic registry/graph/entity gates (these last need
#                       target/catalog.json from a warehouse-connected
#                       `dbt docs generate` — run `make manifest` / docs in the
#                       dbt container first if stale)
#   make manifest       refresh target/manifest.json via the dbt container
#   make agent-context  rebuild target/agent_context.json from the manifest
#   make context M=<model> [T=build|fix|backfill|review]   print a change packet
#   make impact [BASE=main]   change-aware contract/impact gate

PY ?= python3
BASE ?= main
T ?= fix

.PHONY: check-fast check manifest agent-context context impact

check-fast:
	$(PY) scripts/checks/run_all.py --fast

check:
	$(PY) scripts/checks/run_all.py --full --base-ref $(BASE)

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

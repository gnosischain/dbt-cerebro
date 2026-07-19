# AGENTS.md — how to work in this repo

dbt project on ClickHouse Cloud modeling Gnosis Chain data. Layers: `stg_` (views over
sources) → `int_` (intermediate, mostly incremental) → `fct_`/`api_` marts. `api_` models
are served by the Cerebro API/MCP and consumed by the metrics-dashboard repo. Counts,
model lists, and class membership are always derived from `target/manifest.json` — do not
trust numbers quoted in prose (including any doc in this repo).

Scoped guides with domain-specific rules live next to the code:
[models/contracts/AGENTS.md](models/contracts/AGENTS.md),
[models/execution/AGENTS.md](models/execution/AGENTS.md),
[models/execution/gnosis_app_gt/AGENTS.md](models/execution/gnosis_app_gt/AGENTS.md),
[models/revenue/AGENTS.md](models/revenue/AGENTS.md),
[models/consensus/AGENTS.md](models/consensus/AGENTS.md),
[models/mixpanel_ga/AGENTS.md](models/mixpanel_ga/AGENTS.md),
[models/quarterly_data/AGENTS.md](models/quarterly_data/AGENTS.md),
[models/celo/AGENTS.md](models/celo/AGENTS.md),
[models/bridges/AGENTS.md](models/bridges/AGENTS.md),
[scripts/refresh/AGENTS.md](scripts/refresh/AGENTS.md),
[scripts/full_refresh/AGENTS.md](scripts/full_refresh/AGENTS.md).
Read the one covering the directory you are changing; domains without a guide
follow the root rules here.

Repeatable workflows (vendor-neutral; Claude slash commands are thin wrappers):
[docs/workflows/new-model.md](docs/workflows/new-model.md),
[docs/workflows/generate-schema.md](docs/workflows/generate-schema.md),
[docs/workflows/refresh-advisor.md](docs/workflows/refresh-advisor.md),
[docs/workflows/incident.md](docs/workflows/incident.md).

## Required workflow for any model change

1. **Gather context** — `python scripts/agent_context/context.py --select <model> --task
   <build|fix|backfill|review>` prints the model's change packet (contract, hazards,
   lineage, validation selectors). Until you have run it, do not assume you know a
   model's failure modes. Also skim `docs/lessons/INDEX.md` for the mistake classes.
2. **Inspect grain and lineage** — schema.yml (grain, `meta`), `dbt ls -s +<model>+`.
3. **Identify incremental/backfill behavior before running anything** — materialization,
   `incremental_strategy`, `partition_by`, `meta.full_refresh` stages, and whether any
   downstream model reads `{{ this }}` (cumulative — backfill ordering matters).
4. **Implement**, following the rules below and the scoped AGENTS.md.
5. **Validate** — `python scripts/checks/run_all.py` is THE command (works from a
   fresh checkout and inside the dbt container; `make check-fast` / `make check` are
   thin aliases for its `--fast` / `--full` modes); plus the model-specific selectors
   from the change packet.
6. **Record new lessons** — if you diagnosed a new mistake class, add a record under
   `docs/lessons/` (follow `docs/workflows/incident.md`; every lesson needs evidence
   refs).

## Refresh levers — which tool, when

| Situation | Lever |
|---|---|
| Daily forward catch-up (production hot path) | `scripts/refresh/dbt_incremental_runner.py` — refuses stages with too many missing slices; cannot seed an empty table; **does not recover backfilled history** |
| Raw source was backfilled into an already-passed month (decode chains) | `scripts/refresh/gap_window_refresh.py --months … --select <decode>+` — drops the gap-month partition to lower the watermark, then re-runs scoped |
| Multi-month / full-history rebuild of a large model | `scripts/full_refresh/refresh.py` with the model's `meta.full_refresh` stages — batch 1 `--full-refresh` recreates, later stages append non-overlapping |
| One-off small model or smoke test | plain `dbt run -s <model>` |
| Force re-decode ignoring the watermark | `dbt run --full-refresh -s <model>` (re-reads the full raw source) |

Footnotes that have burned hours:
- Refresh runs have per-run state. Never assume a pending `--resume` can survive starting
  a different selection — check for pending state first (see `scripts/full_refresh/AGENTS.md`).
- Table-materialized models whose SQL branches on `start_month`/`end_month` are rebuilt
  whole by every batch — a batched refresh leaves them holding only the **last** batch.
  A plain `dbt run` self-heals them.

## Non-negotiable modeling rules

- **Never a wide `delete+insert`.** The delete is a lightweight mutation that keeps
  running in the background after dbt reports failure — it can silently wipe the window
  while the insert never runs. Reprocess per `slice`/partition instead. CI gate:
  `scripts/checks/no_delete_insert.py`.
- **`insert_overwrite` partition grain must equal the overwrite grain.** It REPLACEs
  whole partitions, rejects `unique_key`, and must never be combined with `slice` vars or
  staged `meta.full_refresh` batches — staged models use an append-if-`start_month`
  strategy expression instead (see `docs/lessons/staged-insert-overwrite-wipe.md`).
- **Backfill ordering:** if any downstream model reads `{{ this }}`, it is cumulative —
  backfill history first, chronologically, before advancing it. Stateless downstreams can
  be refreshed after. Classify with `grep -rl '{{ this }}' models/`.
- **Hooks come in pairs.** Whatever a `pre_hook` turns on, a `post_hook` turns back off.
  Query-level knobs (`max_threads`, `max_memory_usage`, spill settings) go in
  `query_settings=`; `settings=` is storage/DDL only.
- **Meta contract** — model `meta` keys in active use (derive the census with
  `python scripts/checks/check_meta_keys.py`): `owner`, `authoritative`,
  `full_refresh`, `inference_notes`, `agent` (schema in `agent_context/profiles.yml`),
  `api` (e.g. `api.exclude_from_api` for intentionally-internal `api_*` models),
  `privacy_tier`, `expose_to_mcp` (MCP opt-out, direct or under `semantic`),
  `grain`, `guard`. Don't invent new keys; generator bookkeeping
  (`generated_by`, `_generated_at`, `_generated_fields`) is CI-banned.
- **Tag contract** — `api:<endpoint>` models must carry `granularity:*` and a tier tag
  and a complete typed column schema. CI gate: `scripts/checks/check_api_tags.py`.
- **A model is not just its SQL.** Adding, renaming, or retiring a model with metrics on
  it requires updating `semantic/authoring/**` and passing
  `python scripts/semantic/build_registry.py --target-dir target --validate
  --max-warnings 0`. Renaming an `api_` model also requires grepping the
  metrics-dashboard repo's query SQL by `FROM`/`JOIN` (filenames and IDs carry legacy
  names — grep the SQL, not the names).

## ClickHouse gotchas (the short list)

- An output alias **shadows** the source column in a same-level `WHERE` — relabel
  constants in an outer subquery.
- LEFT JOINs that need NULLs on unmatched rows require `SET join_use_nulls = 1` in a
  `pre_hook` — not `nullIf` workarounds. (Models that must restore the default add the
  paired `post_hook` reset; see the pools models.)
- Balance math in exact `Int256` (`reinterpretAsInt256(reverse(unhex(...)))`), never
  `Float64` — float sums fabricate "balanced" results.
- `execution.logs` topics/addresses are bare hex, **no `0x` prefix**.
- Never estimate a date from a block delta — block↔date is non-linear; join
  `execution.blocks` / read `block_timestamp`.
- One INSERT touching >100 partitions fails (Code 252) and ClickHouse Cloud blocks
  raising `max_partitions_per_insert_block` (Code 452) — but never "fix" this by
  widening partitions on an `insert_overwrite` model (grain rule above).
- Memory error Code 241 mentioning `(total)` and `allocate chunk 0.00 B` means the
  **server** is saturated (your query is the overcommit victim), not that the model is
  too fat — retry when the cron is idle before rewriting anything.
- The Cerebro query surface rejects correlated subqueries and blocks `SYSTEM`/DDL.
- The *why* behind the duplication rules (engine zoo = merge-time collision policies;
  lazy merges; value-inflation that row checks can't see):
  `docs/lessons/ch-merge-semantics-primer.md`.

## Where knowledge lives

- `docs/agents.md` — architecture of this knowledge system itself (file roles,
  artifact pipeline, gates/ratchets, CI tiers, diagrams) — read when changing
  the system rather than using it.
- `docs/lessons/INDEX.md` — mistake classes with status (`observed → remediated →
  enforced`) and evidence. **Check here before diagnosing a data-quality symptom.**
- `docs/README.md` — which docs are durable references vs point-in-time snapshots
  (`docs/model_review/` is a dated audit; do not act on its findings without re-verifying).
- `README.md` — deep detail: "Running Models", "Data Modeling Conventions", "Contract
  Decoding System", "Semantic Layer Workflow", "Observability and Testing".
- `docs/semantic-authoring.md` + `semantic/README.md` — semantic layer authoring.
- `scripts/refresh/README.md`, `scripts/full_refresh/README.md` — runner internals.

## Verification caveats

- `dbt docs generate` writes a catalog with **0 model nodes** (dbt-clickhouse emits
  sources only) — expected, don't chase it. New decode models crash docs generate at
  compile time until first built.
- Schema tests use `var('test_lookback_days', 7)`; Elementary anomaly tests are tagged
  `elementary`.
- Before quoting a "latest" snapshot from any mart, check `max(date)` — sources have
  silently halted before and argMax-style marts keep serving the last day as current.

# New Model Checklist

Scaffold a new dbt model correctly and completely. Input: model name / purpose /
domain.

Work through ALL of these — a model is not just its SQL:

1. **Read the scoped guide** for the target directory (`models/<domain>/AGENTS.md` if
   present, plus root AGENTS.md rules) and check `docs/lessons/INDEX.md` for classes
   that apply to the model's shape (incremental? decode? cumulative? insert_overwrite?).

2. **SQL config conventions**: correct layer prefix (stg_/int_/fct_/api_), tags
   (`production`, domain, `api:<endpoint>` + `granularity:` + tier if a mart),
   materialization + strategy per dbt_project.yml defaults, `partition_by` grain equal
   to the overwrite grain if insert_overwrite, query knobs in `query_settings=`,
   paired hooks if any session setting is changed. A model NAMED `api_*` must either
   enter the endpoint convention (api: tag + production) or set
   `meta.api.exclude_from_api: true` — the api-tags gate rejects the in-between.

3. **Build order**: build inputs first; seed the new model once with
   `dbt run --full-refresh -s <model>`, then VERIFY IT HAS ROWS —
   docs/lessons/never-seeded-incremental.md. A new decode model must be built before
   `dbt docs generate` (compile-time crash otherwise).

4. **schema.yml**: run the [generate-schema workflow](generate-schema.md) for the
   directory. Add `meta.full_refresh` stages if the model needs orchestrated
   backfills (staged models MUST use the append-if-`start_month` strategy expression
   — the incremental-policy gate rejects a staged literal insert_overwrite); add
   `meta.agent` (grain/invariants/hazards — see agent_context/profiles.yml header)
   if the model is high-risk (incremental / cumulative / staged / decode).

5. **Semantic layer** (tracked api_/fct_/int_ models): author the block in
   `semantic/authoring/<module>/`, then `python scripts/semantic/build_registry.py
   --target-dir target --validate --max-warnings 0`. The scaffold gate
   (`scripts/semantic/scaffold_candidates.py --gate`) fails on new unauthored
   tracked models. See docs/semantic-authoring.md.

6. **Validate**: `python scripts/checks/run_all.py` (after `dbt parse` so the new
   model resolves — the change-aware gate BLOCKS on changed models unknown to the
   agent context), then `dbt build -s <model>`.

7. **Cross-repo**: if it's an `api_` model the metrics-dashboard and Cerebro API may
   need wiring — flag this to the user rather than assuming.

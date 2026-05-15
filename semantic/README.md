# Semantic Layer

This directory holds the authoring side of the Gnosis Analytics
semantic layer — the metric-and-relationship registry that AI agents,
dashboards, and BI tools consume.

**Full documentation lives at** [docs.analytics.gnosis.io →
Data Modeling → Semantic Layer](https://docs.analytics.gnosis.io/data-pipeline/transformation/semantic-layer/)
(source: `cerebro-docs/docs/data-pipeline/transformation/semantic-layer/`).
This README is the short version for authors working in this repo.

## What lives here

```
semantic/
├── authoring/                     -- per-module semantic_models + metrics
│   ├── consensus/semantic_models.yml
│   ├── execution/<submodule>/semantic_models.yml
│   ├── revenue/semantic_models.yml
│   ├── bridges/semantic_models.yml
│   ├── mixpanel_ga/semantic_models.yml
│   └── shared/semantic_models.yml      -- the time spines
├── relationships/                 -- cross-sector join edges
│   ├── time_spines.yml                  -- time-grain composition
│   ├── user_pseudonym.yml               -- cross-sector user-overlap
│   ├── execution_graph.yml              -- entity-specific joins
│   └── execution_transactions.yml
└── overrides/
    └── defaults.yml                     -- metric aliases, docs enrichments
```

The user-keyed marts referenced by `semantic/authoring/` live in:

- `models/shared/marts/dim_time_spine_{daily,weekly,monthly}.sql`
- `models/revenue/marts/fct_revenue_per_user_{weekly,monthly}.sql`
- `models/execution/gpay/marts/fct_execution_gpay_users_distinct.sql`
- `models/execution/gnosis_app/marts/fct_execution_gnosis_app_users_distinct.sql`
- `models/execution/Circles/marts/fct_execution_circles_human_avatars_distinct.sql`

## Five invariants (skim before authoring)

1. **Measure names are globally unique.** Convention:
   `<metric_name>_value`. Two `value_value` measures in two
   semantic_models is a build error.
2. **Root semantic_model's `quality_tier` matches the metric's.** A
   metric tagged `approved` against a `candidate` root model appears
   in `discover_metrics` but `query_metrics` rejects it at runtime.
3. **Monday-anchored weeks everywhere.** Use `toStartOfWeek(date, 1)`
   or `toMonday(date)` — never bare `toStartOfWeek(date)`.
4. **`user_pseudonym` hash space is project-wide.** Always use the
   `pseudonymize_address` macro. Never call `sipHash64` directly.
5. **Relationships only reference materialised models.** `dbt build`
   first, declare the relationship second.

## Quick authoring workflow

```bash
# 1. Edit semantic/authoring/<module>/semantic_models.yml
# 2. Edit semantic/relationships/*.yml if cross-sector
# 3. Build the registry locally
python3 scripts/semantic/build_registry.py --target-dir target --validate

# 4. Regenerate the graph diagram (gets committed to cerebro-docs)
python3 scripts/semantic/generate_graph_diagram.py \
    --target-dir target \
    --output ../cerebro-docs/docs/data-pipeline/transformation/semantic-layer/graph.md

# 5. Force-reload the MCP runtime to pick up your changes
# (in your MCP client, call):
#   mcp__cerebro-dev__reload_semantic_registry()

# 6. Smoke-test
#   mcp__cerebro-dev__discover_metrics(query="<your new metric>")
#   mcp__cerebro-dev__query_metrics(metrics=[...], dimensions=[...])
```

## Where the planner code lives

The MCP-side planner (the `query_metrics` / `discover_metrics` /
`explain_metric_query` / `reload_semantic_registry` implementation)
is in the **`cerebro-mcp`** repo, not this one. Planner bugs and
feature work happen there. Key files:

- `src/cerebro_mcp/semantic_sql_compiler.py` (SQL emission, agg
  translation: `count_distinct` → `uniqExact`)
- `src/cerebro_mcp/semantic_planner.py` (dimension resolution,
  time-spine upcasts)
- `src/cerebro_mcp/semantic_graph.py` (reachability graph + cost-based
  path search)
- `src/cerebro_mcp/tools/semantic.py` (MCP tool surface)

## Where to file issues

- **Authoring / data-model questions** (this repo, dbt-cerebro): file
  an issue with the `semantic-layer` tag.
- **Planner / SQL-compiler bugs** (cerebro-mcp): file an issue against
  cerebro-mcp; reference the metric or query that exposed the bug.
- **Documentation gaps** (cerebro-docs): PR against
  `docs/data-pipeline/transformation/semantic-layer/`.

# Authoring the Knowledge Graph

This guide is the recipe for exposing a dbt model to the **knowledge-graph
semantic layer** — the node/edge types that power the published
`semantic_graph_catalog.json` contract, the Graph Explorer mini-app, and the
agent-facing MCP tools (`search_graph_catalog`, `explore_neighborhood`,
`calculate_flow_efficiency`).

A model becomes a graph **edge** by adding a `config.meta.cerebro.graph` block.
Its endpoints are **nodes** of a declared *kind*. Cross-model joins (relationship
"axes") are authored separately in `semantic/relationships/*.yml`.

## TL;DR recipe

1. **Find the entity pair** in your model — the two columns that form an edge
   (e.g. `fct_user_token_positions`: `user_address` → `token_address`).
2. **Check the kinds exist** in [`graph_kinds.yml`](graph_kinds.yml). If a kind is
   new, add it there (with a `description` + `synonyms`) and bump
   `schema_version`. Unknown kinds are a **build error** (`graph_meta_unknown_kind`).
3. **(Optional) scaffold it**: `python scripts/semantic/scaffold_candidates.py --modules execution --write`
   suggests a candidate, `enabled: false` graph block for high-confidence column
   patterns (marked with `cerebro.graph_review_required: true`). Review and edit.
4. **Author the block** under the model in `semantic/authoring/<module>/semantic_models.yml`:
   ```yaml
   - name: execution_user_token_positions
     model: ref('fct_user_token_positions')
     config:
       meta:
         cerebro:
           owner: execution_team
           quality_tier: candidate          # graph tier inherits the model's
           question_synonyms: [who holds this token, user token holdings]
           graph:
             enabled: true
             profile: user_to_token         # GLOBALLY UNIQUE id
             source_column: user_address
             target_column: token_address
             source_kind: address           # must be in graph_kinds.yml
             target_kind: token
             directed: true
             time_column: block_date        # optional; must be temporal
             weight_column: balance_usd     # optional; must be NUMERIC (required for flow)
   ```
5. **Validate locally**:
   ```bash
   python scripts/semantic/build_registry.py --target-dir target --validate
   python scripts/semantic/graph_gate.py --target-dir target   # must print "0 net-new graph errors"
   ```
   Triage any `graph_meta_*` rows in `target/semantic_validation_report.json`.
6. **Verify in the MCP** (after `reload_semantic_registry`):
   `search_graph_catalog(query="user_to_token", min_quality_tier="candidate")`
   should return your profile.
7. **Promote** later by flipping `quality_tier: approved` once the columns and
   profile name are stable (PR + domain-owner signoff). Profile names are a
   public contract — renames need a deprecation window.

## Field reference (`config.meta.cerebro.graph`)

| Field | Req | Notes |
|---|---|---|
| `enabled` | yes | `true` to activate. `false` blocks are inert (used by scaffolding). |
| `profile` | yes | Globally-unique edge id. Becomes the catalog `edge_type` + tool selector. |
| `source_column` / `target_column` | yes | Edge endpoints. Must exist on the model (backtick reserved words: `` `from` ``). Expressions allowed but skip existence check. |
| `source_kind` / `target_kind` | yes | Node kinds — **must be registered in `graph_kinds.yml`**. |
| `directed` | yes* | `true`/`false`. Undirected edges are de-duplicated on the canonical pair. |
| `time_column` | no | Temporal column for windowed traversal. |
| `weight_column` | no | Numeric column. **Required for `calculate_flow_efficiency`**; else flow falls back to edge counts. |
| `node_enrichment_model` / `node_enrichment_key` | no | Model + key that labels nodes. |
| `evidence_model` / `evidence_source_column` / `evidence_target_column` | no | Backing rows for an edge (defaults to the model + endpoints). |
| `default_filters` | no | `column -> predicate` row filters (`valid_address`, `not_null_or_empty`, or a literal). Control keys (`limit`, `hops`, …) are stripped automatically. |

\* `directed` is required in spirit (always author it); the extractor defaults to `true` if omitted.

## Rules that the build enforces

- Unknown node kind → **error** (`graph_meta_unknown_kind`).
- Missing required field on an `enabled` block → error.
- `source/target/time/weight_column` not on the model → error (expressions exempt).
- `node_enrichment_model` / `evidence_model` not in the registry → error.
- Duplicate `profile` id across models → error.
- A graph block on a `docs_only` model → error; on `candidate`/`approved` it
  inherits that tier.

## How it flows downstream

`config.meta.cerebro.graph` (authoring) → flattened to `model.semantic.meta.graph`
in `semantic_registry.json` → compiled into `semantic_graph_catalog.json`
(`profiles` are 1:1 with the cerebro-mcp `GraphProfile`; the committed
[`schemas/semantic_graph_catalog.schema.json`](../schemas/semantic_graph_catalog.schema.json)
is the shared contract) → cerebro-mcp loads the catalog (falling back to live
discovery on any mismatch) → the Graph Explorer + the three graph tools.

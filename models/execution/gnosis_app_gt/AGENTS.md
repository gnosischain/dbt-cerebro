# models/execution/gnosis_app_gt/ — scoped guide

Gnosis App data indexed by the envio_ga indexer (see `_envio_ga_sources.yml`).
This tree has its own SOURCE semantics, PRIVACY tiers, and a dedicated policy
gate. Read with models/execution/AGENTS.md and root AGENTS.md; dated build
evidence lives in docs/gnosis_app_gt_build_spec.md (verify before reuse).

## Source semantics (envio_ga is NOT execution.*)

- Incremental watermarks key on **`_synced_block`** — there is no
  insert_version-style column here; don't port watermark idioms from the
  execution decode chains.
- **Money fields arrive as raw integer strings.** Convert via exact
  decimal/Int paths only — reinterpret/Float64 casts corrupt large values
  (root AGENTS.md Int256 rule applies with force here).
- **Registry rows ≠ users.** Registration tables include entries with no
  active avatar/wallet; user counts come from the avatar/wallet spine models,
  never raw registry cardinality.

## Privacy tiers

- Staging models carry raw addresses/identities → `privacy_tier: internal`,
  excluded from MCP/API, some blocked from ad-hoc query. Reason about them
  from schema.yml, not by sampling.
- Only the `*_public` marts expose sanitized fields. Never add `api:` tags or
  MCP exposure to a non-public model in this tree.

## Policy gate (CI: scripts/checks/envio_ga_policy.py, runs in run_all.py)

- Incremental models over envio_ga must set `partition_by`.
- Models reading the STRETCH tables (`transaction`, `transfer`,
  `transaction_action` — hundreds of millions of rows) must carry the
  `stretch` tag and build via the microbatch/batch runner, never a plain full
  run.

## Validation

- `python scripts/checks/run_all.py`; containment/floor gates:
  `dbt test -s gnosis_app_gt` (tests/gnosis_app_gt_*.sql).

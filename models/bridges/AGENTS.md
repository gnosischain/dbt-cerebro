# models/bridges/ — scoped guide

Cross-chain bridge flows and netflow marts (per bridge / chain / token /
direction). Read with root AGENTS.md.

## Invariants

- **Netflow is signed by `direction`** (in vs out per bridge). Any new mart
  must preserve the sign convention of the intermediates — a dropped direction
  filter silently turns netflow into volume.
- **Cumulative netflow marts are window-function cumulations over the fct
  layer, NOT `{{ this }}` self-references** — they recompute safely for any
  window; no backfill-ordering constraints in this tree.
- **Staging is pre-aggregated upstream** (externally fed daily aggregates, not
  raw on-chain decode). A silent upstream halt shows up as flat KPIs, not
  errors — check staging `max(date)` before touching model logic.
- v1 and v2 intermediates coexist (v2 expects pre-aggregated daily input and
  is dev-tagged until cutover). Check the `dev` tag to see which variant feeds
  production before editing either.
- KPI snapshot marts serve latest values — stale-snapshot-caveat applies:
  verify freshness before quoting an "all-time"/"7d" number.

## Validation

- `python scripts/checks/run_all.py`; `dbt test -s tag:bridges` after changes.

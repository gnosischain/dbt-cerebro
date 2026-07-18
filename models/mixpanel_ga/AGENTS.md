# models/mixpanel_ga/ — scoped guide

Mixpanel product-analytics data for the Gnosis App. PRIVACY IS THE DESIGN
CONSTRAINT of this directory. Read with root AGENTS.md.

## Privacy boundary (non-negotiable)

- Every model here inherits `privacy:mixpanel_ga` tagging and
  `meta.api.exclude_from_api: true` from dbt_project.yml — NO model in this
  tree is ever served by the Cerebro API.
- **Never add an `api:` tag here** — `tests/test_mixpanel_privacy.py` fails the
  build if one appears (it runs in `scripts/checks/run_all.py`).
- MCP exposure is two-tier: aggregate-only views may be reachable; per-user
  grain and cross-domain joins are blocked at the semantic registry
  (`quality_tier: blocked`) and sensitive views opt out per-model with
  `meta.expose_to_mcp: false` in schema.yml. Preserve those opt-outs; the
  agent-context public artifact also drops these models.
- Identifiers are pseudonymized with the `CEREBRO_PII_SALT` env salt — never
  materialize a raw user identifier into a model other agents/apps can read,
  and never join salted ids back to on-chain addresses in an exposed model.

## Write path

- The daily models are staged (`meta.full_refresh`) monthly-partitioned
  incrementals — staged models must use the append-if-`start_month` strategy
  expression (staged-insert-overwrite-wipe; gate-enforced for new models).

## Validation

- `python scripts/checks/run_all.py` (includes the privacy pytest) and
  `dbt test -s tag:mixpanel_ga`.

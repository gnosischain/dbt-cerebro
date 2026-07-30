# models/celo/ — scoped guide

Gnosis Pay activity mirrored on the Celo chain (wallets, payments, balances,
retention). Read with root AGENTS.md.

## Invariants

- **Sources are crawler-fed** (`crawlers_data.celo_gpay_*`), not on-chain
  decode: freshness depends on an external crawler. A flat metric here is a
  halted crawler until proven otherwise — check the source `max(date)` before
  diagnosing model logic (stale-snapshot-caveat pattern).
- **Keep Celo and Gnosis gpay metrics separate.** Same product, different
  chain: never sum or blend them in a mart without an explicit chain
  dimension; cross-chain totals are a presentation-layer decision.
- **No cumulative (`{{ this }}`) models in this tree** — backfills are
  order-free; monthly insert_overwrite reprocessing is safe per partition.
- Wallet-recognition timing: a Safe can be recognized shortly AFTER its first
  activity lands; the activity models rely on ReplacingMergeTree latest-row
  semantics to reconcile — don't "fix" apparent same-key duplicates by hand,
  read docs/lessons/ch-merge-semantics-primer.md first.

## Validation

- `python scripts/checks/run_all.py`; `dbt test -s tag:celo` (or the gpay
  subtree selector) after any change.

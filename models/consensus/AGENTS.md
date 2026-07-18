# models/consensus/ — scoped guide

Beacon-chain data: validators, balances, income, deposits/withdrawals, APY.
Read with root AGENTS.md; lessons cited by id live in docs/lessons/.

## Invariants

- **mGNO→GNO conversion happens ONLY in the originating int models.** Everything
  downstream reads GNO units already. Before adding any `/ 32` (or `* 32`),
  grep the lineage for an existing conversion — a second one silently scales
  every downstream mart. If you add a new originating model, convert there and
  nowhere else.
- **Never quote a "latest"/period-end snapshot without checking `max(date)`.**
  The consensus source has silently halted before; argMax-style marts keep
  serving the last ingested day as current (stale-snapshot-caveat).
- **APY derives from the income chain.** The per-index APY model is a thin
  derivation of daily income — do not reintroduce an independent APY
  calculation (deposit-rounding tricks were retired for being wrong).
- **Income reconciliation uses raw vs EFFECTIVE deposits.** Raw deposit events
  and effective-balance changes are different series; the reconciliation tests
  under tests/ encode which belongs where — read them before "fixing" a
  mismatch.

## Write path

- Most heavy models: monthly `insert_overwrite` partitions with staged
  `meta.full_refresh` — staged models MUST use the append-if-`start_month`
  strategy expression; a literal insert_overwrite on a staged model is exactly
  the class that wiped withdrawals/proposer history
  (staged-insert-overwrite-wipe; the incremental-policy gate now rejects it).
- Per-index models are memory-heavy: external group-by/sort knobs are set in
  paired pre/post hooks — keep the pairs intact (root AGENTS.md hooks rule).

## Validation

- `python scripts/checks/run_all.py` plus the consensus reconciliation tests:
  `dbt test -s tag:consensus` (income/proposer/attestation reconciliations).

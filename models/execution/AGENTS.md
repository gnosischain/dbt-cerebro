# models/execution — tokens, balances, pools rules

## Balances and token math

- Reconcile balances in exact **`Int256`** — never `Float64`. Float sums over ~1e20-wei
  values fabricate "balanced" results for addresses that are actually short an inflow.
  Decode raw values with `reinterpretAsInt256(reverse(unhex(substring(data,1,64))))`;
  `execution.logs` topics/addresses are bare hex (no `0x`).
- When a balance looks wrong, **verify against the chain, not the model** — on-chain
  `balanceOf` (or `eth_getBalance` for native xDAI `0xeeee…eeee`) is ground truth. A
  negative balance for a real holder (not the zero-address sink) means a dropped inflow
  upstream, usually in the decode layer (`docs/lessons/decode-watermark-late-logs.md`)
  or a raw `execution.logs` hole (`docs/lessons/raw-logs-ingestion-holes.md`).

## Whitelisted tokens (`tokens_whitelist` seed)

- **Every new wrapper/vault token needs a price path** in
  `int_execution_token_prices_daily` (wrapper/derived-price branch) or it renders `$0`
  everywhere USD-valued. See `docs/lessons/unpriced-wrapper-token.md`.
- **And historical seeding**: an incremental model whose join input came online after
  the table was first created empty stays at 0 rows forever — the daily runner can't
  reach back. One-time `dbt run --full-refresh -s <model>` first. See
  `docs/lessons/never-seeded-incremental.md`.
- A token's stage `start_date` (in `meta.full_refresh` stages / whitelist `date_start`)
  must not post-date its real first on-chain activity, or history is silently short and
  balances go negative. See `docs/lessons/late-start-mis-staging.md`.
- Respect per-token `decimals` and `date_start`/`date_end` address pairs (EURe/GBPe
  have historical↔current addresses).
- **Adding a token is a documented workflow, not a judgement call** — follow
  [docs/workflows/add-token.md](../../docs/workflows/add-token.md) end to end (seed →
  verify first on-chain activity → confirm a price path → stages → ordered backfill →
  verify).

### Per-token scoping: where it works, and where it dies

`macros/db/symbol_filter.sql` + `var('symbol')` is the only dimension filter in the repo.
A `meta.full_refresh` stage's `vars` are **inert metadata** until the model body reads
them — dbt does not error on an unread var. Only the intersection is safe: the SQL reads
`var('symbol')` **and** the strategy resolves to `append` when `start_month` is set.

```
execution.logs + tokens_whitelist
  → int_execution_transfers_whitelisted_daily   literal insert_overwrite, no symbol var → month-scoped only
  → int_execution_tokens_address_diffs_daily    literal insert_overwrite, HAS symbol var → NEVER symbol-scope
  → int_execution_tokens_balances_native_daily  append-if-start_month, cumulative        → safe to symbol-scope
  → int_execution_tokens_balances_daily         append-if-start_month                    → safe to symbol-scope
  → int_execution_tokens_balance_cohorts_daily  append-if-start_month                    → safe to symbol-scope
  → int_execution_tokens_balances_by_sector_daily  append-if-start_month                 → safe to symbol-scope
  → int_execution_tokens_supply_holders_daily   literal insert_overwrite, no symbol var  → month-scoped only
  int_execution_tokens_transfers_daily          literal insert_overwrite, HAS symbol var → NEVER symbol-scope
```

Symbol-scoping a literal `insert_overwrite` model REPLACEs each whole month partition
with filtered-only content, wiping every other token. Re-run those months **unfiltered**
instead — `REPLACE PARTITION` is atomic and idempotent, so a whole-month recompute
reproduces every existing token exactly and picks up the new one. Conversely, running a
scopable model with `start_month` but *no* `symbol` appends a second copy of every token.
See [docs/lessons/stage-vars-scope-illusion.md](../../docs/lessons/stage-vars-scope-illusion.md);
CI gate: `scripts/checks/no_delete_insert.py` rules `stage_var_not_read` /
`staged_scoped_include_overwrite`.

## Daily carry-forward / spine models (pools, reserves, balances)

- Anchor incremental carry-forward at the **per-entity frontier**
  (`min(max(date)) GROUP BY entity`), never a single global `max(date)` — a thin,
  sporadically-active series falls off a global frontier and accretes permanent gaps.
  See `docs/lessons/global-frontier-carry-forward.md`.
- Give consuming marts their own daily spine so an upstream gap can't reach a chart.

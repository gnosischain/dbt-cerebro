# Add a Token to the Whitelist

Add one or more tokens to `seeds/tokens_whitelist.csv` and backfill **only those tokens**
through the token chain, without recomputing history for the ~50 tokens already there.
Input: token address(es), decimals, symbol, and the intended `date_start`.

The governing constraint: per-token scoping exists only where a model reads
`var('symbol')` **and** takes the `append` path. Everywhere else the run is
all-tokens-or-nothing. Getting that wrong silently duplicates or wipes the other tokens —
see `docs/lessons/stage-vars-scope-illusion.md`.

## Procedure

### 1. Author the seed row

Columns: `address,decimals,symbol,date_start,date_end,token_class`. Verify `decimals` on
chain rather than assuming 18.

### 2. Verify `date_start` against real first activity

A `date_start` later than the token's first on-chain Transfer silently truncates history
and drives balances negative (`docs/lessons/late-start-mis-staging.md`). Confirm there
are no Transfer logs before it — remember `execution.logs` addresses/topics are bare hex,
**no `0x` prefix**:

```sql
SELECT address, count(), min(toDate(block_timestamp))
FROM execution.logs
WHERE block_timestamp >= toDateTime('<date_start minus ~6 months>')
  AND block_timestamp <  toDateTime('<date_start>')
  AND topic0 = 'ddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef'
  AND address IN ('<addr without 0x, lowercase>')
GROUP BY address;
```

Zero rows means `date_start` is safe. Narrow the time window if the query times out.

### 3. Confirm a price path exists

Every new token needs one or it renders `$0` everywhere
(`docs/lessons/unpriced-wrapper-token.md`). Check the feeds the hub actually reads —
`stg_crawlers_data__defillama_prices`, `__coingecko_prices`, `__dune_prices` — for the
symbol. Wrapper/vault tokens instead need a derived-price branch in
`int_execution_token_prices_daily`.

Casing is load-bearing: `int_execution_tokens_balances_daily` filters prices with an
**exact-case** `symbol IN (…)`, and the hub takes its display casing from the seed. Seed
first, then rebuild prices, then balances.

### 4. Add stages — but only to models that can honour them

For each model you intend to run per-token, both must hold:

```bash
grep -c "var('symbol'" <model>.sql          # must be > 0
grep -n "incremental_strategy" <model>.sql  # must be ('append' if start_month else …)
```

A stage on a model failing either test is worse than no stage: the vars are inert (run
covers all tokens, duplicating them on an `append` model), or they are honoured on
`insert_overwrite` and REPLACE the partition with filtered-only content, wiping every
other token. `scripts/checks/no_delete_insert.py` rules `stage_var_not_read` and
`staged_scoped_include_overwrite` gate exactly this — run it before you run dbt.

If a model *should* be scopable but isn't, add the wiring rather than working around it:
`{% set symbol = var('symbol', none) %}` + `symbol_exclude`, the `{{ symbol_filter(...) }}`
calls, and `filters_sql=symbol_sql` on `apply_monthly_incremental_filter` so the watermark
subquery is scoped too. Mirror `int_execution_tokens_balances_daily.sql`. When adding a
`stages:` key to a `meta.full_refresh` that had none, also add an explicit `all` stage
carrying `symbol_exclude: "{{ var('symbol_exclude') }}"` — otherwise you change what a
plain `refresh.py --select <model>` does. Editing a high-risk model's `.sql` puts it in
the `agent-context-check` ratchet, so budget a `meta.agent` contract for it.

### 5. Run, in this order

Preflight: `dbt parse && python scripts/agent_context/build_agent_context.py`, and check
`target/refresh_state/` for a pending run that overlaps your selection.

Capture a per-symbol baseline for the affected window **before** the first write —
`REPLACE PARTITION` has no undo.

| Step | Command | Why |
|---|---|---|
| Seed | `dbt seed -s tokens_whitelist --full-refresh` | clean recreate; a duplicate seed row propagates forever (`duplicate-seed-drift`) |
| Prices | `dbt run -s <staging price views> int_execution_token_prices_daily` | table rebuild, no vars; must precede balances |
| Transfers/diffs | `dbt run -s int_execution_transfers_whitelisted_daily --vars '{"start_month":"<first month>","end_month":"<last month>"}'` then the same for `int_execution_tokens_address_diffs_daily` and `int_execution_tokens_transfers_daily` | literal `insert_overwrite`, **never** pass `symbol` — REPLACE PARTITION is atomic and idempotent, so whole-month recompute reproduces existing tokens exactly |
| Balances | `python scripts/full_refresh/refresh.py --select int_execution_tokens_balances_native_daily --stage <stage> --incremental-only`, then `int_execution_tokens_balances_daily` | cumulative model first; `--incremental-only` is mandatory or batch 1 recreates the table |
| Scopable marts | same `refresh.py --stage … --incremental-only` | purely additive: new rows land at a new `token_address` |
| Non-scopable marts | `dbt run -s int_execution_tokens_supply_holders_daily --vars '{"start_month":…,"end_month":…}'` | literal `insert_overwrite`, no symbol var |
| Table/view marts | plain `dbt run` | self-healing; **never** with month vars (`table-mat-batch-vars-truncation`) |

Do **not** pass `--inprocess` to `refresh.py` here. It parses once and reuses the
manifest, but `incremental_strategy` is a parse-time `var()` inside `config()` — frozen at
the initial parse it resolves to `insert_overwrite` for the balances models, which is the
wipe. Avoid running across UTC midnight: `refresh.py` derives batches from `date.today()`,
and the new token must land on the same frontier day as the rest of the table.

### 6. Verify

- **No collateral damage**: per-symbol counts for the window vs the baseline. Every
  pre-existing symbol unchanged; a decrease is a wipe, an exact doubling is the append
  duplicate.
- **Coverage**: `min(date)` equals the seed `date_start`; `max(date)` equals the table's
  **global** `max(date)` (a lagging frontier makes the next daily run reset the new
  token's balance to zero); `rows = holders × days` for dense carry-forward.
- **No duplicates**: `count() - uniqExact(<grain>)` = 0.
- **Conservation**: `sum(balance_raw)` over all addresses *including* `0x0` is exactly 0
  per `(symbol, date)`. Summing `balance_usd` without excluding `0x0` correctly yields 0 —
  exclude it to read real supply.
- **No negative real holders**, and `balance_usd` non-null coverage ≈ 100%.
- `dbt test -s <models>` plus `dq_daily_unpriced_tokens`, `dq_daily_late_token_start`,
  `dq_daily_balance_conservation`.
- `python scripts/checks/run_all.py --full` — every step, not `--fast`.

## Retry safety

Steps using `REPLACE PARTITION` are idempotent — retry freely. The `append` steps are
exactly-once **into empty space**; a retry after a partial write appends a second copy.
Repair per month, chronologically, with the model's documented overlapping-window path:

```bash
dbt run -s int_execution_tokens_balances_native_daily \
  --vars '{"start_month":"<M>","end_month":"<M>","symbol":"<tokens>","reprocess_overwrite":true}'
```

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

## Daily carry-forward / spine models (pools, reserves, balances)

- Anchor incremental carry-forward at the **per-entity frontier**
  (`min(max(date)) GROUP BY entity`), never a single global `max(date)` — a thin,
  sporadically-active series falls off a global frontier and accretes permanent gaps.
  See `docs/lessons/global-frontier-carry-forward.md`.
- Give consuming marts their own daily spine so an upstream gap can't reach a chart.

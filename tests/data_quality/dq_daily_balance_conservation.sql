{{ config(enabled=false, severity='warn', tags=['data_quality', 'balances', 'deprecated']) }}
-- RETIRED 2026-09 (WL-054 Stage 2): guarded the transfer-accumulated chain's double-entry identity (sum over every address, zero address included, must be 0). The census successor int_rpc_state_indexer_token_balances_daily is not cumulative, stores no tombstones and excludes the zero address, so the identity does not exist there; the census invariant is rpc_state_indexer_token_balances_coverage_daily (holder sum == totalSupply per token-day). Kept disabled, not deleted: docs/lessons/sparse-zero-row-stale-survival.md cites it as the 2026-07-17 detection.
-- Accounting identity, not a heuristic: the balances chain is double-entry (every
-- transfer debits the sender and credits the recipient; mints/burns net against the
-- 0x0 sink), so per (symbol, date) the sum of balance_raw over ALL addresses —
-- INCLUDING 0x0...0 — must be EXACTLY zero. A nonzero residual means some address's
-- row is stale (frozen at a prior value). Catches the stale-POSITIVE class that the
-- negative-balance test is blind to (a spend-to-zero address keeping its old positive
-- row inflates apparent supply). Lesson: sparse-zero-row-stale-survival — first
-- caught 2026-07-17 via this exact check: 78/78 GNO spend-to-zero addresses stale,
-- +3,115.80 GNO phantom vs on-chain totalSupply(), 16 tokens affected.
-- (xDAI needs no exclusion: native xDAI has no rows in this table since its
-- balance_diffs source halted; if it returns, verify its identity before including.)
SELECT symbol, date, sum(balance_raw) AS residual_raw
FROM {{ ref('int_execution_tokens_balances_native_daily') }}
WHERE date >= today() - 3
GROUP BY symbol, date
HAVING residual_raw != 0
ORDER BY abs(residual_raw) DESC

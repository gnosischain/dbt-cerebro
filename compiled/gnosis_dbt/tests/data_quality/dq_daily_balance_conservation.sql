
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
FROM `dbt`.`int_execution_tokens_balances_native_daily`
WHERE date >= today() - 3
GROUP BY symbol, date
HAVING residual_raw != 0
ORDER BY abs(residual_raw) DESC
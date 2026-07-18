{{ config(severity='warn', tags=['production', 'data_quality', 'data_quality_daily', 'balances']) }}
-- A non-rebasing ERC-20 holder can't be negative on-chain; a negative balance for a
-- REAL holder (not the 0x00..00 mint/burn sink) = a dropped inflow upstream.
-- Lessons: decode-watermark-late-logs, raw-logs-ingestion-holes, duplicate-seed-drift.
-- -0.001 floor skips rounding noise. WARN, not error: a transient residual can be a
-- raw-layer gap awaiting re-index.
SELECT symbol, address, date, balance_raw, balance
FROM {{ ref('int_execution_tokens_balances_native_daily') }}
WHERE date >= today() - 3
  AND address != '0x0000000000000000000000000000000000000000'
  AND balance < -0.001
ORDER BY balance ASC

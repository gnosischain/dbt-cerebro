{{ config(enabled=false, severity='warn', tags=['data_quality', 'balances', 'deprecated']) }}
-- RETIRED 2026-09 (WL-054 Stage 2): guarded against negative real-holder balances in the transfer-accumulated chain. The census successor stores only balance_raw > 0 (UInt256 at the source) and excludes the zero address, so a negative balance is impossible by construction and this test would pass vacuously. Kept disabled, not deleted.
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

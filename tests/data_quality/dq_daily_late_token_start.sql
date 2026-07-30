{{ config(severity='warn', tags=['production', 'data_quality', 'data_quality_daily']) }}
-- A whitelisted token whose first modeled transfer is materially later than its
-- whitelist date_start was mis-staged or never backfilled — its early history is
-- silently absent and cumulative balances can go negative.
-- Lesson: late-start-mis-staging (wstETH was staged 2025-01 but live since 2023-02).
SELECT
    w.symbol,
    toString(w.date_start) AS whitelist_start,
    toString(t.first_seen) AS model_first_seen
FROM {{ ref('tokens_whitelist') }} w
INNER JOIN (
    SELECT symbol, min(date) AS first_seen
    FROM {{ ref('int_execution_transfers_whitelisted_daily') }}
    GROUP BY symbol
) t ON t.symbol = w.symbol
WHERE t.first_seen > addMonths(toDate(w.date_start), 1)

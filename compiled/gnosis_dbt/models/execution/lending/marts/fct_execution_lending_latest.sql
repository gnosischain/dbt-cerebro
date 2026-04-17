

-- Lender / borrower counts per (window, protocol, token) plus protocol-scoped 'ALL'
-- tokens and an all-protocols 'ALL' bucket for cross-protocol totals. Widgets use
-- `token = 'ALL'` rows and apply the optional `protocol` filter (filterField3).
-- `protocol = 'ALL'` rows are retained for dashboards that intentionally aggregate
-- across protocols (e.g. a "total lenders on Gnosis" KPI).

WITH

latest_date AS (
    SELECT MAX(date) AS max_date
    FROM `dbt`.`int_execution_lending_aave_daily`
    WHERE date < today()
),

rng AS (
    SELECT '7D' AS window, 7 AS days
),

bounds AS (
    SELECT
        r.window,
        r.days,
        w.max_date,
        subtractDays(w.max_date, r.days)        AS curr_start,
        w.max_date                              AS curr_end,
        subtractDays(w.max_date, 2 * r.days)    AS prev_start,
        subtractDays(w.max_date, r.days)        AS prev_end
    FROM rng r
    CROSS JOIN latest_date w
),

-- Per (protocol, token)
curr_lenders AS (
    SELECT b.window, d.protocol, d.symbol AS token,
        toUInt64(groupBitmapMerge(d.lenders_bitmap_state)) AS value
    FROM `dbt`.`int_execution_lending_aave_daily` d
    INNER JOIN bounds b
        ON d.date > b.curr_start AND d.date <= b.curr_end AND d.lenders_bitmap_state IS NOT NULL
    GROUP BY b.window, d.protocol, d.symbol
),
prev_lenders AS (
    SELECT b.window, d.protocol, d.symbol AS token,
        toUInt64(groupBitmapMerge(d.lenders_bitmap_state)) AS value
    FROM `dbt`.`int_execution_lending_aave_daily` d
    INNER JOIN bounds b
        ON d.date > b.prev_start AND d.date <= b.prev_end AND d.lenders_bitmap_state IS NOT NULL
    GROUP BY b.window, d.protocol, d.symbol
),
curr_borrowers AS (
    SELECT b.window, d.protocol, d.symbol AS token,
        toUInt64(groupBitmapMerge(d.borrowers_bitmap_state)) AS value
    FROM `dbt`.`int_execution_lending_aave_daily` d
    INNER JOIN bounds b
        ON d.date > b.curr_start AND d.date <= b.curr_end AND d.borrowers_bitmap_state IS NOT NULL
    GROUP BY b.window, d.protocol, d.symbol
),
prev_borrowers AS (
    SELECT b.window, d.protocol, d.symbol AS token,
        toUInt64(groupBitmapMerge(d.borrowers_bitmap_state)) AS value
    FROM `dbt`.`int_execution_lending_aave_daily` d
    INNER JOIN bounds b
        ON d.date > b.prev_start AND d.date <= b.prev_end AND d.borrowers_bitmap_state IS NOT NULL
    GROUP BY b.window, d.protocol, d.symbol
),

-- Per-protocol all-tokens (collapse token dimension, keep protocol)
curr_lenders_protocol_all AS (
    SELECT b.window, d.protocol, 'ALL' AS token,
        toUInt64(groupBitmapMerge(d.lenders_bitmap_state)) AS value
    FROM `dbt`.`int_execution_lending_aave_daily` d
    INNER JOIN bounds b
        ON d.date > b.curr_start AND d.date <= b.curr_end AND d.lenders_bitmap_state IS NOT NULL
    GROUP BY b.window, d.protocol
),
prev_lenders_protocol_all AS (
    SELECT b.window, d.protocol, 'ALL' AS token,
        toUInt64(groupBitmapMerge(d.lenders_bitmap_state)) AS value
    FROM `dbt`.`int_execution_lending_aave_daily` d
    INNER JOIN bounds b
        ON d.date > b.prev_start AND d.date <= b.prev_end AND d.lenders_bitmap_state IS NOT NULL
    GROUP BY b.window, d.protocol
),
curr_borrowers_protocol_all AS (
    SELECT b.window, d.protocol, 'ALL' AS token,
        toUInt64(groupBitmapMerge(d.borrowers_bitmap_state)) AS value
    FROM `dbt`.`int_execution_lending_aave_daily` d
    INNER JOIN bounds b
        ON d.date > b.curr_start AND d.date <= b.curr_end AND d.borrowers_bitmap_state IS NOT NULL
    GROUP BY b.window, d.protocol
),
prev_borrowers_protocol_all AS (
    SELECT b.window, d.protocol, 'ALL' AS token,
        toUInt64(groupBitmapMerge(d.borrowers_bitmap_state)) AS value
    FROM `dbt`.`int_execution_lending_aave_daily` d
    INNER JOIN bounds b
        ON d.date > b.prev_start AND d.date <= b.prev_end AND d.borrowers_bitmap_state IS NOT NULL
    GROUP BY b.window, d.protocol
),

-- All protocols, all tokens (aggregate everything — unique wallets across Gnosis)
curr_lenders_all AS (
    SELECT b.window, 'ALL' AS protocol, 'ALL' AS token,
        toUInt64(groupBitmapMerge(d.lenders_bitmap_state)) AS value
    FROM `dbt`.`int_execution_lending_aave_daily` d
    INNER JOIN bounds b
        ON d.date > b.curr_start AND d.date <= b.curr_end AND d.lenders_bitmap_state IS NOT NULL
    GROUP BY b.window
),
prev_lenders_all AS (
    SELECT b.window, 'ALL' AS protocol, 'ALL' AS token,
        toUInt64(groupBitmapMerge(d.lenders_bitmap_state)) AS value
    FROM `dbt`.`int_execution_lending_aave_daily` d
    INNER JOIN bounds b
        ON d.date > b.prev_start AND d.date <= b.prev_end AND d.lenders_bitmap_state IS NOT NULL
    GROUP BY b.window
),
curr_borrowers_all AS (
    SELECT b.window, 'ALL' AS protocol, 'ALL' AS token,
        toUInt64(groupBitmapMerge(d.borrowers_bitmap_state)) AS value
    FROM `dbt`.`int_execution_lending_aave_daily` d
    INNER JOIN bounds b
        ON d.date > b.curr_start AND d.date <= b.curr_end AND d.borrowers_bitmap_state IS NOT NULL
    GROUP BY b.window
),
prev_borrowers_all AS (
    SELECT b.window, 'ALL' AS protocol, 'ALL' AS token,
        toUInt64(groupBitmapMerge(d.borrowers_bitmap_state)) AS value
    FROM `dbt`.`int_execution_lending_aave_daily` d
    INNER JOIN bounds b
        ON d.date > b.prev_start AND d.date <= b.prev_end AND d.borrowers_bitmap_state IS NOT NULL
    GROUP BY b.window
)

SELECT 'Lenders' AS label, c.window, c.protocol, c.token,
    toFloat64(COALESCE(c.value, 0)) AS value,
    CASE WHEN p.value IS NULL OR p.value = 0 THEN NULL
         ELSE ROUND((toFloat64(c.value) / toFloat64(p.value) - 1) * 100, 1) END AS change_pct
FROM curr_lenders c
LEFT JOIN prev_lenders p USING (window, protocol, token)

UNION ALL

SELECT 'Borrowers', c.window, c.protocol, c.token,
    toFloat64(COALESCE(c.value, 0)),
    CASE WHEN p.value IS NULL OR p.value = 0 THEN NULL
         ELSE ROUND((toFloat64(c.value) / toFloat64(p.value) - 1) * 100, 1) END
FROM curr_borrowers c
LEFT JOIN prev_borrowers p USING (window, protocol, token)

UNION ALL

SELECT 'Lenders', c.window, c.protocol, c.token,
    toFloat64(COALESCE(c.value, 0)),
    CASE WHEN p.value IS NULL OR p.value = 0 THEN NULL
         ELSE ROUND((toFloat64(c.value) / toFloat64(p.value) - 1) * 100, 1) END
FROM curr_lenders_protocol_all c
LEFT JOIN prev_lenders_protocol_all p USING (window, protocol, token)

UNION ALL

SELECT 'Borrowers', c.window, c.protocol, c.token,
    toFloat64(COALESCE(c.value, 0)),
    CASE WHEN p.value IS NULL OR p.value = 0 THEN NULL
         ELSE ROUND((toFloat64(c.value) / toFloat64(p.value) - 1) * 100, 1) END
FROM curr_borrowers_protocol_all c
LEFT JOIN prev_borrowers_protocol_all p USING (window, protocol, token)

UNION ALL

SELECT 'Lenders', c.window, c.protocol, c.token,
    toFloat64(COALESCE(c.value, 0)),
    CASE WHEN p.value IS NULL OR p.value = 0 THEN NULL
         ELSE ROUND((toFloat64(c.value) / toFloat64(p.value) - 1) * 100, 1) END
FROM curr_lenders_all c
LEFT JOIN prev_lenders_all p USING (window, protocol, token)

UNION ALL

SELECT 'Borrowers', c.window, c.protocol, c.token,
    toFloat64(COALESCE(c.value, 0)),
    CASE WHEN p.value IS NULL OR p.value = 0 THEN NULL
         ELSE ROUND((toFloat64(c.value) / toFloat64(p.value) - 1) * 100, 1) END
FROM curr_borrowers_all c
LEFT JOIN prev_borrowers_all p USING (window, protocol, token)
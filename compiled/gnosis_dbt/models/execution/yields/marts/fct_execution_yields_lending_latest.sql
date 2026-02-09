

WITH

latest_date AS (
    SELECT MAX(date) AS max_date
    FROM `dbt`.`int_execution_yields_aave_daily`
    WHERE date < today()
),

-- Define time windows (currently only 7D, but structure allows for 30D, 90D later)
rng AS (
    SELECT '7D' AS window, 7 AS days
    -- UNION ALL
    -- SELECT '30D' AS window, 30 AS days
    -- UNION ALL
    -- SELECT '90D' AS window, 90 AS days
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

curr_lenders AS (
    SELECT
        b.window,
        d.symbol AS token,
        toUInt64(groupBitmapMerge(d.lenders_bitmap_state)) AS value
    FROM `dbt`.`int_execution_yields_aave_daily` d
    INNER JOIN bounds b
        ON d.date > b.curr_start
        AND d.date <= b.curr_end
        AND d.lenders_bitmap_state IS NOT NULL
    GROUP BY b.window, d.symbol
),

prev_lenders AS (
    SELECT
        b.window,
        d.symbol AS token,
        toUInt64(groupBitmapMerge(d.lenders_bitmap_state)) AS value
    FROM `dbt`.`int_execution_yields_aave_daily` d
    INNER JOIN bounds b
        ON d.date > b.prev_start
        AND d.date <= b.prev_end
        AND d.lenders_bitmap_state IS NOT NULL
    GROUP BY b.window, d.symbol
),

curr_borrowers AS (
    SELECT
        b.window,
        d.symbol AS token,
        toUInt64(groupBitmapMerge(d.borrowers_bitmap_state)) AS value
    FROM `dbt`.`int_execution_yields_aave_daily` d
    INNER JOIN bounds b
        ON d.date > b.curr_start
        AND d.date <= b.curr_end
        AND d.borrowers_bitmap_state IS NOT NULL
    GROUP BY b.window, d.symbol
),

prev_borrowers AS (
    SELECT
        b.window,
        d.symbol AS token,
        toUInt64(groupBitmapMerge(d.borrowers_bitmap_state)) AS value
    FROM `dbt`.`int_execution_yields_aave_daily` d
    INNER JOIN bounds b
        ON d.date > b.prev_start
        AND d.date <= b.prev_end
        AND d.borrowers_bitmap_state IS NOT NULL
    GROUP BY b.window, d.symbol
)

SELECT
    'Lenders' AS label,
    c.window,
    c.token,
    toFloat64(COALESCE(c.value, 0)) AS value,
    ROUND((COALESCE(c.value / NULLIF(p.value, 0), 0) - 1) * 100, 1) AS change_pct
FROM curr_lenders c
LEFT JOIN prev_lenders p ON p.window = c.window AND p.token = c.token

UNION ALL

SELECT
    'Borrowers' AS label,
    c.window,
    c.token,
    toFloat64(COALESCE(c.value, 0)) AS value,
    ROUND((COALESCE(c.value / NULLIF(p.value, 0), 0) - 1) * 100, 1) AS change_pct
FROM curr_borrowers c
LEFT JOIN prev_borrowers p ON p.window = c.window AND p.token = c.token
WITH

latest_date AS (
    SELECT MAX(date) AS max_date
    FROM `dbt`.`int_execution_yields_pools_lps_daily`
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
        subtractDays(w.max_date, r.days)     AS curr_start,
        w.max_date                           AS curr_end,
        subtractDays(w.max_date, 2 * r.days) AS prev_start,
        subtractDays(w.max_date, r.days)     AS prev_end
    FROM rng r
    CROSS JOIN latest_date w
),

pool_token_map AS (
    SELECT pt.pool_address, pt.protocol, tm.token
    FROM `dbt`.`stg_pools__v3_pool_registry` pt
    INNER JOIN `dbt`.`stg_yields__tokens_meta` tm ON tm.token_address = pt.token0_address
    WHERE tm.token IS NOT NULL

    UNION ALL

    SELECT pt.pool_address, pt.protocol, tm.token
    FROM `dbt`.`stg_pools__v3_pool_registry` pt
    INNER JOIN `dbt`.`stg_yields__tokens_meta` tm ON tm.token_address = pt.token1_address
    WHERE tm.token IS NOT NULL
),

curr_lps AS (
    SELECT
        b.window,
        ptm.token,
        toUInt64(groupBitmapMerge(d.lps_bitmap_state)) AS value
    FROM `dbt`.`int_execution_yields_pools_lps_daily` d
    INNER JOIN pool_token_map ptm
        ON ptm.pool_address = d.pool_address
        AND ptm.protocol = d.protocol
    INNER JOIN bounds b
        ON d.date > b.curr_start
        AND d.date <= b.curr_end
    WHERE d.lps_bitmap_state IS NOT NULL
    GROUP BY b.window, ptm.token
),

prev_lps AS (
    SELECT
        b.window,
        ptm.token,
        toUInt64(groupBitmapMerge(d.lps_bitmap_state)) AS value
    FROM `dbt`.`int_execution_yields_pools_lps_daily` d
    INNER JOIN pool_token_map ptm
        ON ptm.pool_address = d.pool_address
        AND ptm.protocol = d.protocol
    INNER JOIN bounds b
        ON d.date > b.prev_start
        AND d.date <= b.prev_end
    WHERE d.lps_bitmap_state IS NOT NULL
    GROUP BY b.window, ptm.token
)

SELECT
    c.window,
    c.token,
    toFloat64(COALESCE(c.value, 0)) AS value,
    CASE
        WHEN p.value IS NULL OR p.value = 0 THEN NULL
        ELSE ROUND((toFloat64(c.value) / toFloat64(p.value) - 1) * 100, 1)
    END AS change_pct
FROM curr_lps c
LEFT JOIN prev_lps p ON p.window = c.window AND p.token = c.token